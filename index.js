var EventEmitter = require('events');
var util = require('util');

var Q = require('q-plus');
var _ = require('lodash');
var disque = require('thunk-disque');

var Worker = require('./worker');
var Job = require('./job');
var config = require('./config');

function Tasqueue(opts) {
    var that = this;

    // Initialize as an Event Emitter
    EventEmitter.call(that);

    // Default options
    that.opts = _.defaults(opts || {}, {
        host: 'localhost',          // disque-server host
        port: 7711,                 // disque-server port
        pollDelay: 15*1000,         // Polling delay in ms when no workers are available
        jobTimeout: 60*60*1000,     // Timeout in ms before a job is considered as failed
        queuedTTL: 3*24*60*60,      // Queued jobs TTL in sec
        failedTTL: 24*60*60,        // Failed jobs TTL in sec
        completedTTL: 24*60*60      // Completed jobs TTL in sec
    });

    that.pollTimeout = null;
    that.workers = {};
}

// Event emitter inheritance
util.inherits(Tasqueue, EventEmitter);

// Init connection to disque
Tasqueue.prototype.init = function() {
    var that = this;
    var d = Q.defer();

    that.client = disque.createClient(that.opts.port, that.opts.host, { usePromise: true })
    .on('connect', function() {
        that.running = true;
        d.resolve();
    })
    .on('error', function(err) {
        d.reject(err);
    });

    return d.promise;
};

// Shutdown the client
Tasqueue.prototype.shutdown = function(n, cb) {
    var that = this;
    that.running = false;

    // Get all jobs being processed
    var processing = that.getProcessingJobs();

    // Nothing running, stop already
    if (_.size(processing) == 0) {
        that.client.clientEnd();
        return cb();
    }

    // Maximum wait
    var timeout = setTimeout(cb, n);
    return Q.all(_.values(processing))
    .fin(function() {
        clearTimeout(timeout);
        that.client.clientEnd();
        return cb();
    });
};

// Register a new job handler
Tasqueue.prototype.registerHandler = function(handler) {
    var that = this;

    if (!!that.workers[handler.type]) {
        that.emit('error:existing-handler', handler.type);
        return;
    }

    that.workers[handler.type] = new Worker(that, handler);
    that.emit('handler:register', handler.type);
};

// Return a worker by its type
Tasqueue.prototype.getWorkerByType = function(type) {
    return this.workers[type];
};

// Return a count of available workers
Tasqueue.prototype.countAvailableWorkers = function() {
    var that = this;
    return _.reduce(that.workers, function(count, worker) {
        return count + worker.countAvailable();
    }, 0);
};

// Get a list of handled types
Tasqueue.prototype.listTypes = function() {
    return _.chain(this.workers)
        .map('type')
        .value();
};

// Return the list of all jobs being processed by workers
Tasqueue.prototype.getProcessingJobs = function() {
    return _.chain(this.workers)
        .map('processing')
        .value();
};

// Delay polling
Tasqueue.prototype.delayPoll = function() {
    var that = this;

    if (that.pollTimeout) {
        clearTimeout(that.pollTimeout);
        that.pollTimeout = null;
    }

    that.emit('client:delaying', that.opts.pollDelay);
    that.pollTimeout = setTimeout(function() {
        that.pollTimeout = null;
        that.poll();
    }, that.opts.pollDelay);
};

// Poll a job and execute it
Tasqueue.prototype.poll = function() {
    var that = this;
    if (!that.running) return;

    // Check number of available workers
    var nWorkers = 0;
    var availableWorkers = that.countAvailableWorkers();

    if (availableWorkers <= 0) {
        that.emit('client:no-workers');
        return that.delayPoll();
    }

    // List jobs that have available workers
    var types = _.chain(that.workers)
        .filter(function(worker) {
            nWorkers = nWorkers + worker.concurrency;
            return worker.isAvailable();
        })
        .map('type')
        .value();

    that.emit('client:polling', types.length, availableWorkers, nWorkers);
    Q(that.client.getjob(['NOHANG', 'WITHCOUNTERS', 'FROM', config.QUEUE]))
    .then(function(res) {
        // No job available, reset TIMEOUT
        if (!res) return that.delayPoll();

        // Convert returned array to an element
        res = res.length > 0? res[0] : null;
        // Reformat job info
        var job = Job.fromQueue(that, res);

        // Get a worker for this job
        var worker = that.getWorkerForJob(job);
        if (!worker) return that.poll();

        // Process job
        that.emit('job:start', job.id, job.getType());

        worker.processJob(job)
        .then(function() {
            return that.poll();
        });

        // There are maybe other jobs pending,
        // and we still have concurrent workers available
        if (that.countAvailableWorkers() > 0) return that.poll();
        else return that.delayPoll();
    })
    .fail(function(err) {
        that.emit('error:polling', err);
        return that.delayPoll();
    });
};

// Get a worker for a job
// Handles requeueing or canceling a job if necessary
Tasqueue.prototype.getWorkerForJob = function(job) {
    var that = this;

    var worker = this.getWorkerByType(job.getType());

    // No registered handler for this type
    // Mark job as failed
    if (!worker) {
        that.emit('job:nohandler', job.id, job.getType());

        return job.cancel(true)
        .then(function() {
            return null;
        });
    }

    // No available worker for this job
    // Requeue job
    if (!worker.isAvailable()) {
        that.emit('job:requeue', job.id, job.getType());

        return Q(that.client.enqueue(job.id))
        .then(function() {
            return null;
        });
    }

    return worker;
};

// Base list function
// Takes state = 'active' (default), 'queued', 'failed', 'completed'
Tasqueue.prototype.list = function(state, opts) {
    var that = this;

    state = state || 'active';
    opts = _.defaults(opts || {}, {
        start: 0,
        limit: 100
    });

    var params = (state === 'active' || state === 'queued')? ['QUEUE', config.QUEUE, 'STATE', state] :
        (state === 'failed')? ['QUEUE', config.FAILED] :
        ['QUEUE', config.COMPLETED];

    var query = [opts.start, 'COUNT', opts.limit, 'REPLY', 'all'].concat(params);
    return Q(that.client.jscan(query))
    .then(function(res) {
        return res[1].map(mapScan);
    });
};

// Base count function
// Takes state = 'active' (default), 'queued', 'failed', 'completed'
// Takes opts for 'active' and 'queued' state:
//      limit: limit to count
//      all: flag (default is false), count over the whole QUEUE length
Tasqueue.prototype.count = function(state, opts) {
    state = state || 'active';

    if (state === 'failed') return this.countFailed();
    if (state === 'completed') return this.countCompleted();

    opts = _.defaults(opts || {}, {
        limit: 100,
        all: false
    });

    var query = ['COUNT', opts.limit, 'QUEUE', config.QUEUE, 'STATE', state, 'REPLY', 'all'];
    if (opts.all) query.push('BUSYLOOP');

    return Q(this.client.jscan(query))
    .then(function(res) {
        return res[1].length;
    });
};

// Count completed jobs
Tasqueue.prototype.countCompleted = function() {
    return Q(this.client.qlen(config.COMPLETED));
};

// List of completed jobs
Tasqueue.prototype.listCompleted = function(opts) {
    return this.list('completed', opts);
};

// Count failed jobs
Tasqueue.prototype.countFailed = function() {
    return Q(this.client.qlen(config.FAILED));
};

// List of failed jobs
Tasqueue.prototype.listFailed = function(opts) {
    return this.list('failed', opts);
};

// Count queued jobs
Tasqueue.prototype.countQueued = function(opts) {
    return this.count('queued', opts);
};

// List of queued jobs
Tasqueue.prototype.listQueued = function(opts) {
    return this.list('queued', opts);
};

// Count active jobs
Tasqueue.prototype.countActive = function(opts) {
    return this.count('active', opts);
};

// List of active jobs
Tasqueue.prototype.listActive = function(opts) {
    return this.list('active', opts);
};

// Push a new job
Tasqueue.prototype.pushJob = function(type, body) {
    var that = this;

    body = body || {};
    // Hack to store the type of a job in the job's body
    body._jobType = type;

    return Q(that.client.addjob(config.QUEUE, JSON.stringify(body), 0, 'TTL', that.opts.queuedTTL))
    .then(function(jobId) {
        that.emit('job:push', jobId, type);
        return jobId;
    });
};

// Get a Job by its id
Tasqueue.prototype.getJob = function(id) {
    var that = this;

    return Q(that.client.show(id))
    .then(function(_job) {
        if (!_job) return Q.reject('job doesn\'t exist');

        var job = new Job(that);
        job = _.assign(job, _job);

        // Parse job body as pure JS
        job.body = JSON.parse(job.body);
        return job;
    });
};

// Map results of a disque JSCAN to its SHOW equivalent
function mapScan(res) {
    var job = {};
    for (var i = 0; i < res.length; i+=2) {
        job[res[i]] = res[i+1];
    }

    job.body = JSON.parse(job.body);
    return job;
}

module.exports = Tasqueue;
