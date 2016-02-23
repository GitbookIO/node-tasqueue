var EventEmitter = require('events');
var domain = require('domain');
var util = require('util');

var Q = require('q-plus');
var _ = require('lodash');
var disque = require('thunk-disque');

var QUEUE = 'queue';           // Queue name for queued/active jobs
var FAILED = 'failed';         // Queue name and TTL for failed jobs
var COMPLETED = 'completed';   // Queue name and TTL for completed jobs

function Tasqueue(opts) {
    var that = this;

    // Initialize as an Event Emitter
    EventEmitter.call(that);

    // Default options
    opts = _.defaults(opts || {}, {
        port: 7711,                 // disque-server port
        pollDelay: 15*1000,         // Polling delay in ms when no workers are available
        jobTimeout: 60*60*1000,     // Timeout in ms before a job is considered as failed
        failedTTL: 3*24*60*60,      // Failed jobs TTL in sec
        completedTTL: 3*24*60*60    // Completed jobs TTL in sec
    });

    // Other properties
    that.disquePort = opts.port;
    that.pollDelay = opts.pollDelay;
    that.jobTimeout = opts.jobTimeout;
    that.failedTTL = opts.failedTTL;
    that.completedTTL = opts.completedTTL;

    that.pollTimeout = null;
    that.workers = {};
    that.processing = {};
}

// Event emitter inheritance
util.inherits(Tasqueue, EventEmitter);

// Init connection to disque
Tasqueue.prototype.init = function() {
    var that = this;
    var d = Q.defer();

    that.client = disque.createClient(that.disquePort, { usePromise: true })
    .on('connect', function() {
        that.running = true;
        that.poll();
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

    // Nothing running, stop already
    if (_.size(that.processing) == 0) {
        that.client.clientEnd();
        return cb();
    }

    // Maximum wait
    var timeout = setTimeout(cb, n);
    return Q.all(_.values(that.processing))
    .fin(function() {
        clearTimeout(timeout);
        that.client.clientEnd();
        cb();
    });
};

// Register a new job handler
Tasqueue.prototype.registerHandler = function(handler) {
    var that = this;

    if (!!that.workers[handler.type]) {
        that.emit('error:existing-handler', handler.type);
        return;
    }

    that.workers[handler.type] = {
        type: handler.type,
        current: 0,
        concurrency: handler.concurrency || 1,
        maxAttemps: handler.maxAttemps || 1,
        exec: function(job) {
            var d = Q.defer();
            var dmn = domain.create();

            var cleanup = function() {
                // Cleanup domain
                dmn.removeAllListeners();
            };

            var next = function(err) {
                cleanup();

                if (err) d.reject(err);
                else d.resolve();
            };

            dmn.once('error', function(err) {
                that.emit('error:domain', err);
                cleanup();
                next(err);
            });
            dmn.run(function() {
                Q()
                .then(function() {
                    return handler.exec(job.body, job);
                })
                .nodeify(next);
            });

            return d.promise;
        }
    };

    that.emit('handler:register', handler.type);
};

// Return a count of available workers
Tasqueue.prototype.countAvailableWorkers = function() {
    var that = this;
    return _.reduce(that.workers, function(count, worker) {
        return count + (worker.concurrency - worker.current);
    }, 0);
};

// Return a count of available workers for a type
Tasqueue.prototype.hasAvailableWorkerFor = function(type) {
    var that = this;
    return that.workers[type].concurrency > that.workers[type].current;
};

// Get a list of handled types
Tasqueue.prototype.listTypes = function() {
    return _.chain(this.workers)
    .map('type')
    .value();
};

// Delay polling
Tasqueue.prototype.delayPoll = function() {
    var that = this;

    if (that.pollTimeout) {
        clearTimeout(that.pollTimeout);
        that.pollTimeout = null;
    }

    that.emit('client:delaying', that.pollDelay);
    that.pollTimeout = setTimeout(function() {
        that.pollTimeout = null;
        that.poll();
    }, that.pollDelay);
};

// Poll a job and execute it
Tasqueue.prototype.poll = function() {
    var that = this;
    if (!that.running) return;

    // Check number of available workers
    var nWorkers = 0;
    var availableWorkers = that.countAvailableWorkers();

    if (availableWorkers <= 0) {
        that.emit('client:noworkers');
        return that.delayPoll();
    }

    // List jobs that have available workers
    var types = _.chain(that.workers)
        .filter(function(worker) {
            nWorkers = nWorkers + worker.concurrency;
            return (worker.current < worker.concurrency);
        })
        .map('type')
        .value();

    that.emit('client:polling', types.length, availableWorkers, nWorkers);
    that.client.getjob(['NOHANG', 'WITHCOUNTERS', 'FROM', QUEUE])
    .then(function(res) {
        // No job available, reset TIMEOUT
        if (!res) return that.delayPoll();

        // Convert returned array to an element
        res = res.length > 0? res[0] : null;
        // Reformat job info
        var job = {
            id: res[1],
            body: JSON.parse(res[2]),
            nacks: res[4],
            additionalDeliveries: res[6]
        };

        job.type = job.body._jobType;
        delete job.body._jobType;

        // No registered handler for this type
        if (!that.workers[job.type]) {
            that.emit('job:nohandler', job.id, job.type);
            return that.poll();
        }

        // No available worker for this job
        if (!that.hasAvailableWorkerFor(job.type)) {
            that.emit('job:requeue', job.id, job.type);
            return that.client.enqueue(job.id);
        }

        // Mark worker as taken and process
        that.workers[job.type].current += 1;
        that.processJob(job)
        .fin(function() {
            that.workers[job.type].current -= 1;
            return that.poll();
        });

        // There are maybe other jobs pending,
        // and we still have concurrent workers available
        if (that.countAvailableWorkers() > 0) {
            return that.poll();
        }
        else {
            return that.delayPoll();
        }
    })
    .catch(function(err) {
        that.emit('error:polling', err);
        return that.delayPoll();
    });
};

// Process a job
Tasqueue.prototype.processJob = function(job) {
    var that = this;

    return Q()
    .then(function() {
        that.emit('job:start', job.id, job.type);

        that.processing[job.id] = that.wrapJobprocess(job, that.workers[job.type].exec(job));
        return that.processing[job.id];
    })
    .fail(function(err) {
        that.emit('error:job', job.id, job.type, err);
    })
    .fin(function() {
        delete that.processing[job.id];
    });
};

// Wrap job processing with exec function
Tasqueue.prototype.wrapJobprocess = function(job, exec) {
    var that = this;

    return Q(exec)
    .timeout(that.jobTimeout, 'took longer than '+Math.ceil(that.jobTimeout/1000)+' seconds to process')
    .then(function(result) {
        return that.acknowledgeJob(job);
    }, function(err) {
        return that.handleFailedJob(job, err);
    });
};

// Base function to push a job to the COMPLETED queue
Tasqueue.prototype.setAsCompleted = function(job) {
    return this.client.addjob(COMPLETED, JSON.stringify(job.body), 0, 'TTL', this.completedTTL);
};

// Base function to push a job to the FAILED queue
Tasqueue.prototype.setAsFailed = function(job) {
    return this.client.addjob(FAILED, JSON.stringify(job.body), 0, 'TTL', this.failedTTL);
};

// Acknowledge job to disque then push to completed queue
Tasqueue.prototype.acknowledgeJob = function(job) {
    var that = this;

    that.emit('job:success', job.id, job.type);
    return that.client.fastack(job.id)
    .then(function() {
        return that.setAsCompleted(job);
    });
};

// Handle failed job: nack and requeue or push to failed
Tasqueue.prototype.handleFailedJob = function(job, err) {
    var that = this;

    // Get maxAttempts for this type of job
    var maxAttemps = that.workers[job.type].maxAttemps;

    // Too many nacks, push to failed queue
    if ((job.nacks+1) >= maxAttemps) {
        that.emit('job:fail', job.id, job.type, err);

        // Add error info to job
        job.body.error = {
            message: err.message,
            stack: err.stack
        };
        return that.setAsFailed(job);
    }
    // Requeue job
    else {
        that.emit('job:requeue', job.id, job.type, job.nacks+1);
        return that.client.nack(job.id);
    }
};

// Base list function
// Takes state = 'active' (default), 'queued', 'failed', 'completed'
Tasqueue.prototype._list = function(state, opts) {
    var that = this;

    state = state || 'active';
    opts = _.defaults(opts || {}, {
        start: 0,
        limit: 100
    });

    var params = (state === 'active' || state === 'queued')? ['QUEUE', QUEUE, 'STATE', state] :
        (state === 'failed')? ['QUEUE', FAILED] :
        ['QUEUE', COMPLETED];

    var query = [opts.start, 'COUNT', opts.limit, 'REPLY', 'all'].concat(params);
    return that.client.jscan(query)
    .then(function(res) {
        return Q(res[1].map(mapScan));
    })
    .catch(function(err) {
        return Q.reject(err);
    });
};

// Base count function
// Takes state = 'active' (default), 'queued', 'failed', 'completed'
// Takes opts for 'active' and 'queued' state:
//      limit: limit to count
//      all: flag (default is false), count over the whole QUEUE length
Tasqueue.prototype._count = function(state, opts) {
    state = state || 'active';

    if (state === 'failed') return this.countFailed();
    if (state === 'completed') return this.countCompleted();

    opts = _.defaults(opts || {}, {
        limit: 100,
        all: false
    });

    var query = ['COUNT', opts.limit, 'QUEUE', QUEUE, 'STATE', state, 'REPLY', 'all'];
    if (opts.all) query.push('BUSYLOOP');

    return this.client.jscan(query)
    .then(function(res) {
        return Q(res[1].length);
    })
    .catch(function(err) {
        return Q.reject(err);
    });
};

// Count completed jobs
Tasqueue.prototype.countCompleted = function() {
    return this.client.qlen(COMPLETED)
    .then(function(length) {
        return Q(length);
    })
    .catch(function(err) {
        return Q.reject(err);
    });
};

// List of completed jobs
Tasqueue.prototype.listCompleted = function(opts) {
    return this._list('completed', opts);
};

// Count failed jobs
Tasqueue.prototype.countFailed = function() {
    return this.client.qlen(FAILED)
    .then(function(length) {
        return Q(length);
    })
    .catch(function(err) {
        return Q.reject(err);
    });
};

// List of failed jobs
Tasqueue.prototype.listFailed = function(opts) {
    return this._list('failed', opts);
};

// Count queued jobs
Tasqueue.prototype.countQueued = function(opts) {
    return this._count('queued', opts);
};

// List of queued jobs
Tasqueue.prototype.listQueued = function(opts) {
    return this._list('queued', opts);
};

// Count active jobs
Tasqueue.prototype.countActive = function(opts) {
    return this._count('active', opts);
};

// List of active jobs
Tasqueue.prototype.listActive = function(opts) {
    return this._list('active', opts);
};

// Return a job's details
Tasqueue.prototype.details = function(jobId) {
    return this.client.show(jobId)
    .then(function(job) {
        // Parse job body as pure JS
        job.body = JSON.parse(job.body);
        // Reformat job type
        job.type = job.body._jobType;
        delete job.body._jobType;
        // Set job.state based on queue and state
        job.state = job.queue === QUEUE? job.state : job.queue;

        return Q(job);
    })
    .catch(function(err) {
        return Q.reject(err);
    });
};

// Push a new job
Tasqueue.prototype.push = function(type, body) {
    var that = this;

    body = body || {};
    body._jobType = type;

    return this.client.addjob(QUEUE, JSON.stringify(body), 0)
    .then(function(jobId) {
        that.emit('job:push', jobId, type);
        return Q(jobId);
    })
    .catch(function(err) {
        that.emit('error:push', type, err);
        return Q.reject(err);
    });
};

// Utterly delete a job
Tasqueue.prototype.delete = function(id) {
    var that = this;
    if (_.isArray(id)) return Q.all(_.map(id, that.removeJob));

    return that.client.deljob(id)
    .then(function() {
        that.emit('job:delete', id);
        return Q();
    })
    .catch(function(err) {
        return Q.reject(err);
    });
};

// Cancel a job -> move to failed queue
// Error if job is not 'queued' in QUEUE
Tasqueue.prototype.cancel = function(id) {
    var that = this;

    if (_.isArray(id)) return Q.all(_.map(id, that.cancelJob));

    // Päuse QUEUE queue to prevent GETJOB operations on this job
    return that.client.pause(QUEUE, 'out')
    .then(function() {
        return that.details(id);
    })
    .then(function(job) {
        if (job.queue !== QUEUE || job.state !== 'queued') {
            throw new Error('Only queued jobs may be cancelled');
        }

        that.emit('job:cancel', id);

        var err = new Error('Canceled');
        job.body.error = {
            message: err.message,
            stack: err.stack
        };
        return that.setAsFailed(job)
        .then(function() {
            return that.deleteJob(id);
        })
        .then(function() {
            return that.client.pause(QUEUE, 'none');
        });
    })
    .catch(function(err) {
        that.emit('error:cancel', id);
        return that.client.pause(QUEUE, 'none');
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
