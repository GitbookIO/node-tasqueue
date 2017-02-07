var EventEmitter = require('events');
var util         = require('util');

var Q      = require('q-plus');
var _      = require('lodash');
var disque = require('thunk-disque');

var Job    = require('./job');
var Queue  = require('./queue');
var Worker = require('./worker');
var config = require('./config');

/**
 *  CONSTRUCTOR
 */

function Tasqueue(opts) {
    var that = this;

    // Initialize as an Event Emitter
    EventEmitter.call(that);

    // Default options
    that.opts = _.defaults(opts || {}, {
        authPass:      null,            // AUTH password for disque-server
        host:          'localhost',     // disque-server host
        port:          7711,            // disque-server port
        pollDelay:     1000 * 15,       // Polling delay in ms when no workers are available
        jobTimeout:    1000 * 60 * 60,  // Timeout in ms before a job is considered as failed
        failedTTL:     60 * 60 * 24,    // Failed jobs TTL in sec
        completedTTL:  60 * 60 * 24,    // Completed jobs TTL in sec
        queuedTTL:     60 * 60 * 24,    // Queued jobs TTL in sec
        activeTTL:     60 * 60 * 1,     // Active job TTL in sec
        maxAttempts:   60,              // Max reconnection attempts
        retryMaxDelay: 1000 * 60        // Prevent exponential reconnection delay
    });

    that.pollTimeout = null;
    that.workers     = {};
    that.queues      = {};
}

// Event emitter inheritance
util.inherits(Tasqueue, EventEmitter);


/**
 *  START / STOP / WORK
 */

// Initialize connection to disque
Tasqueue.prototype.init = function() {
    var that = this;
    var d    = Q.defer();

    that.client = disque.createClient(that.opts.port, that.opts.host, {
        authPass:      that.opts.authPass,
        usePromise:    true,
        maxAttempts:   that.opts.maxAttempts,
        retryMaxDelay: that.opts.retryMaxDelay
    })
    .on('connect', function() {
        that.running = true;

        // Initialize Queues
        that.queues[config.QUEUED] = new Queue(that, {
            name: config.QUEUED,
            TTL:  that.opts.queuedTTL
        });
        that.queues[config.ACTIVE] = new Queue(that, {
            name: config.ACTIVE,
            TTL:  that.opts.activeTTL
        });
        that.queues[config.FAILED] = new Queue(that, {
            name: config.FAILED,
            TTL:  that.opts.failedTTL
        });
        that.queues[config.COMPLETED] = new Queue(that, {
            name: config.COMPLETED,
            TTL:  that.opts.completedTTL
        });

        // Tasqueue is ready
        that.emit('client:connected', {
            host: that.opts.host,
            port: that.opts.port
        });

        d.resolve();
    })
    .on('error', function(err) {
        d.reject(err);
    })
    // Handle re-creating a client on Disque server failure
    .on('close', function() {
        that.emit('client:closed');
        that.init();
    });

    return d.promise;
};

// Shutdown disque client and execute cb()
Tasqueue.prototype.shutdown = function(timeout, cb) {
    var that     = this;
    that.running = false;

    // Get all jobs being processed
    var processing = that.getProcessingJobs();

    // Close connection and call end callback
    function done() {
        return Q(that.client.clientEnd())
        .fin(function() {
            cb();
        });
    }

    // Nothing running, stop already
    if (_.size(processing) == 0) {
        return done();
    }

    // Maximum wait
    var _timeout = setTimeout(function() {
        done(new Error('Timeout'));
    }, timeout);

    return Q.all(_.values(processing))
    .fin(function() {
        clearTimeout(_timeout);
        done();
    });
};

// Start polling and workers activity
Tasqueue.prototype.poll = function() {
    var that = this;
    if (!that.running) {
        return;
    }

    // Check number of available workers
    var nWorkers         = 0;
    var availableWorkers = that.countAvailableWorkers();
    var pollImmediately  = false;

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

    // Emit event
    that.emit('client:polling', {
        types:            types.length,
        availableWorkers: availableWorkers,
        totalWorkers:     nWorkers
    });

    // GETJOB from QUEUED
    that.queues[config.QUEUED].getJob({ noHang: true })
    .then(function(queuedId) {
        // No job available, reset TIMEOUT
        if (!queuedId) {
            return;
        }

        // References to the job
        var queuedJob = null;
        var activeJob = null;

        // Get corresponding Job
        return that.getJob(queuedId)
        .then(function(_job) {
            // Get a worker for this job
            queuedJob = _job;
            return that.getWorkerForJob(queuedJob);
        })
        .then(function(worker) {
            // No worker
            if (!worker) {
                return;
            }

            // Push job to ACTIVE queue
            return that.queues[config.ACTIVE].addJob(queuedJob)
            .then(function(activeId) {
                // Acknowledge job in QUEUED
                return queuedJob.acknowledge()
                .then(function() {
                    // Return job's id in ACTIVE
                    return activeId;
                });
            })
            .then(function(activeId) {
                // Get real job instance
                return that.getJob(activeId);
            })
            .then(function(_job) {
                activeJob = _job;

                // Emit event
                that.emit('job:started', {
                    id:   activeJob.id,
                    type: activeJob.getType()
                });

                // Mark worker as taken and process job
                worker.processJob(activeJob);

                // There are maybe other jobs pending,
                // and we still have concurrent workers available
                pollImmediately = that.countAvailableWorkers() > 0;
            });
        });
    })
    .fail(function(err) {
        that.emit('error:polling', err);
        throw err;
    })

    // Continue polling (after a small wait or not)
    .fin(function() {
        if (pollImmediately) {
            return that.poll();
        }
        else {
            return that.delayPoll();
        }
    });
};

// Ping disque to check connection
Tasqueue.prototype.ping = function() {
    return Q(this.client.info());
};


/**
 *  JOBS API
 */

// Register a new job type worker handler
Tasqueue.prototype.registerHandler = function(handler) {
    var that = this;

    if (!!that.workers[handler.type]) {
        that.emit('error:existing-handler', new Error('Handler already registered for type '+handler.type), {
            type: handler.type
        });
        return;
    }

    that.workers[handler.type] = new Worker(that, handler);
    that.emit('handler:registered', {
        type: handler.type
    });
};

// Get a list of handled types
Tasqueue.prototype.listHandlers = function() {
    return _.chain(this.workers)
    .map('type')
    .value();
};

// Push a new job
Tasqueue.prototype.pushJob = function(type, body) {
    var that = this;

    return that.queues[config.QUEUED].addRawJob(type, body)
    .then(function(jobId) {
        that.emit('job:pushed', {
            id:   jobId,
            type: type
        });

        return jobId;
    });
};

// Get a Job by its id
Tasqueue.prototype.getJob = function(id) {
    var that = this;

    return Q(that.client.show(id))
    .then(function(_job) {
        if (!_job) {
            return Q.reject('job doesn\'t exist');
        }

        return new Job(that, _job);
    });
};

// Count jobs by state
Tasqueue.prototype.count = function(state) {
    if (!state) {
        return Q.reject(new Error('State must be provided as first argument for this function'));
    }

    var queue = this.queues[state];
    if (!queue) {
        return Q.reject(new Error('Invalid state for this request'));
    }

    return queue.length();
};

// Count completed jobs
Tasqueue.prototype.countCompleted = function() {
    return this.queues[config.COMPLETED].length();
};

// Count failed jobs
Tasqueue.prototype.countFailed = function() {
    return this.queues[config.FAILED].length();
};

// Count queued jobs
Tasqueue.prototype.countQueued = function(opts) {
    return this.queues[config.QUEUED].length();
};

// Count active jobs
Tasqueue.prototype.countActive = function(opts) {
    return this.queues[config.ACTIVE].length();
};

// List jobs by state
Tasqueue.prototype.list = function(state, opts) {
    if (!state) {
        return Q.reject(new Error('State must be provided as first argument for this function'));
    }

    var queue = this.queues[state];
    if (!queue) {
        return Q.reject(new Error('Invalid state for this request'));
    }

    return queue.list(opts);
};

// List of completed jobs
Tasqueue.prototype.listCompleted = function(opts) {
    return this.queues[config.COMPLETED].list(opts);
};

// List of failed jobs
Tasqueue.prototype.listFailed = function(opts) {
    return this.queues[config.FAILED].list(opts);
};

// List of queued jobs
Tasqueue.prototype.listQueued = function(opts) {
    return this.queues[config.QUEUED].list(opts);
};

// List of active jobs
Tasqueue.prototype.listActive = function(opts) {
    return this.queues[config.ACTIVE].list(opts);
};


/**
 *  INTERNAL UTILITY FUNCTIONS
 */

// Delay polling
Tasqueue.prototype.delayPoll = function() {
    var that = this;
    // Delay already called
    if (that.pollTimeout) {
        return;
    }

    that.emit('client:delaying', {
        delay: that.opts.pollDelay
    });
    // Reset polling delay
    that.pollTimeout = setTimeout(function() {
        that.pollTimeout = null;
        that.poll();
    }, that.opts.pollDelay);
};

// Get a worker for a job
// Handles requeueing or canceling a job if necessary
Tasqueue.prototype.getWorkerForJob = function(job) {
    var that = this;

    var worker = this.getWorkerByType(job.getType());

    // No registered handler for this type
    // Mark job as failed
    if (!worker) {
        that.emit('error:no-handler', new Error('No handler registered for type '+job.getType()), {
            id:   job.id,
            type: job.getType()
        });

        return job.cancel()
        .then(function() {
            return null;
        });
    }

    // No available worker for this job
    // Requeue job
    if (!worker.isAvailable()) {
        return Q(that.client.enqueue(job.id))
        .then(function() {
            return null;
        });
    }

    return Q(worker);
};

// Return the list of all jobs being processed by workers
Tasqueue.prototype.getProcessingJobs = function() {
    return _.chain(this.workers)
    .map('processing')
    .value();
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

module.exports = Tasqueue;
