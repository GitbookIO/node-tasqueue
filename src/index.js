const EventEmitter = require('events');
const util = require('util');

const Promise = require('q-plus');
const _ = require('lodash');
const disque = require('thunk-disque');

const Job = require('./job');
const Queue = require('./queue');
const Worker = require('./worker');
const config = require('./config');

/**
 *  CONSTRUCTOR
 */

function Tasqueue(opts) {
    // Initialize as an Event Emitter
    EventEmitter.call(this);

    // Default options
    this.opts = Object.assign({
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
    }, opts);

    this.pollTimeout = null;
    this.workers = {};
    this.queues = {};

    // Tasqueue status
    this.running = false;
    // Flag to prevent auto-reconnection
    this.endedOnPurpose = false;
}

// Event emitter inheritance
util.inherits(Tasqueue, EventEmitter);


/**
 *  START / STOP / WORK
 */

// Initialize connection to disque
Tasqueue.prototype.init = function() {
    const d = Promise.defer();

    this.client = disque.createClient(this.opts.port, this.opts.host, {
        authPass:      this.opts.authPass,
        usePromise:    true,
        maxAttempts:   this.opts.maxAttempts,
        retryMaxDelay: this.opts.retryMaxDelay
    })
    .on('connect', () => {
        this.running = true;

        // Initialize Queues
        this.queues[config.QUEUED] = new Queue(this, {
            name: config.QUEUED,
            TTL:  this.opts.queuedTTL
        });
        this.queues[config.ACTIVE] = new Queue(this, {
            name: config.ACTIVE,
            TTL:  this.opts.activeTTL
        });
        this.queues[config.FAILED] = new Queue(this, {
            name: config.FAILED,
            TTL:  this.opts.failedTTL
        });
        this.queues[config.COMPLETED] = new Queue(this, {
            name: config.COMPLETED,
            TTL:  this.opts.completedTTL
        });

        // Tasqueue is ready
        this.emit('client:connected', {
            host: this.opts.host,
            port: this.opts.port
        });

        d.resolve();
    })
    .on('error', (err) => {
        d.reject(err);
    })
    // Handle re-creating a client on Disque server failure
    .on('close', () => {
        this.emit('client:closed');

        // Check if should reconnect
        if (!this.endedOnPurpose) {
            this.init();
        }
    });

    return d.promise;
};

// Shutdown disque client and execute cb()
Tasqueue.prototype.shutdown = function(timeout, cb) {
    this.running = false;
    this.endedOnPurpose = true;

    // Get all jobs being processed
    const processing = this.getProcessingJobs();

    // Close connection and call end callback
    const done = () => {
        return Promise(this.client.clientEnd())
        .fin(() => {
            cb();
        });
    };

    // Nothing running, stop already
    if (_.size(processing) == 0) {
        return done();
    }

    // Maximum wait
    const _timeout = setTimeout(() => {
        done(new Error('Timeout'));
    }, timeout);

    return Promise.all(_.values(processing))
    .fin(() => {
        clearTimeout(_timeout);
        done();
    });
};

// Start polling and workers activity
Tasqueue.prototype.poll = function() {
    if (!this.running) {
        return;
    }

    // Check number of available workers
    let nWorkers = 0;
    const availableWorkers = this.countAvailableWorkers();
    let pollImmediately = false;

    if (availableWorkers <= 0) {
        this.emit('client:no-workers');
        return this.delayPoll();
    }

    // List jobs that have available workers
    const types = _.chain(this.workers)
        .filter((worker) => {
            nWorkers = nWorkers + worker.concurrency;
            return worker.isAvailable();
        })
        .map('type')
        .value();

    // Emit event
    this.emit('client:polling', {
        types:            types.length,
        availableWorkers,
        totalWorkers:     nWorkers
    });

    // GETJOB from QUEUED
    this.queues[config.QUEUED].getJob({ noHang: true })
    .then((queuedId) => {
        // No job available, reset TIMEOUT
        if (!queuedId) {
            return;
        }

        // References to the job
        let queuedJob = null;
        let activeJob = null;

        // Get corresponding Job
        return this.getJob(queuedId)
        .then((_job) => {
            // Get a worker for this job
            queuedJob = _job;
            return this.getWorkerForJob(queuedJob);
        })
        .then((worker) => {
            // No worker
            if (!worker) {
                return;
            }

            // Push job to ACTIVE queue
            return this.queues[config.ACTIVE].addJob(queuedJob)
            .then((activeId) => {
                // Acknowledge job in QUEUED
                return queuedJob.acknowledge()
                .then(() => {
                    // Return job's id in ACTIVE
                    return activeId;
                });
            })
            .then((activeId) => {
                // Get real job instance
                return this.getJob(activeId);
            })
            .then((_job) => {
                activeJob = _job;

                // Emit event
                this.emit('job:started', {
                    id:   activeJob.id,
                    type: activeJob.getType()
                });

                // Mark worker as taken and process job
                worker.processJob(activeJob);

                // There are maybe other jobs pending,
                // and we still have concurrent workers available
                pollImmediately = this.countAvailableWorkers() > 0;
            });
        });
    })
    .fail((err) => {
        this.emit('error:polling', err);
        throw err;
    })

    // Continue polling (after a small wait or not)
    .fin(() => {
        if (pollImmediately) {
            return this.poll();
        }
        else {
            return this.delayPoll();
        }
    });
};

// Ping disque to check connection
Tasqueue.prototype.ping = function() {
    return Promise(this.client.info());
};


/**
 *  JOBS API
 */

// Register a new job type worker handler
Tasqueue.prototype.registerHandler = function(handler) {
    if (!!this.workers[handler.type]) {
        this.emit('error:existing-handler', new Error('Handler already registered for type ' + handler.type), {
            type: handler.type
        });
        return;
    }

    this.workers[handler.type] = new Worker(this, handler);
    this.emit('handler:registered', {
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
    return this.queues[config.QUEUED].addRawJob(type, body)
    .then((jobId) => {
        this.emit('job:pushed', {
            id:   jobId,
            type
        });

        return jobId;
    });
};

// Get a Job by its id
Tasqueue.prototype.getJob = function(id) {
    return Promise(this.client.show(id))
    .then((_job) => {
        if (!_job) {
            return Promise.reject('job doesn\'t exist');
        }

        return new Job(this, _job);
    });
};

// Count jobs by state
Tasqueue.prototype.count = function(state) {
    if (!state) {
        return Promise.reject(new Error('State must be provided as first argument for this function'));
    }

    const queue = this.queues[state];
    if (!queue) {
        return Promise.reject(new Error('Invalid state for this request'));
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
        return Promise.reject(new Error('State must be provided as first argument for this function'));
    }

    const queue = this.queues[state];
    if (!queue) {
        return Promise.reject(new Error('Invalid state for this request'));
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
    // Delay already called
    if (this.pollTimeout) {
        return;
    }

    this.emit('client:delaying', {
        delay: this.opts.pollDelay
    });
    // Reset polling delay
    this.pollTimeout = setTimeout(() => {
        this.pollTimeout = null;
        this.poll();
    }, this.opts.pollDelay);
};

// Get a worker for a job
// Handles requeueing or canceling a job if necessary
Tasqueue.prototype.getWorkerForJob = function(job) {
    const worker = this.getWorkerByType(job.getType());

    // No registered handler for this type
    // Mark job as failed
    if (!worker) {
        this.emit('error:no-handler', new Error('No handler registered for type ' + job.getType()), {
            id:   job.id,
            type: job.getType()
        });

        return job.cancel()
        .then(() => {
            return null;
        });
    }

    // No available worker for this job
    // Requeue job
    if (!worker.isAvailable()) {
        return Promise(this.client.enqueue(job.id))
        .then(() => {
            return null;
        });
    }

    return Promise(worker);
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
    return _.reduce(this.workers, (count, worker) => {
        return count + worker.countAvailable();
    }, 0);
};

module.exports = Tasqueue;
