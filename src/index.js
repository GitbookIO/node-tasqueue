const EventEmitter = require('events');

const Promise = require('q-plus');
const _ = require('lodash');
const disque = require('thunk-disque');

const Job = require('./job');
const Queue = require('./queue');
const Worker = require('./worker');
const config = require('./config');

class Tasqueue extends EventEmitter {

    /**
     *  CONSTRUCTOR
     *  @param {Object} opts
     */
    constructor(opts = {}) {
        // Initialize as an Event Emitter
        super();

        // Default options
        this.opts = Object.assign({
            authPass:       null,            // AUTH password for disque-server
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

    /**
     *  START / STOP / WORK
     */

    /**
     *  Initialize connection to disque
     *  @return {Promise}
     */
    init() {
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
    }

    /**
     *  Shutdown disque client and execute cb()
     *  @param {Number} timeout
     *  @param {Function} cb
     *  @return {Promise}
     */
    shutdown(timeout, cb) {
        this.running = false;
        this.endedOnPurpose = true;

        // Get all jobs being processed
        const processing = this.getProcessingJobs();

        // Close connection and call end callback
        const done = () => {
            return Promise(this.client.clientEnd())
            .fin(cb);
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
    }

    // Start polling and workers activity
    poll() {
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
            types: types.length,
            availableWorkers,
            totalWorkers: nWorkers
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
                    // Return job's id in ACTIVE
                    .then(() => activeId);
                })
                // Get real job instance
                .then(activeId => this.getJob(activeId))
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
    }

    // Ping disque to check connection
    ping() {
        return Promise(this.client.info());
    }

    /**
     *  JOBS API
     */

    /**
     * Register a new job type worker handler
     * @param {Object} handler
     */
    registerHandler(handler) {
        if (Boolean(this.workers[handler.type])) {
            this.emit('error:existing-handler', new Error(`Handler already registered for type ${handler.type}`), {
                type: handler.type
            });
            return;
        }

        this.workers[handler.type] = new Worker(this, handler);
        this.emit('handler:registered', {
            type: handler.type
        });
    }

    /**
     * Get a list of handled types
     * @return {Array<String>}
     */
    listHandlers() {
        return _.chain(this.workers)
        .map('type')
        .value();
    }

    /**
     * Push a new job
     * @param {String} type
     * @param {Object} body
     * @return {Promise<String>}
     */
    pushJob(type, body) {
        return this.queues[config.QUEUED].addRawJob(type, body)
        .then((jobId) => {
            this.emit('job:pushed', {
                id: jobId,
                type
            });

            return jobId;
        });
    }

    /**
     * Get a Job by its id
     * @param {String} id
     * @return {Promise<Job>}
     */
    getJob(id) {
        return Promise(this.client.show(id))
        .then((_job) => {
            if (!_job) {
                return Promise.reject('job doesn\'t exist');
            }

            return new Job(this, _job);
        });
    }

    /**
     * Count jobs by state
     * @param  {String} state
     * @return {Promise<Number>}
     */
    count(state) {
        if (!state) {
            return Promise.reject(new Error('State must be provided as first argument for this function'));
        }

        const queue = this.queues[state];
        if (!queue) {
            return Promise.reject(new Error('Invalid state for this request'));
        }

        return queue.length();
    }

    /**
     * Count completed jobs
     * @return {Promise<Number>}
     */
    countCompleted() {
        return this.queues[config.COMPLETED].length();
    }

    /**
     * Count failed jobs
     * @return {Promise<Number>}
     */
    countFailed() {
        return this.queues[config.FAILED].length();
    }

    /**
     * Count queued jobs
     * @return {Promise<Number>}
     */
    countQueued(opts) {
        return this.queues[config.QUEUED].length();
    }

    /**
     * Count active jobs
     * @return {Promise<Number>}
     */
    countActive(opts) {
        return this.queues[config.ACTIVE].length();
    }

    /**
     * List jobs by state
     * @param {String} state
     * @param {Object} opts
     * @return {Object} { prev: {Number}, next: {Number}, list: {Array<Job>} }
     */
    list(state, opts = {}) {
        if (!state) {
            return Promise.reject(new Error('State must be provided as first argument for this function'));
        }

        const queue = this.queues[state];
        if (!queue) {
            return Promise.reject(new Error('Invalid state for this request'));
        }

        return queue.list(opts);
    }

    /**
     * List of completed jobs
     * @param {Object} opts
     * @return {Object} { prev: {Number}, next: {Number}, list: {Array<Job>} }
     */
    listCompleted(opts) {
        return this.queues[config.COMPLETED].list(opts);
    }

    /**
     * List of failed jobs
     * @param {Object} opts
     * @return {Object} { prev: {Number}, next: {Number}, list: {Array<Job>} }
     */
    listFailed(opts) {
        return this.queues[config.FAILED].list(opts);
    }

    /**
     * List of queued jobs
     * @param {Object} opts
     * @return {Object} { prev: {Number}, next: {Number}, list: {Array<Job>} }
     */
    listQueued(opts) {
        return this.queues[config.QUEUED].list(opts);
    }

    /**
     * List of active jobs
     * @param {Object} opts
     * @return {Object} { prev: {Number}, next: {Number}, list: {Array<Job>} }
     */
    listActive(opts) {
        return this.queues[config.ACTIVE].list(opts);
    }


    /**
    *  INTERNAL UTILITY FUNCTIONS
    */

    /**
     *  Delay polling
     */
    delayPoll() {
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
    }

    /**
     * Get a worker for a job
     * Handles requeueing or canceling a job if necessary
     * @param {Job} job
     * @return {Promise<Worker>}
     */
    getWorkerForJob(job) {
        const worker = this.getWorkerByType(job.getType());

        // No registered handler for this type
        // Mark job as failed
        if (!worker) {
            this.emit('error:no-handler', new Error(`No handler registered for type ${job.getType()}`), {
                id:   job.id,
                type: job.getType()
            });

            return job.cancel()
            .then(() => null);
        }

        // No available worker for this job
        // Requeue job
        if (!worker.isAvailable()) {
            return Promise(this.client.enqueue(job.id))
            .then(() => null);
        }

        return Promise(worker);
    }

    /**
     * Return the list of all jobs being processed by workers
     * @return {Array<Promise>}
     */
    getProcessingJobs() {
        return _.chain(this.workers)
        .map('processing')
        .value();
    }

    /**
     * Return a worker by its type
     * @param {String} type
     * @return {Worker}
     */
    getWorkerByType(type) {
        return this.workers[type];
    }

    /**
     * Return a count of available workers
     * @return {Number}
     */
    countAvailableWorkers() {
        return _.reduce(this.workers, (count, worker) => {
            return count + worker.countAvailable();
        }, 0);
    }
}

module.exports = Tasqueue;
