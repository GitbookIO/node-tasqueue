const _ = require('lodash');
const Promise = require('q');
const config = require('./config');

class Job {

    /**
     *  Create a new Job and extend with disque SHOW details
     *  @param {Tasqueue} tasqueue
     *  @param {Object} details
     */
    constructor(tasqueue, details) {
        this.tasqueue = tasqueue;

        // Extend job with disque details
        _.forIn(details, (v, k) => {
            this[k] = v;
        });

        // Parse job body as pure JS
        this.body = JSON.parse(this.body);
    }


    /**
    *  JOBS API
    */

    /**
     * Return pretty informations about this job
     * @return {Object}
     */
    details() {
        return {
            id:       this.id,
            type:     this.getType(),
            body:     this.getBody(),
            state:    this.getState(),
            created:  new Date(this.getCreationTime()),
            ended:    new Date(this.getEndTime()),
            duration: this.getDuration(),
            result:   this.getResult(),
            error:    this.getError(),
            attempt:  this.getAttempts()
        };
    }

    /**
     * Cancel a job -> move to FAILED
     * Error if job is not in QUEUED
     * @param {Boolean} force
     * @return {Promise}
     */
    cancel(force) {
        if (!this.isQueued()) {
            return Promise.reject(new Error('Only queued jobs may be cancelled'));
        }

        // Pause QUEUED to prevent GETJOB operations on this job
        return this.tasqueue.queues[config.QUEUED].pause('out')
        .then(() => {
            // Add job to FAILED
            return this.setAsFailed(new Error('Canceled'))
            .then(() => {
                // Emit event
                this.tasqueue.emit('job:canceled', {
                    id:   this.id,
                    type: this.getType()
                });
            });
        })
        .fail((err) => {
            // Emit event
            this.tasqueue.emit('error:job-cancel', err, {
                id:   this.id,
                type: this.getType()
            });
        })
        // Unpause QUEUED whatever the result is
        .fin(() => this.tasqueue.queues[config.QUEUED].pause('none'));
    }

    /**
     * Utterly delete a job
     * @param {Boolean} emit
     * @return {Promise}
     */
    delete(emit) {
        emit = emit || true;
        return Promise(this.tasqueue.client.deljob(this.id))
        .then(() => {
            if (emit) {
                this.tasqueue.emit('job:deleted', {
                    id:   this.id,
                    type: this.getType()
                });
            }
        });
    }


    /**
    *  COMPUTED PROPERTIES
    */

    /**
     * Return a job's type stored in body
     * @return {String}
     */
    getType() {
        return this.body._jobType;
    }

    /**
     * Return a job's actual body
     * @return {Object}
     */
    getBody() {
        return _.omit(this.body, [
            '_jobType',
            '_created',
            '_dequeued',
            '_ended',
            '_duration',
            '_result',
            '_error',
            '_nacks'
        ]);
    }

    /**
     * Return a job's nacks count
     * @return {Number}
     */
    getNacks() {
        return this.body._nacks || 0;
    }

    /**
     * Return a job's attempts count
     * @return {Number}
     */
    getAttempts() {
        return this.getNacks() + 1;
    }

    /**
     * Return true if the job has failed
     * @return {Boolean}
     */
    hasFailed() {
        return this.queue === config.FAILED;
    }

    /**
     * Return true if the job was successful
     * @return {Boolean}
     */
    hasSucceeded() {
        return this.queue === config.COMPLETED;
    }

    /**
     * Return true if the job is queued
     * @return {Boolean}
     */
    isQueued() {
        return this.queue === config.QUEUED;
    }

    /**
     * Return true if the job is active
     * @return {Boolean}
     */
    isActive() {
        return this.queue === config.ACTIVE;
    }

    /**
     * Return true if the job is finished
     * @return {Boolean}
     */
    isFinished() {
        return this.hasSucceeded() || this.hasFailed();
    }

    /**
     * Return a job's state, basically its queue name
     * @return {String}
     */
    getState() {
        return this.queue;
    }

    /**
     * Return a job's creation date
     * @return {Number}
     */
    getCreationTime() {
        // Get stored date for active, completed and failed jobs
        if (!this.isQueued()) {
            return this.body._created;
        }

        // For queued jobs, use ctime (stored in nanosecs)
        return Math.floor(this.ctime / 1000000);
    }

    /**
     * Return a job's end date
     * @return {Number}
     */
    getEndTime() {
        // Get stored date for completed and failed jobs
        if (this.isFinished()) {
            return this.body._ended;
        }

        // Return now for queued and active jobs
        else {
            return Date.now();
        }
    }

    /**
     * Return a job's duration
     * @return {Number}
     */
    getDuration() {
        // Get stored date for completed and failed jobs
        if (this.isFinished()) {
            return this.body._duration;
        }
        // For queued jobs, compute based on creation time
        if (this.isQueued()) {
            return this.getEndTime() - this.getCreationTime();
        }
        // For active jobs, compute based on dequeued property
        else {
            return this.getEndTime() - this.body._dequeued;
        }
    }

    /**
     * Return a job's result if any
     * @return {?Any}
     */
    getResult() {
        return (this.hasSucceeded() && this.body._result) ? this.body._result : null;
    }

    /**
     * Return a job's error if any
     * @return {?Error}
     */
    getError() {
        return this.hasFailed() ? this.body._error : null;
    }


    /**
    *  INTERNAL DISQUE MANAGEMENT FOR JOBS
    */

    /**
     * Acknowledge job to disque
     * @return {Promise}
     */
    acknowledge() {
        return Promise(this.tasqueue.client.fastack(this.id));
    }

    /**
     * Handle failed job: nack and requeue or push to FAILED
     * @param {Error} err
     * @return {Promise}
     */
    failed(err) {
        // Get maxAttempts for this type of job
        const maxAttemps = this.tasqueue.getWorkerByType(this.getType()).maxAttemps;

        // Too many nacks, push to failed queue
        if (this.getAttempts() >= maxAttemps) {
            return this.setAsFailed(err);
        }
        else {
            // Emit event
            this.tasqueue.emit('job:requeued', {
                id:      this.id,
                type:    this.getType(),
                attempt: this.getAttempts()
            });

            // Requeue job into QUEUED with updated nacks count
            this.body._nacks = this.getAttempts();
            return this.tasqueue.queues[config.QUEUED].addJob(this)
            // Delete job from ACTIVE
            .then(() => this.delete(false));
        }
    }

    /**
     * Base function to push a job to the COMPLETED queue
     * @param {Any} result
     * @return {Promise}
     */
    setAsCompleted(result) {
        // Add result to body
        this.body._result = result;

        // Add to COMPLETED
        return this.tasqueue.queues[config.COMPLETED].addJob(this)
        .then(() => {
            // Acknowledge job in ACTIVE
            return this.acknowledge()
            .then(() => {
                // Emit event
                this.tasqueue.emit('job:success', {
                    id:   this.id,
                    type: this.getType()
                });
            });
        });
    }

    /**
     * Base function to push a job to the FAILED queue
     * @param {Error} err
     * @return {Promise}
     */
    setAsFailed(err) {
        // Add error info to body
        this.body._error = {
            message: err.message,
            stack:   err.stack
        };

        // Add to FAILED
        return this.tasqueue.queues[config.FAILED].addJob(this)
        .then(() => {
            // Acknowledge job in ACTIVE
            return this.acknowledge()
            .then(() => {
                // Emit event
                this.tasqueue.emit('error:job-failed', err, {
                    id:   this.id,
                    type: this.getType()
                });
            });
        });
    }

    /**
     * Map results of a disque JSCAN occurence to a new Job
     * @param {Tasqueue} tasqueue
     * @param {Object} infos
     * @return {Object}
     */
    fromJSCAN(tasqueue, infos) {
        // Map job info from JSCAN result
        const _job = {};
        for (let i = 0; i < infos.length; i += 2) {
            _job[infos[i]] = infos[i + 1];
        }

        // Create an actual Job object and return details
        const job = new Job(tasqueue, _job);
        return job.details();
    }
}

module.exports = Job;
