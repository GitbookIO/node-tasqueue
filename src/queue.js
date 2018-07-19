const _ = require('lodash');
const Promise = require('q-plus');
const Job = require('./job');
const config = require('./config');

class Queue {

    /**
    *  CONSTRUCTOR
    *  @param {Tasqueue} tasqueue
    *  @param {Object} opts
    */
    constructor(tasqueue, opts) {
        this.tasqueue = tasqueue;
        this.name = opts.name;
        this.TTL = opts.TTL;
    }


    /**
    *  API FOR TASQUEUE
    */

    /**
     * Add a job to a queue
     * @param {String} type
     * @param {Object} body
     * @return {Promise}
     */
    addRawJob(type, body = {}) {
        // Add job type to body
        body._jobType = type;

        // Construct query
        const query = [
            this.name,              // queue_name
            JSON.stringify(body),   // job
            0,                      // <ms-timeout>
            'TTL', this.TTL         // TTL
        ];

        // ADDJOB
        return Promise(this.tasqueue.client.addjob(query));
    }

    /**
     * Add an existing Job object to a queue
     * @param {Job} job
     * @return {Promise}
     */
    addJob(job) {
        // Add meta-info depending on queue
        if (this.name !== config.QUEUED) {
            // Pass original creation time
            job.body._created = job.getCreationTime();
        }
        if (this.name === config.ACTIVE) {
            // Set dequeued time when pushed to ACTIVE
            job.body._dequeued = Date.now();
        }
        if (this.name === config.COMPLETED || this.name === config.FAILED) {
            // Add ended time when pushed to COMPLETED or FAILED
            job.body._ended = Date.now();
            // Add job's duration
            job.body._duration = job.getDuration();
        }

        return this.addRawJob(job.getType(), job.body);
    }

    /**
     * Get a job from a queue
     * @param {Object} opts
     * @return {Promise<String>}
     */
    getJob(opts = {}) {
        // Default options
        opts = Object.assign({
            noHang:       false,
            withCounters: false
        }, opts);

        // Construct query
        const query = [
            'FROM', this.name   // queue_name
        ];

        if (opts.withCounters) {
            query.unshift('WITHCOUNTERS');
        }
        if (opts.noHang) {
            query.unshift('NOHANG');
        }

        // GETJOB
        return Promise(this.tasqueue.client.getjob(query))
        .then((res) => {
            // No job available
            if (!res) {
                return null;
            }

            // Reformat result and return jobId
            res = (res.length > 0) ? res[0] : null;
            const jobId = (res.length > 0) ? res[1] : null;

            return jobId;
        });
    }

    /**
     * Get the queue length
     * @return {Promise<Number>}
     */
    length() {
        return Promise(this.tasqueue.client.qlen(this.name));
    }

    /**
     * List jobs in queue
     * @param {Object} opts
     * @return {Object} { prev: {Number}, next: {Number}, list: {Array<Job>} }
     */
    list(opts = {}) {
        // Default options
        opts = Object.assign({
            start: 0,
            limit: 100
        }, opts);

        // While waiting for QPEEK to have a CURSOR option
        // Compute limit based on start
        opts.limit += opts.start;

        return Promise(this.tasqueue.client.qpeek(this.name, opts.limit))
        .then((res) => {
            // Slice res to obtain real result
            res = res.slice(opts.start, opts.limit);
            // Reset opts.limit to its initial value
            opts.limit -= opts.start;

            // Get the jobs ids
            const ids = _.chain(res)
            // jobId is stored in second position
            .map(jobInfos => jobInfos[1])
            .value();

            // Create the list of Job objects
            const list = [];
            return Promise(ids).eachSeries((id) => {
                return Promise(this.tasqueue.client.show(id))
                .then((_job) => {
                    // Don't add job if doesn't exist
                    if (!_job) {
                        return;
                    }
                    // Add job to list
                    list.push(new Job(this.tasqueue, _job));
                });
            })
            .then(() => {
                // Create cursors based on the list length
                const prev = (!!res.length && opts.start > 0) ? Math.max(opts.start - opts.limit, 0) : null;
                const next = (res.length === opts.limit) ? opts.start + opts.limit : null;
                // Return list and cursors
                return {
                    prev,
                    next,
                    list
                };
            });
        });
    }

    // Handle queue pausing
    pause(direction) {
        return Promise(this.tasqueue.client.pause(this.name, direction));
    }
}

module.exports = Queue;
