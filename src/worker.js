const domain = require('domain');
const Promise = require('q');

class Worker {

    /**
    *  CONSTRUCTOR
    *  @param {Tasqueue} tasqueue
    *  @param {Object} handler
    */
    constructor(tasqueue, handler) {
        this.processing = {};
        this.current = 0;
        this.type = handler.type;
        this.concurrency = handler.concurrency || 1;
        this.maxAttemps = handler.maxAttemps || 1;

        this.exec = function(job) {
            const d = Promise.defer();
            const dmn = domain.create();

            // Cleanup domain
            const cleanup = () => {
                dmn.removeAllListeners();
            };

            // Finish with an eror (or not)
            const next = (err) => {
                cleanup();

                if (err) {
                    d.reject(err);
                }
                else {
                    d.resolve();
                }
            };

            dmn.once('error', (err) => {
                cleanup();
                next(err);
            });
            dmn.run(() => {
                Promise()
                .then(() => {
                    return handler.exec(job.body, job);
                })
                .nodeify(next);
            });

            return d.promise;
        };
    }


    /**
    *  API FOR TASQUEUE
    */

    /**
     * Process a job
     * @param {Job} job
     * @return {Promise}
     */
    processJob(job) {
        this.current++;

        return Promise()
        .then(() => {
            this.processing[job.id] = this.wrapJobprocess(job, this.exec(job));
            return this.processing[job.id];
        })
        .fin(() => {
            this.current--;
            delete this.processing[job.id];
        });
    }

    /**
     * Return a count of available occurences for this worker
     * @return {Number}
     */
    countAvailable() {
        return this.concurrency - this.current;
    }

    /**
     * Check if a worker is available to work
     * @return {Boolean}
     */
    isAvailable() {
        return this.concurrency > this.current;
    }


    /**
    *  INTERNAL UTILITY FUNCTIONS
    */

    /**
     * Wrap job processing with exec function
     * @param {Job} job
     * @param {Function} exec
     * @return {Promise}
     */
    wrapJobprocess(job, exec) {
        // Exec job handler
        return Promise(exec)
        .timeout(job.tasqueue.opts.jobTimeout, `took longer than ${Math.ceil(job.tasqueue.opts.jobTimeout / 1000)} seconds to process`)
        .then(
            // Job succeeded
            result => job.setAsCompleted(result),
            // Job failed
            err => job.failed(err)
        );
    }
}

module.exports = Worker;
