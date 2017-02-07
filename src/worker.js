const domain = require('domain');
const Q      = require('q');

/**
 *  CONSTRUCTOR
 */

function Worker(tasqueue, handler) {
    const that = this;

    that.processing  = {};
    that.current     = 0;
    that.type        = handler.type;
    that.concurrency = handler.concurrency || 1;
    that.maxAttemps  = handler.maxAttemps || 1;

    that.exec = function(job) {
        const d   = Q.defer();
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
            Q()
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

// Process a job
Worker.prototype.processJob = function(job) {
    const that = this;

    this.current++;

    return Q()
    .then(() => {
        that.processing[job.id] = that.wrapJobprocess(job, that.exec(job));
        return that.processing[job.id];
    })
    .fin(() => {
        that.current--;
        delete that.processing[job.id];
    });
};

// Return a count of available occurences for this worker
Worker.prototype.countAvailable = function() {
    return this.concurrency - this.current;
};

// Check if a worker is available to work
Worker.prototype.isAvailable = function() {
    return this.concurrency > this.current;
};


/**
 *  INTERNAL UTILITY FUNCTIONS
 */

// Wrap job processing with exec function
Worker.prototype.wrapJobprocess = function(job, exec) {
    // Exec job handler
    return Q(exec)
    .timeout(job.tasqueue.opts.jobTimeout, 'took longer than ' + Math.ceil(job.tasqueue.opts.jobTimeout / 1000) + ' seconds to process')
    .then((result) => {
        // Job succeeded
        return job.setAsCompleted(result);
    }, (err) => {
        // Job failed
        return job.failed(err);
    });
};

module.exports = Worker;
