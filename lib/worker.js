var domain =    require('domain');
var Q =         require('q');

/**
 *  CONSTRUCTOR
 */

function Worker(tasqueue, handler) {
    var that = this;

    that.processing =   {};
    that.current =      0;
    that.type =         handler.type;
    that.concurrency =  handler.concurrency || 1;
    that.maxAttemps =   handler.maxAttemps || 1;

    that.exec = function(job) {
        var d =     Q.defer();
        var dmn =   domain.create();

        // Cleanup domain
        var cleanup = function() {
            dmn.removeAllListeners();
        };

        // Finish with an eror (or not)
        var next = function(err) {
            cleanup();

            if (err) d.reject(err);
            else d.resolve();
        };

        dmn.once('error', function(err) {
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
    };
}


/**
 *  API FOR TASQUEUE
 */

// Process a job
Worker.prototype.processJob = function(job) {
    var that = this;

    this.current++;

    return Q()
    .then(function() {
        that.processing[job.id] = that.wrapJobprocess(job, that.exec(job));
        return that.processing[job.id];
    })
    .fin(function() {
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
    .timeout(job.tasqueue.opts.jobTimeout, 'took longer than '+Math.ceil(job.tasqueue.opts.jobTimeout/1000)+' seconds to process')
    .then(function(result) {
        // Job succeeded
        return job.setAsCompleted(result);
    }, function(err) {
        // Job failed
        return job.failed(err);
    });
};

module.exports = Worker;
