var domain = require('domain');

var _ = require('lodash');
var Q = require('q');

function Worker(tasqueue, handler) {
    var that = this;

    that.processing = {};
    that.type = handler.type;
    that.concurrency = handler.concurrency || 1;
    that.maxAttemps = handler.maxAttemps || 1;

    that.exec = function(job) {
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

// Process a job
Worker.prototype.processJob = function(job) {
    var that = this;

    return Q()
    .then(function() {
        that.processing[job.id] = that.wrapJobprocess(job, that.exec(job));
        return that.processing[job.id];
    })
    .fin(function() {
        delete that.processing[job.id];
    });
};

// Wrap job processing with exec function
Worker.prototype.wrapJobprocess = function(job, exec) {
    var startTime = Date.now();

    return Q(exec)
    .timeout(job.tasqueue.opts.jobTimeout, 'took longer than '+Math.ceil(job.tasqueue.opts.jobTimeout/1000)+' seconds to process')
    .then(function(result) {
        var duration = Date.now() - startTime;
        return job.acknowledge(result, duration);
    }, function(err) {
        var duration = Date.now() - startTime;
        return job.failed(err, duration);
    });
};

// Return a count of processing jobs for this worker
Worker.prototype.countProcessing = function() {
    return _.size(this.processing);
};

// Return a count of available occurences for this worker
Worker.prototype.countAvailable = function() {
    return this.concurrency - this.countProcessing();
};

// Check if a worker is available to work
Worker.prototype.isAvailable = function() {
    return this.concurrency > this.countProcessing();
};

module.exports = Worker;