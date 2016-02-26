var _ =         require('lodash');
var Q =         require('q');
var Job =       require('./job');
var config =    require('./config');

/**
 *  CONSTRUCTOR
 */

function Queue(tasqueue, opts) {
    this.tasqueue = tasqueue;
    this.name =     opts.name;
    this.TTL =      opts.TTL;
}


/**
 *  API FOR TASQUEUE
 */

// Add a job to a queue
Queue.prototype.addRawJob = function(type, body) {
    body = body || {};

    // Add job type to body
    body._jobType = type;

    // Construct query
    var query = [
        this.name,              // queue_name
        JSON.stringify(body),   // job
        0,                      // <ms-timeout>
        'TTL', this.TTL         // TTL
    ];

    // ADDJOB
    return Q(this.tasqueue.client.addjob(query));
};

// Add an existing Job object to a queue
Queue.prototype.addJob = function(job) {
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
};

// Get a job from a queue
Queue.prototype.getJob = function(opts) {
    // Default options
    opts = _.defaults(opts || {}, {
        noHang:         false,
        withCounters:   false
    });

    // Construct query
    var query = [
        'FROM', this.name   // queue_name
    ];

    if (opts.withCounters)  query.unshift('WITHCOUNTERS');
    if (opts.noHang)        query.unshift('NOHANG');

    // GETJOB
    return Q(this.tasqueue.client.getjob(query))
    .then(function(res) {
        // No job available
        if (!res) return null;

        // Reformat result and return jobId
        res =       (res.length > 0)? res[0] : null;
        var jobId = (res.length > 0)? res[1] : null;

        return jobId;
    });
};

// Get the queue length
Queue.prototype.length = function() {
    return Q(this.tasqueue.client.qlen(this.name));
};

// List jobs in queue
Queue.prototype.list = function(opts) {
    var that = this;

    // Default options
    opts = _.defaults(opts || {}, {
        start: 0,
        limit: 100
    });

    // While waiting for QPEEK to have a CURSOR option
    // Compute limit based on start
    opts.limit += opts.start;

    return Q(that.tasqueue.client.qpeek(that.name, opts.limit))
    .then(function(res) {
        // Slice res to obtain real result
        res = res.slice(opts.start, opts.limit);
        // Reset opts.limit to its initial value
        opts.limit -= opts.start;

        // Get the jobs ids
        var ids = _.chain(res)
            .map(function(jobInfos) {
                // jobId is stored in second position
                return jobInfos[1];
            })
            .value();

        // Create the list of Job objects
        var list = [];
        return Q(ids).eachSeries(function(id) {
            return Q(that.tasqueue.client.show(id))
            .then(function(_job) {
                // Don't add job if doesn't exist
                if (!_job) return;
                // Add job to list
                list.push(new Job(that.tasqueue, _job));
            });
        })
        .then(function() {
            // Create cursors based on the list length
            var prev = (!!res.length && opts.start > 0)?    Math.max(opts.start - opts.limit, 0) : null;
            var next = (res.length === opts.limit)?         opts.start + opts.limit : null;
            // Return list and cursors
            return {
                prev: prev,
                next: next,
                list: list
            };
        });
    });
};

// Handle queue pausing
Queue.prototype.pause = function(direction) {
    return Q(this.tasqueue.client.pause(this.name, direction));
};

module.exports = Queue;
