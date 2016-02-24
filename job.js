var _ = require('lodash');
var Q = require('q');
var config = require('./config');

function Job(tasqueue) {
    this.tasqueue = tasqueue;
}

// Format a job obtained from a queue with GETJOB
Job.fromQueue = function fromQueue(tasqueue, infos) {
    var job = new Job(tasqueue);

    job.id = infos[1];
    job.body = JSON.parse(infos[2]);
    job.nacks = infos[4];
    job.additionalDeliveries = infos[6];

    return job;
};

// Return a job's type stored in body
Job.prototype.getType = function() {
    return this.body._jobType;
};

// Return a job's actual body
Job.prototype.getBody = function() {
    return _.omit(this.body, [
        '_jobType',
        '_created',
        '_ended',
        '_duration',
        '_result',
        '_error'
    ]);
};

// Return a job's real state based on its queue and state
Job.prototype.getState = function() {
    return this.queue === config.QUEUE? this.state : this.queue;
};

// Return a job's creation date based on its queue and state
Job.prototype.getCreationDate = function() {
    // Get stored date for completed and failed jobs
    if (this.queue !== config.QUEUE) return this.body._created;

    // For queued jobs, use ctime (stored in nanosecs)
    return Math.floor(this.ctime / 1000000);
};

// Return a job's end date based on its queue and state
Job.prototype.getEndDate = function() {
    // Get stored date for completed and failed jobs
    if (this.queue !== config.QUEUE) return this.body._ended;

    // For queued jobs, return now
    else return Date.now();
};

// Return a job's duration based on its queue and state
Job.prototype.getDuration = function() {
    // Get stored date for completed and failed jobs
    if (this.queue !== config.QUEUE) return this.body._duration;

    // For queued jobs, compute
    return this.getEndDate() - this.getCreationDate();
};

// Return a job's result if any
Job.prototype.getResult = function() {
    return this.queue === config.COMPLETED? this.body._result : null;
};

// Return a job's error if any
Job.prototype.getError = function() {
    return this.queue === config.FAILED? this.body._error : null;
};

// Return pretty informations for this job
Job.prototype.details = function() {
    return {
        id: this.id,
        type: this.getType(),
        body: this.getBody(),
        state: this.getState(),
        created: this.getCreationDate(),
        ended: this.getEndDate(),
        duration: this.getDuration(),
        result: this.getResult(),
        error: this.getError()
    };
};

// Acknowledge job to disque then push to completed queue
Job.prototype.acknowledge = function(result) {
    var that = this;

    that.tasqueue.emit('job:success', that.id, that.getType());
    return Q(that.tasqueue.client.fastack(that.id))
    .then(function() {
        return that.setAsCompleted(result);
    });
};

// Handle failed job: nack and requeue or push to failed
Job.prototype.failed = function(err, duration) {
    var that = this;

    // Get maxAttempts for this type of job
    var maxAttemps = that.tasqueue.workers[that.getType()].maxAttemps;

    // Too many nacks, push to failed queue
    if ((that.nacks+1) >= maxAttemps) {
        that.tasqueue.emit('job:fail', that.id, that.getType(), err);
        return that.setAsFailed(err, duration);
    }
    // Requeue job
    else {
        that.tasqueue.emit('job:requeue', that.id, that.getType(), that.nacks+1);
        return Q(that.client.nack(that.id));
    }
};

// Utterly delete a job
Job.prototype.delete = function(emit) {
    var that = this;

    emit = emit || true;
    return Q(that.tasqueue.client.deljob(that.id))
    .then(function() {
        if (emit) that.tasqueue.emit('job:delete', that.id);
        return Q();
    });
};

// Cancel a job -> move to failed queue
// Error if job is not 'queued' in QUEUE
Job.prototype.cancel = function(force) {
    var that = this;

    // Force cancelation of a job that is not queued
    force = force || false;

    // Päuse QUEUE queue to prevent GETJOB operations on this job
    return Q(that.tasqueue.client.pause(config.QUEUE, 'out'))
    .then(function() {
        if (!force && (that.queue !== config.QUEUE || that.state !== 'queued')) {
            throw new Error('Only queued jobs may be cancelled');
        }

        that.tasqueue.emit('job:cancel', that.id);

        return that.setAsFailed(new Error('Canceled'), that.getDuration())
        .then(function() {
            return that.delete();
        })
        .then(function() {
            return Q(that.tasqueue.client.pause(config.QUEUE, 'none'));
        });
    })
    .fail(function(err) {
        that.tasqueue.emit('error:cancel', that.id);
        return Q(that.tasqueue.client.pause(config.QUEUE, 'none'));
    });
};

// Base function to push a job to the COMPLETED queue
Job.prototype.setAsCompleted = function(result, duration) {
    var that = this;

    // Add created and ended times to body
    that.body._created = that.getCreationDate();
    that.body._ended = new Date();
    that.body._duration = duration;

    // Add result to body
    that.body._result = result;

    return Q(that.tasqueue.client.addjob(config.COMPLETED, JSON.stringify(that.body), 0, 'TTL', that.tasqueue.opts.completedTTL))
    .then(function() {
        return that.delete(false);
    });
};

// Base function to push a job to the FAILED queue
Job.prototype.setAsFailed = function(err, duration) {
    var that = this;

    // Add created and ended times to body
    that.body._created = that.getCreationDate();
    that.body._ended = new Date();
    that.body._duration = duration;

    // Add error info to body
    that.body._error = {
        message: err.message,
        stack: err.stack
    };

    return Q(that.tasqueue.client.addjob(config.FAILED, JSON.stringify(that.body), 0, 'TTL', that.tasqueue.opts.failedTTL))
    .then(function() {
        return that.delete(false);
    });
};

module.exports = Job;