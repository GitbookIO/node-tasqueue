var _ = require('lodash');
var Q = require('q');
var config = require('./config');

/**
 *  CONSTRUCTOR
 */

// Create a new Job and extend with disque SHOW details
function Job(tasqueue, details) {
    var that = this;

    that.tasqueue = tasqueue;

    // Extend job with disque details
    _.forIn(details, function(v, k) {
        that[k] = v;
    });

    // Parse job body as pure JS
    that.body = JSON.parse(that.body);
}


/**
 *  JOBS API
 */

// Return pretty informations about this job
Job.prototype.details = function() {
    return {
        id:         this.id,
        type:       this.getType(),
        body:       this.getBody(),
        state:      this.getState(),
        created:    new Date(this.getCreationTime()),
        ended:      new Date(this.getEndTime()),
        duration:   this.getDuration(),
        result:     this.getResult(),
        error:      this.getError(),
        attempt:    this.getAttempts()
    };
};

// Cancel a job -> move to FAILED
// Error if job is not in QUEUED
Job.prototype.cancel = function(force) {
    var that = this;

    if (!that.isQueued()) {
        return Q.reject(new Error('Only queued jobs may be cancelled'));
    }

    // Pause QUEUED to prevent GETJOB operations on this job
    return that.tasqueue.queues[config.QUEUED].pause('out')
    .then(function() {
        // Add job to FAILED
        return that.setAsFailed(new Error('Canceled'))
        .then(function() {
            // Emit event
            that.tasqueue.emit('job:canceled', {
                id:     that.id,
                type:   that.getType()
            });
        });
    })
    .fail(function(err) {
        // Emit event
        that.tasqueue.emit('error:job-cancel', err, {
            id:     that.id,
            type:   that.getType()
        });
    })
    // Unpause QUEUED whatever the result is
    .fin(function() {
        return that.tasqueue.queues[config.QUEUED].pause('none');
    });
};

// Utterly delete a job
Job.prototype.delete = function(emit) {
    var that = this;

    emit = emit || true;
    return Q(that.tasqueue.client.deljob(that.id))
    .then(function() {
        if (emit) {
            that.tasqueue.emit('job:deleted', {
                id:     that.id,
                type:   that.getType()
            });
        }
    });
};


/**
 *  COMPUTED PROPERTIES
 */

// Return a job's type stored in body
Job.prototype.getType = function() {
    return this.body._jobType;
};

// Return a job's actual body
Job.prototype.getBody = function() {
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
};

// Return a job's nacks count
Job.prototype.getNacks = function() {
    return this.body._nacks || 0;
};

// Return a job's attempts count
Job.prototype.getAttempts = function() {
    return this.getNacks() + 1;
};

// Return true if the job has failed
Job.prototype.hasFailed = function() {
    return this.queue === config.FAILED;
};

// Return true if the job was successful
Job.prototype.hasSucceeded = function() {
    return this.queue === config.COMPLETED;
};

// Return true if the job is queued
Job.prototype.isQueued = function() {
    return this.queue === config.QUEUED;
};

// Return true if the job is active
Job.prototype.isActive = function() {
    return this.queue === config.ACTIVE;
};

// Return true if the job is finished
Job.prototype.isFinished = function() {
    return this.hasSucceeded() || this.hasFailed();
};

// Return a job's state, basically its queue name
Job.prototype.getState = function() {
    return this.queue;
};

// Return a job's creation date
Job.prototype.getCreationTime = function() {
    // Get stored date for active, completed and failed jobs
    if (!this.isQueued()) return this.body._created;

    // For queued jobs, use ctime (stored in nanosecs)
    return Math.floor(this.ctime / 1000000);
};

// Return a job's end date
Job.prototype.getEndTime = function() {
    // Get stored date for completed and failed jobs
    if (this.isFinished()) return this.body._ended;

    // Return now for queued and active jobs
    else return Date.now();
};

// Return a job's duration
Job.prototype.getDuration = function() {
    // Get stored date for completed and failed jobs
    if (this.isFinished()) return this.body._duration;
    // For queued jobs, compute based on creation time
    if (this.isQueued()) return this.getEndTime() - this.getCreationTime();
    // For active jobs, compute based on dequeued property
    else return this.getEndTime() - this.body._dequeued;
};

// Return a job's result if any
Job.prototype.getResult = function() {
    return this.hasSucceeded() && this.body._result? this.body._result : null;
};

// Return a job's error if any
Job.prototype.getError = function() {
    return this.hasFailed()? this.body._error : null;
};


/**
 *  INTERNAL DISQUE MANAGEMENT FOR JOBS
 */

// Acknowledge job to disque
Job.prototype.acknowledge = function() {
    var that = this;
    return Q(that.tasqueue.client.fastack(that.id));
};

// Handle failed job: nack and requeue or push to FAILED
Job.prototype.failed = function(err) {
    var that = this;

    // Get maxAttempts for this type of job
    var maxAttemps = that.tasqueue.getWorkerByType(that.getType()).maxAttemps;

    // Too many nacks, push to failed queue
    if (that.getAttempts() >= maxAttemps) {
        return that.setAsFailed(err);
    }
    else {
        // Emit event
        that.tasqueue.emit('job:requeued', {
            id:         that.id,
            type:       that.getType(),
            attempt:    that.getAttempts()
        });

        // Requeue job into QUEUED with updated nacks count
        that.body._nacks = that.getAttempts();
        return that.tasqueue.queues[config.QUEUED].addJob(that)
        .then(function() {
            // Delete job from ACTIVE
            return that.delete(false);
        });
    }
};

// Base function to push a job to the COMPLETED queue
Job.prototype.setAsCompleted = function(result) {
    var that = this;

    // Add result to body
    that.body._result = result;

    // Add to COMPLETED
    return that.tasqueue.queues[config.COMPLETED].addJob(that)
    .then(function() {
        // Acknowledge job in ACTIVE
        return that.acknowledge()
        .then(function() {
            // Emit event
            that.tasqueue.emit('job:success', {
                id:     that.id,
                type:   that.getType()
            });
        });
    });
};

// Base function to push a job to the FAILED queue
Job.prototype.setAsFailed = function(err) {
    var that = this;

    // Add error info to body
    that.body._error = {
        message: err.message,
        stack: err.stack
    };

    // Add to FAILED
    return that.tasqueue.queues[config.FAILED].addJob(that)
    .then(function() {
        // Acknowledge job in ACTIVE
        return that.acknowledge()
        .then(function() {
            // Emit event
            that.tasqueue.emit('error:job-failed', {
                id:     that.id,
                type:   that.getType(),
                error:  err
            });
        });
    });
};

// Map results of a disque JSCAN occurence to a new Job
Job.fromJSCAN = function(tasqueue, infos) {
    // Map job info from JSCAN result
    var _job = {};
    for (var i = 0; i < infos.length; i+=2) {
        _job[infos[i]] = infos[i+1];
    }

    // Create an actual Job object and return details
    var job = new Job(tasqueue, _job);
    return job.details();
};

module.exports = Job;
