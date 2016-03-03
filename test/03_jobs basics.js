var Q = require('q');
var Tasqueue = require('../lib/index');

var jobType = 'custom';
var jobId = null;

describe('tasqueue.pushJob()', function() {
    var tasqueue = new Tasqueue();
    var pushed = 0;

    tasqueue.on('job:pushed', function(job) {
        if (job.type === jobType) pushed++;
    });

    it('should push new jobs and emit job:pushed', function() {
        return tasqueue.init()
        .then(function() {
            return tasqueue.pushJob(jobType);
        })
        .then(function(_jobId) {
            jobId = _jobId;
            return Q.delay(5);
        })
        .then(function() {
            if (!pushed) throw new Error('job:pushed should have been fired by now');
        })
        .fin(function() {
            return tasqueue.shutdown(1000, function() {});
        });
    });
});

describe('tasqueue.getJob()', function() {
    var tasqueue = new Tasqueue();

    it('should retrieve a job', function() {
        return tasqueue.init()
        .then(function() {
            return tasqueue.getJob(jobId);
        })
        .then(function(job) {
            var jobInfos = job.details();
            if (jobInfos.type !== jobType) throw new Error('job should be of type '+jobType);
        })
        .fin(function() {
            return tasqueue.shutdown(1000, function() {});
        });
    });
});

describe('job.cancel()', function() {
    var tasqueue = new Tasqueue();
    var canceled = false;

    tasqueue.on('job:canceled', function(job) {
        if (job.id === jobId) canceled = true;
    });

    it('should cancel a job and emit job:canceled', function() {
        return tasqueue.init()
        .then(function() {
            return tasqueue.getJob(jobId);
        })
        .then(function(job) {
            return job.cancel();
        })
        .then(function() {
            return Q.delay(5);
        })
        .then(function() {
            if (!canceled) throw new Error('job:canceled should have been fired by now');
        })
        .then(function() {
            return tasqueue.getJob(jobId);
        })
        .then(function() {
            throw new Error('job '+jobId+' should have been canceled');
        }, function() {
            return Q();
        })
        .fin(function() {
            return tasqueue.shutdown(1000, function() {});
        });
    });
});

describe('tasqueue.countFailed()', function() {
    var tasqueue = new Tasqueue();

    it('should return the number of failed jobs', function() {
        return tasqueue.init()
        .then(function() {
            return tasqueue.countFailed();
        })
        .then(function(countFailed) {
            if (!countFailed === 1) throw new Error('a canceled job should be marked as failed');
            return Q();
        })
        .fin(function() {
            return tasqueue.shutdown(1000, function() {});
        });
    });
});

describe('tasqueue.listFailed()', function() {
    var tasqueue = new Tasqueue();

    it('should return the list of failed jobs', function() {
        return tasqueue.init()
        .then(function() {
            return tasqueue.listFailed();
        })
        .then(function(res) {
            if (!res.list) throw new Error('#listFailed() result should have a list property');
            if (!res.list.length) throw new Error('canceled job should be in the #listFailed() result list');

            var job = res.list[0].details();
            jobId = job.id;
        })
        .fin(function() {
            return tasqueue.shutdown(1000, function() {});
        });
    });
});

describe('job.delete()', function() {
    var tasqueue = new Tasqueue();
    var deleted  = false;

    tasqueue.on('job:deleted', function(job) {
        if (job.id === jobId) deleted = true;
    });

    it('should delete a job and emit job:deleted', function() {
        return tasqueue.init()
        .then(function() {
            return tasqueue.getJob(jobId);
        })
        .then(function(job) {
            return job.delete();
        })
        .then(function() {
            return Q.delay(5);
        })
        .then(function() {
            if (!deleted) throw new Error('job:deleted should have been fired by now');
        })
        .then(function() {
            return tasqueue.getJob(jobId);
        })
        .then(function() {
            throw new Error('job '+jobId+' should have been deleted');
        }, function() {
            return Q();
        })
        .fin(function() {
            return tasqueue.shutdown(1000, function() {});
        });
    });
});



