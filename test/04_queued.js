var Q = require('q-plus');
var _ = require('lodash');
var Tasqueue = require('../lib/index');

var jobType = 'custom';
var TO_PUSH  = 10;

describe('tasqueue.countQueued()', function() {
    var tasqueue = new Tasqueue();

    it('should get the count of queued jobs', function() {
        return tasqueue.init()
        .then(function() {
            return Q(_.range(TO_PUSH)).eachSeries(function() {
                return tasqueue.pushJob(jobType);
            });
        })
        .then(function() {
            return tasqueue.countQueued();
        })
        .then(function(count) {
            if (count !== TO_PUSH) throw new Error('The number of queued jobs should be '+TO_PUSH);
        })
        .fin(function() {
            return tasqueue.shutdown(1000, function() {});
        });
    });
});

describe('tasqueue.listQueued()', function() {
    var tasqueue = new Tasqueue();

    it('should get the list of queued jobs', function() {
        return tasqueue.init()
        .then(function() {
            return tasqueue.listQueued({ limit: TO_PUSH });
        })
        .then(function(res) {
            var jobs     = res.list;
            var firstJob = jobs[0].details();

            if (jobs.length !== TO_PUSH) throw new Error('The number of listed jobs should be '+TO_PUSH);
            if (firstJob.type !== jobType) throw new Error('Listed jobs should be of type '+jobType);

            return Q(jobs).eachSeries(function(job) {
                return job.delete();
            });
        })
        .fin(function() {
            return tasqueue.shutdown(1000, function() {});
        });
    });
});