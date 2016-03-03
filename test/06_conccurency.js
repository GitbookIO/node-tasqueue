var Q = require('q-plus');
var _ = require('lodash');
var Tasqueue = require('../lib/index');

var TO_PUSH = 5;
var jobType1 = 'handler1';
var jobType2 = 'handler2';

describe('Concurrency', function() {
    var tasqueue  = new Tasqueue({
        pollDelay: 100
    });

    var handler1 = { type: jobType1, exec: function() {
        return Q.delay(2000);
    }};
    var handler2 = { type: jobType2, concurrency: TO_PUSH, exec: function() {
        return Q.delay(200);
    }};

    tasqueue.registerHandler(handler1);
    tasqueue.registerHandler(handler2);

    it('should initialize the connection to disque', function() {
        return tasqueue.init();
    });

    it('should push a blocking job', function() {
        return tasqueue.pushJob(jobType1);
    });

    it('should push some other jobs', function() {
        return Q(_.range(TO_PUSH)).eachSeries(function() {
            return tasqueue.pushJob(jobType2);
        });
    });

    it('should start polling', function() {
        tasqueue.poll();
    });

    it('should be processing all jobs', function() {
        return Q.delay(100)
        .then(function() {
            return tasqueue.countActive();
        })
        .then(function(countActive) {
            if (countActive !== (TO_PUSH + 1)) throw new Error('all jobs should be processing');
            return Q();
        });
    });

    it('should have processed all jobs after some time', function() {
        return Q.delay(3000)
        .then(function() {
            return tasqueue.countCompleted();
        })
        .then(function(countCompleted) {
            if (countCompleted !== (TO_PUSH + 1)) throw new Error('all pushed jobs should have succeeded');
            return Q();
        });
    });

    it('should clean all jobs before shuting down', function() {
        return tasqueue.listFailed()
        .then(function(res) {
            return Q(res.list).eachSeries(function(job) {
                return job.delete();
            });
        })
        .then(function() {
            return tasqueue.listCompleted();
        })
        .then(function(res) {
            return Q(res.list).eachSeries(function(job) {
                return job.delete();
            });
        });
    });

    it('should end the connection to disque', function() {
        return tasqueue.shutdown(1000, function() {});
    });
});