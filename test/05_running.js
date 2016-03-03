var Q = require('q-plus');
var _ = require('lodash');
var Tasqueue = require('../lib/index');

var TO_PUSH = 10;
var jobType1 = 'handler1';
var jobType2 = 'handler2';

describe('Running tasqueue', function() {
    var tasqueue  = new Tasqueue({
        pollDelay: 100
    });
    var nHandlers = 0;
    var polling   = false;

    tasqueue.on('handler:registered', function(h) {
        nHandlers++;
    })
    .on('client:polling', function(d) {
        polling = true;
    });

    var handler1 = { type: jobType1, exec: function() {
        return Q.delay(200)
        .then(function() {
            throw new Error('job should fail');
        });
    }};
    var handler2 = { type: jobType2, exec: function() {
        return Q.delay(200);
    }};

    it('should initialize the connection to disque', function() {
        return tasqueue.init();
    });

    it('should register some job handlers', function() {
        tasqueue.registerHandler(handler1);
        tasqueue.registerHandler(handler2);

        return Q.delay(5)
        .then(function() {
            if (nHandlers !== 2) throw new Error('handlers should be registered properly');
        });
    });

    it('should push some jobs', function() {
        return Q(_.range(TO_PUSH)).eachSeries(function(n, i) {
            var jobType = (i % 2 === 0)? jobType1 : jobType2;
            return tasqueue.pushJob(jobType);
        });
    });

    it('should start polling', function() {
        tasqueue.poll();

        return Q.delay(5)
        .then(function() {
            if (!polling) throw new Error('client should start polling');
        });
    });

    it('should be processing jobs', function() {
        return Q.delay(100)
        .then(function() {
            return tasqueue.countActive();
        })
        .then(function(countActive) {
            if (!countActive) throw new Error('some jobs should be processing');
        });
    });

    it('should have processed all jobs after some time', function() {
        return Q.delay(3000)
        .then(function() {
            return tasqueue.countFailed();
        })
        .then(function(countFailed) {
            if (countFailed !== (TO_PUSH / 2)) throw new Error('half of the pushed jobs should have failed');
            return tasqueue.countCompleted();
        })
        .then(function(countCompleted) {
            if (countCompleted !== (TO_PUSH / 2)) throw new Error('half of the pushed jobs should have succeeded');
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