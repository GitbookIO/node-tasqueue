var Q = require('q');
var Tasqueue = require('../lib/index');

describe('tasqueue.init()', function() {
    var tasqueue = new Tasqueue();

    it('should connect to disque', function() {
        return tasqueue.init()
        .fail(function(err) {
            throw err;
        })
        .fin(function() {
            return tasqueue.shutdown(1000, function() {});
        });
    });
});

describe('tasqueue.ping()', function() {
    var tasqueue = new Tasqueue();

    it('should check connection to disque', function() {
        return tasqueue.init()
        .then(function() {
            return tasqueue.ping();
        })
        .fail(function(err) {
            throw err;
        })
        .fin(function() {
            return tasqueue.shutdown(1000, function() {});
        });
    });
});

describe('tasqueue.shutdown()', function() {
    var tasqueue = new Tasqueue();

    it('should end connection to disque', function() {
        return tasqueue.init()
        .then(function() {
            return tasqueue.ping();
        })
        .then(function() {
            return tasqueue.shutdown(1000, function() {});
        })
        .fail(function(err) {
            throw err;
        })
        .fin(function() {
            return tasqueue.ping();
        })
        .then(function() {
            throw new Error('Client shouldn\'t be able to ping on a closed connection');
        }, function(err) {
            return Q();
        });
    });
});



