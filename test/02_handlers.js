var Q = require('q');
var _ = require('lodash');
var Tasqueue = require('../lib/index');

describe('tasqueue.registerHandler()', function() {
    var tasqueue = new Tasqueue();
    var registered = false;
    var registeredTwice = false;

    tasqueue.on('handler:registered', function(h) {
        if (h.type === 'custom') registered = true;
    });
    tasqueue.on('error:existing-handler', function(err, h) {
        if (h.type === 'custom') registeredTwice = true;
    });

    var handler = {
        type: 'custom',
        exec: function() {}
    };


    it('should register a new handler and emit handler:registered', function() {
        return tasqueue.init()
        .then(function() {
            tasqueue.registerHandler(handler);
            return Q.delay(5);
        })
        .then(function() {
            if (!registered) throw new Error('handler:registered should have been fired by now');
        })
        .fin(function() {
            return tasqueue.shutdown(1000, function() {});
        });
    });

    it('should emit error:existing-handler when registering an handler twice', function() {
        return tasqueue.init()
        .then(function() {
            tasqueue.registerHandler(handler);
            tasqueue.registerHandler(handler);
            return Q.delay(5);
        })
        .then(function() {
            if (!registeredTwice) throw new Error('error:existing-handler should have been fired by now');
        })
        .fin(function() {
            return tasqueue.shutdown(1000, function() {});
        });
    });
});

describe('tasqueue.listHandlers()', function() {
    var tasqueue = new Tasqueue();

    var handler1 = { type: 'handler1', exec: function() {}};
    var handler2 = { type: 'handler2', exec: function() {}};
    var handler3 = { type: 'handler3', exec: function() {}};
    var handlers = ['handler1', 'handler2', 'handler3'];

    it('should return the list of registered handlers', function() {
        return tasqueue.init()
        .then(function() {
            tasqueue.registerHandler(handler1);
            tasqueue.registerHandler(handler2);
            tasqueue.registerHandler(handler3);

            var list = tasqueue.listHandlers();
            if (!_.isEqual(list, handlers)) throw new Error('all handlers should be listed');
        })
        .fin(function() {
            return tasqueue.shutdown(1000, function() {});
        });
    });
});