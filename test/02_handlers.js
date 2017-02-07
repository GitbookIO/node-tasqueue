const Promise = require('q');
const _ = require('lodash');
const Tasqueue = require('../src');

describe('tasqueue.registerHandler()', () => {
    const tasqueue = new Tasqueue();
    let registered = false;
    let registeredTwice = false;

    const handler = {
        type: 'custom',
        exec() {}
    };

    tasqueue.on('handler:registered', (h) => {
        if (h.type === 'custom') {
            registered = true;
        }
    })
    .on('error:existing-handler', (err, h) => {
        if (h.type === 'custom') {
            registeredTwice = true;
        }
    });

    it('should register a new handler and emit handler:registered', () => {
        return tasqueue.init()
        .then(() => {
            tasqueue.registerHandler(handler);
            return Promise.delay(5);
        })
        .then(() => {
            if (!registered) {
                throw new Error('handler:registered should have been fired by now');
            }
        })
        .fin(() => tasqueue.shutdown(1000, () => {}));
    });

    it('should emit error:existing-handler when registering an handler twice', () => {
        return tasqueue.init()
        .then(() => {
            tasqueue.registerHandler(handler);
            tasqueue.registerHandler(handler);
            return Promise.delay(5);
        })
        .then(() => {
            if (!registeredTwice) {
                throw new Error('error:existing-handler should have been fired by now');
            }
        })
        .fin(() => tasqueue.shutdown(1000, () => {}));
    });
});

describe('tasqueue.listHandlers()', () => {
    const tasqueue = new Tasqueue();

    const handler1 = { type: 'handler1', exec() {}};
    const handler2 = { type: 'handler2', exec() {}};
    const handler3 = { type: 'handler3', exec() {}};
    const handlers = [ 'handler1', 'handler2', 'handler3' ];

    it('should return the list of registered handlers', () => {
        return tasqueue.init()
        .then(() => {
            tasqueue.registerHandler(handler1);
            tasqueue.registerHandler(handler2);
            tasqueue.registerHandler(handler3);

            const list = tasqueue.listHandlers();
            if (!_.isEqual(list, handlers)) {
                throw new Error('all handlers should be listed');
            }
        })
        .fin(() => tasqueue.shutdown(1000, () => {}));
    });
});
