const Promise = require('q');
const Tasqueue = require('../src');

describe('tasqueue.init()', () => {
    const tasqueue = new Tasqueue();

    it('should connect to disque', () => {
        return tasqueue.init()
        .fail((err) => {
            throw err;
        })
        .fin(() => tasqueue.shutdown(1000, () => {}));
    });
});

describe('tasqueue.ping()', () => {
    const tasqueue = new Tasqueue();

    it('should check connection to disque', () => {
        return tasqueue.init()
        .then(() => tasqueue.ping())
        .fail((err) => {
            throw err;
        })
        .fin(() => tasqueue.shutdown(1000, () => {}));
    });
});

describe('tasqueue.shutdown()', () => {
    const tasqueue = new Tasqueue();

    it('should end connection to disque', () => {
        return tasqueue.init()
        .then(() => tasqueue.ping())
        .then(() => tasqueue.shutdown(1000, () => {}))
        .fail((err) => {
            throw err;
        })
        .fin(() => tasqueue.ping())
        .then(
            () => {
                throw new Error('Client shouldn\'t be able to ping on a closed connection');
            },
            () => Promise()
        );
    });
});
