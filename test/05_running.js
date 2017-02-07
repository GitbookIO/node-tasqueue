const Promise = require('q-plus');
const _ = require('lodash');
const Tasqueue = require('../src');

const TO_PUSH = 10;
const jobType1 = 'handler1';
const jobType2 = 'handler2';

describe('Running tasqueue', () => {
    const tasqueue = new Tasqueue({
        pollDelay: 100
    });
    let nHandlers = 0;
    let polling = false;

    tasqueue.on('handler:registered', (h) => {
        nHandlers++;
    })
    .on('client:polling', (d) => {
        polling = true;
    });

    const handler1 = {
        type: jobType1,
        exec() {
            return Promise.delay(200)
            .then(() => {
                throw new Error('job should fail');
            });
        }
    };

    const handler2 = {
        type: jobType2,
        exec() {
            return Promise.delay(200);
        }
    };

    it('should initialize the connection to disque', () => {
        return tasqueue.init();
    });

    it('should register some job handlers', () => {
        tasqueue.registerHandler(handler1);
        tasqueue.registerHandler(handler2);

        return Promise.delay(5)
        .then(() => {
            if (nHandlers !== 2) {
                throw new Error('handlers should be registered properly');
            }
        });
    });

    it('should push some jobs', () => {
        return Promise(_.range(TO_PUSH))
        .eachSeries((n, i) => {
            const jobType = (i % 2 === 0) ? jobType1 : jobType2;
            return tasqueue.pushJob(jobType);
        });
    });

    it('should start polling', () => {
        tasqueue.poll();

        return Promise.delay(5)
        .then(() => {
            if (!polling) {
                throw new Error('client should start polling');
            }
        });
    });

    it('should be processing jobs', () => {
        return Promise.delay(100)
        .then(() => tasqueue.countActive())
        .then((countActive) => {
            if (!countActive) {
                throw new Error('some jobs should be processing');
            }
        });
    });

    it('should have processed all jobs after some time', () => {
        return Promise.delay(3000)
        .then(() => {
            return Promise.all([
                tasqueue.countFailed(),
                tasqueue.countCompleted()
            ]);
        })
        .spread((countFailed, countCompleted) => {
            if (countFailed !== (TO_PUSH / 2)) {
                throw new Error('half of the pushed jobs should have failed');
            }
            if (countCompleted !== (TO_PUSH / 2)) {
                throw new Error('half of the pushed jobs should have succeeded');
            }
        });
    });

    it('should clean all jobs before shuting down', () => {
        return tasqueue.listFailed()
        .then((res) => {
            return Promise(res.list)
            .eachSeries(job => job.delete());
        })
        .then(() => tasqueue.listCompleted())
        .then((res) => {
            return Promise(res.list)
            .eachSeries(job => job.delete());
        });
    });

    it('should end the connection to disque', () => {
        return tasqueue.shutdown(1000, () => {});
    });
});
