const Promise = require('q-plus');
const _ = require('lodash');
const Tasqueue = require('../src');

const TO_PUSH = 5;
const jobType1 = 'handler1';
const jobType2 = 'handler2';

describe('Concurrency', () => {
    const tasqueue  = new Tasqueue({
        pollDelay: 100
    });

    const handler1 = { type: jobType1, exec() {
        return Promise.delay(2000);
    }};
    const handler2 = { type: jobType2, concurrency: TO_PUSH, exec() {
        return Promise.delay(200);
    }};

    tasqueue.registerHandler(handler1);
    tasqueue.registerHandler(handler2);

    it('should initialize the connection to disque', () => {
        return tasqueue.init();
    });

    it('should push a blocking job', () => {
        return tasqueue.pushJob(jobType1);
    });

    it('should push some other jobs', () => {
        return Promise(_.range(TO_PUSH)).eachSeries(() => {
            return tasqueue.pushJob(jobType2);
        });
    });

    it('should start polling', () => {
        tasqueue.poll();
    });

    it('should be processing all jobs', () => {
        return Promise.delay(100)
        .then(() => {
            return tasqueue.countActive();
        })
        .then((countActive) => {
            if (countActive !== (TO_PUSH + 1)) throw new Error('all jobs should be processing');
            return Promise();
        });
    });

    it('should have processed all jobs after some time', () => {
        return Promise.delay(3000)
        .then(() => {
            return tasqueue.countCompleted();
        })
        .then((countCompleted) => {
            if (countCompleted !== (TO_PUSH + 1)) throw new Error('all pushed jobs should have succeeded');
            return Promise();
        });
    });

    it('should clean all jobs before shuting down', () => {
        return tasqueue.listFailed()
        .then((res) => {
            return Promise(res.list).eachSeries((job) => {
                return job.delete();
            });
        })
        .then(() => {
            return tasqueue.listCompleted();
        })
        .then((res) => {
            return Promise(res.list).eachSeries((job) => {
                return job.delete();
            });
        });
    });

    it('should end the connection to disque', () => {
        return tasqueue.shutdown(1000, () => {});
    });
});
