const Promise = require('q-plus');
const _ = require('lodash');
const Tasqueue = require('../src');

const jobType = 'custom';
const TO_PUSH  = 10;

describe('tasqueue.countQueued()', () => {
    const tasqueue = new Tasqueue();

    it('should get the count of queued jobs', () => {
        return tasqueue.init()
        .then(() => {
            return Promise(_.range(TO_PUSH)).eachSeries(() => {
                return tasqueue.pushJob(jobType);
            });
        })
        .then(() => {
            return tasqueue.countQueued();
        })
        .then((count) => {
            if (count !== TO_PUSH) throw new Error('The number of queued jobs should be ' + TO_PUSH);
        })
        .fin(() => {
            return tasqueue.shutdown(1000, () => {});
        });
    });
});

describe('tasqueue.listQueued()', () => {
    const tasqueue = new Tasqueue();

    it('should get the list of queued jobs', () => {
        return tasqueue.init()
        .then(() => {
            return tasqueue.listQueued({ limit: TO_PUSH });
        })
        .then((res) => {
            const jobs     = res.list;
            const firstJob = jobs[0].details();

            if (jobs.length !== TO_PUSH) throw new Error('The number of listed jobs should be ' + TO_PUSH);
            if (firstJob.type !== jobType) throw new Error('Listed jobs should be of type ' + jobType);

            return Promise(jobs).eachSeries((job) => {
                return job.delete();
            });
        })
        .fin(() => {
            return tasqueue.shutdown(1000, () => {});
        });
    });
});
