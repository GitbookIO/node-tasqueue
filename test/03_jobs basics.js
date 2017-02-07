const Promise = require('q');
const Tasqueue = require('../src');

const jobType = 'custom';
let jobId = null;

describe('tasqueue.pushJob()', () => {
    const tasqueue = new Tasqueue();
    let pushed = 0;

    tasqueue.on('job:pushed', (job) => {
        if (job.type === jobType) {
            pushed++;
        }
    });

    it('should push new jobs and emit job:pushed', () => {
        return tasqueue.init()
        .then(() => tasqueue.pushJob(jobType))
        .then((_jobId) => {
            jobId = _jobId;
            return Promise.delay(5);
        })
        .then(() => {
            if (!pushed) {
                throw new Error('job:pushed should have been fired by now');
            }
        })
        .fin(() => tasqueue.shutdown(1000, () => {}));
    });
});

describe('tasqueue.getJob()', () => {
    const tasqueue = new Tasqueue();

    it('should retrieve a job', () => {
        return tasqueue.init()
        .then(() => tasqueue.getJob(jobId))
        .then((job) => {
            const jobInfos = job.details();

            if (jobInfos.type !== jobType) {
                throw new Error(`job should be of type ${jobType}`);
            }
        })
        .fin(() => tasqueue.shutdown(1000, () => {}));
    });
});

describe('job.cancel()', () => {
    const tasqueue = new Tasqueue();
    let canceled = false;

    tasqueue.on('job:canceled', (job) => {
        if (job.id === jobId) {
            canceled = true;
        }
    });

    it('should cancel a job and emit job:canceled', () => {
        return tasqueue.init()
        .then(() => tasqueue.getJob(jobId))
        .then(job => job.cancel())
        .then(() => Promise.delay(5))
        .then(() => {
            if (!canceled) {
                throw new Error('job:canceled should have been fired by now');
            }
        })
        .then(() => tasqueue.getJob(jobId))
        .then(
            () => {
                throw new Error(`job ${jobId} should have been canceled`);
            },
            () => Promise()
        )
        .fin(() => tasqueue.shutdown(1000, () => {}));
    });
});

describe('tasqueue.countFailed()', () => {
    const tasqueue = new Tasqueue();

    it('should return the number of failed jobs', () => {
        return tasqueue.init()
        .then(() => tasqueue.countFailed())
        .then((countFailed) => {
            if (!countFailed === 1) {
                throw new Error('a canceled job should be marked as failed');
            }

            return Promise();
        })
        .fin(() => tasqueue.shutdown(1000, () => {}));
    });
});

describe('tasqueue.listFailed()', () => {
    const tasqueue = new Tasqueue();

    it('should return the list of failed jobs', () => {
        return tasqueue.init()
        .then(() => tasqueue.listFailed())
        .then((res) => {
            if (!res.list) {
                throw new Error('#listFailed() result should have a list property');
            }
            if (!res.list.length) {
                throw new Error('canceled job should be in the #listFailed() result list');
            }

            const job = res.list[0].details();
            jobId = job.id;
        })
        .fin(() => tasqueue.shutdown(1000, () => {}));
    });
});

describe('job.delete()', () => {
    const tasqueue = new Tasqueue();
    let deleted  = false;

    tasqueue.on('job:deleted', (job) => {
        if (job.id === jobId) {
            deleted = true;
        }
    });

    it('should delete a job and emit job:deleted', () => {
        return tasqueue.init()
        .then(() => tasqueue.getJob(jobId))
        .then(job => job.delete())
        .then(() => Promise.delay(5))
        .then(() => {
            if (!deleted) {
                throw new Error('job:deleted should have been fired by now');
            }
        })
        .then(() => tasqueue.getJob(jobId))
        .then(
            () => {
                throw new Error(`job ${jobId} should have been deleted`);
            },
            () => Promise()
        )
        .fin(() => tasqueue.shutdown(1000, () => {}));
    });
});
