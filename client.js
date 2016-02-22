var disque = require('thunk-disque');

var client = disque.createClient(7711, { usePromise: true });

var jobId;

client.addjob('testqueue', JSON.stringify({ type: 'job', content: 35 }), 0)
.then(function(res) {
    jobId = res;
    return client.qlen('testqueue');
})
.then(function(res) {
    console.log('qlen');
    console.log(res);
    return client.show(jobId);
})
.then(function(res) {
    console.log('show');
    console.log(res);
    return client.jscan('QUEUE', 'testqueue', 'REPLY', 'all');
})
.then(function(jobs) {
    console.log('then called');
    console.log(jobs[1].map(mapScan));
    return client.deljob(jobId);
})
.then(function(res) {
    console.log(res);
})
.catch(function(err) {
    console.log('err '+err);
});

function mapScan(res) {
    var job = {};
    for (var i = 0; i < res.length; i+=2) {
        job[res[i]] = res[i+1];
    }
    job.body = JSON.parse(job.body);
    return job;
}