// var fs = require('fs');
var Q = require('q');
var Tasqueue = require('../index');

var tasqueue = new Tasqueue();

// var helloHandler = {
//     concurrency: 10000,
//     type: 'say:hello',
//     exec: function(payload) {
//         return Q.nfcall(fs.appendFile, 'hello.txt', 'Hello, '+payload.name+'\n', 'utf8');
//     }
// };

// var hiHandler = {
//     concurrency: 10000,
//     type: 'say:hi',
//     exec: function(payload) {
//         return Q.nfcall(fs.appendFile, 'hi.txt', 'Hi, '+payload.name+'\n', 'utf8');
//     }
// };

var random = {
    concurrency: 5,
    type: 'job:random',
    exec: function(payload) {
        var d = Q.defer();

        setTimeout(function() {
            var n = Math.random();
            if (n < 0.5) d.resolve();
            else d.reject(new Error('wrong number'));
        }, 1);

        return d.promise;
    }
};

var count = 0;

tasqueue.on('client:connected', function() {
    console.log('connected to client');
})
.on('handler:register', function(type) {
    console.log('registered handler for '+type);
})
.on('client:delaying', function(delay) {
    // console.log('delay polling by '+delay+' ms');
})
.on('client:noworkers', function() {
    // console.log('no workers available...');
})
.on('client:polling', function(nbTypes, availableWorkers, totalWorkers) {
    // console.log('polling '+nbTypes+' types with '+availableWorkers+'/'+totalWorkers+' available workers');
})
.on('job:nohandler', function(jobId, jobType) {
    console.log('no registered handler for job '+jobId+' of type '+jobType);
})
.on('job:push', function(jobId, jobType) {
    // console.log('pushed job of type '+jobType+' with id '+jobId);
})
.on('error:push', function(jobType, err) {
    console.log('error pushing job of type '+jobType);
    console.log(err.message);
    console.log(err.stack);
})
.on('job:requeue', function(jobId, jobType) {
    // console.log('requeueing job '+jobId+': no available workers for type '+jobType);
})
.on('job:start', function(jobId, jobType) {
    // console.log('starting job '+jobId+' of type '+jobType);
})
.on('job:error', function(jobId, jobType, err) {
    console.log('error with job '+jobId+' of type '+jobType);
    console.log(err.message);
    console.log(err.stack);
})
.on('job:success', function(jobId, jobType) {
    count++;
    if (count % 1000 === 0) listAll(count);
    // console.log('finished job '+jobId);
})
.on('job:fail', function(jobId, jobType, err) {
    count++;
    if (count % 1000 === 0) listAll(count);
    // console.log('job '+jobId+' failed');
    // console.log(err.message);
    // console.log(err.stack);
});

// tasqueue.registerHandler(helloHandler);
// tasqueue.registerHandler(hiHandler);
tasqueue.registerHandler(random);

for (var i = 0; i < 50000; i++) {
    // tasqueue.push('say:hello', { name: 'world' });
    // tasqueue.push('say:hi', { name: 'world' });
    tasqueue.push('job:random');
}

function listAll(count) {
    Q.all([
        tasqueue.countCompleted(),
        tasqueue.countFailed(),
        tasqueue.countQueued(),
        tasqueue.countActive()
    ])
    .spread(function(completed, failed, queued, active) {
        console.log('**********');
        console.log('Reached: '+count);
        console.log('completed: '+completed);
        console.log('failed: '+failed);
        console.log('queued: '+queued);
        console.log('active: '+active);
    });
}