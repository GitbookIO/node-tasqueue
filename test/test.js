// var fs = require('fs');
var Q = require('q');
var Tasqueue = require('../index');

var tasqueue = new Tasqueue({
    host: 'localhost',
    port: 7711
});

var random = {
    concurrency: 5,
    maxAttemps: 3,
    type: 'job:random',
    exec: function(payload) {
        var d = Q.defer();

        setTimeout(function() {
            var n = Math.random();
            if (n < 0.1) d.resolve();
            else d.reject(new Error('wrong number'));
        }, 3000);

        return d.promise;
    }
};

var count = 0;
var countPushed = 0;
var id = null;
var print = false;

tasqueue.on('client:connected', function() {
    console.log('connected to client');
})
.on('handler:register', function(handler) {
    console.log('registered handler for '+handler.type);
})
.on('client:delaying', function(delay) {
    console.log('delay polling by '+delay.delay+' ms');
})
.on('client:noworkers', function() {
    console.log('no workers available...');
})
.on('client:polling', function(data) {
    console.log('polling '+data.types+' types with '+data.availableWorkers+'/'+data.totalWorkers+' available workers');
})
.on('job:nohandler', function(job) {
    console.log('no registered handler for job '+job.id+' of type '+job.type);
})
.on('job:push', function(job) {
    if (!id) {
        id = job.id;
        console.log('pushed job of type '+job.type+' with id '+job.id);
    }
})
.on('error:polling', function(err) {
    console.log('error:polling');
    console.log(err);
})
.on('error:push', function(jobType, err) {
    console.log('error pushing job of type '+jobType);
    console.log(err.message);
    console.log(err.stack);
})
.on('job:requeue', function(jobId, jobType) {
    // console.log('requeueing job '+jobId+': no available workers for type '+jobType);
})
.on('job:start', function(job) {
    // console.log('starting job '+job.id+' of type '+job.type);
    if (job.id === id) {
        console.log('starting job '+job.id+' of type '+job.type);
        tasqueue.getJob(id)
        .then(function(_job) {
            console.log('Details for '+_job.id);
            console.log(_job.details());
        });
    }
})
.on('job:error', function(jobId, jobType, err) {
    console.log('error with job '+jobId+' of type '+jobType);
    console.log(err.message);
    console.log(err.stack);
})
.on('job:success', function(job) {
    count++;
    console.log('count: '+count);
    if (count % 100 === 0) listAll(count);
    if (job.id === id) {
        console.log('job '+job.id+' was successful');
        tasqueue.getJob(id)
        .then(function(_job) {
            console.log('Details for '+_job.id);
            console.log(_job.details());
        });
    }
})
.on('job:fail', function(job) {
    count++;
    if (count % 100 === 0) listAll(count);
    if (job.id === id) {
        console.log('job '+job.id+' failed');
        tasqueue.getJob(id)
        .then(function(_job) {
            console.log('Details for '+_job.id);
            console.log(_job.details());
        });
    }
});

tasqueue.init()
.then(function() {
    // Register handler for job:random
    tasqueue.registerHandler(random);

    // Launch polling
    tasqueue.poll();

    // Push list of jobs
    for (var i = 0; i < 20; i++) {
        tasqueue.pushJob('job:random');
//        .then(function() {
//            countPushed++;
//            if (countPushed % 10000 === 0) console.log('pushed: '+countPushed);
//            if (!!id && !print) {
//                print = true;
//                tasqueue.getJob(id)
//                .then(function(job) {
//                    console.log('Details for '+job.id);
//                    console.log(job.details());
//                });
//            }
//        });
    }
}, function(err) {
    console.log(err);
    tasqueue.shutdown(1000, function() {
        console.log('Tasqueue was shutdown');
    });
});

setInterval(function() {
    tasqueue.listActive({ start: 2, limit: 2 })
    .then(function(activeList) {
        console.log('**********');
        console.log('***** active list: ');
        console.log('***** prev: '+activeList.prev);
        console.log('***** next: '+activeList.next);
        activeList.list.map(function(job) { console.log(job.details()); });
        console.log('**********');
    });
    tasqueue.countActive()
    .then(function(activeCount) {
        console.log('**********');
        console.log('***** active count: ');
        console.log(activeCount);
        console.log('**********');
    });
}, 5000);

function listAll(count) {
    Q.all([
        tasqueue.countCompleted(),
        tasqueue.countFailed(),
        tasqueue.countQueued(),
        tasqueue.countActive(),
        tasqueue.listActive()
    ])
    .spread(function(completed, failed, queued, active, activeList) {
        console.log('**********');
        console.log('Reached: '+count);
        console.log('completed: '+completed);
        console.log('failed: '+failed);
        console.log('queued: '+queued);
        console.log('active: '+active);
        console.log('**********');
    });
}
