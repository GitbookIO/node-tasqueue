var fs = require('fs');
var Jobber = require('./index');

var jobber = new Jobber();

var helloHandler = {
    concurrency: 10000,
    type: 'say:hello',
    exec: function(payload) {
        fs.appendFile('hello.txt', 'Hello, '+payload.name+'\n', 'utf8', function(err) {
            if (err) throw err;
        });
    }
};

var hiHandler = {
    concurrency: 10000,
    type: 'say:hi',
    exec: function(payload) {
        fs.appendFile('hi.txt', 'Hi, '+payload.name+'\n', 'utf8', function(err) {
            if (err) throw err;
        });
    }
};

jobber.registerHandler(helloHandler);
jobber.registerHandler(hiHandler);

for (var i = 0; i < 50000; i++) {
    jobber.pushJob('say:hello', { name: 'world' });
    jobber.pushJob('say:hi', { name: 'world' });
}
