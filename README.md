# tasqueue

[![npm version](https://badge.fury.io/js/tasqueue.svg)](https://badge.fury.io/js/tasqueue)
[![Build Status](https://travis-ci.org/GitbookIO/node-tasqueue.svg?branch=master)](https://travis-ci.org/GitbookIO/node-tasqueue)

Promise-based Node.js job/task-queue library using disque.

### How it works

1. Create a client
2. Register new job types handlers
3. Push jobs

Tasqueue is a job/task-queue library based on [disque](https://www.github.com/antirez/disque) and using [Q](https://github.com/kriskowal/q). It aims to be simple, fast and to handle a high charge.

Monitoring functions of Tasqueue can only be entrusted when using a single-node instance of disque.

### Create a client
```js
var Tasqueue = require('tasqueue');

// Default options
var opts = {
    authPass:      null,            // AUTH password for disque-server
    host:          'localhost',     // disque-server host
    port:          7711,            // disque-server port
    pollDelay:     1000 * 15,       // Polling delay in ms when no workers are available
    jobTimeout:    1000 * 60 * 60,  // Timeout in ms before a job is considered as failed
    failedTTL:     60 * 60 * 24,    // Failed jobs TTL in sec
    completedTTL:  60 * 60 * 24,    // Completed jobs TTL in sec
    queuedTTL:     60 * 60 * 24,    // Queued jobs TTL in sec
    activeTTL:     60 * 60 * 1,     // Active job TTL in sec
    maxAttempts:   60,              // Max reconnection attempts
    retryMaxDelay: 1000 * 60        // Prevent exponential reconnection delay
};

var tasqueue = new Tasqueue(opts);
```

## Queue API

### `tasqueue.init()`
_**Async**_:
Initialize the client.

###### Example
```js
tasqueue.init()
.then(function() {
    // Start working
}, function(err) {
    // Connection to disque-server failed
});
```

### `tasqueue.shutdown(timeoutMs, callback)`
_**Async**_:
End the client.

###### Example
```js
tasqueue.init()
.then(function() {
    // ...
    tasqueue.shutdown(1000, function() {
        console.log('Tasqueue was shut down after at most 1000 ms.');
    });
});
```

### `tasqueue.poll()`
Start polling and jobs execution. This function should be run only once.

###### Example
```js
tasqueue.init()
.then(function() {
    tasqueue.poll();
});
```

### `tasqueue.registerHandler(handler)`
Register a job handler. `handler` should have the following properties:

```js
var handler = {
  type: 'jobType', // {String}  will be used as the queue name
  concurrency: 5,  // {Integer} max number of concurrent workers for this type, default = 1
  maxAttempts: 5,  // {Integer} max number of retry for this job type, default = 1
  exec: function(body) {
    // do whatever using the body passed for this job
  }
};
```

### `tasqueue.listHandlers()`
List of registered handlers types as an array.

###### Example
```js
var handler1 = { type: 'type:1', ... };
var handler2 = { type: 'type:2', ... };

tasqueue.registerHandler(handler1);
tasqueue.registerHandler(handler2);
var registeredHandlers = tasqueue.listHandlers();
// registeredHandlers equals ['type:1', 'type:2']
```

### `tasqueue.pushJob(jobType, body)`
_**Async**_:
Push a new job that will be processed by the corresponding `jobType` handler. The worker will call the handler's `exec` function with `body` used as its argument.

When successful, returns the added job id.

###### Example
```js
var handler1 = {
    type: 'type:1',
    exec: function(body) {
        console.log('hello '+body.name);
    }
};

tasqueue.pushJob('type:1', { name: 'Johan' })
.then(function(jobId) {
    // jobId will be a disque id
});

// After some time...
// Logs 'hello Johan'
```

### `tasqueue.getJob(id)`
_**Async**_:
Returns a Job object that can be easily manipulated. You can find the API for Jobs a bit below.

The promise is rejected if the queried job doesn't exist.

###### Example
```js
tasqueue.getJob('someDisqueId')
.then(function(job) {
    console.log(job.details());
});
```

### Count jobs
_**Async**_:
Returns the count of jobs by state.

#### `tasqueue.count(state)`
`state` must be one of `['active', 'queued', 'completed', 'failed']`.
#### `tasqueue.countActive()`
#### `tasqueue.countQueued()`
#### `tasqueue.countCompleted()`
#### `tasqueue.countFailed()`

### List jobs
_**Async**_:
Returns the list of jobs for each state and cursors to paginate through the jobs.

###### Example
```js
var opts = {
    start: 10,  // Start/skip cursor
    limit: 10   // Number of jobs to return
};

tasqueue.listActive(opts)
.then(function(res) {
    // res looks like
    {
        prev: 0,       // Cursor to get the previous 10 jobs or null
        next: null,    // Cursor to get the next 10 jobs or null
        list: [ ... ]  // List of Jobs objects
    }
});
```

#### `tasqueue.list(state)`
`state` must be one of `['active', 'queued', 'completed', 'failed']`.
#### `tasqueue.listActive()`
#### `tasqueue.listQueued()`
#### `tasqueue.listCompleted()`
#### `tasqueue.listFailed()`

## Jobs API

### `job.details()`
Get the job's informations in a pretty form.

###### Example
```js
tasqueue.getJob('someId')
.then(function(job) {
    console.log(job.details());
    {
        id:         {String},
        type:       {String},
        body:       {Object},
        state:      {String} - one of ['queued', 'active', 'completed', 'failed']
        created:    {Date},
        ended:      {Date},
        attempt:    {Number} - Attempt at which the job failed/completed,
        duration:   {Number} - in ms,
        result:     {Object} - anything returned by the exec function on success,
        error:      {Error} - details about why the job failed
    }
});
```

### `job.cancel()`
_**Async**_:
Cancels the job and set it as failed.

Only queued jobs may be cancelled. The promise is rejected if the job is not in the `queued` state.

### `job.delete()`
_**Async**_:
Utterly delete a job, whichever its state is.

### Events
Tasqueue inherits the Node.js `EventEmitter` class. Below is the list of all events emitted by tasqueue during execution:

#### Client

###### Client connection
```js
emit('client:connected', {
    // disque-server informations client is connected to
    host: {String},
    port: {Number}
});
```

###### Client closed
```js
emit('client:closed');
```

#### Queue execution

###### Polling jobs
```js
emit('client:polling', {
    types:            {Number}, // Number of available job types that can be processed by this poll
    availableWorkers: {Number}, // Total number of available workers for these types
    totalWorkers:     {Number}  // Total number of workers registered
});
```

###### Polling delayed
```js
emit('client:delaying', {
    delay: {Number} - tasqueue instance configured/default poll delay
});
```

###### No worker available
```js
emit('client:no-workers')
```

###### Error while polling
```js
emit('error:polling', error);
```

#### Jobs

###### Job started
```js
emit('job:started', {
    id:   {String}, // The job id
    type: {String}  // The job type
});
```

###### Job successfully pushed
```js
emit('job:pushed', {
    id:   {String}, // The job id
    type: {String}  // The job type
});
```

###### Job successfully canceled
```js
emit('job:canceled', {
    id:   {String}, // The job id
    type: {String}  // The job type
});
```

###### Job successfully deleted
```js
emit('job:deleted', {
    id:   {String}, // The job id
    type: {String}  // The job type
});
```

###### Job re-queued after failure
```js
emit('job:requeued', {
    id:      {String}, // The job id
    type:    {String}, // The job type
    attempt: {Number}  // The last failed attempt for this job
});
```

###### Job passed
```js
emit('job:success', {
    id:   {String}, // The job id
    type: {String}  // The job type
});
```

###### Job failed
```js
emit('error:job-failed', error, {
    id:    {String}, // The job id
    type:  {String}  // The job type
});
```

###### Error canceling a job
```js
emit('error:job-cancel', error, {
    id:   {String}, // The job id
    type: {String}  // The job type
});
```

###### No handler registered for a job
```js
emit('error:no-handler', error, {
    id:   {String}, // The job id
    type: {String}  // The job type
});
```

#### Handlers

###### Handler successfully registered
```js
emit('handler:registered', {
    handler.type
});
```

###### Error: handler already exists
```js
emit('error:existing-handler', error, {
    type: handler.type
});
```
