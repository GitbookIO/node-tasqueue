# tasqueue

Node.js job/task-queue library using disque.

### How it works

1. Create a client
2. Register new job types handlers
3. Push jobs

Tasqueue is a job/task-queue library based on [disque](https://www.github.com/antirez/disque). It aims to be simple, fast and to handle a high charge.

### API

#### Create a client

This creates a new client and initializes the connection to `disque-server`. The client starts working right after the connection is established.
```JavaScript
var Tasqueue = require('tasqueue');
var opts = {
  port: 7711,               // disque-server instance port
  pollDelay: 15*1000,       // Polling delay in ms when no workers are available
  jobTimeout: 60*60*1000,   // Timeout in ms before a job is considered as failed
  failedTTL: 3*24*60*60,    // Failed jobs TTL in secs
  completedTTL: 3*24*60*60  // Completed jobs TTL in secs
};
var tasqueue = new Tasqueue(opts);
```

###### *Events*
`emit('client:connected')` as soon as the client is connected to the `disque-server` instance.

#### Register a job handler
`tasqueue.registerHandler(handler)`

`handler` should have the following properties:

```JavaScript
var handler = {
  type: 'jobType', // {String}  will be used as the queue name
  concurrency: 5,  // {Integer} max number of concurrent workers for this type
  maxAttempts: 5,  // {Integer} max number of retry for this job type, default = 1
  exec: function(payload) {}
};
```

###### *Events*
`emit('handler:register', handler.type)` as soon as the handler as been added to the client.

#### List handled types
`tasqueue.listTypes()`

#### Push a new job
`tasqueue.push(jobType, payload)`

Push a new job that will be processed by the corresponding `jobType` handler. The handler's `exec` function is called with `payload` used as its argument.

When successful, returns the added job id.

###### *Events*
`emit('job:push', jobId, jobType)` as soon as the new job has been queued by the `disque-server` instance.

#### List/Count all jobs by status
`tasqueue.listActive()`, `client.countActive()`

`tasqueue.listQueued()`, `client.countQueued()`

`tasqueue.listCompleted()`, `client.countCompleted()`

`tasqueue.listFailed()`, `client.countFailed()`

#### Get details about a specific job
```JavaScript
tasqueue.details(jobId)
.then(function(job) {
  job === {
    id: 'D-14696a69-3x3GhrqkZEqjIwwuLiDnZ+LQ-05a1',
    type: 'jobType',
    body: { ... },
    queue: 'queue',
    state: 'queued',
    repl: 1,
    ttl: 86400,
    ctime: 1455843565980000000,
    delay: 0,
    retry: 300,
    nacks: 0,
    'additional-deliveries': 0,
    'nodes-delivered': [ '14696a6963e70cf719a214f48b806ba4fdad3d6f' ],
    'nodes-confirmed': [],
    'next-requeue-within': 299674,
    'next-awake-within': 299174
  };
});
```

#### Cancel a job
`tasqueue.cancel(jobId)`

#### Delete a job
`tasqueue.delete(jobId)`

###### *Events*
`emit('job:delete', jobId)` as soon as the job has been deleted by the `disque-server` instance.

#### Shutdown
`tasqueue.shutdown(maxWaitTimeInMs, callback)`