# b-task

Simple library for processing messages between microservices.

## Installation

```plaintext
npm install @fazpi-ai/b-task
```

## Basic Usage

### Import
```javascript
import { Queue, Worker } from '@fazpi-ai/b-task';
```

### Initialize a Queue

```javascript
const queue = new Queue('my-queue', {
  redis: {
    host: 'localhost',
    port: 6379
  }
});

// Publish a job
await queue.add(
  // Job data
  {
    orderId: '12345',
    items: ['item1', 'item2'],
    partitionKey: 'user-123' // Partition is specified in the data
  }
);
```

### Initialize a Worker

```javascript
// Handler receives the job data and metadata
const handler = async (data, jobInfo) => {
  console.log('Processing:', data);
  console.log('Job ID:', jobInfo.id);
  console.log('Attempt:', jobInfo.attempts);
  console.log('Partition:', jobInfo.partitionKey);
  
  // If an error occurs, throw it and the worker will handle retries
  // throw new Error('Error processing job');
  
  // Return processing result
  return { processed: true };
};

const worker = new Worker(
  'my-queue',   // queue name
  handler,      // job processing function
  {
    concurrency: 1,     // number of jobs processed simultaneously
    maxRetries: 3,      // maximum number of retries
    backoffDelay: 1000  // initial delay in ms for retries
  }
);

// Start the worker
await worker.start();
```

## Features

* Asynchronous message processing
* Partition support (FIFO per partition)
* Automatic retries with exponential backoff
* Job status monitoring
* Redis-based for high availability

## Advanced Configuration

### Queue Options
```javascript
const queueOptions = {
  redis: {
    host: 'localhost',
    port: 6379,
    password: 'optional',
    db: 0
  }
};

const queue = new Queue('my-queue', queueOptions);
```

### Worker Options
```javascript
const workerOptions = {
  concurrency: 1,      // number of jobs processed simultaneously
  maxRetries: 3,       // maximum number of retries
  backoffDelay: 1000   // initial delay in ms for retries
};

const worker = new Worker('my-queue', handler, workerOptions);
```

## Examples

### Example with Partitions
```javascript
// Producer
const queue = new Queue('orders');

// Partition is specified in the job data
await queue.add({
  orderId: '12345',
  items: ['item1', 'item2'],
  partitionKey: 'user-123' // ensures FIFO order for this user
});

// Consumer
const worker = new Worker(
  'orders',
  async (data, jobInfo) => {
    const { orderId, items, partitionKey } = data;
    console.log(`Processing order ${orderId} from partition ${partitionKey}`);
    await processOrder(orderId, items);
    return { success: true };
  }
);

await worker.start();
```

## Contributing

Contributions are welcome. Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)