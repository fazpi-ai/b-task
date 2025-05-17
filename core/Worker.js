import Queue from './Queue.js';

class Worker {
    constructor(queueName, handler, options = {}) {
        this.handler = handler;
        this.options = {
            concurrency: options.concurrency || 1,
            maxRetries: options.maxRetries || 3,
            backoffDelay: options.backoffDelay || 1000,
            ...options
        };

        this.queue = new Queue(queueName, options);
        this.isRunning = false;
        this.activeJobs = new Map(); // Map of active jobs by ID
        this.partitionWorkers = new Map(); // Map of workers by partition
        this.partitionLocks = new Map(); // Map of locks by partition
    }

    async init() {
        await this.queue.init();
    }

    async start() {
        if (this.isRunning) return;
        this.isRunning = true;

        // Start the partition discovery process
        this.startPartitionDiscovery();
    }

    async startPartitionDiscovery() {
        while (this.isRunning) {
            try {
                // Get all partitions
                const partitions = await this.queue.getPartitions();

                // Start workers for new partitions
                for (const partition of partitions) {
                    if (!this.partitionWorkers.has(partition)) {
                        console.log(`New partition discovered: ${partition}`);
                        this.startPartitionWorkers(partition);
                    }
                }

                // Wait before the next check
                await new Promise(resolve => setTimeout(resolve, 1000));
            } catch (error) {
                console.error('Error in partition discovery:', error);
                await new Promise(resolve => setTimeout(resolve, this.options.backoffDelay));
            }
        }
    }

    startPartitionWorkers(partition) {
        // Create workers for the partition according to the concurrency
        const workers = Array(this.options.concurrency).fill(null).map((_, i) => {
            const workerId = `${partition}-worker-${i + 1}`;
            const worker = this.startWorker(workerId, partition).catch(error => {
                console.error(`Error in worker ${workerId}:`, error);
            });
            return worker;
        });

        this.partitionWorkers.set(partition, workers);
    }

    async stop() {
        this.isRunning = false;
        // Wait for active jobs to finish
        await Promise.all(Array.from(this.activeJobs.values()));
        await this.queue.close();
    }

    async startWorker(workerId, partition) {
        console.log(`[${workerId}] Worker started for partition ${partition}`);

        while (this.isRunning) {
            try {
                // Try to get the partition lock
                if (this.partitionLocks.get(partition)) {
                    await new Promise(resolve => setTimeout(resolve, 100));
                    continue;
                }

                // Get the next job from the partition
                const job = await this.queue.getNextJobFromPartition(partition);
                if (!job) {
                    await new Promise(resolve => setTimeout(resolve, 1000));
                    continue;
                }

                // Check if the job is already being processed
                if (this.activeJobs.has(job.id)) {
                    continue;
                }

                // Mark the partition as in use
                this.partitionLocks.set(partition, true);

                const processPromise = this.processJob(job, workerId);
                this.activeJobs.set(job.id, processPromise);

                try {
                    await processPromise;
                } finally {
                    // Release resources
                    this.activeJobs.delete(job.id);
                    this.partitionLocks.set(partition, false);
                }
            } catch (error) {
                console.error(`[${workerId}] Error in the worker:`, error);
                this.partitionLocks.set(partition, false);
                await new Promise(resolve => setTimeout(resolve, this.options.backoffDelay));
            }
        }

        console.log(`[${workerId}] Worker stopped`);
    }

    async processJob(job, workerId) {
        const attempts = parseInt(job.attempts || 0);
        console.log(`[${workerId}] Processing job ${job.id} from partition ${job.partitionKey} (attempt ${attempts + 1}/${job.maxRetries}):`,
            JSON.stringify(job.data, null, 2));

        try {
            const result = await this.handler(job.data, {
                id: job.id,
                timestamp: parseInt(job.timestamp),
                attempts: attempts,
                partitionKey: job.partitionKey
            });

            await this.completeJob(job.id, result, workerId);
            console.log(`[${workerId}] Job ${job.id} completed successfully`);
            return result;
        } catch (error) {
            console.error(`[${workerId}] Error processing job ${job.id}:`, error.message);
            await this.failJob(job.id, error, workerId);
            throw error;
        }
    }

    async completeJob(jobId, result, workerId) {
        const timestamp = Date.now();

        await this.queue.redis.hmset(`${this.queue.keys.jobs}:${jobId}`, {
            result: JSON.stringify(result),
            completedAt: timestamp,
            status: 'completed'
        });

        await this.queue.redis.zrem(this.queue.keys.active, jobId);
        await this.queue.redis.zadd(this.queue.keys.completed, timestamp, jobId);
        await this.queue.redis.hincrby(this.queue.keys.queue, 'active', -1);
        await this.queue.redis.hincrby(this.queue.keys.queue, 'completed', 1);
    }

    async failJob(jobId, error, workerId) {
        const timestamp = Date.now();
        const job = await this.queue.getJobById(jobId);
        const attempts = parseInt(job.attempts || 0) + 1;

        if (attempts < job.maxRetries) {
            const delay = this.options.backoffDelay * Math.pow(2, attempts - 1);
            const nextTry = timestamp + delay;

            console.log(`[${workerId}] Retrying job ${jobId} in ${delay}ms (attempt ${attempts}/${job.maxRetries})`);

            await this.queue.redis.hmset(`${this.queue.keys.jobs}:${jobId}`, {
                attempts,
                lastError: error.message,
                status: 'waiting',
                nextTry
            });

            // Return to the corresponding partition queue
            const partitionQueueKey = this.queue.getPartitionKey(job.partitionKey);
            await this.queue.redis.zrem(this.queue.keys.active, jobId);
            await this.queue.redis.zadd(partitionQueueKey, nextTry, jobId);
            await this.queue.redis.hincrby(this.queue.keys.queue, 'active', -1);
            await this.queue.redis.hincrby(this.queue.keys.queue, 'waiting', 1);
        } else {
            console.log(`[${workerId}] Job ${jobId} failed after ${attempts} attempts`);

            await this.queue.redis.hmset(`${this.queue.keys.jobs}:${jobId}`, {
                attempts,
                lastError: error.message,
                failedAt: timestamp,
                status: 'failed'
            });

            await this.queue.redis.zrem(this.queue.keys.active, jobId);
            await this.queue.redis.zadd(this.queue.keys.failed, timestamp, jobId);
            await this.queue.redis.hincrby(this.queue.keys.queue, 'active', -1);
            await this.queue.redis.hincrby(this.queue.keys.queue, 'failed', 1);
        }
    }
}

export default Worker; 