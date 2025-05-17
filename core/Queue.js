import Redis from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import { loadScripts } from './utils/index.js';

class Queue {
    constructor(name, options = {}) {
        this.name = name;
        this.redis = new Redis(options.redis || {
            host: 'localhost',
            port: 6379,
            maxRetriesPerRequest: null
        });

        // Redis base keys
        this.keys = {
            queue: `queue:${name}`,
            jobs: `queue:${name}:jobs`,
            partitions: `queue:${name}:partitions`, // List of partitions
            waiting: `queue:${name}:waiting`,       // Global waiting list
            active: `queue:${name}:active`,
            completed: `queue:${name}:completed`,
            failed: `queue:${name}:failed`
        };
    }

    // Get the partition key
    getPartitionKey(partition) {
        return `${this.keys.waiting}:${partition}`;
    }

    async init() {
        // Load Lua scripts
        const scripts = await loadScripts();
        this.scripts = {};

        for (const [name, script] of Object.entries(scripts)) {
            this.scripts[name] = this.redis.defineCommand(name, {
                numberOfKeys: name === 'drain' ? 5 : 4,
                lua: script
            });
        }

        // Initialize queue if it doesn't exist
        await this.redis.hmset(this.keys.queue, {
            waiting: 0,
            active: 0,
            completed: 0,
            failed: 0
        });
    }

    async add(data, options = {}) {
        const jobId = uuidv4();
        const timestamp = Date.now();
        const partitionKey = options.partitionKey || 'default';

        const jobData = {
            id: jobId,
            data,
            timestamp,
            attempts: 0,
            maxRetries: options.maxRetries || 3,
            status: 'waiting',
            partitionKey
        };

        // Pipeline for atomic operations
        const pipeline = this.redis.pipeline();

        // Save job data
        pipeline.hmset(`${this.keys.jobs}:${jobId}`, {
            ...jobData,
            data: JSON.stringify(data)
        });

        // Add to the specific partition queue
        const partitionQueueKey = this.getPartitionKey(partitionKey);
        pipeline.zadd(partitionQueueKey, timestamp, jobId);
        
        // Register the partition if it's new
        pipeline.sadd(this.keys.partitions, partitionKey);
        
        // Increment the global counter
        pipeline.hincrby(this.keys.queue, 'waiting', 1);

        await pipeline.exec();
        return jobData;
    }

    async getJobById(jobId) {
        const jobData = await this.redis.hgetall(`${this.keys.jobs}:${jobId}`);
        if (!jobData || Object.keys(jobData).length === 0) return null;

        try {
            return {
                ...jobData,
                data: JSON.parse(jobData.data)
            };
        } catch (error) {
            console.error('Error deserializing job data:', error);
            return null;
        }
    }

    async getNextJobFromPartition(partition) {
        const partitionQueueKey = this.getPartitionKey(partition);
        const waiting = await this.redis.zrange(partitionQueueKey, 0, 0, 'WITHSCORES');
        
        if (!waiting.length) return null;

        const [jobId, score] = waiting;
        const jobData = await this.getJobById(jobId);
        if (!jobData) return null;

        // Check if the job is ready to be processed
        const now = Date.now();
        if (parseInt(score) > now) {
            return null;
        }

        // Move the job to the active queue (using pipeline for atomic operation)
        const pipeline = this.redis.pipeline();
        pipeline.zrem(partitionQueueKey, jobId);
        pipeline.zadd(this.keys.active, now, jobId);
        pipeline.hincrby(this.keys.queue, 'waiting', -1);
        pipeline.hincrby(this.keys.queue, 'active', 1);
        await pipeline.exec();

        return jobData;
    }

    async getPartitions() {
        return await this.redis.smembers(this.keys.partitions);
    }

    async getQueueStatus() {
        const [status, partitions] = await Promise.all([
            this.redis.hgetall(this.keys.queue),
            this.getPartitions()
        ]);

        const partitionStats = {};
        for (const partition of partitions) {
            const count = await this.redis.zcard(this.getPartitionKey(partition));
            partitionStats[partition] = count;
        }

        return {
            global: {
                waiting: parseInt(status.waiting) || 0,
                active: parseInt(status.active) || 0,
                completed: parseInt(status.completed) || 0,
                failed: parseInt(status.failed) || 0
            },
            partitions: partitionStats
        };
    }

    async close() {
        await this.redis.quit();
    }

    async debug() {
        const status = await this.getQueueStatus();
        console.log('Global queue status:', status.global);
        console.log('Partition status:', status.partitions);

        const partitions = await this.getPartitions();
        for (const partition of partitions) {
            const waiting = await this.redis.zrange(
                this.getPartitionKey(partition),
                0, -1,
                'WITHSCORES'
            );
            console.log(`\nJobs in partition ${partition}:`, waiting);
        }

        console.log('\nActive jobs:', 
            await this.redis.zrange(this.keys.active, 0, -1, 'WITHSCORES'));
        console.log('Failed jobs:', 
            await this.redis.zrange(this.keys.failed, 0, -1, 'WITHSCORES'));
    }

    async drain() {
        const partitions = await this.getPartitions();
        const pipeline = this.redis.pipeline();

        // Clean all partitions
        for (const partition of partitions) {
            pipeline.del(this.getPartitionKey(partition));
        }

        // Clean global lists
        pipeline.del(this.keys.queue);
        pipeline.del(this.keys.jobs);
        pipeline.del(this.keys.partitions);
        pipeline.del(this.keys.active);
        pipeline.del(this.keys.completed);
        pipeline.del(this.keys.failed);

        await pipeline.exec();

        // Reinitialize counters
        await this.init();
    }
}

export default Queue;