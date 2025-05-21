import Redis from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import { loadScripts } from '../utils/index.js'; // Asegúrate que la ruta es correcta

class Queue {
    constructor(name, options = {}) {
        this.name = name;
        this.options = {
            redis: { host: 'localhost', port: 6379, maxRetriesPerRequest: null, ...options.redis },
            defaultJobOptions: {
                leaseDuration: 30000, // 30 segundos por defecto para el alquiler del job
                stalledJobRetryDelay: 5000, // 5 segundos de delay antes de reintentar un job estancado
                ...options.defaultJobOptions,
            },
        };
        this.redis = new Redis(this.options.redis); // Comentar sobre pasar más opciones de ioredis aquí

        this.keys = {
            queue: `queue:${name}`,
            jobs: `queue:${name}:jobs`,
            partitions: `queue:${name}:partitions`,
            waitingPrefix: `queue:${name}:waiting`,
            active: `queue:${name}:active`,
            completed: `queue:${name}:completed`,
            failed: `queue:${name}:failed`,
        };
        this.scripts = {};
    }

    getPartitionKey(partition) {
        return `${this.keys.waitingPrefix}:${partition}`;
    }

    async init() {
        const loadedLuaScripts = await loadScripts();

        const scriptDefinitions = {
            addJob: { numberOfKeys: 4, lua: loadedLuaScripts.addJob },
            getNextJob: { numberOfKeys: 4, lua: loadedLuaScripts.getNextJob },
            completeJob: { numberOfKeys: 4, lua: loadedLuaScripts.completeJob },
            handleFailureLua: { numberOfKeys: 5, lua: loadedLuaScripts.handleFailure }, // Archivo: handleFailure.lua
            renewJobLease: { numberOfKeys: 2, lua: loadedLuaScripts.renewJobLease },
            requeueStalledJob: { numberOfKeys: 5, lua: loadedLuaScripts.requeueStalledJob },
            drain: { numberOfKeys: 7, lua: loadedLuaScripts.drain },
        };

        for (const [name, def] of Object.entries(scriptDefinitions)) {
            if (!def.lua) {
                console.warn(`[Queue ${this.name}] Lua script for '${name}' (expected file: ${name.replace('Lua', '').toLowerCase()}.lua or similar) not found or loaded. Functionality depending on it will fail.`);
                continue;
            }
            // Usar el nombre de la propiedad (ej. 'handleFailureLua') para definir el comando
            this.scripts[name] = this.redis.defineCommand(name, def);
        }

        const countersExist = await this.redis.exists(this.keys.queue);
        if (!countersExist) {
            await this.redis.hmset(this.keys.queue, {
                waiting: 0, active: 0, completed: 0, failed: 0,
                totalProcessed: 0, totalFailedPermanently: 0, totalRetried: 0,
            });
        }
    }

    async add(data, options = {}) {
        const jobId = uuidv4(); // Genera el ID aquí
        const creationTimestamp = Date.now();
        const partitionKey = options.partitionKey || 'default';

        const delay = options.delay || 0;
        const scoreForWaitingQueue = creationTimestamp + delay;

        const jobDataForRedis = { // Este objeto se guarda en Redis
            id: jobId, // El ID generado se incluye aquí
            data: JSON.stringify(data),
            timestamp: creationTimestamp,
            attempts: 0,
            maxRetries: options.maxRetries || 3, // Deberías tomar de this.options.defaultJobOptions si no está en options
            status: 'waiting',
            partitionKey,
            scheduledAt: scoreForWaitingQueue,
        };

        const partitionSpecificWaitingQueueKey = this.getPartitionKey(partitionKey);

        if (!this.scripts.addJob) {
            console.error(`[Queue ${this.name}] CRITICAL: addJob script not loaded/defined. Cannot add job.`);
            throw new Error("addJob script not loaded. Ensure queue.init() was called and succeeded.");
        }

        try {
            // El script Lua addJob también puede devolver el jobId, pero esta función
            // ya tiene el jobId generado por uuidv4.
            await this.scripts.addJob(
                this.keys.jobs,
                partitionSpecificWaitingQueueKey,
                this.keys.partitions,
                this.keys.queue,
                jobId, // Se pasa el jobId generado al script
                JSON.stringify(jobDataForRedis),
                scoreForWaitingQueue,
                partitionKey
            );
        } catch (scriptError) {
            console.error(`[Queue ${this.name}] Error executing addJob script for job ${jobId}:`, scriptError);
            throw scriptError;
        }

        return jobId; // DEVUELVES EL STRING jobId
    }

    async getJobById(jobId) {
        const jobDataReply = await this.redis.hgetall(`${this.keys.jobs}:${jobId}`);
        if (!jobDataReply || Object.keys(jobDataReply).length === 0) return null;

        try {
            const job = { ...jobDataReply };
            ['timestamp', 'attempts', 'maxRetries', 'completedAt', 'failedAt', 'startedAt', 'nextTry', 'leaseExpiresAt', 'scheduledAt'].forEach(field => {
                if (job[field] !== undefined && job[field] !== null && job[field] !== '') job[field] = parseInt(job[field], 10);
            });
            if (job.data && typeof job.data === 'string') job.data = JSON.parse(job.data);
            if (job.result && typeof job.result === 'string') {
                try { job.result = JSON.parse(job.result); } catch (e) { /* no es json */ }
            }
            if (job.lastError && typeof job.lastError === 'string') {
                try { job.lastError = JSON.parse(job.lastError); } catch (e) { /* no es json */ }
            }
            return job;
        } catch (error) {
            console.error(`[Queue ${this.name}] Error deserializing job data for ${jobId}:`, error, jobDataReply);
            return null;
        }
    }

    async getNextJobFromPartition(partition) {
        const now = Date.now();
        const partitionQueueKey = this.getPartitionKey(partition);
        const leaseDuration = (this.options.defaultJobOptions && this.options.defaultJobOptions.leaseDuration) || 30000;

        if (!this.scripts.getNextJob) {
            console.error(`[Queue ${this.name}] CRITICAL: getNextJob script not loaded/defined.`);
            throw new Error("getNextJob script not loaded. Ensure queue.init() was called and succeeded.");
        }
        const jobDataJson = await this.scripts.getNextJob(
            partitionQueueKey,
            this.keys.active,
            this.keys.jobs,
            this.keys.queue,
            now,
            leaseDuration
        );

        if (!jobDataJson) return null;
        if (jobDataJson === 'STALE_JOB_REMOVED') {
            console.warn(`[Queue ${this.name}] Stale job ID removed from partition ${partition} by getNextJob script.`);
            return null;
        }

        try {
            const jobDataFromLua = JSON.parse(jobDataJson);
            const job = { ...jobDataFromLua };
            ['timestamp', 'attempts', 'maxRetries', 'startedAt', 'leaseExpiresAt', 'scheduledAt'].forEach(field => {
                if (job[field] !== undefined && job[field] !== null && job[field] !== '') job[field] = parseInt(job[field], 10);
            });
            if (job.data && typeof job.data === 'string') job.data = JSON.parse(job.data);
            return job;
        } catch (error) {
            console.error(`[Queue ${this.name}] Error deserializing job data from getNextJob (partition: ${partition}):`, error, jobDataJson);
            return null;
        }
    }

    async renewJobLease(jobId, leaseDuration) {
        if (!this.scripts.renewJobLease) {
            console.error(`[Queue ${this.name}] CRITICAL: renewJobLease script not loaded/defined.`);
            // Considera lanzar un error o devolver un valor que indique fallo crítico
            return 0;
        }
        try {
            const result = await this.scripts.renewJobLease(
                this.keys.jobs,
                this.keys.active,
                jobId,
                leaseDuration,
                Date.now()
            );
            return parseInt(result, 10);
        } catch (error) {
            console.error(`[Queue ${this.name}] Error renewing lease for job ${jobId}:`, error);
            return 0;
        }
    }

    async findAndRequeueStalledJobs(maxAgeBeforeStalled = 0, maxJobsToProcess = 10) {
        if (!this.scripts.requeueStalledJob) {
            console.error(`[Queue ${this.name}] CRITICAL: requeueStalledJob script not loaded/defined.`);
            return { processed: 0, requeued: 0, failed: 0, errors: 0, other: 0 };
        }
        const results = { processed: 0, requeued: 0, failed: 0, errors: 0, other: 0 };
        const cutOffTime = Date.now() - maxAgeBeforeStalled;

        const stalledJobIds = await this.redis.zrangebyscore(this.keys.active, '-inf', `(${cutOffTime}`, 'LIMIT', 0, maxJobsToProcess);

        if (!stalledJobIds || stalledJobIds.length === 0) {
            return results;
        }
        console.log(`[Queue ${this.name} Janitor] Found ${stalledJobIds.length} potentially stalled jobs.`);

        const stalledErrorMsg = JSON.stringify({ message: 'Job stalled, lease expired.', source: 'janitor', reclaimedAt: Date.now() });
        const retryDelay = (this.options.defaultJobOptions && this.options.defaultJobOptions.stalledJobRetryDelay) || 5000;

        for (const jobId of stalledJobIds) {
            try {
                const status = await this.scripts.requeueStalledJob(
                    this.keys.jobs,
                    this.keys.active,
                    this.keys.waitingPrefix,
                    this.keys.failed,
                    this.keys.queue,
                    jobId,
                    stalledErrorMsg,
                    Date.now(),
                    retryDelay
                );
                results.processed++;
                if (status === 'REQUEUED_STALLED') results.requeued++;
                else if (status === 'FAILED_STALLED') results.failed++;
                else {
                    results.other++;
                    console.warn(`[Queue ${this.name} Janitor] Job ${jobId} handling status: ${status}`);
                }
            } catch (error) {
                results.errors++;
                console.error(`[Queue ${this.name} Janitor] Error processing stalled job ${jobId}:`, error);
            }
        }
        if (results.processed > 0) {
            console.log(`[Queue ${this.name} Janitor] Processed: ${results.processed}, Requeued: ${results.requeued}, Failed: ${results.failed}, Errors: ${results.errors}, Other: ${results.other}`);
        }
        return results;
    }

    async getPartitions() {
        return await this.redis.smembers(this.keys.partitions);
    }

    async getQueueStatus() {
        const [statusReply, partitions] = await Promise.all([
            this.redis.hgetall(this.keys.queue),
            this.getPartitions()
        ]);

        const partitionStats = {};
        if (partitions.length > 0) {
            const multi = this.redis.multi();
            for (const partition of partitions) {
                multi.zcard(this.getPartitionKey(partition));
            }
            const counts = await multi.exec();
            partitions.forEach((partition, index) => {
                partitionStats[partition] = (counts[index] && counts[index][1] !== null) ? parseInt(counts[index][1], 10) : 0;
            });
        }

        const globalCounters = {};
        for (const key in statusReply) {
            globalCounters[key] = parseInt(statusReply[key], 10) || 0;
        }
        return { global: globalCounters, partitions: partitionStats };
    }

    async close() {
        await this.redis.quit();
    }

    async drain() {
        if (!this.scripts.drain) {
            console.error(`[Queue ${this.name}] CRITICAL: drain script not loaded/defined.`);
            throw new Error("drain script not loaded. Ensure queue.init() was called and succeeded.");
        }
        try {
            const totalJobsDeleted = await this.scripts.drain(
                this.keys.jobs, this.keys.partitions, this.keys.waitingPrefix,
                this.keys.active, this.keys.completed, this.keys.failed, this.keys.queue
            );
            console.log(`[Queue ${this.name}] Drain successful. ${totalJobsDeleted} individual job hashes deleted.`);
            await this.init();
        } catch (error) {
            console.error(`[Queue ${this.name}] Error during drain operation:`, error);
            throw error;
        }
    }

    async debug() {
        const status = await this.getQueueStatus();
        console.log(`Global queue status (${this.name}):`, status.global);
        console.log(`Partition status (${this.name}):`, status.partitions);

        const partitions = await this.getPartitions();
        for (const partition of partitions) {
            const waiting = await this.redis.zrange(
                this.getPartitionKey(partition), 0, -1, 'WITHSCORES'
            );
            if (waiting.length > 0) console.log(`\nJobs in partition ${partition} (${this.getPartitionKey(partition)}):`, waiting);
        }

        const activeJobs = await this.redis.zrange(this.keys.active, 0, -1, 'WITHSCORES');
        if (activeJobs.length > 0) console.log(`\nActive jobs (${this.keys.active}):`, activeJobs);

        const completedJobs = await this.redis.zrange(this.keys.completed, 0, -1, 'WITHSCORES');
        if (completedJobs.length > 0) console.log(`\nCompleted jobs (${this.keys.completed}):`, completedJobs);

        const failedJobs = await this.redis.zrange(this.keys.failed, 0, -1, 'WITHSCORES');
        if (failedJobs.length > 0) console.log(`\nFailed jobs (${this.keys.failed}):`, failedJobs);
    }
}

export default Queue;