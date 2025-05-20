import Queue from './Queue.js';
import EventEmitter from 'events'; // 1. Importar EventEmitter

// 2. Hacer que Worker herede de EventEmitter
class Worker extends EventEmitter {
    constructor(queueName, handler, options = {}) {
        super(); // Llamar al constructor de EventEmitter
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

    // Aunque 'on' es heredado, podemos definirlo explícitamente para claridad o encadenamiento.
    on(eventName, listener) {
        super.on(eventName, listener);
        return this; // Permitir encadenamiento: worker.on(...).on(...)
    }

    async init() {
        await this.queue.init();
    }

    async start() {
        if (this.isRunning) return;
        this.isRunning = true;
        this.emit('worker.started', { queueName: this.queue.name });
        this.startPartitionDiscovery();
    }

    async startPartitionDiscovery() {
        while (this.isRunning) {
            try {
                const partitions = await this.queue.getPartitions();
                for (const partition of partitions) {
                    if (!this.partitionWorkers.has(partition)) {
                        this.emit('partition.discovered', { partition, queueName: this.queue.name });
                        console.log(`New partition discovered: ${partition}`);
                        this.startPartitionWorkers(partition);
                    }
                }
                await new Promise(resolve => setTimeout(resolve, 1000)); // Check for new partitions periodically
            } catch (error) {
                console.error('Error in partition discovery:', error);
                this.emit('worker.error', { error, context: 'partitionDiscovery', queueName: this.queue.name });
                await new Promise(resolve => setTimeout(resolve, this.options.backoffDelay));
            }
        }
    }

    startPartitionWorkers(partition) {
        const workers = Array(this.options.concurrency).fill(null).map((_, i) => {
            const workerId = `${this.queue.name}-${partition}-worker-${i + 1}`;
            const workerPromise = this.startWorker(workerId, partition).catch(error => {
                console.error(`Error initializing worker ${workerId}:`, error);
                this.emit('worker.error', { error, context: 'partitionWorkerInitialization', workerId, partition, queueName: this.queue.name });
            });
            return workerPromise;
        });
        this.partitionWorkers.set(partition, workers);
    }

    async stop() {
        this.isRunning = false;
        this.emit('worker.stopping', { queueName: this.queue.name });
        // Esperar a que los trabajos activos terminen (esto es una simplificación, un manejo más robusto podría ser necesario)
        await Promise.all(Array.from(this.activeJobs.values()));
        await this.queue.close();
        this.emit('worker.stopped', { queueName: this.queue.name });
    }

    async startWorker(workerId, partition) {
        this.emit('partition.worker.started', { workerId, partition, queueName: this.queue.name });
        console.log(`[${workerId}] Worker started for partition ${partition}`);

        while (this.isRunning) {
            try {
                if (this.partitionLocks.get(partition)) {
                    await new Promise(resolve => setTimeout(resolve, 100)); // Short delay if partition is locked
                    continue;
                }

                const job = await this.queue.getNextJobFromPartition(partition);
                if (!job) {
                    await new Promise(resolve => setTimeout(resolve, 1000)); // Wait if no job
                    continue;
                }

                if (this.activeJobs.has(job.id)) { // Should not happen if getNextJobFromPartition is correct
                    continue;
                }

                this.partitionLocks.set(partition, true);

                const processPromise = this.processJob(job, workerId);
                this.activeJobs.set(job.id, processPromise);

                try {
                    await processPromise;
                } finally {
                    this.activeJobs.delete(job.id);
                    this.partitionLocks.set(partition, false);
                }
            } catch (error) {
                // Errores de processJob ya emiten eventos. Este catch es para errores en el bucle de startWorker.
                console.error(`[${workerId}] Critical error in worker loop:`, error);
                this.emit('worker.error', { error, context: 'workerLoop', workerId, partition, queueName: this.queue.name });
                this.partitionLocks.set(partition, false); // Ensure lock is released
                await new Promise(resolve => setTimeout(resolve, this.options.backoffDelay));
            }
        }
        this.emit('partition.worker.stopped', { workerId, partition, queueName: this.queue.name });
        console.log(`[${workerId}] Worker stopped`);
    }

    async processJob(job, workerId) {
        // 'job.attempts' es el número de fallos *anteriores*.
        const previousAttempts = parseInt(job.attempts || 0);
        const currentAttemptNumber = previousAttempts + 1; // Este es el intento actual (1-indexed)

        console.log(`[${workerId}] Processing job ${job.id} from partition ${job.partitionKey} (attempt ${currentAttemptNumber}/${job.maxRetries}):`,
            JSON.stringify(job.data, null, 2));

        // 3. Emitir evento 'job.start'
        this.emit('job.start', { job, workerId, attempt: currentAttemptNumber });

        try {
            const result = await this.handler(job.data, {
                id: job.id,
                timestamp: parseInt(job.timestamp),
                attempts: previousAttempts, // El handler ve los intentos previos (0-indexed)
                partitionKey: job.partitionKey
            });

            await this.completeJob(job.id, result, workerId, job);
            // El evento 'job.completed' se emite dentro de completeJob
            return result;
        } catch (error) {
            // El evento 'job.failed' o 'job.retrying' se emite dentro de failJob
            await this.failJob(job.id, error, workerId, job, currentAttemptNumber);
            throw error; // Re-lanzar para que el .finally en startWorker se ejecute correctamente
        }
    }

    async completeJob(jobId, result, workerId, originalJob) {
        // Log al iniciar el proceso de completar el trabajo
        console.log(`[${workerId}] Iniciando proceso para marcar job ${jobId} como COMPLETADO. Resultado:`, result);

        const timestamp = Date.now();

        console.log("TIMESTAMP:")
        console.log(timestamp)

        try {
            // Actualizar los datos del job en Redis
            await this.queue.redis.hmset(`${this.queue.keys.jobs}:${jobId}`, {
                result: JSON.stringify(result),
                completedAt: timestamp,
                status: 'completed'
            });

            // Mover de la lista active a la lista completed
            await this.queue.redis.zrem(this.queue.keys.active, jobId);
            await this.queue.redis.zadd(this.queue.keys.completed, timestamp, jobId);

            // Actualizar contadores globales de la cola
            await this.queue.redis.hincrby(this.queue.keys.queue, 'active', -1);
            await this.queue.redis.hincrby(this.queue.keys.queue, 'completed', 1);

            // Log después de que todas las operaciones en Redis han sido exitosas
            console.log(`[${workerId}] Job ${jobId} marcado exitosamente como COMPLETADO en Redis y contadores actualizados.`);

            // Emitir evento 'job.completed' (esto ya lo tenías y es bueno mantenerlo)
            this.emit('job.completed', { jobId, result, job: originalJob, workerId });
        } catch (error) {
            // Log en caso de error durante el proceso de completar
            console.error(`[${workerId}] ERROR al intentar marcar job ${jobId} como completado en Redis:`, error);
            // Considera si necesitas emitir un evento de error específico aquí o re-lanzar el error
            // Por ahora, el error se propagará si esta función es llamada con await y no tiene su propio try/catch más arriba.
            throw error; // Es importante re-lanzar para que el flujo que llamó sepa del error
        }
    }

    async failJob(jobId, error, workerId, originalJobFromQueue, attemptThatFailed) {
        const timestamp = Date.now();
        const maxRetries = parseInt(originalJobFromQueue.maxRetries || this.options.maxRetries);

        const errorDetails = {
            message: error.message,
            stack: error.stack,
            name: error.name,
            // Puedes agregar más detalles del error si es necesario
        };

        const failurePayload = {
            jobId,
            error: errorDetails,
            job: originalJobFromQueue, // Datos del job tal como se obtuvieron para este intento
            attempt: attemptThatFailed,
            maxRetries,
            workerId
        };

        if (attemptThatFailed < maxRetries) {
            const delay = this.options.backoffDelay * Math.pow(2, attemptThatFailed - 1);
            const nextTry = timestamp + delay;

            console.log(`[${workerId}] Retrying job ${jobId} in ${delay}ms (attempt ${attemptThatFailed}/${maxRetries}). Error: ${error.message}`);

            await this.queue.redis.hmset(`${this.queue.keys.jobs}:${jobId}`, {
                attempts: attemptThatFailed, // Registrar que este intento falló
                lastError: error.message,
                status: 'waiting',
                nextTry
            });

            const partitionQueueKey = this.queue.getPartitionKey(originalJobFromQueue.partitionKey);
            await this.queue.redis.zrem(this.queue.keys.active, jobId);
            await this.queue.redis.zadd(partitionQueueKey, nextTry, jobId);
            await this.queue.redis.hincrby(this.queue.keys.queue, 'active', -1);
            await this.queue.redis.hincrby(this.queue.keys.queue, 'waiting', 1);

            // 3. Emitir evento 'job.retrying'
            this.emit('job.retrying', { ...failurePayload, delay, nextTry });
        } else {
            console.log(`[${workerId}] Job ${jobId} failed after ${attemptThatFailed} attempts. Error: ${error.message}`);

            await this.queue.redis.hmset(`${this.queue.keys.jobs}:${jobId}`, {
                attempts: attemptThatFailed, // Registrar que este (último) intento falló
                lastError: error.message,
                failedAt: timestamp,
                status: 'failed'
            });

            await this.queue.redis.zrem(this.queue.keys.active, jobId);
            await this.queue.redis.zadd(this.queue.keys.failed, timestamp, jobId);
            await this.queue.redis.hincrby(this.queue.keys.queue, 'active', -1);
            await this.queue.redis.hincrby(this.queue.keys.queue, 'failed', 1);

            // 3. Emitir evento 'job.failed' (fallo definitivo)
            this.emit('job.failed', failurePayload);
        }
    }
}

export default Worker;