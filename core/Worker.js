import Queue from './Queue.js'; // Asegúrate que la ruta es correcta
import EventEmitter from 'events';

class Worker extends EventEmitter {
    constructor(queueName, handler, options = {}) {
        super();
        this.queueName = queueName;
        this.handler = handler;
        // Incorporar nuevas opciones y defaults
        this.options = {
            concurrency: 1, // Concurrencia por partición DENTRO de esta instancia de worker
            maxRetries: 3,     // Default para jobs si no lo especifican en el job mismo
            backoffDelay: 1000, // ms para reintentos de job
            partitionDiscoveryInterval: 5000, // ms para buscar nuevas particiones
            noJobSleep: 1000, // ms para dormir si no hay jobs en una partición
            // Opciones para el manejo de "lease" (alquiler) de jobs
            leaseDuration: (options.queueOptions && options.queueOptions.defaultJobOptions && options.queueOptions.defaultJobOptions.leaseDuration) || 30000, // ms
            leaseRenewInterval: options.leaseRenewInterval || 10000, // ms, debe ser < leaseDuration y > 0
            gracefulShutdownTimeout: 30000, // ms
            redis: options.redis, // Para pasar la config de redis a la Queue
            ...options, // Sobrescribir defaults con opciones pasadas
        };

        // Pasar las opciones de redis y defaultJobOptions a la instancia de Queue
        const queueOptions = {
            redis: this.options.redis, // Puede ser undefined, Queue usará su default
            defaultJobOptions: {
                leaseDuration: this.options.leaseDuration,
                stalledJobRetryDelay: (this.options.defaultJobOptions && this.options.defaultJobOptions.stalledJobRetryDelay) || 5000,
            }
        };
        this.queue = new Queue(queueName, queueOptions);
        
        this.isRunning = false;
        this.activeJobPromises = new Set(); // Para rastrear promesas de processJob en curso
        this.partitionWorkersController = new Map(); // Control por partición

        // Recordatorio sobre idempotencia (documentación/comentario):
        // Es CRUCIAL que el 'handler' provisto sea idempotente siempre que sea posible.
        // En ciertos escenarios de fallo (ej. crash del worker después de que el handler
        // procesó exitosamente pero antes de que el job fuera marcado como completado),
        // un job podría ser procesado más de una vez tras un reintento o recuperación de "stalled job".
        // El 'jobId' único puede ayudar a implementar la idempotencia en el handler.
    }

    on(eventName, listener) {
        super.on(eventName, listener);
        return this;
    }

    async init() {
        await this.queue.init(); // Esto define los scripts Lua en this.queue.scripts
    }

    async start() {
        if (this.isRunning) {
            this.emit('worker.already_running', { queueName: this.queueName });
            console.warn(`[Worker ${this.queueName}] Start called but already running.`);
            return;
        }
        this.isRunning = true;
        this.emit('worker.started', { queueName: this.queueName });
        console.log(`[Worker ${this.queueName}] Started. Lease duration: ${this.options.leaseDuration}ms, Renew interval: ${this.options.leaseRenewInterval}ms.`);
        this._startPartitionDiscovery();
    }

    async _startPartitionDiscovery() {
        console.log(`[Worker ${this.queueName}] Partition discovery process started.`);
        while (this.isRunning) {
            try {
                const partitions = await this.queue.getPartitions();
                if (!this.isRunning) break;

                for (const partition of partitions) {
                    if (!this.partitionWorkersController.has(partition) && this.isRunning) {
                        this.emit('partition.discovered', { partition, queueName: this.queueName });
                        console.log(`[Worker ${this.queueName}] New partition discovered: ${partition}. Starting workers...`);
                        this._startWorkersForPartition(partition);
                    }
                }
                // TODO: Podría haber lógica para detener workers de particiones que ya no existen.
                // Esto requeriría comparar la lista actual de particiones con la anterior
                // y llamar a _stopWorkersForPartition si es necesario.

                await new Promise(resolve => setTimeout(resolve, this.options.partitionDiscoveryInterval));
            } catch (error) {
                console.error(`[Worker ${this.queueName}] Error in partition discovery:`, error);
                this.emit('worker.error', { error, context: 'partitionDiscovery', queueName: this.queueName });
                if (this.isRunning) {
                    await new Promise(resolve => setTimeout(resolve, this.options.backoffDelay));
                }
            }
        }
        console.log(`[Worker ${this.queueName}] Partition discovery process stopped.`);
    }

    _startWorkersForPartition(partition) {
        if (!this.isRunning) return; // No iniciar si el worker general se está deteniendo

        const partitionControl = { running: true, promises: new Set() };
        this.partitionWorkersController.set(partition, partitionControl);

        console.log(`[Worker ${this.queueName}] Starting ${this.options.concurrency} worker loops for partition ${partition}.`);
        for (let i = 0; i < this.options.concurrency; i++) {
            const workerId = `${this.queueName}-${partition}-worker-${i + 1}`;
            // Envolver el _workerLoop en una función async para manejar su ciclo de vida de promesa
            const loopFn = async () => {
                try {
                    await this._workerLoop(workerId, partition, partitionControl);
                } catch (loopError) {
                    // Errores no esperados que terminen _workerLoop abruptamente
                    console.error(`[${workerId}] Worker loop ended with unhandled error:`, loopError);
                    this.emit('worker.error', { error: loopError, context: 'workerLoopUnhandled', workerId, partition, queueName: this.queueName });
                } finally {
                     partitionControl.promises.delete(currentPromise); // Limpiar la promesa del set
                }
            };
            const currentPromise = loopFn();
            partitionControl.promises.add(currentPromise);
        }
    }

    _stopWorkersForPartition(partition) {
        const partitionControl = this.partitionWorkersController.get(partition);
        if (partitionControl) {
            console.log(`[Worker ${this.queueName}] Signaling worker loops for partition ${partition} to stop.`);
            partitionControl.running = false;
            // No eliminamos de this.partitionWorkersController aquí; se espera que los bucles terminen.
            // La parada general del worker se encargará de esperar estas promesas.
        }
    }


    async _workerLoop(workerId, partition, partitionControl) {
        this.emit('partition.worker.started', { workerId, partition, queueName: this.queueName });
        // console.log(`[${workerId}] Worker loop started for partition ${partition}`); // Ya logueado en _startWorkersForPartition

        while (this.isRunning && partitionControl.running) {
            let job = null;
            try {
                job = await this.queue.getNextJobFromPartition(partition);

                if (!this.isRunning || !partitionControl.running) break;

                if (job) {
                    this.emit('job.fetched', { job, workerId, queueName: this.queueName });
                    const jobPromise = this.processJob(job, workerId);
                    this.activeJobPromises.add(jobPromise);
                    jobPromise.finally(() => {
                        this.activeJobPromises.delete(jobPromise);
                    });
                    await jobPromise;
                } else {
                    await new Promise(resolve => setTimeout(resolve, this.options.noJobSleep));
                }
            } catch (error) {
                console.error(`[${workerId}] Error in worker loop (partition: ${partition}):`, error);
                this.emit('worker.error', { error, context: 'workerLoop', workerId, partition, jobDetails: job ? job.id : null, queueName: this.queueName });
                if (this.isRunning && partitionControl.running) {
                    await new Promise(resolve => setTimeout(resolve, this.options.backoffDelay));
                }
            }
        }
        this.emit('partition.worker.stopped', { workerId, partition, queueName: this.queueName });
        console.log(`[${workerId}] Worker loop stopped for partition ${partition}.`);
    }


    async processJob(job, workerId) {
        const previousAttempts = parseInt(job.attempts || 0, 10);
        const currentAttemptNumber = previousAttempts + 1;
        // job.startedAt es establecido por getNextJob.lua al tomar el job
        const jobTakenTimestamp = job.startedAt || Date.now(); // Fallback por si acaso
        let processingTimeMs = 0;

        console.log(`[${workerId}] Processing job ${job.id} from partition ${job.partitionKey} (attempt ${currentAttemptNumber}/${job.maxRetries}, lease initially expires at: ${new Date(job.leaseExpiresAt).toISOString()})`);
        this.emit('job.start', { job, workerId, attempt: currentAttemptNumber, queueName: this.queueName });

        let leaseIntervalId = null;
        if (this.options.leaseRenewInterval > 0 && this.options.leaseRenewInterval < this.options.leaseDuration) {
            leaseIntervalId = setInterval(async () => {
                try {
                    if (!this.isRunning) {
                        clearInterval(leaseIntervalId);
                        return;
                    }
                    // console.debug(`[${workerId}] Attempting to renew lease for job ${job.id}`);
                    const renewed = await this.queue.renewJobLease(job.id, this.options.leaseDuration);
                    if (renewed === 1) {
                         // console.debug(`[${workerId}] Lease renewed for job ${job.id}`);
                        this.emit('job.lease_renewed', { jobId: job.id, workerId, newLeaseExpiresAt: Date.now() + this.options.leaseDuration, queueName: this.queueName });
                    } else {
                        console.warn(`[${workerId}] Failed to renew lease for job ${job.id} (or job no longer active/found). Stopping renewal for this job.`);
                        clearInterval(leaseIntervalId);
                    }
                } catch (renewError) {
                    console.error(`[${workerId}] Error during lease renewal for job ${job.id}:`, renewError);
                }
            }, this.options.leaseRenewInterval);
        }

        try {
            const result = await this.handler(job.data, {
                id: job.id, timestamp: job.timestamp, attempts: previousAttempts,
                partitionKey: job.partitionKey, status: 'active', // Status es 'active' porque lo estamos procesando
                startedAt: jobTakenTimestamp, leaseExpiresAt: job.leaseExpiresAt
            });
            processingTimeMs = Date.now() - jobTakenTimestamp;
            await this.completeJob(job, result, workerId, processingTimeMs);
            return result;
        } catch (errorFromHandler) {
            processingTimeMs = Date.now() - jobTakenTimestamp;
            console.warn(`[${workerId}] Handler error processing job ${job.id} (attempt ${currentAttemptNumber}, time ${processingTimeMs}ms):`, errorFromHandler.message);
            await this.failJob(job, errorFromHandler, workerId, currentAttemptNumber, processingTimeMs);
            // No re-lanzar, failJob emite los eventos y el bucle debe continuar.
        } finally {
            if (leaseIntervalId) {
                clearInterval(leaseIntervalId);
            }
        }
    }

    async completeJob(job, result, workerId, processingTimeMs = 0) {
        const timestamp = Date.now();
        if (!this.queue.scripts.completeJob) throw new Error("completeJob script not loaded in queue");
        try {
            await this.queue.scripts.completeJob(
                this.queue.keys.queue, this.queue.keys.active,
                this.queue.keys.jobs, this.queue.keys.completed,
                job.id, JSON.stringify(result), timestamp
            );
            this.emit('job.completed', { jobId: job.id, job, result, workerId, timestamp, processingTimeMs, queueName: this.queueName });
            console.log(`[${workerId}] Job ${job.id} completed successfully (time: ${processingTimeMs}ms).`);
        } catch (scriptError) {
            console.error(`[${workerId}] CRITICAL: Failed to execute completeJob Lua script for job ${job.id}:`, scriptError);
            this.emit('job.error', { jobId: job.id, job, error: scriptError, phase: 'completing', workerId, processingTimeMs, queueName: this.queueName });
            throw scriptError; // Relanzar para que processJob (si lo llama directamente) pueda manejarlo.
        }
    }

    async failJob(job, error, workerId, attemptThatFailed, processingTimeMs = 0) {
        const timestamp = Date.now();
        const maxRetries = parseInt(job.maxRetries || this.options.maxRetries, 10);
        const errorDetailsForRedis = JSON.stringify({ message: error.message, name: error.name /*, stack: error.stack */ });
        let nextTryTimestamp = 0;
        let delayForEvent = 0;
        const willRetry = attemptThatFailed < maxRetries;

        if (willRetry) {
            delayForEvent = this.options.backoffDelay * Math.pow(2, attemptThatFailed - 1); // Exponencial backoff
            nextTryTimestamp = timestamp + delayForEvent;
        }
        const partitionWaitingQueueKey = this.queue.getPartitionKey(job.partitionKey);

        if (!this.queue.scripts.handleFailureLua) throw new Error("handleFailureLua script not loaded in queue");
        try {
            const scriptResult = await this.queue.scripts.handleFailureLua(
                this.queue.keys.jobs, this.queue.keys.active,
                partitionWaitingQueueKey, this.queue.keys.failed, this.queue.keys.queue,
                job.id, errorDetailsForRedis, timestamp, attemptThatFailed, maxRetries,
                nextTryTimestamp, job.partitionKey
            );

            const eventPayload = { jobId: job.id, job, error: { message: error.message, name: error.name, stack: error.stack }, attempt: attemptThatFailed, maxRetries, workerId, processingTimeMs, queueName: this.queueName };

            if (scriptResult === 'RETRIED') {
                console.log(`[${workerId}] Job ${job.id} (attempt ${attemptThatFailed}, time ${processingTimeMs}ms) failed, will retry in ${delayForEvent}ms. Error: ${error.message}`);
                this.emit('job.retrying', { ...eventPayload, delay: delayForEvent, nextTry: nextTryTimestamp });
            } else if (scriptResult === 'FAILED_PERMANENTLY') {
                console.error(`[${workerId}] Job ${job.id} (attempt ${attemptThatFailed}, time ${processingTimeMs}ms) failed permanently. Error: ${error.message}`);
                this.emit('job.failed', eventPayload);
            } else {
                // Resultado inesperado del script Lua
                console.error(`[${workerId}] Unexpected result from handleFailureLua script: ${scriptResult} for job ${job.id}`);
                this.emit('job.error', { ...eventPayload, error: new Error(`Unexpected Lua script result: ${scriptResult}`), phase: 'failing_script_unknown_result' });
            }
        } catch (scriptError) {
            console.error(`[${workerId}] CRITICAL: Failed to execute handleFailureLua script for job ${job.id}:`, scriptError);
            this.emit('job.error', { jobId: job.id, job, error: scriptError, phase: 'failing_script_execution', workerId, processingTimeMs, queueName: this.queueName });
            throw scriptError;
        }
    }

    async stop() {
        if (!this.isRunning) {
            this.emit('worker.already_stopped', { queueName: this.queueName });
            // console.warn(`[Worker ${this.queueName}] Stop called but already stopped.`);
            return;
        }
        this.emit('worker.stopping', { queueName: this.queueName });
        console.log(`[Worker ${this.queueName}] Stopping...`);
        this.isRunning = false;

        // Señalizar a todos los bucles de worker de partición para que se detengan
        this.partitionWorkersController.forEach((control, partition) => {
            // console.debug(`[Worker ${this.queueName}] Signaling workers for partition ${partition} to stop.`);
            control.running = false;
        });

        // Esperar a que los bucles _workerLoop terminen (no necesariamente los jobs que están procesando)
        const allPartitionWorkerLoopPromises = [];
        this.partitionWorkersController.forEach(control => {
            control.promises.forEach(p => allPartitionWorkerLoopPromises.push(p));
        });
        if (allPartitionWorkerLoopPromises.length > 0) {
            // console.debug(`[Worker ${this.queueName}] Waiting for ${allPartitionWorkerLoopPromises.length} partition worker loops to exit.`);
            await Promise.allSettled(allPartitionWorkerLoopPromises);
        }
        // console.debug(`[Worker ${this.queueName}] All partition worker loops have exited.`);
        this.partitionWorkersController.clear();


        // Esperar a que los jobs actualmente en processJob (this.activeJobPromises) terminen o timeout
        if (this.activeJobPromises.size > 0) {
            console.log(`[Worker ${this.queueName}] Waiting for ${this.activeJobPromises.size} active jobs to complete (timeout: ${this.options.gracefulShutdownTimeout}ms)...`);
            try {
                await Promise.race([
                    Promise.allSettled(Array.from(this.activeJobPromises)),
                    new Promise((_, reject) => setTimeout(() => reject(new Error('Graceful shutdown timeout for active jobs')), this.options.gracefulShutdownTimeout))
                ]);
                // console.debug(`[Worker ${this.queueName}] Active jobs processing finished or timed out.`);
            } catch (timeoutError) {
                console.warn(`[Worker ${this.queueName}] While waiting for active jobs: ${timeoutError.message}`);
                this.emit('worker.error', { error: timeoutError, context: 'gracefulShutdownActiveJobs', queueName: this.queueName });
            }
        } else {
            // console.debug(`[Worker ${this.queueName}] No active jobs to wait for.`);
        }

        try {
            await this.queue.close();
            console.log(`[Worker ${this.queueName}] Redis connection closed.`);
        } catch (redisCloseError) {
            console.error(`[Worker ${this.queueName}] Error closing Redis connection:`, redisCloseError);
            this.emit('worker.error', { error: redisCloseError, context: 'redisClose', queueName: this.queueName });
        }

        this.emit('worker.stopped', { queueName: this.queueName });
        console.log(`[Worker ${this.queueName}] Stopped successfully.`);
    }
}

export default Worker;