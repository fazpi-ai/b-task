import { jest } from '@jest/globals';
import Worker from '../../core/Worker.js';
import Queue from '../../core/Queue.js';

jest.mock('../../core/Queue.js');

// Guardar los originales ANTES de que Jest los pueda modificar o espiar
const realSetInterval = global.setInterval;
const realClearInterval = global.clearInterval;

describe('Worker', () => {
  let worker;
  let mockQueue; // Esta será la instancia mockeada de Queue devuelta por el constructor mockeado de Queue
  let mockHandler;
  let spySetInterval; // Usaremos spyOn
  let spyClearInterval; // Usaremos spyOn

  const defaultQueueName = 'unit-test-queue';
  const sampleJob = { // Un job de muestra para usar en múltiples tests
    id: 'job-process-1',
    data: { info: 'test' },
    timestamp: Date.now(),
    attempts: 0,
    maxRetries: 3,
    partitionKey: 'default',
    status: 'active', // Asumimos que llega como 'active' o lo pone así getNextJob
    startedAt: Date.now(), // getNextJob.lua lo establece
    leaseExpiresAt: Date.now() + 30000, // getNextJob.lua lo establece
  };
  const defaultWorkerOptions = {
    concurrency: 1,
    maxRetries: 3,
    backoffDelay: 1000,
    partitionDiscoveryInterval: 100, // ms, rápido para tests
    noJobSleep: 10, // ms
    leaseDuration: 30000, // ms
    leaseRenewInterval: 10000, // ms, debe ser < leaseDuration y > 0
    gracefulShutdownTimeout: 500, // ms, rápido para tests
    // No pasamos 'redis' aquí, asumimos que Queue lo maneja con sus defaults si es necesario
  };

  beforeEach(() => {
    jest.useFakeTimers(); // Activar timers falsos PRIMERO

    // Espiar los timers globales en lugar de reemplazarlos completamente
    spySetInterval = jest.spyOn(global, 'setInterval').mockReturnValue(123); // Retorna un ID de intervalo
    spyClearInterval = jest.spyOn(global, 'clearInterval');

    // Configurar el mock de Queue para que su constructor devuelva una instancia mock bien definida
    mockQueue = {
      init: jest.fn().mockResolvedValue(undefined),
      getPartitions: jest.fn().mockResolvedValue(['default']), // Por defecto, una partición 'default'
      getNextJobFromPartition: jest.fn().mockResolvedValue(null), // Por defecto, no hay jobs
      scripts: { // Simular scripts Lua cargados
        completeJob: jest.fn().mockResolvedValue('OK'),
        handleFailureLua: jest.fn().mockResolvedValue('FAILED_PERMANENTLY') // Default a fallo permanente
      },
      keys: { // Simular las claves que Worker podría necesitar
        queue: `queue:${defaultQueueName}`,
        active: `queue:${defaultQueueName}:active`,
        jobs: `queue:${defaultQueueName}:jobs`,
        completed: `queue:${defaultQueueName}:completed`,
        failed: `queue:${defaultQueueName}:failed`
      },
      close: jest.fn().mockResolvedValue(undefined),
      getPartitionKey: jest.fn(partition => `queue:${defaultQueueName}:waiting:${partition}`),
      renewJobLease: jest.fn().mockResolvedValue(1) // Por defecto, renovación de lease exitosa
    };
    Queue.mockImplementation(() => mockQueue); // Cada `new Queue()` usará este `mockQueue`

    mockHandler = jest.fn().mockResolvedValue('handler success'); // Handler por defecto exitoso
    
    // Crear una instancia de Worker para cada test
    worker = new Worker(defaultQueueName, mockHandler, defaultWorkerOptions);
  });

  afterEach(() => {
    jest.clearAllMocks(); // Limpia todos los mocks, incluyendo spies
    jest.clearAllTimers(); // Limpia cualquier timer pendiente de los fake timers
    jest.useRealTimers(); // MUY IMPORTANTE: Restaurar timers reales para evitar interferencias
    // No es necesario restaurar spies en global si se usa clearAllMocks, pero si fuera manual:
    // spySetInterval.mockRestore();
    // spyClearInterval.mockRestore();
  });

  describe('Constructor and Initialization', () => {
    it('should initialize with default options from constructor and merge passed options', () => {
      const customOptions = { concurrency: 5, leaseRenewInterval: 5000 };
      const w = new Worker(defaultQueueName, mockHandler, customOptions);
      expect(w.queueName).toBe(defaultQueueName);
      expect(w.handler).toBe(mockHandler);
      expect(w.options.concurrency).toBe(5); // Sobrescrito
      expect(w.options.maxRetries).toBe(3);  // Default de Worker
      expect(w.options.leaseRenewInterval).toBe(5000); // Sobrescrito
      expect(w.options.leaseDuration).toBe(30000); // Default de Worker (ya que no se pasó queueOptions.defaultJobOptions.leaseDuration)
    });

    it('should call queue.init() when worker.init() is called', async () => {
      await worker.init();
      expect(mockQueue.init).toHaveBeenCalledTimes(1);
    });
  });

  describe('Start, Stop, and Discovery', () => {
    it('should set isRunning to true and emit "worker.started" on start', async () => {
      const startedSpy = jest.fn();
      worker.on('worker.started', startedSpy);
      worker._startPartitionDiscovery = jest.fn(); // Evitar que el bucle real inicie para este test unitario

      await worker.start();
      expect(worker.isRunning).toBe(true);
      expect(startedSpy).toHaveBeenCalled();
      expect(worker._startPartitionDiscovery).toHaveBeenCalled();
    });

    it('should set isRunning to false, call queue.close() and emit events on stop', async () => {
      worker.isRunning = true;
      const mockPartitionControl = { running: true, promises: new Set([Promise.resolve()]) };
      worker.partitionWorkersController.set('default', mockPartitionControl);

      const stoppingSpy = jest.fn();
      const stoppedSpy = jest.fn();
      worker.on('worker.stopping', stoppingSpy);
      worker.on('worker.stopped', stoppedSpy);

      await worker.stop();

      expect(worker.isRunning).toBe(false);
      expect(mockPartitionControl.running).toBe(false);
      expect(mockQueue.close).toHaveBeenCalled();
      expect(stoppingSpy).toHaveBeenCalled();
      expect(stoppedSpy).toHaveBeenCalled();
    });

    it('_startPartitionDiscovery should discover new partitions and start workers', async () => {
      worker.isRunning = true;
      mockQueue.getPartitions.mockResolvedValueOnce(['p1', 'p2']);
      worker._startWorkersForPartition = jest.fn(); // Mock para no ejecutar la lógica interna

      const discoveryPromise = worker._startPartitionDiscovery();
      // Permitir que la primera iteración del bucle se ejecute
      await jest.advanceTimersByTime(0); // Avanzar microtareas y timers inmediatos

      expect(worker._startWorkersForPartition).toHaveBeenCalledWith('p1');
      expect(worker._startWorkersForPartition).toHaveBeenCalledWith('p2');

      worker.isRunning = false; // Simular parada para que el bucle termine
      // Avanzar timers para que el setTimeout del bucle de descubrimiento se complete
      await jest.advanceTimersByTime(worker.options.partitionDiscoveryInterval + 10);
      await discoveryPromise; // Asegurarse que el bucle de _startPartitionDiscovery termine
    });
  });

  describe('Job Processing Flow', () => {
    it('processJob: should call handler, completeJob, and manage lease interval on success', async () => {
      mockHandler.mockResolvedValue('handler success');
      worker.isRunning = true; // Asegurar que el worker se considera en ejecución
      
      const willSetupLeaseInterval = worker.options.leaseRenewInterval > 0 && worker.options.leaseRenewInterval < worker.options.leaseDuration;
      expect(willSetupLeaseInterval).toBe(true);

      // Capturar la función del intervalo cuando se configura
      let intervalCallback;
      spySetInterval.mockImplementation((callback) => {
        intervalCallback = callback;
        return 123;
      });

      // Iniciar el procesamiento del job
      const processPromise = worker.processJob({ ...sampleJob }, 'w1');

      // Verificar que se configuró el intervalo
      expect(spySetInterval).toHaveBeenCalledWith(
        expect.any(Function),
        worker.options.leaseRenewInterval
      );

      // Ejecutar y esperar el callback del intervalo
      // Esto debe hacerse ANTES de que processPromise resuelva completamente si el handler es rápido
      if (intervalCallback) { // Asegurarse que el callback fue capturado
        await intervalCallback();
      }

      // Verificar que se llamó a renewJobLease
      expect(mockQueue.renewJobLease).toHaveBeenCalledWith(
        sampleJob.id,
        worker.options.leaseDuration
      );

      // Completar el procesamiento del job
      await processPromise;

      // Verificar que se llamó al handler y se completó el job
      expect(mockHandler).toHaveBeenCalledWith(
        sampleJob.data,
        expect.objectContaining({ id: sampleJob.id })
      );
      expect(mockQueue.scripts.completeJob).toHaveBeenCalled();
      
      // Verificar que se limpió el intervalo
      expect(spyClearInterval).toHaveBeenCalledWith(123);
    });

    it('processJob: should call handler, failJob, and manage lease interval on error', async () => {
      const error = new Error('Handler failed!');
      mockHandler.mockRejectedValue(error);
      mockQueue.scripts.handleFailureLua.mockResolvedValue('RETRIED');
      worker.isRunning = true; // Asegurar que el worker se considera en ejecución

      const willSetupLeaseInterval = worker.options.leaseRenewInterval > 0 && worker.options.leaseRenewInterval < worker.options.leaseDuration;
      expect(willSetupLeaseInterval).toBe(true);

      // Capturar la función del intervalo cuando se configura
      let intervalCallback;
      spySetInterval.mockImplementation((callback) => {
        intervalCallback = callback;
        return 123;
      });

      // Iniciar el procesamiento del job
      const processPromise = worker.processJob({ ...sampleJob }, 'w1');

      // Verificar que se configuró el intervalo
      expect(spySetInterval).toHaveBeenCalledWith(
        expect.any(Function),
        worker.options.leaseRenewInterval
      );

      // Ejecutar y esperar el callback del intervalo
      if (intervalCallback) { // Asegurarse que el callback fue capturado
        await intervalCallback();
      }

      // Verificar que se llamó a renewJobLease
      expect(mockQueue.renewJobLease).toHaveBeenCalledWith(
        sampleJob.id,
        worker.options.leaseDuration
      );

      // Esperar a que el proceso falle
      await processPromise;

      // Verificar que se llamó al handler y falló
      expect(mockHandler).toHaveBeenCalledWith(
        sampleJob.data,
        expect.objectContaining({ id: sampleJob.id })
      );
      expect(mockQueue.scripts.handleFailureLua).toHaveBeenCalled();

      // Verificar que se limpió el intervalo
      expect(spyClearInterval).toHaveBeenCalledWith(123);
    });
  });

  describe('completeJob method', () => {
    it('should call queue.scripts.completeJob with correct args and emit event', async () => {
      const jobToComplete = { id: 'c1', data: { info: "done" } };
      const resultPayload = { success: true };
      const processingTime = 123;
      const completedSpy = jest.fn();
      worker.on('job.completed', completedSpy);

      await worker.completeJob(jobToComplete, resultPayload, 'w-id-complete', processingTime);

      expect(mockQueue.scripts.completeJob).toHaveBeenCalledWith(
        mockQueue.keys.queue, mockQueue.keys.active,
        mockQueue.keys.jobs, mockQueue.keys.completed,
        jobToComplete.id, JSON.stringify(resultPayload), expect.any(Number)
      );
      expect(completedSpy).toHaveBeenCalledWith(expect.objectContaining({
        jobId: jobToComplete.id, result: resultPayload, processingTimeMs: processingTime
      }));
    });
  });

  describe('failJob method', () => {
    const jobToFail = { id: 'f1', data: {}, maxRetries: 3, partitionKey: 'p1', attempts: 0 };
    const handlerError = new Error('test failure');

    it('should call queue.scripts.handleFailureLua for retry and emit job.retrying', async () => {
      mockQueue.scripts.handleFailureLua.mockResolvedValue('RETRIED');
      const retryingSpy = jest.fn();
      worker.on('job.retrying', retryingSpy);
      const attemptThatFailed = 1;

      await worker.failJob(jobToFail, handlerError, 'w-id-fail', attemptThatFailed, 100);

      expect(mockQueue.scripts.handleFailureLua).toHaveBeenCalled();
      // Aquí puedes ser más específico con los argumentos de handleFailureLua si es necesario
      expect(retryingSpy).toHaveBeenCalledWith(expect.objectContaining({ 
        attempt: attemptThatFailed, 
        jobId: jobToFail.id,
        error: expect.objectContaining({ message: handlerError.message }) 
      }));
    });

    it('should call queue.scripts.handleFailureLua for permanent fail and emit job.failed', async () => {
      mockQueue.scripts.handleFailureLua.mockResolvedValue('FAILED_PERMANENTLY');
      const failedSpy = jest.fn();
      worker.on('job.failed', failedSpy);
      const attemptThatFailed = 3; // Simular el último intento

      await worker.failJob(jobToFail, handlerError, 'w-id-fail', attemptThatFailed, 100);

      expect(mockQueue.scripts.handleFailureLua).toHaveBeenCalled();
      expect(failedSpy).toHaveBeenCalledWith(expect.objectContaining({ 
        attempt: attemptThatFailed, 
        jobId: jobToFail.id,
        error: expect.objectContaining({ message: handlerError.message })
      }));
    });
  });
});