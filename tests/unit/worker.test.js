import { jest } from '@jest/globals';
import Worker from '../../core/Worker.js';   // Ruta desde tests/unit/ hasta core/
import Queue from '../../core/Queue.js';     // Para mockearlo

// Mockear la clase Queue completa
// La ruta es relativa al archivo de test actual (worker.test.js)
jest.mock('../../core/Queue.js');

// Mock de setInterval y clearInterval
const realSetInterval = global.setInterval;
const realClearInterval = global.clearInterval;

describe('Worker', () => {
  let worker;
  let mockQueue;
  let mockHandler;
  let mockSetInterval;
  let mockClearInterval;

  const defaultQueueName = 'unit-test-queue';
  const defaultWorkerOptions = {
    concurrency: 1,
    leaseDuration: 30000,
    leaseRenewInterval: 10000, // Importante para tests de lease
    noJobSleep: 10, // ms
    partitionDiscoveryInterval: 100, // ms, rápido para tests
    gracefulShutdownTimeout: 500, // ms, rápido para tests
  };

  beforeEach(() => {
    // Configurar los mocks de timers
    mockSetInterval = jest.fn().mockReturnValue(123);
    mockClearInterval = jest.fn();
    global.setInterval = mockSetInterval;
    global.clearInterval = mockClearInterval;

    // Configurar los timers falsos
    jest.useFakeTimers();

    // Configurar el mock de Queue
    mockQueue = {
      init: jest.fn().mockResolvedValue(undefined),
      getPartitions: jest.fn().mockResolvedValue(['default']),
      getNextJobFromPartition: jest.fn(),
      scripts: {
        completeJob: jest.fn().mockResolvedValue('OK'),
        handleFailureLua: jest.fn().mockResolvedValue('OK')
      },
      keys: {
        queue: 'queue:test',
        active: 'queue:test:active',
        jobs: 'queue:test:jobs',
        completed: 'queue:test:completed',
        failed: 'queue:test:failed'
      },
      close: jest.fn().mockResolvedValue(undefined),
      getPartitionKey: jest.fn(key => `queue:test:partition:${key}`),
      renewJobLease: jest.fn().mockResolvedValue(1)
    };

    // Hacer que el constructor de Queue devuelva nuestro mock
    Queue.mockImplementation(() => mockQueue);

    // Mock del handler
    mockHandler = jest.fn();

    // Crear una instancia de Worker para cada test
    worker = new Worker(defaultQueueName, mockHandler, defaultWorkerOptions);
  });

  afterEach(() => {
    // Restaurar los timers originales
    global.setInterval = realSetInterval;
    global.clearInterval = realClearInterval;

    // Asegurarse de que todos los timers y mocks se limpien
    jest.clearAllMocks();
    jest.useRealTimers(); // Restaurar timers reales
  });

  describe('Constructor and Initialization', () => {
    it('should initialize with default options', () => {
      expect(worker.queueName).toBe(defaultQueueName);
      expect(worker.handler).toBe(mockHandler);
      expect(worker.options.concurrency).toBe(1);
      expect(worker.options.maxRetries).toBe(3);
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
      worker._startPartitionDiscovery = jest.fn(); // Evitar que el bucle real inicie

      await worker.start();
      expect(worker.isRunning).toBe(true);
      expect(startedSpy).toHaveBeenCalled();
      expect(worker._startPartitionDiscovery).toHaveBeenCalled();
    });

    it('should set isRunning to false, call queue.close() and emit events on stop', async () => {
      worker.isRunning = true; // Simular que estaba corriendo
      // Simular que hay un controlador de partición para probar su limpieza
      const mockPartitionControl = { running: true, promises: new Set([Promise.resolve()]) };
      worker.partitionWorkersController.set('default', mockPartitionControl);

      const stoppingSpy = jest.fn();
      const stoppedSpy = jest.fn();
      worker.on('worker.stopping', stoppingSpy);
      worker.on('worker.stopped', stoppedSpy);

      await worker.stop();

      expect(worker.isRunning).toBe(false);
      expect(mockPartitionControl.running).toBe(false); // Workers de partición señalados para parar
      expect(mockQueue.close).toHaveBeenCalled();
      expect(stoppingSpy).toHaveBeenCalled();
      expect(stoppedSpy).toHaveBeenCalled();
    });

    // Test para _startPartitionDiscovery es más complejo para unit,
    // pero podemos probar un "tick" del descubrimiento.
    it('_startPartitionDiscovery should discover new partitions and start workers', async () => {
      worker.isRunning = true;
      mockQueue.getPartitions.mockResolvedValueOnce(['p1', 'p2']); // Primera llamada
      worker._startWorkersForPartition = jest.fn();

      // Ejecutar una "iteración" del bucle de descubrimiento
      const discoveryPromise = worker._startPartitionDiscovery();
      // Permitir que la primera iteración (getPartitions, bucle for) se ejecute
      await jest.advanceTimersByTime(0); // Avanzar microtareas
      
      expect(worker._startWorkersForPartition).toHaveBeenCalledWith('p1');
      expect(worker._startWorkersForPartition).toHaveBeenCalledWith('p2');

      // Simular que el worker se detiene para salir del bucle
      worker.isRunning = false;
      await jest.advanceTimersByTime(defaultWorkerOptions.partitionDiscoveryInterval + 10); // Avanzar para que el bucle termine
      await discoveryPromise; // Esperar a que el bucle termine
    });
  });

  describe('Job Processing Flow', () => {
    const sampleJob = {
      id: 'job-process-1', data: { info: 'test' }, timestamp: Date.now(), attempts: 0, maxRetries: 3,
      partitionKey: 'default', status: 'active', startedAt: Date.now(), leaseExpiresAt: Date.now() + 30000,
    };

    it('processJob: should call handler, then completeJob on success, and manage lease interval', async () => {
      mockHandler.mockResolvedValue('handler success');
      
      const processResult = worker.processJob({ ...sampleJob }, 'w1');
      
      // Dejar que el intervalo de lease se ejecute una vez si está configurado
      if (worker.options.leaseRenewInterval > 0) {
        expect(mockSetInterval).toHaveBeenCalledWith(
          expect.any(Function),
          worker.options.leaseRenewInterval
        );
        jest.advanceTimersByTime(worker.options.leaseRenewInterval + 5); // Avanzar el tiempo
        // Esperar a que la promesa de renewJobLease (si la hay) se resuelva
        await Promise.resolve(); // Permitir que las microtareas se ejecuten
        expect(mockQueue.renewJobLease).toHaveBeenCalledWith(sampleJob.id, worker.options.leaseDuration);
      }

      await processResult; // Esperar a que processJob termine

      expect(mockHandler).toHaveBeenCalledWith(sampleJob.data, expect.anything());
      expect(mockQueue.scripts.completeJob).toHaveBeenCalled();
      if (worker.options.leaseRenewInterval > 0) {
        expect(mockClearInterval).toHaveBeenCalledWith(123); // El intervalo se limpia
      }
    });

    it('processJob: should call handler, then failJob on error, and manage lease interval', async () => {
      const error = new Error('Handler failed!');
      mockHandler.mockRejectedValue(error);
      mockQueue.scripts.handleFailureLua.mockResolvedValue('RETRIED'); // Simular que se reintenta

      const processResult = worker.processJob({ ...sampleJob }, 'w1');
      if (worker.options.leaseRenewInterval > 0) {
        expect(mockSetInterval).toHaveBeenCalledWith(
          expect.any(Function),
          worker.options.leaseRenewInterval
        );
        jest.advanceTimersByTime(worker.options.leaseRenewInterval + 5);
        await Promise.resolve(); 
        expect(mockQueue.renewJobLease).toHaveBeenCalled();
      }
      await processResult;

      expect(mockHandler).toHaveBeenCalled();
      expect(mockQueue.scripts.handleFailureLua).toHaveBeenCalled();
      if (worker.options.leaseRenewInterval > 0) {
        expect(mockClearInterval).toHaveBeenCalledWith(123);
      }
    });
  });

  describe('completeJob method', () => {
    it('should call queue.scripts.completeJob with correct args and emit event', async () => {
      const job = { id: 'c1', data: {} };
      const result = { success: true };
      const processingTime = 123;
      const completedSpy = jest.fn();
      worker.on('job.completed', completedSpy);

      await worker.completeJob(job, result, 'w-id', processingTime);

      expect(mockQueue.scripts.completeJob).toHaveBeenCalledWith(
        mockQueue.keys.queue, mockQueue.keys.active,
        mockQueue.keys.jobs, mockQueue.keys.completed,
        job.id, JSON.stringify(result), expect.any(Number)
      );
      expect(completedSpy).toHaveBeenCalledWith(expect.objectContaining({
        jobId: job.id, result, processingTimeMs: processingTime
      }));
    });
  });

  describe('failJob method', () => {
    const job = { id: 'f1', data: {}, maxRetries: 3, partitionKey: 'p1', attempts: 0 };
    const error = new Error('test error');

    it('should call queue.scripts.handleFailureLua for retry and emit job.retrying', async () => {
      mockQueue.scripts.handleFailureLua.mockResolvedValue('RETRIED');
      const retryingSpy = jest.fn();
      worker.on('job.retrying', retryingSpy);
      const attemptThatFailed = 1;

      await worker.failJob(job, error, 'w-id', attemptThatFailed, 100);

      expect(mockQueue.scripts.handleFailureLua).toHaveBeenCalled();
      expect(retryingSpy).toHaveBeenCalledWith(expect.objectContaining({ attempt: attemptThatFailed, jobId: job.id }));
    });

    it('should call queue.scripts.handleFailureLua for permanent fail and emit job.failed', async () => {
      mockQueue.scripts.handleFailureLua.mockResolvedValue('FAILED_PERMANENTLY');
      const failedSpy = jest.fn();
      worker.on('job.failed', failedSpy);
      const attemptThatFailed = 3; // Último intento

      await worker.failJob(job, error, 'w-id', attemptThatFailed, 100);

      expect(mockQueue.scripts.handleFailureLua).toHaveBeenCalled();
      expect(failedSpy).toHaveBeenCalledWith(expect.objectContaining({ attempt: attemptThatFailed, jobId: job.id }));
    });
  });
});