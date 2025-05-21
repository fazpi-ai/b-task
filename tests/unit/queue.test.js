import { jest } from '@jest/globals';
import Queue from '../../core/Queue.js';
import { loadScripts } from '../../utils/index.js';
import Redis from 'ioredis';
import { v4 as uuidv4 } from 'uuid';

// Mockear módulos
jest.mock('ioredis');
jest.mock('uuid');
jest.mock('../../utils/index.js', () => ({
    loadScripts: jest.fn()
}));

describe('Queue', () => {
    let queue; // Instancia de Queue bajo test
    let mockRedis; // Instancia mock de Redis asociada con 'queue'
    let mockUuidCounter; // Contador para IDs de job predecibles

    const mockLuaScriptsLoaded = {
        addJob: 'addJob-script-content',
        getNextJob: 'getNextJob-script-content',
        completeJob: 'completeJob-script-content',
        handleFailure: 'handleFailure-script-content', // El archivo es handleFailure.lua
        renewJobLease: 'renewJobLease-script-content',
        requeueStalledJob: 'requeueStalledJob-script-content',
        drain: 'drain-script-content'
    };

    // Este beforeAll se ejecuta una vez para toda la suite 'Queue'
    // Ideal para configurar mocks que deben persistir o tener estado a través de múltiples 'describe' o 'it'
    beforeAll(() => {
        mockUuidCounter = 0; // Inicializar el contador de UUIDs una vez
        uuidv4.mockImplementation(() => `job-id-${++mockUuidCounter}`);
    });

    beforeEach(() => {
        // Limpiar el mock de Redis (llamadas e instancias) y loadScripts antes de CADA test 'it'
        Redis.mockClear();
        loadScripts.mockClear().mockResolvedValue(mockLuaScriptsLoaded);
        // NO reseteamos mockUuidCounter aquí para que la secuencia continúe a través de los tests
        // si están en el mismo nivel de describe o si el beforeAll superior lo maneja.
        // Sin embargo, para mayor aislamiento entre tests, si cada 'it' debe ser independiente,
        // se podría resetear aquí o en un beforeAll del describe específico.
        // Con el beforeAll de arriba, la secuencia será continua en todo el archivo.

        // Configurar el mock de la instancia de Redis
        // Esta función se llamará cada vez que se haga 'new Redis()'
        mockRedis = {
            exists: jest.fn().mockResolvedValue(0), // Default: contadores no existen
            hmset: jest.fn().mockResolvedValue('OK'),
            hgetall: jest.fn().mockResolvedValue(null), // Default: job no encontrado
            defineCommand: jest.fn().mockImplementation((name, { lua }) => {
                const scriptMock = jest.fn().mockName(`lua-${name}`);
                // Configurar el comportamiento por defecto de los scripts mockeados
                if (name === 'addJob') {
                    // El script addJob mockeado devuelve el jobId que se le pasó
                    scriptMock.mockImplementation((keys, args, jobIdArg) => {
                        // En la implementación real, los argumentos son pasados de forma diferente.
                        // Para el mock, asumimos que el último argumento relevante es el ID.
                        // En nuestro script real, jobId es ARGV[1].
                        // En la llamada de ioredis, es (KEYS..., ARGV...).
                        // El mock de la función addJob en ioredis sería:
                        // (key1, key2, key3, key4, arg1_jobId, arg2_data, arg3_score, arg4_partition)
                        // Por lo tanto, el 5to argumento (índice 4) de la llamada al script es jobIdArg.
                        // O si es el primer ARGV, sería el primer elemento después de las KEYS.
                        // El mock actual de defineCommand en Queue.js lo pasa bien.
                        // Aquí, para el mock que simula la ejecución del script:
                        const actualJobIdPassedToScript = arguments[4]; // ARGV[1] es el 5to argumento a `redis.scriptName(...)`
                        return Promise.resolve(actualJobIdPassedToScript);
                    });
                } else {
                    scriptMock.mockResolvedValue('OK'); // Default para otros scripts
                }
                return scriptMock;
            }),
            quit: jest.fn().mockResolvedValue('OK'),
            on: jest.fn(),
            // Añadir otros métodos que Queue.js podría llamar directamente
            zrangebyscore: jest.fn().mockResolvedValue([]),
            multi: jest.fn(function() {
                const commands = [];
                const multiInstance = {
                    zcard: jest.fn(function() { commands.push(['zcard']); return this; }),
                    exec: jest.fn(() => Promise.resolve(commands.map(() => [null, 0]))),
                };
                return multiInstance;
            }),
        };
        Redis.mockImplementation(() => mockRedis);

        // Crear una nueva instancia de Queue para cada test
        queue = new Queue('test-queue');
    });

    describe('Constructor', () => {
        it('should set name and default options', () => {
            expect(queue.name).toBe('test-queue');
            expect(queue.options.defaultJobOptions.leaseDuration).toBe(30000);
        });

        it('should create Redis client with custom options', () => {
            const customRedisOptions = { host: 'custom-host', port: 6380 };
            new Queue('test-queue-custom', { redis: customRedisOptions }); // Llama a new Redis()
            // Verificar la última llamada a Redis (constructor mock)
            expect(Redis).toHaveBeenLastCalledWith(expect.objectContaining(customRedisOptions));
        });
    });

    describe('init', () => {
        it('should load scripts and define Redis commands, populating queue.scripts', async () => {
            mockRedis.exists.mockResolvedValueOnce(0); // Para la verificación de contadores
            await queue.init();

            expect(loadScripts).toHaveBeenCalledTimes(1);
            expect(mockRedis.defineCommand).toHaveBeenCalledWith('addJob', expect.objectContaining({ lua: mockLuaScriptsLoaded.addJob, numberOfKeys: 4 }));
            expect(mockRedis.defineCommand).toHaveBeenCalledWith('getNextJob', expect.objectContaining({ lua: mockLuaScriptsLoaded.getNextJob, numberOfKeys: 4 }));
            expect(mockRedis.defineCommand).toHaveBeenCalledWith('completeJob', expect.objectContaining({ lua: mockLuaScriptsLoaded.completeJob, numberOfKeys: 4 }));
            expect(mockRedis.defineCommand).toHaveBeenCalledWith('handleFailureLua', expect.objectContaining({ lua: mockLuaScriptsLoaded.handleFailure, numberOfKeys: 5 }));
            expect(mockRedis.defineCommand).toHaveBeenCalledWith('renewJobLease', expect.objectContaining({ lua: mockLuaScriptsLoaded.renewJobLease, numberOfKeys: 2 }));
            expect(mockRedis.defineCommand).toHaveBeenCalledWith('requeueStalledJob', expect.objectContaining({ lua: mockLuaScriptsLoaded.requeueStalledJob, numberOfKeys: 5 }));
            expect(mockRedis.defineCommand).toHaveBeenCalledWith('drain', expect.objectContaining({ lua: mockLuaScriptsLoaded.drain, numberOfKeys: 7 }));

            // Verificar que queue.scripts ahora contiene las funciones mock devueltas por defineCommand
            expect(jest.isMockFunction(queue.scripts.addJob)).toBe(true);
            expect(jest.isMockFunction(queue.scripts.getNextJob)).toBe(true);
            // ... y para los demás scripts
        });

        it('should initialize counters if they do not exist', async () => {
            mockRedis.exists.mockResolvedValueOnce(0); // Contadores no existen
            await queue.init();

            expect(mockRedis.exists).toHaveBeenCalledWith(queue.keys.queue);
            expect(mockRedis.hmset).toHaveBeenCalledWith(
                queue.keys.queue,
                expect.objectContaining({
                    waiting: 0, active: 0, completed: 0, failed: 0,
                    totalProcessed: 0, totalFailedPermanently: 0, totalRetried: 0,
                })
            );
        });

        it('should not initialize counters if they already exist', async () => {
            mockRedis.exists.mockResolvedValueOnce(1); // Contadores SÍ existen
            mockRedis.hmset.mockClear(); // Limpiar llamadas previas de otros tests

            await queue.init();

            expect(mockRedis.exists).toHaveBeenCalledWith(queue.keys.queue);
            expect(mockRedis.hmset).not.toHaveBeenCalled();
        });
    });

    describe('add', () => {
        // El beforeAll en el describe('Queue') ya configura mockUuidCounter y uuidv4
        // Si necesitas una secuencia de ID específica solo para los tests de 'add',
        // puedes poner un beforeAll aquí para resetear mockUuidCounter a 0.
        // Para este ejemplo, asumiremos que la secuencia continúa desde tests anteriores.

        beforeEach(async () => {
            // init es crucial para que queue.scripts.addJob sea la función mock
            mockRedis.exists.mockResolvedValueOnce(1); // Asumir contadores existen para init
            await queue.init();
            // Ahora queue.scripts.addJob es la función mock que defineCommand retornó.
            // Su implementación ya está mockeada por defineCommand para devolver el jobId.
        });

        it('should add a job with default options and return the jobId', async () => {
            const currentExpectedId = `job-id-${mockUuidCounter + 1}`; // El ID que uuidv4 generará
            const jobData = { task: 'test default' };
            
            const returnedJobId = await queue.add(jobData); // Llama a uuidv4, mockUuidCounter se incrementa

            expect(returnedJobId).toBe(currentExpectedId);
            expect(queue.scripts.addJob).toHaveBeenCalledWith(
                queue.keys.jobs,
                queue.getPartitionKey('default'),
                queue.keys.partitions,
                queue.keys.queue,
                currentExpectedId, // El jobId generado y pasado al script
                expect.any(String), 
                expect.any(Number),
                'default'
            );
        });

        it('should add a job with custom options and return the jobId', async () => {
            const currentExpectedId = `job-id-${mockUuidCounter + 1}`; // El ID que uuidv4 generará
            const jobData = { task: 'test custom' };
            const options = {
                partitionKey: 'custom-partition',
                delay: 1000
            };
            const creationTime = Date.now(); // Necesitamos controlar el tiempo para el score
            jest.spyOn(Date, 'now').mockReturnValue(creationTime);

            const returnedJobId = await queue.add(jobData, options); // Llama a uuidv4

            expect(returnedJobId).toBe(currentExpectedId);
            expect(queue.scripts.addJob).toHaveBeenCalledWith(
                queue.keys.jobs,
                queue.getPartitionKey('custom-partition'),
                queue.keys.partitions,
                queue.keys.queue,
                currentExpectedId, // El jobId generado
                expect.stringContaining(`"id":"${currentExpectedId}"`), // Verificar que el ID está en los datos serializados
                creationTime + options.delay, // El score correcto
                'custom-partition'
            );
            Date.now.mockRestore();
        });
    });

    describe('getJobById', () => {
        // No se necesita init() aquí si getJobById no usa queue.scripts
        it('should return parsed job data if found', async () => {
            const now = Date.now();
            const mockJobDataFromRedis = {
                id: 'job-123',
                data: JSON.stringify({ task: 'fetch test' }),
                timestamp: String(now),
                attempts: '0',
                maxRetries: '3',
                status: 'waiting',
            };
            mockRedis.hgetall.mockResolvedValue(mockJobDataFromRedis);

            const job = await queue.getJobById('job-123');

            expect(mockRedis.hgetall).toHaveBeenCalledWith(`${queue.keys.jobs}:job-123`);
            expect(job).not.toBeNull();
            expect(job.id).toBe('job-123');
            expect(job.data.task).toBe('fetch test');
            expect(job.timestamp).toBe(now);
            expect(job.attempts).toBe(0);
        });

        it('should return null if job not found (hgetall returns null)', async () => {
            mockRedis.hgetall.mockResolvedValue(null);
            const job = await queue.getJobById('unknown-id');
            expect(job).toBeNull();
        });

        it('should return null if job not found (hgetall returns empty object)', async () => {
            mockRedis.hgetall.mockResolvedValue({});
            const job = await queue.getJobById('unknown-id-2');
            expect(job).toBeNull();
        });
    });
    // ... (más tests para getNextJobFromPartition, renewJobLease, etc.)
});