import { jest } from '@jest/globals';
import Queue from '../../core/Queue.js';
import { loadScripts } from '../../utils/index.js';
import Redis from 'ioredis';
import { v4 as uuidv4 } from 'uuid';

jest.mock('ioredis');
jest.mock('uuid');
jest.mock('../../utils/index.js', () => ({
    loadScripts: jest.fn()
}));

describe('Queue', () => {
    let queue;
    let mockRedis;
    let mockUuidCounter = 0;
    const mockLuaScripts = {
        addJob: 'addJob-script',
        getNextJob: 'getNextJob-script',
        completeJob: 'completeJob-script',
        handleFailure: 'handleFailure-script',
        renewJobLease: 'renewJobLease-script',
        requeueStalledJob: 'requeueStalledJob-script',
        drain: 'drain-script'
    };

    beforeEach(() => {
        // Limpiar todos los mocks
        jest.clearAllMocks();
        mockUuidCounter = 0;

        // Mock de uuid
        uuidv4.mockImplementation(() => `job-id-${++mockUuidCounter}`);

        // Configurar el mock de Redis
        mockRedis = {
            exists: jest.fn().mockResolvedValue(0),
            hmset: jest.fn().mockResolvedValue('OK'),
            hgetall: jest.fn(),
            defineCommand: jest.fn().mockImplementation((name, { lua }) => {
                // Cada vez que se define un comando, devolvemos una nueva función mock
                if (name === 'addJob') {
                    mockRedis[name] = jest.fn().mockImplementation((_, __, ___, ____, jobId) => Promise.resolve(jobId));
                } else {
                    mockRedis[name] = jest.fn().mockResolvedValue('OK');
                }
                return mockRedis[name];
            }),
            quit: jest.fn().mockResolvedValue('OK'),
            on: jest.fn()
        };

        // Hacer que el constructor de Redis devuelva nuestro mock
        Redis.mockImplementation(() => mockRedis);

        // Configurar el mock de loadScripts
        loadScripts.mockResolvedValue(mockLuaScripts);

        // Crear una nueva instancia de Queue para cada test
        queue = new Queue('test-queue');
    });

    describe('Constructor', () => {
        it('should set name and default options', () => {
            expect(queue.name).toBe('test-queue');
            expect(queue.options.defaultJobOptions.leaseDuration).toBe(30000);
        });

        it('should create Redis client with custom options', () => {
            const customRedisOptions = {
                host: 'custom-host',
                port: 6380
            };
            const queueWithCustomRedis = new Queue('test-queue', { redis: customRedisOptions });
            expect(Redis).toHaveBeenCalledWith(expect.objectContaining(customRedisOptions));
        });
    });

    describe('init', () => {
        it('should load scripts and define Redis commands', async () => {
            await queue.init();

            expect(loadScripts).toHaveBeenCalled();
            expect(mockRedis.defineCommand).toHaveBeenCalledWith('addJob', expect.any(Object));
            expect(mockRedis.defineCommand).toHaveBeenCalledWith('getNextJob', expect.any(Object));
            expect(mockRedis.defineCommand).toHaveBeenCalledWith('completeJob', expect.any(Object));
            expect(mockRedis.defineCommand).toHaveBeenCalledWith('handleFailureLua', expect.any(Object));
        });

        it('should initialize counters if they do not exist', async () => {
            mockRedis.exists.mockResolvedValue(0);
            await queue.init();

            expect(mockRedis.exists).toHaveBeenCalledWith(queue.keys.queue);
            expect(mockRedis.hmset).toHaveBeenCalledWith(
                queue.keys.queue,
                expect.objectContaining({
                    waiting: 0,
                    totalProcessed: 0
                })
            );
        });

        it('should not initialize counters if they already exist', async () => {
            mockRedis.exists.mockResolvedValue(1);
            await queue.init();

            expect(mockRedis.exists).toHaveBeenCalledWith(queue.keys.queue);
            expect(mockRedis.hmset).not.toHaveBeenCalled();
        });
    });

    describe('add', () => {
        beforeEach(async () => {
            await queue.init();
        });

        it('should add a job with default options', async () => {
            const jobData = { task: 'test' };

            const jobId = await queue.add(jobData);

            expect(jobId).toBe('job-id-1');
            expect(mockRedis.addJob).toHaveBeenCalledWith(
                queue.keys.jobs,
                queue.getPartitionKey('default'), // partition key
                queue.keys.partitions,
                queue.keys.queue,
                'job-id-1', // job id
                expect.any(String), // serialized job data
                expect.any(Number), // timestamp
                'default' // default partition
            );
        });

        it('should add a job with custom options', async () => {
            const jobData = { task: 'test' };
            const options = {
                partitionKey: 'custom-partition',
                delay: 1000
            };

            const jobId = await queue.add(jobData, options);

            expect(jobId).toBe('job-id-2');
            expect(mockRedis.addJob).toHaveBeenCalledWith(
                queue.keys.jobs,
                queue.getPartitionKey('custom-partition'),
                queue.keys.partitions,
                queue.keys.queue,
                'job-id-2',
                expect.any(String),
                expect.any(Number),
                'custom-partition'
            );
        });
    });

    describe('getJobById', () => {
        it('should return parsed job data', async () => {
            const mockJobData = {
                id: '123',
                data: JSON.stringify({ task: 'test' }),
                timestamp: '1000'
            };
            mockRedis.hgetall.mockResolvedValue(mockJobData);

            const job = await queue.getJobById('123');

            expect(mockRedis.hgetall).toHaveBeenCalledWith(`${queue.keys.jobs}:123`);
            expect(job.data.task).toBe('test');
            expect(job.timestamp).toBe(1000);
        });

        it('should return null if job not found', async () => {
            mockRedis.hgetall.mockResolvedValue(null);

            const job = await queue.getJobById('unknown');

            expect(job).toBeNull();
        });
    });
});