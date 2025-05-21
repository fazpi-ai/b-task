import { jest } from '@jest/globals';

// Mock de Redis
const mockRedis = {
  exists: jest.fn(),
  hmset: jest.fn().mockResolvedValue('OK'),
  hgetall: jest.fn(),
  defineCommand: jest.fn(),
  quit: jest.fn().mockResolvedValue('OK'),
  on: jest.fn(),
};

jest.mock('ioredis', () => {
  return jest.fn().mockImplementation(() => mockRedis);
});

global.mockRedis = mockRedis; 