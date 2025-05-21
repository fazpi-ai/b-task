import { jest } from '@jest/globals';

const mockRedisInstance = {
  exists: jest.fn(),
  hmset: jest.fn().mockResolvedValue('OK'),
  hgetall: jest.fn(),
  defineCommand: jest.fn().mockImplementation((name, { lua }) => {
    // Cada vez que se define un comando, devolvemos una nueva función mock
    mockRedisInstance[name] = jest.fn().mockResolvedValue('OK');
    return mockRedisInstance[name];
  }),
  quit: jest.fn().mockResolvedValue('OK'),
  on: jest.fn(),
};

const IORedisMock = jest.fn().mockImplementation(() => mockRedisInstance);

// Añadir las propiedades estáticas y métodos al constructor mock
IORedisMock.prototype.Command = {
  setArgumentTransformer: jest.fn(),
  setReplyTransformer: jest.fn(),
};

export default IORedisMock; 