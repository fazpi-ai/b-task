-- Script para completar un trabajo
local queueKey = KEYS[1]
local activeKey = KEYS[2]
local jobsKey = KEYS[3]
local completedKey = KEYS[4]
local jobId = ARGV[1]
local result = ARGV[2]
local timestamp = ARGV[3]

local jobKey = jobsKey .. ':' .. jobId

-- Mover de active a completed
redis.call('ZREM', activeKey, jobId)
redis.call('ZADD', completedKey, timestamp, jobId)

-- Actualizar estado del trabajo
redis.call('HSET', jobKey, 
    'status', 'completed',
    'result', result,
    'finishedAt', timestamp
)

-- Actualizar contadores
redis.call('HINCRBY', queueKey, 'active', -1)
redis.call('HINCRBY', queueKey, 'completed', 1)

return 1 