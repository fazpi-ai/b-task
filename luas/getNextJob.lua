-- Script para obtener el siguiente trabajo de la cola
local queueKey = KEYS[1]
local jobsKey = KEYS[2]
local waitKey = KEYS[3]
local activeKey = KEYS[4]
local timestamp = ARGV[1]

-- Obtener el trabajo más antiguo de la lista de espera
local jobs = redis.call('ZRANGE', waitKey, 0, 0, 'WITHSCORES')
if #jobs == 0 then
    return nil
end

local jobId = jobs[1]
local jobKey = jobsKey .. ':' .. jobId

-- Verificar que el trabajo existe
if redis.call('EXISTS', jobKey) == 0 then
    redis.call('ZREM', waitKey, jobId)
    return nil
end

-- Obtener datos del trabajo antes de moverlo
local jobData = redis.call('HGETALL', jobKey)
if #jobData == 0 then
    redis.call('ZREM', waitKey, jobId)
    return nil
end

-- Mover de waiting a active
redis.call('ZREM', waitKey, jobId)
redis.call('ZADD', activeKey, timestamp, jobId)

-- Actualizar estado del trabajo
redis.call('HSET', jobKey, 'status', 'active', 'startedAt', timestamp)

-- Actualizar contadores
redis.call('HINCRBY', queueKey, 'waiting', -1)
redis.call('HINCRBY', queueKey, 'active', 1)

-- Devolver jobId y datos
return {jobId, cjson.encode(jobData)} 