-- Script para agregar un trabajo a la cola
local jobId = ARGV[1]
local jobData = ARGV[2]
local timestamp = ARGV[3]
local queueKey = KEYS[1]
local jobsKey = KEYS[2]
local waitKey = KEYS[3]
local activeKey = KEYS[4]

-- Crear la clave completa del trabajo
local jobKey = jobsKey .. ':' .. jobId

-- Guardar datos del trabajo en un hash
redis.call('HSET', jobKey, 
    'id', jobId,
    'data', jobData,
    'timestamp', timestamp,
    'status', 'waiting',
    'createdAt', timestamp
)

-- Agregar a la lista de espera con timestamp como score
redis.call('ZADD', waitKey, timestamp, jobId)

-- Incrementar contador de trabajos
redis.call('HINCRBY', queueKey, 'waiting', 1)

-- Verificar que los datos se guardaron correctamente
local savedData = redis.call('HGETALL', jobKey)
if #savedData == 0 then
    return nil
end

return jobId 