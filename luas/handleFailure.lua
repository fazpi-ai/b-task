-- handleFailure.lua (Modificado)
--
-- KEYS[1]: jobsHashKeyPrefix
-- KEYS[2]: globalActiveQueueKey
-- KEYS[3]: partitionSpecificWaitingQueueKey
-- KEYS[4]: globalFailedQueueKey
-- KEYS[5]: globalQueueCountersKey
-- KEYS[6]: failedLogKey (NUEVA KEY para la lista de logs de trabajos fallidos)
--
-- ARGV[1]: jobId
-- ARGV[2]: serializedErrorDetails (JSON string)
-- ARGV[3]: currentTimestamp
-- ARGV[4]: attemptThatFailed
-- ARGV[5]: maxRetries
-- ARGV[6]: nextTryTimestamp (para reintentos)
-- ARGV[7]: partitionKey
-- ARGV[8]: queueName (NUEVO ARGV con el nombre de la cola)

local jobHashKey = KEYS[1] .. ':' .. ARGV[1]
local attemptThatFailedNum = tonumber(ARGV[4])
local maxRetriesNum = tonumber(ARGV[5])

local removedFromActive = redis.call('ZREM', KEYS[2], ARGV[1])

if attemptThatFailedNum < maxRetriesNum then
    -- Reintento
    redis.call('HSET', jobHashKey,
        'attempts', attemptThatFailedNum,
        'lastError', ARGV[2],
        'status', 'waiting',
        'nextTry', ARGV[6],
        'leaseExpiresAt', '' -- Limpiar lease
    )
    redis.call('ZADD', KEYS[3], ARGV[6], ARGV[1])
    if removedFromActive == 1 then
        redis.call('HINCRBY', KEYS[5], 'active', -1)
    end
    redis.call('HINCRBY', KEYS[5], 'waiting', 1)
    redis.call('HINCRBY', KEYS[5], 'totalRetried', 1)
    return 'RETRIED'
else
    -- Fallo Permanente
    redis.call('HSET', jobHashKey,
        'attempts', attemptThatFailedNum,
        'lastError', ARGV[2],
        'failedAt', ARGV[3],
        'status', 'failed',
        'leaseExpiresAt', '' -- Limpiar lease
    )
    redis.call('ZADD', KEYS[4], ARGV[3], ARGV[1])
    if removedFromActive == 1 then
        redis.call('HINCRBY', KEYS[5], 'active', -1)
    end
    redis.call('HINCRBY', KEYS[5], 'failed', 1)
    redis.call('HINCRBY', KEYS[5], 'totalFailedPermanently', 1)

    -- Registrar en el log de fallos
    local jobOriginalData = redis.call('HGET', jobHashKey, 'data') -- Obtener el 'data' original del job

    local failedJobEntry = {
        jobId = ARGV[1],
        queueName = ARGV[8], -- Nombre de la cola
        partitionKey = ARGV[7], -- Partition key del job
        data = jobOriginalData, -- Payload original del job (ya es un string JSON)
        error = cjson.decode(ARGV[2]), -- Detalles del error deserializados
        failedAt = tonumber(ARGV[3]) -- Timestamp del fallo
    }
    redis.call('LPUSH', KEYS[6], cjson.encode(failedJobEntry))

    return 'FAILED_PERMANENTLY'
end