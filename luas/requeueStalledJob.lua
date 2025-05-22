-- requeueStalledJob.lua (Modificado)
--
-- Descripción: Reencola o falla un job que se considera estancado (lease expirado).
--
-- KEYS[1]: jobsHashKeyPrefix
-- KEYS[2]: globalActiveQueueKey
-- KEYS[3]: partitionWaitingQueuePrefix
-- KEYS[4]: globalFailedQueueKey
-- KEYS[5]: globalQueueCountersKey
-- KEYS[6]: failedLogKey (NUEVA KEY para la lista de logs de trabajos fallidos)
--
-- ARGV[1]: jobId
-- ARGV[2]: stalledErrorMsg (String JSON)
-- ARGV[3]: currentTime
-- ARGV[4]: retryDelayMs
-- ARGV[5]: queueName (NUEVO ARGV con el nombre de la cola)

local jobHashKey = KEYS[1] .. ':' .. ARGV[1]

local jobDetailsArray = redis.call('HGETALL', jobHashKey)
if #jobDetailsArray == 0 then
    redis.call('ZREM', KEYS[2], ARGV[1])
    return 'JOB_HASH_NOT_FOUND'
end

local jobDetails = {}
for i = 1, #jobDetailsArray, 2 do
    jobDetails[jobDetailsArray[i]] = jobDetailsArray[i+1]
end

if jobDetails.status ~= 'active' then
    redis.call('ZREM', KEYS[2], ARGV[1])
    return 'JOB_NOT_ACTIVE_IN_HASH'
end

local attempts = tonumber(jobDetails.attempts or 0)
local maxRetries = tonumber(jobDetails.maxRetries or 3)
local partitionKey = jobDetails.partitionKey

if not partitionKey then
    attempts = maxRetries -- Forzar fallo si no hay partitionKey
end

local removedFromActive = redis.call('ZREM', KEYS[2], ARGV[1])

if attempts < maxRetries then
    -- Reencolar para reintento
    local newAttempts = attempts + 1
    local nextTryTimestamp = tonumber(ARGV[3]) + tonumber(ARGV[4])
    local partitionSpecificWaitingQueueKey = KEYS[3] .. ':' .. partitionKey

    redis.call('HSET', jobHashKey,
        'status', 'waiting',
        'attempts', newAttempts,
        'lastError', ARGV[2], -- stalledErrorMsg
        'nextTry', nextTryTimestamp,
        'leaseExpiresAt', ''
    )
    redis.call('ZADD', partitionSpecificWaitingQueueKey, nextTryTimestamp, ARGV[1])

    if removedFromActive == 1 then redis.call('HINCRBY', KEYS[5], 'active', -1) end
    redis.call('HINCRBY', KEYS[5], 'waiting', 1)
    redis.call('HINCRBY', KEYS[5], 'totalRetried', 1)
    return 'REQUEUED_STALLED'
else
    -- Fallo definitivo
    redis.call('HSET', jobHashKey,
        'status', 'failed',
        'failedAt', ARGV[3], -- currentTime
        'lastError', ARGV[2], -- stalledErrorMsg
        'attempts', attempts,
        'leaseExpiresAt', ''
    )
    redis.call('ZADD', KEYS[4], ARGV[3], ARGV[1]) -- ARGV[3] es currentTime

    if removedFromActive == 1 then redis.call('HINCRBY', KEYS[5], 'active', -1) end
    redis.call('HINCRBY', KEYS[5], 'failed', 1)
    redis.call('HINCRBY', KEYS[5], 'totalFailedPermanently', 1)

    -- Registrar en el log de fallos
    -- jobDetails ya contiene 'data' y 'partitionKey'
    local failedJobEntry = {
        jobId = ARGV[1],
        queueName = ARGV[5], -- Nombre de la cola
        partitionKey = jobDetails.partitionKey,
        data = jobDetails.data, -- Payload original del job (ya es un string JSON)
        error = cjson.decode(ARGV[2]), -- stalledErrorMsg deserializado
        failedAt = tonumber(ARGV[3]) -- Timestamp del fallo (currentTime)
    }
    redis.call('LPUSH', KEYS[6], cjson.encode(failedJobEntry))

    return 'FAILED_STALLED'
end