-- handleFailure.lua
--
-- KEYS[1]: jobsHashKeyPrefix
-- KEYS[2]: globalActiveQueueKey
-- KEYS[3]: partitionSpecificWaitingQueueKey
-- KEYS[4]: globalFailedQueueKey
-- KEYS[5]: globalQueueCountersKey
--
-- ARGV[1]: jobId
-- ARGV[2]: serializedErrorDetails
-- ARGV[3]: currentTimestamp
-- ARGV[4]: attemptThatFailed
-- ARGV[5]: maxRetries
-- ARGV[6]: nextTryTimestamp (para reintentos)
-- ARGV[7]: partitionKey (opcional, no usado si KEYS[3] es la clave completa)

local jobHashKey = KEYS[1] .. ':' .. ARGV[1]
local attemptThatFailedNum = tonumber(ARGV[4])
local maxRetriesNum = tonumber(ARGV[5])

local removedFromActive = redis.call('ZREM', KEYS[2], ARGV[1])

if attemptThatFailedNum < maxRetriesNum then
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
    redis.call('HINCRBY', KEYS[5], 'totalRetried', 1) -- Nueva métrica
    return 'RETRIED'
else
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
    redis.call('HINCRBY', KEYS[5], 'totalFailedPermanently', 1) -- Nueva métrica
    return 'FAILED_PERMANENTLY'
end