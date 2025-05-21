-- getNextJob.lua
--
-- KEYS[1]: partitionSpecificWaitingQueueKey
-- KEYS[2]: globalActiveQueueKey
-- KEYS[3]: jobsHashKeyPrefix
-- KEYS[4]: globalQueueCountersKey
--
-- ARGV[1]: currentTime (timestamp actual)
-- ARGV[2]: leaseDurationMilliseconds (cuánto dura el "alquiler" del job)

local jobs = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
if #jobs == 0 then
    return nil
end

local jobId = jobs[1]
local jobScore = tonumber(jobs[2])

if jobScore > tonumber(ARGV[1]) then
    return nil -- Job no está listo aún
end

local jobHashKey = KEYS[3] .. ':' .. jobId
if redis.call('EXISTS', jobHashKey) == 0 then
    redis.call('ZREM', KEYS[1], jobId)
    return 'STALE_JOB_REMOVED'
end

local removedFromWaiting = redis.call('ZREM', KEYS[1], jobId)
if removedFromWaiting == 0 then
    return nil
end

local leaseExpiresAt = tonumber(ARGV[1]) + tonumber(ARGV[2])

-- Usar leaseExpiresAt como score en la cola activa
redis.call('ZADD', KEYS[2], leaseExpiresAt, jobId)

redis.call('HSET', jobHashKey,
    'status', 'active',
    'startedAt', ARGV[1],
    'leaseExpiresAt', leaseExpiresAt -- Guardar cuándo expira el alquiler
)

redis.call('HINCRBY', KEYS[4], 'waiting', -1)
redis.call('HINCRBY', KEYS[4], 'active', 1)

local jobDataArray = redis.call('HGETALL', jobHashKey)
local jobDataMap = {}
for i = 1, #jobDataArray, 2 do
    jobDataMap[jobDataArray[i]] = jobDataArray[i+1]
end
return cjson.encode(jobDataMap)