-- completeJob.lua
--
-- KEYS[1]: globalQueueCountersKey
-- KEYS[2]: globalActiveQueueKey
-- KEYS[3]: jobsHashKeyPrefix
-- KEYS[4]: globalCompletedQueueKey
--
-- ARGV[1]: jobId
-- ARGV[2]: serializedResult
-- ARGV[3]: timestamp (completion time)

local jobHashKey = KEYS[3] .. ':' .. ARGV[1]
local removedFromActive = redis.call('ZREM', KEYS[2], ARGV[1])

redis.call('ZADD', KEYS[4], ARGV[3], ARGV[1])

redis.call('HSET', jobHashKey,
    'status', 'completed',
    'result', ARGV[2],
    'completedAt', ARGV[3],
    'leaseExpiresAt', '' -- Limpiar lease
)

if removedFromActive == 1 then
    redis.call('HINCRBY', KEYS[1], 'active', -1)
end
redis.call('HINCRBY', KEYS[1], 'completed', 1)
redis.call('HINCRBY', KEYS[1], 'totalProcessed', 1) -- Nueva métrica

return 1