-- requeueStalledJob.lua
--
-- Descripción: Reencola o falla un job que se considera estancado (lease expirado).
--
-- KEYS[1]: jobsHashKeyPrefix
-- KEYS[2]: globalActiveQueueKey
-- KEYS[3]: partitionWaitingQueuePrefix -- Prefijo, ej: "queue:myqueue:waiting"
-- KEYS[4]: globalFailedQueueKey
-- KEYS[5]: globalQueueCountersKey
--
-- ARGV[1]: jobId
-- ARGV[2]: stalledErrorMsg (String JSON)
-- ARGV[3]: currentTime
-- ARGV[4]: retryDelayMs (cuánto esperar antes del reintento si se reencola)

local jobHashKey = KEYS[1] .. ':' .. ARGV[1]

-- Obtener detalles del job
local jobDetailsArray = redis.call('HGETALL', jobHashKey)
if #jobDetailsArray == 0 then
    -- Si el HASH no existe, pero el jobID estaba en la activa (lo cual es raro aquí, ya que este script
    -- se llama DESPUÉS de encontrar un ID en la activa), intentamos removerlo de activa.
    redis.call('ZREM', KEYS[2], ARGV[1])
    return 'JOB_HASH_NOT_FOUND'
end

local jobDetails = {}
for i = 1, #jobDetailsArray, 2 do
    jobDetails[jobDetailsArray[i]] = jobDetailsArray[i+1]
end

-- Solo procesar si el status en el HASH es 'active'.
-- Esto es una salvaguarda por si el job fue procesado (completado/fallido)
-- pero por alguna razón su ZREM de la activa falló o fue revertido.
if jobDetails.status ~= 'active' then
    -- Si el status no es active, pero el job está en la ZSET activa, es una inconsistencia.
    -- Lo removemos de la activa para limpiar.
    redis.call('ZREM', KEYS[2], ARGV[1])
    return 'JOB_NOT_ACTIVE_IN_HASH'
end

local attempts = tonumber(jobDetails.attempts or 0)
local maxRetries = tonumber(jobDetails.maxRetries or 3) -- Usar un default si no está
local partitionKey = jobDetails.partitionKey

if not partitionKey then
    -- No se puede reencolar sin partitionKey, marcar como fallido directamente.
    -- Forzamos que attempts sea igual a maxRetries para que entre en el bloque de fallo.
    attempts = maxRetries
end

-- Remover de la cola activa es el primer paso seguro
local removedFromActive = redis.call('ZREM', KEYS[2], ARGV[1])
-- Si removedFromActive es 0, significa que ya no estaba en la ZSET activa,
-- lo cual contradice cómo se encontró este jobId (escaneando la ZSET activa).
-- Esto podría indicar una condición de carrera extrema o que el job fue procesado
-- casi simultáneamente por otro worker/proceso.
-- Aún así, actualizaremos el HASH para reflejar que se intentó manejar como estancado.

if attempts < maxRetries then
    -- Reencolar para reintento
    local newAttempts = attempts + 1
    local nextTryTimestamp = tonumber(ARGV[3]) + tonumber(ARGV[4])
    local partitionSpecificWaitingQueueKey = KEYS[3] .. ':' .. partitionKey

    redis.call('HSET', jobHashKey,
        'status', 'waiting',
        'attempts', newAttempts,
        'lastError', ARGV[2],
        'nextTry', nextTryTimestamp,
        'leaseExpiresAt', '' -- Limpiar lease
    )
    redis.call('ZADD', partitionSpecificWaitingQueueKey, nextTryTimestamp, ARGV[1])

    if removedFromActive == 1 then redis.call('HINCRBY', KEYS[5], 'active', -1) end
    redis.call('HINCRBY', KEYS[5], 'waiting', 1)
    redis.call('HINCRBY', KEYS[5], 'totalRetried', 1) -- Contar como reintento por estancamiento
    return 'REQUEUED_STALLED'
else
    -- Fallo definitivo
    redis.call('HSET', jobHashKey,
        'status', 'failed',
        'failedAt', ARGV[3],
        'lastError', ARGV[2],
        'attempts', attempts, -- Mantener el número de intentos que llevó al fallo
        'leaseExpiresAt', '' -- Limpiar lease
    )
    redis.call('ZADD', KEYS[4], ARGV[3], ARGV[1])

    if removedFromActive == 1 then redis.call('HINCRBY', KEYS[5], 'active', -1) end
    redis.call('HINCRBY', KEYS[5], 'failed', 1)
    redis.call('HINCRBY', KEYS[5], 'totalFailedPermanently', 1)
    return 'FAILED_STALLED'
end