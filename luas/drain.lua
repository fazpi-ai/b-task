-- drain.lua
--
-- Descripción:
--   Elimina todos los trabajos y datos asociados con la cola, incluyendo todas las particiones,
--   colas globales (activa, completada, fallida) y contadores.
--
-- KEYS[1]: jobsHashKeyPrefix           (Ej: "queue:myqueue:jobs")
-- KEYS[2]: partitionsSetKey            (Ej: "queue:myqueue:partitions") - SET de todos los nombres de partición.
-- KEYS[3]: partitionWaitingQueuePrefix (Ej: "queue:myqueue:waiting") - Prefijo para las ZSETs de espera de partición.
-- KEYS[4]: globalActiveQueueKey        (Ej: "queue:myqueue:active")
-- KEYS[5]: globalCompletedQueueKey     (Ej: "queue:myqueue:completed")
-- KEYS[6]: globalFailedQueueKey        (Ej: "queue:myqueue:failed")
-- KEYS[7]: globalQueueCountersKey      (Ej: "queue:myqueue") - HASH de contadores globales.

local jobsPrefix = KEYS[1]
local totalJobsDeletedCount = 0

-- Función auxiliar para eliminar trabajos de una ZSET y sus HASHes asociados
local function clearJobQueueAndHashes(queueZsetKey)
    local jobIds = redis.call('ZRANGE', queueZsetKey, 0, -1)
    if #jobIds > 0 then
        local jobHashKeysToDelete = {}
        for i, jobId in ipairs(jobIds) do
            table.insert(jobHashKeysToDelete, jobsPrefix .. ':' .. jobId)
        end
        redis.call('DEL', unpack(jobHashKeysToDelete))
        totalJobsDeletedCount = totalJobsDeletedCount + #jobIds
    end
    redis.call('DEL', queueZsetKey)
end

-- 1. Limpiar trabajos de todas las colas de espera de partición
local partitionNames = redis.call('SMEMBERS', KEYS[2])
local partitionQueueKeysToDelete = {}
table.insert(partitionQueueKeysToDelete, KEYS[2]) -- Añadir el SET de particiones para borrar

for _, partitionName in ipairs(partitionNames) do
    local partitionWaitingQueueKey = KEYS[3] .. ':' .. partitionName
    clearJobQueueAndHashes(partitionWaitingQueueKey) -- Esto ya hace DEL en la ZSET de partición
end


-- 2. Limpiar trabajos de la cola activa global
clearJobQueueAndHashes(KEYS[4])

-- 3. Limpiar trabajos de la cola completada global
clearJobQueueAndHashes(KEYS[5])

-- 4. Limpiar trabajos de la cola fallida global
clearJobQueueAndHashes(KEYS[6])

-- 5. Eliminar el SET de particiones (ya añadido a partitionQueueKeysToDelete) y otras claves principales
--    Podríamos haber hecho DEL KEYS[2], KEYS[4], etc. por separado
--    pero clearJobQueueAndHashes ya borra las ZSETs individuales.
--    Solo nos queda borrar el KEYS[2] (partitionsSetKey) explícitamente si no estaba en un bucle.
redis.call('DEL', KEYS[2])


-- 6. Reiniciar (o eliminar) los contadores globales.
redis.call('HMSET', KEYS[7],
    'waiting', 0,
    'active', 0,
    'completed', 0,
    'failed', 0,
    'totalProcessed', 0,
    'totalFailedPermanently', 0,
    'totalRetried', 0
)

return totalJobsDeletedCount