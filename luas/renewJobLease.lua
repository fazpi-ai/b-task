-- renewJobLease.lua
--
-- Descripción: Renueva el "alquiler" (lease) de un job activo.
--
-- KEYS[1]: jobsHashKeyPrefix
-- KEYS[2]: globalActiveQueueKey
--
-- ARGV[1]: jobId
-- ARGV[2]: leaseDurationMilliseconds (cuánto extender el alquiler desde ahora)
-- ARGV[3]: currentTime

local jobHashKey = KEYS[1] .. ':' .. ARGV[1]
local newLeaseExpiresAt = tonumber(ARGV[3]) + tonumber(ARGV[2])

-- Verificar que el job está aún en la cola activa (ZSCORE devuelve el score o nil)
-- y que su HASH existe
if redis.call('EXISTS', jobHashKey) == 0 then
  return 0 -- Job HASH no encontrado
end

local currentScoreInActive = redis.call('ZSCORE', KEYS[2], ARGV[1])
if not currentScoreInActive then
  return 0 -- Job no está (o ya no está) en la cola activa
end

-- Actualizar el HASH del job y el score en la ZSET activa
redis.call('HSET', jobHashKey, 'leaseExpiresAt', newLeaseExpiresAt)
-- Usar ZADD con la opción XX para actualizar solo si el miembro ya existe.
-- Esto es importante para no re-añadir un job si fue removido entre el ZSCORE y el ZADD.
local updated = redis.call('ZADD', KEYS[2], 'XX', newLeaseExpiresAt, ARGV[1])

if updated == 0 then
  -- No se actualizó, podría significar que el job fue removido de la activa justo ahora.
  -- Opcionalmente, revertir el HSET de leaseExpiresAt si es crítico.
  -- Por ahora, lo dejamos ya que el job ya no está activo.
  return 0
end

return 1 -- Lease renovado