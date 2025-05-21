-- addJob.lua
--
-- Descripción:
--   Agrega un nuevo trabajo a una cola de partición específica, guarda los detalles completos
--   del trabajo en un HASH individual, registra la partición si es nueva, y actualiza
--   el contador global de trabajos en espera.
--
-- KEYS[1]: jobsHashKeyPrefix           (Ej: "queue:myqueue:jobs") - Prefijo para las claves HASH de cada job.
-- KEYS[2]: partitionSpecificWaitingQueueKey (Ej: "queue:myqueue:waiting:partition123") - Clave ZSET de la cola de espera para la partición.
-- KEYS[3]: partitionsSetKey            (Ej: "queue:myqueue:partitions") - Clave SET global que almacena todos los nombres de particiones activas.
-- KEYS[4]: globalQueueCountersKey      (Ej: "queue:myqueue") - Clave HASH para los contadores globales de la cola (waiting, active, etc.).
--
-- ARGV[1]: jobId                       (String) - El ID único del trabajo.
-- ARGV[2]: serializedFullJobData       (String JSON) - Objeto jobData completo, serializado como string JSON.
--                                                    (Debe incluir: id, data (stringified), timestamp, attempts, maxRetries, status, partitionKey)
-- ARGV[3]: scoreForWaitingQueue        (Number) - Score para el ZADD en la cola de espera (puede ser futuro para jobs diferidos).
-- ARGV[4]: partitionKey                (String) - El nombre/identificador de la partición.

-- Construir la clave HASH única para este trabajo
local jobHashKey = KEYS[1] .. ':' .. ARGV[1]

-- Decodificar el objeto jobData completo desde el string JSON
local jobDetails = cjson.decode(ARGV[2])

-- Guardar todos los campos del jobData en el HASH del trabajo.
redis.call('HSET', jobHashKey,
    'id', jobDetails.id,
    'data', jobDetails.data, -- Este 'data' interno ya debería ser un string (payload original JSON.stringified)
    'timestamp', jobDetails.timestamp, -- Timestamp de creación original del job
    'attempts', jobDetails.attempts,
    'maxRetries', jobDetails.maxRetries,
    'status', jobDetails.status,
    'partitionKey', jobDetails.partitionKey,
    'scheduledAt', jobDetails.scheduledAt or ARGV[3] -- Guardar cuándo estaba programado si es diferido
)

-- Agregar el jobId a la ZSET de la cola de espera específica de la partición, usando el scoreForWaitingQueue.
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])

-- Registrar la partición en el SET global de particiones (si no existe, se añade).
redis.call('SADD', KEYS[3], ARGV[4])

-- Incrementar el contador global de trabajos en espera.
redis.call('HINCRBY', KEYS[4], 'waiting', 1)

return ARGV[1] -- Devolver el jobId como confirmación.