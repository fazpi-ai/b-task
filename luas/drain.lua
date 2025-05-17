-- Script para drenar la cola (eliminar todos los trabajos)
local queueKey = KEYS[1]
local jobsKey = KEYS[2]
local waitKey = KEYS[3]
local activeKey = KEYS[4]
local completedKey = KEYS[5]

-- Obtener todos los IDs de trabajos en cada estado
local waitingJobs = redis.call('ZRANGE', waitKey, 0, -1)
local activeJobs = redis.call('ZRANGE', activeKey, 0, -1)
local completedJobs = redis.call('ZRANGE', completedKey, 0, -1)

-- Función para eliminar trabajos
local function removeJobs(jobIds)
    for _, jobId in ipairs(jobIds) do
        local jobKey = jobsKey .. ':' .. jobId
        redis.call('DEL', jobKey)
    end
end

-- Eliminar todos los trabajos
removeJobs(waitingJobs)
removeJobs(activeJobs)
removeJobs(completedJobs)

-- Limpiar todas las listas
redis.call('DEL', waitKey)
redis.call('DEL', activeKey)
redis.call('DEL', completedKey)

-- Reiniciar contadores
redis.call('HSET', queueKey, 
    'waiting', 0,
    'active', 0,
    'completed', 0
)

-- Devolver conteo de trabajos eliminados
return {
    waiting = #waitingJobs,
    active = #activeJobs,
    completed = #completedJobs,
    total = #waitingJobs + #activeJobs + #completedJobs
} 