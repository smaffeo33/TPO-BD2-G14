const crypto = require('crypto');
const { redisClient } = require('../../config/db.redis');

const AGENTE_SINIESTRO_HASH_KEY = 'counts:agente:siniestros';
const AGENTE_SINIESTRO_LOCK_KEY = 'cache:agente_siniestros:lock';
const AGENTE_SINIESTRO_DIRTY_KEY = 'cache:agente_siniestros:dirty';
const AGENTE_SINIESTRO_LOCK_TTL_MS = 10000;

const incrementScript = `
local lockKey = KEYS[1]
local hashKey = KEYS[2]
local dirtyKey = KEYS[3]
local agenteId = ARGV[1]

if redis.call("exists", lockKey) == 1 then
    redis.call("set", dirtyKey, "1")
    return -1 -- lock active, skip increment
end

local current = redis.call("hget", hashKey, agenteId)
if not current then
    redis.call("set", dirtyKey, "1")
    return 0 -- field missing, mark cache dirty
end

return redis.call("hincrby", hashKey, agenteId, 1)
`;

const releaseLockScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
end
return 0
`;

async function incrementAgentSiniestroCount(agenteId) {
    return redisClient.eval(incrementScript, {
        keys: [AGENTE_SINIESTRO_LOCK_KEY, AGENTE_SINIESTRO_HASH_KEY, AGENTE_SINIESTRO_DIRTY_KEY],
        arguments: [agenteId]
    });
}

async function markAgentSiniestroCacheDirty() {
    await redisClient.set(AGENTE_SINIESTRO_DIRTY_KEY, '1');
}

async function clearAgentSiniestroCacheDirtyFlag() {
    await redisClient.del(AGENTE_SINIESTRO_DIRTY_KEY);
}

async function isAgentSiniestroCacheDirty() {
    return (await redisClient.exists(AGENTE_SINIESTRO_DIRTY_KEY)) === 1;
}

async function acquireAgentSiniestroCacheLock(ttlMs = AGENTE_SINIESTRO_LOCK_TTL_MS) {
    const token = crypto.randomUUID();
    const acquired = await redisClient.set(AGENTE_SINIESTRO_LOCK_KEY, token, { NX: true, PX: ttlMs });
    return acquired ? token : null;
}

async function releaseAgentSiniestroCacheLock(token) {
    if (!token) return;
    await redisClient.eval(releaseLockScript, {
        keys: [AGENTE_SINIESTRO_LOCK_KEY],
        arguments: [token]
    });
}

module.exports = {
    AGENTE_SINIESTRO_HASH_KEY,
    incrementAgentSiniestroCount,
    markAgentSiniestroCacheDirty,
    clearAgentSiniestroCacheDirtyFlag,
    isAgentSiniestroCacheDirty,
    acquireAgentSiniestroCacheLock,
    releaseAgentSiniestroCacheLock,
    AGENTE_SINIESTRO_LOCK_TTL_MS,
};
