const { getNeo4jSession } = require('../config/db.neo4j');
const { redisClient } = require('../config/db.redis');

const LOCK_TTL = 40;

/**
 * Función helper interna para poblar el caché desde Neo4j.
 * ESTA FUNCIÓN ASUME QUE QUIEN LA LLAMA YA TIENE EL LOCK.
 */
async function _populateCacheFromNeo4j(hashKey, neo4jQuery) {
    console.log(`(Helper) Poblando caché para ${hashKey} desde Neo4j...`);
    const session = getNeo4jSession();
    try {
        const result = await session.run(neo4jQuery);
        const data = result.records.map(record => ({
            id: record.get('id'),
            total: record.get('total').toNumber()
        }));

        const multi = redisClient.multi();
        multi.del(hashKey);
        if (data.length > 0) {
            for (const item of data) {
                multi.hSet(hashKey, item.id, item.total);
            }
        } else {

            multi.hSet(hashKey, '_placeholder', 'true');
            multi.expire(hashKey, 300);
        }
        await multi.exec();
        console.log(`(Helper) Caché ${hashKey} poblado.`);
    } catch (e) {
        console.error(`(Helper) Error poblando caché ${hashKey}:`, e);
    } finally {
        await session.close();
    }
}

/**
 * Función helper de sincronización.
 * Asegura que el caché HASH exista antes de continuar.
 * Es llamada por Q5 (lectura) y Q15 (escritura).
 * Retorna { wasWarm: true/false } indicando si el caché ya existía.
 */
async function ensureCacheIsWarm(hashKey, lockKey, neo4jQuery) {

    const cacheExists = await redisClient.exists(hashKey);
    if (cacheExists) {
        return { wasWarm: true };
    }


    console.warn(`(ensureCacheIsWarm) Cache MISS en ${hashKey}. Esperando para adquirir lock...`);
    let lockAcquired = false;
    while (!lockAcquired) {

        lockAcquired = await redisClient.set(lockKey, 'true', {
            NX: true,
            EX: LOCK_TTL
        });

        if (!lockAcquired) {
            await new Promise(resolve => setTimeout(resolve, 200));
            const cacheNowExists = await redisClient.exists(hashKey);
            if (cacheNowExists) {
                console.log(`(ensureCacheIsWarm) El caché fue poblado por otro hilo mientras esperábamos. Continuando.`);
                return { wasWarm: true };
            }
        }
    }

    console.log(`(ensureCacheIsWarm) Lock adquirido. Verificando caché y repoblando si es necesario...`);
    try {
        const cacheNowExists = await redisClient.exists(hashKey);
        if (!cacheNowExists) {
            await _populateCacheFromNeo4j(hashKey, neo4jQuery);
        }
    } catch (e) {
        console.error(`(ensureCacheIsWarm) Error durante repoblación:`, e);
    } finally {
        await redisClient.del(lockKey);
        console.log(`(ensureCacheIsWarm) Lock liberado.`);
    }
    return { wasWarm: false };
}

/**
 * Invalidar una cache key con lock para prevenir race conditions.
 * Útil cuando se necesita garantizar que una invalidación no sea sobrescrita
 * por un thread que está calculando datos viejos.
 *
 * @param {string} cacheKey - La key del cache a invalidar
 * @param {string} lockKey - La key del lock a usar
 * @returns {Promise<void>}
 */
async function invalidateCacheWithLock(cacheKey, lockKey) {
    console.log(`(invalidateCacheWithLock) Adquiriendo lock para invalidar ${cacheKey}...`);

    let lockAcquired = false;
    while (!lockAcquired) {
        lockAcquired = await redisClient.set(lockKey, 'true', {
            NX: true,
            EX: LOCK_TTL
        });

        if (!lockAcquired) {
            await new Promise(resolve => setTimeout(resolve, 200));
        }
    }

    try {
        await redisClient.del(cacheKey);
        console.log(`(invalidateCacheWithLock) Cache ${cacheKey} invalidado.`);
    } finally {
        await redisClient.del(lockKey);
        console.log(`(invalidateCacheWithLock) Lock liberado.`);
    }
}

/**
 * Patrón genérico de compute-and-cache con lock.
 * Previene race conditions entre cálculo y escritura del cache.
 *
 * @param {string} cacheKey - La key del cache a leer/escribir
 * @param {string} lockKey - La key del lock a usar
 * @param {Function} computeFn - Función async que calcula los datos (retorna el objeto a cachear)
 * @returns {Promise<any>} Los datos calculados o cacheados
 */
async function computeAndCacheWithLock(cacheKey, lockKey, computeFn) {
    const cached = await redisClient.get(cacheKey);
    if (cached) {
        return JSON.parse(cached);
    }

    console.log(`(computeAndCacheWithLock) Cache MISS en ${cacheKey}. Adquiriendo lock...`);
    let lockAcquired = false;
    while (!lockAcquired) {
        lockAcquired = await redisClient.set(lockKey, 'true', {
            NX: true,
            EX: LOCK_TTL
        });

        if (!lockAcquired) {
            await new Promise(resolve => setTimeout(resolve, 200));
            const nowCached = await redisClient.get(cacheKey);
            if (nowCached) {
                console.log(`(computeAndCacheWithLock) Otro thread pobló el cache.`);
                return JSON.parse(nowCached);
            }
        }
    }

    console.log(`(computeAndCacheWithLock) Lock adquirido. Calculando...`);
    try {
        const nowCached = await redisClient.get(cacheKey);
        if (nowCached) {
            console.log(`(computeAndCacheWithLock) Cache encontrado después de lock.`);
            return JSON.parse(nowCached);
        }

        const data = await computeFn();

        await redisClient.set(cacheKey, JSON.stringify(data));
        console.log(`(computeAndCacheWithLock) Cache ${cacheKey} poblado.`);

        return data;
    } finally {
        await redisClient.del(lockKey);
        console.log(`(computeAndCacheWithLock) Lock liberado.`);
    }
}

module.exports = {
    ensureCacheIsWarm,
    invalidateCacheWithLock,
    computeAndCacheWithLock,
    LOCK_TTL
};
