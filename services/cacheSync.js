const { getNeo4jSession } = require('../config/db.neo4j');
const { redisClient } = require('../config/db.redis');

const LOCK_TTL = 40; // Lock expira en 40 segundos

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

        // Limpiamos la clave (por si acaso) y la poblamos de cero
        const multi = redisClient.multi();
        multi.del(hashKey); // Empezar de limpio
        if (data.length > 0) {
            for (const item of data) {
                multi.hSet(hashKey, item.id, item.total);
            }
        } else {
            // Creamos un hash vacío con expiración corta
            // para evitar que se calcule esto todo el tiempo si no hay datos.
            multi.hSet(hashKey, '_placeholder', 'true');
            multi.expire(hashKey, 300); // Expira en 5 minutos
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
    // 1. Ver si el caché existe
    const cacheExists = await redisClient.exists(hashKey);
    if (cacheExists) {
        return { wasWarm: true }; // El caché ya existía
    }

    // 2. El caché no existe.
    console.warn(`(ensureCacheIsWarm) Cache MISS en ${hashKey}. Esperando para adquirir lock...`);
    let lockAcquired = false;
    while (!lockAcquired) {
        // Intentamos obtener el lock ATÓMICAMENTE con el TTL
        lockAcquired = await redisClient.set(lockKey, 'true', {
            NX: true,
            EX: LOCK_TTL
        });

        if (!lockAcquired) {
            // No lo obtuvimos, esperamos y reintentamos
            await new Promise(resolve => setTimeout(resolve, 200));

            // Si mientras esperábamos, otro hilo ya pobló el caché, salimos y evitamos el trabajo.
            const cacheNowExists = await redisClient.exists(hashKey);
            if (cacheNowExists) {
                console.log(`(ensureCacheIsWarm) El caché fue poblado por otro hilo mientras esperábamos. Continuando.`);
                return { wasWarm: true }; // Otro thread lo pobló
            }
        }
    }

    console.log(`(ensureCacheIsWarm) Lock adquirido. Verificando caché y repoblando si es necesario...`);
    try {
        // 4. Doble Verificación
        const cacheNowExists = await redisClient.exists(hashKey);
        if (!cacheNowExists) {
            await _populateCacheFromNeo4j(hashKey, neo4jQuery);
        }
    } catch (e) {
        console.error(`(ensureCacheIsWarm) Error durante repoblación:`, e);
    } finally {
        // 5. Liberamos el lock
        await redisClient.del(lockKey);
        console.log(`(ensureCacheIsWarm) Lock liberado.`);
    }
    return { wasWarm: false }; // YO lo repoblé
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
    // Fast path: leer cache sin lock
    const cached = await redisClient.get(cacheKey);
    if (cached) {
        return JSON.parse(cached);
    }

    // Cache miss - adquirir lock
    console.log(`(computeAndCacheWithLock) Cache MISS en ${cacheKey}. Adquiriendo lock...`);
    let lockAcquired = false;
    while (!lockAcquired) {
        lockAcquired = await redisClient.set(lockKey, 'true', {
            NX: true,
            EX: LOCK_TTL
        });

        if (!lockAcquired) {
            // Esperar y verificar si otro thread pobló el cache
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
        // Double-check después del lock
        const nowCached = await redisClient.get(cacheKey);
        if (nowCached) {
            console.log(`(computeAndCacheWithLock) Cache encontrado después de lock.`);
            return JSON.parse(nowCached);
        }

        // Calcular datos usando la función provista
        const data = await computeFn();

        // Escribir en Redis (aún tenemos el lock)
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
