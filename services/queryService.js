const { getNeo4jSession } = require('../config/db.neo4j');
const { redisClient } = require('../config/db.redis');
const Cliente = require('../models/Cliente');
const Siniestro = require('../models/Siniestro');
const Poliza = require('../models/Poliza');
const Agente = require('../models/Agente');
const { ensureCacheIsWarm, computeAndCacheWithLock } = require('./cacheSync');

const Q5_HASH_KEY = 'counts:agente:polizas';
const Q5_LOCK_KEY = 'lock:cache:repopulating_q5';
const Q5_NEO4J_QUERY = `
    MATCH (a:Agente {activo: true})-[:GESTIONA]->(p:Poliza)
    RETURN a.id_agente AS id, count(p) AS total
`;

const Q12_HASH_KEY = 'counts:agente:siniestros';
const Q12_LOCK_KEY = 'lock:cache:repopulating_q12';
const Q12_NEO4J_QUERY = `
    MATCH (a:Agente)-[:GESTIONA]->(p:Poliza)-[:CUBRE_SINIESTRO]->(s:Siniestro)
    RETURN a.id_agente AS id, count(s) AS total
`;

const Q7_CACHE_KEY = 'ranking:top10_clientes';
const Q7_LOCK_KEY = 'lock:cache:top10_clientes';

/**
 * Q1: Clientes activos con sus pólizas vigentes (array embebido)
 * Base: Neo4j
 * Cada cliente activo aparece una vez con su array de pólizas vigentes (vacío si no tiene)
 */
async function getClientesActivosConPolizasVigentes() {
    const session = getNeo4jSession();
    try {
        const result = await session.run(`
            MATCH (c:Cliente {activo: true})
            OPTIONAL MATCH (c)-[:TIENE_POLIZA]->(p:Poliza)
            WHERE p.estado = 'vigente' OR p.estado = 'activa'
            WITH c, collect({
                nro_poliza: p.nro_poliza,
                tipo: p.tipo,
                fecha_inicio: p.fecha_inicio,
                fecha_fin: p.fecha_fin,
                cobertura_total: p.cobertura_total
            }) AS polizas_vigentes
            RETURN c.id_cliente AS id_cliente,
                   c.nombre AS cliente_nombre,
                   polizas_vigentes
            ORDER BY c.nombre
        `);
        return result.records.map(record => {
            const polizas = record.get('polizas_vigentes');
            // Filtrar nulls (cuando no hay pólizas, collect devuelve [{nro_poliza: null, ...}])
            const polizasLimpias = polizas.filter(p => p.nro_poliza !== null);

            return {
                id_cliente: record.get('id_cliente'),
                cliente_nombre: record.get('cliente_nombre'),
                polizas_vigentes: polizasLimpias
            };
        });
    } finally {
        await session.close();
    }
}

/**
 * Q2: Siniestros abiertos con tipo, monto y cliente afectado
 * Base: Neo4j
 */
async function getSiniestrosAbiertos() {
    const session = getNeo4jSession();
    try {
        const result = await session.run(`
            MATCH (c:Cliente)-[:TIENE_POLIZA]->(p:Poliza)-[:CUBRE_SINIESTRO]->(s:Siniestro)
            WHERE toLower(s.estado) = 'abierto'
            RETURN s.id_siniestro AS id_siniestro, s.tipo AS tipo,
                   s.monto_estimado AS monto_estimado, c.nombre AS cliente_nombre
            ORDER BY s.fecha DESC
        `);
        return result.records.map(record => ({
            id_siniestro: record.get('id_siniestro'),
            tipo: record.get('tipo'),
            monto_estimado: record.get('monto_estimado'),
            cliente_nombre: record.get('cliente_nombre')
        }));
    } finally {
        await session.close();
    }
}

/**
 * Q3: Vehículos asegurados con su cliente y póliza
 * Base: MongoDB (simple query with embedded data)
 */
async function getVehiculosAsegurados() {
    // Obtener la fecha y hora actual para la comparación
    const today = new Date();

    const result = await Cliente.aggregate([
        {
            // 1. Filtrar clientes que:
            $match: {
                'vehiculos.0': { $exists: true }, // Tengan al menos un vehículo
                'poliza_auto_vigente': { $exists: true }, // Tengan una póliza de auto embebida
                'poliza_auto_vigente.fecha_fin': { $gte: today } // Esa póliza AÚN esté vigente (fecha_fin >= hoy)
            }
        },
        {
            // 2. queremos cada vehículo como un documento separado
            $unwind: '$vehiculos'
        },
        {
            // 3. Filtrar solo los vehículos que están asegurados
            //   TODO: yo eliminaria este booleano, no tiene sentido, ya nos fijamos en cliente si hay póliza. Si hay claramente, está asegurado --> fijarnos en los datasets si hay alguna inconsistencia
            $match: {
                'vehiculos.asegurado': true
            }
        },
        {
            $project: {
                _id: 0,
                vehiculo: {
                    $concat: [
                        '$vehiculos.marca', ' ', '$vehiculos.modelo',
                        ' (', '$vehiculos.patente', ')'
                    ]
                },
                cliente: { $concat: ['$nombre', ' ', '$apellido'] },
                poliza: '$poliza_auto_vigente.nro_poliza'
            }
        }
    ]);

    return result;
}


/**
 * Q4: Clientes sin pólizas activas
 * Base: Neo4j
 */
async function getClientesSinPolizasActivas() {
    const session = getNeo4jSession();
    try {
        const result = await session.run(`
            MATCH (c:Cliente)
            WHERE NOT EXISTS {
                MATCH (c)-[:TIENE_POLIZA]->(p:Poliza)
                WHERE p.estado = 'vigente' OR p.estado = 'activa'
            }
            RETURN c.id_cliente AS id_cliente, c.nombre AS nombre, c.activo AS activo
            ORDER BY c.nombre
        `);
        return result.records.map(record => ({
            id_cliente: record.get('id_cliente'),
            nombre: record.get('nombre'),
            activo: record.get('activo')
        }));
    } finally {
        await session.close();
    }
}

/**
 * Q5: Agentes activos con cantidad de pólizas asignadas
 * Base: Redis (con fallback a Neo4j y sincronización)
 */
async function getAgentesConCantidadPolizas() {
    try {
        // Asegurarnos que el caché esté "caliente".
        // Esta función (definida arriba) chequeará si Q5_HASH_KEY existe.
        // Si no existe, obtendrá un lock y lo poblará desde Neo4j.
        // Si el lock está ocupado, esperará.
        await ensureCacheIsWarm(Q5_HASH_KEY, Q5_LOCK_KEY, Q5_NEO4J_QUERY);

        // Ahora que el caché está caliente, simplemente lo leemos.
        // Toda la lógica de "Cache miss" y repoblación ya no está aquí,
        // está centralizada en 'ensureCacheIsWarm'.
        const counts = await redisClient.hGetAll(Q5_HASH_KEY);

        // Reforzamos el filtro de agentes activos usando Mongo por si alguien
        // desactiva un agente después de poblar el cache.
        const agentesActivos = await Agente.find({ activo: true })
            .select('_id')
            .lean();
        const agentesActivosSet = new Set(agentesActivos.map(a => String(a._id)));

        // Convertir la respuesta de Redis en el formato de array esperado
        return Object.entries(counts)
            .filter(([key, _]) => key !== '_placeholder') // Filtrar el placeholder si existe
            .filter(([id_agente]) => agentesActivosSet.has(id_agente))
            .map(([id_agente, count]) => ({
                id_agente,
                cantidad_polizas: parseInt(count, 10)
            }));

    } catch (error) {
        // Si ensureCacheIsWarm falla (ej. timeout del lock), lanzamos un error.
        console.error('Error en getAgentesConCantidadPolizas:', error.message);
        throw new Error('Could not retrieve agent counts');
    }
}

/**
 * Q6: Pólizas vencidas con el nombre del cliente
 * Base: Neo4j
 */
async function getPolizasVencidas() {
    const session = getNeo4jSession();
    try {
        const result = await session.run(`
            MATCH (c:Cliente)-[:TIENE_POLIZA]->(p:Poliza)
            WHERE p.estado = 'vencida'
            RETURN p.nro_poliza AS nro_poliza, p.tipo AS tipo,
                   c.nombre AS cliente_nombre, p.fecha_fin AS fecha_fin
            ORDER BY p.fecha_fin DESC
        `);
        return result.records.map(record => ({
            nro_poliza: record.get('nro_poliza'),
            tipo: record.get('tipo'),
            cliente_nombre: record.get('cliente_nombre'),
            fecha_fin: record.get('fecha_fin')
        }));
    } finally {
        await session.close();
    }
}

/**
 * Q7: Top 10 clientes por cobertura total
 * Base: Redis (con fallback a Neo4j)
 * Usa lock para evitar race condition de stale write durante invalidación
 */
async function getTop10ClientesPorCobertura() {
    return computeAndCacheWithLock(Q7_CACHE_KEY, Q7_LOCK_KEY, async () => {
        const session = getNeo4jSession();
        try {
            const result = await session.run(`
                MATCH (c:Cliente)-[:TIENE_POLIZA]->(p:Poliza)
                WHERE toLower(p.estado) = 'activa'
                RETURN c.nombre AS cliente_nombre,
                       c.apellido AS cliente_apellido,
                       sum(p.cobertura_total) AS total_cobertura,
                       c.id_cliente AS id_cliente
                ORDER BY total_cobertura DESC, id_cliente ASC
                LIMIT 10
            `);

            return result.records.map(record => {
                const nombre = record.get('cliente_nombre') || '';
                const apellido = record.get('cliente_apellido') || '';
                return {
                    cliente_nombre: `${nombre} ${apellido}`.trim(),
                    total_cobertura: record.get('total_cobertura')
                };
            });
        } finally {
            await session.close();
        }
    });
}

/**
 * Q8: Siniestros tipo "Accidente" del último año
 * Base: MongoDB
 */
async function getSiniestrosAccidenteUltimoAnio() {
    const oneYearAgo = new Date();
    oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);

    const siniestros = await Siniestro.find({
        tipo: 'Accidente',
        fecha: { $gte: oneYearAgo }
    }).sort({ fecha: -1 }).lean();

    return siniestros.map(s => ({
        id_siniestro: s.id_siniestro,
        fecha: s.fecha,
        monto_estimado: s.monto_estimado,
        descripcion: s.descripcion,
        cliente: s.poliza_snapshot.cliente.nombre
    }));
}

/**
 * Q9: Vista de pólizas activas ordenadas por fecha de inicio
 * Base: MongoDB
 */
async function getPolizasActivasOrdenadas() {
    const polizas = await Poliza.find({
        estado: { $in: ['Activa', 'activa'] }
    }).sort({ fecha_inicio: 1 }).lean();

    return polizas.map(p => {
        const agenteNombre = [p.agente?.nombre, p.agente?.apellido]
            .filter(Boolean)
            .join(' ')
            .trim();

        return {
            poliza_id: p._id ? String(p._id) : (p.nro_poliza || null),
            tipo: p.tipo,
            fecha_inicio: p.fecha_inicio,
            fecha_fin: p.fecha_fin,
            agente: agenteNombre || 'Sin asignar',
            prima_mensual: p.prima_mensual
        };
    });
}

/**
 * Q10: Pólizas suspendidas con estado del cliente
 * Base: Neo4j
 */
async function getPolizasSuspendidasConCliente() {
    const session = getNeo4jSession();
    try {
        const result = await session.run(`
            MATCH (c:Cliente)-[:TIENE_POLIZA]->(p:Poliza)
            WHERE p.estado = 'suspendida'
            RETURN p.nro_poliza AS nro_poliza, p.tipo AS tipo,
                   c.nombre AS cliente_nombre, c.activo AS cliente_activo
            ORDER BY p.nro_poliza
        `);
        return result.records.map(record => ({
            nro_poliza: record.get('nro_poliza'),
            tipo: record.get('tipo'),
            cliente_nombre: record.get('cliente_nombre'),
            cliente_activo: record.get('cliente_activo')
        }));
    } finally {
        await session.close();
    }
}

/**
 * Q11: Clientes con más de un vehículo asegurado
 * Base: MongoDB
 */
async function getClientesConVariosVehiculos() {
    const clientes = await Cliente.find({
        'vehiculos.1': { $exists: true } // Has at least 2 vehicles
    }).select('id_cliente nombre apellido vehiculos').lean();

    return clientes.map(c => ({
        id_cliente: c.id_cliente,
        nombre: `${c.nombre} ${c.apellido}`,
        cantidad_vehiculos: c.vehiculos.length,
        vehiculos: c.vehiculos.map(v => `${v.marca} ${v.modelo}`)
    }));
}

/**
 * Q12: Agentes y cantidad de siniestros asociados
 * Base: Redis (con fallback a Neo4j y sincronización)
 */
async function getAgentesConCantidadSiniestros() {
    try {
        // Asegurarnos que el caché esté "caliente"
        await ensureCacheIsWarm(Q12_HASH_KEY, Q12_LOCK_KEY, Q12_NEO4J_QUERY);

        // Leer del caché caliente
        const counts = await redisClient.hGetAll(Q12_HASH_KEY);

        // Convertir la respuesta de Redis en el formato esperado
        return Object.entries(counts)
            .filter(([key, _]) => key !== '_placeholder')
            .map(([id_agente, count]) => ({
                id_agente,
                cantidad_siniestros: parseInt(count, 10)
            }));

    } catch (error) {
        console.error('Error en getAgentesConCantidadSiniestros:', error.message);
        throw new Error('Could not retrieve agent siniestro counts');
    }
}

module.exports = {
    getClientesActivosConPolizasVigentes,
    getSiniestrosAbiertos,
    getVehiculosAsegurados,
    getClientesSinPolizasActivas,
    getAgentesConCantidadPolizas,
    getPolizasVencidas,
    getTop10ClientesPorCobertura,
    getSiniestrosAccidenteUltimoAnio,
    getPolizasActivasOrdenadas,
    getPolizasSuspendidasConCliente,
    getClientesConVariosVehiculos,
    getAgentesConCantidadSiniestros,
    Q7_CACHE_KEY,
    Q7_LOCK_KEY
};
