const { getNeo4jSession } = require('../config/db.neo4j');
const { redisClient } = require('../config/db.redis');
const Cliente = require('../models/Cliente');
const Siniestro = require('../models/Siniestro');
const Poliza = require('../models/Poliza');

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
 * Base: Redis (con fallback a Neo4j)
 */
async function getAgentesConCantidadPolizas() {
    // Try Redis first
    const counts = await redisClient.hGetAll('counts:agente:polizas');

    if (Object.keys(counts).length > 0) {
        // Cache hit
        return Object.entries(counts).map(([id_agente, count]) => ({
            id_agente,
            cantidad_polizas: parseInt(count, 10)
        }));
    }

    // Cache miss - calculate from Neo4j
    const session = getNeo4jSession();
    try {
        const result = await session.run(`
            MATCH (a:Agente {activo: true})-[:GESTIONA]->(p:Poliza)
            RETURN a.id_agente AS id_agente, a.nombre AS nombre, count(p) AS total
            ORDER BY total DESC, a.nombre
        `);

        const data = result.records.map(record => ({
            id_agente: record.get('id_agente'),
            nombre: record.get('nombre'),
            cantidad_polizas: record.get('total').toNumber()
        }));

        // Populate Redis for next time
        if (data.length > 0) {
            const multi = redisClient.multi();
            for (const item of data) {
                multi.hSet('counts:agente:polizas', item.id_agente, item.cantidad_polizas);
            }
            await multi.exec();
        }

        return data;
    } finally {
        await session.close();
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
 */
//TODO: ver si ponemos locks o lo hacemos de una en neo
async function getTop10ClientesPorCobertura() {
    // Try Redis first
    const cached = await redisClient.get('ranking:top10_clientes');

    if (cached) {
        return JSON.parse(cached);
    }

    // Cache miss - calculate from Neo4j
    const session = getNeo4jSession();
    try {
        const result = await session.run(`
            MATCH (c:Cliente)-[:TIENE_POLIZA]->(p:Poliza)
            WHERE p.estado = 'activa'
            RETURN c.nombre AS cliente_nombre, sum(p.cobertura_total) AS total_cobertura
            ORDER BY total_cobertura DESC
            LIMIT 10
        `);

        const data = result.records.map(record => ({
            cliente_nombre: record.get('cliente_nombre'),
            total_cobertura: record.get('total_cobertura')
        }));

        // Store in Redis
        await redisClient.set('ranking:top10_clientes', JSON.stringify(data));

        return data;
    } finally {
        await session.close();
    }
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

    return polizas.map(p => ({
        nro_poliza: p.nro_poliza,
        tipo: p.tipo,
        fecha_inicio: p.fecha_inicio,
        fecha_fin: p.fecha_fin,
        agente: `${p.agente.nombre} ${p.agente.apellido}`,
        prima_mensual: p.prima_mensual
    }));
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
 * Base: Redis (con fallback a Neo4j)
 */
async function getAgentesConCantidadSiniestros() {
    // Try Redis first
    const counts = await redisClient.hGetAll('counts:agente:siniestros');

    if (Object.keys(counts).length > 0) {
        return Object.entries(counts).map(([id_agente, count]) => ({
            id_agente,
            cantidad_siniestros: parseInt(count, 10)
        }));
    }

    // Cache miss - calculate from Neo4j
    const session = getNeo4jSession();
    try {
        const result = await session.run(`
            MATCH (a:Agente)-[:GESTIONA]->(p:Poliza)-[:CUBRE_SINIESTRO]->(s:Siniestro)
            RETURN a.id_agente AS id_agente, a.nombre AS nombre, count(s) AS total
            ORDER BY total DESC, a.nombre
        `);

        const data = result.records.map(record => ({
            id_agente: record.get('id_agente'),
            nombre: record.get('nombre'),
            cantidad_siniestros: record.get('total').toNumber()
        }));

        // Populate Redis
        if (data.length > 0) {
            const multi = redisClient.multi();
            for (const item of data) {
                multi.hSet('counts:agente:siniestros', item.id_agente, item.cantidad_siniestros);
            }
            await multi.exec();
        }

        return data;
    } finally {
        await session.close();
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
    getAgentesConCantidadSiniestros
};
