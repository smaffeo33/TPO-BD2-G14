const Poliza = require('../models/Poliza');
const Agente = require('../models/Agente');
const Cliente = require('../models/Cliente');
const Siniestro = require('../models/Siniestro'); // Restaurado
const { getNeo4jSession } = require('../config/db.neo4j');
const { redisClient } = require('../config/db.redis');
const { ensureCacheIsWarm, invalidateCacheWithLock } = require('./cacheSync');
const { Q7_CACHE_KEY, Q7_LOCK_KEY } = require('./queryService');

const Q5_HASH_KEY = 'counts:agente:polizas';
const Q5_LOCK_KEY = 'lock:cache:repopulating_q5';
// CORRECCIÓN 1: Forzar el ID a String para que coincida con loadData.js
const Q5_NEO4J_QUERY = `
    MATCH (a:Agente {activo: true})-[:GESTIONA]->(p:Poliza)
    RETURN toString(a.id_agente) AS id, count(p) AS total
`;

/**
 * Q15: Emisión de nuevas pólizas (validando cliente y agente)
 */
async function createPoliza(polizaData) {
    const session = getNeo4jSession();
    try {
        const numericClienteId = Number(polizaData.id_cliente);
        const numericAgenteId = Number(polizaData.id_agente);

        if (isNaN(numericClienteId) || isNaN(numericAgenteId)) {
            throw new Error('Formato de ID de Cliente o Agente inválido.');
        }

        const validationResult = await session.run(`
            MATCH (c:Cliente {id_cliente: $id_cliente, activo: true})
            MATCH (a:Agente {id_agente: $id_agente, activo: true})
            RETURN c, a
        `, {
            id_cliente: numericClienteId,
            id_agente: numericAgenteId
        });

        if (validationResult.records.length === 0) {
            throw new Error('Cliente or Agente not found or not active');
        }

        const agente = await Agente.findOne({ _id: numericAgenteId }).lean();
        if (!agente) {
            throw new Error('Agente not found in MongoDB');
        }

        const isAutoPolicy = polizaData.tipo.toLowerCase() === 'auto';
        let oldAutoPolizaNro = null;
        let oldAutoPolizaEstado = null;

        if (isAutoPolicy) {
            const cliente = await Cliente.findOne({ _id: numericClienteId }).lean();
            if (cliente && cliente.poliza_auto_vigente) {
                oldAutoPolizaNro = cliente.poliza_auto_vigente.nro_poliza;
                oldAutoPolizaEstado = 'Vencida';
            }
        }

        const polizaId = polizaData.nro_poliza ? String(polizaData.nro_poliza) : undefined;

        if (polizaId) {
            const polizaExistente = await Poliza.findOne({ _id: polizaId }).lean();
            if (polizaExistente) {
                throw new Error(`La póliza ${polizaId} ya existe`);
            }
        }

        const poliza = new Poliza({
            _id: polizaId,
            id_cliente: numericClienteId,
            tipo: polizaData.tipo,
            fecha_inicio: polizaData.fecha_inicio,
            fecha_fin: polizaData.fecha_fin,
            prima_mensual: polizaData.prima_mensual,
            cobertura_total: polizaData.cobertura_total,
            estado: polizaData.estado || 'Vigente',
            agente: {
                id_agente: agente._id,
                nombre: agente.nombre,
                apellido: agente.apellido,
                matricula: agente.matricula
            }
        });
        await poliza.save();

        if (isAutoPolicy) {
            await Cliente.findOneAndUpdate(
                { _id: numericClienteId },
                {
                    $set: {
                        poliza_auto_vigente: {
                            nro_poliza: poliza._id,
                            tipo: poliza.tipo,
                            fecha_inicio: poliza.fecha_inicio,
                            fecha_fin: poliza.fecha_fin,
                            cobertura_total: poliza.cobertura_total,
                            prima_mensual: poliza.prima_mensual
                        }
                    }
                }
            );
        }


        await session.run(`
            CREATE (p:Poliza {
                nro_poliza: $nro_poliza,
                estado: toLower($estado),
                tipo: $tipo,
                fecha_inicio: $fecha_inicio,
                fecha_fin: $fecha_fin,
                cobertura_total: $cobertura_total
            })
        `, {
            nro_poliza: poliza._id,
            estado: poliza.estado,
            tipo: poliza.tipo,
            fecha_inicio: poliza.fecha_inicio.toISOString().split('T')[0],
            fecha_fin: poliza.fecha_fin.toISOString().split('T')[0],
            cobertura_total: poliza.cobertura_total
        });


        await session.run(`
            MATCH (c:Cliente {id_cliente: $id_cliente})
            MATCH (p:Poliza {nro_poliza: $nro_poliza})
            CREATE (c)-[:TIENE_POLIZA]->(p)
        `, {
            id_cliente: numericClienteId,
            nro_poliza: poliza._id
        });

        await session.run(`
            MATCH (a:Agente {id_agente: $id_agente})
            MATCH (p:Poliza {nro_poliza: $nro_poliza})
            CREATE (a)-[:GESTIONA]->(p)
        `, {
            id_agente: numericAgenteId,
            nro_poliza: poliza._id
        });

        if (oldAutoPolizaNro) {
            await session.run(`
                MATCH (p:Poliza {nro_poliza: $nro_poliza})
                SET p.estado = toLower($nuevo_estado)
            `, {
                nro_poliza: oldAutoPolizaNro,
                nuevo_estado: oldAutoPolizaEstado
            });

            // Asumimos que Poliza usa nro_poliza como su _id en Mongo
            await Poliza.findOneAndUpdate(
                { _id: oldAutoPolizaNro }, // <-- Esto está bien si _id = nro_poliza
                { $set: { estado: oldAutoPolizaEstado } }
            );

            console.log(`Updated old Auto policy ${oldAutoPolizaNro} to estado: ${oldAutoPolizaEstado}`);
        }

        await invalidateCacheWithLock(Q7_CACHE_KEY, Q7_LOCK_KEY);

        try {
            const { wasWarm } = await ensureCacheIsWarm(Q5_HASH_KEY, Q5_LOCK_KEY, Q5_NEO4J_QUERY);

            if (wasWarm) {
                await redisClient.hIncrBy(Q5_HASH_KEY, polizaData.id_agente, 1);
                console.log(`Redis: Incremented poliza count for agente ${polizaData.id_agente}`);
            } else {
                console.log(`Redis: Cache was just populated, no increment needed for agente ${polizaData.id_agente}`);
            }

        } catch (redisError) {
            console.error('Redis error (non-fatal) during Q5 sync:', redisError.message);
        }

        return poliza;
    } catch (error) {
        throw new Error(`Error creating poliza: ${error.message}`);
    } finally {
        await session.close();
    }
}

/**
 * Get poliza by nro_poliza
 */
async function getPolizaByNro(nro_poliza) {
    const poliza = await Poliza.findOne({ _id: String(nro_poliza) }).lean();
    if (!poliza) {
        throw new Error('Poliza not found');
    }
    return poliza;
}

/**
 * Get all polizas
 */
async function getAllPolizas() {
    return Poliza.find().sort({fecha_inicio: -1}).lean();
}

/**
 * Get polizas by cliente
 */
async function getPolizasByCliente(id_cliente) {

    const numericId = Number(id_cliente);
    if (isNaN(numericId)) throw new Error('Invalid ID format');

    return Poliza.find({id_cliente: numericId}).sort({fecha_inicio: -1}).lean();
}

/**
 * Get pólizas activas por cliente
 */
async function getActivePolizasByCliente(id_cliente) {

    const numericId = Number(id_cliente);
    if (isNaN(numericId)) throw new Error('Invalid ID format');

    return Poliza.find({
        id_cliente: numericId,
        $or: [{ estado: 'Activa' }, { estado: 'Vigente' }, { estado: 'activa' }, { estado: 'vigente' }] // Ser más robusto con el estado
    }).sort({fecha_inicio: -1}).lean();
}

/**
 * Update poliza estado
 */
async function updatePolizaEstado(nro_poliza, nuevoEstado) {

    const session = getNeo4jSession();
    try {
        const poliza = await Poliza.findOneAndUpdate(
            { _id : nro_poliza },
            { $set: { estado: nuevoEstado } },
            { new: true }
        );

        if (!poliza) {
            throw new Error('Poliza not found');
        }

        await session.run(`
            MATCH (p:Poliza {nro_poliza: $nro_poliza}) 
            SET p.estado = toLower($estado)
        `, { nro_poliza, estado: nuevoEstado });

        await invalidateCacheWithLock(Q7_CACHE_KEY, Q7_LOCK_KEY);

        return poliza;
    } catch (error) {
        throw new Error(`Error updating poliza: ${error.message}`);
    } finally {
        await session.close();
    }
}

module.exports = {
    createPoliza,
    getPolizaByNro,
    getAllPolizas,
    getPolizasByCliente,
    updatePolizaEstado,
    getActivePolizasByCliente
};
