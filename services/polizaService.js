const Poliza = require('../models/Poliza');
const Agente = require('../models/Agente');
const Cliente = require('../models/Cliente');
const Siniestro = require('../models/Siniestro');
const { getNeo4jSession } = require('../config/db.neo4j');
const { redisClient } = require('../config/db.redis');
const { ensureCacheIsWarm } = require('./cacheSync');

const Q5_HASH_KEY = 'counts:agente:polizas';
const Q5_LOCK_KEY = 'lock:cache:repopulating_q5';
const Q5_NEO4J_QUERY = `
    MATCH (a:Agente {activo: true})-[:GESTIONA]->(p:Poliza)
    RETURN a.id_agente AS id, count(p) AS total
`;

/**
 * Q15: Emisión de nuevas pólizas (validando cliente y agente)
 *
 * IMPORTANT LOGIC FOR AUTO POLICIES:
 * - If the policy type is 'Auto' and the client already has a poliza_auto_vigente:
 *   1. Overwrite the poliza_auto_vigente in MongoDB's cliente document
 *   2. Change the estado of the old policy in Neo4j to 'Vencida' or 'Suspendida'
 *
 * Redis INCR: Only if key exists
 * Always write to Neo4j regardless of Redis status
 *
 * Affects: MongoDB (read + write), Neo4j (validation + write), Redis (invalidation + increment)
 */
async function createPoliza(polizaData) {
    const session = getNeo4jSession();
    try {
        // 1. Neo4j (Validación): Validate that cliente and agente exist and are active
        const validationResult = await session.run(`
            MATCH (c:Cliente {id_cliente: $id_cliente, activo: true})
            MATCH (a:Agente {id_agente: $id_agente, activo: true})
            RETURN c, a
        `, {
            id_cliente: polizaData.id_cliente,
            id_agente: polizaData.id_agente
        });

        if (validationResult.records.length === 0) {
            throw new Error('Cliente or Agente not found or not active');
        }

        // 2. MongoDB (Lectura): Get Agente data to embed
        const agente = await Agente.findOne({ id_agente: polizaData.id_agente }).lean();
        if (!agente) {
            throw new Error('Agente not found in MongoDB');
        }

        // 3. Check if this is an Auto policy and if client has existing poliza_auto_vigente
        const isAutoPolicy = polizaData.tipo.toLowerCase() === 'auto';
        let oldAutoPolizaNro = null;
        let oldAutoPolizaEstado = null;

        if (isAutoPolicy) {
            const cliente = await Cliente.findOne({ id_cliente: polizaData.id_cliente }).lean();
            if (cliente && cliente.poliza_auto_vigente) {
                oldAutoPolizaNro = cliente.poliza_auto_vigente.nro_poliza;
                // Determine new estado based on current policy status
                oldAutoPolizaEstado = 'Vencida'; // Default to Vencida
                // Could be 'Suspendida' if there's specific business logic
            }
        }

        // 4. MongoDB (Escritura): Create the new Poliza
        const poliza = new Poliza({
            nro_poliza: polizaData.nro_poliza,
            cliente_id: polizaData.id_cliente,
            tipo: polizaData.tipo,
            fecha_inicio: polizaData.fecha_inicio,
            fecha_fin: polizaData.fecha_fin,
            prima_mensual: polizaData.prima_mensual,
            cobertura_total: polizaData.cobertura_total,
            estado: polizaData.estado || 'Vigente',
            agente: {
                id_agente: agente.id_agente,
                nombre: agente.nombre,
                apellido: agente.apellido,
                matricula: agente.matricula
            }
        });
        await poliza.save();

        // 5. If Auto policy, update Cliente's poliza_auto_vigente (OVERWRITE)
        if (isAutoPolicy) {
            await Cliente.findOneAndUpdate(
                { id_cliente: polizaData.id_cliente },
                {
                    $set: {
                        poliza_auto_vigente: {
                            nro_poliza: poliza.nro_poliza,
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

        // 6. Neo4j (Escritura): Create Poliza node and relationships
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
            nro_poliza: poliza.nro_poliza,
            estado: poliza.estado,
            tipo: poliza.tipo,
            fecha_inicio: poliza.fecha_inicio.toISOString().split('T')[0],
            fecha_fin: poliza.fecha_fin.toISOString().split('T')[0],
            cobertura_total: poliza.cobertura_total
        });

        // Create relationships
        await session.run(`
            MATCH (c:Cliente {id_cliente: $id_cliente})
            MATCH (p:Poliza {nro_poliza: $nro_poliza})
            CREATE (c)-[:TIENE_POLIZA]->(p)
        `, {
            id_cliente: polizaData.id_cliente,
            nro_poliza: poliza.nro_poliza
        });

        await session.run(`
            MATCH (a:Agente {id_agente: $id_agente})
            MATCH (p:Poliza {nro_poliza: $nro_poliza})
            CREATE (a)-[:GESTIONA]->(p)
        `, {
            id_agente: polizaData.id_agente,
            nro_poliza: poliza.nro_poliza
        });

        // 7. If there was an old Auto policy, update its estado in Neo4j
        if (oldAutoPolizaNro) {
            await session.run(`
                MATCH (p:Poliza {nro_poliza: $nro_poliza})
                SET p.estado = toLower($nuevo_estado)
            `, {
                nro_poliza: oldAutoPolizaNro,
                nuevo_estado: oldAutoPolizaEstado
            });

            // Also update in MongoDB
            await Poliza.findOneAndUpdate(
                { nro_poliza: oldAutoPolizaNro },
                { $set: { estado: oldAutoPolizaEstado } }
            );

            console.log(`Updated old Auto policy ${oldAutoPolizaNro} to estado: ${oldAutoPolizaEstado}`);
        }

        // 8. Redis (Invalidación Q7): Invalidate top10 ranking
        await redisClient.del('ranking:top10_clientes');


        // 9. Redis (Incremento Q5):
        try {
            // PASO 9.A: Asegurarnos que el caché esté "caliente".
            // Esta función (definida arriba) chequeará si Q5_HASH_KEY existe.
            // Si no existe, obtendrá un lock y lo poblará desde Neo4j.
            // Retorna { wasWarm: true/false } para saber si debemos incrementar.
            const { wasWarm } = await ensureCacheIsWarm(Q5_HASH_KEY, Q5_LOCK_KEY, Q5_NEO4J_QUERY);

            // PASO 9.B: Solo incrementamos si el caché YA existía.
            // Si wasWarm === false, significa que YO lo repoblé,
            // y Neo4j ya incluyó esta póliza en el conteo → no incrementar.
            if (wasWarm) {
                await redisClient.hIncrBy(Q5_HASH_KEY, polizaData.id_agente, 1);
                console.log(`Redis: Incremented poliza count for agente ${polizaData.id_agente}`);
            } else {
                console.log(`Redis: Cache was just populated, no increment needed for agente ${polizaData.id_agente}`);
            }

        } catch (redisError) {
            // Si ensureCacheIsWarm falla (ej. por timeout del lock),
            // o HINCRBY falla, lo capturamos pero no detenemos la operación.
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
    const poliza = await Poliza.findOne({ nro_poliza }).lean();
    if (!poliza) {
        throw new Error('Poliza not found');
    }
    return poliza;
}

/**
 * Get all polizas
 */
async function getAllPolizas() {
    return await Poliza.find().sort({ fecha_inicio: -1 }).lean();
}

/**
 * Get polizas by cliente
 */
async function getPolizasByCliente(id_cliente) {
    return await Poliza.find({ cliente_id: id_cliente }).sort({ fecha_inicio: -1 }).lean();
}

/**
 * Update poliza estado
 */
async function updatePolizaEstado(nro_poliza, nuevoEstado) {
    const session = getNeo4jSession();
    try {
        const poliza = await Poliza.findOneAndUpdate(
            { nro_poliza },
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

        await redisClient.del('ranking:top10_clientes');

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
    updatePolizaEstado
};
