const Siniestro = require('../models/Siniestro');
const Poliza = require('../models/Poliza');
const { getNeo4jSession } = require('../config/db.neo4j');
const { redisClient } = require('../config/db.redis');

/**
 * Q14: Alta de nuevos siniestros
 *
 * IMPORTANT: Redis INCR should ONLY happen if the key already exists!
 * Always write to Neo4j regardless of Redis success.
 *
 * Affects: MongoDB (read + write), Neo4j (write), Redis (conditional increment)
 */
async function createSiniestro(siniestroData) {
    const session = getNeo4jSession();
    try {
        // 1. MongoDB (Lectura): Get the Poliza to obtain cliente_id and agente data
        const poliza = await Poliza.findOne({ nro_poliza: siniestroData.nro_poliza }).lean();
        if (!poliza) {
            throw new Error(`Poliza ${siniestroData.nro_poliza} not found`);
        }

        const agenteId = poliza.agente.id_agente;

        // 2. MongoDB (Escritura): Create the siniestro with poliza_snapshot
        const siniestro = new Siniestro({
            id_siniestro: siniestroData.id_siniestro,
            fecha: siniestroData.fecha || new Date(),
            tipo: siniestroData.tipo,
            monto_estimado: siniestroData.monto_estimado,
            descripcion: siniestroData.descripcion,
            estado: siniestroData.estado || 'Abierto',
            poliza_snapshot: {
                nro_poliza: poliza.nro_poliza,
                tipo_cobertura: poliza.tipo,
                fecha_vigencia_inicio: poliza.fecha_inicio,
                fecha_vigencia_fin: poliza.fecha_fin,
                cliente: {
                    id_cliente: poliza.cliente_id,
                    nombre: 'Cliente', // We would need to fetch from Cliente collection
                    contacto: 'email@example.com'
                },
                agente: {
                    id_agente: poliza.agente.id_agente,
                    nombre: `${poliza.agente.nombre} ${poliza.agente.apellido}`,
                    matricula: poliza.agente.matricula
                }
            }
        });
        await siniestro.save();

        // 3. Neo4j (Escritura): Create Siniestro node and relationship
        // ALWAYS write to Neo4j regardless of Redis status
        await session.run(`
            MATCH (p:Poliza {nro_poliza: $nro_poliza})
            CREATE (s:Siniestro {
                id_siniestro: $id_siniestro,
                tipo: $tipo,
                fecha: $fecha,
                estado: $estado,
                monto_estimado: $monto_estimado
            })
            CREATE (p)-[:CUBRE_SINIESTRO]->(s)
        `, {
            nro_poliza: poliza.nro_poliza,
            id_siniestro: siniestro.id_siniestro,
            tipo: siniestro.tipo,
            fecha: siniestro.fecha.toISOString(),
            estado: siniestro.estado,
            monto_estimado: siniestro.monto_estimado
        });

        // 4. Redis (Incremento CONDICIONAL):
        // ONLY increment if the key already exists
        try {
            // Check if the hash field exists
            const existingValue = await redisClient.hGet('counts:agente:siniestros', agenteId);

            if (existingValue !== null) {
                // Key exists, safe to increment
                await redisClient.hIncrBy('counts:agente:siniestros', agenteId, 1);
                console.log(`Redis: Incremented siniestro count for agente ${agenteId}`);
            } else {
                // Key doesn't exist, DO NOT increment
                console.log(`Redis: Skipped increment for agente ${agenteId} (key doesn't exist)`);
            }
        } catch (redisError) {
            // Redis error shouldn't fail the entire operation
            console.error('Redis error (non-fatal):', redisError.message);
        }

        return siniestro;
    } catch (error) {
        throw new Error(`Error creating siniestro: ${error.message}`);
    } finally {
        await session.close();
    }
}

/**
 * Get siniestro by id
 */
async function getSiniestroById(id_siniestro) {
    const siniestro = await Siniestro.findOne({ id_siniestro }).lean();
    if (!siniestro) {
        throw new Error('Siniestro not found');
    }
    return siniestro;
}

/**
 * Get all siniestros
 */
async function getAllSiniestros() {
    return await Siniestro.find().sort({ fecha: -1 }).lean();
}

/**
 * Update siniestro estado
 */
async function updateSiniestroEstado(id_siniestro, nuevoEstado) {
    const session = getNeo4jSession();
    try {
        // Update in MongoDB
        const siniestro = await Siniestro.findOneAndUpdate(
            { id_siniestro },
            { $set: { estado: nuevoEstado } },
            { new: true }
        );

        if (!siniestro) {
            throw new Error('Siniestro not found');
        }

        // Update in Neo4j
        await session.run(`
            MATCH (s:Siniestro {id_siniestro: $id_siniestro})
            SET s.estado = $estado
        `, { id_siniestro, estado: nuevoEstado });

        return siniestro;
    } catch (error) {
        throw new Error(`Error updating siniestro: ${error.message}`);
    } finally {
        await session.close();
    }
}

module.exports = {
    createSiniestro,
    getSiniestroById,
    getAllSiniestros,
    updateSiniestroEstado
};
