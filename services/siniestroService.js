// services/siniestro.service.js

const Siniestro = require('../models/Siniestro');
const Poliza = require('../models/Poliza');
const Cliente = require('../models/Cliente');
const { getNeo4jSession } = require('../config/db.neo4j');
const { redisClient } = require('../config/db.redis');
const { ensureCacheIsWarm } = require('./cacheSync');

const Q12_HASH_KEY = 'counts:agente:siniestros';
const Q12_LOCK_KEY = 'lock:cache:repopulating_q12';
// CORRECCIÓN 1: Forzar el ID a String para que coincida con loadData.js
const Q12_NEO4J_QUERY = `
    MATCH (a:Agente)-[:GESTIONA]->(p:Poliza)-[:CUBRE_SINISTRO]->(s:Siniestro)
    RETURN toString(a.id_agente) AS id, count(s) AS total
`;

async function createSiniestro(siniestroData) {
    const session = getNeo4jSession();
    try {
        // 1. MongoDB (Lectura): Poliza._id es un STRING (nro_poliza), esto está OK.
        const poliza = await Poliza.findOne({ _id: siniestroData.nro_poliza }).lean();
        if (!poliza) {
            throw new Error(`Poliza ${siniestroData.nro_poliza} not found`);
        }

        // agenteId será un NÚMERO (del snapshot de poliza)
        const agenteId = poliza.agente.id_agente;
        // id_cliente será un NÚMERO (de la poliza)
        const clienteId = poliza.id_cliente;

        // 1b. MongoDB (Lectura): Cliente.findOne por _id numérico. Esto está OK.
        const cliente = await Cliente.findOne({ _id: clienteId }).lean();
        if (!cliente) {
            throw new Error(`Cliente ${clienteId} asociado a la póliza no fue encontrado.`);
        }

        // 2. MongoDB (Escritura): Siniestro.save() auto-generará el _id numérico
        const siniestro = new Siniestro({
            fecha: siniestroData.fecha || new Date(),
            tipo: siniestroData.tipo,
            monto_estimado: siniestroData.monto_estimado,
            descripcion: siniestroData.descripcion,
            estado: siniestroData.estado || 'Abierto',
            poliza_snapshot: {
                nro_poliza: siniestroData.nro_poliza,
                tipo_cobertura: poliza.tipo,
                fecha_vigencia_inicio: poliza.fecha_inicio,
                fecha_vigencia_fin: poliza.fecha_fin,
                cliente: {
                    id_cliente: clienteId,
                    nombre: `${cliente.nombre} ${cliente.apellido}`,
                    contacto: cliente.email
                },
                agente: {
                    id_agente: agenteId,
                    nombre: `${poliza.agente.nombre} ${poliza.agente.apellido}`,
                    matricula: poliza.agente.matricula
                }
            }
        });
        await siniestro.save(); // El hook pre-validate llena siniestro._id (numérico)

        // 3. Neo4j (Escritura):
        await session.run(`
            MATCH (p:Poliza {nro_poliza: $nro_poliza})
            CREATE (s:Siniestro {
                id_siniestro: $id_siniestro,
                tipo: $tipo,
                fecha: $fecha,
                estado: $estado,
                monto_estimado: $monto_estimado
            })
            CREATE (p)-[:CUBRE_SINISTRO]->(s)
        `, {
            nro_poliza: siniestroData.nro_poliza,
            id_siniestro: siniestro._id, // <-- CORRECCIÓN 2: Usar el _id real (numérico)
            tipo: siniestro.tipo,
            fecha: siniestro.fecha.toISOString(),
            estado: siniestro.estado,
            monto_estimado: siniestro.monto_estimado
        });

        // 4. Redis (Incremento Q12):
        try {
            const { wasWarm } = await ensureCacheIsWarm(Q12_HASH_KEY, Q12_LOCK_KEY, Q12_NEO4J_QUERY);

            if (wasWarm) {
                // CORRECCIÓN 3: Usar String(agenteId) para el HASH
                await redisClient.hIncrBy(Q12_HASH_KEY, String(agenteId), 1);
                console.log(`Redis: Incremented siniestro count for agente ${agenteId}`);
            } else {
                console.log(`Redis: Cache was just populated, no increment needed for agente ${agenteId}`);
            }

        } catch (redisError) {
            console.error('Redis error (non-fatal) during Q12 sync:', redisError.message);
        }

        return siniestro;
    } catch (error) {
        throw new Error(`Error creating siniestro: ${error.message}`);
    } finally {
        await session.close();
    }
}

async function getSiniestroById(id_siniestro) {
    // CORRECCIÓN 4: Convertir el ID de string (de la URL) a número
    const numericId = Number(id_siniestro);
    if (isNaN(numericId)) throw new Error('Invalid ID format');

    // Buscar por el _id numérico
    const siniestro = await Siniestro.findOne({ _id: numericId }).lean();
    if (!siniestro) {
        throw new Error('Siniestro not found');
    }
    return siniestro;
}

async function getAllSiniestros() {
    return await Siniestro.find().sort({ fecha: -1 }).lean();
}

async function updateSiniestroEstado(id_siniestro, nuevoEstado) {
    const session = getNeo4jSession();
    try {
        // CORRECCIÓN 5: Convertir el ID de string (de la URL) a número
        const numericId = Number(id_siniestro);
        if (isNaN(numericId)) throw new Error('Invalid ID format');

        // Update in MongoDB (usando _id numérico)
        const siniestro = await Siniestro.findOneAndUpdate(
            { _id: numericId }, // <-- CORREGIDO
            { $set: { estado: nuevoEstado } },
            { new: true }
        );

        if (!siniestro) {
            throw new Error('Siniestro not found');
        }

        // Update in Neo4j (usando id_siniestro numérico)
        await session.run(`
            MATCH (s:Siniestro {id_siniestro: $id_siniestro})
            SET s.estado = $estado
        `, { id_siniestro: numericId, estado: nuevoEstado }); // <-- CORREGIDO

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