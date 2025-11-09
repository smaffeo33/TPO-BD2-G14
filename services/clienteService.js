const Cliente = require('../models/Cliente');
const { getNeo4jSession } = require('../config/db.neo4j');
const { redisClient } = require('../config/db.redis');
const {mongoose} = require("../config/db.mongo");
// La corrección:
const polizaService = require("./polizaService");


/**
 * Q13: ABM de Clientes
 */

/**
 * Create a new cliente
 * Affects: MongoDB, Neo4j, Redis (invalidation)
 */
async function createCliente(clienteData) {
    const session = getNeo4jSession();
    try {
        // 1. MongoDB: Create cliente
        const cliente = new Cliente({
            nombre: clienteData.nombre,
            apellido: clienteData.apellido,
            dni: clienteData.dni,
            email: clienteData.email,
            telefono: clienteData.telefono,
            direccion: clienteData.direccion,
            ciudad: clienteData.ciudad,
            provincia: clienteData.provincia,
            activo: clienteData.activo !== undefined ? clienteData.activo : true,
            vehiculos: clienteData.vehiculos || [],
            poliza_auto_vigente: null
        });
        await cliente.save();

        // 2. Neo4j: Create node
        await session.run(`
            CREATE (c:Cliente {
                id_cliente: $id_cliente,
                nombre: $nombre,
                activo: $activo
            })
        `, {
            id_cliente: cliente.id_cliente,
            nombre: `${cliente.nombre} ${cliente.apellido}`,
            activo: cliente.activo
        });

        // 3. Redis: Invalidate top10 ranking (in case this client gets policies later)
        // No need to invalidate now, but good practice

        return cliente;
    } catch (error) {
        // Rollback would require transaction support - for now, throw error
        throw new Error(`Error creating cliente: ${error.message}`);
    } finally {
        await session.close();
    }
}

/**
 * Update an existing cliente
 * Affects: MongoDB, Neo4j, Redis (invalidation)
 */
async function updateCliente(id_cliente, updates) {
    const session = getNeo4jSession();
    try {
        const numericId = Number(id_cliente);
        if (isNaN(numericId)) throw new Error('Invalid ID format');
        // 1. MongoDB: Update cliente
        const cliente = await Cliente.findOneAndUpdate(
            { _id: numericId },
            { $set: updates },
            { new: true }
        );

        if (!cliente) {
            throw new Error('Cliente not found');
        }

        // 2. Neo4j: Update node properties
        const neo4jUpdates = {};
        if (updates.nombre || updates.apellido) {
            const currentCliente = await Cliente.findOne({ _id:numericId }).lean();
            neo4jUpdates.nombre = `${updates.nombre || currentCliente.nombre} ${updates.apellido || currentCliente.apellido}`;
        }
        if (updates.activo !== undefined) {
            neo4jUpdates.activo = updates.activo;
        }

        if (Object.keys(neo4jUpdates).length > 0) {
            await session.run(`
                MATCH (c:Cliente {id_cliente: $id_cliente})
                SET c += $updates
            `, {
                id_cliente: numericId,
                updates: neo4jUpdates
            });
        }

        // 3. Redis: Invalidate ranking (name might have changed)
        await redisClient.del('ranking:top10_clientes');

        return cliente;
    } catch (error) {
        throw new Error(`Error updating cliente: ${error.message}`);
    } finally {
        await session.close();
    }
}

/**
 * Delete a cliente
 * Affects: MongoDB, Neo4j, Redis (invalidation)
 */
// services/cliente.service.js

async function deleteCliente(id_cliente) {
    const session = getNeo4jSession();
    try {
        const numericId = Number(id_cliente);
        if (isNaN(numericId)) throw new Error('Invalid ID format');
        // -------------------------


        // 1. Buscar y suspender pólizas activas (¡Esta lógica está perfecta!)
        const polizasActivas = await polizaService.getActivePolizasByCliente(numericId);
        if (polizasActivas && polizasActivas.length > 0) {
            await Promise.all(
                polizasActivas.map(poliza => // <-- 'poliza' es un objeto
                    polizaService.updatePolizaEstado(poliza._id, 'Suspendida') // <-- ¡ARREGLADO!
                )
            );
        }

        // 2. MongoDB: NO BORRAR, "desactivar"
        const cliente = await Cliente.findOneAndUpdate(
            { _id: numericId},
            { $set: { activo: false } },
            { new: true }
        );

        if (!cliente) {
            throw new Error('Cliente not found');
        }

        // 3. Neo4j: NO BORRAR, "desactivar"
        await session.run(`
            MATCH (c:Cliente {id_cliente: $id_cliente})
            SET c.activo = false  
        `, { id_cliente : numericId });

        // 4. Redis: Invalidar caché
        await redisClient.del('ranking:top10_clientes');

        return {
            success: true,
            message: `Cliente ${id_cliente} marcado como INACTIVO.`
        };
    } catch (error) {
        throw new Error(`Error deactivating cliente: ${error.message}`);
    } finally {
        await session.close();
    }
}

/**
 * Get a cliente by id
 */
async function getClienteById(id_cliente) {
    const cliente = await Cliente.findOne({ id_cliente }).lean();
    if (!cliente) {
        throw new Error('Cliente not found');
    }
    return cliente;
}

/**
 * Get all clientes
 */
async function getAllClientes() {
    return await Cliente.find().lean();
}

/**
 * Add vehicle to cliente
 * This is part of Q13 - managing client's vehicles
 */
async function addVehicleToCliente(id_cliente, vehiculoData) {
    const cliente = await Cliente.findOneAndUpdate(
        { id_cliente },
        {
            $push: {
                vehiculos: {
                    id_vehiculo: vehiculoData.id_vehiculo,
                    marca: vehiculoData.marca,
                    modelo: vehiculoData.modelo,
                    anio: vehiculoData.anio,
                    patente: vehiculoData.patente,
                    nro_chasis: vehiculoData.nro_chasis,
                    asegurado: vehiculoData.asegurado || false
                }
            }
        },
        { new: true }
    );

    if (!cliente) {
        throw new Error('Cliente not found');
    }

    return cliente;
}

/**
 * Remove vehicle from cliente
 */
async function removeVehicleFromCliente(id_cliente, id_vehiculo) {
    const cliente = await Cliente.findOneAndUpdate(
        { id_cliente },
        {
            $pull: {
                vehiculos: { id_vehiculo }
            }
        },
        { new: true }
    );

    if (!cliente) {
        throw new Error('Cliente not found');
    }

    return cliente;
}

module.exports = {
    createCliente,
    updateCliente,
    deleteCliente,
    getClienteById,
    getAllClientes,
    addVehicleToCliente,
    removeVehicleFromCliente
};
