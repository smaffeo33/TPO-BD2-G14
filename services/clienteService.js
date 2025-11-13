const Cliente = require('../models/Cliente');
const { getNeo4jSession } = require('../config/db.neo4j');
const { redisClient } = require('../config/db.redis');
const {mongoose} = require("../config/db.mongo");
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

        return cliente;
    } catch (error) {
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
        const cliente = await Cliente.findOneAndUpdate(
            { _id: numericId },
            { $set: updates },
            { new: true }
        );

        if (!cliente) {
            throw new Error('Cliente not found');
        }

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

async function deleteCliente(id_cliente) {
    const session = getNeo4jSession();
    try {
        const numericId = Number(id_cliente);
        if (isNaN(numericId)) throw new Error('Invalid ID format');

        const polizasActivas = await polizaService.getActivePolizasByCliente(numericId);
        if (polizasActivas && polizasActivas.length > 0) {
            await Promise.all(
                polizasActivas.map(poliza =>
                    polizaService.updatePolizaEstado(poliza._id, 'Suspendida')
                )
            );
        }

        const cliente = await Cliente.findOneAndUpdate(
            { _id: numericId},
            { $set: { activo: false } },
            { new: true }
        );

        if (!cliente) {
            throw new Error('Cliente not found');
        }

        await session.run(`
            MATCH (c:Cliente {id_cliente: $id_cliente})
            SET c.activo = false  
        `, { id_cliente : numericId });

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
