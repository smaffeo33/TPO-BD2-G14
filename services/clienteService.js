const Cliente = require('../models/Cliente');
const { getNeo4jSession } = require('../config/db.neo4j');
const { redisClient } = require('../config/db.redis');

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
            id_cliente: clienteData.id_cliente,
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
        // 1. MongoDB: Update cliente
        const cliente = await Cliente.findOneAndUpdate(
            { id_cliente },
            { $set: updates },
            { new: true }
        );

        if (!cliente) {
            throw new Error('Cliente not found');
        }

        // 2. Neo4j: Update node properties
        const neo4jUpdates = {};
        if (updates.nombre || updates.apellido) {
            const currentCliente = await Cliente.findOne({ id_cliente }).lean();
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
                id_cliente,
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
async function deleteCliente(id_cliente) {
    const session = getNeo4jSession();
    try {
        // 1. Check if cliente has active policies
        const result = await session.run(`
            MATCH (c:Cliente {id_cliente: $id_cliente})-[:TIENE_POLIZA]->(p:Poliza)
            WHERE p.estado = 'vigente' OR p.estado = 'activa'
            RETURN count(p) AS count
        `, { id_cliente });

        const activePolizasCount = result.records[0].get('count').toNumber();
        if (activePolizasCount > 0) {
            throw new Error('Cannot delete cliente with active policies');
        }

        // 2. MongoDB: Delete cliente
        const cliente = await Cliente.findOneAndDelete({ id_cliente });
        if (!cliente) {
            throw new Error('Cliente not found');
        }

        // 3. Neo4j: Delete node and all relationships
        await session.run(`
            MATCH (c:Cliente {id_cliente: $id_cliente})
            DETACH DELETE c
        `, { id_cliente });

        // 4. Redis: Invalidate ranking
        await redisClient.del('ranking:top10_clientes');

        return { success: true, message: 'Cliente deleted successfully' };
    } catch (error) {
        throw new Error(`Error deleting cliente: ${error.message}`);
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
