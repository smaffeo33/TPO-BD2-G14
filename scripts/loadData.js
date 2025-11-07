
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

const { mongoose } = require('../config/db.mongo');
const { driver, getNeo4jSession } = require('../config/db.neo4j');
const { redisClient } = require('../config/db.redis');


const resourcesPath = path.join(__dirname, '..', 'resources');

function processCSV(fileName) {
    const filePath = path.join(resourcesPath, fileName);
    const results = [];
    return new Promise((resolve, reject) => {
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', () => resolve(results))
            .on('error', (error) => reject(error));
    });
}

const parseCliente = (c) => ({
    ...c,
    activo: c.activo === 'True' || c.activo === 'true'
});
const parseAgente = (a) => ({
    ...a,
    activo: a.activo === 'True' || a.activo === 'true'
});
const parsePoliza = (p) => ({
    ...p,
    prima_mensual: parseFloat(p.prima_mensual),
    cobertura_total: parseFloat(p.cobertura_total),
    fecha_inicio: parseDateDDMMYYYY(p.fecha_inicio),
    fecha_fin: parseDateDDMMYYYY(p.fecha_fin),
});
const parseVehiculo = (v) => ({
    ...v,
    anio: parseInt(v.anio, 10),
    asegurado: v.asegurado === 'True' || v.asegurado === 'true'
});
const parseSiniestro = (s) => ({
    ...s,
    monto_estimado: parseFloat(s.monto_estimado),
    fecha: parseDateDDMMYYYY(s.fecha),
});

function parseDateDDMMYYYY(dateStr) {
    const [day, month, year] = dateStr.split('/');
    return new Date(year, month - 1, day);
}


async function main() {
    let mongoConn;

    try {
        console.log('Conectando a las bases de datos...');

        mongoConn = await mongoose.connect(process.env.MONGO_URI || 'mongodb://user:pass@localhost:27017/aseguradora?authSource=admin');
        const db = mongoConn.connection.db;

        await driver.verifyConnectivity();
        const neo4jSession = getNeo4jSession();

        await redisClient.connect();

        console.log('Conexiones establecidas.');

        //Limpiamos las bases de datos biejas

        console.log('Borrando datos antiguos...');
        await db.dropDatabase();
        await neo4jSession.run('MATCH (n) DETACH DELETE n');
        await redisClient.flushAll();
        console.log('Bases de datos limpiadas.');

        console.log('Leyendo archivos CSV...');
        const [clientesCSV, agentesCSV, polizasCSV, siniestrosCSV, vehiculosCSV] = await Promise.all([
            processCSV('clientes.csv'),
            processCSV('agentes.csv'),
            processCSV('polizas.csv'),
            processCSV('siniestros.csv'),
            processCSV('vehiculos.csv')
        ]);

        console.log('Mapeando datos...');
        const clientesMap = new Map(clientesCSV.map(c => [c.id_cliente, parseCliente(c)]));
        const agentesMap = new Map(agentesCSV.map(a => [a.id_agente, parseAgente(a)]));

        const validPolizasCSV = polizasCSV.filter(p => {
            if (p.id_agente && p.id_agente.trim() !== '' && !agentesMap.has(p.id_agente)) {
                console.warn(`⚠️  Póliza ${p.nro_poliza} omitida: agente ${p.id_agente} no existe`);
                return false;
            }
            return true;
        });

        const polizasMap = new Map(validPolizasCSV.map(p => [p.nro_poliza, parsePoliza(p)]));

        const vehiculosByClienteMap = vehiculosCSV.reduce((acc, v) => {
            const parsedV = parseVehiculo(v);
            if (!acc.has(parsedV.id_cliente)) acc.set(parsedV.id_cliente, []);
            acc.get(parsedV.id_cliente).push(parsedV);
            return acc;
        }, new Map());

        const polizasByClienteMap = validPolizasCSV.reduce((acc, p) => {
            const parsedP = parsePoliza(p);
            if (!acc.has(parsedP.id_cliente)) acc.set(parsedP.id_cliente, []);
            acc.get(parsedP.id_cliente).push(parsedP);
            return acc;
        }, new Map());

        console.log('Transformando y cargando en MongoDB...');

        const agentesMongo = Array.from(agentesMap.values());
        if (agentesMongo.length > 0) await db.collection('agentes').insertMany(agentesMongo);

        const polizasMongo = Array.from(polizasMap.values()).map(p => {
            if (p.id_agente && p.id_agente.trim() !== '') {
                const agente = agentesMap.get(p.id_agente);
                return {
                    ...p,
                    agente: {
                        id_agente: agente.id_agente,
                        nombre: agente.nombre,
                        apellido: agente.apellido,
                        matricula: agente.matricula
                    }
                };
            } else {
                console.log(`ℹ️  Póliza ${p.nro_poliza} creada sin agente asignado`);
                return { ...p, agente: null };
            }
        });
        if (polizasMongo.length > 0) await db.collection('polizas').insertMany(polizasMongo);

        const siniestrosMongo = siniestrosCSV
            .filter(s => {
                const poliza = polizasMap.get(s.nro_poliza);
                if (!poliza) {
                    console.warn(`⚠️  Siniestro ${s.id_siniestro} omitido: póliza ${s.nro_poliza} no existe`);
                    return false;
                }
                return true;
            })
            .map(s => {
                const parsedS = parseSiniestro(s);
                const poliza = polizasMap.get(parsedS.nro_poliza);
                const cliente = clientesMap.get(poliza.id_cliente);

                let agenteSnapshot = null;
                if (poliza.id_agente && poliza.id_agente.trim() !== '') {
                    const agente = agentesMap.get(poliza.id_agente);
                    if (agente) {
                        agenteSnapshot = {
                            id_agente: agente.id_agente,
                            nombre: `${agente.nombre} ${agente.apellido}`,
                            matricula: agente.matricula
                        };
                    }
                }

                return {
                    ...parsedS,
                    poliza_snapshot: {
                        nro_poliza: poliza.nro_poliza,
                        tipo_cobertura: poliza.tipo,
                        fecha_vigencia_inicio: poliza.fecha_inicio,
                        fecha_vigencia_fin: poliza.fecha_fin,
                        cliente: {
                            id_cliente: cliente.id_cliente,
                            nombre: `${cliente.nombre} ${cliente.apellido}`,
                            contacto: cliente.email
                        },
                        agente: agenteSnapshot
                    }
                };
            });
        if (siniestrosMongo.length > 0) await db.collection('siniestros').insertMany(siniestrosMongo);

        const clientesMongo = Array.from(clientesMap.values()).map(c => {
            const vehiculos = vehiculosByClienteMap.get(c.id_cliente) || [];
            const clientePolizas = polizasByClienteMap.get(c.id_cliente) || [];

            const polizaAutoVigente = clientePolizas.find(p => {
                const estado = p.estado.toLowerCase();
                const tipo = p.tipo.toLowerCase();
                return (estado === 'vigente' || estado === 'activa') && tipo === 'auto';
            });
            return {
                ...c,
                vehiculos: vehiculos,
                poliza_auto_vigente: polizaAutoVigente ? {
                    nro_poliza: polizaAutoVigente.nro_poliza,
                    tipo: polizaAutoVigente.tipo,
                    fecha_inicio: polizaAutoVigente.fecha_inicio,
                    fecha_fin: polizaAutoVigente.fecha_fin,
                    cobertura_total: polizaAutoVigente.cobertura_total,
                    prima_mensual: polizaAutoVigente.prima_mensual
                } : null
            };
        });
        if (clientesMongo.length > 0) await db.collection('clientes').insertMany(clientesMongo);
        console.log('MongoDB cargado.');


        console.log('Cargando en Neo4j (sin vehículos)...');

        await neo4jSession.run('CREATE INDEX cliente_id IF NOT EXISTS FOR (n:Cliente) ON (n.id_cliente)');
        await neo4jSession.run('CREATE INDEX agente_id IF NOT EXISTS FOR (n:Agente) ON (n.id_agente)');
        await neo4jSession.run('CREATE INDEX poliza_id IF NOT EXISTS FOR (n:Poliza) ON (n.nro_poliza)');
        await neo4jSession.run('CREATE INDEX siniestro_id IF NOT EXISTS FOR (n:Siniestro) ON (n.id_siniestro)');

        if (clientesCSV.length > 0) await neo4jSession.run('UNWIND $clientes AS c CREATE (n:Cliente {id_cliente: c.id_cliente, nombre: c.nombre + " " + c.apellido, activo: c.activo = "True" OR c.activo = "true"})', { clientes: clientesCSV });
        if (agentesCSV.length > 0) await neo4jSession.run('UNWIND $agentes AS a CREATE (n:Agente {id_agente: a.id_agente, nombre: a.nombre + " " + a.apellido, activo: a.activo = "True" OR a.activo = "true"})', { agentes: agentesCSV });
        if (validPolizasCSV.length > 0) await neo4jSession.run('UNWIND $polizas AS p CREATE (n:Poliza {nro_poliza: p.nro_poliza, estado: toLower(p.estado), tipo: p.tipo, fecha_inicio: p.fecha_inicio, fecha_fin: p.fecha_fin, cobertura_total: toFloat(p.cobertura_total)})', { polizas: validPolizasCSV });
        if (siniestrosCSV.length > 0) await neo4jSession.run('UNWIND $siniestros AS s CREATE (n:Siniestro {id_siniestro: s.id_siniestro, tipo: s.tipo, fecha: s.fecha, estado: s.estado, monto_estimado: toFloat(s.monto_estimado)})', { siniestros: siniestrosCSV });
        console.log('Nodos de Neo4j cargados.');

        if (validPolizasCSV.length > 0) {
            await neo4jSession.run('UNWIND $polizas AS p MATCH (c:Cliente {id_cliente: p.id_cliente}) MATCH (p_node:Poliza {nro_poliza: p.nro_poliza}) CREATE (c)-[:TIENE_POLIZA]->(p_node)', { polizas: validPolizasCSV });

            const polizasConAgente = validPolizasCSV.filter(p => p.id_agente && p.id_agente.trim() !== '');
            if (polizasConAgente.length > 0) {
                await neo4jSession.run('UNWIND $polizas AS p MATCH (a:Agente {id_agente: p.id_agente}) MATCH (p_node:Poliza {nro_poliza: p.nro_poliza}) CREATE (a)-[:GESTIONA]->(p_node)', { polizas: polizasConAgente });
            }
        }
        if (siniestrosCSV.length > 0) {
            await neo4jSession.run('UNWIND $siniestros AS s MATCH (p_node:Poliza {nro_poliza: s.nro_poliza}) MATCH (s_node:Siniestro {id_siniestro: s.id_siniestro}) CREATE (p_node)-[:CUBRE_SINIESTRO]->(s_node)', { siniestros: siniestrosCSV });
        }
        console.log('Relaciones de Neo4j cargadas.');


        console.log('Calculando y cargando en Redis...');
        const multi = redisClient.multi();

        const agentePolizasCount = validPolizasCSV
            .filter(p => p.id_agente && p.id_agente.trim() !== '')
            .reduce((acc, p) => {
                acc.set(p.id_agente, (acc.get(p.id_agente) || 0) + 1);
                return acc;
            }, new Map());
        for (const [id, count] of agentePolizasCount) {
            multi.hSet("counts:agente:polizas", id, count);
        }

        const agenteSiniestrosCount = siniestrosCSV.reduce((acc, s) => {
            const poliza = polizasMap.get(s.nro_poliza);
            if (poliza && poliza.id_agente && poliza.id_agente.trim() !== '') {
                acc.set(poliza.id_agente, (acc.get(poliza.id_agente) || 0) + 1);
            }
            return acc;
        }, new Map());
        for (const [id, count] of agenteSiniestrosCount) {
            multi.hSet("counts:agente:siniestros", id, count);
        }

        const coberturaPorCliente = Array.from(polizasByClienteMap.entries()).map(([id_cliente, polizas]) => ({
            id_cliente, total_cobertura: polizas.reduce((sum, p) => sum + p.cobertura_total, 0)
        }));
        const topClientesData = coberturaPorCliente.sort((a, b) => b.total_cobertura - a.total_cobertura).slice(0, 10).map(entry => {
            const cliente = clientesMap.get(entry.id_cliente);
            return { cliente_nombre: `${cliente.nombre} ${cliente.apellido}`, total_cobertura: entry.total_cobertura };
        });
        multi.set("ranking:top10_clientes", JSON.stringify(topClientesData));

        await multi.exec();
        console.log('Redis cargado.');
        console.log('¡CARGA DE DATOS COMPLETA Y EXITOSA!');

    } catch (error) {
        console.error('Error durante la carga de datos:', error);
    } finally {
        if (mongoConn) await mongoConn.disconnect();
        await driver.close();
        await redisClient.quit();
        console.log('Conexiones cerradas.');
    }
}

main();