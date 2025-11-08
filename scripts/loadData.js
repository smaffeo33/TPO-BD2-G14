/* eslint-disable no-console */
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

const { mongoose } = require('../config/db.mongo');
const { driver, getNeo4jSession } = require('../config/db.neo4j');
const { redisClient } = require('../config/db.redis');

const resourcesPath = path.join(__dirname, '..', 'resources');

/* ---------- helpers ---------- */

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

// dd/mm/yyyy → Date
function parseDateDDMMYYYY(dateStr) {
    if (!dateStr || typeof dateStr !== 'string') return null;
    const [d, m, y] = dateStr.split('/');
    const day = Number(d), month = Number(m), year = Number(y);
    if (!Number.isFinite(day) || !Number.isFinite(month) || !Number.isFinite(year)) return null;
    return new Date(year, month - 1, day);
}

// normalize booleans from 'True'/'true'
const truthy = (v) => v === true || v === 'True' || v === 'true';

// parse positive integer id, or return null
function toIntId(raw, labelForLog) {
    if (raw === null || raw === undefined) return null;
    const n = Number(String(raw).trim());
    if (!Number.isFinite(n) || n < 0 || !Number.isInteger(n)) {
        console.warn(`⚠️  Invalid numeric id for ${labelForLog}:`, raw);
        return null;
    }
    return n;
}

// extract numeric suffix from like "POL1042" → 1042; otherwise null
function extractNumericSuffix(str) {
    if (typeof str !== 'string') return null;
    const m = str.match(/(\d+)\s*$/);
    return m ? Number(m[1]) : null;
}

/* ---------- row parsers (CSV → JS) ---------- */

const parseCliente = (c) => ({
    _id: toIntId(c.id_cliente, 'cliente._id'),           // numeric _id
    id_cliente: undefined,                                // drop original string prop
    nombre: c.nombre,
    apellido: c.apellido,
    dni: c.dni,
    email: c.email,
    telefono: c.telefono,
    direccion: c.direccion,
    ciudad: c.ciudad,
    provincia: c.provincia,
    activo: truthy(c.activo)
});

const parseAgente = (a) => ({
    _id: toIntId(a.id_agente, 'agente._id'),              // numeric _id
    id_agente: undefined,
    nombre: a.nombre,
    apellido: a.apellido,
    matricula: a.matricula,
    telefono: a.telefono,
    email: a.email,
    zona: a.zona,
    activo: truthy(a.activo)
});

const parseVehiculo = (v) => ({
    _id: toIntId(v.id_vehiculo, 'vehiculo._id'),          // numeric _id (if your CSV has it)
    id_vehiculo: undefined,
    id_cliente: toIntId(v.id_cliente, 'vehiculo.id_cliente'),
    marca: v.marca,
    modelo: v.modelo,
    anio: Number(v.anio),
    patente: v.patente,
    nro_chasis: v.nro_chasis,
    asegurado: truthy(v.asegurado)
});

const parsePoliza = (p) => ({
    // keep natural string id as _id for the collection (avoids inventing ids)
    _id: p.nro_poliza,                                    // string _id = nro_poliza
    nro_poliza: p.nro_poliza,
    id_cliente: toIntId(p.id_cliente, 'poliza.id_cliente'),
    id_agente: p.id_agente && String(p.id_agente).trim() !== '' ? toIntId(p.id_agente, 'poliza.id_agente') : null,
    estado: p.estado ? String(p.estado).toLowerCase() : null,
    tipo: p.tipo,
    fecha_inicio: parseDateDDMMYYYY(p.fecha_inicio),
    fecha_fin: parseDateDDMMYYYY(p.fecha_fin),
    cobertura_total: Number(p.cobertura_total),
    prima_mensual: Number(p.prima_mensual)
});

const parseSiniestro = (s) => ({
    _id: toIntId(s.id_siniestro, 'siniestro._id'),        // numeric _id
    id_siniestro: undefined,
    nro_poliza: s.nro_poliza,                             // string ref to poliza
    tipo: s.tipo,
    fecha: parseDateDDMMYYYY(s.fecha),
    estado: s.estado,
    monto_estimado: Number(s.monto_estimado)
});

/* ---------- main ---------- */

async function main() {
    let mongoConn;

    try {
        console.log('🔌 Conectando a las bases de datos...');
        mongoConn = await mongoose.connect(
            process.env.MONGO_URI || 'mongodb://user:pass@localhost:27017/aseguradora?authSource=admin'
        );
        const db = mongoConn.connection.db;

        await driver.verifyConnectivity();
        const neo4jSession = getNeo4jSession();

        await redisClient.connect();

        console.log('Conexiones establecidas.\n🧹 Borrando datos antiguos...');
        await db.dropDatabase();
        await neo4jSession.run('MATCH (n) DETACH DELETE n');
        await redisClient.flushAll();
        console.log('Bases de datos limpiadas.');

        console.log('📥 Leyendo archivos CSV...');
        const [clientesCSV, agentesCSV, polizasCSV, siniestrosCSV, vehiculosCSV] = await Promise.all([
            processCSV('clientes.csv'),
            processCSV('agentes.csv'),
            processCSV('polizas.csv'),
            processCSV('siniestros.csv'),
            processCSV('vehiculos.csv')
        ]);

        console.log('🗺️  Mapeando / normalizando...');

        // Normalize first to detect & fix inconsistencies
        const clientes = clientesCSV.map(parseCliente).filter(c => {
            if (c._id == null) { console.warn(`⚠️  Cliente sin id válido, omitido`); return false; }
            return true;
        });

        const agentes = agentesCSV.map(parseAgente).filter(a => {
            if (a._id == null) { console.warn(`⚠️  Agente sin id válido, omitido`); return false; }
            return true;
        });

        // build quick lookups by numeric id
        const clienteById = new Map(clientes.map(c => [c._id, c]));
        const agenteById = new Map(agentes.map(a => [a._id, a]));

        // Polizas: keep string key, but numeric refs
        const polizasRaw = polizasCSV.map(parsePoliza).filter(p => {
            if (!p._id) { console.warn(`⚠️  Póliza sin nro_poliza, omitida`); return false; }
            if (p.id_cliente == null || !clienteById.has(p.id_cliente)) {
                console.warn(`⚠️  Póliza ${p._id} omitida: cliente ${p.id_cliente} no existe`);
                return false;
            }
            if (p.id_agente != null && !agenteById.has(p.id_agente)) {
                console.warn(`⚠️  Póliza ${p._id} omitida: agente ${p.id_agente} no existe`);
                return false;
            }
            return true;
        });

        // index polizas by nro_poliza
        const polizaById = new Map(polizasRaw.map(p => [p._id, p]));

        // Vehículos: numeric id_vehiculo (if present), numeric id_cliente
        const vehiculos = vehiculosCSV.map(parseVehiculo).filter(v => {
            if (v._id == null) { console.warn(`⚠️  Vehículo sin id válido, omitido`); return false; }
            if (v.id_cliente == null || !clienteById.has(v.id_cliente)) {
                console.warn(`⚠️  Vehículo ${v._id} omitido: cliente ${v.id_cliente} no existe`);
                return false;
            }
            return true;
        });

        // group vehicles by cliente
        const vehiculosByCliente = vehiculos.reduce((acc, v) => {
            if (!acc.has(v.id_cliente)) acc.set(v.id_cliente, []);
            acc.get(v.id_cliente).push({
                _id: v._id,
                marca: v.marca,
                modelo: v.modelo,
                anio: v.anio,
                patente: v.patente,
                nro_chasis: v.nro_chasis,
                asegurado: v.asegurado
            });
            return acc;
        }, new Map());

        // group polizas by cliente (already validated)
        const polizasByCliente = polizasRaw.reduce((acc, p) => {
            if (!acc.has(p.id_cliente)) acc.set(p.id_cliente, []);
            acc.get(p.id_cliente).push(p);
            return acc;
        }, new Map());

        // Siniestros: numeric id, link to poliza, build snapshot
        const siniestros = siniestrosCSV.map(parseSiniestro).filter(s => {
            if (s._id == null) { console.warn(`⚠️  Siniestro sin id válido, omitido`); return false; }
            if (!s.nro_poliza || !polizaById.has(s.nro_poliza)) {
                console.warn(`⚠️  Siniestro ${s._id} omitido: póliza ${s.nro_poliza} no existe`);
                return false;
            }
            return true;
        }).map(s => {
            const pol = polizaById.get(s.nro_poliza);
            const cli = clienteById.get(pol.id_cliente);
            const ag = pol.id_agente != null ? agenteById.get(pol.id_agente) : null;

            return {
                ...s,
                poliza_snapshot: {
                    nro_poliza: pol.nro_poliza,
                    tipo_cobertura: pol.tipo,
                    fecha_vigencia_inicio: pol.fecha_inicio,
                    fecha_vigencia_fin: pol.fecha_fin,
                    cliente: {
                        id_cliente: cli?._id,
                        nombre: `${cli?.nombre ?? ''} ${cli?.apellido ?? ''}`.trim(),
                        contacto: cli?.email ?? null
                    },
                    agente: ag ? {
                        id_agente: ag._id,
                        nombre: `${ag.nombre ?? ''} ${ag.apellido ?? ''}`.trim(),
                        matricula: ag.matricula ?? null
                    } : null
                }
            };
        });

        /* ---------- Mongo inserts ---------- */

        console.log('🧩 Insertando en MongoDB...');

        if (agentes.length) await db.collection('agentes').insertMany(agentes, { ordered: true });
        if (polizasRaw.length) await db.collection('polizas').insertMany(polizasRaw, { ordered: true });
        if (siniestros.length) await db.collection('siniestros').insertMany(siniestros, { ordered: true });

        // Enriquecer clientes con vehículos y poliza_auto_vigente (tipo=auto & estado activo)
        const clientesDocs = clientes.map(c => {
            const vhs = vehiculosByCliente.get(c._id) || [];
            const ps = polizasByCliente.get(c._id) || [];

            const polizaAutoVigente = ps.find(p => {
                const estado = (p.estado || '').toLowerCase();
                const tipo = (p.tipo || '').toLowerCase();
                return (estado === 'vigente' || estado === 'activa') && tipo === 'auto';
            });

            return {
                ...c,
                vehiculos: vhs,
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

        if (clientesDocs.length) await db.collection('clientes').insertMany(clientesDocs, { ordered: true });

        // finally: insert vehicles as their own collection too (if you want both embedding & separate collection)
        if (vehiculos.length) await db.collection('vehiculos').insertMany(vehiculos, { ordered: true });

        console.log('✅ MongoDB cargado.');

        /* ---------- Initialize counters with MAX ids ---------- */

        console.log('🧮 Inicializando counters...');
        const counters = db.collection('counters');

        const maxOrZero = (arr) => arr.length ? Math.max(...arr) : 0;

        const maxCliente = maxOrZero(clientes.map(c => c._id));
        const maxAgente  = maxOrZero(agentes.map(a => a._id));
        const maxVeh     = maxOrZero(vehiculos.map(v => v._id).filter(n => Number.isFinite(n)));
        const maxSini    = maxOrZero(siniestros.map(s => s._id));

        // Optional counter for polizas (based on numeric suffix), if you ever want to auto-issue new POLnnnn
        const polNumSuffixes = polizasRaw
            .map(p => extractNumericSuffix(p.nro_poliza))
            .filter(n => Number.isFinite(n));
        const maxPolNum = maxOrZero(polNumSuffixes);

        // Upsert all
        const upserts = [
            { _id: 'clientes', seq: maxCliente },
            { _id: 'agentes',  seq: maxAgente  },
            { _id: 'vehiculos',seq: maxVeh     },
            { _id: 'siniestros',seq: maxSini   },
            // keep this if you want to generate POL codes later; otherwise remove
            { _id: 'polizas_num_suffix', seq: maxPolNum }
        ];

        await Promise.all(upserts.map(doc =>
            counters.updateOne(
                { _id: doc._id },
                { $set: { seq: doc.seq } },
                { upsert: true }
            )
        ));
        console.log('✅ Counters inicializados:', upserts);

        /* ---------- Neo4j ---------- */
        function toYMDFromDDMMYYYY(s) {
            // expects "dd/mm/yyyy"
            if (!s || typeof s !== 'string') return null;
            const [dd, mm, yyyy] = s.split('/');
            if (!dd || !mm || !yyyy) return null;
            return `${yyyy}-${mm.padStart(2,'0')}-${dd.padStart(2,'0')}`;
        }

// Build primitive-only arrays for Neo4j
        const clientesForNeo = clientesCSV.map(c => ({
            id_cliente: Number(c.id_cliente),                  // or keep as String if you prefer
            nombre: `${c.nombre} ${c.apellido}`,
            activo: String(c.activo).toLowerCase() === 'true'
        }));

        const agentesForNeo = agentesCSV.map(a => ({
            id_agente: Number(a.id_agente),
            nombre: `${a.nombre} ${a.apellido}`,
            activo: String(a.activo).toLowerCase() === 'true'
        }));

        const polizasForNeo = polizasCSV.map(p => ({
            nro_poliza: p.nro_poliza,                          // e.g. "POL1042"
            estado: String(p.estado || '').toLowerCase(),      // "activa"/"vigente"/etc
            tipo: p.tipo,
            fecha_inicio: toYMDFromDDMMYYYY(p.fecha_inicio),   // "yyyy-mm-dd" string
            fecha_fin: toYMDFromDDMMYYYY(p.fecha_fin),
            cobertura_total: Number(p.cobertura_total),
            id_cliente: Number(p.id_cliente),
            id_agente: p.id_agente && p.id_agente.trim() !== '' ? Number(p.id_agente) : null
        }));

        const siniestrosForNeo = siniestrosCSV.map(s => ({
            id_siniestro: Number(s.id_siniestro),
            tipo: s.tipo,
            fecha: toYMDFromDDMMYYYY(s.fecha),                 // "yyyy-mm-dd" string
            estado: s.estado,
            monto_estimado: Number(s.monto_estimado),
            nro_poliza: s.nro_poliza
        }));


        console.log('🕸️  Cargando en Neo4j...');

        // Indexes (unchanged)
        await neo4jSession.run('CREATE INDEX cliente_id IF NOT EXISTS FOR (n:Cliente) ON (n.id_cliente)');
        await neo4jSession.run('CREATE INDEX agente_id IF NOT EXISTS FOR (n:Agente) ON (n.id_agente)');
        await neo4jSession.run('CREATE INDEX poliza_id  IF NOT EXISTS FOR (n:Poliza)  ON (n.nro_poliza)');
        await neo4jSession.run('CREATE INDEX siniestro_id IF NOT EXISTS FOR (n:Siniestro) ON (n.id_siniestro)');

// Nodes — only primitives, dates parsed in Cypher
        if (clientesForNeo.length) {
            await neo4jSession.run(
                'UNWIND $rows AS c CREATE (:Cliente {id_cliente: c.id_cliente, nombre: c.nombre, activo: c.activo})',
                { rows: clientesForNeo }
            );
        }
        if (agentesForNeo.length) {
            await neo4jSession.run(
                'UNWIND $rows AS a CREATE (:Agente {id_agente: a.id_agente, nombre: a.nombre, activo: a.activo})',
                { rows: agentesForNeo }
            );
        }
        if (polizasForNeo.length) {
            await neo4jSession.run(
                `UNWIND $rows AS p
     CREATE (:Poliza {
       nro_poliza: p.nro_poliza,
       estado: p.estado,
       tipo: p.tipo,
       fecha_inicio: CASE WHEN p.fecha_inicio IS NULL THEN NULL ELSE date(p.fecha_inicio) END,
       fecha_fin:    CASE WHEN p.fecha_fin    IS NULL THEN NULL ELSE date(p.fecha_fin)    END,
       cobertura_total: toFloat(p.cobertura_total)
     })`,
                { rows: polizasForNeo }
            );
        }
        if (siniestrosForNeo.length) {
            await neo4jSession.run(
                `UNWIND $rows AS s
     CREATE (:Siniestro {
       id_siniestro: s.id_siniestro,
       tipo: s.tipo,
       fecha: CASE WHEN s.fecha IS NULL THEN NULL ELSE date(s.fecha) END,
       estado: s.estado,
       monto_estimado: toFloat(s.monto_estimado)
     })`,
                { rows: siniestrosForNeo }
            );
        }

// Relationships — use the sanitized arrays
        if (polizasForNeo.length) {
            await neo4jSession.run(
                `UNWIND $rows AS p
     MATCH (c:Cliente {id_cliente: p.id_cliente})
     MATCH (po:Poliza  {nro_poliza: p.nro_poliza})
     CREATE (c)-[:TIENE_POLIZA]->(po)`,
                { rows: polizasForNeo }
            );

            const polizasConAgente = polizasForNeo.filter(p => p.id_agente !== null);
            if (polizasConAgente.length) {
                await neo4jSession.run(
                    `UNWIND $rows AS p
       MATCH (a:Agente {id_agente: p.id_agente})
       MATCH (po:Poliza {nro_poliza: p.nro_poliza})
       CREATE (a)-[:GESTIONA]->(po)`,
                    { rows: polizasConAgente }
                );
            }
        }

        if (siniestrosForNeo.length) {
            await neo4jSession.run(
                `UNWIND $rows AS s
     MATCH (po:Poliza {nro_poliza: s.nro_poliza})
     MATCH (si:Siniestro {id_siniestro: s.id_siniestro})
     CREATE (po)-[:CUBRE_SINIESTRO]->(si)`,
                { rows: siniestrosForNeo }
            );
        }


        console.log('✅ Neo4j cargado.');

        /* ---------- Redis ---------- */

        console.log('⚡ Cargando cálculos a Redis...');
        const multi = redisClient.multi();

        // 1) hset counts:agente:polizas
        const agentePolizasCount = polizasRaw
            .filter(p => Number.isFinite(p.id_agente))
            .reduce((acc, p) => acc.set(p.id_agente, (acc.get(p.id_agente) || 0) + 1), new Map());
        for (const [id, count] of agentePolizasCount) {
            multi.hSet('counts:agente:polizas', String(id), count);
        }

        // 2) hset counts:agente:siniestros
        const agenteSiniestrosCount = siniestros.reduce((acc, s) => {
            const pol = polizaById.get(s.nro_poliza);
            if (pol && Number.isFinite(pol.id_agente)) {
                acc.set(pol.id_agente, (acc.get(pol.id_agente) || 0) + 1);
            }
            return acc;
        }, new Map());
        for (const [id, count] of agenteSiniestrosCount) {
            multi.hSet('counts:agente:siniestros', String(id), count);
        }

        // 3) top10 clientes por cobertura_total
        const coberturaPorCliente = Array.from(polizasByCliente.entries()).map(([id_cliente, pols]) => ({
            id_cliente,
            total_cobertura: pols.reduce((sum, p) => sum + (Number(p.cobertura_total) || 0), 0)
        }));
        const topClientesData = coberturaPorCliente
            .sort((a, b) => b.total_cobertura - a.total_cobertura)
            .slice(0, 10)
            .map(entry => {
                const c = clienteById.get(entry.id_cliente);
                return { cliente_nombre: `${c?.nombre ?? ''} ${c?.apellido ?? ''}`.trim(), total_cobertura: entry.total_cobertura };
            });

        multi.set('ranking:top10_clientes', JSON.stringify(topClientesData));

        await multi.exec();
        console.log('✅ Redis cargado.\n🎉 ¡CARGA DE DATOS COMPLETA Y EXITOSA!');
    } catch (error) {
        console.error('❌ Error durante la carga de datos:', error);
    } finally {
        try { await mongoose.disconnect(); } catch {}
        try { await driver.close(); } catch {}
        try { await redisClient.quit(); } catch {}
        console.log('🔚 Conexiones cerradas.');
    }
}

main();
