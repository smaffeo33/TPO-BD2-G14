# Sistema de Gestión de Aseguradoras - Grupo 14

Sistema de backoffice que implementa persistencia políglota combinando **MongoDB** (Source of Truth), **Neo4j** (motor de joins) y **Redis** (caché de agregaciones). La API expone 15 requerimientos (Q1–Q15) mediante un servidor Express y se distribuye con Docker Compose y configuración lista para GitHub Codespaces.

---

## 📋 Tabla de Contenidos
1. [Requisitos Previos](#-requisitos-previos)
2. [Instalación](#-instalación)
3. [Levantar Servicios con Docker](#-levantar-servicios-con-docker)
4. [Cargar Datos Iniciales](#-cargar-datos-iniciales)
5. [Iniciar el Servidor](#-iniciar-el-servidor)
6. [Pruebas y Queries Q1–Q12](#-pruebas-y-queries-q1q12)
7. [Servicios Transaccionales Q13–Q15](#-servicios-transaccionales-q13q15)
8. [Verificación de Bases](#-verificación-de-bases)
9. [Uso en GitHub Codespaces](#-uso-en-github-codespaces)
10. [Detener Servicios](#-detener-servicios)
---

## 🔧 Requisitos Previos
- **Docker 24+** y **Docker Compose v2**
- **Node.js 18+** y **npm**
- **curl** y opcionalmente **jq** para formatear JSON (la imagen del contenedor ya los trae preinstalados; solo son necesarios localmente si querés probar desde fuera)

```bash
docker --version
docker compose version
node --version
npm --version
```

---

## Instalación
```bash
cd /ruta/al/proyecto/TPO-BD2-G14
npm install
```

---

## Levantar Servicios con Docker
```bash
docker compose up -d
```
Servicios expuestos: MongoDB (27017), Neo4j (7474/7687), Redis (6379) y la app (3000). Verifica con `docker ps` que estén "Up".

---

## Cargar Datos Iniciales
Espera 10–15 s luego de levantar los contenedores y ejecuta:
```bash
node scripts/loadData.js
```
El script limpia las tres bases, lee los CSV de `resources/` y pobla:
- MongoDB: colecciones `agentes`, `clientes`, `polizas`, `siniestros` (con snapshots y vehículos embebidos)
- Neo4j: nodos Cliente/Agente/Poliza/Siniestro + relaciones TIENE_POLIZA/GESTIONA/CUBRE_SINIESTRO
- Redis: `counts:agente:polizas`, `counts:agente:siniestros`, `ranking:top10_clientes`

---

## Iniciar el Servidor
```bash
npm start   # o node server.js
```
Verás:
```
MongoDB conectado…
Neo4j conectado…
Redis conectado…
🚀 Servidor corriendo en http://localhost:3000
```

---

## Pruebas y Queries Q1–Q12
Abre una terminal aparte (el servidor debe seguir corriendo). Las rutas están bajo `/api/queries/qX`. 
Es muy recomendable utilizar `curl -s … | jq '.'` para formatear.

### Health & Endpoints
```bash
curl http://localhost:3000/health
curl http://localhost:3000/
```

### Consultas
| Query | Descripción | Ejemplo |
|-------|-------------|---------|
| Q1 | Clientes activos con pólizas vigentes | `curl http://localhost:3000/api/queries/q1` |
| Q2 | Siniestros abiertos + cliente | `curl http://localhost:3000/api/queries/q2` |
| Q3 | Vehículos asegurados + cliente/póliza | `curl http://localhost:3000/api/queries/q3` |
| Q4 | Clientes sin pólizas activas| `curl http://localhost:3000/api/queries/q4` |
| Q5 | Agentes con cantidad de pólizas | `curl http://localhost:3000/api/queries/q5` |
| Q6 | Pólizas vencidas + cliente | `curl http://localhost:3000/api/queries/q6` |
| Q7 | Top 10 clientes por cobertura | `curl http://localhost:3000/api/queries/q7` |
| Q8 | Siniestros "Accidente" del último año | `curl http://localhost:3000/api/queries/q8` |
| Q9 | Pólizas activas ordenadas | `curl http://localhost:3000/api/queries/q9` |
| Q10 | Pólizas suspendidas + estado del cliente | `curl http://localhost:3000/api/queries/q10` |
| Q11 | Clientes con más de un vehículo | `curl http://localhost:3000/api/queries/q11` |
| Q12 | Agentes y siniestros asociados | `curl http://localhost:3000/api/queries/q12` |

### Pruebas recomendadas
1. `curl /api/queries/q5` dos veces para ver el impacto de Redis (la primera pobla desde Neo4j, la segunda responde instantáneamente).
2. `curl /api/queries/q7`, luego emitir una nueva póliza (Q15) y volver a consultar para confirmar la invalidación del ranking.
3. `curl /api/queries/q3` tras agregar un vehículo via Q13 para ver el documento embebido actualizado.

---

## Servicios Transaccionales Q13–Q15
Estos endpoints aceptan/retornan JSON. Usa `Content-Type: application/json`.

### Q13 – ABM de Clientes (`services/clienteService.js`)
- **Crear**: `POST /api/queries/q13`
```bash
curl -X POST http://localhost:3000/api/queries/q13 \
  -H "Content-Type: application/json" \
  -d '{
    "nombre": "Juan",
    "apellido": "Pérez",
    "dni": "12345678",
    "email": "juan@example.com",
    "telefono": "123456789",
    "direccion": "Calle Falsa 123",
    "ciudad": "Buenos Aires",
    "provincia": "Buenos Aires",
    "activo": true,
    "vehiculos": [
      {"marca": "Ford", "modelo": "Focus", "anio": 2022, "patente": "ABC123", "asegurado": true}
    ]
  }'
```
  - Mongo crea el documento (IDs autoincrementales vía `nextSeq`)
  - Neo4j crea el nodo Cliente
- **Actualizar**: `PUT /api/queries/q13/:id`
```bash
curl -X PUT http://localhost:3000/api/queries/q13/1 \
  -H "Content-Type: application/json" \
  -d '{ "telefono": "011-555-0000", "nombre": "Juan Carlos" }'
```
  - Mongo actualiza campos
  - Neo4j sincroniza nombre/estado
  - Redis invalida `ranking:top10_clientes`
- **Eliminar (baja lógica)**: `DELETE /api/queries/q13/:id` → marca `activo=false`, suspende pólizas vigentes del cliente y desactiva el nodo en Neo4j.

### Q14 – Alta de Siniestros (`services/siniestroService.js`)
`POST /api/queries/q14`
```bash
curl -X POST http://localhost:3000/api/queries/q14 \
  -H "Content-Type: application/json" \
  -d '{
    "nro_poliza": "POL1001",
    "fecha": "2025-11-07T00:00:00.000Z",
    "tipo": "Accidente",
    "monto_estimado": 150000,
    "descripcion": "Choque en intersección",
    "estado": "Abierto"
  }'
```
- Mongo crea el siniestro y guarda `poliza_snapshot`
- Neo4j crea el nodo y relación `(:Poliza)-[:CUBRE_SINIESTRO]->(:Siniestro)`
- Redis: incrementa `counts:agente:siniestros` **si** la caché estaba caliente; de lo contrario la próxima lectura repobla desde Neo4j.

### Q15 – Emisión de Pólizas (`services/polizaService.js`)
`POST /api/queries/q15`

> El campo `nro_poliza` del payload se usa directamente como `_id` en MongoDB. Si lo omitís, el backend genera automáticamente un `POL####` incremental y lo devuelve en la respuesta.
```bash
curl -X POST http://localhost:3000/api/queries/q15 \
  -H "Content-Type: application/json" \
  -d '{
    "nro_poliza": "POL9999",
    "id_cliente": "1",
    "id_agente": "101",
    "tipo": "Vida",
    "fecha_inicio": "2025-11-07T00:00:00.000Z",
    "fecha_fin": "2026-11-07T00:00:00.000Z",
    "prima_mensual": 35000,
    "cobertura_total": 3000000,
    "estado": "Activa"
  }'
```
- Valida en Neo4j que cliente y agente estén activos
- Mongo guarda la póliza con agente embebido y actualiza `poliza_auto_vigente` del cliente
- Neo4j crea nodo + relaciones; si ya había una póliza Auto vigente se marca como vencida en Mongo/Neo4j
- Redis invalida `ranking:top10_clientes` y si Q5 estaba caliente incrementa `counts:agente:polizas`

---

## erificación de Bases
### MongoDB
```bash
docker exec -it mongo_db mongosh -u user -p pass --authenticationDatabase admin aseguradora
```
Comandos útiles:
```javascript
db.clientes.countDocuments()
db.polizas.findOne()
db.siniestros.find().limit(3)
```

### Neo4j
Browser en `http://localhost:7474` (`neo4j/password123`). Ejemplos:
```cypher
MATCH (c:Cliente)-[:TIENE_POLIZA]->(p:Poliza) RETURN c,p LIMIT 5;
MATCH (a:Agente)-[:GESTIONA]->(p:Poliza) RETURN a,p LIMIT 5;
```

### Redis
```bash
docker exec -it redis_cache redis-cli
KEYS *
HGETALL counts:agente:polizas
GET ranking:top10_clientes
```

---

## Uso en GitHub Codespaces
El archivo `.devcontainer/devcontainer.json` reutiliza `docker-compose.yml` para levantar `app`, `mongo_db`, `neo4j_db` y `redis_cache` dentro de Codespaces.

1. En GitHub → **Code → Create codespace on main**
2. Codespaces monta `/usr/src/app`, ejecuta `npm install` (postCreate) y expone los puertos 3000/7474/7687/6379/27017
3. Como Codespaces ya levantó todos los servicios definidos en `docker-compose.yml`, solo necesitás ejecutar:
   ```bash
   node scripts/loadData.js
   npm start
   ```
   (No hace falta ni es posible correr `docker compose` dentro del contenedor; esa tarea la realiza GitHub al crear el Codespace.)
4. Usa la pestaña **Ports** para abrir el puerto 3000 y probar los endpoints. También podés usar `curl` directamente dentro del Codespace.
5. Validación rápida: `redis-cli -h redis_cache ping` y `cypher-shell -a neo4j://neo4j_db:7687 -u neo4j -p password123 "RETURN 1"`

---

## Detener Servicios
- Servidor Node: `Ctrl + C`
- Contenedores: `docker compose down`
- Para borrar datos: `docker compose down -v`

---
