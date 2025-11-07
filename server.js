// server.js
const express = require('express');
const app = express();
const PORT = process.env.PORT || 3000;

// Import database connections
const { connectMongo } = require('./config/db.mongo');
const { checkNeo4jConnection } = require('./config/db.neo4j');
const { connectRedis } = require('./config/db.redis');

// Import routes
const apiRoutes = require('./routes/api');

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Routes
app.use('/api', apiRoutes);

// Root endpoint
app.get('/', (req, res) => {
    res.json({
        message: '¡El servidor de la Aseguradora está funcionando!',
        endpoints: {
            queries: {
                q1: 'GET /api/queries/q1 - Clientes activos con pólizas vigentes',
                q2: 'GET /api/queries/q2 - Siniestros abiertos',
                q3: 'GET /api/queries/q3 - Vehículos asegurados',
                q4: 'GET /api/queries/q4 - Clientes sin pólizas activas',
                q5: 'GET /api/queries/q5 - Agentes con cantidad de pólizas',
                q6: 'GET /api/queries/q6 - Pólizas vencidas',
                q7: 'GET /api/queries/q7 - Top 10 clientes por cobertura',
                q8: 'GET /api/queries/q8 - Siniestros tipo Accidente último año',
                q9: 'GET /api/queries/q9 - Pólizas activas ordenadas',
                q10: 'GET /api/queries/q10 - Pólizas suspendidas con cliente',
                q11: 'GET /api/queries/q11 - Clientes con varios vehículos',
                q12: 'GET /api/queries/q12 - Agentes con cantidad de siniestros',
                q13: 'POST /api/queries/q13: - Agregar cliente',
                q13Put: 'PUT /api/queries/q13:id - Actualizar cliente',
                q13Delete: 'DELETE /api/queries/q13:id - Eliminar cliente',
                q14: 'POST /api/queries/q14 - Alta de nuevos siniestros',
                q15: 'POST /api/queries/q15 - Emision de nuevas polizas'
            },
        }
    });
});

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

// Error handling middleware
app.use((err, req, res, next) => {
    console.error('Error:', err);
    res.status(500).json({
        success: false,
        error: err.message || 'Internal server error'
    });
});

// Initialize and start server
async function startServer() {
    try {
        console.log('🔌 Conectando a las bases de datos...');

        // Connect to MongoDB
        await connectMongo();

        // Connect to Neo4j
        await checkNeo4jConnection();

        // Connect to Redis
        await connectRedis();

        console.log('✅ Todas las bases de datos conectadas exitosamente\n');

        // Start Express server
        app.listen(PORT, () => {
            console.log(`🚀 Servidor corriendo en http://localhost:${PORT}`);
            console.log(`📖 Ver endpoints disponibles en http://localhost:${PORT}/`);
        });

    } catch (error) {
        console.error('❌ Error al iniciar el servidor:', error);
        process.exit(1);
    }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
    console.log('\n🛑 Cerrando servidor...');
    process.exit(0);
});

// Start the server
startServer();