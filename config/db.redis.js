const { createClient } = require('redis');


const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = process.env.REDIS_PORT || 6379;

const redisClient = createClient({
    socket: {
        host: REDIS_HOST,
        port: REDIS_PORT,
    },
});

redisClient.on('error', (err) => {
    console.error('Error en el cliente de Redis:', err);
});


const connectRedis = async () => {
    try {
        await redisClient.connect();
        console.log('Redis conectado exitosamente.');
    } catch (error) {
        console.error('Error al conectar a Redis:', error);
        process.exit(1);
    }
};

module.exports = { redisClient, connectRedis };