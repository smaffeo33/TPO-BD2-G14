const mongoose = require('mongoose');

//TODO: Chequear si va a usar localhost o que
const MONGO_URI = process.env.MONGO_URI || 'mongodb://user:pass@localhost:27017/aseguradora?authSource=admin';

const connectMongo = async () => {
    try {
        await mongoose.connect(MONGO_URI);
        console.log('MongoDB conectado exitosamente.');
    } catch (error) {
        console.error('Error al conectar a MongoDB:', error);
        process.exit(1); // Detiene la aplicación si no se puede conectar
    }
};


module.exports = { connectMongo, mongoose };