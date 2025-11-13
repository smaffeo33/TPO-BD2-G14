const mongoose = require('mongoose');

const MONGO_URI = process.env.MONGO_URI || 'mongodb://user:pass@localhost:27017/aseguradora?authSource=admin';

const connectMongo = async () => {
    try {
        await mongoose.connect(MONGO_URI);
        console.log('MongoDB conectado exitosamente.');
    } catch (error) {
        console.error('Error al conectar a MongoDB:', error);
        process.exit(1);
    }
};


module.exports = { connectMongo, mongoose };