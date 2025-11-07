const mongoose = require('mongoose');

const AgenteSchema = new mongoose.Schema({
    id_agente: { type: String, required: true, unique: true, index: true },
    nombre: String,
    apellido: String,
    matricula: String,
    telefono: String,
    email: String,
    zona: String,
    activo: Boolean
}, { collection: 'agentes' });

module.exports = mongoose.model('Agente', AgenteSchema);
