const mongoose = require('mongoose');

const AgenteEmbebidoSchema = new mongoose.Schema({
    id_agente: String,
    nombre: String,
    apellido: String,
    matricula: String
}, { _id: false });

const PolizaSchema = new mongoose.Schema({
    nro_poliza: { type: String, required: true, unique: true, index: true },
    cliente_id: { type: String, required: true, index: true },
    tipo: String,
    fecha_inicio: Date,
    fecha_fin: Date,
    prima_mensual: Number,
    cobertura_total: Number,
    estado: String,
    agente: { type: AgenteEmbebidoSchema, default: null }
}, { collection: 'polizas' });

module.exports = mongoose.model('Poliza', PolizaSchema);
