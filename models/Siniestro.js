const mongoose = require('mongoose');

const PolizaSnapshotSchema = new mongoose.Schema({
    nro_poliza: String,
    tipo_cobertura: String,
    fecha_vigencia_inicio: Date,
    fecha_vigencia_fin: Date,
    cliente: {
        id_cliente: String,
        nombre: String,
        contacto: String
    },
    agente: {
        id_agente: String,
        nombre: String,
        matricula: String
    }
}, { _id: false });

const SiniestroSchema = new mongoose.Schema({
    id_siniestro: { type: String, required: true, unique: true, index: true },
    fecha: Date,
    tipo: String,
    monto_estimado: Number,
    descripcion: String,
    estado: String,
    poliza_snapshot: PolizaSnapshotSchema
}, { collection: 'siniestros' });

module.exports = mongoose.model('Siniestro', SiniestroSchema);
