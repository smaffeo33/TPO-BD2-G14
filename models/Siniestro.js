const mongoose = require('mongoose');
const nextSeq = require("./nextSeq");

const PolizaSnapshotSchema = new mongoose.Schema({
    nro_poliza: String,
    tipo_cobertura: String,
    fecha_vigencia_inicio: Date,
    fecha_vigencia_fin: Date,
    cliente: {
        id_cliente: Number,
        nombre: String,
        contacto: String
    },
    agente: {
        id_agente: Number,
        nombre: String,
        matricula: String
    }
}, { _id: false });

const SiniestroSchema = new mongoose.Schema({
    _id: { type: Number },
    fecha: Date,
    tipo: String,
    monto_estimado: Number,
    descripcion: String,
    estado: String,
    poliza_snapshot: PolizaSnapshotSchema
}, {
    collection: 'siniestros',
    toJSON: { virtuals: true },
    toObject: { virtuals: true },
    versionKey: false
});

SiniestroSchema.virtual('id_siniestro')
    .get(function () { return this._id; })
    .set(function (v) { this._id = (v == null ? v : Number(v)); });

SiniestroSchema.pre('validate', async function (next) {
    try {
        if (this.isNew && (this._id === undefined || this._id === null)) {
            this._id = await nextSeq('siniestros');   // Number
        }
        next();
    } catch (err) {
        next(err);
    }
});

module.exports = mongoose.model('Siniestro', SiniestroSchema);
