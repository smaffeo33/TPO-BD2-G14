const mongoose = require('mongoose');
const nextSeq = require('./nextSeq');

const AgenteEmbebidoSchema = new mongoose.Schema({
    id_agente: Number,
    nombre: String,
    apellido: String,
    matricula: String
}, { _id: false });

const PolizaSchema = new mongoose.Schema({
    _id: { type: String },
    id_cliente: { type: Number, required: true, index: true },
    tipo: String,
    fecha_inicio: Date,
    fecha_fin: Date,
    prima_mensual: Number,
    cobertura_total: Number,
    estado: String,
    agente: { type: AgenteEmbebidoSchema, default: null }
}, {
    collection: 'polizas',
    toJSON: { virtuals: true },
    toObject: { virtuals: true },
    versionKey: false
});


PolizaSchema.pre('validate', async function (next) {
    try {
        if (this.isNew && !this._id) {
            const numSuffix = await nextSeq('polizas_num_suffix');
            this._id = `POL${numSuffix}`;
        }
        next();
    } catch (error) {
        next(error);
    }
});

PolizaSchema.virtual('nro_poliza')
    .get(function () {
        return this._id;
    })
    .set(function (v) {
        this._id = v == null ? v : String(v);
    });


module.exports = mongoose.model('Poliza', PolizaSchema);
