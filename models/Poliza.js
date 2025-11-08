const mongoose = require('mongoose');
const nextSeq = require('./nextSeq');

const AgenteEmbebidoSchema = new mongoose.Schema({
    id_agente: Number,
    nombre: String,
    apellido: String,
    matricula: String
}, { _id: false });

const PolizaSchema = new mongoose.Schema({
    _id: { type: String },  // polizas keep string IDs like "POL1042"
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


/* mirror field if you still want nro_poliza in JSON */
PolizaSchema.virtual('nro_poliza')
    .get(function () { return this._id; })
    .set(function (v) { this._id = (v == null ? v : String(v)); });


PolizaSchema.pre('validate', async function (next) {
    try {
        if (this.isNew && (this._id === undefined || this._id === null)) {
            const numSuffix = await nextSeq('polizas_num_suffix');   // String
            this._id = "POL" + numSuffix;
        }
        next();
    } catch (error) {
        next(error);
    }
});


module.exports = mongoose.model('Poliza', PolizaSchema);
