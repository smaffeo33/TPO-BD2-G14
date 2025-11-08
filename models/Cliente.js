const mongoose = require('mongoose');
const nextSeq = require('./nextSeq');

/* ---------- Embedded schemas ---------- */

const VehiculoSchema = new mongoose.Schema({
    _id: {type: Number},            // was String → use Number to match your numeric IDs
    marca: String,
    modelo: String,
    anio: Number,
    patente: String,
    nro_chasis: String,
    asegurado: Boolean
}, {
    collection: 'vehiculos',
    toJSON: { virtuals: true },
    toObject: { virtuals: true },
    versionKey: false
});

const PolizaAutoVigenteSchema = new mongoose.Schema({
    nro_poliza: String,             // polizas keep string IDs like "POL1042"
    tipo: String,
    fecha_inicio: Date,
    fecha_fin: Date,
    cobertura_total: Number,
    prima_mensual: Number
}, { _id: false });

/* ---------- Cliente schema with numeric _id ---------- */

const ClienteSchema = new mongoose.Schema({
    _id: { type: Number },          // was String → Number
    nombre: String,
    apellido: String,
    dni: String,
    email: String,
    telefono: String,
    direccion: String,
    ciudad: String,
    provincia: String,
    activo: Boolean,
    vehiculos: [VehiculoSchema],
    poliza_auto_vigente: PolizaAutoVigenteSchema
}, {
    collection: 'clientes',
    toJSON: { virtuals: true },
    toObject: { virtuals: true },
    versionKey: false
});

/* mirror field if you still want id_cliente in JSON */
ClienteSchema.virtual('id_cliente')
    .get(function () { return this._id; })
    .set(function (v) { this._id = (v == null ? v : Number(v)); });

/* auto-assign numeric _id from counters on create */
ClienteSchema.pre('validate', async function (next) {
    try {
        if (this.isNew && (this._id === undefined || this._id === null)) {
            this._id = await nextSeq('clientes');   // Number
        }
        next();
    } catch (err) {
        next(err);
    }
});

VehiculoSchema.pre('validate', async function (next) {
    try {
        if (this.isNew && (this._id === undefined || this._id === null)) {
            this._id = await nextSeq('vehiculos');   // Number
        } // TODO check if i can still try and specify the id manually. If so, i need to blow up on the else here (bad request maybe)
        next();
    } catch (err) {
        next(err);
    }
})

module.exports = mongoose.model('Cliente', ClienteSchema);
