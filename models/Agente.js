const mongoose = require('mongoose');

const AgenteSchema = new mongoose.Schema({
    _id: { type: Number },
    nombre: String,
    apellido: String,
    matricula: String,
    telefono: String,
    email: String,
    zona: String,
    activo: Boolean
}, {
    collection: 'agentes',
    toJSON: { virtuals: true },
    toObject: { virtuals: true },
    versionKey: false
});

AgenteSchema.virtual('id_agente')
    .get(function () {return this._id;})
    .set(function (v) { this._id = (v == null ? v : Number(v)); });

module.exports = mongoose.model('Agente', AgenteSchema);
