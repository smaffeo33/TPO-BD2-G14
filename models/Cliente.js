const mongoose = require('mongoose');

const VehiculoSchema = new mongoose.Schema({
    id_vehiculo: String,
    marca: String,
    modelo: String,
    anio: Number,
    patente: String,
    nro_chasis: String,
    asegurado: Boolean
}, { _id: false });

const PolizaAutoVigenteSchema = new mongoose.Schema({
    nro_poliza: String,
    tipo: String,
    fecha_inicio: Date,
    fecha_fin: Date,
    cobertura_total: Number,
    prima_mensual: Number
}, { _id: false });

const ClienteSchema = new mongoose.Schema({
    id_cliente: { type: String, required: true, unique: true, index: true },
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
}, { collection: 'clientes' });

module.exports = mongoose.model('Cliente', ClienteSchema);
