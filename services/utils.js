

function updateClientMapper(update) {
    const res = {};
    if (update.nombre) res.nombre = update.nombre;
    if (update.apellido) res.apellido = update.apellido;
    if (update.email) res.email = update.email;
    if (update.telefono) res.telefono = update.telefono;
    if (update.direccion) res.direccion = update.direccion;
    if (update.ciudad) res.ciudad = update.ciudad;
    if (update.provincia) res.provincia = update.provincia;
    return res;
}

module.exports = {updateClientMapper}