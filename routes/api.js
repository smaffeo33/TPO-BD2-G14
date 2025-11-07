const express = require('express');
const router = express.Router();

// Import services
const queryService = require('../services/queryService');
const clienteService = require('../services/clienteService');
const siniestroService = require('../services/siniestroService');
const polizaService = require('../services/polizaService');

// ============================================================
// READ QUERIES (Q1-Q12)
// ============================================================

/**
 * Q1: Clientes activos con sus pólizas vigentes
 * GET /api/queries/q1
 */
router.get('/queries/q1', async (req, res) => {
    try {
        const result = await queryService.getClientesActivosConPolizasVigentes();
        res.json({ success: true, data: result });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * Q2: Siniestros abiertos con tipo, monto y cliente afectado
 * GET /api/queries/q2
 */
router.get('/queries/q2', async (req, res) => {
    try {
        const result = await queryService.getSiniestrosAbiertos();
        res.json({ success: true, data: result });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * Q3: Vehículos asegurados con su cliente y póliza
 * GET /api/queries/q3
 */
router.get('/queries/q3', async (req, res) => {
    try {
        const result = await queryService.getVehiculosAsegurados();
        res.json({ success: true, data: result });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * Q4: Clientes sin pólizas activas
 * GET /api/queries/q4
 */
router.get('/queries/q4', async (req, res) => {
    try {
        const result = await queryService.getClientesSinPolizasActivas();
        res.json({ success: true, data: result });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * Q5: Agentes activos con cantidad de pólizas asignadas
 * GET /api/queries/q5
 */
router.get('/queries/q5', async (req, res) => {
    try {
        const result = await queryService.getAgentesConCantidadPolizas();
        res.json({ success: true, data: result });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * Q6: Pólizas vencidas con el nombre del cliente
 * GET /api/queries/q6
 */
router.get('/queries/q6', async (req, res) => {
    try {
        const result = await queryService.getPolizasVencidas();
        res.json({ success: true, data: result });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * Q7: Top 10 clientes por cobertura total
 * GET /api/queries/q7
 */
router.get('/queries/q7', async (req, res) => {
    try {
        const result = await queryService.getTop10ClientesPorCobertura();
        res.json({ success: true, data: result });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * Q8: Siniestros tipo "Accidente" del último año
 * GET /api/queries/q8
 */
router.get('/queries/q8', async (req, res) => {
    try {
        const result = await queryService.getSiniestrosAccidenteUltimoAnio();
        res.json({ success: true, data: result });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * Q9: Vista de pólizas activas ordenadas por fecha de inicio
 * GET /api/queries/q9
 */
router.get('/queries/q9', async (req, res) => {
    try {
        const result = await queryService.getPolizasActivasOrdenadas();
        res.json({ success: true, data: result });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * Q10: Pólizas suspendidas con estado del cliente
 * GET /api/queries/q10
 */
router.get('/queries/q10', async (req, res) => {
    try {
        const result = await queryService.getPolizasSuspendidasConCliente();
        res.json({ success: true, data: result });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * Q11: Clientes con más de un vehículo asegurado
 * GET /api/queries/q11
 */
router.get('/queries/q11', async (req, res) => {
    try {
        const result = await queryService.getClientesConVariosVehiculos();
        res.json({ success: true, data: result });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * Q12: Agentes y cantidad de siniestros asociados
 * GET /api/queries/q12
 */
router.get('/queries/q12', async (req, res) => {
    try {
        const result = await queryService.getAgentesConCantidadSiniestros();
        res.json({ success: true, data: result });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ============================================================
// Q13: ABM DE CLIENTES
// ============================================================

/**
 * Create new cliente
 * POST /queries/q13
 */
router.post('/queries/q13', async (req, res) => {
    try {
        const cliente = await clienteService.createCliente(req.body);
        res.status(201).json({ success: true, data: cliente });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});


/**
 * Update cliente
 * PUT /api/clientes/:id
 */
router.put('/queries/q13/:id', async (req, res) => {
    try {
        const cliente = await clienteService.updateCliente(req.params.id, req.body);
        res.json({ success: true, data: cliente });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * Delete cliente
 * DELETE /api/clientes/:id
 */

router.delete('/queries/q13/:id', async (req, res) => {
    try {
        const result = await clienteService.deleteCliente(req.params.id);
        res.json({ success: true, data: result });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});




// ============================================================
// Q14: ALTA DE SINIESTROS
// ============================================================

/**
 * Create new siniestro
 * POST /api/siniestros
 */
router.post('/queries/q14', async (req, res) => {
    try {
        const siniestro = await siniestroService.createSiniestro(req.body);
        res.status(201).json({ success: true, data: siniestro });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});






// ============================================================
// Q15: EMISIÓN DE PÓLIZAS
// ============================================================

/**
 * Create new poliza (with validation)
 * POST /api/polizas
 */
router.post('/queries/q15', async (req, res) => {
    try {
        const poliza = await polizaService.createPoliza(req.body);
        res.status(201).json({ success: true, data: poliza });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});



module.exports = router;
