const express = require('express');
const { getSettings, getTenantSettings } = require('../controllers/settingsController');

const router = express.Router();

/**
 * Public endpoint - returns Web Order URL for QR code generation
 */
router.get('/', getSettings);

/**
 * Protected endpoint - returns tenant-specific settings
 * Requires tenant to be resolved in middleware
 */
router.get('/:tenantId', getTenantSettings);

module.exports = router;
