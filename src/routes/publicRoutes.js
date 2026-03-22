const express = require('express');
const { healthCheck } = require('../controllers/healthController');
const { login } = require('../controllers/authController');
const imageProxyRoutes = require('./imageProxyRoutes');

const router = express.Router();

router.get('/health', healthCheck);
router.post('/auth/login', login);

// Image proxy endpoint (public, no auth required)
// Pattern: GET /images/:encodedKey
router.use('/images', imageProxyRoutes);

module.exports = router;
