const express = require('express');
const { healthCheck } = require('../controllers/healthController');
const { login } = require('../controllers/authController');
const imageProxyRoutes = require('./imageProxyRoutes');
const {
	getQrMenu,
	createQrOrder,
	checkoutQrOrder,
	handlePaymentWebhook,
} = require('../controllers/publicQrController');

const router = express.Router();

router.get('/health', healthCheck);
router.post('/auth/login', login);
router.get('/api/v1/qr-menu/:tenantId', getQrMenu);
router.post('/api/v1/qr-orders', createQrOrder);
router.post('/api/v1/qr-orders/checkout', checkoutQrOrder);
router.post('/api/v1/webhooks/payment', handlePaymentWebhook);

// Image proxy endpoint (public, no auth required)
// Pattern: GET /images/:encodedKey
router.use('/images', imageProxyRoutes);

module.exports = router;
