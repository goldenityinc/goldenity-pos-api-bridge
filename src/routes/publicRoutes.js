const express = require('express');
const multer = require('multer');
const { healthCheck } = require('../controllers/healthController');
const { login } = require('../controllers/authController');
const imageProxyRoutes = require('./imageProxyRoutes');
const settingsRoutes = require('./settingsRoutes');
const {
	getQrMenu,
	createQrOrder,
	checkoutQrOrder,
	handlePaymentWebhook,
} = require('../controllers/publicQrController');

const router = express.Router();
const paymentProofUpload = multer({
	storage: multer.memoryStorage(),
	limits: {
		fileSize: 8 * 1024 * 1024,
	},
});

router.get('/health', healthCheck);
router.post('/auth/login', login);

// Settings endpoint - used by Flutter app for Web Order URL configuration
router.use('/api/v1/settings', settingsRoutes);

router.get('/api/v1/qr-menu/:tenantId', getQrMenu);
router.post('/api/v1/qr-orders', paymentProofUpload.single('payment_proof'), createQrOrder);
router.post('/api/v1/public/qr/order', paymentProofUpload.single('payment_proof'), createQrOrder);
router.post('/api/v1/qr-orders/checkout', checkoutQrOrder);
router.post('/api/v1/webhooks/payment', handlePaymentWebhook);

// Image proxy endpoint (public, no auth required)
// Pattern: GET /images/:encodedKey
router.use('/images', imageProxyRoutes);

module.exports = router;
