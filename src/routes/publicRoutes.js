const express = require('express');
const multer = require('multer');
const { healthCheck } = require('../controllers/healthController');
const { login } = require('../controllers/authController');
const imageProxyRoutes = require('./imageProxyRoutes');
const settingsRoutes = require('./settingsRoutes');
const {
	updateTenantQrisImage,
	updateTenantReceiptFooter,
	updateTenantPrinterConfigs,
} = require('../controllers/settingsController');
const {
	getQrMenu,
	createQrOrder,
	checkoutQrOrder,
	handlePaymentWebhook,
} = require('../controllers/publicQrController');
// 🔴 Import adminCoreFetch untuk proxy device register & heartbeat routes.
//    POS Flutter akan fallback ke Bridge ketika Core URL tidak reachable.
const { adminCoreFetch } = require('../services/posOrderQueue');

const router = express.Router();
const paymentProofUpload = multer({
	storage: multer.memoryStorage(),
	limits: {
		fileSize: 8 * 1024 * 1024,
	},
});

const resolveTenantIdForDeviceEndpoint = (req) => {
	const candidates = [
		req.headers['x-tenant-id'],
		req.headers['tenant-id'],
		req.body && req.body.tenantId,
		req.body && req.body.tenant_id,
		req.query && req.query.tenantId,
		req.query && req.query.tenant_id,
		req.tenant && (req.tenant.id || req.tenant.tenantId || req.tenant.tenant_id),
	];
	for (const c of candidates) {
		const s = (c || '').toString().trim();
		if (s) return s;
	}
	return '';
};

const buildAdminCoreDeviceRequestHeaders = (req) => {
	const forward = {
		'X-Bridge-Proxy': '1',
	};
	const forwardHeaders = ['authorization', 'cookie', 'x-tenant-id', 'tenant-id', 'x-branch-id', 'branch-id', 'x-internal-relay', 'x-internal-token'];
	for (const h of forwardHeaders) {
		const v = req.headers[h];
		if (v !== undefined && v !== null) {
			forward[h] = Array.isArray(v) ? v.join(',') : String(v);
		}
	}
	if (req.headers['x-internal-relay']) {
		forward['X-Internal-Relay'] = String(req.headers['x-internal-relay']);
	}
	return forward;
};

// 🔴 [FAST TRACK FIX] POS Flutter Device Registration via Bridge proxy.
//    Target: Admin Core /api/v1/devices/register (flexibleAuth + ensureTenantContext)
//    Route yang sama dipakai Flutter POS via fallback BridgeBaseURL.
const proxyAdminCoreDeviceRequest = async (req, res, upstreamPath) => {
	try {
		const tenantId = resolveTenantIdForDeviceEndpoint(req);
		const bodyRaw = req.body || {};
		// 🔴 Pastikan body selalu ada tenantId (jika tidak ada, inject dari header/query).
		const body = { ...bodyRaw };
		if (tenantId) {
			body.tenantId = body.tenantId || body.tenant_id || tenantId;
			body.tenant_id = body.tenant_id || body.tenantId;
		}
		const headers = buildAdminCoreDeviceRequestHeaders(req);
		const sep = upstreamPath.includes('?') ? '&' : '?';
		const qsTenant = tenantId ? `${sep}tenantId=${encodeURIComponent(tenantId)}` : '';
		const upstream = `${upstreamPath}${qsTenant}`;
		const result = await adminCoreFetch(upstream, {
			method: req.method.toUpperCase(),
			headers,
			body: JSON.stringify(body),
		}).catch((err) => ({
			ok: false,
			status: 502,
			error: err && err.message ? err.message : 'Bridge upstream error',
		}));
		if (result.ok) {
			return res.status(result.status || 200).json(result.data || { success: true, message: 'OK' });
		}
		// Upstream gagal: forward response mentah jika bisa.
		const errMsg = (result && result.error) ? result.error : (result && result.data ? result.data.message : 'Upstream Admin Core gagal merespon');
		const errCode = (result && result.status) || 502;
		return res.status(errCode).json({
			success: false,
			message: typeof errMsg === 'string' ? errMsg : 'Device endpoint proxy gagal',
			_upstreamPath: upstream,
			_upstreamHasTenant: Boolean(tenantId),
			error: typeof errMsg === 'object' ? errMsg : undefined,
		});
	} catch (err) {
		console.error('[Bridge] device proxy error:', err && err.stack ? err.stack : err);
		return res.status(500).json({
			success: false,
			message: 'Internal error saat memproses permintaan device ke Admin Core',
			error: process.env.NODE_ENV !== 'production' ? (err && err.message ? err.message : String(err)) : undefined,
		});
	}
};

// 🔴 3 Device Routes untuk POS Flutter (3 format berbeda dari Flutter Fallback).
// 1. Format public: /api/v1/devices/register
router.post('/api/v1/devices/register', (req, res) =>
	proxyAdminCoreDeviceRequest(req, res, '/api/v1/devices/register'),
);
// 2. Format internal (X-Internal-Relay): /api/v1/internal/devices/register
router.post('/api/v1/internal/devices/register', (req, res) =>
	proxyAdminCoreDeviceRequest(req, res, '/api/v1/devices/register'),
);
// 3a. Heartbeat body style (modern) POST /api/v1/devices/heartbeat
router.post('/api/v1/devices/heartbeat', (req, res) =>
	proxyAdminCoreDeviceRequest(req, res, '/api/v1/devices/heartbeat'),
);
// 3b. Heartbeat path param style (legacy) POST /api/v1/devices/:uuid/heartbeat
router.post('/api/v1/devices/:uuid/heartbeat', (req, res) =>
	proxyAdminCoreDeviceRequest(
		req,
		res,
		`/api/v1/devices/${encodeURIComponent(req.params.uuid)}/heartbeat`,
	),
);

router.get('/health', healthCheck);
router.post('/auth/login', login);

// Settings endpoint - used by Flutter app for Web Order URL configuration
router.use('/api/v1/settings', settingsRoutes);
router.post('/api/v1/settings/qris-image', paymentProofUpload.single('qrisImage'), updateTenantQrisImage);
router.post('/api/v1/settings/receipt-footer', updateTenantReceiptFooter);
router.post('/api/v1/settings/printer-configs', updateTenantPrinterConfigs);

router.get('/api/v1/qr-menu/:tenantId', getQrMenu);
router.post('/api/v1/qr-orders', paymentProofUpload.single('payment_proof'), createQrOrder);
router.post('/api/v1/public/qr/order', paymentProofUpload.single('payment_proof'), createQrOrder);
router.post('/api/v1/qr-orders/checkout', checkoutQrOrder);
router.post('/api/v1/webhooks/payment', handlePaymentWebhook);

// Image proxy endpoint (public, no auth required)
// Pattern: GET /images/:encodedKey
router.use('/images', imageProxyRoutes);

module.exports = router;
