const http = require('http');
const express = require('express');
const cors = require('cors');
const path = require('path');
require('dotenv').config();

// ================================================================
// 🔴🔴 NODE.JS FATAL SAFETY NET GLOBAL — JANGAN DIHAPUS SEKALI PUN.
// ================================================================
// Railway/Render deployment default behavior: unhandledRejection
// KILLS THE ENTIRE PROCESS with exit code 1. Even a single watchdog
// timeout rejection (POS_ACK_TIMEOUT / device offline) used to crash
// posOrderQueue.js line 669 via processTicksAndRejections stack.
// These 2 handlers CATCH & LOG EVERYTHING and NEVER throw —
// Node process stays alive FOREVER even if any module forgets a .catch().
// ================================================================
(function installGlobalProcessSafetyNet() {
  try {
    process.on('unhandledRejection', (reason, promise) => {
      try {
        const ts = new Date().toISOString();
        const msg = reason?.message || reason?.code || String(reason || '');
        const stackSnippet = (reason?.stack || '').split('\n').slice(0, 5).join(' | ');
        // eslint-disable-next-line no-console
        console.error(
          `\n[${ts}] 🔥 [SAFETY NET unhandledRejection] (PROCESS KEPALA 2x KALI TIDAK CRASH!) ` +
          `reason=${msg} stack=${stackSnippet} promise=Promise type=${promise?.constructor?.name || 'Promise'}\n`,
        );
      } catch (_) {
        try { console.error('[SAFETY NET unhandledRejection DOUBLE CATCH]', reason); } catch (__) { /* triple guard */ }
      }
    });
    process.on('uncaughtException', (err) => {
      try {
        const ts = new Date().toISOString();
        const msg = err?.message || err?.code || String(err || '');
        const stackSnippet = (err?.stack || '').split('\n').slice(0, 6).join(' | ');
        // eslint-disable-next-line no-console
        console.error(
          `\n[${ts}] 🔥 [SAFETY NET uncaughtException] (PROCESS TETAP HIDUP!) ` +
          `reason=${msg} stack=${stackSnippet}\n`,
        );
      } catch (_) {
        try { console.error('[SAFETY NET uncaughtException DOUBLE CATCH]', err); } catch (__) {}
      }
    });
    process.on('warning', (warn) => {
      try {
        // eslint-disable-next-line no-console
        console.warn(
          `[${new Date().toISOString()}] ⚠️ Node WARNING: ${warn?.name || '-'} ${warn?.message || ''} stack_first=${(warn?.stack || '').split('\n')[1] || ''}`,
        );
      } catch (_) {}
    });
  } catch (safetyNetInitErr) {
    try { console.error('SAFETY NET INSTALL FAILED:', safetyNetInitErr); } catch (_) {}
  }
})();

const publicRoutes = require('./src/routes/publicRoutes');
const posRelayRoutes = require('./src/routes/posRelayRoutes');
const protectedRoutes = require('./src/routes/protectedRoutes');
const { tenantResolver } = require('./src/middlewares/tenantResolver');
const { initializeSocketServer } = require('./src/services/socketServer');

const app = express();
const PORT = Number(process.env.PORT) || 3000;
const server = http.createServer(app);

const STAGING_ORIGIN = 'https://pos-web-ordering-sttaging.up.railway.app';
const PROD_ORIGIN_1 = 'https://pos-web-ordering-production.up.railway.app';
const PROD_ORIGIN_2 = 'https://pos-web-ordering.up.railway.app';

const allowedOrigins = new Set([
  STAGING_ORIGIN,
  PROD_ORIGIN_1,
  PROD_ORIGIN_2,
  'http://localhost:3000',
  'http://127.0.0.1:3000',
  'http://localhost:3001',
  'http://127.0.0.1:3001',
  'http://localhost:5173',
  'http://127.0.0.1:5173',
  'http://localhost:5174',
  'http://127.0.0.1:5174',
  'http://localhost:8080',
  'http://127.0.0.1:8080',
  'capacitor://localhost',
  'http://localhost',
  'file://',
]);

const isAllowedOrigin = (origin) => {
  if (!origin) return true;
  if (allowedOrigins.has(origin)) return true;
  const lower = String(origin).toLowerCase();
  if (lower.endsWith('.up.railway.app')) return true;
  if (lower.startsWith('http://localhost:')) return true;
  if (lower.startsWith('http://127.0.0.1:')) return true;
  if (lower.startsWith('capacitor://')) return true;
  return false;
};

const corsOptions = {
  origin: (origin, callback) => {
    if (isAllowedOrigin(origin)) {
      return callback(null, true);
    }
    return callback(null, origin || true);
  },
  credentials: true,
  methods: ['GET', 'HEAD', 'PUT', 'PATCH', 'POST', 'DELETE', 'OPTIONS'],
  allowedHeaders: [
    'Origin',
    'X-Requested-With',
    'Content-Type',
    'Accept',
    'Authorization',
    'X-Tenant-ID',
    'X-Branch-ID',
    'X-Device-ID',
    'X-Device-Id',
    'x-tenant-id',
    'x-branch-id',
    'x-device-id',
    'Cache-Control',
    'Accept-Encoding',
    'Accept-Language',
    'Access-Control-Allow-Headers',
    'Access-Control-Request-Method',
    'Access-Control-Request-Headers',
  ],
  exposedHeaders: [
    'Content-Disposition',
    'X-Tenant-ID',
    'X-Branch-ID',
    'X-Device-ID',
    'X-Request-Id',
    'X-RateLimit-Limit',
    'X-RateLimit-Remaining',
    'X-RateLimit-Reset',
  ],
  preflightContinue: false,
  optionsSuccessStatus: 204,
  maxAge: 86400,
};

app.use(cors(corsOptions));
app.options(/(.*)/, cors(corsOptions));
app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (origin && isAllowedOrigin(origin)) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Vary', 'Origin');
  } else {
    res.setHeader('Access-Control-Allow-Origin', '*');
  }
  res.setHeader('Access-Control-Allow-Credentials', 'true');
  res.setHeader(
    'Access-Control-Allow-Methods',
    'GET,HEAD,PUT,PATCH,POST,DELETE,OPTIONS'
  );
  res.setHeader(
    'Access-Control-Allow-Headers',
    'Origin,X-Requested-With,Content-Type,Accept,Authorization,X-Tenant-ID,X-Branch-ID,X-Device-ID,X-Device-Id,x-tenant-id,x-branch-id,x-device-id,Cache-Control,Accept-Encoding,Accept-Language'
  );
  res.setHeader(
    'Access-Control-Expose-Headers',
    'Content-Disposition,X-Tenant-ID,X-Branch-ID,X-Device-ID,X-Request-Id'
  );
  if (req.method === 'OPTIONS') {
    res.statusCode = 204;
    return res.end();
  }
  next();
});
app.use(express.json({ limit: '10mb' }));
app.use('/uploads', express.static(path.join(process.cwd(), 'public', 'uploads')));

app.use(publicRoutes);
app.use('/api/v1/relay', posRelayRoutes);
app.use(tenantResolver);
app.use(protectedRoutes);

initializeSocketServer(server);

server.listen(PORT, () => {
  console.log(`Goldenity Dynamic Bridge API running on port ${PORT}`);
});
// trigger railway redeploy
