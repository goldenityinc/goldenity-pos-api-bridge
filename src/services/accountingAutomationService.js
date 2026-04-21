const http = require('http');
const https = require('https');

const SALES_FINAL_STATUSES = new Set([
  'paid',
  'lunas',
  'settled',
  'completed',
  'success',
]);

const BLOCKED_STATUSES = new Set([
  'void',
  'voided',
  'cancelled',
  'canceled',
  'refunded',
  'reversed',
]);

const AUTO_POST_TIMEOUT_MS = Number(process.env.ADMIN_CORE_API_TIMEOUT_MS || 5000);

const normalizeText = (value) => (value ?? '').toString().trim().toLowerCase();

const toNumber = (value) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
};

const getAdminCoreBaseUrl = () => {
  const rawBaseUrl = (
    process.env.ADMIN_CORE_API_BASE_URL ||
    process.env.ADMIN_CORE_API_URL ||
    ''
  )
    .toString()
    .trim();

  return rawBaseUrl.replace(/\/+$/, '');
};

const getInternalToken = () => (
  process.env.ADMIN_CORE_INTERNAL_TOKEN ||
  process.env.INTERNAL_SERVICE_TOKEN ||
  ''
)
  .toString()
  .trim();

const isConfigured = () => Boolean(getAdminCoreBaseUrl() && getInternalToken());

const postJson = (path, payload) => new Promise((resolve, reject) => {
  const baseUrl = getAdminCoreBaseUrl();
  const internalToken = getInternalToken();

  if (!baseUrl || !internalToken) {
    resolve({ skipped: true, reason: 'missing-config' });
    return;
  }

  const url = new URL(path, `${baseUrl}/`);
  const transport = url.protocol === 'https:' ? https : http;
  const body = JSON.stringify(payload);

  const request = transport.request(
    url,
    {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(body),
        'x-internal-token': internalToken,
      },
      timeout: AUTO_POST_TIMEOUT_MS,
    },
    (response) => {
      let raw = '';
      response.setEncoding('utf8');
      response.on('data', (chunk) => {
        raw += chunk;
      });
      response.on('end', () => {
        const successful = response.statusCode && response.statusCode >= 200 && response.statusCode < 300;
        if (successful) {
          resolve({ skipped: false });
          return;
        }

        reject(
          new Error(
            `Admin core accounting request gagal (${response.statusCode || 0}): ${raw || 'no response body'}`,
          ),
        );
      });
    },
  );

  request.on('timeout', () => {
    request.destroy(new Error('Admin core accounting request timeout'));
  });
  request.on('error', reject);
  request.write(body);
  request.end();
});

const schedule = (label, task) => {
  if (!isConfigured()) {
    console.warn(`[AccountingAutomation] skip ${label}: ADMIN_CORE_API_BASE_URL or ADMIN_CORE_INTERNAL_TOKEN belum dikonfigurasi.`);
    return;
  }

  setImmediate(() => {
    task().catch((error) => {
      console.error(`[AccountingAutomation] gagal ${label}:`, error.message || error);
    });
  });
};

const shouldPostSalesJournal = (record = {}) => {
  const paymentStatus = normalizeText(record.payment_status || record.paymentStatus);
  if (!paymentStatus || BLOCKED_STATUSES.has(paymentStatus)) {
    return false;
  }

  if (!SALES_FINAL_STATUSES.has(paymentStatus)) {
    return false;
  }

  const remainingBalance = toNumber(record.remaining_balance ?? record.remainingBalance);
  const outstandingBalance = toNumber(record.outstanding_balance ?? record.outstandingBalance);
  return remainingBalance <= 0 && outstandingBalance <= 0;
};

const shouldPostExpenseJournal = (record = {}) => {
  const status = normalizeText(record.status);
  if (status && BLOCKED_STATUSES.has(status)) {
    return false;
  }

  return true;
};

const scheduleSalesJournalPosting = ({ salesRecord, tenantId }) => {
  const salesTransactionId = (salesRecord?.id ?? '').toString().trim();
  const resolvedTenantId = (tenantId ?? salesRecord?.tenant_id ?? '').toString().trim();

  if (!salesTransactionId || !resolvedTenantId || !shouldPostSalesJournal(salesRecord)) {
    return;
  }

  schedule(`sales ${salesTransactionId}`, () => postJson('/api/internal/accounting/sales', {
    salesTransactionId,
    tenantId: resolvedTenantId,
  }));
};

const scheduleExpenseJournalPosting = ({ expenseRecord, tenantId }) => {
  const expenseTransactionId = (expenseRecord?.id ?? '').toString().trim();
  const resolvedTenantId = (tenantId ?? expenseRecord?.tenant_id ?? '').toString().trim();

  if (!expenseTransactionId || !resolvedTenantId || !shouldPostExpenseJournal(expenseRecord)) {
    return;
  }

  schedule(`expense ${expenseTransactionId}`, () => postJson('/api/internal/accounting/expenses', {
    expenseTransactionId,
    tenantId: resolvedTenantId,
  }));
};

module.exports = {
  scheduleSalesJournalPosting,
  scheduleExpenseJournalPosting,
};