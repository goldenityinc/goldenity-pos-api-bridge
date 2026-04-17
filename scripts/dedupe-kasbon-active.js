require('dotenv').config();

const jwt = require('jsonwebtoken');
const {
  getSharedPool,
} = require('../src/middlewares/tenantResolver');

function getCliArg(prefix) {
  return process.argv.find((arg) => arg.startsWith(prefix))?.replace(prefix, '');
}

function hasFlag(flag) {
  return process.argv.includes(flag);
}

function wantsHelp() {
  return hasFlag('--help') || hasFlag('-h');
}

function printHelp() {
  console.log(`Gunakan mode audit (default):

  node scripts/dedupe-kasbon-active.js --tenant-id=<TENANT_ID> --db-url=<TENANT_DB_URL>

Untuk apply perubahan (menutup duplikat aktif non-canonical):

  node scripts/dedupe-kasbon-active.js --tenant-id=<TENANT_ID> --db-url=<TENANT_DB_URL> --apply

Atau pakai token login bridge:

  node scripts/dedupe-kasbon-active.js --token=<JWT_LOGIN_BRIDGE>
  node scripts/dedupe-kasbon-active.js --token=<JWT_LOGIN_BRIDGE> --apply

Catatan:
- Script ini enforce aturan satu Kas Bon aktif canonical per pelanggan.
- Canonical dipilih dari row paling baru (updated_at/created_at/id terbesar).
- Row aktif duplikat lainnya akan ditandai lunas + saldo 0 saat --apply.
- Jalankan audit dulu sebelum apply.`);
}

function resolveTenantContext() {
  const token = (getCliArg('--token=') || process.env.BRIDGE_JWT || '').trim();
  if (token) {
    const jwtSecret = process.env.JWT_SECRET;
    if (!jwtSecret) {
      throw new Error('JWT_SECRET wajib diisi untuk memakai --token.');
    }

    const payload = jwt.verify(token, jwtSecret);
    const tenantId = (payload?.tenantId || payload?.tenant_id || '').toString().trim();
    const dbUrl = (payload?.dbUrl || '').toString().trim();
    if (!tenantId || !dbUrl) {
      throw new Error('Token bridge tidak mengandung tenantId/dbUrl yang valid.');
    }
    return { tenantId, dbUrl, source: 'token' };
  }

  const tenantId = (
    getCliArg('--tenant-id=') ||
    process.env.TENANT_ID ||
    process.env.TENANT_ID_TPP ||
    ''
  ).trim();
  const dbUrl = (
    getCliArg('--db-url=') ||
    process.env.DATABASE_URL_TPP ||
    process.env.DATABASE_URL ||
    ''
  ).trim();

  if (!tenantId) {
    throw new Error('tenantId tidak ditemukan. Kirim --tenant-id=<TENANT_ID> atau gunakan --token=<JWT_LOGIN_BRIDGE>.');
  }
  if (!dbUrl) {
    throw new Error('Database URL tidak ditemukan. Kirim --db-url=<TENANT_DB_URL> atau gunakan --token=<JWT_LOGIN_BRIDGE>.');
  }

  return { tenantId, dbUrl, source: 'cli/env' };
}

async function getTableColumns(client, tableName) {
  const result = await client.query(
    `SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name = $1`,
    [tableName],
  );
  return new Set((result.rows || []).map((row) => row.column_name));
}

function buildOrderBy(columns) {
  const parts = [];
  if (columns.has('updated_at')) {
    parts.push('updated_at DESC');
  }
  if (columns.has('created_at')) {
    parts.push('created_at DESC');
  }
  parts.push('id DESC');
  return parts.join(', ');
}

function toNum(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function customerKey(row, columns) {
  const customerId = columns.has('customer_id')
    ? (row.customer_id || '').toString().trim()
    : '';
  const customerName = (row.customer_name || '').toString().trim().toLowerCase();
  if (customerId) {
    return `customer_id:${customerId}`;
  }
  return `customer_name:${customerName}`;
}

async function loadActiveKasbonRows(client, tenantId, columns) {
  const paymentColumn = columns.has('payment_method')
    ? 'payment_method'
    : (columns.has('payment_type') ? 'payment_type' : null);
  const amountColumn = columns.has('total_price')
    ? 'total_price'
    : (columns.has('total_amount') ? 'total_amount' : null);
  const remainingExpr = columns.has('remaining_balance')
    ? 'COALESCE(remaining_balance, outstanding_balance, total_price, total_amount, 0)'
    : 'COALESCE(outstanding_balance, total_price, total_amount, 0)';

  if (!paymentColumn || !amountColumn || !columns.has('customer_name')) {
    throw new Error('sales_records tidak punya kolom minimum untuk dedupe kasbon.');
  }

  const paymentExpr = `REPLACE(REPLACE(REPLACE(UPPER(COALESCE(${paymentColumn}::text, '')), ' ', ''), '-', ''), '_', '')`;
  const orderBy = buildOrderBy(columns);

  const result = await client.query(
    `SELECT *
     FROM sales_records
     WHERE tenant_id = $1
       AND COALESCE(TRIM(customer_name), '') <> ''
       AND ${paymentExpr} = 'KASBON'
       AND ${remainingExpr} > 0
       AND UPPER(COALESCE(payment_status::text, 'BELUM LUNAS')) <> 'LUNAS'
     ORDER BY ${orderBy}`,
    [tenantId],
  );

  return (result.rows || []).map((row) => ({
    ...row,
    _remaining_balance_num: toNum(
      row.remaining_balance ?? row.outstanding_balance ?? row.total_price ?? row.total_amount,
    ),
  }));
}

function splitCanonicalAndDuplicates(rows, columns) {
  const seen = new Set();
  const canonical = [];
  const duplicates = [];

  for (const row of rows) {
    const key = customerKey(row, columns);
    if (seen.has(key)) {
      duplicates.push(row);
      continue;
    }
    seen.add(key);
    canonical.push(row);
  }

  return { canonical, duplicates };
}

async function applyDuplicateClose(client, duplicates) {
  let closedCount = 0;

  for (const row of duplicates) {
    const id = Number(row.id);
    if (!Number.isFinite(id)) {
      continue;
    }

    const remaining = toNum(row._remaining_balance_num);
    await client.query(
      `UPDATE sales_records
       SET payment_status = 'LUNAS',
           payment_method = COALESCE(NULLIF(payment_method, ''), 'Kas Bon'),
           remaining_balance = 0,
           outstanding_balance = 0,
           amount_paid = COALESCE(amount_paid, 0) + $2::numeric,
           updated_at = NOW()
       WHERE id = $1`,
      [id, remaining],
    );

    closedCount += 1;
  }

  return closedCount;
}

async function closePools() {
  const sharedPool = global.__goldenitySharedPool;
  if (sharedPool?.end) {
    await sharedPool.end().catch(() => {});
  }
}

async function main() {
  if (wantsHelp()) {
    printHelp();
    return;
  }

  const applyChanges = hasFlag('--apply');
  const { tenantId, dbUrl, source } = resolveTenantContext();
  const pool = getSharedPool();
  const client = await pool.connect();

  try {
    const columns = await getTableColumns(client, 'sales_records');
    const activeRows = await loadActiveKasbonRows(client, tenantId, columns);
    const { canonical, duplicates } = splitCanonicalAndDuplicates(activeRows, columns);

    console.log(`Tenant: ${tenantId} (source=${source})`);
    console.log(`Kas Bon aktif terdeteksi: ${activeRows.length}`);
    console.log(`Canonical customer rows: ${canonical.length}`);
    console.log(`Duplikat aktif kandidat ghost: ${duplicates.length}`);

    if (duplicates.length > 0) {
      console.log('Sample duplikat (maks 10):');
      for (const row of duplicates.slice(0, 10)) {
        console.log(`- id=${row.id} receipt=${row.receipt_number || '-'} customer=${row.customer_name || '-'} remaining=${row._remaining_balance_num}`);
      }
    }

    if (!applyChanges) {
      console.log('Mode audit selesai. Jalankan ulang dengan --apply untuk menutup duplikat aktif.');
      return;
    }

    if (duplicates.length === 0) {
      console.log('Tidak ada duplikat aktif untuk ditutup.');
      return;
    }

    await client.query('BEGIN');
    const closedCount = await applyDuplicateClose(client, duplicates);
    await client.query('COMMIT');

    console.log(`Apply selesai. Duplikat aktif yang ditutup: ${closedCount}`);
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
    await closePools();
  }
}

main().catch((error) => {
  console.error('Gagal dedupe kasbon aktif:', error.message);
  process.exit(1);
});
