require('dotenv').config();

const jwt = require('jsonwebtoken');

const {
  getOrCreateTenantPool,
  pools,
} = require('../src/middlewares/tenantResolver');

function getCliArg(prefix) {
  return process.argv
    .find((arg) => arg.startsWith(prefix))
    ?.replace(prefix, '');
}

function wantsHelp() {
  return process.argv.includes('--help') || process.argv.includes('-h');
}

function printHelp() {
  console.log(`Gunakan salah satu mode berikut:

  node scripts/reset-kasbon.js --mode=delete --tenant-id=<TENANT_ID> --db-url=<TENANT_DB_URL>
  node scripts/reset-kasbon.js --mode=soft --tenant-id=<TENANT_ID> --db-url=<TENANT_DB_URL>
  node scripts/reset-kasbon.js --mode=delete --token=<JWT_LOGIN_BRIDGE>

Atau lewat npm script:

  npm run kasbon:reset -- --tenant-id=<TENANT_ID> --db-url=<TENANT_DB_URL>
  npm run kasbon:soft-fix -- --tenant-id=<TENANT_ID> --db-url=<TENANT_DB_URL>

Catatan:
- Script ini memakai logika koneksi tenant yang sama dengan bridge runtime melalui getOrCreateTenantPool().
- Bridge tidak melakukan lookup tenant dari db_tpp; tenantId dan dbUrl harus tersedia dari CLI/env atau token login bridge.
- Jika tenant punya tabel literal "KasBon", mode delete akan menjalankan DELETE FROM "KasBon".
- Jika tabel "KasBon" tidak ada, script otomatis memakai model live sales_records + kas_bon_payment_history.
- Gunakan URL database tenant POS, bukan database auth/admin pusat.`);
}

function getMode() {
  const mode = (getCliArg('--mode=') || 'delete').trim().toLowerCase();
  if (mode === 'delete' || mode === 'soft') {
    return mode;
  }
  throw new Error("Mode tidak valid. Gunakan '--mode=delete' atau '--mode=soft'.");
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

async function hasTable(client, tableName) {
  const result = await client.query(
    `SELECT EXISTS (
       SELECT 1
       FROM information_schema.tables
       WHERE table_schema = 'public'
         AND table_name = $1
     ) AS exists`,
    [tableName],
  );
  return result.rows[0]?.exists === true;
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

function buildKasBonPredicate(columns) {
  const paymentColumn = columns.has('payment_method')
    ? 'payment_method'
    : (columns.has('payment_type') ? 'payment_type' : null);

  if (!paymentColumn) {
    throw new Error('Kolom payment_method/payment_type tidak ditemukan di sales_records.');
  }

  return `UPPER(COALESCE(${paymentColumn}::text, '')) = 'KAS BON'`;
}

async function deleteLiteralKasBonTable(client) {
  const result = await client.query('DELETE FROM "KasBon"');
  console.log(`✅ Berhasil menghapus ${result.rowCount || 0} data dari tabel literal "KasBon".`);
}

async function deleteKasBonSalesRecords(client, columns) {
  const predicate = buildKasBonPredicate(columns);

  let deletedHistory = 0;
  if (await hasTable(client, 'kas_bon_payment_history')) {
    const historyResult = await client.query(`
      DELETE FROM kas_bon_payment_history
      WHERE sales_record_id IN (
        SELECT id FROM sales_records WHERE ${predicate}
      )
    `);
    deletedHistory = historyResult.rowCount || 0;
  }

  const salesResult = await client.query(`
    DELETE FROM sales_records
    WHERE ${predicate}
  `);

  console.log(`✅ Berhasil menghapus ${salesResult.rowCount || 0} data Kas Bon dari sales_records.`);
  console.log(`ℹ️ Riwayat pembayaran terhapus: ${deletedHistory}`);
}

async function softFixKasBonSalesRecords(client, columns) {
  const predicate = buildKasBonPredicate(columns);
  const updates = [];

  if (columns.has('payment_status')) {
    updates.push(`payment_status = 'LUNAS'`);
  }
  if (columns.has('remaining_balance')) {
    updates.push('remaining_balance = 0');
  }
  if (columns.has('outstanding_balance')) {
    updates.push('outstanding_balance = 0');
  }

  if (updates.length === 0) {
    throw new Error('Tidak ada kolom status/saldo Kas Bon yang bisa diperbarui di sales_records.');
  }

  const result = await client.query(`
    UPDATE sales_records
    SET ${updates.join(', ')}
    WHERE ${predicate}
      AND (
        ${columns.has('payment_status') ? "UPPER(COALESCE(payment_status::text, 'BELUM LUNAS')) <> 'LUNAS'" : 'TRUE'}
        OR ${columns.has('remaining_balance') ? 'COALESCE(remaining_balance, 0) > 0' : 'FALSE'}
        OR ${columns.has('outstanding_balance') ? 'COALESCE(outstanding_balance, 0) > 0' : 'FALSE'}
      )
  `);

  console.log(`✅ Berhasil menidurkan ${result.rowCount || 0} data Kas Bon di sales_records.`);
}

async function runReset(client, mode) {
  const hasLiteralKasBon = await hasTable(client, 'KasBon');
  if (hasLiteralKasBon) {
    if (mode !== 'delete') {
      throw new Error('Mode soft tidak didukung untuk tabel literal "KasBon". Gunakan --mode=delete.');
    }

    console.log('Tabel literal "KasBon" ditemukan. Menjalankan reset langsung ke tabel tersebut...');
    await deleteLiteralKasBonTable(client);
    return;
  }

  const salesRecordColumns = await getTableColumns(client, 'sales_records');
  if (!salesRecordColumns.has('id')) {
    throw new Error('Tabel literal "KasBon" tidak ditemukan, dan sales_records tidak tersedia atau tidak valid.');
  }

  console.log('Tabel literal "KasBon" tidak ditemukan. Menggunakan model live sales_records + kas_bon_payment_history...');
  if (mode === 'delete') {
    await deleteKasBonSalesRecords(client, salesRecordColumns);
  } else {
    await softFixKasBonSalesRecords(client, salesRecordColumns);
  }
}

async function closePools() {
  const closers = [];
  for (const entry of pools.values()) {
    if (entry?.pool?.end) {
      closers.push(entry.pool.end().catch(() => {}));
    }
  }
  await Promise.all(closers);
}

async function main() {
  if (wantsHelp()) {
    printHelp();
    return;
  }

  const mode = getMode();
  const { tenantId, dbUrl, source } = resolveTenantContext();
  const pool = getOrCreateTenantPool(tenantId, dbUrl);
  const client = await pool.connect();

  try {
    await client.query('SELECT 1');
    console.log(`Mulai proses Kas Bon mode=${mode} tenantId=${tenantId} source=${source}...`);
    await client.query('BEGIN');
    await runReset(client, mode);
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
    await closePools();
  }
}

main().catch((error) => {
  console.error('Gagal memproses reset Kas Bon:', error.message);
  process.exit(1);
});