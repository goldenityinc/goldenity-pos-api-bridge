require('dotenv').config();

const { PrismaClient } = require('@prisma/client');

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

  node scripts/reset-kasbon.js --mode=delete --db-url=<TENANT_DB_URL>
  node scripts/reset-kasbon.js --mode=soft --db-url=<TENANT_DB_URL>

Atau lewat npm script:

  npm run kasbon:reset -- --db-url=<TENANT_DB_URL>
  npm run kasbon:soft-fix -- --db-url=<TENANT_DB_URL>

Catatan:
- Script ini berjalan pada tabel sales_records dan kas_bon_payment_history.
- Repo bridge ini tidak memiliki model Prisma KasBon terpisah.
- Gunakan URL database tenant POS, bukan database auth/admin pusat.`);
}

function getDatabaseUrl() {
  return (
    getCliArg('--db-url=') ||
    process.env.DATABASE_URL_TPP ||
    process.env.DATABASE_URL
  );
}

function getMode() {
  const mode = (getCliArg('--mode=') || 'delete').trim().toLowerCase();
  if (mode === 'delete' || mode === 'soft') {
    return mode;
  }
  throw new Error("Mode tidak valid. Gunakan '--mode=delete' atau '--mode=soft'.");
}

async function getSalesRecordColumns(prisma) {
  const rows = await prisma.$queryRawUnsafe(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'sales_records'
  `);

  return new Set((rows || []).map((row) => row.column_name));
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

async function deleteKasBon(prisma, columns) {
  const predicate = buildKasBonPredicate(columns);

  const deletedHistory = await prisma.$executeRawUnsafe(`
    DELETE FROM kas_bon_payment_history
    WHERE sales_record_id IN (
      SELECT id FROM sales_records WHERE ${predicate}
    )
  `).catch(() => 0);

  const deletedSales = await prisma.$executeRawUnsafe(`
    DELETE FROM sales_records
    WHERE ${predicate}
  `);

  console.log(`✅ Berhasil menghapus ${deletedSales} data Kas Bon.`);
  console.log(`ℹ️ Riwayat pembayaran terhapus: ${deletedHistory}`);
}

async function softFixKasBon(prisma, columns) {
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

  const updatedSales = await prisma.$executeRawUnsafe(`
    UPDATE sales_records
    SET ${updates.join(', ')}
    WHERE ${predicate}
      AND (
        ${columns.has('payment_status') ? "UPPER(COALESCE(payment_status::text, 'BELUM LUNAS')) <> 'LUNAS'" : 'TRUE'}
        OR ${columns.has('remaining_balance') ? 'COALESCE(remaining_balance, 0) > 0' : 'FALSE'}
        OR ${columns.has('outstanding_balance') ? 'COALESCE(outstanding_balance, 0) > 0' : 'FALSE'}
      )
  `);

  console.log(`✅ Berhasil menidurkan ${updatedSales} data Kas Bon.`);
}

async function main() {
  if (wantsHelp()) {
    printHelp();
    return;
  }

  const databaseUrl = getDatabaseUrl();
  const mode = getMode();

  if (!databaseUrl) {
    throw new Error(
      'Database URL tidak ditemukan. Isi DATABASE_URL_TPP / DATABASE_URL, atau kirim --db-url=<url>.',
    );
  }

  const prisma = new PrismaClient({
    datasources: {
      db: {
        url: databaseUrl,
      },
    },
  });

  try {
    const columns = await getSalesRecordColumns(prisma);
    if (!columns.has('id')) {
      throw new Error('Tabel sales_records tidak tersedia atau tidak valid.');
    }

    console.log(`Mulai proses Kas Bon mode=${mode}...`);
    if (mode === 'delete') {
      await deleteKasBon(prisma, columns);
    } else {
      await softFixKasBon(prisma, columns);
    }
  } finally {
    await prisma.$disconnect();
  }
}

main().catch((error) => {
  console.error('Gagal memproses reset Kas Bon:', error.message);
  process.exit(1);
});