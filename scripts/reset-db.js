require('dotenv').config();

const { PrismaClient } = require('@prisma/client');

function getDatabaseUrl() {
  const cliArg = process.argv
    .find((arg) => arg.startsWith('--db-url='))
    ?.replace('--db-url=', '');

  return cliArg || process.env.DATABASE_URL_TPP || process.env.DATABASE_URL;
}

function quoteIdentifier(identifier) {
  return `"${String(identifier).replace(/"/g, '""')}"`;
}

async function main() {
  const databaseUrl = getDatabaseUrl();

  if (!databaseUrl) {
    throw new Error(
      'Database URL tidak ditemukan. Isi DATABASE_URL_TPP / DATABASE_URL, atau kirim --db-url=<url>.'
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
    const tables = await prisma.$queryRawUnsafe(`
      SELECT tablename
      FROM pg_tables
      WHERE schemaname = 'public'
        AND tablename <> '_prisma_migrations'
      ORDER BY tablename;
    `);

    if (!Array.isArray(tables) || tables.length === 0) {
      console.log('Tidak ada tabel public yang perlu di-truncate.');
      return;
    }

    const tableList = tables
      .map((t) => quoteIdentifier(t.tablename))
      .join(', ');

    const sql = `TRUNCATE TABLE ${tableList} RESTART IDENTITY CASCADE;`;

    await prisma.$executeRawUnsafe(sql);

    console.log('Reset DB selesai. Tabel yang di-truncate:');
    for (const table of tables) {
      console.log(`- ${table.tablename}`);
    }
  } finally {
    await prisma.$disconnect();
  }
}

main().catch((error) => {
  console.error('Gagal reset database:', error.message);
  process.exit(1);
});
