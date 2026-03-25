require('dotenv').config();

const { PrismaClient } = require('@prisma/client');

function getDatabaseUrl() {
  return (
    process.argv.find((arg) => arg.startsWith('--db-url='))?.replace('--db-url=', '') ||
    process.env.DATABASE_URL_TPP ||
    process.env.DATABASE_URL
  );
}

async function main() {
  const databaseUrl = getDatabaseUrl();

  if (!databaseUrl) {
    throw new Error('Database URL tidak ditemukan. Kirim --db-url=<TENANT_DB_URL>.');
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
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
      ORDER BY table_name
    `);

    const columns = await prisma.$queryRawUnsafe(`
      SELECT table_name, column_name
      FROM information_schema.columns
      WHERE table_schema = 'public'
      ORDER BY table_name, ordinal_position
    `);

    const foreignKeys = await prisma.$queryRawUnsafe(`
      SELECT
        tc.table_name,
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
       AND tc.table_schema = kcu.table_schema
      JOIN information_schema.constraint_column_usage ccu
        ON ccu.constraint_name = tc.constraint_name
       AND ccu.table_schema = tc.table_schema
      WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = 'public'
      ORDER BY tc.table_name, kcu.column_name
    `);

    console.log(JSON.stringify({ tables, columns, foreignKeys }, null, 2));
  } finally {
    await prisma.$disconnect();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});