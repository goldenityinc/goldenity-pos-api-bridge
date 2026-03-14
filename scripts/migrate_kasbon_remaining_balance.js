const fs = require('fs');
const path = require('path');
const { Client } = require('pg');

const migrationPath = path.join(__dirname, 'sql', '20260314_add_remaining_balance_sales_records.sql');

const run = async () => {
  const dbUrl = process.argv[2] || process.env.DATABASE_URL;

  if (!dbUrl) {
    throw new Error('DATABASE_URL belum diisi. Gunakan env DATABASE_URL atau argumen pertama command.');
  }

  const sql = fs.readFileSync(migrationPath, 'utf8');
  const client = new Client({ connectionString: dbUrl, ssl: { rejectUnauthorized: false } });

  await client.connect();

  try {
    await client.query('BEGIN');
    await client.query(sql);
    await client.query('COMMIT');
    console.log('Migrasi kas bon selesai: kolom remaining_balance siap digunakan.');
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    await client.end();
  }
};

run().catch((error) => {
  console.error('Migrasi gagal:', error.message);
  process.exit(1);
});
