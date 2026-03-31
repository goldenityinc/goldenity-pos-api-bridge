/**
 * migrate-prod-schema.js
 *
 * Applies missing production schema patches that cannot wait for Railway
 * redeploy or require structural column additions:
 *
 *   1. custom_roles."updatedAt" SET DEFAULT NOW()
 *      (removes NOT NULL violation on role INSERT)
 *
 *   2. categories ADD COLUMN category_type TEXT DEFAULT 'PRODUCT'
 *      (enables expense vs product categorization in POS)
 *      -- also backfills 'Gaji', 'Pengeluaran'-style names as EXPENSE
 *
 * Safe to run multiple times (all DDL is idempotent or guarded).
 */

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const { Pool } = require('pg');
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

async function run() {
  const check = (label, promise) =>
    promise
      .then(() => console.log(`[OK] ${label}`))
      .catch((err) => console.log(`[SKIP] ${label}: ${err.message}`));

  // ── 1. custom_roles timestamp defaults ───────────────────────────────────
  await check(
    'custom_roles."createdAt" SET DEFAULT NOW()',
    pool.query('ALTER TABLE custom_roles ALTER COLUMN "createdAt" SET DEFAULT NOW()'),
  );
  await check(
    'custom_roles."updatedAt" SET DEFAULT NOW()',
    pool.query('ALTER TABLE custom_roles ALTER COLUMN "updatedAt" SET DEFAULT NOW()'),
  );
  await check(
    'custom_roles.created_at SET DEFAULT NOW()',
    pool.query('ALTER TABLE custom_roles ALTER COLUMN created_at SET DEFAULT NOW()'),
  );
  await check(
    'custom_roles.updated_at SET DEFAULT NOW()',
    pool.query('ALTER TABLE custom_roles ALTER COLUMN updated_at SET DEFAULT NOW()'),
  );

  // ── 2. categories: add category_type column ───────────────────────────────
  await check(
    'categories ADD COLUMN category_type TEXT',
    pool.query("ALTER TABLE categories ADD COLUMN IF NOT EXISTS category_type TEXT NOT NULL DEFAULT 'PRODUCT'"),
  );

  // Backfill obvious expense-named categories to EXPENSE
  const expenseKeywords = ['gaji', 'pengeluaran', 'expense', 'biaya', 'listrik', 'air', 'sewa', 'transport'];
  for (const kw of expenseKeywords) {
    await check(
      `backfill EXPENSE for name ILIKE %${kw}%`,
      pool.query(
        "UPDATE categories SET category_type = 'EXPENSE' WHERE LOWER(name) LIKE $1 AND category_type != 'EXPENSE'",
        [`%${kw}%`],
      ),
    );
  }

  // ── Verify ────────────────────────────────────────────────────────────────
  const crRow = await pool.query(
    "SELECT column_name, column_default, is_nullable FROM information_schema.columns WHERE table_name='custom_roles' AND column_name IN ('createdAt','updatedAt','created_at','updated_at') ORDER BY column_name"
  );
  console.log('\n=== custom_roles timestamps after migration ===');
  crRow.rows.forEach(r => console.log(` ${r.column_name}: default=${r.column_default} nullable=${r.is_nullable}`));

  const catCols = await pool.query(
    "SELECT column_name, data_type, column_default FROM information_schema.columns WHERE table_name='categories' ORDER BY ordinal_position"
  );
  console.log('\n=== categories columns after migration ===');
  catCols.rows.forEach(r => console.log(` ${r.column_name} | ${r.data_type} | default=${r.column_default}`));

  const sampleCat = await pool.query("SELECT id, name, category_type FROM categories LIMIT 5");
  console.log('\n=== categories sample after migration ===');
  sampleCat.rows.forEach(r => console.log(` ${r.id} | ${r.name} | ${r.category_type}`));

  await pool.end();
  console.log('\nMigration complete.');
}

run().catch(e => {
  console.error('FATAL', e.message);
  pool.end();
  process.exit(1);
});
