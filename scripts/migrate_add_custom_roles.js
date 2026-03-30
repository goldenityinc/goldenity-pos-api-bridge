/**
 * migrate_add_custom_roles.js
 *
 * Membuat tabel custom_roles di tenant database dan menambah kolom
 * custom_role_id ke app_users (jika belum ada).
 *
 * Jalankan: node scripts/migrate_add_custom_roles.js <DB_URL>
 * atau via env: DATABASE_URL=... node scripts/migrate_add_custom_roles.js
 */
const { Pool } = require('pg');
require('dotenv').config();

const dbUrl = process.argv[2] || process.env.DATABASE_URL;
if (!dbUrl) {
  console.error('Usage: node migrate_add_custom_roles.js <DB_URL>');
  process.exit(1);
}

const pool = new Pool({ connectionString: dbUrl });

const SQL = `
-- custom_roles: definisi role dinamis per tenant
CREATE TABLE IF NOT EXISTS custom_roles (
  id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  name        VARCHAR(80) NOT NULL,
  description TEXT,
  is_default  BOOLEAN     NOT NULL DEFAULT false,
  permissions JSONB       NOT NULL DEFAULT '{}',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(name)
);

-- Tambah kolom is_default jika tabel sudah ada (idempotent)
ALTER TABLE custom_roles ADD COLUMN IF NOT EXISTS is_default BOOLEAN NOT NULL DEFAULT false;

-- Trigger agar updated_at otomatis diperbarui
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS custom_roles_updated_at ON custom_roles;
CREATE TRIGGER custom_roles_updated_at
  BEFORE UPDATE ON custom_roles
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Tambah kolom custom_role_id ke app_users (nullable FK ke custom_roles)
ALTER TABLE app_users
  ADD COLUMN IF NOT EXISTS custom_role_id UUID REFERENCES custom_roles(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_app_users_custom_role ON app_users(custom_role_id);
`;

(async () => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(SQL);
    await client.query('COMMIT');
    console.log('✅ custom_roles migration applied successfully.');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('❌ Migration failed:', err.message);
    process.exit(1);
  } finally {
    client.release();
    await pool.end();
  }
})();
