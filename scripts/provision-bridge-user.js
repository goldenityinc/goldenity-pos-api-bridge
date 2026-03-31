/**
 * provision-bridge-user.js
 *
 * Upserts a POS user into the bridge `app_users` table so that
 * bridge-native login works independently of Admin Core.
 *
 * Usage:
 *   node scripts/provision-bridge-user.js
 *
 * Reads DATABASE_URL from .env or environment.
 */

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const { Pool } = require('pg');
const bcrypt = require('bcryptjs');
const { randomUUID } = require('crypto');

// ── Configuration ────────────────────────────────────────────────────────────
const TARGET_USERS = [
  {
    username: 'andre',
    password: 'Admin123',
    tenantId: '96bfc081-2ce8-4a0e-be90-c052f499bed5',
    role: 'TENANT_ADMIN',
  },
];
// ─────────────────────────────────────────────────────────────────────────────

async function run() {
  const connectionString = (process.env.DATABASE_URL || '').trim();
  if (!connectionString) {
    console.error('DATABASE_URL is not set');
    process.exit(1);
  }

  const pool = new Pool({ connectionString });

  try {
    // Ensure app_users has the minimum expected schema
    await pool.query(`
      CREATE TABLE IF NOT EXISTS app_users (
        id         UUID        PRIMARY KEY,
        username   TEXT        NOT NULL,
        password   TEXT        NOT NULL,
        tenant_id  TEXT        NOT NULL,
        role       TEXT        NOT NULL DEFAULT 'KASIR',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(username, tenant_id)
      )
    `);
    await pool.query('ALTER TABLE app_users ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT \'KASIR\'');
    await pool.query('ALTER TABLE app_users ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()');
    await pool.query('ALTER TABLE app_users ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()');

    for (const user of TARGET_USERS) {
      const hashed = await bcrypt.hash(user.password, 12);

      // Check if exists
      const check = await pool.query(
        'SELECT id, username, tenant_id, password FROM app_users WHERE username = $1 AND tenant_id = $2 LIMIT 1',
        [user.username, user.tenantId],
      );

      if (check.rows[0]) {
        const existing = check.rows[0];
        const passwordOk = await bcrypt.compare(user.password, existing.password || '').catch(() => false)
          || existing.password === user.password;

        if (passwordOk) {
          console.log(`[OK] ${user.username} already exists with correct password — no change needed`);
        } else {
          await pool.query(
            'UPDATE app_users SET password = $1, updated_at = NOW() WHERE id = $2',
            [hashed, existing.id],
          );
          console.log(`[UPDATED] ${user.username} password updated`);
        }
      } else {
        // Check if username exists under any tenant
        const anyCheck = await pool.query(
          'SELECT id, tenant_id FROM app_users WHERE username = $1 LIMIT 5',
          [user.username],
        );
        if (anyCheck.rows.length > 0) {
          console.log(`[INFO] ${user.username} exists under different tenantId(s): ${anyCheck.rows.map(r => r.tenant_id).join(', ')}`);
        }

        await pool.query(
          `INSERT INTO app_users (username, password, tenant_id, role)
           VALUES ($1, $2, $3, $4)`,
          [user.username, hashed, user.tenantId, user.role],
        );
        console.log(`[INSERTED] ${user.username} created for tenant ${user.tenantId}`);
      }
    }

    // Verify login works
    console.log('\n--- Verifying login ---');
    for (const user of TARGET_USERS) {
      const result = await pool.query(
        'SELECT id, username, tenant_id, password, role FROM app_users WHERE username = $1 AND tenant_id = $2 LIMIT 1',
        [user.username, user.tenantId],
      );
      if (!result.rows[0]) {
        console.log(`[FAIL] ${user.username} NOT FOUND after provisioning!`);
        continue;
      }
      const ok = await bcrypt.compare(user.password, result.rows[0].password || '');
      const status = ok ? 'PASS' : 'FAIL';
      console.log(`[${status}] ${user.username} login verification — role=${result.rows[0].role}`);
    }
  } finally {
    await pool.end();
  }
}

run().catch((err) => {
  console.error('FATAL', err);
  process.exit(1);
});
