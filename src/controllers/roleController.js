const { jsonOk, jsonError } = require('../utils/http');
const { randomUUID } = require('crypto');
const { normalizeTenantId } = require('../utils/sqlHelpers');

const normalizeRoleRow = (row) => ({
  ...row,
  isDefault: row.is_default === true,
});

const roleModules = [
  'penjualan',
  'inventaris',
  'daftar_belanja',
  'riwayat',
  'kas_bon',
  'data_pelanggan',
  'laporan_keuangan',
  'pengeluaran',
  'data_supplier',
  'laporan_pajak',
  'pengaturan',
  'manajemen_user',
  'manajemen_kategori',
];

const perms = (c, r, u, d) => ({ c, r, u, d });

const fullPerms = () =>
  Object.fromEntries(roleModules.map((moduleKey) => [moduleKey, perms(true, true, true, true)]));

const emptyPerms = () =>
  Object.fromEntries(roleModules.map((moduleKey) => [moduleKey, perms(false, false, false, false)]));

const defaultRolePayloads = () => {
  const kasirPerms = emptyPerms();
  kasirPerms.penjualan = perms(true, true, false, false);
  kasirPerms.daftar_belanja = perms(true, true, true, false);
  kasirPerms.riwayat = perms(false, true, false, false);
  kasirPerms.data_pelanggan = perms(false, true, false, false);

  const pajakPerms = emptyPerms();
  pajakPerms.penjualan = perms(false, true, false, false);
  pajakPerms.laporan_pajak = perms(false, true, false, false);

  return [
    {
      name: 'Admin',
      description: 'Akses penuh ke semua fitur',
      permissions: fullPerms(),
    },
    {
      name: 'Kasir',
      description: 'Akses operasional kasir harian',
      permissions: kasirPerms,
    },
    {
      name: 'Pajak',
      description: 'Akses laporan pajak dan dashboard',
      permissions: pajakPerms,
    },
  ];
};

const ensureCustomRolesInfra = async (pool) => {
  // Self-healing guard: tenant DB lama mungkin belum menjalankan migrasi custom_roles.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS custom_roles (
      id          UUID PRIMARY KEY,
      tenant_id   TEXT NOT NULL,
      name        VARCHAR(80) NOT NULL,
      description TEXT,
      is_default  BOOLEAN NOT NULL DEFAULT false,
      permissions JSONB NOT NULL DEFAULT '{}',
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(tenant_id, name)
    )
  `);

  await pool.query(
    'ALTER TABLE custom_roles ADD COLUMN IF NOT EXISTS tenant_id TEXT',
  );

  await pool.query(
    'ALTER TABLE custom_roles ADD COLUMN IF NOT EXISTS is_default BOOLEAN NOT NULL DEFAULT false',
  );

  await pool.query(
    'ALTER TABLE custom_roles ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()',
  );

  await pool.query(
    'ALTER TABLE custom_roles ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()',
  );

  await pool.query(
    'ALTER TABLE custom_roles ALTER COLUMN tenant_id SET NOT NULL',
  ).catch(() => {});

  await pool.query(
    'CREATE INDEX IF NOT EXISTS idx_custom_roles_is_default ON custom_roles(is_default)',
  );
  await pool.query(
    'CREATE INDEX IF NOT EXISTS idx_custom_roles_tenant_id ON custom_roles(tenant_id)',
  );

  await pool.query(`
    ALTER TABLE app_users
    ADD COLUMN IF NOT EXISTS custom_role_id UUID REFERENCES custom_roles(id) ON DELETE SET NULL
  `);
};

const listRoles = async (req, res) => {
  try {
    const pool = req.db;
    const tenantId = normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);
    await ensureCustomRolesInfra(pool);

    const columnsResult = await pool.query(
      `SELECT column_name
       FROM information_schema.columns
       WHERE table_schema = ANY(current_schemas(false))
         AND table_name = 'custom_roles'`,
    );
    const columns = new Set(columnsResult.rows.map((row) => row.column_name));
    const hasTenantColumn = columns.has('tenant_id');

    let result;
    if (hasTenantColumn) {
      if (tenantId) {
        result = await pool.query(
          `
          SELECT id, name, description, permissions, COALESCE(is_default, false) AS is_default, created_at, updated_at
          FROM custom_roles
          WHERE tenant_id = $1 OR tenant_id IS NULL
          ORDER BY COALESCE(is_default, false) DESC, created_at ASC
          `,
          [tenantId],
        );
      } else {
        result = await pool.query(
          `
          SELECT id, name, description, permissions, COALESCE(is_default, false) AS is_default, created_at, updated_at
          FROM custom_roles
          WHERE tenant_id IS NULL
          ORDER BY COALESCE(is_default, false) DESC, created_at ASC
          `,
        );
      }
    } else {
      result = await pool.query(
        `
        SELECT id, name, description, permissions, COALESCE(is_default, false) AS is_default, created_at, updated_at
        FROM custom_roles
        ORDER BY COALESCE(is_default, false) DESC, created_at ASC
        `,
      );
    }

    return jsonOk(res, result.rows.map(normalizeRoleRow));
  } catch (error) {
    console.error('Role Fetch Error:', error);
    return jsonError(res, 500, 'Gagal mengambil daftar role', error.message);
  }
};

const createRole = async (req, res) => {
  try {
    const pool = req.db;
    const tenantId = normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);
    await ensureCustomRolesInfra(pool);
    const { name, description = null, permissions } = req.body || {};

    if (!name || typeof name !== 'string' || !name.trim()) {
      return jsonError(res, 400, 'name wajib diisi');
    }
    if (!permissions || typeof permissions !== 'object' || Array.isArray(permissions)) {
      return jsonError(res, 400, 'permissions wajib berupa JSON object');
    }

    const result = await pool.query(
      `
      INSERT INTO custom_roles (id, tenant_id, name, description, permissions, is_default)
      VALUES ($1, $2, $3, $4, $5::jsonb, false)
      RETURNING id, name, description, permissions, COALESCE(is_default, false) AS is_default, created_at, updated_at
      `,
      [randomUUID(), tenantId, name.trim(), description, JSON.stringify(permissions)],
    );

    return jsonOk(res, normalizeRoleRow(result.rows[0]), 'Role created', 201);
  } catch (error) {
    if (error.code === '23505') {
      return jsonError(res, 409, 'Nama role sudah digunakan');
    }
    return jsonError(res, 500, 'Gagal membuat role', error.message);
  }
};

const updateRole = async (req, res) => {
  try {
    const pool = req.db;
    const tenantId = normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);
    await ensureCustomRolesInfra(pool);
    const { id } = req.params;
    const { name, description, permissions } = req.body || {};

    const existingResult = await pool.query(
      `
      SELECT id, name, description, permissions, COALESCE(is_default, false) AS is_default
      FROM custom_roles
      WHERE id = $1 AND tenant_id = $2
      LIMIT 1
      `,
      [id, tenantId],
    );

    const existing = existingResult.rows[0];
    if (!existing) {
      return jsonError(res, 404, 'Role tidak ditemukan');
    }

    if (!permissions || typeof permissions !== 'object' || Array.isArray(permissions)) {
      return jsonError(res, 400, 'permissions wajib berupa JSON object');
    }

    if (existing.is_default === true) {
      // Role bawaan hanya boleh update permissions
      if (typeof name === 'string' && name.trim() !== '' && name.trim() !== existing.name) {
        return jsonError(res, 403, 'Role bawaan tidak boleh diubah namanya');
      }
      if (description !== undefined && description !== existing.description) {
        return jsonError(res, 403, 'Role bawaan hanya boleh diubah permissions-nya');
      }

      const updatedDefault = await pool.query(
        `
        UPDATE custom_roles
        SET permissions = $2::jsonb, updated_at = NOW()
        WHERE id = $1 AND tenant_id = $3
        RETURNING id, name, description, permissions, COALESCE(is_default, false) AS is_default, created_at, updated_at
        `,
        [id, JSON.stringify(permissions), tenantId],
      );

      return jsonOk(res, normalizeRoleRow(updatedDefault.rows[0]), 'Role updated');
    }

    const safeName = typeof name === 'string' && name.trim() ? name.trim() : existing.name;
    const safeDescription = description === undefined ? existing.description : description;

    const updated = await pool.query(
      `
      UPDATE custom_roles
      SET name = $2,
          description = $3,
          permissions = $4::jsonb,
          updated_at = NOW()
      WHERE id = $1 AND tenant_id = $5
      RETURNING id, name, description, permissions, COALESCE(is_default, false) AS is_default, created_at, updated_at
      `,
      [id, safeName, safeDescription, JSON.stringify(permissions), tenantId],
    );

    return jsonOk(res, normalizeRoleRow(updated.rows[0]), 'Role updated');
  } catch (error) {
    if (error.code === '23505') {
      return jsonError(res, 409, 'Nama role sudah digunakan');
    }
    return jsonError(res, 500, 'Gagal mengubah role', error.message);
  }
};

const deleteRole = async (req, res) => {
  try {
    const pool = req.db;
    const tenantId = normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);
    await ensureCustomRolesInfra(pool);
    const { id } = req.params;

    const existingResult = await pool.query(
      'SELECT id, COALESCE(is_default, false) AS is_default FROM custom_roles WHERE id = $1 AND tenant_id = $2 LIMIT 1',
      [id, tenantId],
    );

    const existing = existingResult.rows[0];
    if (!existing) {
      return jsonError(res, 404, 'Role tidak ditemukan');
    }

    if (existing.is_default === true) {
      return jsonError(res, 403, 'Role bawaan tidak boleh dihapus');
    }

    await pool.query('DELETE FROM custom_roles WHERE id = $1 AND tenant_id = $2', [id, tenantId]);
    return jsonOk(res, null, 'Role deleted');
  } catch (error) {
    return jsonError(res, 500, 'Gagal menghapus role', error.message);
  }
};

const seedDefaultRoles = async (req, res) => {
  try {
    const pool = req.db;
    const tenantId = normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);
    await ensureCustomRolesInfra(pool);

    for (const role of defaultRolePayloads()) {
      await pool.query(
        `
        INSERT INTO custom_roles (id, tenant_id, name, description, permissions, is_default)
        VALUES ($1, $2, $3, $4, $5::jsonb, true)
        ON CONFLICT (tenant_id, name) DO NOTHING
        `,
        [randomUUID(), tenantId, role.name, role.description, JSON.stringify(role.permissions)],
      );
    }

    const result = await pool.query(
      `
      SELECT id, name, description, permissions, COALESCE(is_default, false) AS is_default, created_at, updated_at
      FROM custom_roles
      WHERE tenant_id = $1
      ORDER BY created_at ASC
      `,
      [tenantId],
    );

    return jsonOk(res, result.rows.map(normalizeRoleRow), 'Default roles seeded');
  } catch (error) {
    return jsonError(res, 500, 'Gagal seed role bawaan', error.message);
  }
};

module.exports = {
  listRoles,
  createRole,
  updateRole,
  deleteRole,
  seedDefaultRoles,
};
