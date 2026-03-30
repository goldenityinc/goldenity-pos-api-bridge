const { jsonOk, jsonError } = require('../utils/http');
const {
  buildInsertQuery,
  buildUpdateQuery,
  buildDeleteQuery,
  getTableColumnDefinitions,
  getTableColumnSet,
  normalizePayloadByColumnDefinitions,
  enforceTenantIdOnPayload,
  normalizeTenantId,
  runSelect,
} = require('../utils/sqlHelpers');
const { emitTableMutation } = require('../services/realtimeEmitter');

const normalizeProductPayload = (payload = {}) => {
  const next = { ...payload };

  if (next.imageUrl !== undefined && next.image_url === undefined) {
    next.image_url = next.imageUrl;
  }

  delete next.imageUrl;
  return next;
};

const normalizeCustomerPayload = (payload = {}, { isCreate = false } = {}) => {
  const next = { ...payload };

  delete next.totalSpent;
  delete next.total_spent;

  if (isCreate) {
    next.total_spent = 0;
  }

  return next;
};

const normalizePayloadForTable = (table, payload, options = {}) => {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return payload;
  }

  if (table === 'products') {
    return normalizeProductPayload(payload);
  }

  if (table === 'customers') {
    return normalizeCustomerPayload(payload, options);
  }

  return payload;
};

const ensureCustomersTable = async (tenantDb, table) => {
  if (table !== 'customers') return;

  await tenantDb.query(`
    CREATE TABLE IF NOT EXISTS customers (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      phone TEXT,
      total_spent DOUBLE PRECISION DEFAULT 0,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await tenantDb.query(`
    ALTER TABLE customers
    ADD COLUMN IF NOT EXISTS total_spent DOUBLE PRECISION DEFAULT 0;
  `);
  await tenantDb.query(`
    ALTER TABLE customers
    ADD COLUMN IF NOT EXISTS tenant_id TEXT;
  `);
};

const ensureProductsTableColumns = async (tenantDb, table) => {
  if (table !== 'products') return;

  await tenantDb.query(`
    ALTER TABLE products
    ADD COLUMN IF NOT EXISTS image_url TEXT;
  `);

  await tenantDb.query(`
    ALTER TABLE products
    ADD COLUMN IF NOT EXISTS reference_id TEXT;
  `);

  await tenantDb.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_products_reference_id_unique
    ON products (reference_id)
    WHERE reference_id IS NOT NULL;
  `);
};

const findExistingRecordByReferenceId = async (tenantDb, table, payload, tenantId) => {
  if (table !== 'products' || !payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return null;
  }

  const referenceId = (
    payload.reference_id ?? payload.referenceId ?? payload.local_id ?? payload.localId ?? ''
  )
    .toString()
    .trim();
  if (!referenceId) {
    return null;
  }

  return runSelect(tenantDb, table, {
    eq__reference_id: referenceId,
    maybeSingle: true,
  }, { tenantId });
};

const prepareTableSchema = async (tenantDb, table) => {
  await ensureCustomersTable(tenantDb, table);
  await ensureProductsTableColumns(tenantDb, table);
};

const hasMutationFields = (payload) => {
  return !!payload && typeof payload === 'object' && !Array.isArray(payload) && Object.keys(payload).length > 0;
};

const resolveTenantId = (req) => normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);

const runSync = async (req, res) => {
  try {
    const { table, action, data, id } = req.body || {};
    const tenantId = resolveTenantId(req);

    if (!table || !action) {
      return jsonError(res, 400, 'table dan action wajib diisi');
    }

    await prepareTableSchema(req.tenantDb, table);

    if (action === 'INSERT') {
      const payload = normalizePayloadForTable(table, { ...(data || {}) }, { isCreate: true });
      if (typeof payload.id === 'string') {
        delete payload.id;
      }

      const columnDefinitions = await getTableColumnDefinitions(req.tenantDb, table);
      const tenantScopedPayload = enforceTenantIdOnPayload(payload, tenantId, columnDefinitions);
      const filteredPayload = normalizePayloadByColumnDefinitions(tenantScopedPayload, columnDefinitions);
      if (!hasMutationFields(filteredPayload)) {
        return jsonError(res, 400, `Tidak ada kolom yang cocok untuk tabel ${table}`);
      }

      const existingRecord = await findExistingRecordByReferenceId(
        req.tenantDb,
        table,
        filteredPayload,
        tenantId,
      );
      if (existingRecord) {
        return jsonOk(res, [existingRecord], 'Sync insert already exists');
      }

      const { sql, values } = buildInsertQuery(table, filteredPayload);
      const result = await req.tenantDb.query(sql, values);
      for (const row of result.rows) {
        emitTableMutation(req, {
          table,
          action: 'INSERT',
          record: row,
        });
      }
      return jsonOk(res, result.rows, 'Sync insert success', 201);
    }

    if (action === 'UPDATE') {
      const payload = normalizePayloadForTable(table, data || {}, { isCreate: false });
      const columnDefinitions = await getTableColumnDefinitions(req.tenantDb, table);
      const tenantScopedPayload = enforceTenantIdOnPayload(payload, tenantId, columnDefinitions);
      const filteredPayload = normalizePayloadByColumnDefinitions(tenantScopedPayload, columnDefinitions);
      if (!hasMutationFields(filteredPayload)) {
        const existing = await runSelect(req.tenantDb, table, {
          eq__id: id,
          maybeSingle: true,
        }, { tenantId });
        return jsonOk(res, existing ? [existing] : [], 'Sync update skipped');
      }
      const columnSet = await getTableColumnSet(req.tenantDb, table);
      const { sql, values } = buildUpdateQuery(
        table,
        filteredPayload,
        'id',
        id,
        { tenantId, hasTenantColumn: columnSet.has('tenant_id') },
      );
      const result = await req.tenantDb.query(sql, values);
      emitTableMutation(req, {
        table,
        action: 'UPDATE',
        record: result.rows[0] || null,
        id,
      });
      return jsonOk(res, result.rows, 'Sync update success');
    }

    if (action === 'DELETE') {
      const columnSet = await getTableColumnSet(req.tenantDb, table);
      const { sql, values } = buildDeleteQuery(
        table,
        'id',
        id,
        { tenantId, hasTenantColumn: columnSet.has('tenant_id') },
      );
      const result = await req.tenantDb.query(sql, values);
      emitTableMutation(req, {
        table,
        action: 'DELETE',
        record: result.rows[0] || null,
        id,
      });
      return jsonOk(res, result.rows, 'Sync delete success');
    }

    return jsonError(res, 400, `Action tidak didukung: ${action}`);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

module.exports = {
  runSync,
};
