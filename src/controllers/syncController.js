const { jsonOk, jsonError } = require('../utils/http');
const {
  buildInsertQuery,
  buildUpdateQuery,
  buildDeleteQuery,
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
};

const ensureProductsTableColumns = async (tenantDb, table) => {
  if (table !== 'products') return;

  await tenantDb.query(`
    ALTER TABLE products
    ADD COLUMN IF NOT EXISTS image_url TEXT;
  `);
};

const prepareTableSchema = async (tenantDb, table) => {
  await ensureCustomersTable(tenantDb, table);
  await ensureProductsTableColumns(tenantDb, table);
};

const runSync = async (req, res) => {
  try {
    const { table, action, data, id } = req.body || {};

    if (!table || !action) {
      return jsonError(res, 400, 'table dan action wajib diisi');
    }

    await prepareTableSchema(req.tenantDb, table);

    if (action === 'INSERT') {
      const payload = normalizePayloadForTable(table, { ...(data || {}) }, { isCreate: true });
      if (typeof payload.id === 'string') {
        delete payload.id;
      }

      const { sql, values } = buildInsertQuery(table, payload);
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
      const { sql, values } = buildUpdateQuery(table, payload, 'id', id);
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
      const { sql, values } = buildDeleteQuery(table, 'id', id);
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
