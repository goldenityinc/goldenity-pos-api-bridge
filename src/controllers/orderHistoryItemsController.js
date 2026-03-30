const { jsonOk, jsonError } = require('../utils/http');
const { emitTableMutation } = require('../services/realtimeEmitter');
const {
  parseBodyArray,
  parseBodyObject,
  buildInsertQuery,
  getTableColumnDefinitions,
  getTableColumnSet,
  normalizePayloadByColumnDefinitions,
  enforceTenantIdOnPayload,
  normalizeTenantId,
  runSelect,
} = require('../utils/sqlHelpers');
const {
  getCachedResponse,
  storeResponse,
} = require('../utils/idempotencyCache');

const ensureSelectedColumn = (selectValue, columnName) => {
  if (!selectValue || selectValue === '*') {
    return selectValue;
  }

  const columns = selectValue
    .toString()
    .split(',')
    .map((column) => column.trim())
    .filter(Boolean);

  const hasColumn = columns.some((column) => column.toLowerCase() === columnName.toLowerCase());
  if (hasColumn) {
    return columns.join(',');
  }

  return `${columns.join(',')},${columnName}`;
};

// GET /order_history/items
// Query params yang didukung:
//   ?eq__is_completed=true   → hanya item Sudah Selesai
//   ?eq__is_completed=false  → hanya item Belum Selesai
//   ?limit=N, ?orderBy=col, ?ascending=true/false, ?select=col1,col2
const getOrderHistoryItems = async (req, res) => {
  try {
    const tenantId = normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);
    const query = {
      ...req.query,
      select: ensureSelectedColumn(req.query?.select, 'id'),
    };
    const rows = await runSelect(req.tenantDb, 'order_history_items', query, { tenantId });
    return jsonOk(res, rows);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

const createOrderHistoryItems = async (req, res) => {
  try {
    const tenantId = normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);
    const cachedResponse = getCachedResponse(req, 'order_history_items');
    if (cachedResponse !== null) {
      return jsonOk(res, cachedResponse, 'Created (idempotent)', 200);
    }

    const payload = parseBodyArray(req.body) || parseBodyObject(req.body);
    const columnDefinitions = await getTableColumnDefinitions(req.tenantDb, 'order_history_items');
    const tenantScopedPayload = enforceTenantIdOnPayload(payload, tenantId, columnDefinitions);
    const filteredPayload = normalizePayloadByColumnDefinitions(tenantScopedPayload, columnDefinitions);
    const { sql, values } = buildInsertQuery('order_history_items', filteredPayload);
    const result = await req.tenantDb.query(sql, values);
    for (const row of result.rows) {
      emitTableMutation(req, {
        table: 'order_history_items',
        action: 'INSERT',
        record: row,
      });
    }
    storeResponse(req, 'order_history_items', result.rows);
    return jsonOk(res, result.rows, 'Created', 201);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

// PUT /order_history/items/:id/complete
// Tandai satu item sebagai Sudah Selesai (is_completed = true).
const completeOrderHistoryItem = async (req, res) => {
  try {
    const { id } = req.params;
    const tenantId = normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);
    const columnSet = await getTableColumnSet(req.tenantDb, 'order_history_items');
    const hasTenantColumn = columnSet.has('tenant_id');
    const query = hasTenantColumn
      ? {
          sql: 'UPDATE order_history_items SET is_completed = true WHERE id = $1 AND tenant_id = $2 RETURNING *',
          values: [id, tenantId],
        }
      : {
          sql: 'UPDATE order_history_items SET is_completed = true WHERE id = $1 RETURNING *',
          values: [id],
        };
    const result = await req.tenantDb.query(query.sql, query.values);
    if ((result.rowCount || 0) === 0) {
      return jsonError(res, 404, 'Item tidak ditemukan');
    }
    emitTableMutation(req, {
      table: 'order_history_items',
      action: 'UPDATE',
      record: result.rows[0] || null,
      id,
    });
    return jsonOk(res, result.rows[0] || null, 'Status diperbarui menjadi Selesai');
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

module.exports = {
  getOrderHistoryItems,
  createOrderHistoryItems,
  completeOrderHistoryItem,
};
