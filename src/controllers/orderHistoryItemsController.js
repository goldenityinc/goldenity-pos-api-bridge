const { jsonOk, jsonError } = require('../utils/http');
const {
  parseBodyArray,
  parseBodyObject,
  buildInsertQuery,
  runSelect,
} = require('../utils/sqlHelpers');

// GET /order_history/items
// Query params yang didukung:
//   ?eq__is_completed=true   → hanya item Sudah Selesai
//   ?eq__is_completed=false  → hanya item Belum Selesai
//   ?limit=N, ?orderBy=col, ?ascending=true/false, ?select=col1,col2
const getOrderHistoryItems = async (req, res) => {
  try {
    const rows = await runSelect(req.tenantDb, 'order_history_items', req.query);
    return jsonOk(res, rows);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

const createOrderHistoryItems = async (req, res) => {
  try {
    const payload = parseBodyArray(req.body) || parseBodyObject(req.body);
    const { sql, values } = buildInsertQuery('order_history_items', payload);
    const result = await req.tenantDb.query(sql, values);
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
    const result = await req.tenantDb.query(
      'UPDATE order_history_items SET is_completed = true WHERE id = $1 RETURNING *',
      [id],
    );
    if ((result.rowCount || 0) === 0) {
      return jsonError(res, 404, 'Item tidak ditemukan');
    }
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
