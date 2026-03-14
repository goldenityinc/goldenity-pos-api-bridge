const { jsonOk, jsonError } = require('../utils/http');
const {
  buildInsertQuery,
  buildUpdateQuery,
  buildDeleteQuery,
} = require('../utils/sqlHelpers');

const runSync = async (req, res) => {
  try {
    const { table, action, data, id } = req.body || {};

    if (!table || !action) {
      return jsonError(res, 400, 'table dan action wajib diisi');
    }

    if (action === 'INSERT') {
      const payload = { ...(data || {}) };
      if (typeof payload.id === 'string') {
        delete payload.id;
      }

      const { sql, values } = buildInsertQuery(table, payload);
      const result = await req.tenantDb.query(sql, values);
      return jsonOk(res, result.rows, 'Sync insert success', 201);
    }

    if (action === 'UPDATE') {
      const { sql, values } = buildUpdateQuery(table, data || {}, 'id', id);
      const result = await req.tenantDb.query(sql, values);
      return jsonOk(res, result.rows, 'Sync update success');
    }

    if (action === 'DELETE') {
      const { sql, values } = buildDeleteQuery(table, 'id', id);
      const result = await req.tenantDb.query(sql, values);
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
