const { jsonOk, jsonError } = require('../utils/http');
const { buildInsertQuery } = require('../utils/sqlHelpers');

const createTransaction = async (req, res) => {
  try {
    const payload = { ...req.body };
    const { sql, values } = buildInsertQuery('sales_records', payload);
    const result = await req.tenantDb.query(sql, values);
    return jsonOk(res, result.rows[0] || null, 'Transaction saved', 201);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

module.exports = {
  createTransaction,
};
