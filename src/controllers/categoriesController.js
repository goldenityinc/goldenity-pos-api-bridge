const { jsonOk, jsonError } = require('../utils/http');
const { runSelect } = require('../utils/sqlHelpers');

const getCategories = async (req, res) => {
  try {
    const rows = await runSelect(req.tenantDb, 'categories', req.query);
    return jsonOk(res, rows);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

module.exports = {
  getCategories,
};
