const { jsonOk, jsonError } = require('../utils/http');
const { runSelect } = require('../utils/sqlHelpers');

const getProducts = async (req, res) => {
  try {
    const rows = await runSelect(req.tenantDb, 'products', req.query);
    return jsonOk(res, rows);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

module.exports = {
  getProducts,
};
