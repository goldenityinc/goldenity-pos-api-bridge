const { jsonOk } = require('../utils/http');

const healthCheck = (req, res) => {
  return jsonOk(res, { status: 'ok' }, 'Bridge API healthy');
};

module.exports = {
  healthCheck,
};
