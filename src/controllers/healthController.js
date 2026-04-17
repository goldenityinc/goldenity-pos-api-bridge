const { jsonOk } = require('../utils/http');

const healthCheck = (req, res) => {
  return jsonOk(res, { status: 'ok', build: 'dd6e729-cast-fix' }, 'Bridge API healthy');
};

module.exports = {
  healthCheck,
};
