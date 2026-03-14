const { jsonError } = require('../utils/http');

const uploadNotSupported = (req, res) => {
  return jsonError(res, 501, 'Storage upload tidak didukung pada bridge PostgreSQL ini');
};

const deleteNotSupported = (req, res) => {
  return jsonError(res, 501, 'Storage delete tidak didukung pada bridge PostgreSQL ini');
};

module.exports = {
  uploadNotSupported,
  deleteNotSupported,
};
