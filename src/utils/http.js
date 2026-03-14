const jsonOk = (res, data, message = 'Success', status = 200) => {
  res.status(status).json({ success: true, message, data });
};

const jsonError = (res, status, message, error) => {
  res.status(status).json({
    success: false,
    message,
    error: error || null,
  });
};

module.exports = {
  jsonOk,
  jsonError,
};
