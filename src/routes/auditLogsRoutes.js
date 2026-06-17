const express = require('express');
const { listAuditLogs } = require('../controllers/auditLogsController');

const router = express.Router();

router.get('/', listAuditLogs);

module.exports = router;
