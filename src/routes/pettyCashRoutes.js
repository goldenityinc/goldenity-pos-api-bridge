const express = require('express');

const {
  getTodayPettyCashLogs,
  createPettyCashLog,
} = require('../controllers/pettyCashController');

const router = express.Router();

router.get('/', getTodayPettyCashLogs);
router.post('/', createPettyCashLog);

module.exports = router;