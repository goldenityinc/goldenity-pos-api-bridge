const express = require('express');
const { createTransaction, settleKasBon } = require('../controllers/transactionsController');

const router = express.Router();

router.post('/', createTransaction);
router.post('/kas-bon/:id/settle', settleKasBon);

module.exports = router;
