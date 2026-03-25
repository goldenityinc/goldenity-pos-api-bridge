const express = require('express');
const {
	createTransaction,
	listActiveKasBon,
	settleKasBon,
} = require('../controllers/transactionsController');

const router = express.Router();

router.get('/kas-bon', listActiveKasBon);
router.post('/', createTransaction);
router.post('/kas-bon/:id/settle', settleKasBon);
router.post('/kas-bon/:id/pay', settleKasBon);

module.exports = router;
