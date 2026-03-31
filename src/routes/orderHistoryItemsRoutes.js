const express = require('express');
const {
  getOrderHistoryItems,
  createOrderHistoryItems,
  completeOrderHistoryItem,
  completeOrderHistoryBatch,
} = require('../controllers/orderHistoryItemsController');

const router = express.Router();

// GET  /             — semua item; filter via ?eq__is_completed=true|false
// POST /             — buat item baru
// PUT  /:id/complete — tandai item sebagai Sudah Selesai
router.get('/', getOrderHistoryItems);
router.post('/', createOrderHistoryItems);
router.put('/complete', completeOrderHistoryBatch);
router.put('/mark-complete', completeOrderHistoryBatch);
router.put('/:id/complete', completeOrderHistoryItem);

module.exports = router;
