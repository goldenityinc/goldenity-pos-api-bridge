const express = require('express');
const {
  getOrderHistoryItems,
  createOrderHistoryItems,
  archiveOrderHistoryItems,
} = require('../controllers/orderHistoryItemsController');

const router = express.Router();

router.get('/', getOrderHistoryItems);
router.post('/', createOrderHistoryItems);
router.put('/archive', archiveOrderHistoryItems);

module.exports = router;
