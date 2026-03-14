const express = require('express');
const {
  getOrderHistoryItems,
  getArchivedOrderHistoryItems,
  createOrderHistoryItems,
  archiveOrderHistoryItems,
} = require('../controllers/orderHistoryItemsController');

const router = express.Router();

router.get('/', getOrderHistoryItems);
router.get('/archived', getArchivedOrderHistoryItems);
router.post('/', createOrderHistoryItems);
router.put('/archive', archiveOrderHistoryItems);

module.exports = router;
