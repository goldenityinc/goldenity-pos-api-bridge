const express = require('express');

const {
  createServiceOrder,
  getServiceOrders,
  updateServiceStatus,
} = require('../controllers/serviceOrderController');

const router = express.Router();

router.get('/', getServiceOrders);
router.post('/', createServiceOrder);
router.patch('/:id/status', updateServiceStatus);

module.exports = router;
