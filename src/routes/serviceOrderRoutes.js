const express = require('express');

const {
  createServiceOrder,
  getServiceOrders,
  updateServiceOrder,
} = require('../controllers/serviceOrderController');

const router = express.Router();

router.get('/', getServiceOrders);
router.post('/', createServiceOrder);
router.patch('/:id', updateServiceOrder);

module.exports = router;
