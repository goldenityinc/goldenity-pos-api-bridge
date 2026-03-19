const express = require('express');
const { getProducts, reduceStock } = require('../controllers/productController');

const router = express.Router();

router.get('/', getProducts);
router.post('/:id/reduce-stock', reduceStock);
router.post('/:id/adjust-stock', reduceStock);
router.post('/:id/adjust', reduceStock);

module.exports = router;
