const express = require('express');
const { getCategories } = require('../controllers/categoriesController');

const router = express.Router();

router.get('/', getCategories);

module.exports = router;
