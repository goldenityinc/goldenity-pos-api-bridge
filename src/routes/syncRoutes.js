const express = require('express');
const { runSync } = require('../controllers/syncController');

const router = express.Router();

router.post('/', runSync);

module.exports = router;
