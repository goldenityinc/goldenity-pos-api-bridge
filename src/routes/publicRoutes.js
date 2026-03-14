const express = require('express');
const { healthCheck } = require('../controllers/healthController');
const { login } = require('../controllers/authController');

const router = express.Router();

router.get('/health', healthCheck);
router.post('/auth/login', login);

module.exports = router;
