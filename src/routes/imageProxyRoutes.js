const express = require('express');
const { serveImage } = require('../controllers/imageProxyController');

const router = express.Router();

/**
 * GET /images/:encodedKey
 * Serves images from S3-compatible storage (private bucket)
 * 
 * Usage:
 *   GET /images/logo_1234567.png
 *   GET /images/products%2Fproduct_123.jpg (URL-encoded path with subdirs)
 */
router.get('/:encodedKey', serveImage);

module.exports = router;
