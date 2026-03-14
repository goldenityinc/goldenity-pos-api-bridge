const express = require('express');
const {
  uploadNotSupported,
  deleteNotSupported,
} = require('../controllers/storageController');

const router = express.Router();

router.post('/upload', uploadNotSupported);
router.delete('/:bucket/:fileName', deleteNotSupported);

module.exports = router;
