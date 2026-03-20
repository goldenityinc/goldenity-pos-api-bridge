const express = require('express');
const {
  uploadBase64,
  deleteStoredObject,
} = require('../controllers/storageController');

const router = express.Router();

router.post('/upload', uploadBase64);
router.delete('/:bucket/:fileName', deleteStoredObject);

module.exports = router;
