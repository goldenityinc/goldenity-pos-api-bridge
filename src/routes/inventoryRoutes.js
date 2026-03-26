const express = require('express');
const multer = require('multer');
const {
  downloadInventoryTemplate,
  exportInventoryCsv,
  importInventoryCsv,
} = require('../controllers/inventoryController');

const router = express.Router();
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 5 * 1024 * 1024,
  },
});

router.get('/template', downloadInventoryTemplate);
router.get('/export', exportInventoryCsv);
router.post('/import', upload.single('file'), importInventoryCsv);

module.exports = router;