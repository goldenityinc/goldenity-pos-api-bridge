const express = require('express');
const multer = require('multer');
const {
  listRecords,
  createRecords,
  upsertRecords,
  updateRecordById,
  deleteRecordById,
  uploadExpenseAttachment,
} = require('../controllers/recordsController');

const router = express.Router();
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 8 * 1024 * 1024,
  },
});

const maybeExpenseAttachmentUpload = (req, res, next) => {
  const table = (req.params?.table || '').toString().trim().toLowerCase();
  if (table !== 'expenses') {
    return next();
  }
  return upload.single('attachment')(req, res, next);
};

router.post('/expenses/attachment', upload.single('attachment'), uploadExpenseAttachment);
router.get('/:table', listRecords);
router.post('/:table', maybeExpenseAttachmentUpload, createRecords);
router.post('/:table/upsert', upsertRecords);
router.put('/:table/:id', maybeExpenseAttachmentUpload, updateRecordById);
router.delete('/:table/:id', deleteRecordById);

module.exports = router;
