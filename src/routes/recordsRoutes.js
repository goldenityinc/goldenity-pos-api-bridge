const express = require('express');
const {
  listRecords,
  createRecords,
  upsertRecords,
  updateRecordById,
  deleteRecordById,
} = require('../controllers/recordsController');

const router = express.Router();

router.get('/:table', listRecords);
router.post('/:table', createRecords);
router.post('/:table/upsert', upsertRecords);
router.put('/:table/:id', updateRecordById);
router.delete('/:table/:id', deleteRecordById);

module.exports = router;
