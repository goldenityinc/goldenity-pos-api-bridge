const express = require('express');
const {
  listRoles,
  createRole,
  updateRole,
  deleteRole,
  seedDefaultRoles,
} = require('../controllers/roleController');

const router = express.Router();

// GET /api/roles
router.get('/', listRoles);

// POST /api/roles
router.post('/', createRole);

// POST /api/roles/seed
router.post('/seed', seedDefaultRoles);

// PUT /api/roles/:id
router.put('/:id', updateRole);

// DELETE /api/roles/:id
router.delete('/:id', deleteRole);

module.exports = router;
