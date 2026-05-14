const express = require('express');
const { listBranchesForCurrentUser } = require('../controllers/branchesController');

const router = express.Router();

router.get('/', listBranchesForCurrentUser);

module.exports = router;
