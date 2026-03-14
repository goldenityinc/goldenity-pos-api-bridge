const express = require('express');
const { createCrudController } = require('../controllers/crudController');

const createCrudTableRoutes = (table) => {
  const router = express.Router();
  const controller = createCrudController(table);

  router.get('/', controller.list);
  router.post('/', controller.create);
  router.put('/:id', controller.updateById);
  router.delete('/:id', controller.deleteById);

  return router;
};

module.exports = {
  createCrudTableRoutes,
};
