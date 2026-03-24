const express = require('express');
const productsRoutes = require('./productsRoutes');
const categoriesRoutes = require('./categoriesRoutes');
const transactionsRoutes = require('./transactionsRoutes');
const syncRoutes = require('./syncRoutes');
const orderHistoryItemsRoutes = require('./orderHistoryItemsRoutes');
const recordsRoutes = require('./recordsRoutes');
const storageRoutes = require('./storageRoutes');
const pettyCashRoutes = require('./pettyCashRoutes');
const { createCrudTableRoutes } = require('./crudTableRoutes');
const { resetOperationalData } = require('../controllers/debugController');

const router = express.Router();

router.use('/products', productsRoutes);
router.use('/categories', categoriesRoutes);
router.use('/transactions', transactionsRoutes);
router.use('/sync', syncRoutes);
router.use('/order_history/items', orderHistoryItemsRoutes);
router.use('/order-history/items', orderHistoryItemsRoutes);
router.use('/shopping-list', orderHistoryItemsRoutes);
router.use('/records', recordsRoutes);
router.use('/storage', storageRoutes);
router.use('/petty-cash', pettyCashRoutes);
router.post('/debug/reset-data', resetOperationalData);

router.use('/users', createCrudTableRoutes('app_users'));
router.use('/suppliers', createCrudTableRoutes('suppliers'));
router.use('/restock_history', createCrudTableRoutes('restock_history'));
router.use('/order_history', createCrudTableRoutes('order_history'));
router.use('/daily_cash', createCrudTableRoutes('daily_cash'));
router.use('/expenses', createCrudTableRoutes('expenses'));
router.use('/store_settings', createCrudTableRoutes('store_settings'));

module.exports = router;
