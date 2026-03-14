const { jsonOk, jsonError } = require('../utils/http');
const {
  normalizeArray,
  parseQueryValue,
  parseBodyArray,
  parseBodyObject,
  buildInsertQuery,
  runSelect,
} = require('../utils/sqlHelpers');

const getOrderHistoryItems = async (req, res) => {
  try {
    const rows = await runSelect(req.tenantDb, 'order_history_items', req.query);
    return jsonOk(res, rows);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

const createOrderHistoryItems = async (req, res) => {
  try {
    const payload = parseBodyArray(req.body) || parseBodyObject(req.body);
    const { sql, values } = buildInsertQuery('order_history_items', payload);
    const result = await req.tenantDb.query(sql, values);
    return jsonOk(res, result.rows, 'Created', 201);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

const archiveOrderHistoryItems = async (req, res) => {
  try {
    const { archiveAll = false, productIds = [], manualItemIds = [] } = req.body || {};

    if (archiveAll) {
      const result = await req.tenantDb.query(
        'UPDATE order_history_items SET is_archived = true WHERE is_archived = false RETURNING *',
      );
      return jsonOk(res, result.rows, 'Archived');
    }

    const archived = [];
    const productIdList = normalizeArray(productIds).map(parseQueryValue);
    const manualIdList = normalizeArray(manualItemIds).map(parseQueryValue);

    if (productIdList.length > 0) {
      const result = await req.tenantDb.query(
        'UPDATE order_history_items SET is_archived = true WHERE product_id = ANY($1) AND is_archived = false RETURNING *',
        [productIdList],
      );
      archived.push(...result.rows);
    }

    if (manualIdList.length > 0) {
      const result = await req.tenantDb.query(
        'UPDATE order_history_items SET is_archived = true WHERE manual_item_id = ANY($1) AND is_archived = false RETURNING *',
        [manualIdList],
      );
      archived.push(...result.rows);
    }

    return jsonOk(res, archived, 'Archived');
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

module.exports = {
  getOrderHistoryItems,
  createOrderHistoryItems,
  archiveOrderHistoryItems,
};
