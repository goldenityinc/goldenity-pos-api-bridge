const { jsonOk, jsonError } = require('../utils/http');
const { emitTableMutation } = require('../services/realtimeEmitter');
const { ensureTenantScopedTable } = require('../utils/tenantScope');
const {
  parseBodyArray,
  parseBodyObject,
  buildInsertQuery,
  getTableColumnDefinitions,
  getTableColumnSet,
  normalizePayloadByColumnDefinitions,
  enforceTenantIdOnPayload,
  normalizeTenantId,
  runSelect,
} = require('../utils/sqlHelpers');
const {
  getCachedResponse,
  storeResponse,
} = require('../utils/idempotencyCache');

const ensureSelectedColumn = (selectValue, columnName) => {
  if (!selectValue || selectValue === '*') {
    return selectValue;
  }

  const columns = selectValue
    .toString()
    .split(',')
    .map((column) => column.trim())
    .filter(Boolean);

  const hasColumn = columns.some((column) => column.toLowerCase() === columnName.toLowerCase());
  if (hasColumn) {
    return columns.join(',');
  }

  return `${columns.join(',')},${columnName}`;
};

const removeSelectedColumn = (selectValue, columnName) => {
  if (!selectValue || selectValue === '*') {
    return selectValue;
  }

  const columns = selectValue
    .toString()
    .split(',')
    .map((column) => column.trim())
    .filter(Boolean)
    .filter((column) => column.toLowerCase() !== columnName.toLowerCase());

  return columns.length > 0 ? columns.join(',') : '*';
};

const normalizeCompletionStatus = (row = {}) => {
  // Use is_received as the completion indicator (is_completed column doesn't exist)
  const isReceived = parseBool(row.is_received);
  const receivedQty = Number(row.received_qty || 0);
  
  // Item is completed if it's marked as received and has a received quantity > 0
  return isReceived && receivedQty > 0;
};

const parseBool = (value) => {
  if (typeof value === 'boolean') return value;
  const normalized = (value ?? '').toString().trim().toLowerCase();
  return normalized === 'true' || normalized === '1' || normalized === 'yes';
};

const parsePositiveInt = (value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  const normalized = Math.floor(parsed);
  return normalized > 0 ? normalized : null;
};

const parsePositiveNumber = (value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return parsed;
};

const resolveReceivedQty = (item, payload = {}) => {
  const fromPayload =
    parsePositiveInt(payload.received_qty) ??
    parsePositiveInt(payload.receivedQty) ??
    parsePositiveInt(payload.qty_received) ??
    parsePositiveInt(payload.qty);

  if (fromPayload !== null) {
    return fromPayload;
  }

  return (
    parsePositiveInt(item.received_qty) ??
    parsePositiveInt(item.receivedQty) ??
    parsePositiveInt(item.qty) ??
    1
  );
};

const completeItemWithinTransaction = async ({
  client,
  req,
  tenantId,
  itemId,
  payload,
  orderItemsColumnSet,
  productsColumnSet,
}) => {
  const hasItemsTenant = orderItemsColumnSet.has('tenant_id');
  const hasProductsTenant = productsColumnSet.has('tenant_id');

  const itemResult = await client.query(
    hasItemsTenant
      ? 'SELECT * FROM order_history_items WHERE id = $1 AND tenant_id = $2 LIMIT 1 FOR UPDATE'
      : 'SELECT * FROM order_history_items WHERE id = $1 LIMIT 1 FOR UPDATE',
    hasItemsTenant ? [itemId, tenantId] : [itemId],
  );

  if ((itemResult.rowCount || 0) === 0) {
    return { notFound: true };
  }

  const item = itemResult.rows[0] || {};
  const productId = (
    item.product_id ?? item.entity_id ?? item.productId ?? item.entityId ?? ''
  ).toString().trim();
  const isManual = parseBool(item.is_manual);
  const alreadyReceived = parseBool(item.is_received);
  const alreadyCompleted = parseBool(item.is_completed);
  const receivedQty = resolveReceivedQty(item, payload);

  let productUpdate = null;
  if (!isManual && productId && !alreadyReceived && !alreadyCompleted) {
    const receivedPurchasePrice =
      parsePositiveNumber(payload.received_purchase_price) ??
      parsePositiveNumber(payload.receivedPurchasePrice);

    const productSetClauses = [
      'stock = COALESCE(stock, 0) + $1',
      'updated_at = NOW()',
    ];

    if (productsColumnSet.has('purchase_price') && receivedPurchasePrice !== null) {
      productSetClauses.push(
        `purchase_price = CASE
          WHEN (COALESCE(stock, 0) + $1) > 0
            THEN ROUND((((COALESCE(stock, 0) * COALESCE(purchase_price, 0)) + ($1 * $2)) / (COALESCE(stock, 0) + $1))::numeric, 0)
          ELSE COALESCE(purchase_price, 0)
        END`,
      );
    }

    const productUpdateValues = [receivedQty];
    if (productsColumnSet.has('purchase_price') && receivedPurchasePrice !== null) {
      productUpdateValues.push(receivedPurchasePrice);
    }
    productUpdateValues.push(productId);
    if (hasProductsTenant) {
      productUpdateValues.push(tenantId);
    }

    const productUpdateResult = await client.query(
      hasProductsTenant
        ? `UPDATE products
           SET ${productSetClauses.join(', ')}
           WHERE id = $${productsColumnSet.has('purchase_price') && receivedPurchasePrice !== null ? 3 : 2}
             AND tenant_id = $${productsColumnSet.has('purchase_price') && receivedPurchasePrice !== null ? 4 : 3}
           RETURNING *`
        : `UPDATE products
           SET ${productSetClauses.join(', ')}
           WHERE id = $${productsColumnSet.has('purchase_price') && receivedPurchasePrice !== null ? 3 : 2}
           RETURNING *`,
      productUpdateValues,
    );

    if ((productUpdateResult.rowCount || 0) === 0) {
      throw new Error(`Produk ${productId} tidak ditemukan saat restock penerimaan`);
    }

    productUpdate = productUpdateResult.rows[0] || null;
  }

  const updateClauses = [];
  const updateValues = [];
  const pushUpdate = (column, value) => {
    updateValues.push(value);
    updateClauses.push(`${column} = $${updateValues.length}`);
  };

  // Note: is_completed column does not exist in schema, only is_received
  if (orderItemsColumnSet.has('is_received')) {
    pushUpdate('is_received', true);
  }
  if (orderItemsColumnSet.has('received_qty')) {
    pushUpdate('received_qty', receivedQty);
  }
  if (orderItemsColumnSet.has('received_at')) {
    pushUpdate('received_at', new Date().toISOString());
  }
  if (orderItemsColumnSet.has('received_purchase_price')) {
    const receivedPurchasePrice =
      parsePositiveInt(payload.received_purchase_price) ??
      parsePositiveInt(payload.receivedPurchasePrice);
    if (receivedPurchasePrice !== null) {
      pushUpdate('received_purchase_price', receivedPurchasePrice);
    }
  }

  if (updateClauses.length === 0) {
    return { item: itemResult.rows[0] || null, productUpdate };
  }

  updateValues.push(itemId);
  const whereClauses = [`id = $${updateValues.length}`];
  if (hasItemsTenant) {
    updateValues.push(tenantId);
    whereClauses.push(`tenant_id = $${updateValues.length}`);
  }

  const updatedItemResult = await client.query(
    `UPDATE order_history_items
     SET ${updateClauses.join(', ')}
     WHERE ${whereClauses.join(' AND ')}
     RETURNING *`,
    updateValues,
  );

  const updatedItem = updatedItemResult.rows[0] || itemResult.rows[0] || null;

  emitTableMutation(req, {
    table: 'order_history_items',
    action: 'UPDATE',
    record: updatedItem,
    id: itemId,
  });

  if (productUpdate) {
    emitTableMutation(req, {
      table: 'products',
      action: 'UPDATE',
      record: productUpdate,
      id: productId,
    });
  }

  return {
    item: updatedItem,
    productUpdate,
  };
};

// GET /order_history/items
// Query params yang didukung:
//   ?eq__is_completed=true   → hanya item Sudah Selesai
//   ?eq__is_completed=false  → hanya item Belum Selesai
//   ?limit=N, ?orderBy=col, ?ascending=true/false, ?select=col1,col2
const getOrderHistoryItems = async (req, res) => {
  try {
    const tenantId = normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);
    const columnSet = await getTableColumnSet(req.tenantDb, 'order_history_items');
    const query = {
      ...req.query,
      select: ensureSelectedColumn(req.query?.select, 'id'),
    };
    if (!columnSet.has('is_completed')) {
      delete query.eq__is_completed;
      delete query.neq__is_completed;
      delete query.gte__is_completed;
      delete query.lte__is_completed;
      delete query.ilike__is_completed;
      delete query.in__is_completed;
      query.select = removeSelectedColumn(query.select, 'is_completed');
    }
    const rows = await runSelect(req.tenantDb, 'order_history_items', query, { tenantId });
    const normalizedRows = (rows || []).map((row) => {
      if (columnSet.has('is_completed')) {
        return row;
      }

      return {
        ...row,
        is_completed: normalizeCompletionStatus(row),
      };
    });
    return jsonOk(res, normalizedRows);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

const createOrderHistoryItems = async (req, res) => {
  try {
    const tenantId = normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);
    const cachedResponse = getCachedResponse(req, 'order_history_items');
    if (cachedResponse !== null) {
      return jsonOk(res, cachedResponse, 'Created (idempotent)', 200);
    }

    const payload = parseBodyArray(req.body) || parseBodyObject(req.body);
    const columnDefinitions = await getTableColumnDefinitions(req.tenantDb, 'order_history_items');
    const tenantScopedPayload = enforceTenantIdOnPayload(payload, tenantId, columnDefinitions);
    const filteredPayload = normalizePayloadByColumnDefinitions(tenantScopedPayload, columnDefinitions);
    const { sql, values } = buildInsertQuery('order_history_items', filteredPayload);
    const result = await req.tenantDb.query(sql, values);
    for (const row of result.rows) {
      emitTableMutation(req, {
        table: 'order_history_items',
        action: 'INSERT',
        record: row,
      });
    }
    storeResponse(req, 'order_history_items', result.rows);
    return jsonOk(res, result.rows, 'Created', 201);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

// PUT /order_history/items/:id/complete
// Tandai satu item sebagai Sudah Selesai (is_completed = true).
const completeOrderHistoryItem = async (req, res) => {
  const client = await req.tenantDb.connect();
  try {
    const { id } = req.params;
    const tenantId = normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);
    await ensureTenantScopedTable(client, 'order_history_items', tenantId);
    await ensureTenantScopedTable(client, 'products', tenantId);

    await client.query('BEGIN');

    const orderItemsColumnSet = await getTableColumnSet(client, 'order_history_items');
    const productsColumnSet = await getTableColumnSet(client, 'products');

    const result = await completeItemWithinTransaction({
      client,
      req,
      tenantId,
      itemId: id,
      payload: req.body || {},
      orderItemsColumnSet,
      productsColumnSet,
    });

    if (result.notFound) {
      await client.query('ROLLBACK');
      return jsonError(res, 404, 'Item tidak ditemukan');
    }

    await client.query('COMMIT');
    return jsonOk(res, result.item || null, 'Status diperbarui menjadi Selesai');
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  } finally {
    client.release();
  }
};

const completeOrderHistoryBatch = async (req, res) => {
  const client = await req.tenantDb.connect();
  try {
    const tenantId = normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);
    await ensureTenantScopedTable(client, 'order_history_items', tenantId);
    await ensureTenantScopedTable(client, 'products', tenantId);

    const itemIdsFromPayload = Array.isArray(req.body?.item_ids)
      ? req.body.item_ids.map((id) => (id ?? '').toString().trim()).filter((id) => id.length > 0)
      : [];
    const orderHistoryId = (req.body?.order_history_id ?? '').toString().trim();

    const orderItemsColumnSet = await getTableColumnSet(client, 'order_history_items');
    const productsColumnSet = await getTableColumnSet(client, 'products');
    const hasItemsTenant = orderItemsColumnSet.has('tenant_id');

    const targetItemIds = new Set(itemIdsFromPayload);
    if (orderHistoryId) {
      const relationColumn = orderItemsColumnSet.has('order_history_id')
        ? 'order_history_id'
        : (orderItemsColumnSet.has('order_id') ? 'order_id' : null);

      if (relationColumn) {
        const relatedItemsResult = await client.query(
          hasItemsTenant
            ? `SELECT id FROM order_history_items WHERE ${relationColumn} = $1 AND tenant_id = $2`
            : `SELECT id FROM order_history_items WHERE ${relationColumn} = $1`,
          hasItemsTenant ? [orderHistoryId, tenantId] : [orderHistoryId],
        );
        for (const row of relatedItemsResult.rows || []) {
          const id = (row.id ?? '').toString().trim();
          if (id) {
            targetItemIds.add(id);
          }
        }
      }
    }

    const finalItemIds = Array.from(targetItemIds);
    if (finalItemIds.length === 0) {
      return jsonError(res, 400, 'item_ids atau order_history_id wajib diisi');
    }

    await client.query('BEGIN');

    const completedItems = [];
    let incrementedProducts = 0;

    for (const itemId of finalItemIds) {
      const result = await completeItemWithinTransaction({
        client,
        req,
        tenantId,
        itemId,
        payload: req.body || {},
        orderItemsColumnSet,
        productsColumnSet,
      });
      if (!result.notFound && result.item) {
        completedItems.push(result.item);
      }
      if (result.productUpdate) {
        incrementedProducts += 1;
      }
    }

    await client.query('COMMIT');

    return jsonOk(
      res,
      {
        completed_count: completedItems.length,
        incremented_products: incrementedProducts,
        items: completedItems,
      },
      'Order history batch completed',
    );
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  } finally {
    client.release();
  }
};

module.exports = {
  getOrderHistoryItems,
  createOrderHistoryItems,
  completeOrderHistoryItem,
  completeOrderHistoryBatch,
};
