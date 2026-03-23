const { emitToTenant } = require('./socketServer');

const resolveTenantIdFromRequest = (req) => {
  return (req?.tenant?.tenantId ?? req?.auth?.tenantId ?? req?.auth?.tenant_id ?? '')
    .toString()
    .trim();
};

const toIsoNow = () => new Date().toISOString();

const toNumberOrNull = (value) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const normalizeRecord = (record) => {
  if (!record || typeof record !== 'object' || Array.isArray(record)) {
    return null;
  }

  return { ...record };
};

const emitInventoryUpdated = (req, productRecord, extra = {}) => {
  const tenantId = resolveTenantIdFromRequest(req);
  const product = normalizeRecord(productRecord);
  if (!tenantId || !product) {
    return;
  }

  const productId = (product.id ?? extra.productId ?? '').toString().trim();
  const newStock = toNumberOrNull(product.stock ?? extra.newStock);
  if (!productId || newStock === null) {
    return;
  }

  emitToTenant(tenantId, 'inventory_updated', {
    productId,
    newStock,
    product,
    reason: (extra.reason ?? '').toString().trim(),
    source: (extra.source ?? 'bridge').toString(),
    timestamp: toIsoNow(),
  });
};

const emitProductChanged = (req, action, productRecord) => {
  const tenantId = resolveTenantIdFromRequest(req);
  const product = normalizeRecord(productRecord);
  const productId = (product?.id ?? '').toString().trim();
  if (!tenantId || !productId) {
    return;
  }

  emitToTenant(tenantId, 'product_changed', {
    action,
    productId,
    product,
    timestamp: toIsoNow(),
  });

  emitInventoryUpdated(req, product, {
    reason: `product_${action.toLowerCase()}`,
    source: 'product_changed',
  });
};

const emitProductDeleted = (req, productId) => {
  const tenantId = resolveTenantIdFromRequest(req);
  const normalizedId = (productId ?? '').toString().trim();
  if (!tenantId || !normalizedId) {
    return;
  }

  emitToTenant(tenantId, 'product_changed', {
    action: 'DELETE',
    productId: normalizedId,
    timestamp: toIsoNow(),
  });
};

const emitCategoryChanged = (req, action, categoryRecord) => {
  const tenantId = resolveTenantIdFromRequest(req);
  const category = normalizeRecord(categoryRecord);
  const categoryId = (category?.id ?? '').toString().trim();
  if (!tenantId || !categoryId) {
    return;
  }

  emitToTenant(tenantId, 'category_changed', {
    action,
    categoryId,
    category,
    timestamp: toIsoNow(),
  });
};

const emitCategoryDeleted = (req, categoryId) => {
  const tenantId = resolveTenantIdFromRequest(req);
  const normalizedId = (categoryId ?? '').toString().trim();
  if (!tenantId || !normalizedId) {
    return;
  }

  emitToTenant(tenantId, 'category_changed', {
    action: 'DELETE',
    categoryId: normalizedId,
    timestamp: toIsoNow(),
  });
};

const emitTransactionCreated = (req, transactionRecord, extra = {}) => {
  const tenantId = resolveTenantIdFromRequest(req);
  const transaction = normalizeRecord(transactionRecord);
  if (!tenantId || !transaction) {
    return;
  }

  emitToTenant(tenantId, 'transaction_created', {
    transactionId: (transaction.id ?? extra.transactionId ?? '').toString(),
    receiptNumber: (transaction.receipt_number ?? extra.receiptNumber ?? '').toString(),
    totalPrice: toNumberOrNull(transaction.total_price ?? extra.totalPrice),
    paymentMethod: (transaction.payment_method ?? extra.paymentMethod ?? '').toString(),
    transaction,
    timestamp: toIsoNow(),
  });

  const inventoryUpdates = Array.isArray(extra.inventoryUpdates)
    ? extra.inventoryUpdates
    : [];
  for (const product of inventoryUpdates) {
    emitInventoryUpdated(req, product, {
      reason: 'transaction_created',
      source: 'transactions',
    });
  }
};

const emitTransactionUpdated = (req, transactionRecord, extra = {}) => {
  const tenantId = resolveTenantIdFromRequest(req);
  const transaction = normalizeRecord(transactionRecord);
  if (!tenantId || !transaction) {
    return;
  }

  emitToTenant(tenantId, 'transaction_updated', {
    transactionId: (transaction.id ?? extra.transactionId ?? '').toString(),
    transaction,
    timestamp: toIsoNow(),
  });
};

const emitTableMutation = (req, { table, action, record, id, extra = {} }) => {
  const normalizedTable = (table ?? '').toString().trim();
  const normalizedAction = (action ?? '').toString().trim().toUpperCase();
  const normalizedRecord = normalizeRecord(record);
  const recordId = (id ?? normalizedRecord?.id ?? '').toString().trim();

  if (normalizedTable === 'products') {
    if (normalizedAction === 'DELETE') {
      emitProductDeleted(req, recordId);
      return;
    }

    emitProductChanged(req, normalizedAction, normalizedRecord);
    return;
  }

  if (normalizedTable === 'categories') {
    if (normalizedAction === 'DELETE') {
      emitCategoryDeleted(req, recordId);
      return;
    }

    emitCategoryChanged(req, normalizedAction, normalizedRecord);
    return;
  }

  if (normalizedTable === 'sales_records') {
    if (normalizedAction === 'INSERT') {
      emitTransactionCreated(req, normalizedRecord, extra);
      return;
    }

    emitTransactionUpdated(req, normalizedRecord, extra);
  }
};

module.exports = {
  emitInventoryUpdated,
  emitProductChanged,
  emitTransactionCreated,
  emitTransactionUpdated,
  emitTableMutation,
};