const { emitToTenant } = require('./socketServer');

const resolveTenantIdFromRequest = (req) => {
  return (req?.user?.tenantId
    ?? req?.user?.tenant_id
    ?? req?.tenant?.tenantId
    ?? req?.auth?.tenantId
    ?? req?.auth?.tenant_id
    ?? '')
    .toString()
    .trim();
};

const resolveDeviceIdFromRequest = (req) => {
  const headerDeviceId = req?.headers?.['x-device-id'];
  if (typeof headerDeviceId === 'string' && headerDeviceId.trim()) {
    return headerDeviceId.trim();
  }

  return (req?.body?.deviceId ?? req?.body?.device_id ?? '')
    .toString()
    .trim();
};

const toIsoNow = () => new Date().toISOString();

const normalizeAction = (value) => (value ?? '').toString().trim().toUpperCase();

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

const emitDbMutation = (
  req,
  {
    type,
    entity,
    table,
    action,
    record,
    recordId,
    payload,
    meta = {},
  },
) => {
  const tenantId = resolveTenantIdFromRequest(req);
  const deviceId = resolveDeviceIdFromRequest(req);
  const normalizedPayload = normalizeRecord(payload ?? record) ?? {};
  const resolvedRecordId = (recordId ?? normalizedPayload.id ?? '').toString().trim();
  if (!tenantId || !type || !entity) {
    return;
  }

  emitToTenant(tenantId, 'db_mutation', {
    tenantId,
    type: type.toString().trim().toUpperCase(),
    entity,
    table: (table ?? entity).toString().trim(),
    action: normalizeAction(action),
    recordId: resolvedRecordId,
    deviceId,
    payload: normalizedPayload,
    meta: { ...meta, deviceId },
    timestamp: toIsoNow(),
  });
};

const mapProductMutationType = (action) => {
  switch (normalizeAction(action)) {
    case 'INSERT':
      return 'PRODUCT_ADDED';
    case 'DELETE':
      return 'PRODUCT_DELETED';
    default:
      return 'PRODUCT_UPDATED';
  }
};

const mapTransactionMutationType = (action) => {
  switch (normalizeAction(action)) {
    case 'INSERT':
      return 'TRANSACTION_ADDED';
    case 'DELETE':
      return 'TRANSACTION_DELETED';
    default:
      return 'TRANSACTION_UPDATED';
  }
};

const mapOrderHistoryMutationType = (table, action) => {
  const normalizedTable = (table ?? '').toString().trim().toLowerCase();
  const normalizedAction = normalizeAction(action);

  if (normalizedTable === 'order_history_items') {
    switch (normalizedAction) {
      case 'INSERT':
        return 'ORDER_HISTORY_ITEM_ADDED';
      case 'DELETE':
        return 'ORDER_HISTORY_ITEM_DELETED';
      default:
        return 'ORDER_HISTORY_ITEM_UPDATED';
    }
  }

  switch (normalizedAction) {
    case 'INSERT':
      return 'ORDER_HISTORY_BATCH_ADDED';
    case 'DELETE':
      return 'ORDER_HISTORY_BATCH_DELETED';
    default:
      return 'ORDER_HISTORY_BATCH_UPDATED';
  }
};

const mapGenericMutationType = (table, action) => {
  const normalizedTable = (table ?? '').toString().trim().toUpperCase();
  const normalizedAction = normalizeAction(action);
  const safePrefix = normalizedTable.replace(/[^A-Z0-9]+/g, '_');

  switch (normalizedAction) {
    case 'INSERT':
      return `${safePrefix}_ADDED`;
    case 'DELETE':
      return `${safePrefix}_DELETED`;
    default:
      return `${safePrefix}_UPDATED`;
  }
};

const emitInventoryUpdated = (req, productRecord, extra = {}) => {
  const tenantId = resolveTenantIdFromRequest(req);
  const deviceId = resolveDeviceIdFromRequest(req);
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
    deviceId,
    reason: (extra.reason ?? '').toString().trim(),
    source: (extra.source ?? 'bridge').toString(),
    timestamp: toIsoNow(),
  });

  if (extra.suppressDbMutation === true) {
    return;
  }

  emitDbMutation(req, {
    type: 'PRODUCT_UPDATED',
    entity: 'products',
    table: 'products',
    action: 'UPDATE',
    record: product,
    recordId: productId,
    payload: {
      ...product,
      stock: newStock,
    },
    meta: {
      reason: (extra.reason ?? '').toString().trim(),
      source: (extra.source ?? 'bridge').toString().trim(),
    },
  });
};

const emitProductChanged = (req, action, productRecord) => {
  const tenantId = resolveTenantIdFromRequest(req);
  const deviceId = resolveDeviceIdFromRequest(req);
  const product = normalizeRecord(productRecord);
  const productId = (product?.id ?? '').toString().trim();
  if (!tenantId || !productId) {
    return;
  }

  emitToTenant(tenantId, 'product_changed', {
    action,
    productId,
    product,
    deviceId,
    timestamp: toIsoNow(),
  });

  emitDbMutation(req, {
    type: mapProductMutationType(action),
    entity: 'products',
    table: 'products',
    action,
    record: product,
    recordId: productId,
  });

  emitInventoryUpdated(req, product, {
    reason: `product_${action.toLowerCase()}`,
    source: 'product_changed',
    suppressDbMutation: true,
  });
};

const emitProductDeleted = (req, productId) => {
  const tenantId = resolveTenantIdFromRequest(req);
  const deviceId = resolveDeviceIdFromRequest(req);
  const normalizedId = (productId ?? '').toString().trim();
  if (!tenantId || !normalizedId) {
    return;
  }

  emitToTenant(tenantId, 'product_changed', {
    action: 'DELETE',
    productId: normalizedId,
    deviceId,
    timestamp: toIsoNow(),
  });

  emitDbMutation(req, {
    type: 'PRODUCT_DELETED',
    entity: 'products',
    table: 'products',
    action: 'DELETE',
    recordId: normalizedId,
    payload: { id: normalizedId },
  });
};

const emitCategoryChanged = (req, action, categoryRecord) => {
  const tenantId = resolveTenantIdFromRequest(req);
  const deviceId = resolveDeviceIdFromRequest(req);
  const category = normalizeRecord(categoryRecord);
  const categoryId = (category?.id ?? '').toString().trim();
  if (!tenantId || !categoryId) {
    return;
  }

  emitToTenant(tenantId, 'category_changed', {
    action,
    categoryId,
    category,
    deviceId,
    timestamp: toIsoNow(),
  });

  emitDbMutation(req, {
    type: mapGenericMutationType('categories', action),
    entity: 'categories',
    table: 'categories',
    action,
    record: category,
    recordId: categoryId,
  });
};

const emitCategoryDeleted = (req, categoryId) => {
  const tenantId = resolveTenantIdFromRequest(req);
  const deviceId = resolveDeviceIdFromRequest(req);
  const normalizedId = (categoryId ?? '').toString().trim();
  if (!tenantId || !normalizedId) {
    return;
  }

  emitToTenant(tenantId, 'category_changed', {
    action: 'DELETE',
    categoryId: normalizedId,
    deviceId,
    timestamp: toIsoNow(),
  });

  emitDbMutation(req, {
    type: mapGenericMutationType('categories', 'DELETE'),
    entity: 'categories',
    table: 'categories',
    action: 'DELETE',
    recordId: normalizedId,
    payload: { id: normalizedId },
  });
};

const emitTransactionCreated = (req, transactionRecord, extra = {}) => {
  const tenantId = resolveTenantIdFromRequest(req);
  const deviceId = resolveDeviceIdFromRequest(req);
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
    deviceId,
    timestamp: toIsoNow(),
  });

  emitDbMutation(req, {
    type: 'TRANSACTION_ADDED',
    entity: 'transactions',
    table: 'sales_records',
    action: 'INSERT',
    record: transaction,
    recordId: transaction.id,
    meta: {
      receiptNumber: (transaction.receipt_number ?? extra.receiptNumber ?? '').toString(),
      paymentMethod: (transaction.payment_method ?? extra.paymentMethod ?? '').toString(),
      inventoryUpdatesCount: Array.isArray(extra.inventoryUpdates) ? extra.inventoryUpdates.length : 0,
    },
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

const emitKasBonCreated = (req, transactionRecord, extra = {}) => {
  const tenantId = resolveTenantIdFromRequest(req);
  const deviceId = resolveDeviceIdFromRequest(req);
  const transaction = normalizeRecord(transactionRecord);
  if (!tenantId || !transaction) {
    return;
  }

  emitToTenant(tenantId, 'kas_bon_created', {
    transactionId: (transaction.id ?? extra.transactionId ?? '').toString(),
    receiptNumber: (transaction.receipt_number ?? extra.receiptNumber ?? '').toString(),
    customerName: (transaction.customer_name ?? extra.customerName ?? '').toString(),
    totalPrice: toNumberOrNull(transaction.total_price ?? extra.totalPrice),
    remainingBalance: toNumberOrNull(
      transaction.remaining_balance ??
      transaction.outstanding_balance ??
      extra.remainingBalance,
    ),
    paymentStatus: (transaction.payment_status ?? extra.paymentStatus ?? '').toString(),
    transaction,
    deviceId,
    timestamp: toIsoNow(),
  });
};

const emitKasBonUpdated = (req, transactionRecord, extra = {}) => {
  const tenantId = resolveTenantIdFromRequest(req);
  const deviceId = resolveDeviceIdFromRequest(req);
  const transaction = normalizeRecord(transactionRecord);
  if (!tenantId || !transaction) {
    return;
  }

  emitToTenant(tenantId, 'kas_bon_updated', {
    id: (transaction.id ?? extra.transactionId ?? '').toString(),
    transactionId: (transaction.id ?? extra.transactionId ?? '').toString(),
    receiptNumber: (transaction.receipt_number ?? extra.receiptNumber ?? '').toString(),
    customerName: (transaction.customer_name ?? extra.customerName ?? '').toString(),
    status: (extra.status ?? transaction.payment_status ?? '').toString(),
    paymentStatus: (transaction.payment_status ?? extra.paymentStatus ?? extra.status ?? '').toString(),
    paidAmount: toNumberOrNull(extra.paidAmount),
    remainingBalance: toNumberOrNull(
      transaction.remaining_balance ??
      transaction.outstanding_balance ??
      extra.remainingBalance,
    ),
    transaction,
    deviceId,
    timestamp: toIsoNow(),
  });
};

const emitTransactionUpdated = (req, transactionRecord, extra = {}) => {
  const tenantId = resolveTenantIdFromRequest(req);
  const deviceId = resolveDeviceIdFromRequest(req);
  const transaction = normalizeRecord(transactionRecord);
  if (!tenantId || !transaction) {
    return;
  }

  emitToTenant(tenantId, 'transaction_updated', {
    transactionId: (transaction.id ?? extra.transactionId ?? '').toString(),
    transaction,
    deviceId,
    timestamp: toIsoNow(),
  });

  const mutationType = (extra.mutationType ?? mapTransactionMutationType(extra.action ?? 'UPDATE'))
    .toString()
    .trim()
    .toUpperCase();
  const payload = mutationType === 'KASBON_SETTLED'
    ? {
        transaction,
        paymentHistory: normalizeRecord(extra.paymentHistory),
        paidAmount: toNumberOrNull(extra.paidAmount),
        remainingBalance: toNumberOrNull(extra.remainingBalance),
        status: (extra.status ?? '').toString().trim(),
      }
    : transaction;

  emitDbMutation(req, {
    type: mutationType,
    entity: mutationType.startsWith('KASBON') ? 'kasbon' : 'transactions',
    table: 'sales_records',
    action: extra.action ?? 'UPDATE',
    record: transaction,
    recordId: transaction.id ?? extra.transactionId,
    payload,
  });
};

const emitPettyCashUpdated = (req, pettyCashRecord) => {
  const tenantId = resolveTenantIdFromRequest(req);
  const deviceId = resolveDeviceIdFromRequest(req);
  const log = normalizeRecord(pettyCashRecord);
  const recordId = (log?.id ?? '').toString().trim();
  if (!tenantId || !recordId || !log) {
    return;
  }

  emitToTenant(tenantId, 'petty_cash_updated', {
    tenantId,
    log,
    deviceId,
    timestamp: toIsoNow(),
  });

  emitDbMutation(req, {
    type: 'PETTY_CASH_UPDATED',
    entity: 'petty_cash',
    table: 'petty_cash_logs',
    action: 'INSERT',
    record: log,
    recordId,
    payload: { log },
  });
};

const emitProcurementCreated = (req, procurementRecord, extra = {}) => {
  const tenantId = resolveTenantIdFromRequest(req);
  const deviceId = resolveDeviceIdFromRequest(req);
  const procurement = normalizeRecord(procurementRecord);
  const recordId = (procurement?.id ?? extra.recordId ?? '').toString().trim();
  if (!tenantId || !recordId || !procurement) {
    return;
  }

  emitToTenant(tenantId, 'procurement_created', {
    tenantId,
    id: recordId,
    procurementId: recordId,
    procurement,
    deviceId,
    timestamp: toIsoNow(),
  });
};

const emitProcurementUpdated = (req, procurementRecord, extra = {}) => {
  const tenantId = resolveTenantIdFromRequest(req);
  const deviceId = resolveDeviceIdFromRequest(req);
  const procurement = normalizeRecord(procurementRecord) ?? normalizeRecord(extra.payload);
  const recordId = (
    procurement?.id ??
    extra.recordId ??
    procurement?.order_history_id ??
    procurement?.order_id ??
    ''
  )
    .toString()
    .trim();
  if (!tenantId || !recordId) {
    return;
  }

  emitToTenant(tenantId, 'procurement_updated', {
    tenantId,
    id: recordId,
    procurementId: recordId,
    procurement,
    table: (extra.table ?? '').toString().trim(),
    action: normalizeAction(extra.action),
    deviceId,
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
    return;
  }

  if (
    normalizedTable === 'order_history' ||
    normalizedTable === 'order_history_items'
  ) {
    if (normalizedTable === 'order_history' && normalizedAction === 'INSERT') {
      emitProcurementCreated(req, normalizedRecord, {
        recordId,
        action: normalizedAction,
        table: normalizedTable,
      });
    } else {
      emitProcurementUpdated(req, normalizedRecord, {
        recordId,
        action: normalizedAction,
        table: normalizedTable,
        payload: normalizedRecord,
      });
    }

    emitDbMutation(req, {
      type: mapOrderHistoryMutationType(normalizedTable, normalizedAction),
      entity: normalizedTable,
      table: normalizedTable,
      action: normalizedAction,
      record: normalizedRecord,
      recordId,
      meta: { ...extra },
    });
    return;
  }

  if (normalizedTable) {
    emitDbMutation(req, {
      type: mapGenericMutationType(normalizedTable, normalizedAction),
      entity: normalizedTable,
      table: normalizedTable,
      action: normalizedAction,
      record: normalizedRecord,
      recordId,
      payload:
          normalizedAction === 'DELETE' && !normalizedRecord
            ? { id: recordId }
            : normalizedRecord,
      meta: { ...extra },
    });
  }
};

module.exports = {
  emitDbMutation,
  emitInventoryUpdated,
  emitProductChanged,
  emitKasBonCreated,
  emitKasBonUpdated,
  emitTransactionCreated,
  emitTransactionUpdated,
  emitPettyCashUpdated,
  emitProcurementCreated,
  emitProcurementUpdated,
  emitTableMutation,
};