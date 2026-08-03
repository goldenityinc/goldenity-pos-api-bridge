const { jsonOk, jsonError } = require('../utils/http');
const http = require('http');
const https = require('https');
const crypto = require('crypto');
const {
  buildInsertQuery,
  getTableColumnDefinitions,
  getTableColumnSet,
  normalizePayloadByColumnDefinitions,
  enforceTenantIdOnPayload,
  normalizeTenantId,
} = require('../utils/sqlHelpers');
const { ensureTenantScopedTable } = require('../utils/tenantScope');
const {
  emitKasBonCreated,
  emitKasBonUpdated,
  emitTransactionCreated,
  emitTransactionUpdated,
} = require('../services/realtimeEmitter');
const {
  isTransientDbError,
  withRetries,
  getClientFromPool,
  runTransaction,
  storeFailedPayload,
  normalizePayloadCartItems,
  normalizeCartItemInPlace,
} = require('../utils/dbSafe');

const normalizePaymentType = (value) => (value || '').toString().trim().toUpperCase();

const VOID_TERMINAL_STATUS_SET = new Set([
  'VOID',
  'CANCELLED',
  'CANCELED',
  'BATAL',
  'DIBATALKAN',
]);

const normalizeStatusValue = (value) => (value || '').toString().trim().toUpperCase();

const isVoidedSalesRecord = (record) => {
  if (!record || typeof record !== 'object') {
    return false;
  }

  const statusCandidates = [
    record.status,
    record.order_status,
    record.orderStatus,
    record.transaction_status,
    record.transactionStatus,
  ];

  if (statusCandidates.some((value) => VOID_TERMINAL_STATUS_SET.has(normalizeStatusValue(value)))) {
    return true;
  }

  return record.is_void === true || record.isVoid === true;
};

const toNumber = (value) => {
  const numberValue = Number(value);
  return Number.isFinite(numberValue) ? numberValue : NaN;
};

const toPositiveInteger = (value) => {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return null;
  }
  return parsed;
};

const normalizeReferenceId = (payload = {}, fallbackValue = '') => {
  return (
    payload.reference_id ??
    payload.referenceId ??
    payload.transaction_id ??
    payload.transactionId ??
    payload.local_id ??
    payload.localId ??
    fallbackValue ??
    ''
  )
    .toString()
    .trim();
};

const resolveTenantIdFromRequest = (req) => normalizeTenantId(
  req?.user?.tenantId ||
  req?.user?.tenant_id ||
  req?.tenant?.tenantId ||
  req?.auth?.tenantId ||
  req?.auth?.tenant_id,
);

const mirrorCancelledTransactionToBridgeSalesRecord = async ({
  tenantDb,
  tenantId,
  transactionId,
  cancelledTransaction,
  fallbackVoidReason,
}) => {
  if (!tenantDb) {
    return;
  }

  const numericTransactionId = Number.parseInt((transactionId || '').toString(), 10);
  const receiptNumber = (
    cancelledTransaction?.receipt_number ?? cancelledTransaction?.receiptNumber ?? ''
  ).toString().trim();
  const referenceId = (
    cancelledTransaction?.reference_id ?? cancelledTransaction?.referenceId ?? ''
  ).toString().trim();
  const voidReasonRaw =
    cancelledTransaction?.void_reason ??
    cancelledTransaction?.voidReason ??
    fallbackVoidReason ??
    null;
  const voidReason = voidReasonRaw == null
    ? null
    : voidReasonRaw.toString().trim() || null;
  const nowIso = new Date().toISOString();

  await ensureTenantScopedTable(tenantDb, 'sales_records', tenantId);
  const columnSet = await getTableColumnSet(tenantDb, 'sales_records');

  const updateEntries = [];
  const addUpdate = (columnName, value) => {
    if (!columnSet.has(columnName)) {
      return;
    }
    updateEntries.push({ columnName, value });
  };

  addUpdate('status', 'CANCELLED');
  addUpdate('order_status', 'CANCELLED');
  addUpdate('transaction_status', 'VOID');
  addUpdate('is_void', true);
  addUpdate('isVoid', true);
  addUpdate('void_reason', voidReason);
  addUpdate('voided_at', nowIso);
  addUpdate('updated_at', nowIso);

  if (updateEntries.length === 0) {
    return;
  }

  const updateValues = [];
  const setClauses = updateEntries.map(({ columnName, value }, index) => {
    updateValues.push(value);
    return `${columnName} = $${index + 1}`;
  });

  const whereClauses = [];
  if (Number.isFinite(numericTransactionId) && numericTransactionId > 0) {
    updateValues.push(numericTransactionId);
    whereClauses.push(`id = $${updateValues.length}`);
  }
  if (receiptNumber && columnSet.has('receipt_number')) {
    updateValues.push(receiptNumber);
    whereClauses.push(`receipt_number = $${updateValues.length}`);
  }
  if (referenceId && columnSet.has('reference_id')) {
    updateValues.push(referenceId);
    whereClauses.push(`reference_id = $${updateValues.length}`);
  }

  if (whereClauses.length === 0) {
    return;
  }

  const hasTenantColumn = columnSet.has('tenant_id');
  const normalizedTenantId = (tenantId || '').toString().trim();
  const whereSql = whereClauses.join(' OR ');
  let sql = `UPDATE sales_records SET ${setClauses.join(', ')} WHERE (${whereSql})`;
  if (hasTenantColumn && normalizedTenantId) {
    updateValues.push(normalizedTenantId);
    sql += ` AND tenant_id = $${updateValues.length}`;
  }

  await tenantDb.query(sql, updateValues);
};

const assertColumnsExist = async (client, table, columns = []) => {
  const result = await client.query(
    `SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = ANY(current_schemas(false))
       AND table_name = $1`,
    [table],
  );
  const existingColumns = new Set((result.rows || []).map((row) => row.column_name));
  const missingColumns = columns.filter((column) => !existingColumns.has(column));
  if (missingColumns.length > 0) {
    throw new Error(
      `Schema guard: tabel ${table} belum memiliki kolom wajib: ${missingColumns.join(', ')}. Jalankan migrasi di core service.`,
    );
  }
};

const ensureSalesRecordsReferenceIdColumn = async (client) => {
  await assertColumnsExist(client, 'sales_records', ['reference_id']);
};

const ensureSalesRecordsReceiptNumberColumn = async (client) => {
  await assertColumnsExist(client, 'sales_records', ['receipt_number']);
};

const ensureSalesRecordsCashierColumns = async (client) => {
  await assertColumnsExist(client, 'sales_records', ['cashier_id', 'cashier_name']);
};

const ensureSalesRecordsNoteColumns = async (client) => {
  await client.query(
    `ALTER TABLE sales_records
       ADD COLUMN IF NOT EXISTS cashier_note TEXT,
       ADD COLUMN IF NOT EXISTS order_note TEXT`,
  );
};

const ensureSalesRecordsCustomerColumn = async (client) => {
  await assertColumnsExist(client, 'sales_records', ['customer_name']);
};

const ensureSalesRecordsKasBonColumns = async (client) => {
  await client.query(
    `ALTER TABLE sales_records
       ADD COLUMN IF NOT EXISTS remaining_balance NUMERIC,
       ADD COLUMN IF NOT EXISTS outstanding_balance NUMERIC,
       ADD COLUMN IF NOT EXISTS amount_paid NUMERIC DEFAULT 0`,
  );
  await assertColumnsExist(client, 'sales_records', [
    'payment_method',
    'payment_status',
    'remaining_balance',
    'outstanding_balance',
    'amount_paid',
  ]);
};

const ensureSalesRecordsFinancialColumns = async (client) => {
  await client.query(
    `ALTER TABLE sales_records
       ADD COLUMN IF NOT EXISTS total_discount BIGINT,
       ADD COLUMN IF NOT EXISTS total_tax BIGINT,
       ADD COLUMN IF NOT EXISTS total_profit BIGINT`,
  );
};

const hasMeaningfulValue = (value) => {
  if (value === null || value === undefined) {
    return false;
  }

  if (typeof value === 'string') {
    return value.trim().length > 0;
  }

  return true;
};

const normalizeTransactionItems = (items) => {
  if (!Array.isArray(items)) {
    return [];
  }

  return items
    .map((item) => {
      if (!item || typeof item !== 'object') {
        return null;
      }

      const product = item.product && typeof item.product === 'object'
        ? item.product
        : {};
      const productId = (
        product.id ?? product.product_id ?? product.productId ??
        item.product_id ?? item.productId ?? ''
      ).toString().trim();
      const productName = (
        product.name ?? product.product_name ?? product.productName ??
        item.product_name ?? item.productName ?? ''
      ).toString().trim();
      const isService =
        product.is_service === true ||
        product.isService === true ||
        product.is_custom_item === true ||
        product.isCustomItem === true ||
        item.is_service === true ||
        item.isService === true ||
        item.is_custom_item === true ||
        item.isCustomItem === true;
      const qty = toPositiveInteger(
        item.qty ?? item.quantity ??
        product.qty ?? product.quantity
      );
      if (qty === null) {
        return null;
      }

      if (!productId && !isService) {
        return null;
      }

      return {
        productId,
        productName,
        qty,
        isService,
      };
    })
    .filter(Boolean);
};

const normalizeTransactionItemsWithNotes = (items) => {
  if (!Array.isArray(items)) {
    return [];
  }

  return items
    .map((item) => {
      if (!item || typeof item !== 'object') {
        return null;
      }

      const product = item.product && typeof item.product === 'object'
        ? item.product
        : {};
      const productId = (
        product.id ?? product.product_id ?? product.productId ??
        item.product_id ?? item.productId ?? ''
      ).toString().trim();
      const productName = (
        product.name ?? product.product_name ?? product.productName ??
        item.product_name ?? item.productName ?? ''
      ).toString().trim();
      const note = (
        product.note ??
        item.note ??
        item.item_note ??
        item.product_note ??
        item.notes ??
        item.remark ??
        item.remarks ??
        ''
      )
        .toString()
        .trim();
      const qty = toPositiveInteger(
        item.qty ?? item.quantity ??
        product.qty ?? product.quantity
      );
      if (qty === null) {
        return null;
      }
      let customPrice = toNumber(
        item.custom_price ??
        item.customPrice ??
        product.price ??
        product.unit_price ??
        product.unitPrice ??
        product.sale_price ??
        product.harga_jual ??
        item.price ??
        item.unit_price ??
        item.unitPrice ??
        item.service_item_price ??
        item.harga_jual ??
        item?.product?.harga_jual ??
        0,
      );
      if (!Number.isFinite(customPrice)) {
        const lineTotal = toNumber(
          item.line_total ?? item.lineTotal ?? item.total_price ?? item.total ?? item.subtotal,
        );
        if (Number.isFinite(lineTotal) && qty > 0) {
          customPrice = lineTotal / qty;
        }
      }

      const isService =
        product.is_service === true ||
        product.isService === true ||
        product.is_custom_item === true ||
        product.isCustomItem === true ||
        item.is_service === true ||
        item.isService === true ||
        item.is_custom_item === true ||
        item.isCustomItem === true;

      const mechanicId = (
        product.mechanic_id ??
        product.mechanicId ??
        item.mechanic_id ??
        item.mechanicId ??
        item.employee_id ??
        item.employeeId ??
        item.user_id ??
        item.userId ??
        null
      );
      const normalizedMechanicId = mechanicId !== null && mechanicId !== undefined
        ? mechanicId.toString().trim() || null
        : null;

      if (!productId && !isService) {
        return null;
      }

      return {
        productId,
        productName,
        qty,
        customPrice: Number.isFinite(customPrice) && customPrice > 0 ? customPrice : undefined,
        note,
        isService,
        mechanicId: normalizedMechanicId,
      };
    })
    .filter(Boolean);
};

const ensureSalesRecordsItemsColumn = async (client) => {
  await assertColumnsExist(client, 'sales_records', ['items_json']);
};

const ensureSalesRecordItemsTable = async (client) => {
  await assertColumnsExist(client, 'sales_record_items', [
    'sales_record_id',
    'tenant_id',
    'product_id',
    'qty',
  ]);
};

const ensureSalesRecordItemsMechanicIdColumn = async (client) => {
  await client.query(
    `ALTER TABLE sales_record_items
       ADD COLUMN IF NOT EXISTS mechanic_id TEXT`,
  );
};

const toStoredSalesRecordItems = (items) => {
  return normalizeTransactionItemsWithNotes(items).map((item) => ({
    product_id: item.productId || null,
    product_name: item.productName || null,
    qty: item.qty,
    custom_price: Number.isFinite(item.customPrice) ? item.customPrice : null,
    note: item.note || null,
    is_service: item.isService === true,
    mechanic_id: item.mechanicId || null,
  }));
};

const parseItemsFromJsonField = (value) => {
  if (!value) {
    return [];
  }

  if (Array.isArray(value)) {
    return value;
  }

  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) {
      return [];
    }
    try {
      const parsed = JSON.parse(trimmed);
      return Array.isArray(parsed) ? parsed : [];
    } catch (_) {
      return [];
    }
  }

  return [];
};

const loadSalesRecordItems = async (client, salesRecordId, tenantId = '') => {
  const normalizedTenantId = normalizeTenantId(tenantId);
  try {
    await ensureSalesRecordItemsTable(client);
    const itemsColumnsResult = await client.query(
      `SELECT column_name
       FROM information_schema.columns
       WHERE table_schema = ANY(current_schemas(false))
         AND table_name = 'sales_record_items'`,
    );
    const itemsColumnSet = new Set((itemsColumnsResult.rows || []).map((r) => r.column_name));
    const hasMechanicId = itemsColumnSet.has('mechanic_id');
    const selectClause = `product_id, product_name, qty, custom_price, note, is_service${hasMechanicId ? ', mechanic_id' : ''}`;
    const result = await client.query(
      normalizedTenantId
        ? `SELECT ${selectClause}
           FROM sales_record_items
           WHERE sales_record_id = $1
             AND tenant_id = $2
           ORDER BY id ASC`
        : `SELECT ${selectClause}
           FROM sales_record_items
           WHERE sales_record_id = $1
           ORDER BY id ASC`,
      normalizedTenantId ? [salesRecordId, normalizedTenantId] : [salesRecordId],
    );

    return (result.rows || []).map((row) => ({
      product_id: row.product_id || null,
      product_name: row.product_name || '',
      qty: Number(row.qty || 0),
      custom_price: row.custom_price === null ? null : Number(row.custom_price),
      note: (row.note || '').toString(),
      is_service: row.is_service === true,
      mechanic_id: row.mechanic_id ?? null,
    }));
  } catch (_) {
    return [];
  }
};

const syncSalesRecordItems = async (client, salesRecordId, tenantId, items) => {
  const normalizedTenantId = normalizeTenantId(tenantId);
  if (!Number.isFinite(Number(salesRecordId))) {
    return;
  }

  await ensureSalesRecordItemsTable(client);

  await client.query(
    normalizedTenantId
      ? 'DELETE FROM sales_record_items WHERE sales_record_id = $1 AND tenant_id = $2'
      : 'DELETE FROM sales_record_items WHERE sales_record_id = $1',
    normalizedTenantId ? [salesRecordId, normalizedTenantId] : [salesRecordId],
  );

  if (!Array.isArray(items) || items.length === 0) {
    return;
  }

  const syncColumnsResult = await client.query(
    `SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = ANY(current_schemas(false))
       AND table_name = 'sales_record_items'`,
  );
  const syncColumnSet = new Set((syncColumnsResult.rows || []).map((r) => r.column_name));
  const syncHasMechanicId = syncColumnSet.has('mechanic_id');

  for (const item of items) {
    if (syncHasMechanicId) {
      await client.query(
        `INSERT INTO sales_record_items (
           tenant_id,
           sales_record_id,
           product_id,
           product_name,
           qty,
           custom_price,
           note,
           is_service,
           mechanic_id,
           updated_at
         ) VALUES ($1::text, $2::bigint, $3::text, $4::text, $5::integer, $6::numeric, $7::text, $8::boolean, $9::text, NOW())`,
        [
          normalizedTenantId || '',
          Number.isFinite(Number(salesRecordId)) ? Number(salesRecordId) : 0,
          (item.product_id || '').toString().trim() || null,
          (item.product_name || '').toString().trim() || null,
          Number.isInteger(Number(item.qty)) ? Number(item.qty) : 1,
          Number.isFinite(Number(item.custom_price)) ? Number(item.custom_price) : null,
          (item.note || '').toString().trim() || null,
          item.is_service === true,
          (item.mechanic_id || '').toString().trim() || null,
        ],
      );
    } else {
      await client.query(
        `INSERT INTO sales_record_items (
           tenant_id,
           sales_record_id,
           product_id,
           product_name,
           qty,
           custom_price,
           note,
           is_service,
           updated_at
         ) VALUES ($1::text, $2::bigint, $3::text, $4::text, $5::integer, $6::numeric, $7::text, $8::boolean, NOW())`,
        [
          normalizedTenantId || '',
          Number.isFinite(Number(salesRecordId)) ? Number(salesRecordId) : 0,
          (item.product_id || '').toString().trim() || null,
          (item.product_name || '').toString().trim() || null,
          Number.isInteger(Number(item.qty)) ? Number(item.qty) : 1,
          Number.isFinite(Number(item.custom_price)) ? Number(item.custom_price) : null,
          (item.note || '').toString().trim() || null,
          item.is_service === true,
        ],
      );
    }
  }
};

const ensureKasBonHistoryTable = async (client) => {
  await client.query(
    `ALTER TABLE kas_bon_payment_history
       ADD COLUMN IF NOT EXISTS payment_method TEXT,
       ADD COLUMN IF NOT EXISTS paid_at TIMESTAMPTZ DEFAULT NOW(),
       ADD COLUMN IF NOT EXISTS note TEXT`,
  );
  await assertColumnsExist(client, 'kas_bon_payment_history', [
    'sales_record_id',
    'tenant_id',
    'paid_amount',
    'remaining_balance',
    'payment_method',
    'paid_at',
    'note',
  ]);
};

const listActiveKasBon = async (req, res) => {
  const client = await req.tenantDb.connect();

  try {
    const tenantId = resolveTenantIdFromRequest(req);
    await ensureTenantScopedTable(client, 'sales_records', tenantId);
    await ensureSalesRecordsCustomerColumn(client);
    const columnsResult = await client.query(
      `SELECT column_name
       FROM information_schema.columns
       WHERE table_schema = ANY(current_schemas(false))
         AND table_name = 'sales_records'`,
    );
    const columns = new Set(columnsResult.rows.map((row) => row.column_name));

    const paymentColumn = columns.has('payment_method')
      ? 'payment_method'
      : (columns.has('payment_type') ? 'payment_type' : null);
    const amountColumn = columns.has('total_price')
      ? 'total_price'
      : (columns.has('total_amount') ? 'total_amount' : null);
    const remainingColumn = columns.has('remaining_balance')
      ? 'remaining_balance'
      : (columns.has('outstanding_balance') ? 'outstanding_balance' : null);

    if (!paymentColumn || !amountColumn) {
      return jsonError(res, 500, 'Kolom kas bon sales_records tidak lengkap');
    }

    await ensureKasBonHistoryTable(client);

    const statusFilter = (req.query?.status || 'all').toString().trim().toLowerCase();

    const normalizedPaymentExpression = `REPLACE(REPLACE(REPLACE(UPPER(COALESCE(${paymentColumn}::text, '')), ' ', ''), '-', ''), '_', '')`;
    const filters = [
      `(${normalizedPaymentExpression} = 'KASBON' OR ${normalizedPaymentExpression} LIKE 'LUNAS%' OR kh.sales_record_id IS NOT NULL)`,
    ];
    filters.push('tenant_id = $1');
    // Only use payment_status column if it explicitly exists
    const paymentStatusColumn = columns.has('payment_status') ? 'payment_status' : null;
    if (paymentStatusColumn) {
      const normalizedStatusExpression = `REPLACE(REPLACE(REPLACE(UPPER(COALESCE(${paymentStatusColumn}::text, 'BELUM LUNAS')), ' ', ''), '-', ''), '_', '')`;
      filters.push(`${normalizedStatusExpression} IN ('BELUMLUNAS', 'UNPAID', 'PARTIAL', 'PARTIALLYPAID', 'OPEN', 'PENDING', '')`);
    }

    const balanceExpression = remainingColumn
      ? `COALESCE(${remainingColumn}, ${amountColumn}, 0)`
      : `COALESCE(${amountColumn}, 0)`;
    const amountPaidExpression = `COALESCE(sr.amount_paid, kh.total_paid, GREATEST(COALESCE(sr.${amountColumn}, 0) - ${balanceExpression}, 0), 0)`;

    if (statusFilter === 'unpaid' || statusFilter === 'belum-bayar') {
      filters.push(`${balanceExpression} > 0`);
      filters.push(`${amountPaidExpression} <= 0`);
    } else if (statusFilter === 'partial' || statusFilter === 'bayar-sebagian') {
      filters.push(`${balanceExpression} > 0`);
      filters.push(`${amountPaidExpression} > 0`);
    } else if (statusFilter === 'paid' || statusFilter === 'lunas') {
      filters.push(`(${balanceExpression} <= 0 OR UPPER(COALESCE(sr.payment_status::text, '')) = 'LUNAS')`);
    }

    const orderColumn = columns.has('created_at') ? 'created_at' : 'id';
    const rowsResult = await client.query(
      `SELECT sr.*,
              kh.total_paid AS history_paid_amount,
              ${amountPaidExpression} AS computed_amount_paid
       FROM sales_records sr
       LEFT JOIN (
         SELECT sales_record_id, SUM(paid_amount) AS total_paid
         FROM kas_bon_payment_history
         WHERE tenant_id = $1
         GROUP BY sales_record_id
       ) kh ON kh.sales_record_id = sr.id
       WHERE ${filters.join(' AND ')}
       ORDER BY sr.${orderColumn} DESC`,
      columns.has('tenant_id') ? [tenantId] : [],
    );

    const normalizedRows = (rowsResult.rows || []).map((row) => {
      const totalAmount = toNumber(row[amountColumn] ?? row.total_price ?? row.total_amount);
      const remainingBalance = toNumber(
        row[remainingColumn] ?? row.remaining_balance ?? row.outstanding_balance ?? row[amountColumn],
      );
      const amountPaid = toNumber(
        row.amount_paid ?? row.computed_amount_paid ?? row.history_paid_amount,
      );

      const normalizedRemaining = Number.isFinite(remainingBalance) ? remainingBalance : 0;
      const normalizedPaid = Number.isFinite(amountPaid)
        ? amountPaid
        : Math.max(0, (Number.isFinite(totalAmount) ? totalAmount : 0) - normalizedRemaining);

      const normalizedStatus = (
        row[paymentStatusColumn || 'payment_status'] ?? row.payment_status ?? ''
      ).toString().trim();

      const resolvedStatus = normalizedRemaining <= 0
        ? 'LUNAS'
        : (normalizedPaid > 0 ? 'BELUM LUNAS (DICICIL)' : (normalizedStatus || 'BELUM LUNAS'));

      return {
        ...row,
        total_price: Number.isFinite(totalAmount) ? totalAmount : 0,
        total_amount: Number.isFinite(totalAmount) ? totalAmount : 0,
        remaining_balance: normalizedRemaining,
        outstanding_balance: normalizedRemaining,
        amount_paid: normalizedPaid,
        payment_status: resolvedStatus,
        payment_status_label: resolvedStatus,
      };
    });

    const rowRecency = (row) => {
      const updatedAtRaw = row.updated_at ?? row.updatedAt ?? row.created_at ?? row.createdAt;
      const updatedAtMs = Date.parse((updatedAtRaw ?? '').toString());
      if (Number.isFinite(updatedAtMs)) {
        return updatedAtMs;
      }
      const numericId = Number(row.id);
      return Number.isFinite(numericId) ? numericId : 0;
    };

    const sortedRows = [...normalizedRows].sort((a, b) => rowRecency(b) - rowRecency(a));
    const shouldCanonicalizeActiveRows = statusFilter !== 'paid' && statusFilter !== 'lunas';

    if (!shouldCanonicalizeActiveRows) {
      return jsonOk(res, sortedRows, 'Kas bon aktif berhasil dimuat');
    }

    const selectedCustomerKeys = new Set();
    const canonicalRows = [];

    for (const row of sortedRows) {
      const remainingBalance = toNumber(row.remaining_balance ?? row.outstanding_balance ?? 0);
      const normalizedRemainingBalance = Number.isFinite(remainingBalance) ? remainingBalance : 0;
      const statusToken = (row.payment_status ?? '').toString().trim().toUpperCase();
      const isOpenKasBon = normalizedRemainingBalance > 0 && statusToken !== 'LUNAS';

      if (!isOpenKasBon) {
        canonicalRows.push(row);
        continue;
      }

      const customerId = (row.customer_id ?? row.customerId ?? '').toString().trim();
      const customerName = (row.customer_name ?? row.customerName ?? '')
        .toString()
        .trim()
        .toLowerCase();
      const customerKey = customerId
        ? `customer_id:${customerId}`
        : (customerName ? `customer_name:${customerName}` : `row:${row.id ?? row.receipt_number ?? canonicalRows.length}`);

      if (selectedCustomerKeys.has(customerKey)) {
        continue;
      }

      selectedCustomerKeys.add(customerKey);
      canonicalRows.push(row);
    }

    return jsonOk(res, canonicalRows, 'Kas bon aktif berhasil dimuat');
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  } finally {
    client.release();
  }
};

const createTransaction = async (req, res) => {
  try { normalizePayloadCartItems(req); } catch (_) {}
  const requestId = `${Date.now()}-${crypto.randomBytes(4).toString('hex')}`;
  try {
    const incomingLogPayload = (req.body && typeof req.body === 'object') ? { ...req.body } : null;
    if (incomingLogPayload && Array.isArray(incomingLogPayload.items)) {
      for (let i = 0; i < incomingLogPayload.items.length; i += 1) normalizeCartItemInPlace(incomingLogPayload.items[i]);
    }
    const method = (req.method || 'POST').toUpperCase();
    const url = req.originalUrl || req.url || '/transactions';
    const tenantId = resolveTenantIdFromRequest(req);
    const summary = {
      requestId,
      method,
      url,
      tenant: tenantId || null,
      table: incomingLogPayload?.table_id ?? incomingLogPayload?.tableId ?? null,
      itemsCount: Array.isArray(incomingLogPayload?.items) ? incomingLogPayload.items.length : 0,
      totalAmount: incomingLogPayload?.total_amount ?? incomingLogPayload?.totalAmount ?? incomingLogPayload?.total ?? incomingLogPayload?.subtotal ?? null,
      firstItems: Array.isArray(incomingLogPayload?.items) ? (incomingLogPayload.items.slice(0, 3).map((it) => ({
        id: it?.id ?? null,
        productId: it?.productId ?? it?.product_id ?? null,
        nestedProductId: it?.product?.id ?? null,
        qty: it?.qty ?? it?.quantity ?? null,
        price: it?.price ?? it?.unit_price ?? it?.custom_price ?? it?.product?.price ?? null,
        name: it?.name ?? it?.product_name ?? it?.productName ?? it?.product?.name ?? null,
      }))) : [],
    };
    console.log(`[TRANSACTION ENTRY ${requestId}] ${method} ${url} payload=`, JSON.stringify(summary));
    if (incomingLogPayload && Array.isArray(incomingLogPayload.items)) {
      console.log(`[TRANSACTION ENTRY ${requestId}] RAW ITEMS JSON=`, JSON.stringify(incomingLogPayload.items).slice(0, 3500));
    }
  } catch (_) {}
  const incomingBody = (req.body && typeof req.body === 'object')
    ? { ...req.body }
    : {};
  let referenceId = '';
  let tenantIdForLog = resolveTenantIdFromRequest(req);
  let hasSalesTenantColumn = false;

  const runTransactionCore = async () => {
    const client = await getClientFromPool(req.tenantDb);
    let referenceIdLocal = '';
    let tenantIdLocal = '';
    let hasSalesTenantColumnLocal = false;
    try {
      tenantIdLocal = resolveTenantIdFromRequest(req);
      tenantIdForLog = tenantIdForLog || tenantIdLocal;
      const payload = { ...incomingBody };
      const clientProvidedId = typeof payload.id === 'string'
        ? payload.id.trim()
        : '';
      const existingTransactionIdToUpdate = (
        payload.existing_transaction_id ?? payload.existingTransactionId ?? ''
      )
        .toString()
        .trim();
      const existingReceiptNumberToUpdate = (
        payload.existing_receipt_number ?? payload.existingReceiptNumber ?? ''
      )
        .toString()
        .trim();
      if (clientProvidedId) {
        delete payload.id;
      }
      delete payload.existing_transaction_id;
      delete payload.existingTransactionId;
      delete payload.existing_receipt_number;
      delete payload.existingReceiptNumber;
      referenceIdLocal = normalizeReferenceId(payload, clientProvidedId);
      referenceId = referenceIdLocal;
      if (referenceIdLocal) {
        payload.reference_id = referenceIdLocal;
      }
      const isKasBonTransaction = normalizePaymentType(
        payload.payment_method ?? payload.payment_type,
      ) === 'KAS BON';
      if (isKasBonTransaction) {
        if (!hasMeaningfulValue(payload.payment_status)) {
          payload.payment_status = 'Belum Lunas';
        }

        const resolvedBalance = toNumber(
          payload.remaining_balance ??
          payload.outstanding_balance ??
          payload.total_price ??
          payload.total_amount,
        );
        if (Number.isFinite(resolvedBalance)) {
          if (!hasMeaningfulValue(payload.remaining_balance)) {
            payload.remaining_balance = resolvedBalance;
          }
          if (!hasMeaningfulValue(payload.outstanding_balance)) {
            payload.outstanding_balance = resolvedBalance;
          }
        }
      }
      const inventoryUpdates = [];
      const inputItems = Array.isArray(payload.items) && payload.items.length > 0
        ? payload.items
        : payload.orderItems;
      const transactionItems = normalizeTransactionItems(inputItems);
      const storedItems = toStoredSalesRecordItems(inputItems);

      let committedResponse = null;

      await runTransaction(
        client,
        async (txClient) => {
          await ensureSalesRecordsReferenceIdColumn(txClient);
          await ensureSalesRecordsReceiptNumberColumn(txClient);
          await ensureSalesRecordsCashierColumns(txClient);
          await ensureSalesRecordsCustomerColumn(txClient);
          await ensureSalesRecordsKasBonColumns(txClient);
          await ensureSalesRecordsFinancialColumns(txClient);
          await ensureSalesRecordsItemsColumn(txClient);
          await ensureSalesRecordsNoteColumns(txClient);
          await ensureSalesRecordItemsTable(txClient);
          await ensureSalesRecordItemsMechanicIdColumn(txClient);
          await ensureTenantScopedTable(txClient, 'sales_records', tenantIdLocal);
          await ensureTenantScopedTable(txClient, 'products', tenantIdLocal);
          const salesRecordColumnDefinitions = await getTableColumnDefinitions(txClient, 'sales_records');
          const salesRecordColumnSet = new Set(salesRecordColumnDefinitions.keys());
          hasSalesTenantColumnLocal = salesRecordColumnSet.has('tenant_id');
          hasSalesTenantColumn = hasSalesTenantColumnLocal;
          const productsColumnSet = await getTableColumnSet(txClient, 'products');
          const hasProductsTenantColumn = productsColumnSet.has('tenant_id');
          const productTracksStockCol = productsColumnSet.has('is_stock_tracked')
            ? 'is_stock_tracked'
            : productsColumnSet.has('stock_tracked')
              ? 'stock_tracked'
              : productsColumnSet.has('is_stock')
                ? 'is_stock'
                : null;
          const productIsServiceCol = productsColumnSet.has('is_service')
            ? 'is_service'
            : productsColumnSet.has('is_custom_item')
              ? 'is_custom_item'
              : null;

          if (existingTransactionIdToUpdate || existingReceiptNumberToUpdate) {
            const existingRecordResult = await txClient.query(
              existingTransactionIdToUpdate
                ? (hasSalesTenantColumnLocal
                  ? 'SELECT * FROM sales_records WHERE id = $1 AND tenant_id = $2 LIMIT 1 FOR UPDATE'
                  : 'SELECT * FROM sales_records WHERE id = $1 LIMIT 1 FOR UPDATE')
                : (hasSalesTenantColumnLocal
                  ? 'SELECT * FROM sales_records WHERE receipt_number = $1 AND tenant_id = $2 LIMIT 1 FOR UPDATE'
                  : 'SELECT * FROM sales_records WHERE receipt_number = $1 LIMIT 1 FOR UPDATE'),
              hasSalesTenantColumnLocal
                ? [
                  existingTransactionIdToUpdate || existingReceiptNumberToUpdate,
                  tenantIdLocal,
                ]
                : [existingTransactionIdToUpdate || existingReceiptNumberToUpdate],
            );

            if ((existingRecordResult.rowCount || 0) === 0) {
              const error = new Error('Transaksi target untuk update tidak ditemukan');
              error.statusCode = 404;
              throw error;
            }

            const existingRecord = existingRecordResult.rows[0] || null;
            const existingSalesRecordId = existingRecord?.id;
            if (!existingSalesRecordId) {
              const error = new Error('ID transaksi target tidak valid');
              error.statusCode = 400;
              throw error;
            }

            if (isVoidedSalesRecord(existingRecord)) {
              const preservedTransaction = existingRecord;
              if (preservedTransaction) {
                const hydratedItems = await loadSalesRecordItems(
                  req.tenantDb,
                  preservedTransaction.id,
                  tenantIdLocal,
                );
                preservedTransaction.items = hydratedItems.length > 0
                  ? hydratedItems
                  : parseItemsFromJsonField(preservedTransaction.items_json);
              }

              committedResponse = {
                body: preservedTransaction,
                message: 'Transaction already cancelled; terminal status preserved',
                status: 200,
                emit: () => {
                  emitTransactionUpdated(req, preservedTransaction, {
                    transactionId: existingSalesRecordId,
                    action: 'UPDATE',
                    mutationType: 'CHECKOUT_UPDATE_SKIPPED_ALREADY_CANCELLED',
                  });
                },
              };
              return;
            }

            const tenantScopedPayload = enforceTenantIdOnPayload(
              payload,
              tenantIdLocal,
              salesRecordColumnDefinitions,
            );
            const filteredPayload = normalizePayloadByColumnDefinitions(
              tenantScopedPayload,
              salesRecordColumnDefinitions,
            );
            delete filteredPayload.id;

            if (storedItems.length > 0) {
              filteredPayload.items_json = JSON.stringify(storedItems);
            }

            const updateEntries = Object.entries(filteredPayload).filter(
              ([, value]) => value !== undefined,
            );

            let updatedRow = existingRecord;
            if (updateEntries.length > 0) {
              const setClause = updateEntries
                .map(([column], index) => `${column} = $${index + 1}`)
                .join(', ');
              const updateValues = updateEntries.map(([, value]) => (
                value === undefined ? null : value
              ));
              const targetParam = updateValues.length + 1;
              const tenantParam = updateValues.length + 2;
              const updateSql = `UPDATE sales_records
                                 SET ${setClause}
                                 WHERE id = $${targetParam}
                                 ${hasSalesTenantColumnLocal ? `AND tenant_id = $${tenantParam}` : ''}
                                 RETURNING *`;
              const updateResult = await txClient.query(
                updateSql,
                hasSalesTenantColumnLocal
                  ? [...updateValues, existingSalesRecordId, tenantIdLocal]
                  : [...updateValues, existingSalesRecordId],
              );
              updatedRow = updateResult.rows?.[0] || existingRecord;
            }

            await syncSalesRecordItems(txClient, existingSalesRecordId, tenantIdLocal, storedItems);

            const savedTransaction = updatedRow || null;
            if (savedTransaction) {
              const hydratedItems = await loadSalesRecordItems(
                req.tenantDb,
                savedTransaction.id,
                tenantIdLocal,
              );
              savedTransaction.items = hydratedItems.length > 0
                ? hydratedItems
                : parseItemsFromJsonField(savedTransaction.items_json);
            }

            committedResponse = {
              body: savedTransaction,
              message: 'Transaction updated',
              status: 200,
              emit: () => {
                emitTransactionUpdated(req, savedTransaction, {
                  transactionId: existingSalesRecordId,
                  action: 'UPDATE',
                  mutationType: 'CHECKOUT_UPDATE_EXISTING',
                });
                if (isKasBonTransaction) {
                  emitKasBonUpdated(req, savedTransaction, {
                    transactionId: existingSalesRecordId,
                    paymentStatus: payload.payment_status,
                    remainingBalance: payload.remaining_balance ?? payload.outstanding_balance,
                  });
                }
              },
            };
            return;
          }

          if (referenceIdLocal) {
            const existingResult = await txClient.query(
              hasSalesTenantColumnLocal
                ? 'SELECT * FROM sales_records WHERE reference_id = $1 AND tenant_id = $2 LIMIT 1 FOR UPDATE'
                : 'SELECT * FROM sales_records WHERE reference_id = $1 LIMIT 1 FOR UPDATE',
              hasSalesTenantColumnLocal ? [referenceIdLocal, tenantIdLocal] : [referenceIdLocal],
            );

            if ((existingResult.rowCount || 0) > 0) {
              const existingRow = existingResult.rows[0] || null;
              if (existingRow) {
                const hydratedItems = await loadSalesRecordItems(
                  req.tenantDb,
                  existingRow.id,
                  tenantIdLocal,
                );
                existingRow.items = hydratedItems.length > 0
                  ? hydratedItems
                  : parseItemsFromJsonField(existingRow.items_json);
              }
              committedResponse = {
                body: existingRow,
                message: 'Transaction already exists',
                status: 200,
                emit: () => {},
              };
              return;
            }
          }

          for (const item of transactionItems) {
            if (item.isService) {
              continue;
            }
            normalizeCartItemInPlace(item);

            const nestedProduct =
              item.product && typeof item.product === 'object' ? item.product : {};
            const rawProductId =
              nestedProduct.id ??
              nestedProduct.product_id ??
              nestedProduct.productId ??
              item.product_id ??
              item.productId ??
              item.id;
            const safeProductId = (rawProductId === undefined || rawProductId === null)
              ? ''
              : `${rawProductId}`.trim();
            const safeTenantId = (tenantIdLocal === undefined || tenantIdLocal === null)
              ? ''
              : `${tenantIdLocal}`.trim();

            const lookupValues = hasProductsTenantColumn
              ? [safeProductId, safeTenantId]
              : [safeProductId];

            console.log('DEBUG (1/5): STOCK SELECT LOOKUP:', {
              rawProductId,
              safeProductId,
              typeofRaw: typeof rawProductId,
              safeTypeof: typeof safeProductId,
              itemKeys: Object.keys(item || {}),
              rawItem: item,
              hasProductsTenantColumn,
              safeTenantId,
            });

            const currentResult = await txClient.query(
              hasProductsTenantColumn
                ? `SELECT id, tenant_id, name, stock,
                          ${productIsServiceCol ? `${productIsServiceCol} AS is_service,` : 'false AS is_service,'}
                          ${productTracksStockCol ? `${productTracksStockCol} AS is_stock_tracked` : 'true AS is_stock_tracked'}
                   FROM "products" WHERE id = $1::bigint AND tenant_id = $2::text LIMIT 1 FOR UPDATE`
                : `SELECT id, name, stock,
                          ${productIsServiceCol ? `${productIsServiceCol} AS is_service,` : 'false AS is_service,'}
                          ${productTracksStockCol ? `${productTracksStockCol} AS is_stock_tracked` : 'true AS is_stock_tracked'}
                   FROM "products" WHERE id = $1::bigint LIMIT 1 FOR UPDATE`,
              lookupValues,
            );

            if ((currentResult.rowCount || 0) === 0) {
              console.warn('DEBUG (2/5): STOCK SELECT MISS! rowCount=0. Running debug SELECT WITHOUT tenant_id:', {
                productId: safeProductId,
                tenantId: safeTenantId,
              });
              try {
                const diagSql = 'SELECT id, tenant_id, name, stock FROM "products" WHERE id = $1::bigint LIMIT 5';
                const diagParams = [safeProductId];
                console.log('DEBUG SQL:', diagSql, 'PARAMS:', diagParams, 'ITEM:', item);
                const diag = await txClient.query(diagSql, diagParams);
                console.warn('  → SELECT WITHOUT tenant_id rows:', diag.rows);
              } catch (diagErr) {
                console.warn('  → diagnostic select failed:', diagErr && diagErr.message);
              }
              const error = new Error(`Produk ${item.productName || item.product_name || safeProductId} tidak ditemukan`);
              error.statusCode = 404;
              throw error;
            }

            const currentProduct = currentResult.rows[0];
            console.log('DEBUG (3/5): STOCK SELECT ROW HIT:', {
              id: currentProduct && currentProduct.id,
              typeofId: currentProduct && typeof currentProduct.id,
              idStr: currentProduct && String(currentProduct.id),
              tenantId: currentProduct && currentProduct.tenant_id,
              expectedTenantId: safeTenantId,
              tenantMatch: currentProduct && String(currentProduct.tenant_id || '') === String(safeTenantId || ''),
              name: currentProduct && currentProduct.name,
              stock: currentProduct && currentProduct.stock,
            });

            if (currentProduct.is_service === true) {
              continue;
            }

            const safeQty = Number.isFinite(Number(
              item.qty ?? item.quantity ?? nestedProduct.qty ?? nestedProduct.quantity
            )) ? Number(item.qty ?? item.quantity ?? nestedProduct.qty ?? nestedProduct.quantity) : 0;
            if (safeQty <= 0) {
              continue;
            }

            const stockTrackedOnProduct = currentProduct.is_stock_tracked !== false
              && currentProduct.is_stock_tracked !== 'false'
              && currentProduct.is_stock_tracked !== 0;
            const shouldCheckStock = !productTracksStockCol || stockTrackedOnProduct;

            if (!shouldCheckStock) {
              const unchangedValues = hasProductsTenantColumn
                ? [safeProductId, safeTenantId]
                : [safeProductId];
              const unchangedSql = hasProductsTenantColumn
                ? `UPDATE "products" SET updated_at = NOW() WHERE id = $1::bigint AND tenant_id = $2::text RETURNING *`
                : `UPDATE "products" SET updated_at = NOW() WHERE id = $1::bigint RETURNING *`;
              console.log('DEBUG SQL:', unchangedSql, 'PARAMS:', unchangedValues, 'ITEM:', item);
              const unchangedUpdate = await txClient.query(unchangedSql, unchangedValues);
              if ((unchangedUpdate.rowCount || 0) > 0) {
                inventoryUpdates.push(unchangedUpdate.rows[0]);
              }
              continue;
            }

            const currentStock = Number.isFinite(Number(currentProduct.stock ?? 0))
              ? Number(currentProduct.stock ?? 0)
              : 0;

            if (!Number.isFinite(currentStock)) {
              throw new Error(`Stok produk ${safeProductId} tidak valid`);
            }

            if (currentStock < safeQty) {
              console.warn(
                `⚠️ STOCK INSUFFICIENT: Product=${currentProduct.name}, ID=${safeProductId}, Current=${currentStock}, Requested=${safeQty}, Tenant=${safeTenantId}`,
              );
              const error = new Error(`Stok produk ${currentProduct.name || item.product_name || item.productName || safeProductId} tidak mencukupi / tidak ditemukan saat potong stok (Current=${currentStock}, Requested=${safeQty}, id=${safeProductId})`);
              error.statusCode = 400;
              throw error;
            }

            const nextStock = currentStock - safeQty;

            const updateSql = hasProductsTenantColumn
              ? `UPDATE "products"
                 SET stock = $1::numeric,
                     updated_at = NOW()
                 WHERE id = $2::bigint
                   AND tenant_id = $3::text
                 RETURNING *`
              : `UPDATE "products"
                 SET stock = $1::numeric,
                     updated_at = NOW()
                 WHERE id = $2::bigint
                 RETURNING *`;

            const queryParams = hasProductsTenantColumn
              ? [nextStock, safeProductId, safeTenantId]
              : [nextStock, safeProductId];

            console.log('DEBUG STOCK UPDATE (4/5):', {
              sql: updateSql,
              params: queryParams,
              safeProductId,
              qty: safeQty,
              currentStock,
              nextStock,
              hasTenant: hasProductsTenantColumn,
              tenantId: safeTenantId,
            });
            console.log('DEBUG SQL:', updateSql, 'PARAMS:', queryParams, 'ITEM:', item);

            let updateResult = await txClient.query(updateSql, queryParams);

            if ((updateResult.rowCount || 0) === 0 && hasProductsTenantColumn) {
              console.warn('DEBUG (5/5): STOCK UPDATE 0 rows WITH tenant_id. Retrying WITHOUT tenant_id fallback:', {
                safeProductId,
                expectedTenant: safeTenantId,
                actualTenantOnRow: currentProduct && currentProduct.tenant_id,
                sql: updateSql,
                params: queryParams,
              });
              const fallbackSql = `UPDATE "products"
                                   SET stock = $1::numeric,
                                       updated_at = NOW()
                                   WHERE id = $2::bigint
                                   RETURNING *`;
              const fallbackParams = [nextStock, safeProductId];
              console.log('DEBUG SQL FALLBACK:', fallbackSql, 'PARAMS:', fallbackParams, 'ITEM:', item);
              updateResult = await txClient.query(fallbackSql, fallbackParams);
              console.warn(`  → Fallback result: rowCount=${updateResult.rowCount || 0}`);
            }

            if ((updateResult.rowCount || 0) === 0) {
              console.warn(
                `⚠️ STOCK UPDATE NO ROWS (FINAL FAIL): Product=${currentProduct.name}, ID=${safeProductId}, Current=${currentStock}, Requested=${safeQty}, Tenant=${safeTenantId}, hasTenant=${hasProductsTenantColumn}`,
              );
              const error = new Error(`Stok produk ${currentProduct.name || item.product_name || item.productName || safeProductId} tidak ditemukan saat potong stok (id=${safeProductId}, tenant=${safeTenantId}, hasTenant=${hasProductsTenantColumn}, actualTenantOnRow=${String(currentProduct && currentProduct.tenant_id)})`);
              error.statusCode = 400;
              throw error;
            }

            if ((updateResult.rowCount || 0) > 0) {
              inventoryUpdates.push(updateResult.rows[0]);
            }
          }

          const tenantScopedPayload = enforceTenantIdOnPayload(payload, tenantIdLocal, salesRecordColumnDefinitions);
          const filteredPayload = normalizePayloadByColumnDefinitions(tenantScopedPayload, salesRecordColumnDefinitions);

          if (storedItems.length > 0) {
            filteredPayload.items_json = JSON.stringify(storedItems);
          }

          if (Object.keys(filteredPayload).length === 0) {
            throw new Error('Payload transaksi tidak cocok dengan schema sales_records tenant');
          }

          const { sql, values } = buildInsertQuery('sales_records', filteredPayload);
          const result = await txClient.query(sql, values);

          const insertedSalesRecordId = result.rows?.[0]?.id;
          if (insertedSalesRecordId !== undefined && insertedSalesRecordId !== null) {
            await syncSalesRecordItems(txClient, insertedSalesRecordId, tenantIdLocal, storedItems);
          }

          const savedTransaction = result.rows[0] || null;
          if (savedTransaction) {
            const hydratedItems = await loadSalesRecordItems(
              req.tenantDb,
              savedTransaction.id,
              tenantIdLocal,
            );
            savedTransaction.items = hydratedItems.length > 0
              ? hydratedItems
              : parseItemsFromJsonField(savedTransaction.items_json);
          }

          committedResponse = {
            body: savedTransaction,
            message: 'Transaction saved',
            status: 201,
            emit: () => {
              emitTransactionCreated(req, savedTransaction, {
                inventoryUpdates,
              });
              if (isKasBonTransaction) {
                emitKasBonCreated(req, savedTransaction, {
                  paymentStatus: payload.payment_status,
                  remainingBalance: payload.remaining_balance ?? payload.outstanding_balance,
                });
              }
            },
          };
        },
        {
          label: `create_transaction:${tenantIdLocal || 'unknown'}:${referenceIdLocal || 'noref'}`,
          maxAttempts: 4,
          baseDelayMs: 150,
          maxDelayMs: 2500,
        },
      );

      if (!committedResponse) {
        throw new Error('Internal inconsistency: transaction completed without committedResponse');
      }

      if (typeof committedResponse.emit === 'function') {
        try { committedResponse.emit(); } catch (_) {}
      }

      return jsonOk(res, committedResponse.body, committedResponse.message, committedResponse.status);
    } finally {
      client.release();
    }
  };

  try {
    return await withRetries(
      runTransactionCore,
      {
        label: `create_transaction:${requestId}`,
        maxAttempts: 3,
        baseDelayMs: 200,
        maxDelayMs: 1500,
        shouldRetry: (error, attempt) => {
          if (error?.statusCode && error.statusCode >= 400 && error.statusCode < 500) {
            return false;
          }
          if (attempt >= 3) return false;
          return isTransientDbError(error);
        },
      },
    );
  } catch (error) {
    try {
      await storeFailedPayload(
        'pos_transaction_create',
        {
          requestId,
          tenantId: tenantIdForLog,
          referenceId,
          body: incomingBody,
          query: req.query || null,
        },
        error,
        {
          url: req.url,
          method: req.method,
          headers: req.headers
            ? Object.fromEntries(
                Object.entries(req.headers).filter(([k]) => !/authorization|cookie|secret|signature/i.test(k)),
              )
            : null,
        },
      );
    } catch (_) {}

    console.error(
      `❌ Transaction Creation Error [requestId=${requestId}, Tenant=${tenantIdForLog}, Ref=${referenceId}]: ${error.message}`,
      {
        code: error?.code,
        constraint: error?.constraint,
        statusCode: error?.statusCode || null,
        stack: error?.stack,
      },
    );

    if (error?.code === '23505' && referenceId) {
      try {
        const existingResult = await req.tenantDb.query(
          hasSalesTenantColumn
            ? 'SELECT * FROM sales_records WHERE reference_id = $1 AND tenant_id = $2 LIMIT 1'
            : 'SELECT * FROM sales_records WHERE reference_id = $1 LIMIT 1',
          hasSalesTenantColumn ? [referenceId, tenantIdForLog] : [referenceId],
        );

        if ((existingResult.rowCount || 0) > 0) {
          const existingRow = existingResult.rows[0] || null;
          if (existingRow) {
            const hydratedItems = await loadSalesRecordItems(
              req.tenantDb,
              existingRow.id,
              tenantIdForLog,
            );
            existingRow.items = hydratedItems.length > 0
              ? hydratedItems
              : parseItemsFromJsonField(existingRow.items_json);
          }
          return jsonOk(res, existingRow, 'Transaction already exists (23505 idempotent recovery)');
        }
      } catch (_) {}
    }

    return jsonError(
      res,
      error?.statusCode || 500,
      error.message || 'Internal server error',
      error.message,
    );
  }
};

const settleKasBon = async (req, res) => {
  const requestId = `${Date.now()}-${crypto.randomBytes(4).toString('hex')}`;
  const incomingBody = (req.body && typeof req.body === 'object')
    ? { ...req.body }
    : {};
  const id = req.params.id;
  let tenantIdForLog = resolveTenantIdFromRequest(req);

  const runSettleCore = async () => {
    const client = await getClientFromPool(req.tenantDb);
    try {
      const tenantId = resolveTenantIdFromRequest(req);
      tenantIdForLog = tenantIdForLog || tenantId;

      const paidAmountRaw = incomingBody.paid_amount ?? incomingBody.amount ?? incomingBody.amount_paid;
      const paidAmount = toNumber(paidAmountRaw);
      const settlementMethodInput =
        incomingBody.settlement_method ?? incomingBody.payment_method ?? 'Cash';
      const settlementMethod = (settlementMethodInput ?? 'Cash').toString().trim() || 'Cash';
      const settlementNoteInput =
        incomingBody.payment_note ?? incomingBody.note ?? incomingBody.settlement_note ?? null;
      const settlementNote = settlementNoteInput === null || settlementNoteInput === undefined
        ? null
        : settlementNoteInput.toString().trim() || null;
      const pb1AmountRaw = incomingBody.tax_pb1_amount ?? incomingBody.pb1_amount ?? 0;
      const pb1Amount = Number.parseInt(pb1AmountRaw, 10);
      const normalizedPb1Amount = Number.isFinite(pb1Amount) ? pb1Amount : 0;
      const paidAt = incomingBody.paid_at ? new Date(incomingBody.paid_at) : new Date();
      const safeTenantId = (tenantId || '').toString().trim() || null;
      const salesRecordId = Number.parseInt((id || '').toString(), 10);
      const safeSalesRecordId = Number.isFinite(salesRecordId) ? salesRecordId : null;

      if (!id || safeSalesRecordId === null) {
        const error = new Error('id transaksi wajib diisi');
        error.statusCode = 400;
        throw error;
      }

      if (!Number.isFinite(paidAmount) || paidAmount <= 0) {
        const error = new Error('Nominal pembayaran harus lebih dari 0');
        error.statusCode = 400;
        throw error;
      }

      let committedResponse = null;

      await runTransaction(
        client,
        async (txClient) => {
          await ensureTenantScopedTable(txClient, 'sales_records', tenantId);
          await ensureSalesRecordsKasBonColumns(txClient);

          const columnsResult = await txClient.query(
            `SELECT column_name
             FROM information_schema.columns
             WHERE table_schema = ANY(current_schemas(false))
               AND table_name = 'sales_records'`,
          );
          const columns = new Set(columnsResult.rows.map((row) => row.column_name));

          const paymentColumn = columns.has('payment_method')
            ? 'payment_method'
            : (columns.has('payment_type') ? 'payment_type' : null);
          const amountColumn = columns.has('total_price')
            ? 'total_price'
            : (columns.has('total_amount') ? 'total_amount' : null);

          if (!paymentColumn || !amountColumn) {
            const error = new Error('Kolom pembayaran sales_records tidak lengkap');
            error.statusCode = 500;
            throw error;
          }

          const transactionResult = await txClient.query(
            `SELECT id,
                    ${paymentColumn} AS payment_value,
                    ${amountColumn} AS amount_value,
                    ${columns.has('payment_status') ? 'payment_status,' : "NULL::TEXT AS payment_status,"}
                    ${columns.has('amount_paid') ? 'amount_paid,' : 'NULL::NUMERIC AS amount_paid,'}
                    ${columns.has('outstanding_balance') ? 'outstanding_balance,' : 'NULL::NUMERIC AS outstanding_balance,'}
                    remaining_balance
             FROM sales_records
             WHERE id = $1::bigint
               ${columns.has('tenant_id') ? 'AND tenant_id = $2::text' : ''}
             FOR UPDATE`,
            columns.has('tenant_id') ? [safeSalesRecordId, safeTenantId || ''] : [safeSalesRecordId],
          );

          if (transactionResult.rowCount === 0) {
            const error = new Error('Data transaksi tidak ditemukan');
            error.statusCode = 404;
            throw error;
          }

          const transaction = transactionResult.rows[0];
          const paymentType = normalizePaymentType(transaction.payment_value);
          const paymentStatus = normalizePaymentType(transaction.payment_status);

          if (paymentType !== 'KAS BON') {
            const error = new Error('Transaksi ini bukan tipe pembayaran KAS BON');
            error.statusCode = 400;
            throw error;
          }

          if (paymentStatus === 'LUNAS') {
            const error = new Error('Kas Bon ini sudah dilunasi sebelumnya!');
            error.statusCode = 400;
            throw error;
          }

          const fallbackBalance = toNumber(transaction.amount_value);
          const currentBalance = Number.isFinite(toNumber(transaction.remaining_balance))
            ? toNumber(transaction.remaining_balance)
            : (Number.isFinite(toNumber(transaction.outstanding_balance))
              ? toNumber(transaction.outstanding_balance)
              : fallbackBalance);
          const currentAmountPaid = Number.isFinite(toNumber(transaction.amount_paid))
            ? toNumber(transaction.amount_paid)
            : Math.max(0, (Number.isFinite(fallbackBalance) ? fallbackBalance : 0) - currentBalance);

          if (currentBalance <= 0) {
            const error = new Error('Kas Bon ini sudah dilunasi sebelumnya!');
            error.statusCode = 400;
            throw error;
          }

          if (!Number.isFinite(currentBalance) || currentBalance < 0) {
            const error = new Error('Nilai remaining_balance tidak valid pada transaksi');
            error.statusCode = 400;
            throw error;
          }

          if (paidAmount > currentBalance) {
            const error = new Error('Nominal pembayaran melebihi sisa kas bon');
            error.statusCode = 400;
            throw error;
          }

          const normalizedPaidAmount = Number.isFinite(paidAmount)
            ? Number(paidAmount.toFixed(2))
            : 0;
          const nextBalanceRaw = currentBalance - normalizedPaidAmount;
          const nextBalance = Number(nextBalanceRaw.toFixed(2));
          const isLunas = nextBalance <= 0;
          const normalizedBalance = isLunas ? 0 : nextBalance;
          const safeCurrentBalance = Number.isFinite(currentBalance)
            ? Number(currentBalance.toFixed(2))
            : 0;
          const safeRemainingBalance = Number.isFinite(normalizedBalance)
            ? normalizedBalance
            : 0;
          const nextAmountPaid = Number((currentAmountPaid + normalizedPaidAmount).toFixed(2));
          const safeNextAmountPaid = Number.isFinite(nextAmountPaid) ? nextAmountPaid : 0;

          await ensureKasBonHistoryTable(txClient);
          await ensureTenantScopedTable(txClient, 'kas_bon_payment_history', tenantId);

          await txClient.query(
            `INSERT INTO kas_bon_payment_history (
               tenant_id,
               sales_record_id,
               paid_amount,
               previous_balance,
               remaining_balance,
               payment_method,
               paid_at,
               note
             ) VALUES (
               $1::text,
               $2::bigint,
               $3::numeric,
               $4::numeric,
               $5::numeric,
               $6::text,
               $7::timestamptz,
               $8::text
             )`,
            [
              safeTenantId,
              safeSalesRecordId,
              normalizedPaidAmount || 0,
              safeCurrentBalance || 0,
              safeRemainingBalance || 0,
              settlementMethod || 'Cash',
              paidAt || new Date(),
              settlementNote || null,
            ],
          );

          const updateClauses = ['remaining_balance = $1::numeric', 'amount_paid = $3::numeric'];
          if (columns.has('outstanding_balance')) {
            updateClauses.push('outstanding_balance = $1::numeric');
          }

          let updatedTransaction = null;
          let responseBody = null;

          if (columns.has('payment_method')) {
            const hasLastPaymentMethodColumn = columns.has('last_payment_method');
            const finalPaymentMethod = isLunas
              ? `Lunas - ${settlementMethod}`
              : 'Kas Bon';
            const paymentMethodToSave = (finalPaymentMethod || '').toString().trim() || 'Cash';

            const values = [
              safeRemainingBalance || 0,
              safeSalesRecordId,
              safeNextAmountPaid || 0,
            ];

            if (hasLastPaymentMethodColumn) {
              updateClauses.push('last_payment_method = $4::text');
              values.push((settlementMethod || 'Cash').toString().trim() || 'Cash');
            }

            const paymentMethodParamPosition = values.length + 1;
            updateClauses.push(
              hasLastPaymentMethodColumn
                ? 'payment_method = $5::text'
                : `payment_method = $${paymentMethodParamPosition}::text`,
            );
            values.push(paymentMethodToSave || 'Cash');

            if (hasLastPaymentMethodColumn) {
              const lastPaymentAmountParamPosition = values.length + 1;
              updateClauses.push(`last_payment_amount = $${lastPaymentAmountParamPosition}::numeric`);
              values.push(normalizedPaidAmount || 0);
            }

            if (columns.has('payment_status')) {
              const paramPosition = values.length + 1;
              updateClauses.push(`payment_status = $${paramPosition}::text`);
              values.push(isLunas ? 'LUNAS' : 'BELUM LUNAS');
            }

            const updateSql = `
              UPDATE sales_records
              SET ${updateClauses.join(', ')}
              WHERE id = $2::bigint
                ${columns.has('tenant_id') ? `AND tenant_id = $${values.length + 1}::text` : ''}
              RETURNING *
            `;

            const updateResult = await txClient.query(
              updateSql,
              columns.has('tenant_id') ? [...values, safeTenantId || ''] : values,
            );
            updatedTransaction = updateResult.rows[0] || null;
          } else {
            const values = [safeRemainingBalance || 0, safeSalesRecordId, safeNextAmountPaid || 0];

            if (columns.has('payment_status')) {
              const paramPosition = values.length + 1;
              updateClauses.push(`payment_status = $${paramPosition}::text`);
              values.push(isLunas ? 'LUNAS' : 'BELUM LUNAS');
            }

            const updateSql = `
              UPDATE sales_records
              SET ${updateClauses.join(', ')}
              WHERE id = $2::bigint
                ${columns.has('tenant_id') ? `AND tenant_id = $${values.length + 1}::text` : ''}
              RETURNING *
            `;

            const updateResult = await txClient.query(
              updateSql,
              columns.has('tenant_id') ? [...values, safeTenantId || ''] : values,
            );
            updatedTransaction = updateResult.rows[0] || null;
          }

          responseBody = {
            transaction: updatedTransaction,
            paid_amount: normalizedPaidAmount,
            remaining_balance: safeRemainingBalance,
            amount_paid: safeNextAmountPaid,
            payment_method: settlementMethod || 'Cash',
            note: settlementNote,
            tax_pb1_amount: normalizedPb1Amount,
            status: isLunas ? 'LUNAS' : 'BELUM LUNAS',
          };

          committedResponse = {
            body: responseBody,
            message: 'Pelunasan kas bon berhasil',
            status: 200,
            emit: () => {
              emitKasBonUpdated(req, updatedTransaction, {
                transactionId: id,
                paidAmount: normalizedPaidAmount,
                remainingBalance: safeRemainingBalance,
                status: isLunas ? 'Lunas' : 'Belum Lunas',
                paymentMethod: settlementMethod || 'Cash',
              });

              emitTransactionUpdated(req, updatedTransaction, {
                transactionId: id,
                action: 'UPDATE',
                mutationType: 'KASBON_SETTLED',
                paymentHistory: {
                  sales_record_id: id,
                  paid_amount: normalizedPaidAmount,
                  previous_balance: safeCurrentBalance,
                  remaining_balance: safeRemainingBalance,
                  payment_method: settlementMethod || 'Cash',
                  paid_at: paidAt,
                  note: settlementNote,
                },
                paidAmount: normalizedPaidAmount,
                remainingBalance: safeRemainingBalance,
                amountPaid: safeNextAmountPaid,
                tax_pb1_amount: normalizedPb1Amount,
                status: isLunas ? 'LUNAS' : 'BELUM LUNAS',
              });
            },
          };
        },
        {
          label: `settle_kas_bon:${tenantId || 'unknown'}:${id || 'noid'}`,
          maxAttempts: 4,
          baseDelayMs: 150,
          maxDelayMs: 2500,
        },
      );

      if (!committedResponse) {
        throw new Error('Internal inconsistency: kas bon settlement completed without committedResponse');
      }

      if (typeof committedResponse.emit === 'function') {
        try { committedResponse.emit(); } catch (_) {}
      }

      return jsonOk(
        res,
        committedResponse.body,
        committedResponse.message,
        committedResponse.status,
      );
    } finally {
      client.release();
    }
  };

  try {
    return await withRetries(
      runSettleCore,
      {
        label: `settle_kas_bon:${requestId}`,
        maxAttempts: 3,
        baseDelayMs: 200,
        maxDelayMs: 1500,
        shouldRetry: (error, attempt) => {
          if (error?.statusCode && error.statusCode >= 400 && error.statusCode < 500) {
            return false;
          }
          if (attempt >= 3) return false;
          return isTransientDbError(error);
        },
      },
    );
  } catch (error) {
    try {
      await storeFailedPayload(
        'pos_kas_bon_settle',
        {
          requestId,
          tenantId: tenantIdForLog,
          transactionId: id,
          body: incomingBody,
          params: req.params || null,
          query: req.query || null,
        },
        error,
        {
          url: req.url,
          method: req.method,
          headers: req.headers
            ? Object.fromEntries(
                Object.entries(req.headers).filter(([k]) => !/authorization|cookie|secret|signature/i.test(k)),
              )
            : null,
        },
      );
    } catch (_) {}

    console.error(
      `❌ Kas Bon Settlement Error [requestId=${requestId}, Tenant=${tenantIdForLog}, ID=${id}]: ${error.message}`,
      {
        code: error?.code,
        statusCode: error?.statusCode || null,
        stack: error?.stack,
      },
    );

    const phaseMessage = `[settleKasBon] ${error.message || 'Internal server error'}`;
    return jsonError(
      res,
      error?.statusCode || 500,
      phaseMessage,
      error.message || phaseMessage,
    );
  }
};

const cancelTransaction = async (req, res) => {
  try {
    const transactionId = req.params.id;

    if (!transactionId) {
      return jsonError(res, 400, 'Transaction ID wajib diisi');
    }

    const numericId = Number.parseInt((transactionId || '').toString(), 10);
    if (!Number.isFinite(numericId) || numericId <= 0) {
      return jsonError(res, 400, 'Transaction ID harus berupa angka positif');
    }
    const tenantId = resolveTenantIdFromRequest(req);
    const rawBaseUrl = (
      process.env.ADMIN_CORE_API_BASE_URL ||
      process.env.ADMIN_CORE_API_URL ||
      process.env.CORE_API_BASE_URL ||
      process.env.CORE_API_URL ||
      // Safety fallback for local/debug environments where only bridge is started.
      'https://goldenity-admin-core-backend-production.up.railway.app'
    )
      .toString()
      .trim();

    if (!rawBaseUrl) {
      return jsonError(
        res,
        500,
        'ADMIN_CORE_API_BASE_URL / CORE_API_BASE_URL belum dikonfigurasi',
      );
    }

    const baseUrl = new URL(rawBaseUrl);
    const basePath = baseUrl.pathname.replace(/\/+$/, '');
    const cancelPath = basePath.endsWith('/api')
      ? `${basePath}/v1/transactions/${encodeURIComponent(transactionId)}/cancel`
      : `${basePath}/api/v1/transactions/${encodeURIComponent(transactionId)}/cancel`;

    baseUrl.pathname = cancelPath;
    baseUrl.search = '';
    baseUrl.hash = '';

    const payload = {
      ...(req.body && typeof req.body === 'object' ? req.body : {}),
      ...(tenantId ? { tenant_id: tenantId } : {}),
    };

    const timeoutMs = Number(process.env.ADMIN_CORE_API_TIMEOUT_MS || 7000);
    const transport = baseUrl.protocol === 'https:' ? https : http;

    const response = await new Promise((resolve, reject) => {
      const body = JSON.stringify(payload);
      const request = transport.request(
        baseUrl,
        {
          method: 'PATCH',
          headers: {
            'content-type': 'application/json',
            'content-length': Buffer.byteLength(body),
            ...(req.headers.authorization
              ? { authorization: req.headers.authorization }
              : {}),
            ...(tenantId ? { 'x-tenant-id': tenantId } : {}),
            ...(req.headers['x-branch-id']
              ? { 'x-branch-id': req.headers['x-branch-id'] }
              : {}),
          },
          timeout: Number.isFinite(timeoutMs) ? timeoutMs : 7000,
        },
        (coreRes) => {
          let raw = '';
          coreRes.setEncoding('utf8');
          coreRes.on('data', (chunk) => {
            raw += chunk;
          });
          coreRes.on('end', () => {
            let parsed;
            try {
              parsed = raw ? JSON.parse(raw) : {};
            } catch (_) {
              parsed = { message: raw };
            }

            resolve({
              statusCode: coreRes.statusCode || 500,
              body: parsed,
            });
          });
        },
      );

      request.on('timeout', () => {
        request.destroy(new Error('Admin core cancel request timeout'));
      });
      request.on('error', reject);
      request.write(body);
      request.end();
    });

    const statusCode = response.statusCode || 500;
    const responseBody =
      response.body && typeof response.body === 'object'
        ? response.body
        : { message: 'Invalid response from admin core' };

    const normalizedResponseText = JSON.stringify(responseBody || {});
    const alreadyVoidedOnCore = statusCode === 409
      && /TRANSACTION_ALREADY_VOIDED/i.test(normalizedResponseText);
    const treatAsSuccess = (statusCode >= 200 && statusCode < 300) || alreadyVoidedOnCore;

    if (treatAsSuccess) {
      let cancelledTransaction = responseBody.data || responseBody;

      if (alreadyVoidedOnCore && (!cancelledTransaction || typeof cancelledTransaction !== 'object')) {
        cancelledTransaction = {
          id: numericId,
          status: 'CANCELLED',
          order_status: 'CANCELLED',
          transaction_status: 'VOID',
          is_void: true,
          isVoid: true,
        };
      }

      try {
        await mirrorCancelledTransactionToBridgeSalesRecord({
          tenantDb: req.tenantDb,
          tenantId,
          transactionId,
          cancelledTransaction,
          fallbackVoidReason: payload.void_reason ?? payload.voidReason,
        });
      } catch (mirrorError) {
        console.error(
          `⚠️ Cancel mirror warning [Tenant=${tenantId}, ID=${transactionId}]: ${mirrorError.message}`,
        );
      }
      emitTransactionUpdated(req, cancelledTransaction, {
        transactionId,
        action: 'CANCEL',
        mutationType: 'TRANSACTION_CANCELLED',
      });

      if (alreadyVoidedOnCore) {
        return res.status(200).json({
          success: true,
          message: 'Transaksi sudah dibatalkan sebelumnya. Status tetap CANCELLED.',
          data: cancelledTransaction,
        });
      }
    }

    return res.status(statusCode).json(responseBody);
  } catch (error) {
    console.error(
      `❌ Cancel Transaction Error [Tenant=${resolveTenantIdFromRequest(req)}, ID=${req.params.id}]: ${error.message}`,
      {
        code: error?.code,
        stack: error?.stack,
      },
    );

    return jsonError(
      res,
      502,
      error.message || 'Gagal meneruskan pembatalan ke admin core',
      error.message,
    );
  }
};

module.exports = {
  createTransaction,
  listActiveKasBon,
  settleKasBon,
  cancelTransaction,
};
