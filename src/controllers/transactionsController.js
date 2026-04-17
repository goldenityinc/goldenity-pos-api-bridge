const { jsonOk, jsonError } = require('../utils/http');
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

const normalizePaymentType = (value) => (value || '').toString().trim().toUpperCase();

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
        item.product_id ?? item.productId ?? product.id ?? ''
      ).toString().trim();
      const productName = (
        item.product_name ?? item.productName ?? product.name ?? ''
      ).toString().trim();
      const isService =
        item.is_service === true ||
        item.isService === true ||
        item.is_custom_item === true ||
        item.isCustomItem === true ||
        product.is_service === true ||
        product.isService === true ||
        product.is_custom_item === true ||
        product.isCustomItem === true;
      const qty = toPositiveInteger(item.qty ?? item.quantity);
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
        item.product_id ?? item.productId ?? product.id ?? ''
      ).toString().trim();
      const productName = (
        item.product_name ?? item.productName ?? product.name ?? ''
      ).toString().trim();
      const note = (item.note ?? item.item_note ?? item.product_note ?? '')
        .toString()
        .trim();
      const customPrice = toNumber(
        item.custom_price ?? item.customPrice ?? item.price ?? item.unit_price ?? 0,
      );
      const qty = toPositiveInteger(item.qty ?? item.quantity);
      if (qty === null) {
        return null;
      }

      const isService =
        item.is_service === true ||
        item.isService === true ||
        item.is_custom_item === true ||
        item.isCustomItem === true ||
        product.is_service === true ||
        product.isService === true ||
        product.is_custom_item === true ||
        product.isCustomItem === true;

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

const toStoredSalesRecordItems = (items) => {
  return normalizeTransactionItemsWithNotes(items).map((item) => ({
    product_id: item.productId || null,
    product_name: item.productName || null,
    qty: item.qty,
    custom_price: Number.isFinite(item.customPrice) ? item.customPrice : null,
    note: item.note || null,
    is_service: item.isService === true,
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
    const result = await client.query(
      normalizedTenantId
        ? `SELECT product_id, product_name, qty, custom_price, note, is_service
           FROM sales_record_items
           WHERE sales_record_id = $1
             AND tenant_id = $2
           ORDER BY id ASC`
        : `SELECT product_id, product_name, qty, custom_price, note, is_service
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

  for (const item of items) {
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
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())`,
      [
        normalizedTenantId || null,
        salesRecordId,
        item.product_id || null,
        item.product_name || null,
        Number.isInteger(Number(item.qty)) ? Number(item.qty) : 1,
        Number.isFinite(Number(item.custom_price)) ? Number(item.custom_price) : null,
        item.note || null,
        item.is_service === true,
      ],
    );
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

    return jsonOk(res, normalizedRows, 'Kas bon aktif berhasil dimuat');
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  } finally {
    client.release();
  }
};

const createTransaction = async (req, res) => {
  const client = await req.tenantDb.connect();
  let referenceId = '';
  let tenantId = '';
  let hasSalesTenantColumn = false;

  try {
    tenantId = resolveTenantIdFromRequest(req);
    const payload = { ...req.body };
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
    referenceId = normalizeReferenceId(payload, clientProvidedId);
    if (referenceId) {
      payload.reference_id = referenceId;
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
    const transactionItems = normalizeTransactionItems(payload.items);
    const storedItems = toStoredSalesRecordItems(payload.items);

    await client.query('BEGIN');
    await ensureSalesRecordsReferenceIdColumn(client);
    await ensureSalesRecordsReceiptNumberColumn(client);
    await ensureSalesRecordsCashierColumns(client);
    await ensureSalesRecordsCustomerColumn(client);
    await ensureSalesRecordsKasBonColumns(client);
    await ensureSalesRecordsFinancialColumns(client);
    await ensureSalesRecordsItemsColumn(client);
    await ensureSalesRecordItemsTable(client);
    await ensureTenantScopedTable(client, 'sales_records', tenantId);
    await ensureTenantScopedTable(client, 'products', tenantId);
    const salesRecordColumnDefinitions = await getTableColumnDefinitions(client, 'sales_records');
    const salesRecordColumnSet = new Set(salesRecordColumnDefinitions.keys());
    hasSalesTenantColumn = salesRecordColumnSet.has('tenant_id');
    const productsColumnSet = await getTableColumnSet(client, 'products');
    const hasProductsTenantColumn = productsColumnSet.has('tenant_id');

    if (existingTransactionIdToUpdate || existingReceiptNumberToUpdate) {
      const existingRecordResult = await client.query(
        existingTransactionIdToUpdate
          ? (hasSalesTenantColumn
            ? 'SELECT * FROM sales_records WHERE id = $1 AND tenant_id = $2 LIMIT 1 FOR UPDATE'
            : 'SELECT * FROM sales_records WHERE id = $1 LIMIT 1 FOR UPDATE')
          : (hasSalesTenantColumn
            ? 'SELECT * FROM sales_records WHERE receipt_number = $1 AND tenant_id = $2 LIMIT 1 FOR UPDATE'
            : 'SELECT * FROM sales_records WHERE receipt_number = $1 LIMIT 1 FOR UPDATE'),
        hasSalesTenantColumn
          ? [
            existingTransactionIdToUpdate || existingReceiptNumberToUpdate,
            tenantId,
          ]
          : [existingTransactionIdToUpdate || existingReceiptNumberToUpdate],
      );

      if ((existingRecordResult.rowCount || 0) === 0) {
        throw new Error('Transaksi target untuk update tidak ditemukan');
      }

      const existingRecord = existingRecordResult.rows[0] || null;
      const existingSalesRecordId = existingRecord?.id;
      if (!existingSalesRecordId) {
        throw new Error('ID transaksi target tidak valid');
      }

      const tenantScopedPayload = enforceTenantIdOnPayload(
        payload,
        tenantId,
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
                           ${hasSalesTenantColumn ? `AND tenant_id = $${tenantParam}` : ''}
                           RETURNING *`;
        const updateResult = await client.query(
          updateSql,
          hasSalesTenantColumn
            ? [...updateValues, existingSalesRecordId, tenantId]
            : [...updateValues, existingSalesRecordId],
        );
        updatedRow = updateResult.rows?.[0] || existingRecord;
      }

      await syncSalesRecordItems(client, existingSalesRecordId, tenantId, storedItems);
      await client.query('COMMIT');

      const savedTransaction = updatedRow || null;
      if (savedTransaction) {
        const hydratedItems = await loadSalesRecordItems(
          req.tenantDb,
          savedTransaction.id,
          tenantId,
        );
        savedTransaction.items = hydratedItems.length > 0
          ? hydratedItems
          : parseItemsFromJsonField(savedTransaction.items_json);
      }

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

      return jsonOk(res, savedTransaction, 'Transaction updated');
    }

    if (referenceId) {
      const existingResult = await client.query(
        hasSalesTenantColumn
          ? 'SELECT * FROM sales_records WHERE reference_id = $1 AND tenant_id = $2 LIMIT 1'
          : 'SELECT * FROM sales_records WHERE reference_id = $1 LIMIT 1',
        hasSalesTenantColumn ? [referenceId, tenantId] : [referenceId],
      );

      if ((existingResult.rowCount || 0) > 0) {
        await client.query('ROLLBACK');
        return jsonOk(res, existingResult.rows[0] || null, 'Transaction already exists');
      }
    }

    for (const item of transactionItems) {
      if (item.isService) {
        continue;
      }

      const currentResult = await client.query(
        hasProductsTenantColumn
          ? 'SELECT id, name, stock, is_service FROM "products" WHERE id = $1 AND tenant_id = $2 LIMIT 1 FOR UPDATE'
          : 'SELECT id, name, stock, is_service FROM "products" WHERE id = $1 LIMIT 1 FOR UPDATE',
        hasProductsTenantColumn ? [item.productId, tenantId] : [item.productId],
      );

      if ((currentResult.rowCount || 0) === 0) {
        throw new Error(`Produk ${item.productName || item.productId} tidak ditemukan`);
      }

      const currentProduct = currentResult.rows[0];
      if (currentProduct.is_service === true) {
        continue;
      }

      const currentStock = Number(currentProduct.stock ?? 0);
      if (!Number.isFinite(currentStock)) {
        throw new Error(`Stok produk ${item.productId} tidak valid`);
      }

      const nextStock = currentStock - item.qty;
      if (nextStock < 0) {
        console.warn(
          `⚠️ STOCK INSUFFICIENT: Product=${currentProduct.name}, ID=${item.productId}, Current=${currentStock}, Requested=${item.qty}, Tenant=${tenantId}`,
        );
        throw new Error(`Stok produk ${currentProduct.name || item.productId} tidak mencukupi`);
      }

      const updateResult = await client.query(
        hasProductsTenantColumn
          ? 'UPDATE "products" SET stock = $1 WHERE id = $2 AND tenant_id = $3 RETURNING *'
          : 'UPDATE "products" SET stock = $1 WHERE id = $2 RETURNING *',
        hasProductsTenantColumn ? [nextStock, item.productId, tenantId] : [nextStock, item.productId],
      );
      if ((updateResult.rowCount || 0) > 0) {
        inventoryUpdates.push(updateResult.rows[0]);
      }
    }

    const tenantScopedPayload = enforceTenantIdOnPayload(payload, tenantId, salesRecordColumnDefinitions);
    const filteredPayload = normalizePayloadByColumnDefinitions(tenantScopedPayload, salesRecordColumnDefinitions);

    if (storedItems.length > 0) {
      filteredPayload.items_json = JSON.stringify(storedItems);
    }

    if (Object.keys(filteredPayload).length === 0) {
      throw new Error('Payload transaksi tidak cocok dengan schema sales_records tenant');
    }

    const { sql, values } = buildInsertQuery('sales_records', filteredPayload);
    const result = await client.query(sql, values);

    const insertedSalesRecordId = result.rows?.[0]?.id;
    if (insertedSalesRecordId !== undefined && insertedSalesRecordId !== null) {
      await syncSalesRecordItems(client, insertedSalesRecordId, tenantId, storedItems);
    }

    await client.query('COMMIT');

    const savedTransaction = result.rows[0] || null;
    if (savedTransaction) {
      const hydratedItems = await loadSalesRecordItems(
        req.tenantDb,
        savedTransaction.id,
        tenantId,
      );
      savedTransaction.items = hydratedItems.length > 0
        ? hydratedItems
        : parseItemsFromJsonField(savedTransaction.items_json);
    }
    emitTransactionCreated(req, savedTransaction, {
      inventoryUpdates,
    });
    if (isKasBonTransaction) {
      emitKasBonCreated(req, savedTransaction, {
        paymentStatus: payload.payment_status,
        remainingBalance: payload.remaining_balance ?? payload.outstanding_balance,
      });
    }

    return jsonOk(res, savedTransaction, 'Transaction saved', 201);
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});

    console.error(
      `❌ Transaction Creation Error [Tenant=${tenantId}, Ref=${referenceId}]: ${error.message}`,
      {
        code: error?.code,
        constraint: error?.constraint,
        stack: error?.stack,
      },
    );

    if (error?.code === '23505' && referenceId) {
      try {
        const existingResult = await req.tenantDb.query(
          hasSalesTenantColumn
            ? 'SELECT * FROM sales_records WHERE reference_id = $1 AND tenant_id = $2 LIMIT 1'
            : 'SELECT * FROM sales_records WHERE reference_id = $1 LIMIT 1',
          hasSalesTenantColumn ? [referenceId, tenantId] : [referenceId],
        );

        if ((existingResult.rowCount || 0) > 0) {
          const existingRow = existingResult.rows[0] || null;
          if (existingRow) {
            const hydratedItems = await loadSalesRecordItems(
              req.tenantDb,
              existingRow.id,
              tenantId,
            );
            existingRow.items = hydratedItems.length > 0
              ? hydratedItems
              : parseItemsFromJsonField(existingRow.items_json);
          }
          return jsonOk(res, existingRow, 'Transaction already exists');
        }
      } catch (_) {}
    }

    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  } finally {
    client.release();
  }
};

const settleKasBon = async (req, res) => {
  const client = await req.tenantDb.connect();

  try {
    const tenantId = resolveTenantIdFromRequest(req);
    await ensureTenantScopedTable(client, 'sales_records', tenantId);
    await ensureSalesRecordsKasBonColumns(client);
    const id = req.params.id;
    const paidAmountRaw = req.body?.paid_amount ?? req.body?.amount ?? req.body?.amount_paid;
    const paidAmount = toNumber(paidAmountRaw);
    const settlementMethodInput =
      req.body?.settlement_method ?? req.body?.payment_method ?? 'Cash';
    const settlementMethod = (settlementMethodInput ?? 'Cash').toString().trim() || 'Cash';
    const settlementNoteInput =
      req.body?.payment_note ?? req.body?.note ?? req.body?.settlement_note ?? null;
    const settlementNote = settlementNoteInput === null || settlementNoteInput === undefined
      ? null
      : settlementNoteInput.toString().trim() || null;
    const pb1AmountRaw = req.body?.tax_pb1_amount ?? req.body?.pb1_amount ?? 0;
    const pb1Amount = Number.parseInt(pb1AmountRaw, 10);
    const normalizedPb1Amount = Number.isFinite(pb1Amount) ? pb1Amount : 0;
    const paidAt = req.body?.paid_at ? new Date(req.body.paid_at) : new Date();
    const safeTenantId = (tenantId || '').toString().trim() || null;
    const salesRecordId = Number.parseInt((id || '').toString(), 10);
    const safeSalesRecordId = Number.isFinite(salesRecordId) ? salesRecordId : null;

    if (!id || safeSalesRecordId === null) {
      return jsonError(res, 400, 'id transaksi wajib diisi');
    }

    if (!Number.isFinite(paidAmount) || paidAmount <= 0) {
      return jsonError(res, 400, 'Nominal pembayaran harus lebih dari 0');
    }

    await client.query('BEGIN');

    // Use current_schemas(false) so the lookup works regardless of whether the
    // tenant's tables live in the 'public' schema or a custom search_path schema.
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

    if (!paymentColumn || !amountColumn) {
      await client.query('ROLLBACK');
      return jsonError(res, 500, 'Kolom pembayaran sales_records tidak lengkap');
    }

    const transactionResult = await client.query(
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
      await client.query('ROLLBACK');
      return jsonError(res, 404, 'Data transaksi tidak ditemukan');
    }


    const transaction = transactionResult.rows[0];
    const paymentType = normalizePaymentType(transaction.payment_value);
    const paymentStatus = normalizePaymentType(transaction.payment_status);

    if (paymentType !== 'KAS BON') {
      await client.query('ROLLBACK');
      return jsonError(res, 400, 'Transaksi ini bukan tipe pembayaran KAS BON');
    }

    if (paymentStatus === 'LUNAS') {
      await client.query('ROLLBACK');
      return jsonError(res, 400, 'Kas Bon ini sudah dilunasi sebelumnya!');
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
      await client.query('ROLLBACK');
      return jsonError(res, 400, 'Kas Bon ini sudah dilunasi sebelumnya!');
    }

    if (!Number.isFinite(currentBalance) || currentBalance < 0) {
      await client.query('ROLLBACK');
      return jsonError(res, 400, 'Nilai remaining_balance tidak valid pada transaksi');
    }

    if (paidAmount > currentBalance) {
      await client.query('ROLLBACK');
      return jsonError(res, 400, 'Nominal pembayaran melebihi sisa kas bon');
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

    await ensureKasBonHistoryTable(client);
    await ensureTenantScopedTable(client, 'kas_bon_payment_history', tenantId);

    await client.query(
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
    if (columns.has('last_payment_method')) {
      updateClauses.push('last_payment_method = $4::text');
    }
    if (columns.has('payment_method')) {
      const finalPaymentMethod = isLunas
        ? `Lunas - ${settlementMethod}`
        : 'Kas Bon';
      updateClauses.push(`payment_method = $${columns.has('last_payment_method') ? 5 : 4}::text`);
      if (columns.has('last_payment_method')) {
        updateClauses.push('last_payment_amount = $6::numeric');
      }
      const values = [
        safeRemainingBalance || 0,
        safeSalesRecordId,
        safeNextAmountPaid || 0,
        settlementMethod || 'Cash',
        finalPaymentMethod,
        normalizedPaidAmount || 0,
      ];

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

      const updateResult = await client.query(
        updateSql,
        columns.has('tenant_id') ? [...values, safeTenantId || ''] : values,
      );
      await client.query('COMMIT');

      const updatedTransaction = updateResult.rows[0] || null;

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

      return jsonOk(
        res,
        {
          transaction: updatedTransaction,
          paid_amount: normalizedPaidAmount,
          remaining_balance: safeRemainingBalance,
          amount_paid: safeNextAmountPaid,
          payment_method: settlementMethod || 'Cash',
          note: settlementNote,
          tax_pb1_amount: normalizedPb1Amount,
          status: isLunas ? 'LUNAS' : 'BELUM LUNAS',
        },
        'Pelunasan kas bon berhasil',
      );
    }

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

    const updateResult = await client.query(
      updateSql,
      columns.has('tenant_id') ? [...values, safeTenantId || ''] : values,
    );
    await client.query('COMMIT');

    const updatedTransaction = updateResult.rows[0] || null;

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

    return jsonOk(
      res,
      {
        transaction: updatedTransaction,
        paid_amount: normalizedPaidAmount,
        remaining_balance: safeRemainingBalance,
        amount_paid: safeNextAmountPaid,
        payment_method: settlementMethod || 'Cash',
        note: settlementNote,
        tax_pb1_amount: normalizedPb1Amount,
        status: isLunas ? 'LUNAS' : 'BELUM LUNAS',
      },
      'Pelunasan kas bon berhasil',
    );
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  } finally {
    client.release();
  }
};

module.exports = {
  createTransaction,
  listActiveKasBon,
  settleKasBon,
};
