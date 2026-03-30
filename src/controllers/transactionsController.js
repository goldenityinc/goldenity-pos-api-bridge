const { jsonOk, jsonError } = require('../utils/http');
const {
  buildInsertQuery,
  getTableColumnDefinitions,
  getTableColumnSet,
  normalizePayloadByColumnDefinitions,
  enforceTenantIdOnPayload,
  normalizeTenantId,
} = require('../utils/sqlHelpers');
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

const ensureSalesRecordsReferenceIdColumn = async (client) => {
  await client.query(`
    ALTER TABLE sales_records
    ADD COLUMN IF NOT EXISTS reference_id TEXT;
  `);

  await client.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_sales_records_reference_id_unique
    ON sales_records (reference_id)
    WHERE reference_id IS NOT NULL;
  `);
};

const ensureSalesRecordsCashierColumns = async (client) => {
  await client.query(`
    ALTER TABLE sales_records
    ADD COLUMN IF NOT EXISTS cashier_id TEXT,
    ADD COLUMN IF NOT EXISTS cashier_name TEXT;
  `);
};

const ensureSalesRecordsKasBonColumns = async (client) => {
  await client.query(`
    ALTER TABLE sales_records
    ADD COLUMN IF NOT EXISTS payment_method TEXT,
    ADD COLUMN IF NOT EXISTS payment_status TEXT,
    ADD COLUMN IF NOT EXISTS remaining_balance NUMERIC(14,2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS outstanding_balance NUMERIC(14,2) DEFAULT 0;
  `);
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
      const qty = toPositiveInteger(item.qty ?? item.quantity);
      if (!productId || qty === null) {
        return null;
      }

      return {
        productId,
        qty,
      };
    })
    .filter(Boolean);
};

const ensureKasBonHistoryTable = async (client) => {
  await client.query(`
    CREATE TABLE IF NOT EXISTS kas_bon_payment_history (
      id BIGSERIAL PRIMARY KEY,
      sales_record_id BIGINT NOT NULL,
      paid_amount NUMERIC(14,2) NOT NULL,
      previous_balance NUMERIC(14,2) NOT NULL,
      remaining_balance NUMERIC(14,2) NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_kas_bon_payment_history_sales_record_id
    ON kas_bon_payment_history (sales_record_id);
  `);
};

const listActiveKasBon = async (req, res) => {
  const client = await req.tenantDb.connect();

  try {
    const tenantId = normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);
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

    const filters = [
      `UPPER(COALESCE(${paymentColumn}::text, '')) = 'KAS BON'`,
    ];
    if (columns.has('tenant_id')) {
      filters.push('tenant_id = $1');
    }
    if (columns.has('payment_status')) {
      filters.push(`UPPER(COALESCE(payment_status::text, 'BELUM LUNAS')) <> 'LUNAS'`);
    }

    const balanceExpression = remainingColumn
      ? `COALESCE(${remainingColumn}, ${amountColumn}, 0)`
      : `COALESCE(${amountColumn}, 0)`;
    filters.push(`${balanceExpression} > 0`);

    const orderColumn = columns.has('created_at') ? 'created_at' : 'id';
    const rowsResult = await client.query(
      `SELECT *
       FROM sales_records
       WHERE ${filters.join(' AND ')}
       ORDER BY ${orderColumn} DESC`,
      columns.has('tenant_id') ? [tenantId] : [],
    );

    return jsonOk(res, rowsResult.rows || [], 'Kas bon aktif berhasil dimuat');
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
    tenantId = normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);
    const payload = { ...req.body };
    const clientProvidedId = typeof payload.id === 'string'
      ? payload.id.trim()
      : '';
    if (clientProvidedId) {
      delete payload.id;
    }
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

    await client.query('BEGIN');
    await ensureSalesRecordsReferenceIdColumn(client);
    await ensureSalesRecordsCashierColumns(client);
    await ensureSalesRecordsKasBonColumns(client);
    const salesRecordColumnDefinitions = await getTableColumnDefinitions(client, 'sales_records');
    const salesRecordColumnSet = new Set(salesRecordColumnDefinitions.keys());
    hasSalesTenantColumn = salesRecordColumnSet.has('tenant_id');
    const productsColumnSet = await getTableColumnSet(client, 'products');
    const hasProductsTenantColumn = productsColumnSet.has('tenant_id');

    if (referenceId) {
      const hasSalesTenantColumn = salesRecordColumnSet.has('tenant_id');
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
      const currentResult = await client.query(
        hasProductsTenantColumn
          ? 'SELECT id, name, stock, is_service FROM "products" WHERE id = $1 AND tenant_id = $2 LIMIT 1 FOR UPDATE'
          : 'SELECT id, name, stock, is_service FROM "products" WHERE id = $1 LIMIT 1 FOR UPDATE',
        hasProductsTenantColumn ? [item.productId, tenantId] : [item.productId],
      );

      if ((currentResult.rowCount || 0) === 0) {
        throw new Error(`Produk ${item.productId} tidak ditemukan`);
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

    if (Object.keys(filteredPayload).length === 0) {
      throw new Error('Payload transaksi tidak cocok dengan schema sales_records tenant');
    }

    const { sql, values } = buildInsertQuery('sales_records', filteredPayload);
    const result = await client.query(sql, values);
    await client.query('COMMIT');

    const savedTransaction = result.rows[0] || null;
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

    if (error?.code === '23505' && referenceId) {
      try {
        const existingResult = await req.tenantDb.query(
          hasSalesTenantColumn
            ? 'SELECT * FROM sales_records WHERE reference_id = $1 AND tenant_id = $2 LIMIT 1'
            : 'SELECT * FROM sales_records WHERE reference_id = $1 LIMIT 1',
          hasSalesTenantColumn ? [referenceId, tenantId] : [referenceId],
        );

        if ((existingResult.rowCount || 0) > 0) {
          return jsonOk(res, existingResult.rows[0] || null, 'Transaction already exists');
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
    const tenantId = normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);
    const id = req.params.id;
    const paidAmount = toNumber(req.body?.paid_amount ?? req.body?.amount);

    if (!id) {
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
              ${columns.has('outstanding_balance') ? 'outstanding_balance,' : 'NULL::NUMERIC AS outstanding_balance,'}
              remaining_balance
       FROM sales_records
       WHERE id = $1
         ${columns.has('tenant_id') ? 'AND tenant_id = $2' : ''}
       FOR UPDATE`,
      columns.has('tenant_id') ? [id, tenantId] : [id],
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

    const nextBalanceRaw = currentBalance - paidAmount;
    const nextBalance = Number(nextBalanceRaw.toFixed(2));
    const isLunas = nextBalance <= 0;
    const normalizedBalance = isLunas ? 0 : nextBalance;

    await ensureKasBonHistoryTable(client);

    await client.query(
      `INSERT INTO kas_bon_payment_history (
         sales_record_id,
         paid_amount,
         previous_balance,
         remaining_balance
       ) VALUES ($1, $2, $3, $4)`,
      [id, paidAmount, currentBalance, normalizedBalance],
    );

    const updateClauses = ['remaining_balance = $1'];
    if (columns.has('outstanding_balance')) {
      updateClauses.push('outstanding_balance = $1');
    }
    const values = [normalizedBalance, id];

    if (columns.has('payment_status')) {
      const paramPosition = values.length + 1;
      updateClauses.push(`payment_status = $${paramPosition}`);
      values.push(isLunas ? 'LUNAS' : 'BELUM LUNAS');
    }

    const updateSql = `
      UPDATE sales_records
      SET ${updateClauses.join(', ')}
      WHERE id = $2
        ${columns.has('tenant_id') ? `AND tenant_id = $${values.length + 1}` : ''}
      RETURNING *
    `;

    const updateResult = await client.query(
      updateSql,
      columns.has('tenant_id') ? [...values, tenantId] : values,
    );
    await client.query('COMMIT');

    const updatedTransaction = updateResult.rows[0] || null;

    emitKasBonUpdated(req, updatedTransaction, {
      transactionId: id,
      paidAmount,
      remainingBalance: normalizedBalance,
      status: isLunas ? 'Lunas' : 'Belum Lunas',
    });

    emitTransactionUpdated(req, updatedTransaction, {
      transactionId: id,
      action: 'UPDATE',
      mutationType: 'KASBON_SETTLED',
      paymentHistory: {
        sales_record_id: id,
        paid_amount: paidAmount,
        previous_balance: currentBalance,
        remaining_balance: normalizedBalance,
      },
      paidAmount,
      remainingBalance: normalizedBalance,
      status: isLunas ? 'LUNAS' : 'BELUM LUNAS',
    });

    return jsonOk(
      res,
      {
        transaction: updatedTransaction,
        paid_amount: paidAmount,
        remaining_balance: normalizedBalance,
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
