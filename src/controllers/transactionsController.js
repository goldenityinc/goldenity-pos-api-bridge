const { jsonOk, jsonError } = require('../utils/http');
const {
  buildInsertQuery,
  getTableColumnDefinitions,
  normalizePayloadByColumnDefinitions,
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

const normalizeReferenceId = (payload = {}) => {
  return (
    payload.reference_id ??
    payload.referenceId ??
    payload.local_id ??
    payload.localId ??
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

const createTransaction = async (req, res) => {
  const client = await req.tenantDb.connect();
  let referenceId = '';

  try {
    const payload = { ...req.body };
    if (typeof payload.id === 'string') {
      delete payload.id;
    }
    referenceId = normalizeReferenceId(payload);
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

    if (referenceId) {
      const existingResult = await client.query(
        'SELECT * FROM sales_records WHERE reference_id = $1 LIMIT 1',
        [referenceId],
      );

      if ((existingResult.rowCount || 0) > 0) {
        await client.query('ROLLBACK');
        return jsonOk(res, existingResult.rows[0] || null, 'Transaction already exists');
      }
    }

    for (const item of transactionItems) {
      const currentResult = await client.query(
        'SELECT id, name, stock, is_service FROM "products" WHERE id = $1 LIMIT 1 FOR UPDATE',
        [item.productId],
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
        'UPDATE "products" SET stock = $1 WHERE id = $2 RETURNING *',
        [nextStock, item.productId],
      );
      if ((updateResult.rowCount || 0) > 0) {
        inventoryUpdates.push(updateResult.rows[0]);
      }
    }

    const salesRecordColumns = await getTableColumnDefinitions(client, 'sales_records');
    const filteredPayload = normalizePayloadByColumnDefinitions(payload, salesRecordColumns);

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
          'SELECT * FROM sales_records WHERE reference_id = $1 LIMIT 1',
          [referenceId],
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
              remaining_balance
       FROM sales_records
       WHERE id = $1
       FOR UPDATE`,
      [id],
    );

    if (transactionResult.rowCount === 0) {
      await client.query('ROLLBACK');
      return jsonError(res, 404, 'Data transaksi tidak ditemukan');
    }

    const transaction = transactionResult.rows[0];
    const paymentType = normalizePaymentType(transaction.payment_value);

    if (paymentType !== 'KAS BON') {
      await client.query('ROLLBACK');
      return jsonError(res, 400, 'Transaksi ini bukan tipe pembayaran KAS BON');
    }

    const fallbackBalance = toNumber(transaction.amount_value);
    const currentBalance = Number.isFinite(toNumber(transaction.remaining_balance))
      ? toNumber(transaction.remaining_balance)
      : fallbackBalance;

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
    const values = [normalizedBalance, id];

    if (isLunas && columns.has('payment_status')) {
      const paramPosition = values.length + 1;
      updateClauses.push(`payment_status = $${paramPosition}`);
      values.push('LUNAS');
    }

    const updateSql = `
      UPDATE sales_records
      SET ${updateClauses.join(', ')}
      WHERE id = $2
      RETURNING *
    `;

    const updateResult = await client.query(updateSql, values);
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
  settleKasBon,
};
