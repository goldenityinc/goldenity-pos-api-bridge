const { jsonOk, jsonError } = require('../utils/http');
const { buildInsertQuery } = require('../utils/sqlHelpers');

const normalizePaymentType = (value) => (value || '').toString().trim().toUpperCase();

const toNumber = (value) => {
  const numberValue = Number(value);
  return Number.isFinite(numberValue) ? numberValue : NaN;
};

const createTransaction = async (req, res) => {
  try {
    const payload = { ...req.body };
    const { sql, values } = buildInsertQuery('sales_records', payload);
    const result = await req.tenantDb.query(sql, values);
    return jsonOk(res, result.rows[0] || null, 'Transaction saved', 201);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
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

    const columnsResult = await client.query(
      `SELECT column_name
       FROM information_schema.columns
       WHERE table_schema = 'public'
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

    return jsonOk(
      res,
      {
        transaction: updateResult.rows[0] || null,
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
