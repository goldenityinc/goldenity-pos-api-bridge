const { jsonOk, jsonError } = require('../utils/http');
const { ensureTenantScopedTable } = require('../utils/tenantScope');
const { ensurePettyCashLogsTable } = require('./pettyCashController');

const WIB_TIME_ZONE = 'Asia/Jakarta';

const resolveTenantIdFromRequest = (req) => (
  req?.user?.tenantId
  ?? req?.user?.tenant_id
  ?? req?.tenant?.tenantId
  ?? req?.auth?.tenantId
  ?? req?.auth?.tenant_id
  ?? ''
)
  .toString()
  .trim();

const getTableColumnSet = async (client, table) => {
  const result = await client.query(
    `SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = ANY(current_schemas(false))
       AND table_name = $1`,
    [table],
  );

  return new Set((result.rows || []).map((row) => row.column_name));
};

const toInt = (value) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.round(parsed) : 0;
};

const getWibDayUtcRange = () => {
  const nowWib = new Date().toLocaleString('sv-SE', {
    timeZone: WIB_TIME_ZONE,
    hour12: false,
  });
  const [datePart] = nowWib.split(' ');
  const [year, month, day] = datePart.split('-').map((value) => Number(value));

  const startUtc = new Date(Date.UTC(year, month - 1, day, -7, 0, 0));
  const endUtc = new Date(Date.UTC(year, month - 1, day + 1, -7, 0, 0));

  return {
    wibDate: `${year.toString().padStart(4, '0')}-${month
      .toString()
      .padStart(2, '0')}-${day.toString().padStart(2, '0')}`,
    startUtcIso: startUtc.toISOString(),
    endUtcIso: endUtc.toISOString(),
  };
};

const getTodayDashboardSummary = async (req, res) => {
  const client = await req.tenantDb.connect();

  try {
    const tenantId = resolveTenantIdFromRequest(req);

    if (!tenantId) {
      return jsonError(res, 401, 'Tenant tidak valid');
    }

    await ensureTenantScopedTable(client, 'sales_records', tenantId);
    await ensureTenantScopedTable(client, 'expenses', tenantId);
    await ensureTenantScopedTable(client, 'daily_cash', tenantId);
    await ensureTenantScopedTable(client, 'products', tenantId);
    await ensurePettyCashLogsTable(client);
    await ensureTenantScopedTable(client, 'petty_cash_logs', tenantId);

    const { startUtcIso, endUtcIso, wibDate } = getWibDayUtcRange();

    const salesColumns = await getTableColumnSet(client, 'sales_records');
    const expensesColumns = await getTableColumnSet(client, 'expenses');
    const dailyCashColumns = await getTableColumnSet(client, 'daily_cash');
    const productColumns = await getTableColumnSet(client, 'products');

    const salesAmountColumn = salesColumns.has('total_price')
      ? 'total_price'
      : (salesColumns.has('total_amount') ? 'total_amount' : '0');
    const salesProfitColumn = salesColumns.has('total_profit')
      ? 'total_profit'
      : (salesColumns.has('profit') ? 'profit' : (salesColumns.has('gross_profit') ? 'gross_profit' : '0'));
    const salesPaymentColumn = salesColumns.has('payment_method')
      ? 'payment_method'
      : (salesColumns.has('payment_type') ? 'payment_type' : null);
    const salesStatusColumn = salesColumns.has('status') ? 'status' : null;
    const salesPaymentStatusColumn = salesColumns.has('payment_status')
      ? 'payment_status'
      : null;
    const salesCreatedAtColumn = salesColumns.has('created_at') ? 'created_at' : null;

    const salesFilters = ['tenant_id = $1'];
    const salesParams = [tenantId];
    if (salesCreatedAtColumn) {
      salesFilters.push(
        `(${salesCreatedAtColumn} >= $2::timestamptz AND ${salesCreatedAtColumn} < $3::timestamptz)`,
      );
      salesParams.push(startUtcIso, endUtcIso);
    }
    if (salesStatusColumn) {
      salesFilters.push(`COALESCE(LOWER(${salesStatusColumn}::text), '') <> 'void'`);
    }

    const salesWhereClause = salesFilters.join(' AND ');
    const normalizedSalesPaymentExpression = salesPaymentColumn
      ? `REPLACE(REPLACE(REPLACE(UPPER(COALESCE(${salesPaymentColumn}::text, '')), ' ', ''), '-', ''), '_', '')`
      : "''";

    const salesResult = await client.query(
      `SELECT
         COUNT(*)::int AS total_transactions,
         COALESCE(SUM(${salesAmountColumn}), 0)::numeric AS total_sales,
         COALESCE(SUM(${salesProfitColumn}), 0)::numeric AS total_gross_profit,
         COALESCE(SUM(CASE
           WHEN ${normalizedSalesPaymentExpression} IN ('CASH', 'TUNAI')
             THEN ${salesAmountColumn}
           ELSE 0
         END), 0)::numeric AS total_income_cash,
         COALESCE(SUM(CASE
           WHEN ${normalizedSalesPaymentExpression} IN ('TRANSFER', 'QRIS')
             THEN ${salesAmountColumn}
           ELSE 0
         END), 0)::numeric AS total_income_non_cash
       FROM sales_records
       WHERE ${salesWhereClause}`,
      salesParams,
    );

    const settledStatusFilters = [...salesFilters];
    const settledStatusPredicates = [];
    if (salesStatusColumn) {
      settledStatusPredicates.push(
        `REPLACE(REPLACE(REPLACE(UPPER(COALESCE(${salesStatusColumn}::text, '')), ' ', ''), '-', ''), '_', '') IN ('SUCCESS', 'PAID', 'LUNAS', 'COMPLETED', 'SELESAI')`,
      );
    }
    if (salesPaymentStatusColumn) {
      settledStatusPredicates.push(
        `REPLACE(REPLACE(REPLACE(UPPER(COALESCE(${salesPaymentStatusColumn}::text, '')), ' ', ''), '-', ''), '_', '') IN ('SUCCESS', 'PAID', 'LUNAS', 'COMPLETED', 'SELESAI')`,
      );
    }
    if (settledStatusPredicates.length > 0) {
      settledStatusFilters.push(`(${settledStatusPredicates.join(' OR ')})`);
    }

    const settledRevenueResult = await client.query(
      `SELECT COALESCE(SUM(${salesAmountColumn}), 0)::numeric AS total_revenue
       FROM sales_records
       WHERE ${settledStatusFilters.join(' AND ')}`,
      salesParams,
    );

    let totalVoidTransactions = 0;
    if (salesStatusColumn) {
      const voidFilters = ['tenant_id = $1'];
      const voidParams = [tenantId];
      if (salesCreatedAtColumn) {
        voidFilters.push(
          `(${salesCreatedAtColumn} >= $2::timestamptz AND ${salesCreatedAtColumn} < $3::timestamptz)`,
        );
        voidParams.push(startUtcIso, endUtcIso);
      }
      voidFilters.push(`COALESCE(LOWER(${salesStatusColumn}::text), '') = 'void'`);

      const voidResult = await client.query(
        `SELECT COUNT(*)::int AS total_void_transactions
         FROM sales_records
         WHERE ${voidFilters.join(' AND ')}`,
        voidParams,
      );
      totalVoidTransactions = toInt(voidResult.rows?.[0]?.total_void_transactions);
    }

    const expenseAmountColumn = expensesColumns.has('amount')
      ? 'amount'
      : (expensesColumns.has('nominal') ? 'nominal' : '0');
    const expensePaymentColumn = expensesColumns.has('payment_method')
      ? 'payment_method'
      : null;
    const expenseStatusColumn = expensesColumns.has('status') ? 'status' : null;
    const expenseDateColumn = expensesColumns.has('created_at')
      ? 'created_at'
      : (expensesColumns.has('expense_date') ? 'expense_date' : null);

    const expenseFilters = ['tenant_id = $1'];
    const expenseParams = [tenantId];
    if (expenseDateColumn) {
      if (expenseDateColumn === 'created_at') {
        expenseFilters.push(
          `(${expenseDateColumn} >= $2::timestamptz AND ${expenseDateColumn} < $3::timestamptz)`,
        );
        expenseParams.push(startUtcIso, endUtcIso);
      } else {
        expenseFilters.push(
          `((${expenseDateColumn} AT TIME ZONE '${WIB_TIME_ZONE}')::date = (CURRENT_TIMESTAMP AT TIME ZONE '${WIB_TIME_ZONE}')::date)`,
        );
      }
    }
    if (expenseStatusColumn) {
      expenseFilters.push(`COALESCE(LOWER(${expenseStatusColumn}::text), '') <> 'void'`);
    }

    const normalizedExpensePaymentExpression = expensePaymentColumn
      ? `REPLACE(REPLACE(REPLACE(UPPER(COALESCE(${expensePaymentColumn}::text, '')), ' ', ''), '-', ''), '_', '')`
      : "''";

    const expensesResult = await client.query(
      `SELECT
         COALESCE(SUM(${expenseAmountColumn}), 0)::numeric AS total_expenses,
         COALESCE(SUM(CASE
           WHEN ${normalizedExpensePaymentExpression} IN ('CASH', 'TUNAI')
             THEN ${expenseAmountColumn}
           ELSE 0
         END), 0)::numeric AS total_expenses_cash,
         COALESCE(SUM(CASE
           WHEN ${normalizedExpensePaymentExpression} NOT IN ('CASH', 'TUNAI')
             THEN ${expenseAmountColumn}
           ELSE 0
         END), 0)::numeric AS total_expenses_non_cash
       FROM expenses
       WHERE ${expenseFilters.join(' AND ')}`,
      expenseParams,
    );

    const pettyCashLogsResult = await client.query(
      `SELECT
         COALESCE(SUM(CASE
           WHEN UPPER(COALESCE(type, 'IN')) = 'OUT' THEN -amount
           ELSE amount
         END), 0)::numeric AS total_petty_cash_logs
       FROM petty_cash_logs
       WHERE tenant_id = $1
         AND (created_at >= $2::timestamptz AND created_at < $3::timestamptz)`,
      [tenantId, startUtcIso, endUtcIso],
    );

    let totalDailyCashOpening = 0;
    if (dailyCashColumns.has('modal_awal')) {
      if (dailyCashColumns.has('tanggal')) {
        const dailyCashResult = await client.query(
          `SELECT COALESCE(MAX(modal_awal), 0)::numeric AS total_daily_cash_opening
           FROM daily_cash
           WHERE tenant_id = $1
             AND tanggal = TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE '${WIB_TIME_ZONE}', 'YYYY-MM-DD')`,
          [tenantId],
        );
        totalDailyCashOpening = toInt(dailyCashResult.rows?.[0]?.total_daily_cash_opening);
      } else if (dailyCashColumns.has('created_at')) {
        const dailyCashResult = await client.query(
          `SELECT COALESCE(MAX(modal_awal), 0)::numeric AS total_daily_cash_opening
           FROM daily_cash
           WHERE tenant_id = $1
             AND (created_at >= $2::timestamptz AND created_at < $3::timestamptz)`,
          [tenantId, startUtcIso, endUtcIso],
        );
        totalDailyCashOpening = toInt(dailyCashResult.rows?.[0]?.total_daily_cash_opening);
      }
    }

    const totalSales = toInt(salesResult.rows?.[0]?.total_sales || 0);
    const totalGrossProfit = toInt(salesResult.rows?.[0]?.total_gross_profit || 0);
    const totalRevenue = toInt(settledRevenueResult.rows?.[0]?.total_revenue || 0);
    const totalTransactions = toInt(salesResult.rows?.[0]?.total_transactions || 0);
    const totalIncomeCash = toInt(salesResult.rows?.[0]?.total_income_cash || 0);
    const totalIncomeNonCash = toInt(salesResult.rows?.[0]?.total_income_non_cash || 0);
    const totalExpenses = toInt(expensesResult.rows?.[0]?.total_expenses || 0);
    const totalExpensesCash = toInt(expensesResult.rows?.[0]?.total_expenses_cash || 0);
    const totalExpensesNonCash = toInt(expensesResult.rows?.[0]?.total_expenses_non_cash || 0);
    const totalPettyCashLogs = toInt(pettyCashLogsResult.rows?.[0]?.total_petty_cash_logs || 0);
    const totalDailyCashOpeningSafe = toInt(totalDailyCashOpening || 0);
    const totalPettyCash = Math.max(totalPettyCashLogs, totalDailyCashOpeningSafe);

    const productFilters = ['tenant_id = $1'];
    if (productColumns.has('is_active')) {
      productFilters.push('COALESCE(is_active, true) = true');
    }
    const productCountResult = await client.query(
      `SELECT COUNT(*)::int AS total_products
       FROM products
       WHERE ${productFilters.join(' AND ')}`,
      [tenantId],
    );
    const totalProducts = toInt(productCountResult.rows?.[0]?.total_products || 0);

    return jsonOk(
      res,
      {
        time_zone: WIB_TIME_ZONE,
        summary_date: wibDate,
        total_sales: totalSales,
        total_revenue: totalRevenue,
        gross_profit: totalGrossProfit,
        total_transactions: totalTransactions,
        total_void_transactions: totalVoidTransactions,
        total_expenses: totalExpenses,
        total_expenses_cash: totalExpensesCash,
        total_expenses_non_cash: totalExpensesNonCash,
        total_income_cash: totalIncomeCash,
        total_income_non_cash: totalIncomeNonCash,
        total_petty_cash: totalPettyCash,
        total_products: totalProducts,
        petty_cash_logs_total: totalPettyCashLogs,
        daily_cash_opening_total: totalDailyCashOpeningSafe,
        cash_drawer_income: totalIncomeCash + totalPettyCash,
      },
      'Dashboard ringkasan hari ini berhasil dimuat',
    );
  } catch (error) {
    console.error('Dashboard Aggregation Error:', error);
    return jsonError(
      res,
      500,
      'Gagal memuat ringkasan dashboard hari ini',
      error.message,
    );
  } finally {
    client.release();
  }
};

module.exports = {
  getTodayDashboardSummary,
};
