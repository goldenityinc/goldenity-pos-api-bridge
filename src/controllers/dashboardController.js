const { jsonOk, jsonError } = require('../utils/http');
const { ensureTenantScopedTable } = require('../utils/tenantScope');
const { ensurePettyCashLogsTable } = require('./pettyCashController');

const WIB_TIME_ZONE = 'Asia/Jakarta';

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

const getTodayDashboardSummary = async (req, res) => {
  const client = await req.tenantDb.connect();

  try {
    const tenantId = (req?.tenant?.tenantId ?? req?.auth?.tenantId ?? '')
      .toString()
      .trim();

    if (!tenantId) {
      return jsonError(res, 401, 'Tenant tidak valid');
    }

    await ensureTenantScopedTable(client, 'sales_records', tenantId);
    await ensureTenantScopedTable(client, 'expenses', tenantId);
    await ensureTenantScopedTable(client, 'daily_cash', tenantId);
    await ensurePettyCashLogsTable(client);
    await ensureTenantScopedTable(client, 'petty_cash_logs', tenantId);

    const salesColumns = await getTableColumnSet(client, 'sales_records');
    const expensesColumns = await getTableColumnSet(client, 'expenses');
    const dailyCashColumns = await getTableColumnSet(client, 'daily_cash');

    const salesAmountColumn = salesColumns.has('total_price')
      ? 'total_price'
      : (salesColumns.has('total_amount') ? 'total_amount' : '0');
    const salesPaymentColumn = salesColumns.has('payment_method')
      ? 'payment_method'
      : (salesColumns.has('payment_type') ? 'payment_type' : null);
    const salesStatusColumn = salesColumns.has('status') ? 'status' : null;
    const salesCreatedAtColumn = salesColumns.has('created_at') ? 'created_at' : null;

    const salesFilters = ['tenant_id = $1'];
    if (salesCreatedAtColumn) {
      salesFilters.push(
        `((${salesCreatedAtColumn} AT TIME ZONE '${WIB_TIME_ZONE}')::date = (CURRENT_TIMESTAMP AT TIME ZONE '${WIB_TIME_ZONE}')::date)`,
      );
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
      [tenantId],
    );

    let totalVoidTransactions = 0;
    if (salesStatusColumn) {
      const voidFilters = ['tenant_id = $1'];
      if (salesCreatedAtColumn) {
        voidFilters.push(
          `((${salesCreatedAtColumn} AT TIME ZONE '${WIB_TIME_ZONE}')::date = (CURRENT_TIMESTAMP AT TIME ZONE '${WIB_TIME_ZONE}')::date)`,
        );
      }
      voidFilters.push(`COALESCE(LOWER(${salesStatusColumn}::text), '') = 'void'`);

      const voidResult = await client.query(
        `SELECT COUNT(*)::int AS total_void_transactions
         FROM sales_records
         WHERE ${voidFilters.join(' AND ')}`,
        [tenantId],
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
    if (expenseDateColumn) {
      expenseFilters.push(
        `((${expenseDateColumn} AT TIME ZONE '${WIB_TIME_ZONE}')::date = (CURRENT_TIMESTAMP AT TIME ZONE '${WIB_TIME_ZONE}')::date)`,
      );
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
      [tenantId],
    );

    const pettyCashLogsResult = await client.query(
      `SELECT
         COALESCE(SUM(CASE
           WHEN UPPER(COALESCE(type, 'IN')) = 'OUT' THEN -amount
           ELSE amount
         END), 0)::numeric AS total_petty_cash_logs
       FROM petty_cash_logs
       WHERE tenant_id = $1
         AND ((created_at AT TIME ZONE '${WIB_TIME_ZONE}')::date = (CURRENT_TIMESTAMP AT TIME ZONE '${WIB_TIME_ZONE}')::date)`,
      [tenantId],
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
             AND ((created_at AT TIME ZONE '${WIB_TIME_ZONE}')::date = (CURRENT_TIMESTAMP AT TIME ZONE '${WIB_TIME_ZONE}')::date)`,
          [tenantId],
        );
        totalDailyCashOpening = toInt(dailyCashResult.rows?.[0]?.total_daily_cash_opening);
      }
    }

    const totalSales = toInt(salesResult.rows?.[0]?.total_sales);
    const totalTransactions = toInt(salesResult.rows?.[0]?.total_transactions);
    const totalIncomeCash = toInt(salesResult.rows?.[0]?.total_income_cash);
    const totalIncomeNonCash = toInt(salesResult.rows?.[0]?.total_income_non_cash);
    const totalExpenses = toInt(expensesResult.rows?.[0]?.total_expenses);
    const totalExpensesCash = toInt(expensesResult.rows?.[0]?.total_expenses_cash);
    const totalExpensesNonCash = toInt(expensesResult.rows?.[0]?.total_expenses_non_cash);
    const totalPettyCashLogs = toInt(pettyCashLogsResult.rows?.[0]?.total_petty_cash_logs);
    const totalPettyCash = Math.max(totalPettyCashLogs, totalDailyCashOpening);

    return jsonOk(
      res,
      {
        time_zone: WIB_TIME_ZONE,
        summary_date: new Date().toLocaleDateString('en-CA', { timeZone: WIB_TIME_ZONE }),
        total_sales: totalSales,
        total_transactions: totalTransactions,
        total_void_transactions: totalVoidTransactions,
        total_expenses: totalExpenses,
        total_expenses_cash: totalExpensesCash,
        total_expenses_non_cash: totalExpensesNonCash,
        total_income_cash: totalIncomeCash,
        total_income_non_cash: totalIncomeNonCash,
        total_petty_cash: totalPettyCash,
        petty_cash_logs_total: totalPettyCashLogs,
        daily_cash_opening_total: totalDailyCashOpening,
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
