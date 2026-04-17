const crypto = require('crypto');

const { jsonOk, jsonError } = require('../utils/http');
const { emitPettyCashUpdated } = require('../services/realtimeEmitter');

const WIB_TIME_ZONE = 'Asia/Jakarta';

const normalizeType = (value) => {
  const normalized = (value ?? 'IN').toString().trim().toUpperCase();
  return normalized === 'OUT' ? 'OUT' : 'IN';
};

const toIntegerAmount = (value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return NaN;
  }
  return Math.round(parsed);
};

const normalizeUtcIsoString = (value) => {
  if (!value) {
    return null;
  }

  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value.toISOString();
  }

  const text = value.toString().trim();
  if (!text) {
    return null;
  }

  const parsedDate = new Date(text);
  if (Number.isNaN(parsedDate.getTime())) {
    return null;
  }

  return parsedDate.toISOString();
};

const resolveUserIdFromRequest = (req) => {
  return (
    req?.auth?.userId ??
    req?.auth?.user_id ??
    req?.auth?.sub ??
    req?.body?.userId ??
    req?.body?.user_id ??
    req?.auth?.username ??
    ''
  )
    .toString()
    .trim();
};

const resolveUsernameFromRequest = (req) => {
  return (
    req?.auth?.username ??
    req?.auth?.userName ??
    req?.auth?.user_name ??
    req?.body?.userName ??
    req?.body?.user_name ??
    ''
  )
    .toString()
    .trim();
};

const resolveTenantIdFromRequest = (req) => {
  return (
    req?.user?.tenantId ??
    req?.user?.tenant_id ??
    req?.tenant?.tenantId ??
    req?.auth?.tenantId ??
    req?.auth?.tenant_id ??
    ''
  )
    .toString()
    .trim();
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

const listTableColumns = async (client, table) => {
  const result = await client.query(
    `SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = ANY(current_schemas(false))
       AND table_name = $1`,
    [table],
  );
  return new Set((result.rows || []).map((row) => row.column_name));
};

const ensurePettyCashLogsTable = async (client) => {
  await assertColumnsExist(client, 'petty_cash_logs', [
    'id',
    'amount',
    'type',
    'notes',
  ]);
  return listTableColumns(client, 'petty_cash_logs');
};

const mapPettyCashRow = (row = {}) => ({
  id: row.id,
  tenantId: row.tenant_id,
  tenant_id: row.tenant_id,
  userId: row.user_id,
  user_id: row.user_id,
  userName: (row.user_name ?? '').toString().trim(),
  user_name: (row.user_name ?? '').toString().trim(),
  amount: Number(row.amount ?? 0),
  type: normalizeType(row.type),
  notes: row.notes ?? '',
  createdAt: normalizeUtcIsoString(row.created_at),
  created_at: normalizeUtcIsoString(row.created_at),
});

const getTodayPettyCashLogs = async (req, res) => {
  const client = await req.tenantDb.connect();

  try {
    const columns = await ensurePettyCashLogsTable(client);

    const tenantId = resolveTenantIdFromRequest(req);
    const hasTenantId = columns.has('tenant_id');
    const hasUserId = columns.has('user_id');
    const hasUserName = columns.has('user_name');
    const hasCreatedAt = columns.has('created_at');

    if (hasTenantId && !tenantId) {
      return jsonError(res, 401, 'Tenant tidak valid');
    }

    const selectFields = [
      'l.id',
      hasTenantId ? 'l.tenant_id' : 'NULL::text AS tenant_id',
      hasUserId ? 'l.user_id' : 'NULL::text AS user_id',
      'l.amount',
      'l.type',
      'l.notes',
      hasCreatedAt ? 'l.created_at' : 'NULL::timestamp AS created_at',
      hasUserName
        ? "COALESCE(NULLIF(l.user_name, ''), u.username, '') AS user_name"
        : "COALESCE(u.username, '') AS user_name",
    ];
    const whereConditions = [];
    const params = [];

    if (hasTenantId) {
      params.push(tenantId);
      whereConditions.push(`l.tenant_id = $${params.length}`);
    }

    if (hasCreatedAt) {
      whereConditions.push(
        `(l.created_at AT TIME ZONE '${WIB_TIME_ZONE}')::date = (CURRENT_TIMESTAMP AT TIME ZONE '${WIB_TIME_ZONE}')::date`,
      );
    }

    const whereClause = whereConditions.length > 0
      ? `WHERE ${whereConditions.join(' AND ')}`
      : '';

    const result = await client.query(
      `SELECT ${selectFields.join(', ')}
       FROM petty_cash_logs l
       LEFT JOIN app_users u
         ON ${hasUserId ? 'CAST(u.id AS TEXT) = l.user_id OR u.username = l.user_id' : 'FALSE'}
       ${whereClause}
       ORDER BY l.created_at DESC`,
      params,
    );

    return jsonOk(
      res,
      result.rows.map(mapPettyCashRow),
      'Petty cash hari ini berhasil dimuat',
    );
  } catch (error) {
    console.error('Dashboard Aggregation Error:', error);
    return jsonError(
      res,
      500,
      'Gagal memuat petty cash hari ini',
      error.message,
    );
  } finally {
    client.release();
  }
};

const createPettyCashLog = async (req, res) => {
  const client = await req.tenantDb.connect();

  try {
    const tenantId = resolveTenantIdFromRequest(req);
    const userId = resolveUserIdFromRequest(req);
    const userName = resolveUsernameFromRequest(req);
    const amount = toIntegerAmount(req.body?.amount);
    const type = normalizeType(req.body?.type);
    const notes = (req.body?.notes ?? '').toString().trim();
    const id = crypto.randomUUID();

    const columns = await ensurePettyCashLogsTable(client);
    const hasTenantId = columns.has('tenant_id');
    const hasUserId = columns.has('user_id');
    const hasUserName = columns.has('user_name');
    const hasCreatedAt = columns.has('created_at');

    if (hasTenantId && !tenantId) {
      return jsonError(res, 401, 'Tenant tidak valid');
    }

    if (!Number.isInteger(amount) || amount <= 0) {
      return jsonError(res, 400, 'Nominal petty cash harus lebih dari 0');
    }

    const insertColumns = ['id'];
    const insertValues = [id];

    if (hasTenantId) {
      insertColumns.push('tenant_id');
      insertValues.push(tenantId || null);
    }
    if (hasUserId) {
      insertColumns.push('user_id');
      insertValues.push(userId || null);
    }
    if (hasUserName) {
      insertColumns.push('user_name');
      insertValues.push(userName || null);
    }

    insertColumns.push('amount');
    insertValues.push(Number(amount));
    insertColumns.push('type');
    insertValues.push(type);
    insertColumns.push('notes');
    insertValues.push(notes || null);

    const placeholders = insertColumns.map((_, index) => `$${index + 1}`).join(', ');
    const returningColumns = [
      'id',
      hasTenantId ? 'tenant_id' : 'NULL::text AS tenant_id',
      hasUserId ? 'user_id' : 'NULL::text AS user_id',
      hasUserName ? 'user_name' : 'NULL::text AS user_name',
      'amount',
      'type',
      'notes',
      hasCreatedAt ? 'created_at' : 'NULL::timestamp AS created_at',
    ];

    const insertResult = await client.query(
      `INSERT INTO petty_cash_logs (
         ${insertColumns.join(', ')}
       ) VALUES (${placeholders})
       RETURNING ${returningColumns.join(', ')}`,
      insertValues,
    );

    const createdLog = mapPettyCashRow({
      ...(insertResult.rows[0] || {}),
      user_name: userName,
    });
    emitPettyCashUpdated(req, createdLog);

    return jsonOk(res, createdLog, 'Petty cash berhasil disimpan', 201);
  } catch (error) {
    console.error('Petty Cash Create Error:', {
      message: error.message,
      code: error.code,
      detail: error.detail,
      hint: error.hint,
      constraint: error.constraint,
      table: error.table,
      column: error.column,
      routine: error.routine,
      stack: error.stack,
    });
    return jsonError(
      res,
      500,
      'Gagal menyimpan petty cash',
      error.message,
    );
  } finally {
    client.release();
  }
};

module.exports = {
  getTodayPettyCashLogs,
  createPettyCashLog,
  ensurePettyCashLogsTable,
};