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

const ensurePettyCashLogsTable = async (client) => {
  await assertColumnsExist(client, 'petty_cash_logs', [
    'id',
    'tenant_id',
    'user_id',
    'user_name',
    'amount',
    'type',
    'notes',
    'created_at',
  ]);
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
    await ensurePettyCashLogsTable(client);

    const tenantId = resolveTenantIdFromRequest(req);

    if (!tenantId) {
      return jsonError(res, 401, 'Tenant tidak valid');
    }

    const result = await client.query(
      `SELECT l.id, l.tenant_id, l.user_id, l.amount, l.type, l.notes, l.created_at,
              COALESCE(NULLIF(l.user_name, ''), u.username, '') AS user_name
       FROM petty_cash_logs l
       LEFT JOIN app_users u
         ON CAST(u.id AS TEXT) = l.user_id
         OR u.username = l.user_id
       WHERE l.tenant_id = $1
         AND (l.created_at AT TIME ZONE '${WIB_TIME_ZONE}')::date =
             (CURRENT_TIMESTAMP AT TIME ZONE '${WIB_TIME_ZONE}')::date
       ORDER BY l.created_at DESC`,
      [tenantId],
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

    if (!tenantId) {
      return jsonError(res, 401, 'Tenant tidak valid');
    }

    if (!Number.isInteger(amount) || amount <= 0) {
      return jsonError(res, 400, 'Nominal petty cash harus lebih dari 0');
    }

    await ensurePettyCashLogsTable(client);

    const insertResult = await client.query(
      `INSERT INTO petty_cash_logs (
         id,
         tenant_id,
         user_id,
         user_name,
         amount,
         type,
         notes
       ) VALUES ($1::text, $2::text, $3::text, $4::text, $5::numeric, $6::text, $7::text)
       RETURNING id, tenant_id, user_id, user_name, amount, type, notes, created_at`,
      [id, tenantId, userId || null, userName || null, Number.isFinite(Number(amount)) ? Number(amount) : 0, type, notes || null],
    );

    const createdLog = mapPettyCashRow({
      ...(insertResult.rows[0] || {}),
      user_name: userName,
    });
    emitPettyCashUpdated(req, createdLog);

    return jsonOk(res, createdLog, 'Petty cash berhasil disimpan', 201);
  } catch (error) {
    console.error('Dashboard Aggregation Error:', error);
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