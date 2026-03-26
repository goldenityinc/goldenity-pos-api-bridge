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

const ensurePettyCashLogsTable = async (client) => {
  await client.query(`
    CREATE TABLE IF NOT EXISTS petty_cash_logs (
      id UUID PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      user_id TEXT,
      user_name TEXT,
      amount INTEGER NOT NULL,
      type VARCHAR(3) NOT NULL DEFAULT 'IN',
      notes TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await client.query(`
    ALTER TABLE petty_cash_logs
    ADD COLUMN IF NOT EXISTS user_name TEXT;
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_petty_cash_logs_created_at
    ON petty_cash_logs (created_at DESC);
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_petty_cash_logs_tenant_created_at
    ON petty_cash_logs (tenant_id, created_at DESC);
  `);
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

    const tenantId = (req?.tenant?.tenantId ?? '').toString().trim();

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
    const tenantId = (req?.tenant?.tenantId ?? '').toString().trim();
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
       ) VALUES ($1, $2, $3, $4, $5, $6, $7)
       RETURNING id, tenant_id, user_id, user_name, amount, type, notes, created_at`,
      [id, tenantId, userId || null, userName || null, amount, type, notes || null],
    );

    const createdLog = mapPettyCashRow({
      ...(insertResult.rows[0] || {}),
      user_name: userName,
    });
    emitPettyCashUpdated(req, createdLog);

    return jsonOk(res, createdLog, 'Petty cash berhasil disimpan', 201);
  } catch (error) {
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