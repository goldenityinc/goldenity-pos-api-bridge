const { jsonOk, jsonError } = require('../utils/http');
const bcrypt = require('bcryptjs');
const { emitTableMutation } = require('../services/realtimeEmitter');
const {
  parseBodyArray,
  parseBodyObject,
  buildInsertQuery,
  buildUpdateQuery,
  buildDeleteQuery,
  getTableColumnDefinitions,
  normalizePayloadByColumnDefinitions,
  enforceTenantIdOnPayload,
  normalizeTenantId,
  runSelect,
} = require('../utils/sqlHelpers');
const { ensureTenantScopedTable } = require('../utils/tenantScope');
const {
  getCachedResponse,
  storeResponse,
} = require('../utils/idempotencyCache');

const BCRYPT_REGEX = /^\$2[aby]\$\d{2}\$.{53}$/;

class BadRequestError extends Error {
  constructor(message) {
    super(message);
    this.name = 'BadRequestError';
    this.statusCode = 400;
  }
}

const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

const TABLE_CLEAR_STATUS = 'AVAILABLE';
const ACTIVE_PAYMENT_STATUS_SET = new Set([
  'UNPAID',
  'OPEN',
  'PENDING_PAYMENT',
]);
const ACTIVE_ORDER_STATUS_SET = new Set([
  'UNPAID',
  'OPEN',
  'PENDING',
  'PENDING_PAYMENT',
  'PREPARING',
  'READY_FOR_PICKUP',
]);
const NON_CANCELLABLE_ORDER_STATUS_SET = new Set([
  'PAID',
  'SETTLED',
  'SETTLEMENT',
  'SUCCESS',
  'COMPLETED',
  'LUNAS',
  'VOID',
  'VOIDED',
  'CANCELLED',
  'CANCELED',
  'FAILED',
  'EXPIRED',
  'DENIED',
  'BATAL',
  'DIBATALKAN',
]);

const normalizeStatusToken = (value) => (value ?? '').toString().trim().toUpperCase();

const shouldAutoCancelOrdersBeforeClearingTable = (table, payload = {}) => {
  if (table !== 'tables' || !payload || typeof payload !== 'object') {
    return false;
  }

  return normalizeStatusToken(payload.status) === TABLE_CLEAR_STATUS;
};

const isOpenLinkedTableOrder = (row = {}) => {
  if (!row || typeof row !== 'object') {
    return false;
  }

  if (row.is_void === true || row.isVoid === true) {
    return false;
  }

  const status = normalizeStatusToken(row.status);
  const orderStatus = normalizeStatusToken(row.order_status ?? row.orderStatus);
  const transactionStatus = normalizeStatusToken(
    row.transaction_status ?? row.transactionStatus,
  );
  const paymentStatus = normalizeStatusToken(row.payment_status ?? row.paymentStatus);
  const tokens = [status, orderStatus, transactionStatus, paymentStatus].filter(Boolean);

  if (tokens.some((token) => NON_CANCELLABLE_ORDER_STATUS_SET.has(token))) {
    return false;
  }

  if (ACTIVE_PAYMENT_STATUS_SET.has(paymentStatus)) {
    return true;
  }

  return [status, orderStatus, transactionStatus].some((token) => (
    ACTIVE_ORDER_STATUS_SET.has(token)
  ));
};

const cancelLinkedOrdersForClearedTable = async ({
  db,
  tenantId,
  tableId,
}) => {
  await ensureTenantScopedTable(db, 'sales_records', tenantId);
  const columnDefinitions = await getTableColumnDefinitions(db, 'sales_records');
  const columnSet = new Set(columnDefinitions.keys());

  if (!columnSet.has('table_id')) {
    return [];
  }

  const selectValues = [tableId];
  let selectSql = 'SELECT * FROM "sales_records" WHERE "table_id" = $1';
  if (columnSet.has('tenant_id')) {
    selectValues.push(tenantId);
    selectSql += ` AND "tenant_id" = $${selectValues.length}`;
  }

  const existingOrdersResult = await db.query(selectSql, selectValues);
  const ordersToCancel = (existingOrdersResult.rows || []).filter(isOpenLinkedTableOrder);

  if (ordersToCancel.length === 0) {
    return [];
  }

  const updateEntries = [];
  const addUpdate = (columnName, value) => {
    if (!columnSet.has(columnName)) {
      return;
    }

    updateEntries.push({ columnName, value });
  };

  const nowIso = new Date().toISOString();
  addUpdate('status', 'CANCELLED');
  addUpdate('order_status', 'CANCELLED');
  addUpdate('transaction_status', 'VOID');
  addUpdate('payment_status', 'CANCELLED');
  addUpdate('is_void', true);
  addUpdate('isVoid', true);
  addUpdate('void_reason', 'TABLE_CLEARED');
  addUpdate('voided_at', nowIso);
  addUpdate('updated_at', nowIso);

  if (updateEntries.length === 0) {
    return [];
  }

  const cancelledIds = ordersToCancel
    .map((row) => Number.parseInt((row?.id ?? '').toString(), 10))
    .filter((value) => Number.isFinite(value) && value > 0);

  if (cancelledIds.length === 0) {
    return [];
  }

  const updateValues = [];
  const setClauses = updateEntries.map(({ columnName, value }, index) => {
    updateValues.push(value);
    return `"${columnName}" = $${index + 1}`;
  });

  updateValues.push(cancelledIds);
  const idParamPosition = updateValues.length;
  let updateSql = `UPDATE "sales_records"
                   SET ${setClauses.join(', ')}
                   WHERE "id" = ANY($${idParamPosition}::bigint[])`;

  if (columnSet.has('tenant_id')) {
    updateValues.push(tenantId);
    updateSql += ` AND "tenant_id" = $${updateValues.length}`;
  }

  updateSql += ' RETURNING *';

  const cancelledResult = await db.query(updateSql, updateValues);
  return cancelledResult.rows || [];
};

const getQueryableClient = async (tenantDb) => {
  const tenantDbIsPool = !!tenantDb
    && typeof tenantDb === 'object'
    && typeof tenantDb.query === 'function'
    && typeof tenantDb.connect === 'function'
    && typeof tenantDb.release !== 'function';
  if (tenantDbIsPool) {
    const client = await tenantDb.connect();
    return {
      db: client,
      release: () => client.release(),
      supportsTransactions: true,
    };
  }

  if (tenantDb && typeof tenantDb === 'object' && typeof tenantDb.release === 'function') {
    return {
      db: tenantDb,
      release: () => {},
      supportsTransactions: true,
    };
  }

  return {
    db: tenantDb,
    release: () => {},
    supportsTransactions: false,
  };
};

const isIntegerColumnDefinition = (columnDefinition = {}) => {
  const dataType = `${columnDefinition.dataType || ''}`.toLowerCase();
  const udtName = `${columnDefinition.udtName || ''}`.toLowerCase();

  return (
    dataType === 'smallint' ||
    dataType === 'integer' ||
    dataType === 'bigint' ||
    udtName === 'int2' ||
    udtName === 'int4' ||
    udtName === 'int8'
  );
};

const sanitizeClientGeneratedPrimaryKey = (payload, columnDefinitions, options = {}) => {
  const { table } = options;
  if (!(columnDefinitions instanceof Map) || !isIntegerColumnDefinition(columnDefinitions.get('id'))) {
    return payload;
  }

  const sanitizeRow = (row) => {
    if (!row || typeof row !== 'object' || Array.isArray(row)) {
      return row;
    }

    const hasOwnId = Object.prototype.hasOwnProperty.call(row, 'id');
    if (!hasOwnId) {
      return row;
    }

    const rawId = row.id;
    if (rawId === undefined || rawId === null) {
      const next = { ...row };
      delete next.id;
      return next;
    }

    const idText = rawId.toString().trim();
    if (!idText) {
      const next = { ...row };
      delete next.id;
      return next;
    }

    if (/^\d+$/.test(idText)) {
      return row;
    }

    const next = { ...row };
    const referenceId = (
      next.reference_id ?? next.referenceId ?? next.local_id ?? next.localId ?? ''
    )
      .toString()
      .trim();

    if (!referenceId) {
      next.reference_id = idText;
    }

    delete next.id;
    return next;
  };

  if (Array.isArray(payload)) {
    return payload.map(sanitizeRow);
  }

  return sanitizeRow(payload);
};

const normalizeSupplierPayloadObject = (payload = {}) => {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return payload;
  }

  const next = { ...payload };
  const normalizedName = (
    next.name ?? next.nama_toko ?? next.namaToko ?? next.store_name ?? ''
  )
    .toString()
    .trim();
  const normalizedPhone = (
    next.phone ?? next.kontak ?? next.contact ?? ''
  )
    .toString()
    .trim();
  const normalizedAddress = (
    next.address ?? next.alamat ?? next.store_address ?? ''
  )
    .toString()
    .trim();

  if (normalizedName) {
    next.name = normalizedName;
    next.nama_toko = normalizedName;
  }
  if (
    normalizedPhone ||
    Object.prototype.hasOwnProperty.call(next, 'phone') ||
    Object.prototype.hasOwnProperty.call(next, 'kontak') ||
    Object.prototype.hasOwnProperty.call(next, 'contact')
  ) {
    next.phone = normalizedPhone;
    next.kontak = normalizedPhone;
  }
  if (
    normalizedAddress ||
    Object.prototype.hasOwnProperty.call(next, 'address') ||
    Object.prototype.hasOwnProperty.call(next, 'alamat') ||
    Object.prototype.hasOwnProperty.call(next, 'store_address')
  ) {
    next.address = normalizedAddress;
    next.alamat = normalizedAddress;
  }

  return next;
};

const normalizeCategoryPayloadObject = (payload = {}) => {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return payload;
  }

  const next = { ...payload };
  const rawType = (next.category_type ?? next.type ?? '').toString().trim().toLowerCase();
  if (!rawType) {
    return next;
  }

  const normalizedType =
    rawType === 'expense' || rawType === 'pengeluaran' ? 'EXPENSE' : 'PRODUCT';
  next.category_type = normalizedType;
  next.type = normalizedType;
  return next;
};

const normalizeAppUserPayloadObject = (payload = {}, options = {}) => {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return payload;
  }

  const next = { ...payload };
  const normalizedTenantId = normalizeTenantId(
    options.tenantId ||
    next.tenant_id ||
    next.tenantId ||
    next.user_tenant_id ||
    next.userTenantId,
  );

  if (normalizedTenantId) {
    next.tenant_id = normalizedTenantId;
    next.tenantId = normalizedTenantId;
  }

  const rawRole = (
    next.role ??
    next.user_role ??
    next.employee_role ??
    next.employeeRole ??
    ''
  )
    .toString()
    .trim()
    .toUpperCase();

  const rawEmployeeType = (
    next.employee_type ??
    next.employeeType ??
    next.type ??
    ''
  )
    .toString()
    .trim()
    .toUpperCase();

  const resolvedRole = rawRole || (rawEmployeeType === 'MECHANIC' ? 'MECHANIC' : 'CRM_STAFF');
  if (resolvedRole) {
    next.role = resolvedRole;
  }

  if (!rawEmployeeType && resolvedRole === 'MECHANIC') {
    next.employee_type = 'MECHANIC';
  }

  return next;
};

const normalizePayloadByTable = (table, payload, options = {}) => {
  const normalizeRow = (row) => {
    if (!row || typeof row !== 'object' || Array.isArray(row)) {
      return row;
    }

    if (table === 'suppliers') {
      return normalizeSupplierPayloadObject(row);
    }

    if (table === 'categories') {
      return normalizeCategoryPayloadObject(row);
    }

    if (table === 'app_users') {
      return normalizeAppUserPayloadObject(row, options);
    }

    return row;
  };

  if (Array.isArray(payload)) {
    return payload.map(normalizeRow);
  }

  return normalizeRow(payload);
};

const resolveTenantIdFromRequest = (req) => {
  return normalizeTenantId(
    req?.user?.tenantId ||
      req?.user?.tenant_id ||
      req?.tenant?.tenantId ||
      req?.auth?.tenantId ||
      req?.auth?.tenant_id,
  );
};

const validateIdValueForTable = (columnDefinitions, idField, rawId) => {
  if (!(columnDefinitions instanceof Map) || !columnDefinitions.has(idField)) {
    return;
  }

  const idValue = (rawId ?? '').toString().trim();
  if (!idValue) {
    throw new BadRequestError('ID wajib diisi');
  }

  const idColumn = columnDefinitions.get(idField) || {};
  const dataType = `${idColumn.dataType || ''}`.toLowerCase();
  const udtName = `${idColumn.udtName || ''}`.toLowerCase();

  const isIntegerId =
    dataType === 'smallint' ||
    dataType === 'integer' ||
    dataType === 'bigint' ||
    udtName === 'int2' ||
    udtName === 'int4' ||
    udtName === 'int8';

  if (isIntegerId && !/^\d+$/.test(idValue)) {
    throw new BadRequestError(`ID ${idField} harus numerik`);
  }

  const isUuidId = dataType === 'uuid' || udtName === 'uuid';
  if (isUuidId && !UUID_REGEX.test(idValue)) {
    throw new BadRequestError(`ID ${idField} harus UUID valid`);
  }
};

async function normalizeUserPassword(table, payload, options = {}) {
  if (table !== 'app_users') {
    return payload;
  }

  if (!payload || typeof payload !== 'object') {
    return payload;
  }

  const next = { ...payload };
  const isCreate = options.isCreate === true;

  if (isCreate && !Object.prototype.hasOwnProperty.call(next, 'password')) {
    throw new BadRequestError('Field password wajib diisi untuk membuat user');
  }

  const rawPassword = next.password;

  if (typeof rawPassword !== 'string') {
    return next;
  }

  const trimmedPassword = rawPassword.trim();
  if (!trimmedPassword) {
    throw new BadRequestError('Password tidak boleh kosong');
  }

  if (BCRYPT_REGEX.test(trimmedPassword)) {
    return next;
  }

  const saltRounds = Number.parseInt(process.env.BCRYPT_SALT_ROUNDS || '12', 10);
  const safeSaltRounds = Number.isNaN(saltRounds) ? 12 : Math.min(Math.max(saltRounds, 4), 14);
  next.password = await bcrypt.hash(trimmedPassword, safeSaltRounds);
  return next;
}

const createCrudController = (table) => ({
  list: async (req, res) => {
    try {
      const tenantId = resolveTenantIdFromRequest(req);
      await ensureTenantScopedTable(req.tenantDb, table, tenantId);
      const rows = await runSelect(req.tenantDb, table, req.query, { tenantId });
      return jsonOk(res, rows);
    } catch (error) {
      return jsonError(res, 500, error.message || 'Internal server error', error.message);
    }
  },

  create: async (req, res) => {
    try {
      const cachedResponse = getCachedResponse(req, table);
      if (cachedResponse !== null) {
        return jsonOk(res, cachedResponse, 'Created (idempotent)', 200);
      }

      const tenantId = resolveTenantIdFromRequest(req);
      if (table === 'app_users' && !tenantId) {
        throw new BadRequestError('tenantId tidak ditemukan pada token/login context');
      }

      const arrayPayload = parseBodyArray(req.body);
      const normalizedIncomingPayload = arrayPayload
        ? normalizePayloadByTable(table, arrayPayload, { tenantId })
        : normalizePayloadByTable(table, parseBodyObject(req.body), { tenantId });
      const payload = arrayPayload
        ? await Promise.all(
            normalizedIncomingPayload.map((item) => normalizeUserPassword(table, item, { isCreate: true })),
          )
        : await normalizeUserPassword(table, normalizedIncomingPayload, { isCreate: true });
      await ensureTenantScopedTable(req.tenantDb, table, tenantId);
      const columnDefinitions = await getTableColumnDefinitions(req.tenantDb, table);
      const sanitizedPayload = sanitizeClientGeneratedPrimaryKey(payload, columnDefinitions, {
        table,
        isCreate: true,
      });
      // Note: product creates must always carry a numeric id for bigint schemas; client UUIDs are preserved in reference_id
      const tenantScopedPayload = enforceTenantIdOnPayload(sanitizedPayload, tenantId, columnDefinitions);
      const filteredPayload = normalizePayloadByColumnDefinitions(tenantScopedPayload, columnDefinitions);
      const hasFields = Array.isArray(filteredPayload)
        ? filteredPayload.some((row) => row && typeof row === 'object' && Object.keys(row).length > 0)
        : !!filteredPayload && typeof filteredPayload === 'object' && Object.keys(filteredPayload).length > 0;
      if (!hasFields) {
        throw new BadRequestError(`Tidak ada kolom yang cocok untuk tabel ${table}`);
      }
      const { sql, values } = buildInsertQuery(table, filteredPayload);
      const result = await req.tenantDb.query(sql, values);
      for (const row of result.rows) {
        emitTableMutation(req, {
          table,
          action: 'INSERT',
          record: row,
        });
      }
      const responsePayload = arrayPayload ? result.rows : (result.rows[0] || null);
      storeResponse(req, table, responsePayload);
      return jsonOk(res, responsePayload, 'Created', 201);
    } catch (error) {
      if (error instanceof BadRequestError) {
        return jsonError(res, error.statusCode, error.message, error.message);
      }
      return jsonError(res, 500, error.message || 'Internal server error', error.message);
    }
  },

  updateById: async (req, res) => {
    try {
      const idField = req.query.idField || 'id';
      const tenantId = resolveTenantIdFromRequest(req);
      const normalizedIncomingPayload = normalizePayloadByTable(
        table,
        parseBodyObject(req.body),
        { tenantId },
      );
      const payload = await normalizeUserPassword(table, normalizedIncomingPayload);
      await ensureTenantScopedTable(req.tenantDb, table, tenantId);
      const columnDefinitions = await getTableColumnDefinitions(req.tenantDb, table);
      validateIdValueForTable(columnDefinitions, idField, req.params.id);
      const tenantScopedPayload = enforceTenantIdOnPayload(payload, tenantId, columnDefinitions);
      const filteredPayload = normalizePayloadByColumnDefinitions(tenantScopedPayload, columnDefinitions);
      const hasFields = !!filteredPayload && typeof filteredPayload === 'object' && Object.keys(filteredPayload).length > 0;
      if (!hasFields) {
        const existing = await runSelect(req.tenantDb, table, {
          [`eq__${idField}`]: req.params.id,
          maybeSingle: true,
        }, { tenantId });
        return jsonOk(res, existing || null, 'Updated');
      }
      const columnSet = new Set(columnDefinitions.keys());
      if (shouldAutoCancelOrdersBeforeClearingTable(table, filteredPayload)) {
        const { db, release, supportsTransactions } = await getQueryableClient(req.tenantDb);

        try {
          let cancelledOrders = [];
          if (supportsTransactions) {
            await db.query('BEGIN');
          }

          cancelledOrders = await cancelLinkedOrdersForClearedTable({
            db,
            tenantId,
            tableId: req.params.id,
          });

          const { sql, values } = buildUpdateQuery(
            table,
            filteredPayload,
            idField,
            req.params.id,
            { tenantId, hasTenantColumn: columnSet.has('tenant_id') },
          );
          const result = await db.query(sql, values);

          if (supportsTransactions) {
            await db.query('COMMIT');
          }

          for (const cancelledOrder of cancelledOrders) {
            emitTableMutation(req, {
              table: 'sales_records',
              action: 'UPDATE',
              record: cancelledOrder,
              extra: {
                mutationType: 'TRANSACTION_CANCELLED',
                reason: 'table_cleared',
              },
            });
          }

          emitTableMutation(req, {
            table,
            action: 'UPDATE',
            record: result.rows[0] || null,
            id: req.params.id,
          });
          return jsonOk(res, result.rows[0] || null, 'Updated');
        } catch (error) {
          if (supportsTransactions) {
            await db.query('ROLLBACK').catch(() => {});
          }
          throw error;
        } finally {
          release();
        }
      }

      const { sql, values } = buildUpdateQuery(
        table,
        filteredPayload,
        idField,
        req.params.id,
        { tenantId, hasTenantColumn: columnSet.has('tenant_id') },
      );
      const result = await req.tenantDb.query(sql, values);
      emitTableMutation(req, {
        table,
        action: 'UPDATE',
        record: result.rows[0] || null,
        id: req.params.id,
      });
      return jsonOk(res, result.rows[0] || null, 'Updated');
    } catch (error) {
      if (error instanceof BadRequestError) {
        return jsonError(res, error.statusCode, error.message, error.message);
      }
      return jsonError(res, 500, error.message || 'Internal server error', error.message);
    }
  },

  deleteById: async (req, res) => {
    try {
      const idField = req.query.idField || 'id';
      const tenantId = resolveTenantIdFromRequest(req);
      await ensureTenantScopedTable(req.tenantDb, table, tenantId);
      const columnDefinitions = await getTableColumnDefinitions(req.tenantDb, table);
      validateIdValueForTable(columnDefinitions, idField, req.params.id);
      const columnSet = new Set(columnDefinitions.keys());
      const { sql, values } = buildDeleteQuery(
        table,
        idField,
        req.params.id,
        { tenantId, hasTenantColumn: columnSet.has('tenant_id') },
      );
      const result = await req.tenantDb.query(sql, values);
      emitTableMutation(req, {
        table,
        action: 'DELETE',
        record: result.rows[0] || null,
        id: req.params.id,
      });
      return jsonOk(res, result.rows[0] || null, 'Deleted');
    } catch (error) {
      return jsonError(res, 500, error.message || 'Internal server error', error.message);
    }
  },
});

module.exports = {
  createCrudController,
};
