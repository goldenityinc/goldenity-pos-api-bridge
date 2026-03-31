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

const sanitizeClientGeneratedPrimaryKey = (payload, columnDefinitions) => {
  if (!(columnDefinitions instanceof Map) || !isIntegerColumnDefinition(columnDefinitions.get('id'))) {
    return payload;
  }

  const sanitizeRow = (row) => {
    if (!row || typeof row !== 'object' || Array.isArray(row)) {
      return row;
    }

    if (!Object.prototype.hasOwnProperty.call(row, 'id')) {
      return row;
    }

    const rawId = row.id;
    if (rawId === undefined || rawId === null) {
      return row;
    }

    const idText = rawId.toString().trim();
    if (!idText || /^\d+$/.test(idText)) {
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
  const normalizedName = (next.name ?? next.nama_toko ?? '').toString().trim();
  const normalizedPhone = (next.phone ?? next.kontak ?? '').toString().trim();
  const normalizedAddress = (next.address ?? next.alamat ?? '').toString().trim();

  if (normalizedName) {
    next.name = normalizedName;
    next.nama_toko = normalizedName;
  }
  if (normalizedPhone || Object.prototype.hasOwnProperty.call(next, 'phone') || Object.prototype.hasOwnProperty.call(next, 'kontak')) {
    next.phone = normalizedPhone;
    next.kontak = normalizedPhone;
  }
  if (normalizedAddress || Object.prototype.hasOwnProperty.call(next, 'address') || Object.prototype.hasOwnProperty.call(next, 'alamat')) {
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

const normalizePayloadByTable = (table, payload) => {
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

    return row;
  };

  if (Array.isArray(payload)) {
    return payload.map(normalizeRow);
  }

  return normalizeRow(payload);
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
      const tenantId = normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);
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

      const arrayPayload = parseBodyArray(req.body);
      const normalizedIncomingPayload = arrayPayload
        ? normalizePayloadByTable(table, arrayPayload)
        : normalizePayloadByTable(table, parseBodyObject(req.body));
      const payload = arrayPayload
        ? await Promise.all(
            normalizedIncomingPayload.map((item) => normalizeUserPassword(table, item, { isCreate: true })),
          )
        : await normalizeUserPassword(table, normalizedIncomingPayload, { isCreate: true });
      const tenantId = normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);
      await ensureTenantScopedTable(req.tenantDb, table, tenantId);
      const columnDefinitions = await getTableColumnDefinitions(req.tenantDb, table);
      const sanitizedPayload = sanitizeClientGeneratedPrimaryKey(payload, columnDefinitions);
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
      const normalizedIncomingPayload = normalizePayloadByTable(
        table,
        parseBodyObject(req.body),
      );
      const payload = await normalizeUserPassword(table, normalizedIncomingPayload);
      const tenantId = normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);
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
      const tenantId = normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);
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
