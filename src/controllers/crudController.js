const { jsonOk, jsonError } = require('../utils/http');
const bcrypt = require('bcryptjs');
const { emitTableMutation } = require('../services/realtimeEmitter');
const {
  parseBodyArray,
  parseBodyObject,
  buildInsertQuery,
  buildUpdateQuery,
  buildDeleteQuery,
  getTableColumnSet,
  filterPayloadByColumnSet,
  runSelect,
} = require('../utils/sqlHelpers');

const BCRYPT_REGEX = /^\$2[aby]\$\d{2}\$.{53}$/;

class BadRequestError extends Error {
  constructor(message) {
    super(message);
    this.name = 'BadRequestError';
    this.statusCode = 400;
  }
}

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
      const rows = await runSelect(req.tenantDb, table, req.query);
      return jsonOk(res, rows);
    } catch (error) {
      return jsonError(res, 500, error.message || 'Internal server error', error.message);
    }
  },

  create: async (req, res) => {
    try {
      const arrayPayload = parseBodyArray(req.body);
      const payload = arrayPayload
        ? await Promise.all(
            arrayPayload.map((item) => normalizeUserPassword(table, item, { isCreate: true })),
          )
        : await normalizeUserPassword(table, parseBodyObject(req.body), { isCreate: true });
      const columnSet = await getTableColumnSet(req.tenantDb, table);
      const filteredPayload = filterPayloadByColumnSet(payload, columnSet);
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
      return jsonOk(res, arrayPayload ? result.rows : (result.rows[0] || null), 'Created', 201);
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
      const payload = await normalizeUserPassword(table, parseBodyObject(req.body));
      const columnSet = await getTableColumnSet(req.tenantDb, table);
      const filteredPayload = filterPayloadByColumnSet(payload, columnSet);
      const hasFields = !!filteredPayload && typeof filteredPayload === 'object' && Object.keys(filteredPayload).length > 0;
      if (!hasFields) {
        const existing = await runSelect(req.tenantDb, table, {
          [`eq__${idField}`]: req.params.id,
          maybeSingle: true,
        });
        return jsonOk(res, existing || null, 'Updated');
      }
      const { sql, values } = buildUpdateQuery(table, filteredPayload, idField, req.params.id);
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
      const { sql, values } = buildDeleteQuery(table, idField, req.params.id);
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
