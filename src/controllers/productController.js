const { jsonOk, jsonError } = require('../utils/http');
const { getTableColumnSet, normalizeTenantId } = require('../utils/sqlHelpers');
const { ensureTenantScopedTable } = require('../utils/tenantScope');
const { emitInventoryUpdated } = require('../services/realtimeEmitter');

const resolveTenantIdFromRequest = (req) => normalizeTenantId(
  req?.user?.tenantId ||
  req?.user?.tenant_id ||
  req?.tenant?.tenantId ||
  req?.auth?.tenantId ||
  req?.auth?.tenant_id,
);

const normalizePositiveInteger = (value, fallback, { min = 0, max = 1000 } = {}) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }

  const normalized = Math.floor(parsed);
  if (normalized < min) {
    return fallback;
  }

  return Math.min(normalized, max);
};

const normalizeLastSyncDate = (value) => {
  const raw = (value ?? '').toString().trim();
  if (!raw) return null;

  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }

  return parsed.toISOString();
};

const resolveProductsSyncExpressions = async (tenantDb) => {
  const columns = await getTableColumnSet(tenantDb, 'products');
  const hasUpdatedAt = columns.has('updated_at');
  const hasCreatedAt = columns.has('created_at');

  if (hasUpdatedAt && hasCreatedAt) {
    return {
      filterExpression: 'COALESCE("updated_at", "created_at")',
      orderExpression: 'COALESCE("updated_at", "created_at")',
      syncColumn: 'updated_at_or_created_at',
    };
  }

  if (hasUpdatedAt) {
    return {
      filterExpression: '"updated_at"',
      orderExpression: '"updated_at"',
      syncColumn: 'updated_at',
    };
  }

  if (hasCreatedAt) {
    return {
      filterExpression: '"created_at"',
      orderExpression: '"created_at"',
      syncColumn: 'created_at',
    };
  }

  return {
    filterExpression: null,
    orderExpression: '"id"',
    syncColumn: 'id',
  };
};

const getProducts = async (req, res) => {
  try {
    const tenantId = resolveTenantIdFromRequest(req);
    if (!tenantId) {
      return jsonError(res, 401, 'Tenant tidak valid');
    }
    await ensureTenantScopedTable(req.tenantDb, 'products', tenantId);
    const limit = normalizePositiveInteger(req.query?.limit, 500, {
      min: 1,
      max: 1000,
    });
    const offset = normalizePositiveInteger(req.query?.offset, 0, {
      min: 0,
      max: 1000000,
    });
    const lastSyncDate = normalizeLastSyncDate(req.query?.lastSyncDate);
    const syncExpressions = await resolveProductsSyncExpressions(req.tenantDb);

    if (req.query?.lastSyncDate && !lastSyncDate) {
      return jsonError(res, 400, 'Format lastSyncDate tidak valid');
    }

    const values = [];
    let whereClause = '';
    const productsColumns = await getTableColumnSet(req.tenantDb, 'products');
    const hasTenantColumn = productsColumns.has('tenant_id');
    if (hasTenantColumn) {
      values.push(tenantId);
      whereClause = ` WHERE tenant_id = $${values.length}`;
    }
    if (lastSyncDate && syncExpressions.filterExpression) {
      values.push(lastSyncDate);
      whereClause += hasTenantColumn ? ` AND ${syncExpressions.filterExpression} > $${values.length}` : ` WHERE ${syncExpressions.filterExpression} > $${values.length}`;
    }

    const countResult = await req.tenantDb.query(
      `SELECT COUNT(*)::int AS total FROM "products"${whereClause}`,
      values,
    );
    const total = Number(countResult.rows?.[0]?.total ?? 0);

    const dataValues = [...values, limit, offset];
    const limitParamIndex = values.length + 1;
    const offsetParamIndex = values.length + 2;
    const rowsResult = await req.tenantDb.query(
      `SELECT *
       FROM "products"
       ${whereClause}
       ORDER BY ${syncExpressions.orderExpression} ASC, "id" ASC
       LIMIT $${limitParamIndex}
       OFFSET $${offsetParamIndex}`,
      dataValues,
    );

    const rows = rowsResult.rows || [];
    return res.status(200).json({
      success: true,
      message: 'Success',
      data: rows,
      meta: {
        total,
        count: rows.length,
        limit,
        offset,
        hasMore: offset + rows.length < total,
        lastSyncDate,
        syncColumn: syncExpressions.syncColumn,
        serverNow: new Date().toISOString(),
      },
    });
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

const reduceStock = async (req, res) => {
  try {
    const tenantId = resolveTenantIdFromRequest(req);
    if (!tenantId) {
      return jsonError(res, 401, 'Tenant tidak valid');
    }
    await ensureTenantScopedTable(req.tenantDb, 'products', tenantId);
    const productId = req.params.id;
    const qty = Number(req.body?.qty);
    const reason = (req.body?.reason ?? '').toString().trim();

    if (!Number.isFinite(qty) || qty <= 0 || !Number.isInteger(qty)) {
      return jsonError(res, 400, 'qty harus berupa angka bulat > 0');
    }

    if (!reason) {
      return jsonError(res, 400, 'reason wajib diisi');
    }

    const productsColumns = await getTableColumnSet(req.tenantDb, 'products');
    const hasTenantColumn = productsColumns.has('tenant_id');
    const currentResult = await req.tenantDb.query(
      hasTenantColumn
        ? 'SELECT id, stock FROM "products" WHERE id = $1 AND tenant_id = $2 LIMIT 1'
        : 'SELECT id, stock FROM "products" WHERE id = $1 LIMIT 1',
      hasTenantColumn ? [productId, tenantId] : [productId],
    );

    if ((currentResult.rowCount || 0) === 0) {
      return jsonError(res, 404, 'Produk tidak ditemukan');
    }

    const currentStock = Number(currentResult.rows[0].stock ?? 0);
    if (!Number.isFinite(currentStock)) {
      return jsonError(res, 500, 'Nilai stok produk tidak valid');
    }

    if (qty > currentStock) {
      return jsonError(
        res,
        400,
        `Gagal: qty melebihi stok saat ini (${currentStock}).`,
      );
    }

    const newStock = currentStock - qty;
    const updateResult = await req.tenantDb.query(
      hasTenantColumn
        ? 'UPDATE "products" SET stock = $1 WHERE id = $2 AND tenant_id = $3 RETURNING *'
        : 'UPDATE "products" SET stock = $1 WHERE id = $2 RETURNING *',
      hasTenantColumn ? [newStock, productId, tenantId] : [newStock, productId],
    );

    const updatedProduct = updateResult.rows[0] || null;
    emitInventoryUpdated(req, updatedProduct, {
      reason,
      source: 'products_reduce_stock',
    });

    return jsonOk(res, {
      ...(updatedProduct || {}),
      reduced_qty: qty,
      reason,
    }, 'Stock reduced');
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

module.exports = {
  getProducts,
  reduceStock,
};
