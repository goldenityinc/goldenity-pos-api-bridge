const { jsonOk, jsonError } = require('../utils/http');
const { runSelect } = require('../utils/sqlHelpers');
const { emitInventoryUpdated } = require('../services/realtimeEmitter');

const getProducts = async (req, res) => {
  try {
    const rows = await runSelect(req.tenantDb, 'products', req.query);
    return jsonOk(res, rows);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

const reduceStock = async (req, res) => {
  try {
    const productId = req.params.id;
    const qty = Number(req.body?.qty);
    const reason = (req.body?.reason ?? '').toString().trim();

    if (!Number.isFinite(qty) || qty <= 0 || !Number.isInteger(qty)) {
      return jsonError(res, 400, 'qty harus berupa angka bulat > 0');
    }

    if (!reason) {
      return jsonError(res, 400, 'reason wajib diisi');
    }

    const currentResult = await req.tenantDb.query(
      'SELECT id, stock FROM "products" WHERE id = $1 LIMIT 1',
      [productId],
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
      'UPDATE "products" SET stock = $1 WHERE id = $2 RETURNING *',
      [newStock, productId],
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
