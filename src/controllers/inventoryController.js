const { parse } = require('csv-parse/sync');
const { jsonError, jsonOk } = require('../utils/http');
const { emitTableMutation } = require('../services/realtimeEmitter');
const { getTableColumnSet, normalizeTenantId } = require('../utils/sqlHelpers');
const { ensureTenantScopedTable } = require('../utils/tenantScope');

const resolveTenantIdFromRequest = (req) => normalizeTenantId(
  req?.user?.tenantId ||
  req?.user?.tenant_id ||
  req?.tenant?.tenantId ||
  req?.auth?.tenantId ||
  req?.auth?.tenant_id,
);

const INVENTORY_CSV_HEADERS = [
  'Nama',
  'Barcode',
  'Kategori',
  'Harga Modal',
  'Harga Jual',
  'Stok',
];

const CSV_MAX_ROWS = 5000;

const toCsvSafeValue = (value) => {
  const text = (value ?? '').toString();
  if (/[",\r\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
};

const buildCsvContent = (rows) => {
  const lines = [INVENTORY_CSV_HEADERS.map(toCsvSafeValue).join(',')];

  for (const row of rows) {
    lines.push([
      row.name,
      row.barcode,
      row.category,
      row.purchase_price,
      row.price,
      row.stock,
    ].map(toCsvSafeValue).join(','));
  }

  return `\uFEFF${lines.join('\r\n')}\r\n`;
};

const normalizeText = (value) => (value ?? '').toString().trim();

const normalizeInteger = (value, { fallback = 0 } = {}) => {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return Math.round(value);
  }

  const normalized = normalizeText(value)
    .replaceAll('.', '')
    .replaceAll(',', '')
    .replaceAll('Rp', '')
    .trim();

  if (!normalized) {
    return fallback;
  }

  const parsed = Number.parseInt(normalized, 10);
  return Number.isNaN(parsed) ? fallback : parsed;
};

const getRowValue = (record, candidates) => {
  for (const candidate of candidates) {
    if (!Object.prototype.hasOwnProperty.call(record, candidate)) {
      continue;
    }

    const value = normalizeText(record[candidate]);
    if (value) {
      return value;
    }
  }

  return '';
};

const parseInventoryCsvRows = (fileBuffer) => {
  const rawText = fileBuffer.toString('utf8').replace(/^\uFEFF/, '');
  const records = parse(rawText, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    bom: true,
  });

  if (!Array.isArray(records)) {
    return [];
  }

  return records.slice(0, CSV_MAX_ROWS);
};

const sendCsvResponse = (res, fileName, rows) => {
  const csvContent = buildCsvContent(rows);
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
  return res.status(200).send(csvContent);
};

const downloadInventoryTemplate = async (_req, res) => {
  return sendCsvResponse(res, 'inventory_template.csv', []);
};

const exportInventoryCsv = async (req, res) => {
  try {
    const tenantId = resolveTenantIdFromRequest(req);
    if (!tenantId) {
      return jsonError(res, 401, 'Tenant tidak valid');
    }
    await ensureTenantScopedTable(req.tenantDb, 'products', tenantId);
    const columnSet = await getTableColumnSet(req.tenantDb, 'products');
    const hasTenantColumn = columnSet.has('tenant_id');
    const result = await req.tenantDb.query(
      `SELECT
          name,
          barcode,
          category,
          purchase_price,
          price,
          stock
       FROM "products"
       ${hasTenantColumn ? 'WHERE tenant_id = $1' : ''}
       ORDER BY LOWER(COALESCE(name, '')) ASC, id ASC`,
      hasTenantColumn ? [tenantId] : [],
    );

    const rows = (result.rows || []).map((row) => ({
      name: normalizeText(row.name),
      barcode: normalizeText(row.barcode),
      category: normalizeText(row.category),
      purchase_price: normalizeInteger(row.purchase_price),
      price: normalizeInteger(row.price),
      stock: normalizeInteger(row.stock),
    }));

    const today = new Date().toISOString().slice(0, 10);
    return sendCsvResponse(res, `inventory_export_${today}.csv`, rows);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Gagal export inventaris', error.message);
  }
};

const importInventoryCsv = async (req, res) => {
  if (!req.file || !req.file.buffer) {
    return jsonError(res, 400, 'File CSV wajib diunggah pada field file');
  }

  try {
    const tenantId = resolveTenantIdFromRequest(req);
    if (!tenantId) {
      return jsonError(res, 401, 'Tenant tidak valid');
    }
    await ensureTenantScopedTable(req.tenantDb, 'products', tenantId);
    const columnSet = await getTableColumnSet(req.tenantDb, 'products');
    const hasTenantColumn = columnSet.has('tenant_id');
    const parsedRows = parseInventoryCsvRows(req.file.buffer);
    if (parsedRows.length === 0) {
      return jsonError(res, 400, 'File CSV kosong atau tidak memiliki baris data');
    }

    const summary = {
      totalRows: parsedRows.length,
      inserted: 0,
      updated: 0,
      skipped: 0,
      errors: [],
    };

    await req.tenantDb.query('BEGIN');

    for (let index = 0; index < parsedRows.length; index += 1) {
      const record = parsedRows[index] || {};
      const rowNumber = index + 2;
      const name = getRowValue(record, ['Nama', 'nama', 'Name', 'name']);
      const barcode = getRowValue(record, ['Barcode', 'barcode', 'SKU', 'sku']);
      const category = getRowValue(record, ['Kategori', 'kategori', 'Category', 'category']);
      const purchasePrice = normalizeInteger(
        getRowValue(record, ['Harga Modal', 'harga modal', 'Purchase Price', 'purchase_price']),
      );
      const sellingPrice = normalizeInteger(
        getRowValue(record, ['Harga Jual', 'harga jual', 'Selling Price', 'price']),
      );
      const stock = normalizeInteger(getRowValue(record, ['Stok', 'stok', 'Stock', 'stock']));

      const isEmptyRow = !name && !barcode && !category && purchasePrice === 0 && sellingPrice === 0 && stock === 0;
      if (isEmptyRow) {
        summary.skipped += 1;
        continue;
      }

      if (!name) {
        summary.skipped += 1;
        summary.errors.push(`Baris ${rowNumber}: Nama wajib diisi.`);
        continue;
      }

      if (!barcode) {
        summary.skipped += 1;
        summary.errors.push(`Baris ${rowNumber}: Barcode wajib diisi untuk upsert.`);
        continue;
      }

      const existingResult = await req.tenantDb.query(
        `SELECT id
         FROM "products"
         WHERE barcode = $1
           ${hasTenantColumn ? 'AND tenant_id = $2' : ''}
         ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST, id DESC
         LIMIT 1`,
        hasTenantColumn ? [barcode, tenantId] : [barcode],
      );

      if ((existingResult.rowCount || 0) > 0) {
        const existingId = existingResult.rows[0].id;
        const updateResult = await req.tenantDb.query(
          `UPDATE "products"
           SET name = $1,
               barcode = $2,
               category = $3,
               purchase_price = $4::numeric,
               price = $5::numeric,
               stock = $6::integer,
               updated_at = NOW()
           WHERE id = $7::bigint
             ${hasTenantColumn ? 'AND tenant_id = $8::text' : ''}
           RETURNING *`,
          hasTenantColumn
            ? [name, barcode, category || null, Number.isFinite(Number(purchasePrice)) ? Number(purchasePrice) : 0, Number.isFinite(Number(sellingPrice)) ? Number(sellingPrice) : 0, Number.isInteger(Number(stock)) ? Number(stock) : 0, existingId, tenantId]
            : [name, barcode, category || null, Number.isFinite(Number(purchasePrice)) ? Number(purchasePrice) : 0, Number.isFinite(Number(sellingPrice)) ? Number(sellingPrice) : 0, Number.isInteger(Number(stock)) ? Number(stock) : 0, existingId],
        );
        emitTableMutation(req, {
          table: 'products',
          action: 'UPDATE',
          record: updateResult.rows[0] || null,
          id: existingId,
        });
        summary.updated += 1;
        continue;
      }

      const insertResult = await req.tenantDb.query(
        `INSERT INTO "products" (
          name,
          barcode,
          category,
          purchase_price,
          price,
          stock
          ${hasTenantColumn ? ', tenant_id' : ''}
        ) VALUES ($1::text, $2::text, $3::text, $4::numeric, $5::numeric, $6::integer${hasTenantColumn ? ', $7::text' : ''}
        RETURNING *`,
        hasTenantColumn
          ? [name, barcode, category || null, Number.isFinite(Number(purchasePrice)) ? Number(purchasePrice) : 0, Number.isFinite(Number(sellingPrice)) ? Number(sellingPrice) : 0, Number.isInteger(Number(stock)) ? Number(stock) : 0, tenantId]
          : [name, barcode, category || null, Number.isFinite(Number(purchasePrice)) ? Number(purchasePrice) : 0, Number.isFinite(Number(sellingPrice)) ? Number(sellingPrice) : 0, Number.isInteger(Number(stock)) ? Number(stock) : 0],
      );

      emitTableMutation(req, {
        table: 'products',
        action: 'INSERT',
        record: insertResult.rows[0] || null,
      });
      summary.inserted += 1;
    }

    await req.tenantDb.query('COMMIT');

    if (summary.inserted == 0 && summary.updated == 0 && summary.errors.length > 0) {
      return jsonError(
        res,
        400,
        'Tidak ada baris yang berhasil diproses.',
        summary.errors.join(' | '),
      );
    }

    return jsonOk(res, summary, 'Import inventaris selesai');
  } catch (error) {
    try {
      await req.tenantDb.query('ROLLBACK');
    } catch (_) {}
    return jsonError(res, 500, error.message || 'Gagal import inventaris', error.message);
  }
};

module.exports = {
  downloadInventoryTemplate,
  exportInventoryCsv,
  importInventoryCsv,
};