const { jsonOk, jsonError } = require('../utils/http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { getSharedPool } = require('../middlewares/tenantResolver');
const { normalizeTenantId, getTableColumnSet } = require('../utils/sqlHelpers');
const { emitToTenant } = require('../services/socketServer');
const { uploadBase64Object } = require('../services/objectStorageService');
const { validatePaymentProof } = require('../utils/receiptScanner');

const FNB_PRODUCT_TYPES = new Set(['FOOD', 'BEVERAGE', 'FNB', 'F&B', 'MENU']);
const PAYMENT_METHOD_CASHIER = 'CASHIER';
const PAYMENT_METHOD_DIGITAL = 'DIGITAL_PAYMENT';
const PAYMENT_METHOD_QRIS = 'QRIS';
const LOCAL_PAYMENT_PROOF_DIR = path.join(process.cwd(), 'public', 'uploads', 'proofs');
const PAYMENT_PROOF_BUCKET =
  (process.env.PAYMENT_PROOF_BUCKET || 'payment-proofs').toString().trim() || 'payment-proofs';
const IMAGE_MIME_TO_EXTENSION = {
  'image/jpeg': 'jpg',
  'image/jpg': 'jpg',
  'image/png': 'png',
  'image/webp': 'webp',
};

const normalizePaymentMethod = (value) => {
  const normalized = (value || '').toString().trim().toUpperCase();
  if (normalized === 'QRIS') {
    return PAYMENT_METHOD_QRIS;
  }
  if (
    normalized === 'DIGITAL' ||
    normalized === 'DIGITAL_PAYMENT' ||
    normalized === 'E_WALLET' ||
    normalized === 'EWALLET'
  ) {
    return PAYMENT_METHOD_DIGITAL;
  }
  return PAYMENT_METHOD_CASHIER;
};

const resolvePaymentState = (paymentMethod) => {
  if (paymentMethod === PAYMENT_METHOD_QRIS) {
    return {
      paymentMethodLabel: 'QRIS',
      paymentStatus: 'PENDING_PAYMENT',
      orderStatus: 'PENDING',
    };
  }

  if (paymentMethod === PAYMENT_METHOD_DIGITAL) {
    return {
      paymentMethodLabel: 'Digital Payment',
      paymentStatus: 'PENDING_PAYMENT',
      orderStatus: 'PENDING',
    };
  }

  return {
    paymentMethodLabel: 'Bayar di Kasir',
    paymentStatus: 'UNPAID',
    orderStatus: 'PENDING',
  };
};

const resolveWebhookPaymentState = (value) => {
  const normalized = (value || '').toString().trim().toUpperCase();
  if (['PAID', 'SETTLED', 'SETTLEMENT', 'SUCCESS'].includes(normalized)) {
    return { paymentStatus: 'PAID', orderStatus: 'PREPARING' };
  }
  if (['FAILED', 'EXPIRED', 'CANCELLED', 'CANCELED', 'DENIED'].includes(normalized)) {
    return { paymentStatus: 'FAILED', orderStatus: 'CANCELLED' };
  }
  return { paymentStatus: 'PENDING_PAYMENT', orderStatus: 'PENDING' };
};

const parseTenantId = (value) => {
  const tenantId = normalizeTenantId(value);
  if (!tenantId) {
    const error = new Error('tenantId wajib diisi');
    error.statusCode = 400;
    throw error;
  }
  return tenantId;
};

const parseTableId = (value) => {
  const text = (value || '').toString().trim();
  if (!/^\d+$/.test(text)) {
    const error = new Error('table_id tidak valid');
    error.statusCode = 400;
    throw error;
  }
  return Number(text);
};

const parseOptionalBranchId = (value) => {
  const text = (value || '').toString().trim();
  if (!text) {
    return null;
  }
  if (!/^\d+$/.test(text)) {
    const error = new Error('branch_id tidak valid');
    error.statusCode = 400;
    throw error;
  }
  return Number.parseInt(text, 10);
};

const parseOrderItems = (items) => {
  if (!Array.isArray(items) || items.length === 0) {
    const error = new Error('items wajib diisi minimal 1 item');
    error.statusCode = 400;
    throw error;
  }

  return items.map((raw, index) => {
    if (!raw || typeof raw !== 'object') {
      const error = new Error(`items[${index}] tidak valid`);
      error.statusCode = 400;
      throw error;
    }

    const productId = (raw.productId || raw.product_id || '').toString().trim();
    if (!productId) {
      const error = new Error(`items[${index}].productId wajib diisi`);
      error.statusCode = 400;
      throw error;
    }

    const qty = Number(raw.qty || raw.quantity);
    if (!Number.isInteger(qty) || qty <= 0) {
      const error = new Error(`items[${index}].qty harus angka bulat > 0`);
      error.statusCode = 400;
      throw error;
    }

    const customPriceRaw = raw.customPrice ?? raw.custom_price;
    const customPrice =
      customPriceRaw === undefined || customPriceRaw === null || customPriceRaw === ''
        ? undefined
        : Number(customPriceRaw);

    if (customPrice !== undefined && (!Number.isFinite(customPrice) || customPrice < 0)) {
      const error = new Error(`items[${index}].customPrice tidak valid`);
      error.statusCode = 400;
      throw error;
    }

    return {
      productId,
      qty,
      note: (
        raw.note ??
        raw.item_note ??
        raw.notes ??
        raw.remark ??
        raw.remarks ??
        ''
      ).toString().trim() || null,
      customPrice,
    };
  });
};

const parseRawItems = (rawItems) => {
  if (Array.isArray(rawItems)) {
    return rawItems;
  }

  if (typeof rawItems === 'string') {
    const trimmed = rawItems.trim();
    if (!trimmed) {
      const error = new Error('items wajib diisi minimal 1 item');
      error.statusCode = 400;
      throw error;
    }

    try {
      const parsed = JSON.parse(trimmed);
      if (!Array.isArray(parsed)) {
        const error = new Error('items harus berupa array');
        error.statusCode = 400;
        throw error;
      }
      return parsed;
    } catch (_) {
      const error = new Error('items multipart harus berupa JSON array yang valid');
      error.statusCode = 400;
      throw error;
    }
  }

  return rawItems;
};

const sanitizePaymentProofMimeType = (mimeType) => {
  const normalized = (mimeType || '').toString().trim().toLowerCase();
  if (!IMAGE_MIME_TO_EXTENSION[normalized]) {
    const error = new Error('Bukti pembayaran wajib berupa gambar (jpg, png, webp)');
    error.statusCode = 400;
    throw error;
  }
  return normalized;
};

const parsePaymentProofBase64 = (rawValue) => {
  const value = (rawValue || '').toString().trim();
  if (!value) {
    return null;
  }

  let mimeType = 'image/jpeg';
  let payload = value;
  const dataUriMatch = value.match(/^data:([^;]+);base64,(.+)$/i);
  if (dataUriMatch) {
    mimeType = dataUriMatch[1].trim().toLowerCase();
    payload = dataUriMatch[2].trim();
  }

  const normalizedMimeType = sanitizePaymentProofMimeType(mimeType);
  const buffer = Buffer.from(payload, 'base64');
  if (!buffer.length) {
    const error = new Error('Konten bukti pembayaran kosong');
    error.statusCode = 400;
    throw error;
  }

  return {
    buffer,
    mimeType: normalizedMimeType,
    base64Data: payload,
  };
};

const buildPaymentProofFileName = ({ tenantId, extension }) => {
  const safeTenant = (tenantId || 'tenant').toString().replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 32) || 'tenant';
  const stamp = Date.now();
  const random = crypto.randomBytes(6).toString('hex');
  return `proof-${safeTenant}-${stamp}-${random}.${extension}`;
};

const savePaymentProofLocally = async ({ tenantId, buffer, mimeType, req }) => {
  const extension = IMAGE_MIME_TO_EXTENSION[mimeType];
  const fileName = buildPaymentProofFileName({ tenantId, extension });
  await fs.promises.mkdir(LOCAL_PAYMENT_PROOF_DIR, { recursive: true });
  const targetPath = path.join(LOCAL_PAYMENT_PROOF_DIR, fileName);
  await fs.promises.writeFile(targetPath, buffer);
  const relativePath = `/uploads/proofs/${fileName}`;
  const host = (req?.get && req.get('host')) ? req.get('host') : '';
  if (!host) {
    return relativePath;
  }
  const protocol = (req.protocol || 'https').toString();
  return `${protocol}://${host}${relativePath}`;
};

const uploadPaymentProof = async ({ tenantId, buffer, mimeType, req }) => {
  const extension = IMAGE_MIME_TO_EXTENSION[mimeType];
  const fileName = buildPaymentProofFileName({ tenantId, extension });
  const base64 = buffer.toString('base64');

  try {
    const uploaded = await uploadBase64Object({
      bucket: PAYMENT_PROOF_BUCKET,
      fileName,
      base64,
      contentType: mimeType,
    });
    const remoteUrl = (uploaded?.url || '').toString().trim();
    if (remoteUrl) {
      return remoteUrl;
    }
  } catch (_) {
    // Fallback to local file storage when object storage is not configured.
  }

  return savePaymentProofLocally({ tenantId, buffer, mimeType, req });
};

const resolvePaymentProofPayload = ({ req }) => {
  const directUrlRaw =
    req.body.paymentProofUrl ??
    req.body.payment_proof_url ??
    req.body.proof_url ??
    req.body.proofUrl ??
    null;
  const directUrl = (directUrlRaw || '').toString().trim();
  if (directUrl) {
    return {
      paymentProofUrl: directUrl,
      uploadBuffer: null,
      uploadMimeType: null,
      base64DataForValidation: null,
    };
  }

  if (req.file?.buffer?.length) {
    const mimeType = sanitizePaymentProofMimeType(req.file.mimetype);
    return {
      paymentProofUrl: null,
      uploadBuffer: req.file.buffer,
      uploadMimeType: mimeType,
      base64DataForValidation: null,
    };
  }

  const encodedProofRaw = req.body.payment_proof ?? req.body.paymentProof ?? null;
  if (!encodedProofRaw) {
    return {
      paymentProofUrl: null,
      uploadBuffer: null,
      uploadMimeType: null,
      base64DataForValidation: null,
    };
  }

  const parsed = parsePaymentProofBase64(encodedProofRaw);
  if (!parsed) {
    return {
      paymentProofUrl: null,
      uploadBuffer: null,
      uploadMimeType: null,
      base64DataForValidation: null,
    };
  }

  return {
    paymentProofUrl: null,
    uploadBuffer: parsed.buffer,
    uploadMimeType: parsed.mimeType,
    base64DataForValidation: parsed.base64Data,
  };
};

const parseBooleanConfig = (value, fallback = true) => {
  if (typeof value === 'boolean') {
    return value;
  }

  const normalized = (value ?? '').toString().trim().toLowerCase();
  if (!normalized) {
    return fallback;
  }
  if (['true', '1', 'yes', 'y', 'on'].includes(normalized)) {
    return true;
  }
  if (['false', '0', 'no', 'n', 'off'].includes(normalized)) {
    return false;
  }
  return fallback;
};

const resolveValidationBase64Data = (paymentProofPayload) => {
  if (!paymentProofPayload || typeof paymentProofPayload !== 'object') {
    return null;
  }

  const direct = (paymentProofPayload.base64DataForValidation || '').toString().trim();
  if (direct) {
    return direct;
  }

  if (paymentProofPayload.uploadBuffer?.length) {
    return paymentProofPayload.uploadBuffer.toString('base64');
  }

  return null;
};

const resolveQrOrderSettings = async ({ client, tenantId, branchId }) => {
  const defaults = {
    allowPayAtCashier: true,
    isPaymentProofMandatory: true,
  };

  try {
    const columnSet = await getTableColumnSet(client, 'store_settings');
    if (!(columnSet instanceof Set) || columnSet.size === 0) {
      return defaults;
    }

    const supportsBranch = columnSet.has('branch_id');
    if (
      columnSet.has('allow_pay_at_cashier') ||
      columnSet.has('is_payment_proof_mandatory') ||
      columnSet.has('enable_qris_ocr')
    ) {
      const whereParts = ['tenant_id = $1'];
      const params = [tenantId];
      let branchParamIndex = 0;

      if (supportsBranch && branchId !== null) {
        branchParamIndex = params.length + 1;
        whereParts.push(`(branch_id = $${branchParamIndex} OR branch_id IS NULL)`);
        params.push(branchId);
      }

      const orderParts = [];
      if (supportsBranch && branchId !== null && branchParamIndex > 0) {
        orderParts.push(`CASE WHEN branch_id = $${branchParamIndex} THEN 0 WHEN branch_id IS NULL THEN 1 ELSE 2 END`);
      }
      if (columnSet.has('updated_at')) {
        orderParts.push('updated_at DESC NULLS LAST');
      }
      if (columnSet.has('created_at')) {
        orderParts.push('created_at DESC NULLS LAST');
      }

      const wideResult = await client.query(
        `SELECT
           ${columnSet.has('allow_pay_at_cashier') ? 'allow_pay_at_cashier' : 'NULL::boolean'} AS allow_pay_at_cashier,
            ${columnSet.has('is_payment_proof_mandatory') ? 'is_payment_proof_mandatory' : 'NULL::boolean'} AS is_payment_proof_mandatory,
            ${columnSet.has('enable_qris_ocr') ? 'enable_qris_ocr' : 'NULL::boolean'} AS enable_qris_ocr
         FROM store_settings
         WHERE ${whereParts.join(' AND ')}
         ORDER BY ${orderParts.length > 0 ? orderParts.join(', ') : 'id DESC'}
         LIMIT 1`,
        params,
      );

      const row = wideResult.rows?.[0];
      if (row) {
        return {
          allowPayAtCashier: parseBooleanConfig(row.allow_pay_at_cashier, true),
          isPaymentProofMandatory: parseBooleanConfig(
            row.is_payment_proof_mandatory,
            parseBooleanConfig(row.enable_qris_ocr, true),
          ),
        };
      }
    }

    if (columnSet.has('key') && columnSet.has('value')) {
      const whereParts = ['tenant_id = $1'];
      const params = [tenantId];
      if (supportsBranch && branchId !== null) {
        whereParts.push('(branch_id = $2 OR branch_id IS NULL)');
        params.push(branchId);
      }

      const keyResult = await client.query(
        `SELECT key, COALESCE(value, '') AS value
         FROM store_settings
         WHERE ${whereParts.join(' AND ')}
           AND key = ANY($${params.length + 1}::text[])
         ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST`,
        [...params, ['allow_pay_at_cashier', 'is_payment_proof_mandatory', 'enable_qris_ocr']],
      );

      const resolved = { ...defaults };
      for (const row of keyResult.rows || []) {
        const key = (row.key || '').toString().trim();
        if (key === 'allow_pay_at_cashier') {
          resolved.allowPayAtCashier = parseBooleanConfig(row.value, resolved.allowPayAtCashier);
        } else if (key === 'is_payment_proof_mandatory' || key === 'enable_qris_ocr') {
          resolved.isPaymentProofMandatory = parseBooleanConfig(
            row.value,
            resolved.isPaymentProofMandatory,
          );
        }
      }
      return resolved;
    }
  } catch (error) {
    console.warn('[publicQrController] Failed resolving QR order settings:', error.message);
  }

  return defaults;
};

const generateReceiptNumber = () => {
  const now = new Date();
  const yyyymmdd = `${now.getFullYear()}${`${now.getMonth() + 1}`.padStart(2, '0')}${`${now.getDate()}`.padStart(2, '0')}`;
  const serial = `${now.getTime() % 10000}`.padStart(4, '0');
  return `INV-${yyyymmdd}-${serial}`;
};

const resolveSoftDeletePredicate = (columnSet) => {
  if (!(columnSet instanceof Set)) {
    return '1=1';
  }

  if (columnSet.has('deleted_at')) {
    return 'deleted_at IS NULL';
  }

  if (columnSet.has('is_deleted')) {
    return 'COALESCE(is_deleted, false) = false';
  }

  return '1=1';
};

const parseJsonArraySafe = (raw) => {
  if (Array.isArray(raw)) {
    return raw;
  }

  if (typeof raw !== 'string') {
    return [];
  }

  const trimmed = raw.trim();
  if (!trimmed) {
    return [];
  }

  try {
    const parsed = JSON.parse(trimmed);
    return Array.isArray(parsed) ? parsed : [];
  } catch (_) {
    return [];
  }
};

const parseBatchSequence = (value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 0;
  }
  return Math.floor(parsed);
};

const getItemBatchSequence = (item) => {
  if (!item || typeof item !== 'object') {
    return 0;
  }

  return parseBatchSequence(
    item.batch_sequence ??
    item.batchSequence ??
    item.batch ??
    item.sequence,
  );
};

const stampItemsWithBatchSequence = (items, batchSequence) => {
  const sequence = parseBatchSequence(batchSequence) || 1;
  return items.map((item) => ({
    ...item,
    batch_sequence: sequence,
    batchSequence: sequence,
  }));
};

const getQrMenu = async (req, res) => {
  try {
    const tenantId = parseTenantId(req.params.tenantId);
    const branchId = parseOptionalBranchId(req.query.branchId || req.query.branch_id);
    const branchNameFromQuery = (req.query.branchName || req.query.branch_name || '')
      .toString()
      .trim();
    const pool = getSharedPool();
    const productColumns = await getTableColumnSet(pool, 'products');
    const softDeletePredicate = resolveSoftDeletePredicate(productColumns);
    const categoryColumns = await getTableColumnSet(pool, 'categories');
    const categorySoftDeletePredicate = resolveSoftDeletePredicate(categoryColumns);
    const supportsPrintDestination = productColumns.has('print_destination');
    const stockTrackedExpression = productColumns.has('is_stock_tracked')
      ? 'COALESCE(is_stock_tracked, true)'
      : productColumns.has('stock_tracked')
        ? 'COALESCE(stock_tracked, true)'
        : 'true';
    const stockVisibilityPredicate = `(
      COALESCE(is_service, false) = true
      OR COALESCE(stock, 0) > 0
      OR ${stockTrackedExpression} = false
    )`;

    const result = await pool.query(
            `SELECT id, name, category, product_type, price, stock, image_url, unit,
              ${supportsPrintDestination ? 'print_destination,' : 'NULL::text AS print_destination,'}
              ${stockTrackedExpression} AS is_stock_tracked,
              COALESCE(is_service, false) AS is_service,
              COALESCE(is_available, true) AS is_available
       FROM products
       WHERE tenant_id = $1
         AND ${softDeletePredicate}
         AND ($3::bigint IS NULL OR branch_id = $3)
         AND COALESCE(is_active, true) = true
         AND (
           UPPER(COALESCE(product_type, '')) = ANY($2::text[])
         )
         AND ${stockVisibilityPredicate}
       ORDER BY name ASC`,
      [tenantId, Array.from(FNB_PRODUCT_TYPES), branchId],
    );

    let rows = result.rows || [];
    if (rows.length === 0) {
      const fallbackResult = await pool.query(
        `SELECT id, name, category, product_type, price, stock, image_url, unit,
          ${stockTrackedExpression} AS is_stock_tracked,
          COALESCE(is_service, false) AS is_service,
          COALESCE(is_available, true) AS is_available
         FROM products
         WHERE tenant_id = $1
           AND ${softDeletePredicate}
           AND ($2::bigint IS NULL OR branch_id = $2)
           AND COALESCE(is_active, true) = true
           AND ${stockVisibilityPredicate}
         ORDER BY name ASC`,
        [tenantId, branchId],
      );
      rows = fallbackResult.rows || [];
    }

    let categoryRows = [];
    try {
      const categoriesResult = await pool.query(
        `SELECT id, name
         FROM categories
         WHERE tenant_id = $1
           AND ${categorySoftDeletePredicate}
           AND ($2::bigint IS NULL OR branch_id = $2)
         ORDER BY name ASC`,
        [tenantId, branchId],
      );
      categoryRows = categoriesResult.rows || [];
    } catch (categoriesError) {
      // Categories table can vary by deployment schema; fallback to deriving categories from products.
      console.warn('[publicQrController] Categories lookup skipped:', categoriesError.message);
    }

    let tenantMeta = null;
    try {
      const tenantMetaResult = await pool.query(
        `SELECT id, name, slug
         FROM tenants
         WHERE id = $1
         LIMIT 1`,
        [tenantId],
      );
      tenantMeta = tenantMetaResult.rows?.[0] || null;
    } catch (tenantMetaError) {
      // Tenant metadata is optional for menu rendering.
      console.warn('[publicQrController] Tenant metadata lookup skipped:', tenantMetaError.message);
    }

    let storeName = '';
    try {
      const storeSettingsResult = await pool.query(
        `SELECT COALESCE(value, '') AS store_name
         FROM store_settings
         WHERE tenant_id = $1
           AND key = ANY($2::text[])
         ORDER BY CASE
           WHEN key = 'store_name' THEN 0
           WHEN key = 'nama_toko' THEN 1
           WHEN key = 'name' THEN 2
           ELSE 3
         END,
         updated_at DESC NULLS LAST,
         created_at DESC NULLS LAST
         LIMIT 1`,
        [tenantId, ['store_name', 'nama_toko', 'name']],
      );
      storeName = (storeSettingsResult.rows?.[0]?.store_name || '').toString().trim();
    } catch (storeSettingsError) {
      // Keep endpoint resilient when store_settings schema differs per environment.
      console.warn('[publicQrController] Store settings lookup skipped:', storeSettingsError.message);
    }

    let branchMeta = null;
    if (branchId !== null) {
      try {
        const branchResult = await pool.query(
          `SELECT id, name, branch_code
           FROM branches
           WHERE tenant_id = $1 AND id = $2
           LIMIT 1`,
          [tenantId, branchId],
        );
        branchMeta = branchResult.rows?.[0] || null;
      } catch (branchError) {
        console.warn('[publicQrController] Branch lookup skipped:', branchError.message);
      }
    }

    const categoriesMap = new Map();
    for (const categoryRow of categoryRows) {
      const categoryIdRaw = (categoryRow.id ?? '').toString().trim();
      const categoryName = (categoryRow.name || 'Menu').toString().trim() || 'Menu';
      if (!categoryName) {
        continue;
      }

      const categoryId =
        categoryIdRaw || categoryName.toLowerCase().replace(/[^a-z0-9]+/g, '-');
      if (!categoriesMap.has(categoryId)) {
        categoriesMap.set(categoryId, {
          id: categoryId,
          name: categoryName,
          sortOrder: categoriesMap.size,
        });
      }
    }

    const products = rows.map((row) => {
      const categoryName = (row.category || 'Menu').toString().trim() || 'Menu';
      const categoryId = categoryName.toLowerCase().replace(/[^a-z0-9]+/g, '-');
      if (!categoriesMap.has(categoryId)) {
        categoriesMap.set(categoryId, {
          id: categoryId,
          name: categoryName,
          sortOrder: categoriesMap.size,
        });
      }

      return {
        id: row.id,
        name: row.name,
        categoryId,
        categoryName,
        unit: (row.unit || 'pcs').toString().trim() || 'pcs',
        unit_name: (row.unit || 'pcs').toString().trim() || 'pcs',
        price: Number(row.price || 0),
        print_destination: (row.print_destination || 'CASHIER').toString().trim().toUpperCase() || 'CASHIER',
        printDestination: (row.print_destination || 'CASHIER').toString().trim().toUpperCase() || 'CASHIER',
        is_available: row.is_available !== false,
        isAvailable:
          row.is_available !== false && (
            Number(row.stock || 0) > 0 ||
            row.is_service === true ||
            row.is_stock_tracked === false
          ),
        is_stock_tracked: row.is_stock_tracked !== false,
        stock: Number(row.stock || 0),
        imageUrl: row.image_url || null,
        sortOrder: 0,
      };
    });

    const payload = {
      tenant: {
        id: tenantId,
        name: storeName || tenantMeta?.name || null,
        slug: tenantMeta?.slug || null,
      },
      branch: {
        id: branchId,
        name: (branchMeta?.name || branchNameFromQuery || '').toString().trim() || null,
        code: (branchMeta?.branch_code || '').toString().trim() || null,
      },
      categories: Array.from(categoriesMap.values()),
      products,
      items: products,
      config: {
        web_order_url: (process.env.WEB_ORDER_URL || 'https://pos-web-ordering-production.up.railway.app').toString().trim(),
      },
    };

    return jsonOk(res, payload, 'QR menu berhasil dimuat');
  } catch (error) {
    return jsonError(res, error.statusCode || 500, error.message || 'Internal server error', error.message);
  }
};

const createQrOrder = async (req, res) => {
  const pool = getSharedPool();
  const client = await pool.connect();

  try {
    const tenantId = parseTenantId(req.body.tenantId || req.body.tenant_id);
    const tableId = parseTableId(req.body.tableId || req.body.table_id);
    const branchId = parseOptionalBranchId(req.body.branchId || req.body.branch_id);
    const rawItems = parseRawItems(req.body.items ?? req.body.orderItems);
    const items = parseOrderItems(rawItems);
    const customerName = (req.body.customerName || req.body.customer_name || 'Guest').toString().trim() || 'Guest';
    const orderNote = (
      req.body.orderNote ??
      req.body.order_note ??
      req.body.special_note ??
      req.body.specialNote ??
      req.body.customerNote ??
      req.body.customer_note ??
      req.body.note ??
      req.body.notes ??
      ''
    ).toString().trim();
    const paymentMethod = normalizePaymentMethod(
      req.body.paymentMethod || req.body.payment_method,
    );
    const qrOrderSettings = await resolveQrOrderSettings({
      client,
      tenantId,
      branchId,
    });
    const paymentProofPayload = resolvePaymentProofPayload({ req });
    const validationBase64Data = resolveValidationBase64Data(paymentProofPayload);
    const hasUploadedProofImage = Boolean(validationBase64Data);
    const hasPaymentProof =
      hasUploadedProofImage ||
      Boolean((paymentProofPayload.paymentProofUrl || '').toString().trim());
    let paymentProofUrl = paymentProofPayload.paymentProofUrl || null;
    let paymentState = resolvePaymentState(paymentMethod);
    let branchMeta = null;

    if (branchId !== null) {
      try {
        const branchResult = await client.query(
          `SELECT id, name, branch_code
           FROM branches
           WHERE tenant_id = $1 AND id = $2
           LIMIT 1`,
          [tenantId, branchId],
        );
        branchMeta = branchResult.rows?.[0] || null;
      } catch (branchError) {
        console.warn('[publicQrController] Branch lookup skipped:', branchError.message);
      }
    }

    if (paymentMethod === PAYMENT_METHOD_CASHIER && !qrOrderSettings.allowPayAtCashier) {
      const error = new Error('Pembayaran di kasir sedang dinonaktifkan untuk web ordering.');
      error.statusCode = 400;
      throw error;
    }

    if (paymentMethod === PAYMENT_METHOD_QRIS && qrOrderSettings.isPaymentProofMandatory && !hasPaymentProof) {
      const error = new Error('Upload bukti transfer QRIS wajib sebelum pesanan dikirim.');
      error.statusCode = 400;
      throw error;
    }

    await client.query('BEGIN');

    const tableResult = await client.query(
      `SELECT id, status, table_number
       FROM tables
       WHERE id = $1 AND tenant_id = $2
       LIMIT 1
       FOR UPDATE`,
      [tableId, tenantId],
    );

    if ((tableResult.rowCount || 0) === 0) {
      const error = new Error('Meja tidak ditemukan untuk tenant ini');
      error.statusCode = 404;
      throw error;
    }

    const productIds = items.map((item) => item.productId);
    const productColumns = await getTableColumnSet(client, 'products');
    const softDeletePredicate = resolveSoftDeletePredicate(productColumns);
    const stockTrackedExpression = productColumns.has('is_stock_tracked')
      ? 'COALESCE(is_stock_tracked, true)'
      : productColumns.has('stock_tracked')
        ? 'COALESCE(stock_tracked, true)'
        : 'true';
    const productsResult = await client.query(
            `SELECT id, name, price,
              COALESCE(is_service, false) AS is_service,
              COALESCE(is_available, true) AS is_available,
              ${stockTrackedExpression} AS is_stock_tracked,
              COALESCE(stock, 0) AS stock
       FROM products
       WHERE tenant_id = $1
         AND id = ANY($2::text[])
         AND ${softDeletePredicate}
         AND ($3::bigint IS NULL OR branch_id = $3)
         AND COALESCE(is_active, true) = true`,
      [tenantId, productIds, branchId],
    );

    const productMap = new Map((productsResult.rows || []).map((row) => [row.id, row]));

    let totalAmount = 0;
    const normalizedItems = items.map((item) => {
      const product = productMap.get(item.productId);
      if (!product) {
        const error = new Error(`Produk tidak ditemukan: ${item.productId}`);
        error.statusCode = 404;
        throw error;
      }

      const isService = product.is_service === true;
      const isStockTracked = product.is_stock_tracked !== false;
      const isAvailable = product.is_available !== false;
      if (!isAvailable) {
        const error = new Error(`Produk sedang habis/tidak tersedia: ${product.name}`);
        error.statusCode = 400;
        throw error;
      }
      const availableStock = Number(product.stock || 0);
      if (!isService && isStockTracked && availableStock < item.qty) {
        const error = new Error(`Stok tidak cukup untuk produk ${product.name}`);
        error.statusCode = 400;
        throw error;
      }

      const unitPrice = item.customPrice !== undefined
        ? item.customPrice
        : Number(product.price || 0);

      totalAmount += unitPrice * item.qty;

      return {
        productId: item.productId,
        productName: product.name,
        qty: item.qty,
        customPrice: unitPrice,
        note: item.note,
        isService,
        isStockTracked,
      };
    });

    if (
      paymentMethod === PAYMENT_METHOD_QRIS &&
      qrOrderSettings.isPaymentProofMandatory &&
      hasUploadedProofImage &&
      validationBase64Data
    ) {
      const validation = await validatePaymentProof(
        validationBase64Data,
        paymentProofPayload.uploadMimeType || 'image/jpeg',
        totalAmount,
      );

      const detectedAmount = Number(validation?.transferredAmount);
      if (Number.isFinite(detectedAmount) && detectedAmount < totalAmount) {
        await client.query('ROLLBACK');
        return res.status(400).json({
          success: false,
          error: `Nominal transfer (${Math.trunc(detectedAmount)}) lebih kecil dari total pesanan (${Math.trunc(totalAmount)}).`,
          code: 'INSUFFICIENT_TRANSFER_AMOUNT',
        });
      }

      if (!validation?.isValid) {
        await client.query('ROLLBACK');
        return res.status(400).json({
          success: false,
          error: validation?.reason || 'Bukti pembayaran tidak valid.',
        });
      }
    }

    if (!paymentProofUrl && paymentProofPayload.uploadBuffer && paymentProofPayload.uploadMimeType) {
      paymentProofUrl = await uploadPaymentProof({
        tenantId,
        buffer: paymentProofPayload.uploadBuffer,
        mimeType: paymentProofPayload.uploadMimeType,
        req,
      });
    }

    if (paymentMethod === PAYMENT_METHOD_QRIS && paymentProofUrl) {
      paymentState = {
        paymentMethodLabel: 'QRIS',
        paymentStatus: 'PAID',
        orderStatus: 'PREPARING',
      };
    }

    const salesRecordColumns = await getTableColumnSet(client, 'sales_records');
    const supportsBranchIdOnSales = salesRecordColumns.has('branch_id');
    const supportsOrderStatus = salesRecordColumns.has('order_status');
    const supportsPaymentStatus = salesRecordColumns.has('payment_status');
    const supportsStatus = salesRecordColumns.has('status');
    const supportsIsVoid = salesRecordColumns.has('is_void');
    const supportsAmountPaid = salesRecordColumns.has('amount_paid');
    const supportsItemsJson = salesRecordColumns.has('items_json');
    const supportsSpecialNote = salesRecordColumns.has('special_note');
    const supportsPaymentProofUrl = salesRecordColumns.has('payment_proof_url');
    const activeSaleFilters = ['tenant_id = $1', 'table_id = $2'];
    const activeSaleParams = [tenantId, tableId];

    if (supportsBranchIdOnSales) {
      if (branchId !== null) {
        activeSaleFilters.push(`(branch_id = $${activeSaleParams.length + 1} OR branch_id IS NULL)`);
        activeSaleParams.push(branchId);
      }
    }

    if (supportsIsVoid) {
      activeSaleFilters.push('COALESCE(is_void, false) = false');
    }

    const activeStatePredicates = [];
    if (supportsStatus) {
      activeStatePredicates.push("UPPER(COALESCE(status::text, '')) = 'ACTIVE'");
    }
    if (supportsPaymentStatus) {
      activeStatePredicates.push("UPPER(COALESCE(payment_status::text, '')) IN ('UNPAID', 'PENDING_PAYMENT', 'PARTIAL', 'PARTIALLY_PAID')");
    }
    if (supportsOrderStatus) {
      activeStatePredicates.push("UPPER(COALESCE(order_status::text, '')) IN ('PENDING', 'PREPARING', 'READY_FOR_PICKUP')");
    }

    if (activeStatePredicates.length > 0) {
      activeSaleFilters.push(`(${activeStatePredicates.join(' OR ')})`);
    }

    const existingSaleResult = await client.query(
      `SELECT id,
              reference_id,
              receipt_number,
              cashier_name,
              total_amount,
              total_price,
              ${supportsOrderStatus ? 'order_status,' : 'NULL::text AS order_status,'}
              ${supportsPaymentStatus ? 'payment_status,' : 'NULL::text AS payment_status,'}
              payment_method,
              ${supportsItemsJson ? 'items_json,' : 'NULL::jsonb AS items_json,'}
              ${supportsAmountPaid ? 'amount_paid,' : 'NULL::numeric AS amount_paid,'}
              ${supportsSpecialNote ? 'special_note,' : 'NULL::text AS special_note,'}
              ${supportsPaymentProofUrl ? 'payment_proof_url,' : 'NULL::text AS payment_proof_url,'}
              customer_name
       FROM sales_records
       WHERE ${activeSaleFilters.join(' AND ')}
       ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
       LIMIT 1
       FOR UPDATE`,
      activeSaleParams,
    );

    let sale = null;
    let orderAction = 'NEW_ORDER';
    let responseStatusCode = 201;

    const existingSale = existingSaleResult.rows?.[0] || null;
    const salesRecordItemColumns = await getTableColumnSet(client, 'sales_record_items');
    const supportsBatchSequence = salesRecordItemColumns.has('batch_sequence');
    let currentBatchSequence = 1;
    let newItemsPayload = stampItemsWithBatchSequence(normalizedItems, currentBatchSequence);
    let mergedItemsJsonPayload = [...newItemsPayload];

    if (existingSale) {
      orderAction = 'APPENDED_TO_EXISTING';
      responseStatusCode = 200;

      const existingBatchFromItems = parseJsonArraySafe(existingSale.items_json)
        .reduce((maxSequence, item) => Math.max(maxSequence, getItemBatchSequence(item)), 0);

      let existingBatchFromRows = 0;
      if (supportsBatchSequence) {
        const existingBatchResult = await client.query(
          `SELECT COALESCE(MAX(batch_sequence), 0) AS max_batch_sequence
           FROM sales_record_items
           WHERE tenant_id = $1
             AND sales_record_id = $2`,
          [tenantId, existingSale.id],
        );
        existingBatchFromRows = parseBatchSequence(existingBatchResult.rows?.[0]?.max_batch_sequence);
      }

      currentBatchSequence = Math.max(existingBatchFromItems, existingBatchFromRows) + 1;
      newItemsPayload = stampItemsWithBatchSequence(normalizedItems, currentBatchSequence);
      mergedItemsJsonPayload = [
        ...parseJsonArraySafe(existingSale.items_json),
        ...newItemsPayload,
      ];

      const existingTotalAmount = Number(existingSale.total_amount ?? existingSale.total_price ?? 0) || 0;
      const mergedTotalAmount = existingTotalAmount + totalAmount;
      const previousAmountPaid = Number(existingSale.amount_paid || 0) || 0;

      let nextPaymentStatus = existingSale.payment_status;
      if (supportsPaymentStatus && previousAmountPaid < mergedTotalAmount) {
        const methodLabel = (existingSale.payment_method || paymentState.paymentMethodLabel || '').toString().toUpperCase();
        if (methodLabel.includes('QRIS') || methodLabel.includes('DIGITAL')) {
          nextPaymentStatus = 'PENDING_PAYMENT';
        } else {
          nextPaymentStatus = 'UNPAID';
        }
      }

      const mergedItemsJson = supportsItemsJson
        ? JSON.stringify(mergedItemsJsonPayload)
        : null;

      const updateFields = [
        'total_price = $1',
        'total_amount = $2',
        'updated_at = NOW()',
      ];
      const updateParams = [mergedTotalAmount, mergedTotalAmount];
      let paramIndex = updateParams.length;

      if (supportsItemsJson) {
        paramIndex += 1;
        updateFields.push(`items_json = $${paramIndex}::jsonb`);
        updateParams.push(mergedItemsJson);
      }

      if (supportsPaymentStatus) {
        paramIndex += 1;
        updateFields.push(`payment_status = $${paramIndex}`);
        updateParams.push(nextPaymentStatus);
      }

      if (supportsPaymentProofUrl && paymentProofUrl) {
        paramIndex += 1;
        updateFields.push(`payment_proof_url = COALESCE(NULLIF(payment_proof_url, ''), $${paramIndex})`);
        updateParams.push(paymentProofUrl);
      }

      paramIndex += 1;
      updateParams.push(tenantId);
      const tenantParamIndex = paramIndex;

      paramIndex += 1;
      updateParams.push(existingSale.id);
      const idParamIndex = paramIndex;

      const saleUpdate = await client.query(
        `UPDATE sales_records
         SET ${updateFields.join(', ')}
         WHERE tenant_id = $${tenantParamIndex}
           AND id = $${idParamIndex}
         RETURNING id, reference_id, receipt_number, cashier_name, order_status, payment_status, payment_method, total_amount${supportsPaymentProofUrl ? ', payment_proof_url' : ''}`,
        updateParams,
      );

      sale = saleUpdate.rows?.[0] || null;
      if (!sale) {
        const error = new Error('Gagal menambahkan item ke pesanan aktif');
        error.statusCode = 500;
        throw error;
      }
    } else {
      const referenceId = `qr_${Date.now()}`;
      const receiptNumber = generateReceiptNumber();
      currentBatchSequence = 1;
      newItemsPayload = stampItemsWithBatchSequence(normalizedItems, currentBatchSequence);
      mergedItemsJsonPayload = [...newItemsPayload];

      const saleInsert = await client.query(
        `INSERT INTO sales_records (
           tenant_id,
           branch_id,
           table_id,
           reference_id,
           receipt_number,
           payment_method,
           payment_status,
           order_type,
           order_status,
           total_price,
           total_amount,
           customer_name,
           cashier_name,
           items_json,
           amount_paid,
           ${supportsPaymentProofUrl ? 'payment_proof_url,' : ''}
           created_at,
           updated_at
         )
         VALUES (
           $1, $2, $3, $4, $5, $6, $7, $8, $9,
           $10, $11, $12, $13, $14::jsonb, $15,
           ${supportsPaymentProofUrl ? '$16,' : ''}
           NOW(), NOW()
         )
         RETURNING id, reference_id, receipt_number, cashier_name, order_status, payment_status, payment_method, total_amount${supportsPaymentProofUrl ? ', payment_proof_url' : ''}`,
        [
          tenantId,
          branchId,
          tableId,
          referenceId,
          receiptNumber,
          paymentState.paymentMethodLabel,
          paymentState.paymentStatus,
          'DINE_IN',
          paymentState.orderStatus,
          totalAmount,
          totalAmount,
          customerName,
          'Online Order',
          JSON.stringify(newItemsPayload),
          paymentState.paymentStatus === 'PAID' ? totalAmount : 0,
          ...(supportsPaymentProofUrl ? [paymentProofUrl] : []),
        ],
      );

      sale = saleInsert.rows?.[0] || null;
      if (!sale) {
        const error = new Error('Gagal membuat pesanan QR');
        error.statusCode = 500;
        throw error;
      }
    }

    const salesRecordItemInsertSql = supportsBatchSequence
      ? `INSERT INTO sales_record_items (
           tenant_id,
           sales_record_id,
           product_id,
           product_name,
           qty,
           custom_price,
           note,
           is_service,
           batch_sequence,
           created_at,
           updated_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW())`
      : `INSERT INTO sales_record_items (
           tenant_id,
           sales_record_id,
           product_id,
           product_name,
           qty,
           custom_price,
           note,
           is_service,
           created_at,
           updated_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW())`;

    for (const item of normalizedItems) {
      await client.query(
        salesRecordItemInsertSql,
        supportsBatchSequence
          ? [
              tenantId,
              sale.id,
              item.productId,
              item.productName,
              item.qty,
              item.customPrice,
              item.note,
              item.isService,
              currentBatchSequence,
            ]
          : [
              tenantId,
              sale.id,
              item.productId,
              item.productName,
              item.qty,
              item.customPrice,
              item.note,
              item.isService,
            ],
      );

      if (!item.isService && item.isStockTracked) {
        await client.query(
          `UPDATE products
           SET stock = COALESCE(stock, 0) - $1,
               updated_at = NOW()
           WHERE tenant_id = $2 AND id = $3`,
          [item.qty, tenantId, item.productId],
        );
      }
    }

    await client.query(
      `UPDATE tables
       SET status = 'OCCUPIED',
           updated_at = NOW()
       WHERE tenant_id = $1 AND id = $2`,
      [tenantId, tableId],
    );

    await client.query('COMMIT');

    // Keep order-note persistence best-effort and outside transaction.
    // A failed statement inside transaction marks PostgreSQL tx as aborted.
    if (orderNote && supportsSpecialNote) {
      try {
        await client.query(
          `UPDATE sales_records
           SET special_note = CASE
                 WHEN COALESCE(TRIM(special_note), '') = '' THEN $1
                 WHEN POSITION($1 IN special_note) > 0 THEN special_note
                 ELSE special_note || E'\n' || $1
               END,
               updated_at = NOW()
           WHERE tenant_id = $2
             AND id = $3`,
          [orderNote, tenantId, sale.id],
        );
      } catch (_) {
        // Backward compatible when sales_records.special_note is not available yet.
      }
    }

    const totalItems = normalizedItems.reduce((sum, item) => sum + (Number(item.qty || 0) || 0), 0);
    const tableLabel = (tableResult.rows?.[0]?.table_number || '').toString().trim();
    const effectivePaymentProofUrl = (sale.payment_proof_url || paymentProofUrl || '').toString().trim() || null;
    const payloadItems = newItemsPayload.map((item) => ({
      product_id: item.productId,
      productId: item.productId,
      product_name: item.productName,
      productName: item.productName,
      qty: Number(item.qty || 0) || 0,
      custom_price: Number(item.customPrice || 0) || 0,
      customPrice: Number(item.customPrice || 0) || 0,
      note: item.note || '',
      item_note: item.note || '',
      notes: item.note || '',
      is_service: item.isService === true,
      isService: item.isService === true,
      batch_sequence: currentBatchSequence,
      batchSequence: currentBatchSequence,
    }));
    const orderPayload = {
      tenantId,
      tenant_id: tenantId,
      orderId: sale.id,
      orderAction,
      order_action: orderAction,
      referenceId: sale.reference_id,
      receiptNumber: sale.receipt_number,
      branchId,
      branchName: (branchMeta?.name || '').toString().trim() || null,
      branchCode: (branchMeta?.branch_code || '').toString().trim() || null,
      tableId,
      table_id: tableId,
      tableName: tableLabel || String(tableId),
      table_number: tableLabel || null,
      orderType: 'DINE_IN',
      order_type: 'DINE_IN',
      currentBatchSequence,
      current_batch_sequence: currentBatchSequence,
      new_items: payloadItems,
      items: payloadItems,
      items_json: mergedItemsJsonPayload,
      orderStatus: sale.order_status,
      paymentStatus: sale.payment_status,
      paymentMethod: sale.payment_method,
      paymentProofUrl: effectivePaymentProofUrl,
      payment_proof_url: effectivePaymentProofUrl,
      customerName,
      orderNote,
      special_note: orderNote || null,
      specialNote: orderNote || null,
      totalItems,
      grandTotal: Number(sale.total_amount || 0),
      created_at: new Date().toISOString(),
    };

    const incomingQrOrderPayload = {
      ...orderPayload,
      orderAction: 'NEW_ORDER',
      order_action: 'NEW_ORDER',
    };

    emitToTenant(tenantId, 'incoming_qr_order', incomingQrOrderPayload);

    emitToTenant(tenantId, 'qr_order_payment_status', {
      tenantId,
      ...orderPayload,
      orderNote: orderNote || null,
      paymentMethod,
      updatedAt: new Date().toISOString(),
    });

    return jsonOk(
      res,
      {
        ...sale,
        ...orderPayload,
        branch_id: branchId,
        branch_name: (branchMeta?.name || '').toString().trim() || null,
        branch_code: (branchMeta?.branch_code || '').toString().trim() || null,
      },
      orderAction === 'APPENDED_TO_EXISTING'
        ? 'Pesanan QR ditambahkan ke transaksi aktif meja'
        : 'Pesanan QR berhasil dibuat',
      responseStatusCode,
    );
  } catch (error) {
    await client.query('ROLLBACK');
    return jsonError(res, error.statusCode || 500, error.message || 'Internal server error', error.message);
  } finally {
    client.release();
  }
};

const checkoutQrOrder = async (req, res) => {
  const client = await getSharedPool().connect();
  try {
    const tenantId = parseTenantId(req.body.tenantId || req.body.tenant_id);
    const branchId = parseOptionalBranchId(req.body.branchId || req.body.branch_id);
    const orderId = (req.body.orderId || req.body.order_id || req.body.salesRecordId || '').toString().trim();
    const referenceId = (req.body.referenceId || req.body.reference_id || '').toString().trim();
    const receiptNumber = (req.body.receiptNumber || req.body.receipt_number || '').toString().trim();
    const paymentMethod = normalizePaymentMethod(req.body.paymentMethod || req.body.payment_method);
    const paymentState = resolvePaymentState(paymentMethod);

    if (!orderId && !referenceId && !receiptNumber) {
      const error = new Error('orderId/referenceId/receiptNumber wajib diisi');
      error.statusCode = 400;
      throw error;
    }

    await client.query('BEGIN');

    const lookup = await client.query(
      `SELECT id, reference_id, receipt_number,
              payment_status, order_status, payment_method,
              total_amount, total_price,
              branch_id,
              table_id,
              table_number,
              order_type,
              customer_name,
              special_note
       FROM sales_records
       WHERE tenant_id = $1
         AND ($2::bigint IS NULL OR branch_id = $2)
         AND (
           ($3::text <> '' AND id::text = $3)
           OR ($4::text <> '' AND reference_id = $4)
           OR ($5::text <> '' AND receipt_number = $5)
         )
       ORDER BY created_at DESC
       LIMIT 1`,
      [tenantId, branchId, orderId, referenceId, receiptNumber],
    );

    const sale = lookup.rows?.[0];
    if (!sale) {
      const error = new Error('Pesanan tidak ditemukan');
      error.statusCode = 404;
      throw error;
    }

    const updatedResult = await client.query(
      `UPDATE sales_records
       SET payment_method = $1,
           payment_status = $2,
           order_status = $3,
           updated_at = NOW()
       WHERE tenant_id = $4
         AND id = $5
       RETURNING id, reference_id, receipt_number,
                 payment_method, payment_status, order_status,
                 total_amount, total_price,
                 branch_id,
                 table_id,
                 table_number,
                 order_type,
                 customer_name,
                 special_note`,
      [
        paymentState.paymentMethodLabel,
        paymentState.paymentStatus,
        paymentState.orderStatus,
        tenantId,
        sale.id,
      ],
    );

    await client.query('COMMIT');

    const updated = updatedResult.rows?.[0] || sale;
    const responsePayload = {
      ...updated,
      payment_method_code: paymentMethod,
      order_note: (updated.special_note || '').toString().trim() || null,
      specialNote: (updated.special_note || '').toString().trim() || null,
      paymentGateway: paymentMethod === PAYMENT_METHOD_DIGITAL
        ? {
            mode: 'HYBRID',
            status: 'PENDING_PAYMENT',
            actionRequired: 'WAIT_WEBHOOK',
          }
        : null,
    };

    emitToTenant(tenantId, 'qr_order_payment_status', {
      tenantId,
      orderId: updated.id,
      referenceId: updated.reference_id,
      receiptNumber: updated.receipt_number,
      branchId: updated.branch_id ?? null,
      tableId: updated.table_id ?? null,
      table_number: (updated.table_number || '').toString().trim() || null,
      orderType: (updated.order_type || '').toString().trim() || null,
      orderNote: (updated.special_note || '').toString().trim() || null,
      special_note: (updated.special_note || '').toString().trim() || null,
      paymentMethod,
      paymentStatus: updated.payment_status,
      orderStatus: updated.order_status,
      updatedAt: new Date().toISOString(),
    });

    return jsonOk(res, responsePayload, 'Checkout QR berhasil diproses', 200);
  } catch (error) {
    await client.query('ROLLBACK');
    return jsonError(res, error.statusCode || 500, error.message || 'Internal server error', error.message);
  } finally {
    client.release();
  }
};

const handlePaymentWebhook = async (req, res) => {
  const client = await getSharedPool().connect();
  try {
    const tenantId = parseTenantId(req.body.tenantId || req.body.tenant_id);
    const orderId = (req.body.orderId || req.body.order_id || req.body.salesRecordId || '').toString().trim();
    const referenceId = (req.body.referenceId || req.body.reference_id || '').toString().trim();
    const receiptNumber = (req.body.receiptNumber || req.body.receipt_number || '').toString().trim();
    const gatewayStatus =
      req.body.paymentStatus ||
      req.body.payment_status ||
      req.body.transactionStatus ||
      req.body.transaction_status ||
      req.body.status;
    const paidAmount = Number(req.body.paidAmount ?? req.body.paid_amount ?? req.body.amount_paid ?? 0) || 0;

    if (!orderId && !referenceId && !receiptNumber) {
      const error = new Error('orderId/referenceId/receiptNumber wajib diisi');
      error.statusCode = 400;
      throw error;
    }

    await client.query('BEGIN');

    const lookup = await client.query(
      `SELECT id, reference_id, receipt_number,
              payment_method, payment_status, order_status,
              payment_proof_url,
              total_amount,
              branch_id,
              table_id,
              table_number,
              order_type,
              special_note
       FROM sales_records
       WHERE tenant_id = $1
         AND (
           ($2::text <> '' AND id::text = $2)
           OR ($3::text <> '' AND reference_id = $3)
           OR ($4::text <> '' AND receipt_number = $4)
         )
       ORDER BY created_at DESC
       LIMIT 1`,
      [tenantId, orderId, referenceId, receiptNumber],
    );

    const sale = lookup.rows?.[0];
    if (!sale) {
      const error = new Error('Pesanan tidak ditemukan untuk webhook pembayaran');
      error.statusCode = 404;
      throw error;
    }

    const paymentState = resolveWebhookPaymentState(gatewayStatus);
    const safePaidAmount = paidAmount > 0 ? paidAmount : null;

    const updatedResult = await client.query(
      `UPDATE sales_records
       SET payment_status = $1,
           order_status = $2,
           amount_paid = COALESCE($3, amount_paid),
           updated_at = NOW()
       WHERE tenant_id = $4
         AND id = $5
       RETURNING id, reference_id, receipt_number,
                 payment_method, payment_status, order_status,
                 payment_proof_url,
                 amount_paid, total_amount,
                 branch_id,
                 table_id,
                 table_number,
                 order_type,
                 special_note`,
      [paymentState.paymentStatus, paymentState.orderStatus, safePaidAmount, tenantId, sale.id],
    );

    await client.query('COMMIT');

    const updated = updatedResult.rows?.[0] || sale;

    emitToTenant(tenantId, 'qr_order_payment_status', {
      tenantId,
      orderId: updated.id,
      referenceId: updated.reference_id,
      receiptNumber: updated.receipt_number,
      branchId: updated.branch_id ?? null,
      tableId: updated.table_id ?? null,
      table_number: (updated.table_number || '').toString().trim() || null,
      orderType: (updated.order_type || '').toString().trim() || null,
      orderNote: (updated.special_note || '').toString().trim() || null,
      special_note: (updated.special_note || '').toString().trim() || null,
      paymentMethod: (updated.payment_method || '').toString().trim() || null,
      paymentStatus: updated.payment_status,
      orderStatus: updated.order_status,
      paymentProofUrl: (updated.payment_proof_url || '').toString().trim() || null,
      payment_proof_url: (updated.payment_proof_url || '').toString().trim() || null,
      amountPaid: updated.amount_paid,
      grandTotal: Number(updated.total_amount || 0),
      updatedAt: new Date().toISOString(),
      source: 'payment_webhook',
    });

    return jsonOk(res, updated, 'Webhook pembayaran diproses', 200);
  } catch (error) {
    await client.query('ROLLBACK');
    return jsonError(res, error.statusCode || 500, error.message || 'Internal server error', error.message);
  } finally {
    client.release();
  }
};

module.exports = {
  getQrMenu,
  createQrOrder,
  checkoutQrOrder,
  handlePaymentWebhook,
};
