const { jsonOk, jsonError } = require('../utils/http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { getSharedPool } = require('../middlewares/tenantResolver');
const { normalizeTenantId, getTableColumnSet } = require('../utils/sqlHelpers');
const { emitToTenant } = require('../services/socketServer');
const { uploadBase64Object } = require('../services/objectStorageService');
const { validatePaymentProof } = require('../utils/receiptScanner');
const {
  isTransientDbError,
  withRetries,
  getClientFromPool,
  runTransaction,
  storeFailedPayload,
} = require('../utils/dbSafe');

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

    const nested = raw.product && typeof raw.product === 'object' ? raw.product : {};
    const productId = (
      nested.id ?? nested.product_id ?? nested.productId ??
      raw.productId ?? raw.product_id ?? ''
    ).toString().trim();
    if (!productId) {
      const error = new Error(`items[${index}].productId wajib diisi`);
      error.statusCode = 400;
      throw error;
    }

    const qty = Number(
      raw.qty ?? raw.quantity ?? nested.qty ?? nested.quantity ?? 0
    );
    if (!Number.isInteger(qty) || qty <= 0) {
      const error = new Error(`items[${index}].qty harus angka bulat > 0`);
      error.statusCode = 400;
      throw error;
    }

    const customPriceRaw = raw.customPrice ?? raw.custom_price ?? nested.customPrice ?? nested.custom_price;
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
        whereParts.push(`(branch_id::text = $${branchParamIndex}::text OR branch_id IS NULL)`);
        params.push(branchId);
      }

      const orderParts = [];
      if (supportsBranch && branchId !== null && branchParamIndex > 0) {
        orderParts.push(`CASE WHEN branch_id::text = $${branchParamIndex}::text THEN 0 WHEN branch_id IS NULL THEN 1 ELSE 2 END`);
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
        whereParts.push('(branch_id::text = $2::text OR branch_id IS NULL)');
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

const toQrOrderPayloadItems = (items) => {
  if (!Array.isArray(items)) {
    return [];
  }

  return items
    .map((item) => {
      if (!item || typeof item !== 'object') {
        return null;
      }

      const nested = item.product && typeof item.product === 'object' ? item.product : {};
      const productId = (
        nested.id ?? nested.product_id ?? nested.productId ??
        item.productId ?? item.product_id ?? ''
      ).toString().trim();
      const productName = (
        nested.name ?? nested.product_name ?? nested.productName ??
        item.productName ?? item.product_name ?? ''
      ).toString().trim();
      const qty = Number(
        item.qty ?? item.quantity ?? nested.qty ?? nested.quantity ?? 0
      );
      const customPrice = Number(
        item.customPrice ?? item.custom_price ??
        nested.customPrice ?? nested.custom_price ??
        nested.price ?? nested.unit_price ?? nested.unitPrice ??
        0
      );
      const note = (
        nested.note ??
        item.note ?? item.item_note ?? item.notes ?? ''
      ).toString().trim();
      const isService =
        nested.isService === true || nested.is_service === true ||
        item.isService === true || item.is_service === true;
      const batchSequence = parseBatchSequence(item.batchSequence ?? item.batch_sequence ?? item.batch ?? item.sequence ?? nested.batchSequence ?? nested.batch_sequence ?? nested.batch ?? nested.sequence) || 1;

      if ((!productId && !isService) || !Number.isFinite(qty) || qty <= 0) {
        return null;
      }

      return {
        product_id: productId || null,
        productId: productId || null,
        product_name: productName || null,
        productName: productName || null,
        qty,
        custom_price: Number.isFinite(customPrice) ? customPrice : 0,
        customPrice: Number.isFinite(customPrice) ? customPrice : 0,
        note: note || '',
        item_note: note || '',
        notes: note || '',
        is_service: isService,
        isService,
        batch_sequence: batchSequence,
        batchSequence,
      };
    })
    .filter(Boolean);
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
    const supportsBranchProducts = productColumns.has('branch_id');
    const supportsBranchCategories = categoryColumns.has('branch_id');
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

    const branchProductsClause = supportsBranchProducts
      ? `AND ($3::text IS NULL OR branch_id::text = $3::text)`
      : `AND ($3::text IS NULL OR true)`;
    const branchFallbackClause = supportsBranchProducts
      ? `AND ($2::text IS NULL OR branch_id::text = $2::text)`
      : `AND ($2::text IS NULL OR true)`;
    const branchCategoriesClause = supportsBranchCategories
      ? `AND ($2::text IS NULL OR branch_id::text = $2::text)`
      : `AND ($2::text IS NULL OR true)`;

    const productParams = [tenantId, Array.from(FNB_PRODUCT_TYPES)];
    if (supportsBranchProducts) productParams.push(branchId);
    else productParams.push(null);

    const result = await pool.query(
            `SELECT id, name, category, product_type, price, stock, image_url, unit,
              ${supportsPrintDestination ? 'print_destination,' : 'NULL::text AS print_destination,'}
              ${stockTrackedExpression} AS is_stock_tracked,
              COALESCE(is_service, false) AS is_service,
              COALESCE(is_available, true) AS is_available
      FROM products
      WHERE tenant_id = $1::text
         AND ${softDeletePredicate}
         ${branchProductsClause}
         AND COALESCE(is_active, true) = true
         AND (
           UPPER(COALESCE(product_type, '')) = ANY($2::text[])
         )
         AND ${stockVisibilityPredicate}
       ORDER BY name ASC`,
      productParams,
    );

    let rows = result.rows || [];
    if (rows.length === 0) {
      const fallbackParams = [tenantId];
      if (supportsBranchProducts) fallbackParams.push(branchId);
      else fallbackParams.push(null);

      const fallbackResult = await pool.query(
        `SELECT id, name, category, product_type, price, stock, image_url, unit,
          ${stockTrackedExpression} AS is_stock_tracked,
          COALESCE(is_service, false) AS is_service,
          COALESCE(is_available, true) AS is_available
         FROM products
         WHERE tenant_id = $1::text
           AND ${softDeletePredicate}
           ${branchFallbackClause}
           AND COALESCE(is_active, true) = true
           AND ${stockVisibilityPredicate}
         ORDER BY name ASC`,
        fallbackParams,
      );
      rows = fallbackResult.rows || [];
    }

    let categoryRows = [];
    try {
      const categoryParams = [tenantId];
      if (supportsBranchCategories) categoryParams.push(branchId);
      else categoryParams.push(null);

      const categoriesResult = await pool.query(
        `SELECT id, name
         FROM categories
         WHERE tenant_id = $1::text
           AND ${categorySoftDeletePredicate}
           ${branchCategoriesClause}
         ORDER BY name ASC`,
        categoryParams,
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
         WHERE tenant_id = $1::text
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

const generateReferenceId = (clientProvided = '') => {
  const trimmed = (clientProvided || '').toString().trim();
  if (trimmed) return trimmed;
  const stamp = Date.now();
  const rand = crypto.randomBytes(6).toString('hex');
  return `qr_${stamp}_${rand}`;
};

const findExistingSaleByReferenceId = async (clientOrPool, { tenantId, referenceId, hasTenantColumn }) => {
  const sql = hasTenantColumn
    ? 'SELECT * FROM sales_records WHERE reference_id = $1 AND tenant_id = $2 ORDER BY id DESC LIMIT 1'
    : 'SELECT * FROM sales_records WHERE reference_id = $1 ORDER BY id DESC LIMIT 1';
  const params = hasTenantColumn ? [referenceId, tenantId] : [referenceId];
  const result = await clientOrPool.query(sql, params);
  return result.rows?.[0] || null;
};

const buildQrOrderPayload = ({
  tenantId,
  branchMeta,
  tableId,
  tableResult,
  normalizedItems,
  newItemsPayload,
  mergedItemsJsonPayload,
  paymentState,
  sale,
  paymentProofUrl,
  customerName,
  orderNote,
  orderAction,
  currentBatchSequence,
}) => {
  const totalItems = normalizedItems.reduce((sum, item) => sum + (Number(item.qty || 0) || 0), 0);
  const tableLabel = (tableResult.rows?.[0]?.table_number || '').toString().trim();
  const effectivePaymentProofUrl = (sale.payment_proof_url || paymentProofUrl || '').toString().trim() || null;
  const payloadItems = toQrOrderPayloadItems(newItemsPayload);
  const branchId = branchMeta?.id ?? null;
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

  return {
    orderPayload,
    payloadItems,
    totalItems,
    effectivePaymentProofUrl,
    tableLabel,
  };
};

const createQrOrder = async (req, res) => {
  const pool = getSharedPool();

  const incomingBody = (req.body && typeof req.body === 'object')
    ? { ...req.body }
    : {};
  const fileMeta = req.file
    ? {
        fieldName: req.file.fieldname,
        originalName: req.file.originalname,
        mimeType: req.file.mimetype,
        sizeBytes: req.file.size,
      }
    : null;
  const tenantIdForLog = normalizeTenantId(
    incomingBody.tenantId ||
    incomingBody.tenant_id ||
    req.user?.tenantId ||
    req.tenant?.tenantId ||
    '',
  );
  let persistedReferenceId = '';
  let persistedOrderId = '';
  const requestId = `${Date.now()}-${crypto.randomBytes(4).toString('hex')}`;

  const runOrderCreation = async () => {
    const client = await getClientFromPool(pool);
    try {
      const tenantId = parseTenantId(incomingBody.tenantId || incomingBody.tenant_id);
      const tableId = parseTableId(incomingBody.tableId || incomingBody.table_id);
      const branchId = parseOptionalBranchId(incomingBody.branchId || incomingBody.branch_id);
      const rawItems = parseRawItems(incomingBody.items ?? incomingBody.orderItems);
      const items = parseOrderItems(rawItems);
      const customerName = (
        incomingBody.customerName ||
        incomingBody.customer_name ||
        'Guest'
      ).toString().trim() || 'Guest';
      const orderNote = (
        incomingBody.orderNote ??
        incomingBody.order_note ??
        incomingBody.special_note ??
        incomingBody.specialNote ??
        incomingBody.customerNote ??
        incomingBody.customer_note ??
        incomingBody.note ??
        incomingBody.notes ??
        ''
      ).toString().trim();
      const paymentMethod = normalizePaymentMethod(
        incomingBody.paymentMethod || incomingBody.payment_method,
      );
      const clientProvidedReferenceId = (
        incomingBody.referenceId ||
        incomingBody.reference_id ||
        incomingBody.localId ||
        incomingBody.local_id ||
        incomingBody.idempotencyKey ||
        incomingBody.idempotency_key ||
        ''
      ).toString().trim();
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

      const salesRecordColumns = await getTableColumnSet(client, 'sales_records');
      const hasTenantColumn = salesRecordColumns.has('tenant_id');
      const supportsItemsJson = salesRecordColumns.has('items_json');
      const supportsItems = salesRecordColumns.has('items');
      const supportsSpecialNote = salesRecordColumns.has('special_note');
      const supportsPaymentProofUrl = salesRecordColumns.has('payment_proof_url');
      const supportsReferenceId = salesRecordColumns.has('reference_id');
      const saleForIdempotency = {
        tenantId,
        branchId,
        tableId,
        paymentMethod,
        paymentProofUrl,
        customerName,
        items,
        orderNote,
      };
      const referenceId = generateReferenceId(
        supportsReferenceId ? clientProvidedReferenceId : '',
      );
      persistedReferenceId = referenceId;

      const transactionResult = await runTransaction(
        client,
        async (txClient) => {
          const tableResult = await txClient.query(
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

          if (supportsReferenceId && referenceId) {
            const existing = await findExistingSaleByReferenceId(
              txClient,
              {
                tenantId,
                referenceId,
                hasTenantColumn,
              },
            );
            if (existing) {
              return {
                sale: existing,
                tableResult,
                isIdempotentHit: true,
                normalizedItems: [],
                mergedItemsJsonPayload: [],
                newItemsPayload: [],
                paymentState: {
                  paymentStatus: existing.payment_status || null,
                  orderStatus: existing.order_status || null,
                },
                paymentProofUrl: existing.payment_proof_url || paymentProofUrl,
                orderAction: 'NEW_ORDER_IDEMPOTENT_HIT',
                currentBatchSequence: Number(existing.current_batch_sequence || 1) || 1,
                supportsSpecialNote,
                saleForIdempotency,
              };
            }
          }

          const extractItemRefs = (item) => {
            if (!item || typeof item !== 'object') return { productId: '', qty: 0 };
            const nested = item.product && typeof item.product === 'object' ? item.product : {};
            const productId = (
              nested.id ?? nested.product_id ?? nested.productId ??
              item.product_id ?? item.productId ?? item.id ?? ''
            ).toString().trim();
            const qtyRaw = item.qty ?? item.quantity ?? nested.qty ?? nested.quantity ?? 0;
            const qty = Number.isFinite(Number(qtyRaw)) ? Number(qtyRaw) : 0;
            const customPriceRaw =
              item.customPrice ?? item.custom_price ??
              nested.customPrice ?? nested.custom_price ??
              nested.price ?? nested.unit_price ?? nested.unitPrice ??
              item.price ?? item.unit_price ?? item.unitPrice ?? null;
            const customPrice = (customPriceRaw === null || customPriceRaw === undefined || customPriceRaw === '')
              ? undefined
              : Number(customPriceRaw);
            const noteRaw =
              nested.note ??
              item.note ?? item.item_note ?? item.notes ?? '';
            return {
              productId,
              qty,
              customPrice: Number.isFinite(customPrice) ? customPrice : undefined,
              note: (noteRaw ?? '').toString().trim(),
            };
          };

          const productIds = items
            .map((item) => extractItemRefs(item).productId)
            .filter((id) => !!`${id}`.trim());
          const productColumns = await getTableColumnSet(txClient, 'products');
          const softDeletePredicate = resolveSoftDeletePredicate(productColumns);
          const stockTrackedExpression = productColumns.has('is_stock_tracked')
            ? 'COALESCE(is_stock_tracked, true)'
            : productColumns.has('stock_tracked')
              ? 'COALESCE(stock_tracked, true)'
              : 'true';
          const supportsProductBranch = productColumns.has('branch_id');
          const productBranchClause = supportsProductBranch
            ? 'AND ($3::text IS NULL OR branch_id::text = $3::text)'
            : 'AND ($3::text IS NULL OR true)';
          const productParams = [tenantId, productIds];
          if (supportsProductBranch) productParams.push(branchId);
          else productParams.push(null);
          const productsResult = await txClient.query(
            `SELECT id, name, price,
              COALESCE(is_service, false) AS is_service,
              COALESCE(is_available, true) AS is_available,
              ${stockTrackedExpression} AS is_stock_tracked,
              COALESCE(stock, 0) AS stock
       FROM products
       WHERE tenant_id = $1::text
         AND id = ANY($2)
         AND ${softDeletePredicate}
         ${productBranchClause}
         AND COALESCE(is_active, true) = true
       FOR UPDATE OF products`,
            productParams,
          );

          const productMap = new Map((productsResult.rows || []).map((row) => [String(row.id), row]));

          let totalAmount = 0;
          const normalizedItems = items.map((item) => {
            const refs = extractItemRefs(item);
            const productId = refs.productId;
            const qty = refs.qty;
            const product = productMap.get(productId);
            if (!product) {
              const error = new Error(`Produk tidak ditemukan: ${productId || '(empty)'}`);
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
            if (!isService && isStockTracked && availableStock < qty) {
              const error = new Error(`Stok tidak cukup untuk produk ${product.name}`);
              error.statusCode = 400;
              throw error;
            }

            const unitPrice = refs.customPrice !== undefined
              ? refs.customPrice
              : Number(product.price || 0);

            totalAmount += unitPrice * qty;

            return {
              productId,
              productName: product.name,
              qty,
              customPrice: unitPrice,
              note: refs.note || item.note || '',
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
              const insufficientError = new Error(
                `Nominal transfer (${Math.trunc(detectedAmount)}) lebih kecil dari total pesanan (${Math.trunc(totalAmount)}).`,
              );
              insufficientError.statusCode = 400;
              insufficientError.code = 'INSUFFICIENT_TRANSFER_AMOUNT';
              throw insufficientError;
            }

            if (!validation?.isValid) {
              const proofError = new Error(validation?.reason || 'Bukti pembayaran tidak valid.');
              proofError.statusCode = 400;
              proofError.code = 'INVALID_PAYMENT_PROOF';
              throw proofError;
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

          const salesRecordItemColumns = await getTableColumnSet(txClient, 'sales_record_items');
          const supportsBatchSequence = salesRecordItemColumns.has('batch_sequence');
          const currentBatchSequence = 1;
          const newItemsPayload = stampItemsWithBatchSequence(normalizedItems, currentBatchSequence);
          const mergedItemsJsonPayload = toQrOrderPayloadItems(newItemsPayload);
          const receiptNumber = generateReceiptNumber();

          const insertColumns = [
            'tenant_id',
            'branch_id',
            'table_id',
            'receipt_number',
            'payment_method',
            'payment_status',
            'order_type',
            'order_status',
            'total_price',
            'total_amount',
            'customer_name',
            'cashier_name',
          ];
          const insertValues = [
            tenantId,
            branchId,
            tableId,
            receiptNumber,
            paymentState.paymentMethodLabel,
            paymentState.paymentStatus,
            'DINE_IN',
            paymentState.orderStatus,
            totalAmount,
            totalAmount,
            customerName,
            'Online Order',
          ];

          if (supportsReferenceId) {
            insertColumns.unshift('reference_id');
            insertValues.unshift(referenceId);
          }

          if (supportsItemsJson) {
            insertColumns.push('items_json');
            insertValues.push(JSON.stringify(mergedItemsJsonPayload));
          }

          if (supportsItems) {
            insertColumns.push('items');
            insertValues.push(JSON.stringify(mergedItemsJsonPayload));
          }

          insertColumns.push('amount_paid');
          insertValues.push(paymentState.paymentStatus === 'PAID' ? totalAmount : 0);

          if (supportsPaymentProofUrl) {
            insertColumns.push('payment_proof_url');
            insertValues.push(paymentProofUrl);
          }

          if (supportsSpecialNote && orderNote) {
            insertColumns.push('special_note');
            insertValues.push(orderNote);
          }

          insertColumns.push('created_at');
          insertColumns.push('updated_at');

          const insertPlaceholders = insertValues.map((_, index) => {
            const param = `$${index + 1}`;
            const columnName = insertColumns[index];
            if (columnName === 'items_json') {
              return `${param}::jsonb`;
            }
            return param;
          });

          insertPlaceholders.push('NOW()');
          insertPlaceholders.push('NOW()');

          const saleInsert = await txClient.query(
            `INSERT INTO sales_records (
               ${insertColumns.join(',\n                 ')}
             )
             VALUES (
               ${insertPlaceholders.join(', ')}
             )
             RETURNING *`,
            insertValues,
          );

          const sale = saleInsert.rows?.[0] || null;
          if (!sale) {
            const error = new Error('Gagal membuat pesanan QR');
            error.statusCode = 500;
            throw error;
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
            await txClient.query(
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
              const nestedProduct =
                item.product && typeof item.product === 'object' ? item.product : {};
              const safeQty = Number.isFinite(Number(
                item.qty ?? item.quantity ?? nestedProduct.qty ?? nestedProduct.quantity
              )) ? Number(item.qty ?? item.quantity ?? nestedProduct.qty ?? nestedProduct.quantity) : 0;
              if (safeQty > 0) {
                const rawProductId =
                  nestedProduct.id ??
                  nestedProduct.product_id ??
                  nestedProduct.productId ??
                  item.product_id ??
                  item.productId ??
                  item.id;
                const safeProductId = (rawProductId === undefined || rawProductId === null)
                  ? ''
                  : `${rawProductId}`.trim();
                const safeTenantId = (tenantId === undefined || tenantId === null)
                  ? ''
                  : `${tenantId}`.trim();
                const productForError =
                  productMap.get(safeProductId) ||
                  productMap.get(String(rawProductId ?? '')) ||
                  productMap.get(rawProductId);
                const currentStock = Number.isFinite(Number(productForError?.stock ?? 0))
                  ? Number(productForError?.stock ?? 0)
                  : 0;

                if (currentStock < safeQty) {
                  const error = new Error(
                    `Stok produk ${productForError?.name || item.product_name || item.productName || safeProductId} tidak mencukupi / tidak ditemukan saat potong stok (Current=${currentStock}, Requested=${safeQty}, ID=${safeProductId})`,
                  );
                  error.statusCode = 400;
                  throw error;
                }

                const nextStock = currentStock - safeQty;

                const updateSql = `UPDATE products
                   SET stock = $1::numeric,
                       updated_at = NOW()
                   WHERE tenant_id = $2::text
                     AND id = $3
                   RETURNING id`;
                const queryParams = [nextStock, safeTenantId, safeProductId];

                console.log('DEBUG STOCK UPDATE CREATEQRORDER:', {
                  sql: updateSql,
                  params: queryParams,
                  rawProductId,
                  rawTypeof: typeof rawProductId,
                  safeProductId,
                  safeTypeof: typeof safeProductId,
                  qty: safeQty,
                  currentStock,
                  nextStock,
                  tenantId: safeTenantId,
                  productName: productForError?.name,
                  productTenant: productForError?.tenant_id,
                });
                console.log('DEBUG SQL:', updateSql, 'PARAMS:', queryParams, 'ITEM:', item);

                let stockUpdate = await txClient.query(updateSql, queryParams);
                if ((stockUpdate.rowCount || 0) === 0) {
                  console.warn('DEBUG CREATEQRORDER FALLBACK: stock update 0 rows WITH tenant_id. Trying fallback without tenant_id.');
                  const fallbackSql = `UPDATE products SET stock = $1::numeric, updated_at = NOW() WHERE id = $2 RETURNING id`;
                  const fallbackParams = [nextStock, safeProductId];
                  console.log('DEBUG SQL FALLBACK:', fallbackSql, 'PARAMS:', fallbackParams, 'ITEM:', item);
                  stockUpdate = await txClient.query(fallbackSql, fallbackParams);
                  console.warn(`  → Fallback rows: ${stockUpdate.rowCount || 0}`);
                }

                if ((stockUpdate.rowCount || 0) === 0) {
                  const error = new Error(
                    `Stok produk ${productForError?.name || item.product_name || item.productName || safeProductId} tidak ditemukan saat potong stok (Current=${currentStock}, Requested=${safeQty}, ID=${safeProductId}, tenantId=${safeTenantId}, productTenant=${productForError?.tenant_id ?? 'unknown'})`,
                  );
                  error.statusCode = 400;
                  throw error;
                }
              }
            }
          }

          const tableUpdate = await txClient.query(
            `UPDATE tables
             SET status = 'OCCUPIED',
                 updated_at = NOW()
             WHERE tenant_id = $1 AND id = $2
             RETURNING id`,
            [tenantId, tableId],
          );
          if ((tableUpdate.rowCount || 0) === 0) {
            throw new Error(
              `Update status meja gagal (row kosong): tenant=${tenantId} table=${tableId}`,
            );
          }

          return {
            sale,
            tableResult,
            isIdempotentHit: false,
            normalizedItems,
            mergedItemsJsonPayload,
            newItemsPayload,
            paymentState,
            paymentProofUrl,
            orderAction: 'NEW_ORDER',
            currentBatchSequence,
            supportsSpecialNote,
            saleForIdempotency,
          };
        },
        {
          label: `create_qr_order:${tenantId || 'unknown'}:${referenceId || 'noref'}`,
          maxAttempts: 4,
        },
      );

      const {
        sale,
        tableResult,
        isIdempotentHit,
        normalizedItems,
        mergedItemsJsonPayload,
        newItemsPayload,
        paymentProofUrl: finalPaymentProofUrl,
        orderAction,
        currentBatchSequence,
      } = transactionResult;

      persistedOrderId = `${sale?.id ?? ''}`;

      if (supportsSpecialNote && orderNote && !isIdempotentHit && sale?.id) {
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
            [orderNote, tenantIdForLog || sale.tenant_id || '', sale.id],
          );
        } catch (_) {}
      }

      const { orderPayload } = buildQrOrderPayload({
        tenantId: tenantIdForLog || sale?.tenant_id || '',
        branchMeta,
        tableId,
        tableResult,
        normalizedItems,
        newItemsPayload,
        mergedItemsJsonPayload,
        paymentState: transactionResult.paymentState,
        sale,
        paymentProofUrl: finalPaymentProofUrl,
        customerName,
        orderNote,
        orderAction,
        currentBatchSequence,
      });

      const incomingQrOrderPayload = {
        ...orderPayload,
        orderAction: isIdempotentHit ? (sale.order_status ? 'NEW_ORDER_REPLAYED' : 'NEW_ORDER') : 'NEW_ORDER',
        order_action: isIdempotentHit ? (sale.order_status ? 'NEW_ORDER_REPLAYED' : 'NEW_ORDER') : 'NEW_ORDER',
      };
      emitToTenant(tenantIdForLog || sale?.tenant_id || '', 'incoming_qr_order', incomingQrOrderPayload);

      emitToTenant(tenantIdForLog || sale?.tenant_id || '', 'qr_order_payment_status', {
        tenantId: tenantIdForLog || sale?.tenant_id || '',
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
          branch_id: branchMeta?.id ?? null,
          branch_name: (branchMeta?.name || '').toString().trim() || null,
          branch_code: (branchMeta?.branch_code || '').toString().trim() || null,
        },
        orderAction === 'APPENDED_TO_EXISTING'
          ? 'Pesanan QR ditambahkan ke transaksi aktif meja'
          : 'Pesanan QR berhasil dibuat',
        isIdempotentHit ? 200 : 201,
      );
    } finally {
      client.release();
    }
  };

  try {
    return await withRetries(
      runOrderCreation,
      {
        label: `create_qr_order:${requestId}`,
        maxAttempts: 2,
        baseDelayMs: 200,
        maxDelayMs: 1200,
        shouldRetry: (error, attempt) => {
          if ((error?.statusCode && error.statusCode >= 400 && error.statusCode < 500)) {
            return false;
          }
          if (attempt >= 2) return false;
          return isTransientDbError(error);
        },
      },
    );
  } catch (error) {
    try {
      await storeFailedPayload(
        'qr_order_create',
        {
          requestId,
          tenantId: tenantIdForLog,
          referenceId: persistedReferenceId,
          orderId: persistedOrderId,
          body: incomingBody,
          fileMeta,
          query: req.query || null,
        },
        error,
        {
          url: req.url,
          method: req.method,
          headers: req.headers
            ? Object.fromEntries(
                Object.entries(req.headers).filter(([k]) => !/authorization|cookie|secret|signature/i.test(k)),
              )
            : null,
        },
      );
    } catch (_) {}

    if (error?.code === '23505' && persistedReferenceId) {
      try {
        const client = await getClientFromPool(pool);
        try {
          const hasTenantColumn = !!(await (async () => {
            try {
              const cols = await getTableColumnSet(client, 'sales_records');
              return cols.has('tenant_id');
            } catch (_) {
              return false;
            }
          })());
          const existing = await findExistingSaleByReferenceId(
            client,
            {
              tenantId: tenantIdForLog,
              referenceId: persistedReferenceId,
              hasTenantColumn,
            },
          );
          if (existing) {
            return jsonOk(
              res,
              existing,
              'Pesanan QR sudah tersimpan (idempotent recovery)',
              200,
            );
          }
        } finally {
          client.release();
        }
      } catch (_) {}
    }

    if (error?.code === 'INSUFFICIENT_TRANSFER_AMOUNT') {
      const truncated = error.message || 'INSUFFICIENT_TRANSFER_AMOUNT';
      return res.status(400).json({
        success: false,
        error: truncated,
        code: 'INSUFFICIENT_TRANSFER_AMOUNT',
        message: truncated,
      });
    }

    if (error?.code === 'INVALID_PAYMENT_PROOF') {
      const truncated = error.message || 'INVALID_PAYMENT_PROOF';
      return res.status(400).json({
        success: false,
        error: truncated,
        message: truncated,
      });
    }

    console.error('[publicQrController] createQrOrder failed:', {
      requestId,
      tenantId: tenantIdForLog,
      referenceId: persistedReferenceId,
      orderId: persistedOrderId,
      message: error?.message || error,
      code: error?.code || null,
      statusCode: error?.statusCode || null,
      stack: error?.stack || null,
    });

    return jsonError(
      res,
      error?.statusCode || 500,
      error?.message || 'Internal server error',
      error?.message || error,
    );
  }
};

const checkoutQrOrder = async (req, res) => {
  const pool = getSharedPool();
  const requestId = `${Date.now()}-${crypto.randomBytes(4).toString('hex')}`;
  const incomingBody = (req.body && typeof req.body === 'object')
    ? { ...req.body }
    : {};
  let tenantIdForLog = normalizeTenantId(
    incomingBody.tenantId ||
    incomingBody.tenant_id ||
    req.user?.tenantId ||
    req.tenant?.tenantId ||
    '',
  );
  let orderLookup = {
    orderId: '',
    referenceId: '',
    receiptNumber: '',
  };

  try {
    return await withRetries(
      async () => {
        const client = await getClientFromPool(pool);
        try {
          const tenantId = parseTenantId(incomingBody.tenantId || incomingBody.tenant_id);
          tenantIdForLog = tenantIdForLog || tenantId;
          const branchId = parseOptionalBranchId(incomingBody.branchId || incomingBody.branch_id);
          const orderId = (
            incomingBody.orderId ||
            incomingBody.order_id ||
            incomingBody.salesRecordId ||
            ''
          ).toString().trim();
          const referenceId = (
            incomingBody.referenceId ||
            incomingBody.reference_id ||
            ''
          ).toString().trim();
          const receiptNumber = (
            incomingBody.receiptNumber ||
            incomingBody.receipt_number ||
            ''
          ).toString().trim();
          orderLookup = { orderId, referenceId, receiptNumber };
          const paymentMethod = normalizePaymentMethod(
            incomingBody.paymentMethod || incomingBody.payment_method,
          );
          const paymentState = resolvePaymentState(paymentMethod);

          if (!orderId && !referenceId && !receiptNumber) {
            const error = new Error('orderId/referenceId/receiptNumber wajib diisi');
            error.statusCode = 400;
            throw error;
          }

          const result = await runTransaction(
            client,
            async (txClient) => {
              const salesColumns = await getTableColumnSet(txClient, 'sales_records');
              const supportsSalesBranch = salesColumns.has('branch_id');
              const hasOrderIdParam = !!(orderId && orderId !== '');
              const hasRefIdParam = !!(referenceId && referenceId !== '');
              const hasReceiptParam = !!(receiptNumber && receiptNumber !== '');

              const orderMatchParts = [];
              const params = [tenantId];
              let branchParamIndex = 0;
              let orderIdParamIndex = 0;
              let refIdParamIndex = 0;
              let receiptParamIndex = 0;

              if (supportsSalesBranch) {
                branchParamIndex = params.length + 1;
                params.push(branchId);
              } else {
                params.push(null);
                branchParamIndex = params.length;
              }

              if (hasOrderIdParam) {
                orderIdParamIndex = params.length + 1;
                orderMatchParts.push(`($${orderIdParamIndex}::text <> '' AND id::text = $${orderIdParamIndex})`);
                params.push(orderId);
              }
              if (hasRefIdParam) {
                refIdParamIndex = params.length + 1;
                orderMatchParts.push(`($${refIdParamIndex}::text <> '' AND reference_id = $${refIdParamIndex})`);
                params.push(referenceId);
              }
              if (hasReceiptParam) {
                receiptParamIndex = params.length + 1;
                orderMatchParts.push(`($${receiptParamIndex}::text <> '' AND receipt_number = $${receiptParamIndex})`);
                params.push(receiptNumber);
              }

              if (orderMatchParts.length === 0) {
                orderMatchParts.push('false');
              }

              const branchClause = supportsSalesBranch
                ? `AND ($${branchParamIndex}::text IS NULL OR branch_id::text = $${branchParamIndex}::text)`
                : `AND ($${branchParamIndex}::text IS NULL OR true)`;

              const columnsToSelectBranch = supportsSalesBranch ? 'branch_id,' : 'NULL::text AS branch_id,';

              const lookupSql = `SELECT id, reference_id, receipt_number,
                        payment_status, order_status, payment_method,
                        total_amount, total_price,
                        ${columnsToSelectBranch}
                        table_id,
                        table_number,
                        order_type,
                        customer_name,
                        special_note
                 FROM sales_records
                 WHERE tenant_id = $1
                   ${branchClause}
                   AND (${orderMatchParts.join(' OR ')})
                 ORDER BY created_at DESC
                 LIMIT 1
                 FOR UPDATE`;

              const lookup = await txClient.query(lookupSql, params);

              const sale = lookup.rows?.[0];
              if (!sale) {
                const error = new Error('Pesanan tidak ditemukan');
                error.statusCode = 404;
                throw error;
              }

              const updatedResult = await txClient.query(
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

              return {
                sale,
                updated: updatedResult.rows?.[0] || sale,
                branchId,
                paymentMethod,
                tenantId,
              };
            },
            {
              label: `checkout_qr_order:${tenantId}:${orderId || referenceId || receiptNumber || 'noid'}`,
              maxAttempts: 4,
            },
          );

          const { updated } = result;
          const updatedForResponse = updated;
          const responsePayload = {
            ...updatedForResponse,
            payment_method_code: paymentMethod,
            order_note: (updatedForResponse.special_note || '').toString().trim() || null,
            specialNote: (updatedForResponse.special_note || '').toString().trim() || null,
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
            orderId: updatedForResponse.id,
            referenceId: updatedForResponse.reference_id,
            receiptNumber: updatedForResponse.receipt_number,
            branchId: updatedForResponse.branch_id ?? branchId ?? null,
            tableId: updatedForResponse.table_id ?? null,
            table_number: (updatedForResponse.table_number || '').toString().trim() || null,
            orderType: (updatedForResponse.order_type || '').toString().trim() || null,
            orderNote: (updatedForResponse.special_note || '').toString().trim() || null,
            special_note: (updatedForResponse.special_note || '').toString().trim() || null,
            paymentMethod,
            paymentStatus: updatedForResponse.payment_status,
            orderStatus: updatedForResponse.order_status,
            updatedAt: new Date().toISOString(),
          });

          return jsonOk(res, responsePayload, 'Checkout QR berhasil diproses', 200);
        } finally {
          client.release();
        }
      },
      {
        label: `checkout_qr_order:${requestId}`,
        maxAttempts: 2,
        baseDelayMs: 200,
        maxDelayMs: 1200,
        shouldRetry: (error, attempt) => {
          if (error?.statusCode && error.statusCode >= 400 && error.statusCode < 500) {
            return false;
          }
          if (attempt >= 2) return false;
          return isTransientDbError(error);
        },
      },
    );
  } catch (error) {
    try {
      await storeFailedPayload(
        'qr_order_checkout',
        {
          requestId,
          tenantId: tenantIdForLog,
          ...orderLookup,
          body: incomingBody,
          query: req.query || null,
        },
        error,
        {
          url: req.url,
          method: req.method,
          headers: req.headers
            ? Object.fromEntries(
                Object.entries(req.headers).filter(([k]) => !/authorization|cookie|secret|signature/i.test(k)),
              )
            : null,
        },
      );
    } catch (_) {}

    console.error('[publicQrController] checkoutQrOrder failed:', {
      requestId,
      tenantId: tenantIdForLog,
      ...orderLookup,
      message: error?.message || error,
      code: error?.code || null,
      statusCode: error?.statusCode || null,
      stack: error?.stack || null,
    });

    return jsonError(
      res,
      error?.statusCode || 500,
      error?.message || 'Internal server error',
      error?.message || error,
    );
  }
};

const handlePaymentWebhook = async (req, res) => {
  const pool = getSharedPool();
  const requestId = `${Date.now()}-${crypto.randomBytes(4).toString('hex')}`;
  const gatewayTrace = {
    requestId,
    receivedAt: new Date().toISOString(),
    ip: (
      (req.headers?.['x-forwarded-for'] || '').toString().split(',')[0] ||
      req.ip ||
      req.socket?.remoteAddress ||
      ''
    ).toString().trim() || null,
    userAgent: (req.headers?.['user-agent'] || '').toString().trim() || null,
  };
  const incomingBody = (req.body && typeof req.body === 'object')
    ? { ...req.body }
    : {};
  let tenantIdForLog = normalizeTenantId(
    incomingBody.tenantId ||
    incomingBody.tenant_id ||
    req.user?.tenantId ||
    req.tenant?.tenantId ||
    '',
  );
  let orderLookup = {
    orderId: '',
    referenceId: '',
    receiptNumber: '',
  };

  const sendResponse = (status, payload) => {
    try {
      if (!res.headersSent) {
        return res.status(status).json(payload);
      }
      return res;
    } catch (_) {
      return res;
    }
  };

  const ackSuccess = (data) => {
    const acknowledgeAt = new Date().toISOString();
    const payload = {
      success: true,
      message: 'Webhook pembayaran diterima',
      receivedAt: gatewayTrace.receivedAt,
      acknowledgeAt,
      requestId: gatewayTrace.requestId,
      data: data || null,
    };
    return sendResponse(200, payload);
  };

  try {
    return await withRetries(
      async () => {
        const client = await getClientFromPool(pool);
        try {
          const tenantId = parseTenantId(incomingBody.tenantId || incomingBody.tenant_id);
          tenantIdForLog = tenantIdForLog || tenantId;
          const orderId = (
            incomingBody.orderId ||
            incomingBody.order_id ||
            incomingBody.salesRecordId ||
            ''
          ).toString().trim();
          const referenceId = (
            incomingBody.referenceId ||
            incomingBody.reference_id ||
            ''
          ).toString().trim();
          const receiptNumber = (
            incomingBody.receiptNumber ||
            incomingBody.receipt_number ||
            ''
          ).toString().trim();
          orderLookup = { orderId, referenceId, receiptNumber };
          const gatewayStatus =
            incomingBody.paymentStatus ||
            incomingBody.payment_status ||
            incomingBody.transactionStatus ||
            incomingBody.transaction_status ||
            incomingBody.status;
          const paidAmount = Number(
            incomingBody.paidAmount ??
            incomingBody.paid_amount ??
            incomingBody.amount_paid ??
            0,
          ) || 0;
          const externalTrace = {
            externalOrderId: (incomingBody.external_order_id || incomingBody.externalOrderId || incomingBody.external_id || '').toString().trim() || null,
            externalPaymentId: (incomingBody.external_payment_id || incomingBody.externalPaymentId || incomingBody.transaction_id || incomingBody.transactionId || '').toString().trim() || null,
            gateway: (incomingBody.gateway || incomingBody.provider || incomingBody.channel || '').toString().trim() || null,
          };

          if (!orderId && !referenceId && !receiptNumber) {
            const error = new Error('orderId/referenceId/receiptNumber wajib diisi');
            error.statusCode = 400;
            throw error;
          }

          const result = await runTransaction(
            client,
            async (txClient) => {
              const lookup = await txClient.query(
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
                 LIMIT 1
                 FOR UPDATE`,
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

              const updatedResult = await txClient.query(
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

              return {
                sale,
                updated: updatedResult.rows?.[0] || sale,
                paymentState,
                tenantId,
              };
            },
            {
              label: `payment_webhook:${tenantId}:${orderId || referenceId || receiptNumber || 'noid'}`,
              maxAttempts: 5,
              baseDelayMs: 150,
              maxDelayMs: 2500,
            },
          );

          const { updated, paymentState } = result;
          const updatedForEmit = updated;

          emitToTenant(tenantId, 'qr_order_payment_status', {
            tenantId,
            orderId: updatedForEmit.id,
            referenceId: updatedForEmit.reference_id,
            receiptNumber: updatedForEmit.receipt_number,
            branchId: updatedForEmit.branch_id ?? null,
            tableId: updatedForEmit.table_id ?? null,
            table_number: (updatedForEmit.table_number || '').toString().trim() || null,
            orderType: (updatedForEmit.order_type || '').toString().trim() || null,
            orderNote: (updatedForEmit.special_note || '').toString().trim() || null,
            special_note: (updatedForEmit.special_note || '').toString().trim() || null,
            paymentMethod: (updatedForEmit.payment_method || '').toString().trim() || null,
            paymentStatus: updatedForEmit.payment_status,
            orderStatus: updatedForEmit.order_status,
            paymentProofUrl: (updatedForEmit.payment_proof_url || '').toString().trim() || null,
            payment_proof_url: (updatedForEmit.payment_proof_url || '').toString().trim() || null,
            amountPaid: updatedForEmit.amount_paid,
            grandTotal: Number(updatedForEmit.total_amount || 0),
            updatedAt: new Date().toISOString(),
            source: 'payment_webhook',
            requestId: gatewayTrace.requestId,
            ...gatewayTrace,
          });

          return ackSuccess(updatedForEmit);
        } finally {
          client.release();
        }
      },
      {
        label: `payment_webhook:${requestId}`,
        maxAttempts: 3,
        baseDelayMs: 200,
        maxDelayMs: 1500,
        shouldRetry: (error, attempt) => {
          if (error?.statusCode && error.statusCode >= 400 && error.statusCode < 500) {
            return false;
          }
          if (attempt >= 3) return false;
          return isTransientDbError(error);
        },
      },
    );
  } catch (error) {
    try {
      await storeFailedPayload(
        'payment_webhook',
        {
          ...gatewayTrace,
          tenantId: tenantIdForLog,
          ...orderLookup,
          body: incomingBody,
          query: req.query || null,
        },
        error,
        {
          url: req.url,
          method: req.method,
          headers: req.headers
            ? Object.fromEntries(
                Object.entries(req.headers).filter(([k]) => !/authorization|cookie|secret|signature/i.test(k)),
              )
            : null,
        },
      );
    } catch (_) {}

    const status = error?.statusCode && error.statusCode >= 400 && error.statusCode < 600
      ? error.statusCode
      : 500;

    console.error('[publicQrController] handlePaymentWebhook failed:', {
      ...gatewayTrace,
      tenantId: tenantIdForLog,
      ...orderLookup,
      message: error?.message || error,
      code: error?.code || null,
      statusCode: status,
      stack: error?.stack || null,
    });

    try {
      return sendResponse(status, {
        success: false,
        message: error?.message || 'Internal server error',
        error: error?.message || error,
        requestId: gatewayTrace.requestId,
        receivedAt: gatewayTrace.receivedAt,
      });
    } catch (_) {
      return sendResponse(status, {
        success: false,
        message: 'Internal server error',
        requestId: gatewayTrace.requestId,
      });
    }
  }
};

module.exports = {
  getQrMenu,
  createQrOrder,
  checkoutQrOrder,
  handlePaymentWebhook,
};
