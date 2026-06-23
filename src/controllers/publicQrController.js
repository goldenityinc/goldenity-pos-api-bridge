const { jsonOk, jsonError } = require('../utils/http');
const { getSharedPool } = require('../middlewares/tenantResolver');
const { normalizeTenantId, getTableColumnSet } = require('../utils/sqlHelpers');
const { emitToTenant } = require('../services/socketServer');

const FNB_PRODUCT_TYPES = new Set(['FOOD', 'BEVERAGE', 'FNB', 'F&B', 'MENU']);
const PAYMENT_METHOD_CASHIER = 'CASHIER';
const PAYMENT_METHOD_DIGITAL = 'DIGITAL_PAYMENT';

const normalizePaymentMethod = (value) => {
  const normalized = (value || '').toString().trim().toUpperCase();
  if (
    normalized === 'DIGITAL' ||
    normalized === 'DIGITAL_PAYMENT' ||
    normalized === 'QRIS' ||
    normalized === 'E_WALLET' ||
    normalized === 'EWALLET'
  ) {
    return PAYMENT_METHOD_DIGITAL;
  }
  return PAYMENT_METHOD_CASHIER;
};

const resolvePaymentState = (paymentMethod) => {
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
    return { paymentStatus: 'PAID', orderStatus: 'PAID' };
  }
  if (['FAILED', 'EXPIRED', 'CANCELLED', 'CANCELED', 'DENIED'].includes(normalized)) {
    return { paymentStatus: 'FAILED', orderStatus: 'PAYMENT_FAILED' };
  }
  return { paymentStatus: 'PENDING_PAYMENT', orderStatus: 'PENDING_PAYMENT' };
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

    const result = await pool.query(
            `SELECT id, name, category, product_type, price, stock, image_url, unit,
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
         AND (
           COALESCE(is_service, false) = true
           OR COALESCE(stock, 0) > 0
         )
       ORDER BY name ASC`,
      [tenantId, Array.from(FNB_PRODUCT_TYPES), branchId],
    );

    let rows = result.rows || [];
    if (rows.length === 0) {
      const fallbackResult = await pool.query(
        `SELECT id, name, category, product_type, price, stock, image_url, unit,
          COALESCE(is_service, false) AS is_service,
          COALESCE(is_available, true) AS is_available
         FROM products
         WHERE tenant_id = $1
           AND ${softDeletePredicate}
           AND ($2::bigint IS NULL OR branch_id = $2)
           AND COALESCE(is_active, true) = true
           AND (
             COALESCE(is_service, false) = true
             OR COALESCE(stock, 0) > 0
           )
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
        is_available: row.is_available !== false,
        isAvailable: row.is_available !== false && (Number(row.stock || 0) > 0 || row.is_service === true),
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
    const items = parseOrderItems(req.body.items ?? req.body.orderItems);
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
    const paymentState = resolvePaymentState(paymentMethod);

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
    const productsResult = await client.query(
            `SELECT id, name, price,
              COALESCE(is_service, false) AS is_service,
              COALESCE(is_available, true) AS is_available,
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
      const isAvailable = product.is_available !== false;
      if (!isAvailable) {
        const error = new Error(`Produk sedang habis/tidak tersedia: ${product.name}`);
        error.statusCode = 400;
        throw error;
      }
      const availableStock = Number(product.stock || 0);
      if (!isService && availableStock < item.qty) {
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
      };
    });

    const referenceId = `qr_${Date.now()}`;
    const receiptNumber = generateReceiptNumber();

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
         created_at,
         updated_at
       )
       VALUES (
         $1, $2, $3, $4, $5, $6, $7, $8, $9,
         $10, $11, $12, $13, $14::jsonb, $15,
         NOW(), NOW()
       )
       RETURNING id, reference_id, receipt_number, cashier_name, order_status, payment_status, payment_method, total_amount`,
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
        JSON.stringify(normalizedItems),
        0,
      ],
    );

    const sale = saleInsert.rows?.[0];
    if (!sale) {
      const error = new Error('Gagal membuat pesanan QR');
      error.statusCode = 500;
      throw error;
    }

    if (orderNote) {
      try {
        await client.query(
          `UPDATE sales_records
           SET special_note = $1,
               updated_at = NOW()
           WHERE tenant_id = $2
             AND id = $3`,
          [orderNote, tenantId, sale.id],
        );
      } catch (_) {
        // Backward compatible when sales_records.special_note is not available yet.
      }
    }

    for (const item of normalizedItems) {
      await client.query(
        `INSERT INTO sales_record_items (
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
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW())`,
        [
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

      if (!item.isService) {
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

    const totalItems = normalizedItems.reduce((sum, item) => sum + (Number(item.qty || 0) || 0), 0);
    const tableLabel = (tableResult.rows?.[0]?.table_number || '').toString().trim();

    emitToTenant(tenantId, 'incoming_qr_order', {
      tenantId,
      orderId: sale.id,
      referenceId: sale.reference_id,
      receiptNumber: sale.receipt_number,
      tableId,
      tableName: tableLabel || String(tableId),
      orderType: 'DINE_IN',
      orderStatus: sale.order_status,
      paymentStatus: sale.payment_status,
      paymentMethod: sale.payment_method,
      customerName,
      orderNote,
      special_note: orderNote || null,
      specialNote: orderNote || null,
      totalItems,
      grandTotal: Number(sale.total_amount || 0),
      items: normalizedItems.map((item) => ({
        product_id: item.productId,
        product_name: item.productName,
        qty: Number(item.qty || 0) || 0,
        custom_price: Number(item.customPrice || 0) || 0,
        note: item.note || '',
        item_note: item.note || '',
        notes: item.note || '',
        is_service: item.isService === true,
      })),
      items_json: normalizedItems.map((item) => ({
        product_id: item.productId,
        product_name: item.productName,
        qty: Number(item.qty || 0) || 0,
        custom_price: Number(item.customPrice || 0) || 0,
        note: item.note || '',
        item_note: item.note || '',
        notes: item.note || '',
        is_service: item.isService === true,
      })),
      created_at: new Date().toISOString(),
    });

    emitToTenant(tenantId, 'qr_order_payment_status', {
      tenantId,
      orderId: sale.id,
      referenceId: sale.reference_id,
      receiptNumber: sale.receipt_number,
      paymentMethod,
      paymentStatus: sale.payment_status,
      orderStatus: sale.order_status,
      updatedAt: new Date().toISOString(),
    });

    return jsonOk(res, sale, 'Pesanan QR berhasil dibuat', 201);
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
      `SELECT id, reference_id, receipt_number, payment_status, order_status, total_amount
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
       RETURNING id, reference_id, receipt_number, payment_method, payment_status, order_status, total_amount`,
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
      `SELECT id, reference_id, receipt_number, total_amount
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
       RETURNING id, reference_id, receipt_number, payment_status, order_status, amount_paid, total_amount`,
      [paymentState.paymentStatus, paymentState.orderStatus, safePaidAmount, tenantId, sale.id],
    );

    await client.query('COMMIT');

    const updated = updatedResult.rows?.[0] || sale;

    emitToTenant(tenantId, 'qr_order_payment_status', {
      tenantId,
      orderId: updated.id,
      referenceId: updated.reference_id,
      receiptNumber: updated.receipt_number,
      paymentStatus: updated.payment_status,
      orderStatus: updated.order_status,
      amountPaid: updated.amount_paid,
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
