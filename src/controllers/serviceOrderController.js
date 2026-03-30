const { jsonOk, jsonError } = require('../utils/http');

const ALLOWED_STATUS = new Set([
  'PENDING',
  'IN_PROGRESS',
  'DONE',
  'DELIVERED',
  'CANCELLED',
]);

const normalizeTenantId = (req) => (
  req?.tenant?.tenantId ?? req?.auth?.tenantId ?? req?.auth?.tenant_id ?? ''
)
  .toString()
  .trim();

const normalizeStatus = (value) => (value ?? '')
  .toString()
  .trim()
  .toUpperCase();

const ensureServiceOrdersTable = async (client) => {
  // Create table if not exists with proper defaults
  await client.query(`
    CREATE TABLE IF NOT EXISTS service_orders (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      customer_name TEXT NOT NULL,
      customer_phone TEXT,
      device_type TEXT NOT NULL,
      device_brand TEXT,
      serial_number TEXT,
      complaint TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'PENDING',
      estimated_cost NUMERIC(14,2),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  // Fix existing table schema if needed - ensure timestamp columns have defaults
  try {
    // Attempt to add/fix default for created_at (ignore if already exists)
    await client.query(`
      ALTER TABLE service_orders 
      ALTER COLUMN created_at SET DEFAULT NOW()
    `);
  } catch (err) {
    // Expected if column already has default
    if (!err.message.includes('already has a default definition')) {
      console.warn('[ensureServiceOrdersTable] created_at ALTER warning:', err.message);
    }
  }

  try {
    // Attempt to add/fix default for updated_at (ignore if already exists)
    await client.query(`
      ALTER TABLE service_orders 
      ALTER COLUMN updated_at SET DEFAULT NOW()
    `);
  } catch (err) {
    // Expected if column already has default
    if (!err.message.includes('already has a default definition')) {
      console.warn('[ensureServiceOrdersTable] updated_at ALTER warning:', err.message);
    }
  }

  // Create indexes if they don't exist
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_service_orders_tenant_id
    ON service_orders (tenant_id);
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_service_orders_tenant_status
    ON service_orders (tenant_id, status);
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_service_orders_created_at
    ON service_orders (created_at DESC);
  `);
};

const mapRow = (row = {}) => ({
  id: row.id,
  tenantId: row.tenant_id,
  tenant_id: row.tenant_id,
  customerName: row.customer_name,
  customer_name: row.customer_name,
  customerPhone: row.customer_phone,
  customer_phone: row.customer_phone,
  deviceType: row.device_type,
  device_type: row.device_type,
  deviceBrand: row.device_brand,
  device_brand: row.device_brand,
  serialNumber: row.serial_number,
  serial_number: row.serial_number,
  complaint: row.complaint,
  status: row.status,
  estimatedCost: row.estimated_cost === null ? null : Number(row.estimated_cost),
  estimated_cost: row.estimated_cost === null ? null : Number(row.estimated_cost),
  createdAt: row.created_at,
  created_at: row.created_at,
  updatedAt: row.updated_at,
  updated_at: row.updated_at,
});

const createServiceOrder = async (req, res) => {
  const client = await req.tenantDb.connect();

  try {
    const tenantId = normalizeTenantId(req);
    if (!tenantId) {
      return jsonError(res, 401, 'Tenant tidak valid');
    }

    // Helper function to safely extract optional string fields (convert empty string to null)
    const safeStringField = (value) => {
      if (value === null || value === undefined) return null;
      const trimmed = value.toString().trim();
      return trimmed === '' ? null : trimmed;
    };

    // Required fields - always convert to string and trim
    const customerName = (req.body?.customerName ?? req.body?.customer_name ?? '')
      .toString()
      .trim();
    const deviceType = (req.body?.deviceType ?? req.body?.device_type ?? '')
      .toString()
      .trim();
    const complaint = (req.body?.complaint ?? '').toString().trim();

    // Optional fields - convert empty string/null to null
    const customerPhone = safeStringField(
      req.body?.customerPhone ?? req.body?.customer_phone,
    );
    const deviceBrand = safeStringField(
      req.body?.deviceBrand ?? req.body?.device_brand,
    );
    const serialNumber = safeStringField(
      req.body?.serialNumber ?? req.body?.serial_number,
    );

    // Numeric optional field
    const estimatedCostRaw = req.body?.estimatedCost ?? req.body?.estimated_cost;
    const estimatedCost =
      estimatedCostRaw === undefined || estimatedCostRaw === null || estimatedCostRaw === ''
        ? null
        : Number(estimatedCostRaw);

    if (!customerName) {
      return jsonError(res, 400, 'customerName wajib diisi');
    }
    if (!deviceType) {
      return jsonError(res, 400, 'deviceType wajib diisi');
    }
    if (!complaint) {
      return jsonError(res, 400, 'complaint wajib diisi');
    }
    if (estimatedCost !== null && !Number.isFinite(estimatedCost)) {
      return jsonError(res, 400, 'estimatedCost harus berupa angka valid');
    }

    await ensureServiceOrdersTable(client);

    const insertResult = await client.query(
      `INSERT INTO service_orders (
         id,
         tenant_id,
         customer_name,
         customer_phone,
         device_type,
         device_brand,
         serial_number,
         complaint,
         status,
         estimated_cost
       ) VALUES (
         COALESCE($1, gen_random_uuid()::text),
         $2,
         $3,
         $4,
         $5,
         $6,
         $7,
         $8,
         'PENDING',
         $9
       )
       RETURNING *`,
      [
        (req.body?.id ?? '').toString().trim() || null,
        tenantId,
        customerName,
        customerPhone,
        deviceType,
        deviceBrand,
        serialNumber,
        complaint,
        estimatedCost,
      ],
    );

    return jsonOk(
      res,
      mapRow(insertResult.rows[0] || {}),
      'Service order berhasil dibuat',
      201,
    );
  } catch (error) {
    console.error('[serviceOrderController.createServiceOrder] Error:', error);
    console.error('Request body:', {
      customerName: req.body?.customerName,
      customerPhone: req.body?.customerPhone,
      deviceType: req.body?.deviceType,
      deviceBrand: req.body?.deviceBrand,
      serialNumber: req.body?.serialNumber,
      complaint: req.body?.complaint,
      estimatedCost: req.body?.estimatedCost,
    });
    return jsonError(
      res,
      500,
      error.message || 'Gagal membuat service order',
    );
  } finally {
    client.release();
  }
};

const getServiceOrders = async (req, res) => {
  const client = await req.tenantDb.connect();

  try {
    const tenantId = normalizeTenantId(req);
    if (!tenantId) {
      return jsonError(res, 401, 'Tenant tidak valid');
    }

    await ensureServiceOrdersTable(client);

    const statusFilter = normalizeStatus(req.query?.status);
    const params = [tenantId];
    let whereClause = 'WHERE tenant_id = $1';

    if (statusFilter) {
      if (!ALLOWED_STATUS.has(statusFilter)) {
        return jsonError(res, 400, 'Status filter tidak valid');
      }
      params.push(statusFilter);
      whereClause += ` AND status = $${params.length}`;
    }

    const result = await client.query(
      `SELECT *
       FROM service_orders
       ${whereClause}
       ORDER BY created_at DESC`,
      params,
    );

    return jsonOk(
      res,
      result.rows.map(mapRow),
      'Data service order berhasil dimuat',
    );
  } catch (error) {
    console.error('[serviceOrderController.getServiceOrders] Error:', error);
    console.error('Query params:', { status: req.query?.status });
    return jsonError(
      res,
      500,
      error.message || 'Gagal memuat service order',
    );
  } finally {
    client.release();
  }
};

const updateServiceStatus = async (req, res) => {
  const client = await req.tenantDb.connect();

  try {
    const tenantId = normalizeTenantId(req);
    const id = (req.params?.id ?? '').toString().trim();
    const nextStatus = normalizeStatus(req.body?.status);

    if (!tenantId) {
      return jsonError(res, 401, 'Tenant tidak valid');
    }
    if (!id) {
      return jsonError(res, 400, 'id service order wajib diisi');
    }
    if (!ALLOWED_STATUS.has(nextStatus)) {
      return jsonError(
        res,
        400,
        'Status tidak valid. Gunakan: PENDING, IN_PROGRESS, DONE, DELIVERED, CANCELLED',
      );
    }

    await ensureServiceOrdersTable(client);

    const result = await client.query(
      `UPDATE service_orders
       SET status = $1,
           updated_at = NOW()
       WHERE id = $2
         AND tenant_id = $3
       RETURNING *`,
      [nextStatus, id, tenantId],
    );

    if (!result.rows.length) {
      return jsonError(res, 404, 'Service order tidak ditemukan');
    }

    return jsonOk(
      res,
      mapRow(result.rows[0]),
      'Status service order berhasil diperbarui',
    );
  } catch (error) {
    console.error('[serviceOrderController.updateServiceStatus] Error:', error);
    console.error('Request params:', {
      id: req.params?.id,
      newStatus: req.body?.status,
    });
    return jsonError(
      res,
      500,
      error.message || 'Gagal memperbarui status service order',
    );
  } finally {
    client.release();
  }
};

module.exports = {
  createServiceOrder,
  getServiceOrders,
  updateServiceStatus,
};
