const { jsonOk, jsonError } = require('../utils/http');

const ALLOWED_STATUS = new Set([
  'PENDING',
  'IN_PROGRESS',
  'WAITING_CONFIRMATION',
  'DONE',
  'CANCELLED',
]);

const normalizeTenantId = (req) => (
  req?.user?.tenantId
  ?? req?.user?.tenant_id
  ?? req?.tenant?.tenantId
  ?? req?.auth?.tenantId
  ?? req?.auth?.tenant_id
  ?? ''
)
  .toString()
  .trim();

const normalizeStatus = (value) => (value ?? '')
  .toString()
  .trim()
  .toUpperCase();

const parseJsonField = (value) => {
  if (value == null) return null;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return null;
    try {
      return JSON.parse(trimmed);
    } catch (_) {
      return value;
    }
  }
  return value;
};

const parseJsonFieldLenient = (value) => {
  if (typeof value !== 'string') {
    return value;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return value;
  }

  try {
    return JSON.parse(trimmed);
  } catch (_) {
    return value;
  }
};

const sanitizeServiceOrderPayload = (rawBody) => {
  if (!rawBody || typeof rawBody !== 'object' || Array.isArray(rawBody)) {
    return {};
  }

  const body = { ...rawBody };
  const jsonCandidateFields = [
    'serviceDetails',
    'service_details',
    'serviceDetailsJson',
    'service_details_json',
    'cost_details',
    'costDetails',
    'spare_parts',
    'spareParts',
    'rincian_biaya',
    'rincianBiaya',
    'items',
  ];

  for (const fieldName of jsonCandidateFields) {
    if (!Object.prototype.hasOwnProperty.call(body, fieldName)) {
      continue;
    }
    body[fieldName] = parseJsonFieldLenient(body[fieldName]);
  }

  const hasPrimaryServiceDetails =
    body.serviceDetails !== undefined ||
    body.service_details !== undefined ||
    body.serviceDetailsJson !== undefined ||
    body.service_details_json !== undefined;

  if (!hasPrimaryServiceDetails) {
    const fallbackServiceDetails =
      body.cost_details ??
      body.costDetails ??
      body.rincian_biaya ??
      body.rincianBiaya ??
      body.spare_parts ??
      body.spareParts ??
      body.items;

    if (fallbackServiceDetails !== undefined) {
      body.serviceDetails = fallbackServiceDetails;
    }
  }

  return body;
};

const normalizeServiceDetails = (rawValue) => {
  if (rawValue === undefined) {
    return { hasValue: false, value: null, error: null };
  }

  if (rawValue === null || rawValue === '') {
    return { hasValue: true, value: null, error: null };
  }

  let parsed = rawValue;
  if (typeof parsed === 'string') {
    try {
      parsed = JSON.parse(parsed);
    } catch (error) {
      return {
        hasValue: true,
        value: null,
        error: 'serviceDetails harus berupa JSON array valid',
      };
    }
  }

  if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
    parsed = parsed.items ?? parsed.details ?? parsed.serviceDetails ?? parsed.data;
  }

  if (!Array.isArray(parsed)) {
    return {
      hasValue: true,
      value: null,
      error: 'serviceDetails harus berupa array item biaya',
    };
  }

  const normalizedItems = [];
  for (const item of parsed) {
    if (!item || typeof item !== 'object') {
      continue;
    }

    const itemName = (item.itemName ?? item.item_name ?? '')
      .toString()
      .trim();
    const priceRaw = item.price ?? item.amount ?? item.cost;
    const price = Number(priceRaw ?? 0);

    if (!itemName) {
      return {
        hasValue: true,
        value: null,
        error: 'serviceDetails.itemName tidak boleh kosong',
      };
    }
    if (!Number.isFinite(price) || price < 0) {
      return {
        hasValue: true,
        value: null,
        error: 'serviceDetails.price harus berupa angka >= 0',
      };
    }

    normalizedItems.push({
      itemName,
      price,
    });
  }

  return {
    hasValue: true,
    value: normalizedItems,
    error: null,
  };
};

const sumServiceDetails = (serviceDetails) => {
  if (!Array.isArray(serviceDetails)) return 0;
  return serviceDetails.reduce((sum, item) => {
    const price = Number(item?.price ?? 0);
    return Number.isFinite(price) ? sum + price : sum;
  }, 0);
};

const assertColumnsExist = async (client, table, columns = []) => {
  const result = await client.query(
    `SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = ANY(current_schemas(false))
       AND table_name = $1`,
    [table],
  );
  const existingColumns = new Set((result.rows || []).map((row) => row.column_name));
  const missingColumns = columns.filter((column) => !existingColumns.has(column));
  if (missingColumns.length > 0) {
    throw new Error(
      `Schema guard: tabel ${table} belum memiliki kolom wajib: ${missingColumns.join(', ')}. Jalankan migrasi di core service.`,
    );
  }
};

const ensureServiceOrdersTable = async (client) => {
  await assertColumnsExist(client, 'service_orders', [
    'id',
    'tenant_id',
    'customer_name',
    'customer_phone',
    'device_type',
    'device_brand',
    'serial_number',
    'complaint',
    'status',
    'estimated_cost',
    'technician_notes',
    'service_details',
    'created_at',
    'updated_at',
  ]);
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
  technicianNotes: row.technician_notes,
  technician_notes: row.technician_notes,
  serviceDetails: parseJsonField(row.service_details),
  service_details: parseJsonField(row.service_details),
  createdAt: row.created_at,
  created_at: row.created_at,
  updatedAt: row.updated_at,
  updated_at: row.updated_at,
});

const safeStringField = (value) => {
  if (value === null || value === undefined) return null;
  const trimmed = value.toString().trim();
  return trimmed === '' ? null : trimmed;
};

const createServiceOrder = async (req, res) => {
  const client = await req.tenantDb.connect();

  try {
    const tenantId = normalizeTenantId(req);
    if (!tenantId) {
      return jsonError(res, 401, 'Tenant tidak valid');
    }

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
    const technicianNotes = safeStringField(
      req.body?.technicianNotes ?? req.body?.technician_notes,
    );
    const serviceDetailsRaw = req.body?.serviceDetails
      ?? req.body?.service_details
      ?? req.body?.serviceDetailsJson
      ?? req.body?.service_details_json;
    const serviceDetailsInput = normalizeServiceDetails(serviceDetailsRaw);
    if (serviceDetailsInput.error) {
      return jsonError(res, 400, serviceDetailsInput.error);
    }

    // Numeric optional field
    const estimatedCostRaw = req.body?.estimatedCost ?? req.body?.estimated_cost;
    let estimatedCost =
      estimatedCostRaw === undefined || estimatedCostRaw === null || estimatedCostRaw === ''
        ? null
        : Number(estimatedCostRaw);

    if (estimatedCost === null && serviceDetailsInput.hasValue && Array.isArray(serviceDetailsInput.value)) {
      estimatedCost = sumServiceDetails(serviceDetailsInput.value);
    }

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
         estimated_cost,
         technician_notes,
         service_details
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
         $9,
         $10,
         $11
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
        technicianNotes,
        serviceDetailsInput.hasValue ? serviceDetailsInput.value : null,
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
      technicianNotes: req.body?.technicianNotes,
      serviceDetails: req.body?.serviceDetails,
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

const updateServiceOrder = async (req, res) => {
  const client = await req.tenantDb.connect();

  try {
    req.body = sanitizeServiceOrderPayload(req.body);

    const tenantId = normalizeTenantId(req);
    const id = (req.params?.id ?? '').toString().trim();
    const nextStatusRaw = req.body?.status;
    const nextStatus = nextStatusRaw == null ? null : normalizeStatus(nextStatusRaw);
    let estimatedCostRaw = req.body?.estimatedCost ?? req.body?.estimated_cost;
    const technicianNotes = safeStringField(
      req.body?.technicianNotes ?? req.body?.technician_notes,
    );
    const serviceDetailsRaw = req.body?.serviceDetails
      ?? req.body?.service_details
      ?? req.body?.serviceDetailsJson
      ?? req.body?.service_details_json;
    const serviceDetailsInput = normalizeServiceDetails(serviceDetailsRaw);
    if (serviceDetailsInput.error) {
      return jsonError(res, 400, serviceDetailsInput.error);
    }

    if (!tenantId) {
      return jsonError(res, 401, 'Tenant tidak valid');
    }
    if (!id) {
      return jsonError(res, 400, 'id service order wajib diisi');
    }
    if (nextStatus !== null && !ALLOWED_STATUS.has(nextStatus)) {
      return jsonError(
        res,
        400,
        'Status tidak valid. Gunakan: PENDING, IN_PROGRESS, WAITING_CONFIRMATION, DONE, CANCELLED',
      );
    }

    let estimatedCost;
    if (estimatedCostRaw === undefined && serviceDetailsInput.hasValue) {
      estimatedCostRaw = serviceDetailsInput.value === null
        ? null
        : sumServiceDetails(serviceDetailsInput.value);
    }

    if (
      estimatedCostRaw === undefined ||
      estimatedCostRaw === null ||
      estimatedCostRaw === ''
    ) {
      estimatedCost = null;
    } else {
      estimatedCost = Number(estimatedCostRaw);
      if (!Number.isFinite(estimatedCost)) {
        return jsonError(res, 400, 'estimatedCost harus berupa angka valid');
      }
    }

    const fields = [];
    const params = [];

    if (nextStatus !== null) {
      params.push(nextStatus);
      fields.push(`status = $${params.length}`);
    }

    if (estimatedCostRaw !== undefined) {
      params.push(estimatedCost);
      fields.push(`estimated_cost = $${params.length}`);
    }

    if (
      Object.prototype.hasOwnProperty.call(req.body ?? {}, 'technicianNotes') ||
      Object.prototype.hasOwnProperty.call(req.body ?? {}, 'technician_notes')
    ) {
      params.push(technicianNotes);
      fields.push(`technician_notes = $${params.length}`);
    }

    if (serviceDetailsInput.hasValue) {
      params.push(serviceDetailsInput.value);
      fields.push(`service_details = $${params.length}`);
    }

    if (!fields.length) {
      return jsonError(
        res,
        400,
        'Minimal satu field harus diupdate: status, estimatedCost, technicianNotes, atau serviceDetails',
      );
    }

    await ensureServiceOrdersTable(client);

    const result = await client.query(
      `UPDATE service_orders
       SET ${fields.join(', ')},
           updated_at = NOW()
       WHERE id = $${fields.length + 1}
         AND tenant_id = $${fields.length + 2}
       RETURNING *`,
      [...params, id, tenantId],
    );

    if (!result.rows.length) {
      return jsonError(res, 404, 'Service order tidak ditemukan');
    }

    return jsonOk(
      res,
      mapRow(result.rows[0]),
      'Service order berhasil diperbarui',
    );
  } catch (error) {
    console.error('[serviceOrderController.updateServiceOrder] Error:', error);
    console.error('Request params:', {
      id: req.params?.id,
      status: req.body?.status,
      estimatedCost: req.body?.estimatedCost,
      technicianNotes: req.body?.technicianNotes,
      serviceDetails: req.body?.serviceDetails,
    });
    return jsonError(
      res,
      500,
      error.message || 'Gagal memperbarui service order',
    );
  } finally {
    client.release();
  }
};

module.exports = {
  createServiceOrder,
  getServiceOrders,
  updateServiceOrder,
};
