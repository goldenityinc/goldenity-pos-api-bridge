const { jsonOk, jsonError } = require('../utils/http');
const {
  buildInsertQuery,
  buildUpdateQuery,
  buildDeleteQuery,
  getTableColumnDefinitions,
  getTableColumnSet,
  normalizePayloadByColumnDefinitions,
  enforceTenantIdOnPayload,
  normalizeTenantId,
  runSelect,
} = require('../utils/sqlHelpers');
const { emitTableMutation } = require('../services/realtimeEmitter');
const { scheduleSalesJournalPosting } = require('../services/accountingAutomationService');

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

const sanitizeClientGeneratedPrimaryKey = (payload, columnDefinitions, options = {}) => {
  const { table } = options;
  if (!(columnDefinitions instanceof Map) || !isIntegerColumnDefinition(columnDefinitions.get('id'))) {
    return payload;
  }

  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return payload;
  }

  if (!Object.prototype.hasOwnProperty.call(payload, 'id')) {
    return payload;
  }

  const rawId = payload.id;
  if (rawId === undefined || rawId === null) {
    const next = { ...payload };
    delete next.id;
    return next;
  }

  const idText = rawId.toString().trim();
  if (!idText) {
    const next = { ...payload };
    delete next.id;
    return next;
  }

  if (/^\d+$/.test(idText)) {
    return payload;
  }

  const next = { ...payload };
  const referenceId = (
    next.reference_id ?? next.referenceId ?? next.local_id ?? next.localId ?? ''
  )
    .toString()
    .trim();

  if (!referenceId && table === 'products') {
    next.reference_id = idText;
  }

  delete next.id;
  return next;
};

const resolveProductMutationTarget = async ({ tenantDb, table, idValue, payload, tenantId }) => {
  if (table !== 'products') {
    return { idField: 'id', idValue };
  }

  const normalizedId = (idValue ?? '').toString().trim();
  if (/^\d+$/.test(normalizedId)) {
    return { idField: 'id', idValue: normalizedId };
  }

  const referenceId = (
    payload?.reference_id ??
    payload?.referenceId ??
    payload?.local_id ??
    payload?.localId ??
    normalizedId
  )
    .toString()
    .trim();

  if (!referenceId) {
    return { idField: 'id', idValue: normalizedId };
  }

  const existing = await runSelect(tenantDb, table, {
    eq__reference_id: referenceId,
    maybeSingle: true,
  }, { tenantId });

  const existingId = (existing?.id ?? '').toString().trim();
  if (/^\d+$/.test(existingId)) {
    return { idField: 'id', idValue: existingId };
  }

  return { idField: 'reference_id', idValue: referenceId };
};

const normalizeProductPayload = (payload = {}, { isCreate = false } = {}) => {
  const next = { ...payload };

  if (next.imageUrl !== undefined && next.image_url === undefined) {
    next.image_url = next.imageUrl;
  }

  delete next.imageUrl;
  return next;
};

const normalizeCustomerPayload = (payload = {}, { isCreate = false } = {}) => {
  const next = { ...payload };

  delete next.totalSpent;
  delete next.total_spent;

  if (isCreate) {
    next.total_spent = 0;
  }

  return next;
};

const normalizePayloadForTable = (table, payload, options = {}) => {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return payload;
  }

  if (table === 'products') {
    return normalizeProductPayload(payload, options);
  }

  if (table === 'customers') {
    return normalizeCustomerPayload(payload, options);
  }

  return payload;
};

const assertTableExists = async (tenantDb, table) => {
  const result = await tenantDb.query(
    `SELECT 1
     FROM information_schema.tables
     WHERE table_schema = ANY(current_schemas(false))
       AND table_name = $1
     LIMIT 1`,
    [table],
  );

  if ((result.rowCount || 0) === 0) {
    throw new Error(`Schema guard: tabel ${table} tidak ditemukan. Jalankan migrasi di core service.`);
  }
};

const assertColumnsExist = async (tenantDb, table, columns = []) => {
  if (!Array.isArray(columns) || columns.length === 0) {
    return;
  }

  const result = await tenantDb.query(
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

const ensureCustomersTable = async (tenantDb, table) => {
  if (table !== 'customers') return;
  await assertTableExists(tenantDb, 'customers');
  await assertColumnsExist(tenantDb, 'customers', ['id', 'name', 'total_spent', 'tenant_id']);
};

const ensureProductsTableColumns = async (tenantDb, table) => {
  if (table !== 'products') return;
  await assertTableExists(tenantDb, 'products');
  await assertColumnsExist(tenantDb, 'products', ['id', 'name', 'tenant_id']);
};

const findExistingRecordByReferenceId = async (tenantDb, table, payload, tenantId) => {
  if (table !== 'products' || !payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return null;
  }

  const referenceId = (
    payload.reference_id ?? payload.referenceId ?? payload.local_id ?? payload.localId ?? ''
  )
    .toString()
    .trim();
  if (!referenceId) {
    return null;
  }

  return runSelect(tenantDb, table, {
    eq__reference_id: referenceId,
    maybeSingle: true,
  }, { tenantId });
};

const prepareTableSchema = async (tenantDb, table) => {
  await ensureCustomersTable(tenantDb, table);
  await ensureProductsTableColumns(tenantDb, table);
};

const hasMutationFields = (payload) => {
  return !!payload && typeof payload === 'object' && !Array.isArray(payload) && Object.keys(payload).length > 0;
};

const resolveTenantId = (req) => normalizeTenantId(
  req?.user?.tenantId ||
  req?.user?.tenant_id ||
  req?.tenant?.tenantId ||
  req?.auth?.tenantId ||
  req?.auth?.tenant_id,
);

const toNumber = (value) => {
  const numberValue = Number(value);
  return Number.isFinite(numberValue) ? numberValue : NaN;
};

const toPositiveInteger = (value) => {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return null;
  }
  return parsed;
};

const parseItemsFromUnknown = (value) => {
  if (!value) {
    return [];
  }

  if (Array.isArray(value)) {
    return value;
  }

  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) {
      return [];
    }
    try {
      const parsed = JSON.parse(trimmed);
      return Array.isArray(parsed) ? parsed : [];
    } catch (_) {
      return [];
    }
  }

  return [];
};

const normalizeSalesRecordItems = (items) => {
  return parseItemsFromUnknown(items)
    .map((item) => {
      if (!item || typeof item !== 'object') {
        return null;
      }

      const product = item.product && typeof item.product === 'object'
        ? item.product
        : {};

      const productId = (
        item.product_id ?? item.productId ?? product.id ?? ''
      ).toString().trim();
      const productName = (
        item.product_name ?? item.productName ?? item.name ?? product.name ?? ''
      ).toString().trim();
      const qty = toPositiveInteger(item.qty ?? item.quantity);
      if (qty === null) {
        return null;
      }

      const isService =
        item.is_service === true ||
        item.isService === true ||
        item.is_custom_item === true ||
        item.isCustomItem === true ||
        product.is_service === true ||
        product.isService === true ||
        product.is_custom_item === true ||
        product.isCustomItem === true;

      if (!productId && !isService) {
        return null;
      }

      const customPrice = toNumber(
        item.custom_price ?? item.customPrice ?? item.price ?? item.unit_price ?? 0,
      );

      return {
        product_id: productId || null,
        product_name: productName || null,
        qty,
        custom_price: Number.isFinite(customPrice) && customPrice > 0 ? customPrice : null,
        note: (item.note ?? item.item_note ?? item.description ?? '').toString().trim() || null,
        is_service: isService,
      };
    })
    .filter(Boolean);
};

const ensureSalesRecordsItemsColumn = async (tenantDb, table) => {
  if (table !== 'sales_records') return;
  await assertTableExists(tenantDb, 'sales_records');
  await assertColumnsExist(tenantDb, 'sales_records', ['id', 'tenant_id', 'items_json']);
};

const ensureSalesRecordItemsTable = async (tenantDb, table) => {
  if (table !== 'sales_records') return;
  await assertTableExists(tenantDb, 'sales_record_items');
  await assertColumnsExist(tenantDb, 'sales_record_items', [
    'sales_record_id',
    'tenant_id',
    'product_id',
    'qty',
  ]);
};

const syncSalesRecordItems = async (tenantDb, tenantId, salesRecordId, items) => {
  if (!Number.isFinite(Number(salesRecordId))) {
    return;
  }

  const normalizedTenantId = normalizeTenantId(tenantId);

  await tenantDb.query(
    normalizedTenantId
      ? 'DELETE FROM sales_record_items WHERE sales_record_id = $1 AND tenant_id = $2'
      : 'DELETE FROM sales_record_items WHERE sales_record_id = $1',
    normalizedTenantId ? [salesRecordId, normalizedTenantId] : [salesRecordId],
  );

  if (!Array.isArray(items) || items.length === 0) {
    return;
  }

  for (const item of items) {
    await tenantDb.query(
      `INSERT INTO sales_record_items (
         tenant_id,
         sales_record_id,
         product_id,
         product_name,
         qty,
         custom_price,
         note,
         is_service,
         updated_at
       ) VALUES ($1::text, $2::bigint, $3::text, $4::text, $5::integer, $6::numeric, $7::text, $8::boolean, NOW())`,
      [
        normalizedTenantId || '',
        Number.isFinite(Number(salesRecordId)) ? Number(salesRecordId) : 0,
        (item.product_id || '').toString().trim() || null,
        (item.product_name || '').toString().trim() || null,
        Number.isInteger(Number(item.qty)) ? Number(item.qty) : 1,
        Number.isFinite(Number(item.custom_price)) ? Number(item.custom_price) : null,
        (item.note || '').toString().trim() || null,
        item.is_service === true,
      ],
    );
  }
};

const runSync = async (req, res) => {
  try {
    const { table, action, data, id } = req.body || {};
    const tenantId = resolveTenantId(req);

    if (!table || !action) {
      return jsonError(res, 400, 'table dan action wajib diisi');
    }

    await prepareTableSchema(req.tenantDb, table);
    await ensureSalesRecordsItemsColumn(req.tenantDb, table);
    await ensureSalesRecordItemsTable(req.tenantDb, table);

    if (action === 'INSERT') {
      const payload = normalizePayloadForTable(table, { ...(data || {}) }, { isCreate: true });
      const normalizedItems = table === 'sales_records'
        ? normalizeSalesRecordItems(payload.items ?? payload.items_json)
        : [];
      if (table === 'sales_records' && normalizedItems.length > 0) {
        payload.items_json = normalizedItems;
      }
      delete payload.items;

      const columnDefinitions = await getTableColumnDefinitions(req.tenantDb, table);
      const sanitizedPayload = sanitizeClientGeneratedPrimaryKey(payload, columnDefinitions, {
        table,
        isCreate: true,
      });
      // Note: product sync inserts must emit a numeric bigint-compatible id; client UUIDs stay in reference_id
      const tenantScopedPayload = enforceTenantIdOnPayload(sanitizedPayload, tenantId, columnDefinitions);
      const filteredPayload = normalizePayloadByColumnDefinitions(tenantScopedPayload, columnDefinitions);
      if (!hasMutationFields(filteredPayload)) {
        return jsonError(res, 400, `Tidak ada kolom yang cocok untuk tabel ${table}`);
      }

      const existingRecord = await findExistingRecordByReferenceId(
        req.tenantDb,
        table,
        filteredPayload,
        tenantId,
      );
      if (existingRecord) {
        return jsonOk(res, [existingRecord], 'Sync insert already exists');
      }

      const { sql, values } = buildInsertQuery(table, filteredPayload);
      const result = await req.tenantDb.query(sql, values);
      if (table === 'sales_records' && (result.rows?.[0]?.id !== undefined)) {
        await syncSalesRecordItems(
          req.tenantDb,
          tenantId,
          result.rows[0].id,
          normalizedItems,
        );
        result.rows[0].items = normalizedItems;
      }
      for (const row of result.rows) {
        emitTableMutation(req, {
          table,
          action: 'INSERT',
          record: row,
        });
      }
      if (table === 'sales_records') {
        for (const row of result.rows || []) {
          scheduleSalesJournalPosting({ salesRecord: row, tenantId });
        }
      }
      return jsonOk(res, result.rows, 'Sync insert success', 201);
    }

    if (action === 'UPDATE') {
      const payload = normalizePayloadForTable(table, data || {}, { isCreate: false });
      const normalizedItems = table === 'sales_records'
        ? normalizeSalesRecordItems(payload.items ?? payload.items_json)
        : [];
      if (table === 'sales_records' && (payload.items !== undefined || payload.items_json !== undefined)) {
        payload.items_json = normalizedItems;
      }
      delete payload.items;
      const columnDefinitions = await getTableColumnDefinitions(req.tenantDb, table);
      const sanitizedPayload = sanitizeClientGeneratedPrimaryKey(payload, columnDefinitions);
      // Note: sanitizeClientGeneratedPrimaryKey removes non-integer IDs to prevent ON CONFLICT errors
      // Products without valid integer IDs will use reference_id instead for lookups
      const tenantScopedPayload = enforceTenantIdOnPayload(sanitizedPayload, tenantId, columnDefinitions);
      const filteredPayload = normalizePayloadByColumnDefinitions(tenantScopedPayload, columnDefinitions);
      const resolvedTarget = await resolveProductMutationTarget({
        tenantDb: req.tenantDb,
        table,
        idValue: id,
        payload: filteredPayload,
        tenantId,
      });
      if (!hasMutationFields(filteredPayload)) {
        const existing = await runSelect(req.tenantDb, table, {
          [`eq__${resolvedTarget.idField}`]: resolvedTarget.idValue,
          maybeSingle: true,
        }, { tenantId });
        return jsonOk(res, existing ? [existing] : [], 'Sync update skipped');
      }
      const columnSet = await getTableColumnSet(req.tenantDb, table);
      const { sql, values } = buildUpdateQuery(
        table,
        filteredPayload,
        resolvedTarget.idField,
        resolvedTarget.idValue,
        { tenantId, hasTenantColumn: columnSet.has('tenant_id') },
      );
      const result = await req.tenantDb.query(sql, values);
      if (table === 'sales_records' && (payload.items_json !== undefined)) {
        await syncSalesRecordItems(req.tenantDb, tenantId, id, normalizedItems);
        if (result.rows?.[0]) {
          result.rows[0].items = normalizedItems;
        }
      }
      emitTableMutation(req, {
        table,
        action: 'UPDATE',
        record: result.rows[0] || null,
        id,
      });
      if (table === 'sales_records' && result.rows?.[0]) {
        scheduleSalesJournalPosting({ salesRecord: result.rows[0], tenantId });
      }
      return jsonOk(res, result.rows, 'Sync update success');
    }

    if (action === 'DELETE') {
      const resolvedTarget = await resolveProductMutationTarget({
        tenantDb: req.tenantDb,
        table,
        idValue: id,
        payload: data || {},
        tenantId,
      });
      const columnSet = await getTableColumnSet(req.tenantDb, table);
      const { sql, values } = buildDeleteQuery(
        table,
        resolvedTarget.idField,
        resolvedTarget.idValue,
        { tenantId, hasTenantColumn: columnSet.has('tenant_id') },
      );
      const result = await req.tenantDb.query(sql, values);
      emitTableMutation(req, {
        table,
        action: 'DELETE',
        record: result.rows[0] || null,
        id,
      });
      return jsonOk(res, result.rows, 'Sync delete success');
    }

    return jsonError(res, 400, `Action tidak didukung: ${action}`);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

module.exports = {
  runSync,
};
