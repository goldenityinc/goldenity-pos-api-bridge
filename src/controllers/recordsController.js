const { jsonOk, jsonError } = require('../utils/http');
const {
  normalizeArray,
  parseBodyArray,
  parseBodyObject,
  buildInsertQuery,
  buildUpdateQuery,
  buildDeleteQuery,
  buildUpsertQuery,
  getTableColumnDefinitions,
  getTableColumnSet,
  normalizePayloadByColumnDefinitions,
  enforceTenantIdOnPayload,
  normalizeTenantId,
  runSelect,
  isTenantScopedTable,
} = require('../utils/sqlHelpers');
const { ensureTenantScopedTable } = require('../utils/tenantScope');
const { emitTableMutation } = require('../services/realtimeEmitter');

const createHttpError = (statusCode, message) => {
  const error = new Error(message);
  error.statusCode = statusCode;
  return error;
};

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

const sanitizeClientGeneratedPrimaryKey = (payload, columnDefinitions) => {
  if (!(columnDefinitions instanceof Map) || !isIntegerColumnDefinition(columnDefinitions.get('id'))) {
    return payload;
  }

  const sanitizeRow = (row) => {
    if (!row || typeof row !== 'object' || Array.isArray(row)) {
      return row;
    }

    if (!Object.prototype.hasOwnProperty.call(row, 'id')) {
      return row;
    }

    const rawId = row.id;
    if (rawId === undefined || rawId === null) {
      return row;
    }

    const idText = rawId.toString().trim();
    if (!idText || /^\d+$/.test(idText)) {
      return row;
    }

    const next = { ...row };
    const referenceId = (
      next.reference_id ?? next.referenceId ?? next.local_id ?? next.localId ?? ''
    )
      .toString()
      .trim();

    if (!referenceId) {
      next.reference_id = idText;
    }

    delete next.id;
    return next;
  };

  if (Array.isArray(payload)) {
    return payload.map(sanitizeRow);
  }

  return sanitizeRow(payload);
};

const resolveProductMutationTarget = async ({
  tenantDb,
  table,
  idField,
  idValue,
  payload,
  tenantId,
}) => {
  if (table !== 'products' || idField !== 'id') {
    return { idField, idValue };
  }

  const normalizedId = (idValue ?? '').toString().trim();
  if (/^\d+$/.test(normalizedId)) {
    return { idField, idValue: normalizedId };
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
    return { idField, idValue: normalizedId };
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

const parseMoneyValue = (value) => {
  if (value === undefined || value === null || value === '') {
    return null;
  }

  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }

  const normalized = value
    .toString()
    .replaceAll('.', '')
    .replaceAll(',', '')
    .trim();

  if (!normalized) {
    return null;
  }

  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
};

const extractSellingPrice = (payload = {}) => parseMoneyValue(
  payload.price ?? payload.selling_price ?? payload.harga_jual,
);

const extractPurchasePrice = (payload = {}) => parseMoneyValue(
  payload.purchase_price ?? payload.cost_price ?? payload.harga_modal,
);

const assertSellingPriceFloor = ({ sellingPrice, purchasePrice }) => {
  if (sellingPrice === null || purchasePrice === null) {
    return;
  }

  if (sellingPrice < purchasePrice) {
    throw createHttpError(400, 'Gagal: Harga jual di bawah harga modal.');
  }
};

const validateProductCreateOrUpsertPayload = (table, payload) => {
  if (table !== 'products') {
    return;
  }

  const rows = Array.isArray(payload) ? payload : [payload];
  for (const row of rows) {
    assertSellingPriceFloor({
      sellingPrice: extractSellingPrice(row),
      purchasePrice: extractPurchasePrice(row),
    });
  }
};

const normalizeSupplierPayloadObject = (payload = {}) => {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return payload;
  }

  const next = { ...payload };
  const normalizedName = (
    next.name ?? next.nama_toko ?? next.namaToko ?? next.store_name ?? ''
  )
    .toString()
    .trim();
  const normalizedPhone = (
    next.phone ?? next.kontak ?? next.contact ?? ''
  )
    .toString()
    .trim();
  const normalizedAddress = (
    next.address ?? next.alamat ?? next.store_address ?? ''
  )
    .toString()
    .trim();

  if (normalizedName) {
    next.name = normalizedName;
    next.nama_toko = normalizedName;
  }
  if (
    normalizedPhone ||
    Object.prototype.hasOwnProperty.call(next, 'phone') ||
    Object.prototype.hasOwnProperty.call(next, 'kontak') ||
    Object.prototype.hasOwnProperty.call(next, 'contact')
  ) {
    next.phone = normalizedPhone;
    next.kontak = normalizedPhone;
  }
  if (
    normalizedAddress ||
    Object.prototype.hasOwnProperty.call(next, 'address') ||
    Object.prototype.hasOwnProperty.call(next, 'alamat') ||
    Object.prototype.hasOwnProperty.call(next, 'store_address')
  ) {
    next.address = normalizedAddress;
    next.alamat = normalizedAddress;
  }

  return next;
};

const generateExpenseNumber = () => `EXP-${Date.now()}`;

const normalizeExpensePayloadObject = (payload = {}, { isCreate = false } = {}) => {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return payload;
  }

  const next = { ...payload };

  const normalizedTitle = (
    next.title ??
    next.judul ??
    next.expense_title ??
    next.name ??
    ''
  )
    .toString()
    .trim();

  const normalizedDescription = (
    next.description ??
    next.keterangan ??
    next.desc ??
    ''
  )
    .toString()
    .trim();

  const normalizedNotes = (
    next.notes ??
    next.catatan ??
    normalizedDescription
  )
    .toString()
    .trim();

  const normalizedAttachmentUrl = (
    next.attachment_url ??
    next.attachmentUrl ??
    next.attachment ??
    next.receipt_url ??
    next.receiptUrl ??
    next.file_path ??
    next.filePath ??
    ''
  )
    .toString()
    .trim();

  const normalizedExpenseNumber = (
    next.expense_number ??
    next.expenseNumber ??
    next.receipt_number ??
    next.receiptNumber ??
    next.transaction_number ??
    next.transactionNumber ??
    next.nomor_transaksi ??
    ''
  )
    .toString()
    .trim();

  if (normalizedTitle || isCreate) {
    next.title = normalizedTitle || 'Pengeluaran';
    next.expense_title = next.title;
    next.name = next.title;
  }

  if (normalizedDescription || Object.prototype.hasOwnProperty.call(next, 'description')) {
    next.description = normalizedDescription;
    next.keterangan = normalizedDescription;
  }

  if (normalizedNotes || Object.prototype.hasOwnProperty.call(next, 'notes')) {
    next.notes = normalizedNotes;
    next.catatan = normalizedNotes;
  }

  if (
    normalizedAttachmentUrl ||
    Object.prototype.hasOwnProperty.call(next, 'attachment_url') ||
    Object.prototype.hasOwnProperty.call(next, 'attachmentUrl')
  ) {
    next.attachment_url = normalizedAttachmentUrl;
    next.attachmentUrl = normalizedAttachmentUrl;
    next.attachment = normalizedAttachmentUrl;
  }

  if (normalizedExpenseNumber) {
    next.expense_number = normalizedExpenseNumber;
    next.receipt_number = normalizedExpenseNumber;
    next.transaction_number = normalizedExpenseNumber;
  } else if (isCreate) {
    const generated = generateExpenseNumber();
    next.expense_number = generated;
    next.receipt_number = generated;
    next.transaction_number = generated;
  }

  return next;
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

  delete next.total_spent;
  delete next.totalSpent;

  // total_spent harus dikelola dari transaksi, bukan input manual create/update.
  if (isCreate) {
    next.total_spent = 0;
  }
  return next;
};

const normalizePayloadForTable = (table, payload, options = {}) => {
  if (Array.isArray(payload)) {
    return payload.map((row) => normalizePayloadForTable(table, row, options));
  }

  if (!payload || typeof payload !== 'object') {
    return payload;
  }

  if (table === 'products') {
    return normalizeProductPayload(payload, options);
  }

  if (table === 'customers') {
    return normalizeCustomerPayload(payload, options);
  }

  if (table === 'suppliers') {
    return normalizeSupplierPayloadObject(payload);
  }

  if (table === 'expenses') {
    return normalizeExpensePayloadObject(payload, options);
  }

  return payload;
};

const validateProductUpdatePayload = async ({
  tenantDb,
  table,
  idField,
  idValue,
  payload,
  tenantId,
}) => {
  if (table !== 'products') {
    return;
  }

  const existing = await runSelect(tenantDb, table, {
    [`eq__${idField}`]: idValue,
    maybeSingle: true,
  }, { tenantId });

  if (!existing) {
    throw createHttpError(404, 'Record tidak ditemukan');
  }

  const sellingPrice = extractSellingPrice(payload) ?? extractSellingPrice(existing);
  const purchasePrice = extractPurchasePrice(payload) ?? extractPurchasePrice(existing);

  assertSellingPriceFloor({ sellingPrice, purchasePrice });
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
    throw createHttpError(500, `Schema guard: tabel ${table} tidak ditemukan. Jalankan migrasi di core service.`);
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
    throw createHttpError(
      500,
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
  if (table !== 'products' || Array.isArray(payload) || !payload) {
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

const isUndefinedCustomersTableError = (error, table) => {
  if (table !== 'customers') return false;
  const message = (error?.message || '').toString().toLowerCase();
  return error?.code === '42P01' || message.includes('relation "customers" does not exist');
};

const runWithCustomersTableRetry = async (tenantDb, table, tenantId, operation) => {
  if (isTenantScopedTable(table)) {
    await ensureTenantScopedTable(tenantDb, table, tenantId);
  }

  await ensureCustomersTable(tenantDb, table);
  await ensureProductsTableColumns(tenantDb, table);
  return operation();
};

const ensureSelectedColumn = (selectValue, columnName) => {
  if (!selectValue || selectValue === '*') {
    return selectValue;
  }

  const columns = selectValue
    .toString()
    .split(',')
    .map((column) => column.trim())
    .filter(Boolean);

  const hasColumn = columns.some((column) => column.toLowerCase() === columnName.toLowerCase());
  if (hasColumn) {
    return columns.join(',');
  }

  return `${columns.join(',')},${columnName}`;
};

const resolvePrimaryKeyColumn = async (tenantDb, table) => {
  const result = await tenantDb.query(
    `SELECT kcu.column_name
     FROM information_schema.table_constraints tc
     JOIN information_schema.key_column_usage kcu
       ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
      AND tc.table_name = kcu.table_name
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND tc.table_schema = ANY(current_schemas(false))
      AND tc.table_name = $1
    ORDER BY kcu.ordinal_position
    LIMIT 1`,
    [table],
  );

  return result.rows[0]?.column_name || 'id';
};

const hasMutationFields = (payload) => {
  if (Array.isArray(payload)) {
    return payload.some(
      (row) => row && typeof row === 'object' && Object.keys(row).length > 0,
    );
  }

  return !!payload && typeof payload === 'object' && Object.keys(payload).length > 0;
};

const hasValidIntegerId = (payload) => {
  const rows = Array.isArray(payload) ? payload : [payload];
  return rows.every((row) => {
    if (!row || typeof row !== 'object') {
      return false;
    }

    const idText = (row.id ?? '').toString().trim();
    return /^\d+$/.test(idText);
  });
};

const resolveTenantId = (req) => normalizeTenantId(
  req?.user?.tenantId ||
  req?.user?.tenant_id ||
  req?.tenant?.tenantId ||
  req?.auth?.tenantId ||
  req?.auth?.tenant_id,
);
const AGGREGATION_TABLES = new Set(['sales_records', 'expenses', 'daily_cash', 'petty_cash_logs']);

const normalizeSalesRecordId = (value) => {
  if (value === null || value === undefined) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const parseItemsFromJsonField = (value) => {
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

const loadSalesRecordItemsMap = async (tenantDb, rows, tenantId) => {
  const ids = rows
    .map((row) => normalizeSalesRecordId(row?.id))
    .filter((id) => id !== null);

  if (ids.length === 0) {
    return new Map();
  }

  try {
    const normalizedTenantId = normalizeTenantId(tenantId);
    const result = await tenantDb.query(
      normalizedTenantId
        ? `SELECT sales_record_id, product_id, product_name, qty, custom_price, note, is_service
           FROM sales_record_items
           WHERE tenant_id = $1
             AND sales_record_id = ANY($2::bigint[])
           ORDER BY id ASC`
        : `SELECT sales_record_id, product_id, product_name, qty, custom_price, note, is_service
           FROM sales_record_items
           WHERE sales_record_id = ANY($1::bigint[])
           ORDER BY id ASC`,
      normalizedTenantId ? [normalizedTenantId, ids] : [ids],
    );

    const grouped = new Map();
    for (const row of result.rows || []) {
      const key = Number(row.sales_record_id);
      if (!grouped.has(key)) {
        grouped.set(key, []);
      }
      grouped.get(key).push({
        product_id: row.product_id || null,
        product_name: row.product_name || '',
        qty: Number(row.qty || 0),
        custom_price: row.custom_price === null ? null : Number(row.custom_price),
        note: (row.note || '').toString(),
        is_service: row.is_service === true,
      });
    }

    return grouped;
  } catch (_) {
    return new Map();
  }
};

const listRecords = async (req, res) => {
  try {
    const table = req.params.table;
    const tenantId = resolveTenantId(req);

    if (table === 'sales_records') {
      const primaryKeyColumn = await resolvePrimaryKeyColumn(req.tenantDb, table);
      const query = {
        ...req.query,
        select: ensureSelectedColumn(req.query?.select, primaryKeyColumn),
      };

      const rows = await runWithCustomersTableRetry(
        req.tenantDb,
        table,
        tenantId,
        () => runSelect(req.tenantDb, table, query, { tenantId }),
      );

      const normalizedRows = Array.isArray(rows)
        ? rows.map((row) => ({
          ...row,
          id: row?.id ?? row?.[primaryKeyColumn] ?? null,
        }))
        : rows;

      const itemsMap = await loadSalesRecordItemsMap(
        req.tenantDb,
        Array.isArray(normalizedRows) ? normalizedRows : [],
        tenantId,
      );

      const hydratedRows = Array.isArray(normalizedRows)
        ? normalizedRows.map((row) => {
          const rowId = normalizeSalesRecordId(row?.id);
          const relationItems = rowId !== null ? itemsMap.get(rowId) || [] : [];
          return {
            ...row,
            items: relationItems.length > 0
              ? relationItems
              : parseItemsFromJsonField(row?.items_json ?? row?.items),
          };
        })
        : normalizedRows;

      return jsonOk(res, hydratedRows);
    }

    let effectiveQuery = { ...req.query };
    if (effectiveQuery.orderBy) {
      const currentColumns = await getTableColumnSet(req.tenantDb, table);
      const requestedOrderBy = (effectiveQuery.orderBy || '').toString().trim();
      if (requestedOrderBy && !currentColumns.has(requestedOrderBy)) {
        if (currentColumns.has('created_at')) {
          effectiveQuery.orderBy = 'created_at';
        } else if (currentColumns.has('updated_at')) {
          effectiveQuery.orderBy = 'updated_at';
        } else if (currentColumns.has('id')) {
          effectiveQuery.orderBy = 'id';
        } else {
          delete effectiveQuery.orderBy;
        }
      }
    }
    if (table === 'customers' && !effectiveQuery.select) {
      const customerColumns = await getTableColumnSet(req.tenantDb, table);
      const preferredColumns = [
        'id',
        'name',
        'phone',
        'address',
        'points',
      ].filter((column) => customerColumns.has(column));

      if (preferredColumns.length > 0) {
        effectiveQuery.select = preferredColumns.join(',');
      }
      if (!effectiveQuery.limit) {
        effectiveQuery.limit = 200;
      }
    }

    if (table === 'expenses' && !effectiveQuery.select) {
      const expenseColumns = await getTableColumnSet(req.tenantDb, table);
      const preferredColumns = [
        'id',
        'tenant_id',
        'expense_number',
        'title',
        'description',
        'notes',
        'attachment_url',
        'category',
        'amount',
        'payment_method',
        'status',
        'expense_date',
        'created_at',
        'updated_at',
      ].filter((column) => expenseColumns.has(column));

      if (preferredColumns.length > 0) {
        effectiveQuery.select = preferredColumns.join(',');
      }
    }

    const rows = await runWithCustomersTableRetry(
      req.tenantDb,
      table,
      tenantId,
      () => runSelect(req.tenantDb, table, effectiveQuery, { tenantId }),
    );
    return jsonOk(res, rows);
  } catch (error) {
    const table = (req?.params?.table || '').toString().trim().toLowerCase();
    if (AGGREGATION_TABLES.has(table)) {
      console.error('Dashboard Aggregation Error:', error);
    }
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

const createRecords = async (req, res) => {
  try {
    const table = req.params.table;
    const tenantId = resolveTenantId(req);
    const result = await runWithCustomersTableRetry(
      req.tenantDb,
      table,
      tenantId,
      async () => {
        const arrayPayload = parseBodyArray(req.body);
        const payload = normalizePayloadForTable(
          table,
          arrayPayload || parseBodyObject(req.body),
          { isCreate: true },
        );
        const columnDefinitions = await getTableColumnDefinitions(req.tenantDb, table);
        const sanitizedPayload = sanitizeClientGeneratedPrimaryKey(payload, columnDefinitions);
        const tenantScopedPayload = enforceTenantIdOnPayload(sanitizedPayload, tenantId, columnDefinitions);
        const filteredPayload = normalizePayloadByColumnDefinitions(tenantScopedPayload, columnDefinitions);
        if (!hasMutationFields(filteredPayload)) {
          throw createHttpError(400, `Tidak ada kolom yang cocok untuk tabel ${table}`);
        }
        const existingRecord = await findExistingRecordByReferenceId(
          req.tenantDb,
          table,
          filteredPayload,
          tenantId,
        );
        if (existingRecord) {
          return {
            rows: [existingRecord],
            rowCount: 1,
            idempotentHit: true,
          };
        }
        validateProductCreateOrUpsertPayload(table, filteredPayload);
        const { sql, values } = buildInsertQuery(table, filteredPayload);
        return req.tenantDb.query(sql, values);
      },
    );
    if (!result.idempotentHit) {
      for (const row of result.rows) {
        emitTableMutation(req, {
          table,
          action: 'INSERT',
          record: row,
        });
      }
    }
    return jsonOk(
      res,
      result.rows,
      result.idempotentHit ? 'Already exists' : 'Created',
      result.idempotentHit ? 200 : 201,
    );
  } catch (error) {
    return jsonError(
      res,
      error.statusCode || 500,
      error.message || 'Internal server error',
      error.message,
    );
  }
};

const upsertRecords = async (req, res) => {
  try {
    const table = req.params.table;
    const tenantId = resolveTenantId(req);
    const result = await runWithCustomersTableRetry(
      req.tenantDb,
      table,
      tenantId,
      async () => {
        const payload = normalizePayloadForTable(
          table,
          parseBodyArray(req.body) || parseBodyObject(req.body),
          { isCreate: true },
        );
        const columnDefinitions = await getTableColumnDefinitions(req.tenantDb, table);
        const sanitizedPayload = sanitizeClientGeneratedPrimaryKey(payload, columnDefinitions);
        const tenantScopedPayload = enforceTenantIdOnPayload(sanitizedPayload, tenantId, columnDefinitions);
        const filteredPayload = normalizePayloadByColumnDefinitions(tenantScopedPayload, columnDefinitions);
        if (!hasMutationFields(filteredPayload)) {
          throw createHttpError(400, `Tidak ada kolom yang cocok untuk tabel ${table}`);
        }
        const existingRecord = await findExistingRecordByReferenceId(
          req.tenantDb,
          table,
          filteredPayload,
          tenantId,
        );
        if (table === 'products') {
          if (existingRecord) {
            const updatePayload = { ...filteredPayload };
            delete updatePayload.id;

            if (!hasMutationFields(updatePayload)) {
              return {
                rows: [existingRecord],
                rowCount: 1,
                idempotentHit: true,
              };
            }

            const columnSet = await getTableColumnSet(req.tenantDb, table);
            const hasTenantColumn = columnSet.has('tenant_id');
            const existingId = (existingRecord.id ?? '').toString().trim();
            const updateTargetId = /^\d+$/.test(existingId)
              ? existingId
              : (existingRecord.reference_id ?? filteredPayload.reference_id ?? '').toString().trim();
            const updateTargetField = /^\d+$/.test(existingId) ? 'id' : 'reference_id';
            const { sql, values } = buildUpdateQuery(
              table,
              updatePayload,
              updateTargetField,
              updateTargetId,
              { tenantId, hasTenantColumn },
            );
            return req.tenantDb.query(sql, values);
          }

          validateProductCreateOrUpsertPayload(table, filteredPayload);
          const { sql, values } = buildInsertQuery(table, filteredPayload);
          return req.tenantDb.query(sql, values);
        }

        if (existingRecord) {
          return {
            rows: [existingRecord],
            rowCount: 1,
            idempotentHit: true,
          };
        }
        validateProductCreateOrUpsertPayload(table, filteredPayload);
        const onConflict = req.body?.onConflict;
        const filteredOnConflict = normalizeArray(onConflict).filter((column) =>
          columnDefinitions.has(column),
        );
        if (filteredOnConflict.length === 0) {
          throw createHttpError(400, 'onConflict tidak cocok dengan schema tabel');
        }

        if (table === 'products' && !hasValidIntegerId(filteredPayload)) {
          const { sql, values } = buildInsertQuery(table, filteredPayload);
          return req.tenantDb.query(sql, values);
        }

        const { sql, values } = buildUpsertQuery(
          table,
          filteredPayload,
          filteredOnConflict,
        );
        return req.tenantDb.query(sql, values);
      },
    );
    if (!result.idempotentHit) {
      for (const row of result.rows) {
        emitTableMutation(req, {
          table,
          action: 'UPSERT',
          record: row,
        });
      }
    }
    return jsonOk(res, result.rows, 'Upserted');
  } catch (error) {
    return jsonError(
      res,
      error.statusCode || 500,
      error.message || 'Internal server error',
      error.message,
    );
  }
};

const updateRecordById = async (req, res) => {
  try {
    const table = req.params.table;
    const tenantId = resolveTenantId(req);
    const result = await runWithCustomersTableRetry(
      req.tenantDb,
      table,
      tenantId,
      async () => {
        const idField = req.query.idField || 'id';
        const payload = normalizePayloadForTable(
          table,
          parseBodyObject(req.body),
          { isCreate: false },
        );
        const columnDefinitions = await getTableColumnDefinitions(req.tenantDb, table);
        const sanitizedPayload = sanitizeClientGeneratedPrimaryKey(payload, columnDefinitions);
        const tenantScopedPayload = enforceTenantIdOnPayload(sanitizedPayload, tenantId, columnDefinitions);
        const filteredPayload = normalizePayloadByColumnDefinitions(tenantScopedPayload, columnDefinitions);
        const resolvedTarget = await resolveProductMutationTarget({
          tenantDb: req.tenantDb,
          table,
          idField,
          idValue: req.params.id,
          payload: filteredPayload,
          tenantId,
        });
        await validateProductUpdatePayload({
          tenantDb: req.tenantDb,
          table,
          idField: resolvedTarget.idField,
          idValue: resolvedTarget.idValue,
          payload: filteredPayload,
          tenantId,
        });
        if (!hasMutationFields(filteredPayload)) {
          const existing = await runSelect(req.tenantDb, table, {
            [`eq__${resolvedTarget.idField}`]: resolvedTarget.idValue,
            maybeSingle: true,
          }, { tenantId });
          return {
            rowCount: existing ? 1 : 0,
            rows: existing ? [existing] : [],
          };
        }
        const columnSet = await getTableColumnSet(req.tenantDb, table);
        const { sql, values } = buildUpdateQuery(
          table,
          filteredPayload,
          resolvedTarget.idField,
          resolvedTarget.idValue,
          { tenantId, hasTenantColumn: columnSet.has('tenant_id') },
        );
        return req.tenantDb.query(sql, values);
      },
    );

    if ((result.rowCount || 0) === 0) {
      return jsonError(res, 404, 'Record tidak ditemukan');
    }
    emitTableMutation(req, {
      table,
      action: 'UPDATE',
      record: result.rows[0] || null,
      id: req.params.id,
    });
    return jsonOk(res, result.rows[0] || null, 'Updated');
  } catch (error) {
    return jsonError(
      res,
      error.statusCode || 500,
      error.message || 'Internal server error',
      error.message,
    );
  }
};

const deleteRecordById = async (req, res) => {
  try {
    const table = req.params.table;
    const tenantId = resolveTenantId(req);
    const result = await runWithCustomersTableRetry(
      req.tenantDb,
      table,
      tenantId,
      async () => {
        const idField = req.query.idField || 'id';
        const columnSet = await getTableColumnSet(req.tenantDb, table);
        const { sql, values } = buildDeleteQuery(
          table,
          idField,
          req.params.id,
          { tenantId, hasTenantColumn: columnSet.has('tenant_id') },
        );
        return req.tenantDb.query(sql, values);
      },
    );
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
};

module.exports = {
  listRecords,
  createRecords,
  upsertRecords,
  updateRecordById,
  deleteRecordById,
};
