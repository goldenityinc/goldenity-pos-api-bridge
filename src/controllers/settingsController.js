const { jsonOk, jsonError } = require('../utils/http');
const { getSharedPool } = require('../middlewares/tenantResolver');
const { getTableColumnSet } = require('../utils/sqlHelpers');
const { uploadBase64Object } = require('../services/objectStorageService');

const DEFAULT_RECEIPT_FOOTER = 'Barang yang sudah dibeli tidak dapat ditukar/dikembalikan';

const normalizeOptionalText = (value) => {
  if (value === undefined || value === null) {
    return null;
  }
  const text = value.toString().trim();
  return text || null;
};

const normalizePrinterConfigs = (value) => {
  let parsed = value;

  if (typeof parsed === 'string') {
    const trimmed = parsed.trim();
    if (!trimmed) {
      return [];
    }

    try {
      parsed = JSON.parse(trimmed);
    } catch (_) {
      return [];
    }
  }

  if (!Array.isArray(parsed)) {
    return [];
  }

  return parsed
    .map((entry) => {
      if (!entry || typeof entry !== 'object') {
        return null;
      }

      const role = normalizeOptionalText(entry.role || entry.printerRole || entry.printer_role);
      const type = normalizeOptionalText(entry.type || entry.printerType || entry.printer_type);
      const address = normalizeOptionalText(
        entry.address || entry.host || entry.macAddress || entry.mac_address || entry.ipAddress || entry.ip_address,
      );

      if (!role || !type || !address) {
        return null;
      }

      return {
        role: role.toUpperCase(),
        type: type.toUpperCase(),
        address,
      };
    })
    .filter(Boolean);
};

const resolveSubscriptionEndDateCandidates = (row = {}) => {
  const candidates = [
    row.endDate,
    row.subscription_end_date,
    row.subscriptionEndDate,
    row.end_date,
  ];

  for (const candidate of candidates) {
    const normalized = normalizeOptionalText(candidate);
    if (normalized) {
      return normalized;
    }
  }

  return null;
};

const resolveSubscriptionEndDateColumn = (columnSet) => {
  if (!(columnSet instanceof Set)) {
    return null;
  }
  if (columnSet.has('subscription_end_date')) {
    return 'subscription_end_date';
  }
  if (columnSet.has('endDate')) {
    return 'endDate';
  }
  if (columnSet.has('end_date')) {
    return 'end_date';
  }
  return null;
};

const quoteIdentifier = (value) => `"${value.toString().replace(/"/g, '""')}"`;

const resolveTenantSubscriptionStatus = async ({ tenantId }) => {
  const normalizedTenantId = (tenantId || '').toString().trim();
  if (!normalizedTenantId) {
    return {
      endDate: null,
      subscription_end_date: null,
    };
  }

  const pool = getSharedPool();
  let resolvedEndDate = null;

  try {
    const appInstanceColumns = await getTableColumnSet(pool, 'app_instances');
    if (appInstanceColumns instanceof Set && appInstanceColumns.size > 0) {
      const endDateExpressions = [];
      if (appInstanceColumns.has('endDate')) {
        endDateExpressions.push('ai."endDate"::text');
      }
      if (appInstanceColumns.has('subscription_end_date')) {
        endDateExpressions.push('ai.subscription_end_date::text');
      }
      if (appInstanceColumns.has('end_date')) {
        endDateExpressions.push('ai.end_date::text');
      }

      const subscriptionResult = await pool.query(
        `SELECT
            ${endDateExpressions.length > 0
    ? `NULLIF(BTRIM(COALESCE(${endDateExpressions.join(', ')})), '')`
    : 'NULL::text'} AS subscription_end_date
         FROM app_instances ai
         LEFT JOIN solutions s ON s.id = ai."solutionId"
         WHERE ai."tenantId" = $1
           AND ai.status = 'ACTIVE'
         ORDER BY
           CASE
             WHEN UPPER(COALESCE(s.code, '')) = 'POS' THEN 0
             WHEN UPPER(COALESCE(s.name, '')) LIKE '%POS%' THEN 1
             ELSE 2
           END,
           ai."updatedAt" DESC
         LIMIT 1`,
        [normalizedTenantId],
      );

      resolvedEndDate = resolveSubscriptionEndDateCandidates(subscriptionResult.rows?.[0] || {});
    }
  } catch (error) {
    console.warn('[settingsController] app_instances subscription lookup skipped:', error.message);
  }

  let tenantSubscriptionColumn = null;
  try {
    const tenantColumns = await getTableColumnSet(pool, 'tenants');
    tenantSubscriptionColumn = resolveSubscriptionEndDateColumn(tenantColumns);

    if (!resolvedEndDate && tenantSubscriptionColumn) {
      const tenantResult = await pool.query(
        `SELECT ${quoteIdentifier(tenantSubscriptionColumn)} AS subscription_end_date
         FROM tenants
         WHERE id = $1
         LIMIT 1`,
        [normalizedTenantId],
      );
      resolvedEndDate = resolveSubscriptionEndDateCandidates(tenantResult.rows?.[0] || {});
    }

    if (resolvedEndDate && tenantSubscriptionColumn) {
      await pool.query(
        `UPDATE tenants
         SET ${quoteIdentifier(tenantSubscriptionColumn)} = $1
         WHERE id = $2`,
        [resolvedEndDate, normalizedTenantId],
      );
    }
  } catch (error) {
    console.warn('[settingsController] tenant subscription cache update skipped:', error.message);
  }

  return {
    endDate: resolvedEndDate,
    subscription_end_date: resolvedEndDate,
  };
};

const parseOptionalBranchId = (value) => {
  const text = (value || '').toString().trim();
  if (!text) {
    return null;
  }

  if (!/^\d+$/.test(text)) {
    const error = new Error('branchId tidak valid');
    error.statusCode = 400;
    throw error;
  }

  return Number.parseInt(text, 10);
};

const parseBooleanSetting = (value, fallback = true) => {
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

const resolveSettingsUploadKey = (req) => {
  return (
    req.headers['x-goldenity-settings-key'] ||
    req.headers['x-settings-key'] ||
    req.body?.settingsKey ||
    req.body?.settings_key ||
    ''
  )
    .toString()
    .trim();
};

const ensureSettingsUploadAllowed = (req) => {
  const configuredKey = (process.env.WEB_SETTINGS_UPLOAD_KEY || '').toString().trim();
  if (!configuredKey) {
    return true;
  }

  const providedKey = resolveSettingsUploadKey(req);
  return providedKey.length > 0 && providedKey === configuredKey;
};

const updateStoreSettingsWideRow = async ({
  pool,
  columnSet,
  tenantId,
  branchId,
  fieldValues,
}) => {
  const normalizedEntries = Object.entries(fieldValues).filter(
    ([key, value]) => columnSet.has(key) && value !== undefined,
  );

  if (normalizedEntries.length === 0) {
    return false;
  }

  const supportsBranchColumn = columnSet.has('branch_id');
  const whereParts = ['tenant_id = $1'];
  const whereValues = [tenantId];

  if (supportsBranchColumn) {
    if (branchId !== null) {
      whereParts.push(`branch_id = $${whereValues.length + 1}`);
      whereValues.push(branchId);
    } else {
      whereParts.push('branch_id IS NULL');
    }
  }

  const assignmentClauses = normalizedEntries.map(
    ([key], index) => `${quoteIdentifier(key)} = $${whereValues.length + index + 1}`,
  );
  const assignmentValues = normalizedEntries.map(([, value]) => value);

  if (columnSet.has('updated_at')) {
    assignmentClauses.push('updated_at = NOW()');
  }

  const updateResult = await pool.query(
    `UPDATE store_settings
     SET ${assignmentClauses.join(', ')}
     WHERE ${whereParts.join(' AND ')}`,
    [...whereValues, ...assignmentValues],
  );

  if ((updateResult.rowCount || 0) > 0) {
    return true;
  }

  const insertColumns = ['tenant_id'];
  const insertValues = [tenantId];

  if (supportsBranchColumn) {
    insertColumns.push('branch_id');
    insertValues.push(branchId);
  }

  normalizedEntries.forEach(([key, value]) => {
    insertColumns.push(key);
    insertValues.push(value);
  });

  if (columnSet.has('created_at')) {
    insertColumns.push('created_at');
  }
  if (columnSet.has('updated_at')) {
    insertColumns.push('updated_at');
  }

  const valuePlaceholders = [];
  let parameterIndex = 1;
  for (const columnName of insertColumns) {
    if (columnName === 'created_at' || columnName === 'updated_at') {
      valuePlaceholders.push('NOW()');
    } else {
      valuePlaceholders.push(`$${parameterIndex}`);
      parameterIndex += 1;
    }
  }

  await pool.query(
    `INSERT INTO store_settings (${insertColumns.map(quoteIdentifier).join(', ')})
     VALUES (${valuePlaceholders.join(', ')})`,
    insertValues,
  );

  return true;
};

const upsertStoreSettingsKeyValue = async ({
  pool,
  columnSet,
  tenantId,
  branchId,
  valuesByKey,
}) => {
  if (!columnSet.has('key') || !columnSet.has('value')) {
    return false;
  }

  const supportsBranchColumn = columnSet.has('branch_id');
  const hasUpdatedAt = columnSet.has('updated_at');
  const hasCreatedAt = columnSet.has('created_at');

  for (const [key, value] of Object.entries(valuesByKey)) {
    if (value === undefined) {
      continue;
    }

    const whereParts = ['tenant_id = $1', 'key = $2'];
    const whereValues = [tenantId, key];

    if (supportsBranchColumn) {
      if (branchId !== null) {
        whereParts.push(`branch_id = $${whereValues.length + 1}`);
        whereValues.push(branchId);
      } else {
        whereParts.push('branch_id IS NULL');
      }
    }

    const updateClauses = [`value = $${whereValues.length + 1}`];
    const updateValues = [...whereValues, value];
    if (hasUpdatedAt) {
      updateClauses.push('updated_at = NOW()');
    }

    const updateResult = await pool.query(
      `UPDATE store_settings
       SET ${updateClauses.join(', ')}
       WHERE ${whereParts.join(' AND ')}`,
      updateValues,
    );

    if ((updateResult.rowCount || 0) > 0) {
      continue;
    }

    const insertColumns = ['tenant_id'];
    const insertValues = [tenantId];
    if (supportsBranchColumn) {
      insertColumns.push('branch_id');
      insertValues.push(branchId);
    }
    insertColumns.push('key', 'value');
    insertValues.push(key, value);
    if (hasCreatedAt) {
      insertColumns.push('created_at');
    }
    if (hasUpdatedAt) {
      insertColumns.push('updated_at');
    }

    const valuePlaceholders = [];
    let parameterIndex = 1;
    for (const columnName of insertColumns) {
      if (columnName === 'created_at' || columnName === 'updated_at') {
        valuePlaceholders.push('NOW()');
      } else {
        valuePlaceholders.push(`$${parameterIndex}`);
        parameterIndex += 1;
      }
    }

    await pool.query(
      `INSERT INTO store_settings (${insertColumns.map(quoteIdentifier).join(', ')})
       VALUES (${valuePlaceholders.join(', ')})`,
      insertValues,
    );
  }

  return true;
};

const persistStoreProfileSettings = async ({
  tenantId,
  branchId,
  qrisImageUrl,
  receiptFooter,
}) => {
  const pool = getSharedPool();
  const columnSet = await getTableColumnSet(pool, 'store_settings');
  if (!(columnSet instanceof Set) || columnSet.size === 0) {
    return false;
  }

  const supportsWideColumns =
    columnSet.has('qris_image_url') ||
    columnSet.has('receipt_footer');

  if (supportsWideColumns) {
    return updateStoreSettingsWideRow({
      pool,
      columnSet,
      tenantId,
      branchId,
      fieldValues: {
        qris_image_url: qrisImageUrl,
        receipt_footer: receiptFooter,
      },
    });
  }

  if (columnSet.has('key') && columnSet.has('value')) {
    return upsertStoreSettingsKeyValue({
      pool,
      columnSet,
      tenantId,
      branchId,
      valuesByKey: {
        qris_image_url: qrisImageUrl,
        receipt_footer: receiptFooter,
      },
    });
  }

  return false;
};

const resolveStoreProfileFromStoreSettings = async ({ tenantId, branchId }) => {
  const normalizedTenantId = (tenantId || '').toString().trim();
  if (!normalizedTenantId) {
    return {
      qrisImageUrl: null,
      logoUrl: null,
      storeName: null,
      storeAddress: null,
      receiptFooter: null,
      printerConfigs: [],
      allowPayAtCashier: true,
      isPaymentProofMandatory: true,
    };
  }

  const pool = getSharedPool();

  try {
    const columnSet = await getTableColumnSet(pool, 'store_settings');
    if (!(columnSet instanceof Set) || columnSet.size === 0) {
      return {
        qrisImageUrl: null,
        logoUrl: null,
        storeName: null,
        storeAddress: null,
        receiptFooter: null,
        printerConfigs: [],
        allowPayAtCashier: true,
        isPaymentProofMandatory: true,
      };
    }

    const supportsBranchColumn = columnSet.has('branch_id');
    const supportsWideColumns =
      columnSet.has('qris_image_url') ||
      columnSet.has('logo_url') ||
      columnSet.has('store_name') ||
      columnSet.has('address') ||
      columnSet.has('receipt_footer');

    if (supportsWideColumns) {
      const whereParts = ['tenant_id = $1'];
      const values = [normalizedTenantId];
      let branchParamIndex = -1;

      if (supportsBranchColumn && branchId !== null) {
        branchParamIndex = values.length + 1;
        whereParts.push(`(branch_id::text = $${branchParamIndex}::text OR branch_id IS NULL)`);
        values.push(branchId);
      }

      const orderParts = [];
      if (supportsBranchColumn && branchId !== null && branchParamIndex > 0) {
        orderParts.push(`CASE WHEN branch_id::text = $${branchParamIndex}::text THEN 0 WHEN branch_id IS NULL THEN 1 ELSE 2 END`);
      }
      if (columnSet.has('updated_at')) {
        orderParts.push('updated_at DESC NULLS LAST');
      }
      if (columnSet.has('created_at')) {
        orderParts.push('created_at DESC NULLS LAST');
      }

      const wideResult = await pool.query(
        `SELECT
            ${columnSet.has('qris_image_url') ? `COALESCE(qris_image_url, '')` : `''`} AS qris_image_url,
            ${columnSet.has('logo_url') ? `COALESCE(logo_url, '')` : `''`} AS logo_url,
            ${columnSet.has('store_name') ? `COALESCE(store_name, '')` : `''`} AS store_name,
            ${columnSet.has('address') ? `COALESCE(address, '')` : `''`} AS store_address,
            ${columnSet.has('receipt_footer') ? `COALESCE(receipt_footer, '')` : `''`} AS receipt_footer,
            ${columnSet.has('printer_configs') ? 'printer_configs' : 'NULL::jsonb'} AS printer_configs,
            ${columnSet.has('allow_pay_at_cashier') ? 'allow_pay_at_cashier' : 'NULL::boolean'} AS allow_pay_at_cashier,
            ${columnSet.has('is_payment_proof_mandatory') ? 'is_payment_proof_mandatory' : 'NULL::boolean'} AS is_payment_proof_mandatory,
            ${columnSet.has('enable_qris_ocr') ? 'enable_qris_ocr' : 'NULL::boolean'} AS enable_qris_ocr
         FROM store_settings
         WHERE ${whereParts.join(' AND ')}
         ORDER BY ${orderParts.length > 0 ? orderParts.join(', ') : 'id DESC'}
         LIMIT 1`,
        values,
      );

      const row = wideResult.rows?.[0] || null;
      if (row) {
        return {
          qrisImageUrl: (row.qris_image_url || '').toString().trim() || null,
          logoUrl: (row.logo_url || '').toString().trim() || null,
          storeName: (row.store_name || '').toString().trim() || null,
          storeAddress: (row.store_address || '').toString().trim() || null,
          receiptFooter: (row.receipt_footer || '').toString().trim() || null,
          printerConfigs: normalizePrinterConfigs(row.printer_configs),
          allowPayAtCashier: parseBooleanSetting(row.allow_pay_at_cashier, true),
          isPaymentProofMandatory: parseBooleanSetting(
            row.is_payment_proof_mandatory,
            parseBooleanSetting(row.enable_qris_ocr, true),
          ),
        };
      }
    }

    if (columnSet.has('key') && columnSet.has('value')) {
      const keyPairs = [
        ['qris_image_url', ['qris_image_url', 'qris_image', 'qris_static_image_url']],
        ['logo_url', ['logo_url', 'store_logo_url']],
        ['store_name', ['store_name', 'nama_toko', 'name']],
        ['store_address', ['address', 'store_address', 'alamat']],
        ['receipt_footer', ['receipt_footer', 'receiptFooter']],
        ['allow_pay_at_cashier', ['allow_pay_at_cashier']],
        ['is_payment_proof_mandatory', ['is_payment_proof_mandatory', 'enable_qris_ocr']],
        ['enable_qris_ocr', ['enable_qris_ocr']],
      ];

      const whereParts = ['tenant_id = $1'];
      const values = [normalizedTenantId];
      if (supportsBranchColumn && branchId !== null) {
        whereParts.push(`(branch_id::text = $2::text OR branch_id IS NULL)`);
        values.push(branchId);
      }

      const profile = {
        qrisImageUrl: null,
        logoUrl: null,
        storeName: null,
        storeAddress: null,
        receiptFooter: null,
        printerConfigs: [],
        allowPayAtCashier: true,
        isPaymentProofMandatory: true,
      };

      for (const [field, keys] of keyPairs) {
        const result = await pool.query(
          `SELECT COALESCE(value, '') AS resolved_value
           FROM store_settings
           WHERE ${whereParts.join(' AND ')}
             AND key = ANY($${values.length + 1}::text[])
           ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
           LIMIT 1`,
          [...values, keys],
        );

        const value = (result.rows?.[0]?.resolved_value || '').toString().trim() || null;
        if (field === 'qris_image_url') {
          profile.qrisImageUrl = value;
        } else if (field === 'logo_url') {
          profile.logoUrl = value;
        } else if (field === 'store_name') {
          profile.storeName = value;
        } else if (field === 'store_address') {
          profile.storeAddress = value;
        } else if (field === 'receipt_footer') {
          profile.receiptFooter = value;
        } else if (field === 'allow_pay_at_cashier') {
          profile.allowPayAtCashier = parseBooleanSetting(value, true);
        } else if (field === 'is_payment_proof_mandatory') {
          profile.isPaymentProofMandatory = parseBooleanSetting(value, true);
        } else if (field === 'enable_qris_ocr') {
          profile.isPaymentProofMandatory = parseBooleanSetting(
            value,
            profile.isPaymentProofMandatory,
          );
        }
      }

      return profile;
    }
  } catch (error) {
    console.warn('[settingsController] store_settings profile lookup skipped:', error.message);
  }

  return {
    qrisImageUrl: null,
    logoUrl: null,
    storeName: null,
    storeAddress: null,
    receiptFooter: null,
    printerConfigs: [],
    allowPayAtCashier: true,
    isPaymentProofMandatory: true,
  };
};

const resolveTenantProfile = async ({ tenantId, branchId }) => {
  const normalizedTenantId = (tenantId || '').toString().trim();
  if (!normalizedTenantId) {
    return {
      qrisImageUrl: null,
      logoUrl: null,
      storeName: null,
      storeAddress: null,
      receiptFooter: null,
      printerConfigs: [],
      allowPayAtCashier: true,
      isPaymentProofMandatory: true,
    };
  }

  const pool = getSharedPool();
  const storeProfile = await resolveStoreProfileFromStoreSettings({
    tenantId: normalizedTenantId,
    branchId,
  });

  try {
    const tenantColumns = await getTableColumnSet(pool, 'tenants');
    const tenantSelectColumns = [
      'qris_image_url',
      'logo_url',
      'name',
      'receipt_footer',
      tenantColumns.has('printer_configs') ? 'printer_configs' : 'NULL::jsonb AS printer_configs',
    ];

    const tenantResult = await pool.query(
      `SELECT ${tenantSelectColumns.join(', ')}
       FROM tenants
       WHERE id = $1
       LIMIT 1`,
      [normalizedTenantId],
    );
    const tenantRow = tenantResult.rows?.[0] || null;
    const tenantUrl = (tenantRow?.qris_image_url || '').toString().trim() || null;
    const tenantLogo = (tenantRow?.logo_url || '').toString().trim() || null;
    const tenantName = (tenantRow?.name || '').toString().trim() || null;
    const tenantReceiptFooter = (tenantRow?.receipt_footer || '').toString().trim() || null;
    const tenantPrinterConfigs = normalizePrinterConfigs(tenantRow?.printer_configs);

    return {
      qrisImageUrl: storeProfile.qrisImageUrl || tenantUrl,
      logoUrl: storeProfile.logoUrl || tenantLogo,
      storeName: storeProfile.storeName || tenantName,
      storeAddress: storeProfile.storeAddress,
      receiptFooter: storeProfile.receiptFooter || tenantReceiptFooter || DEFAULT_RECEIPT_FOOTER,
      printerConfigs: tenantPrinterConfigs.length > 0 ? tenantPrinterConfigs : storeProfile.printerConfigs,
      allowPayAtCashier: storeProfile.allowPayAtCashier,
      isPaymentProofMandatory: storeProfile.isPaymentProofMandatory,
    };
  } catch (error) {
    console.warn('[settingsController] Tenant QRIS lookup skipped:', error.message);
    return storeProfile;
  }
};

/**
 * GET /api/v1/settings
 * Returns application configuration including Web Order URL for Flutter app
 * Used for QR code generation and customer order links
 */
const getSettings = async (req, res) => {
  try {
    const tenantId =
      (req.query.tenantId || req.query.tenant_id || req.user?.tenantId || req.user?.tenant_id || '')
        .toString()
        .trim();
    const branchId = parseOptionalBranchId(req.query.branchId || req.query.branch_id);
    const webOrderUrl = (process.env.WEB_ORDER_URL || 'https://pos-web-ordering-production.up.railway.app')
      .toString()
      .trim();
    const profile = await resolveTenantProfile({ tenantId, branchId });
    const subscriptionStatus = await resolveTenantSubscriptionStatus({ tenantId });

    return jsonOk(res, {
      config: {
        web_order_url: webOrderUrl,
        qris_image_url: profile.qrisImageUrl,
        allow_pay_at_cashier: profile.allowPayAtCashier,
        is_payment_proof_mandatory: profile.isPaymentProofMandatory,
        enable_qris_ocr: profile.isPaymentProofMandatory,
        logo_url: profile.logoUrl,
        store_name: profile.storeName,
        address: profile.storeAddress,
        receipt_footer: profile.receiptFooter || DEFAULT_RECEIPT_FOOTER,
        printer_configs: profile.printerConfigs,
        endDate: subscriptionStatus.endDate,
        subscription_end_date: subscriptionStatus.subscription_end_date,
        api_version: '1.0.0',
      },
      subscription: subscriptionStatus,
      description: 'Bridge API Settings - Used by Flutter app for dynamic QR code generation',
    });
  } catch (error) {
    console.error('[settingsController.getSettings] Error:', error.message);
    return jsonError(res, 500, 'Failed to retrieve settings');
  }
};

/**
 * GET /api/v1/tenant/:tenantId/settings
 * Returns tenant-specific settings including Web Order URL
 */
const getTenantSettings = async (req, res) => {
  try {
    const tenantId = req.params.tenantId || req.user?.tenantId;
    if (!tenantId) {
      return jsonError(res, 400, 'tenantId wajib diisi');
    }

    const branchId = parseOptionalBranchId(req.query.branchId || req.query.branch_id);
    const webOrderUrl = (process.env.WEB_ORDER_URL || 'https://pos-web-ordering-production.up.railway.app')
      .toString()
      .trim();
    const profile = await resolveTenantProfile({ tenantId, branchId });
    const subscriptionStatus = await resolveTenantSubscriptionStatus({ tenantId });

    return jsonOk(res, {
      tenantId,
      config: {
        web_order_url: webOrderUrl,
        qr_order_base_url: webOrderUrl,
        qris_image_url: profile.qrisImageUrl,
        allow_pay_at_cashier: profile.allowPayAtCashier,
        is_payment_proof_mandatory: profile.isPaymentProofMandatory,
        enable_qris_ocr: profile.isPaymentProofMandatory,
        logo_url: profile.logoUrl,
        store_name: profile.storeName,
        address: profile.storeAddress,
        receipt_footer: profile.receiptFooter || DEFAULT_RECEIPT_FOOTER,
        printer_configs: profile.printerConfigs,
        endDate: subscriptionStatus.endDate,
        subscription_end_date: subscriptionStatus.subscription_end_date,
        api_version: '1.0.0',
        description: 'Tenant-specific settings for QR code generation',
      },
      subscription: subscriptionStatus,
    });
  } catch (error) {
    console.error('[settingsController.getTenantSettings] Error:', error.message);
    return jsonError(res, 500, 'Failed to retrieve tenant settings');
  }
};

/**
 * POST /api/v1/settings/qris-image
 * Uploads or sets the static QRIS image URL for a tenant.
 */
const updateTenantQrisImage = async (req, res) => {
  try {
    if (!ensureSettingsUploadAllowed(req)) {
      return jsonError(res, 403, 'Kunci pengaturan QRIS tidak valid');
    }

    const tenantId = (
      req.body?.tenantId ||
      req.body?.tenant_id ||
      req.user?.tenantId ||
      req.user?.tenant_id ||
      ''
    )
      .toString()
      .trim();

    if (!tenantId) {
      return jsonError(res, 400, 'tenantId wajib diisi');
    }

    const branchId = parseOptionalBranchId(req.body?.branchId || req.body?.branch_id);

    let qrisImageUrl = (
      req.body?.qrisImageUrl ||
      req.body?.qris_image_url ||
      ''
    )
      .toString()
      .trim();

    const uploadedFile = req.file || null;
    if (uploadedFile && uploadedFile.buffer) {
      const fileName = (
        req.body?.fileName ||
        req.body?.file_name ||
        `qris-${tenantId}-${Date.now()}.png`
      )
        .toString()
        .trim();
      const contentType = (
        req.body?.contentType ||
        req.body?.content_type ||
        uploadedFile.mimetype ||
        'image/png'
      )
        .toString()
        .trim();
      const uploaded = await uploadBase64Object({
        bucket: process.env.PAYMENT_PROOF_BUCKET || 'payment-proofs',
        fileName,
        base64: uploadedFile.buffer.toString('base64'),
        contentType,
      });
      qrisImageUrl = (uploaded?.url || '').toString().trim();
    }

    if (!qrisImageUrl) {
      return jsonError(res, 400, 'qrisImageUrl atau file upload wajib diisi');
    }

    const pool = getSharedPool();
    const storedInBranchSettings = await persistStoreProfileSettings({
      tenantId,
      branchId,
      qrisImageUrl,
    });

    if (!storedInBranchSettings || branchId === null) {
      await pool.query(
        `UPDATE tenants
         SET qris_image_url = $1,
             updated_at = NOW()
         WHERE id = $2`,
        [qrisImageUrl, tenantId],
      );
    }

    return jsonOk(res, {
      tenantId,
      branchId,
      qrisImageUrl,
    }, 'QRIS static berhasil disimpan');
  } catch (error) {
    if (error?.statusCode) {
      return jsonError(res, error.statusCode, error.message);
    }
    console.error('[settingsController.updateTenantQrisImage] Error:', error.message);
    return jsonError(res, 500, 'Gagal menyimpan QRIS static', error.message);
  }
};

/**
 * POST /api/v1/settings/receipt-footer
 * Updates receipt footer text for a tenant.
 */
const updateTenantReceiptFooter = async (req, res) => {
  try {
    if (!ensureSettingsUploadAllowed(req)) {
      return jsonError(res, 403, 'Kunci pengaturan tidak valid');
    }

    const tenantId = (
      req.body?.tenantId ||
      req.body?.tenant_id ||
      req.user?.tenantId ||
      req.user?.tenant_id ||
      ''
    )
      .toString()
      .trim();

    if (!tenantId) {
      return jsonError(res, 400, 'tenantId wajib diisi');
    }

    const branchId = parseOptionalBranchId(req.body?.branchId || req.body?.branch_id);

    const rawFooter = req.body?.receiptFooter ?? req.body?.receipt_footer;
    const normalizedFooter = normalizeOptionalText(rawFooter) || DEFAULT_RECEIPT_FOOTER;
    if (normalizedFooter.length > 500) {
      return jsonError(res, 400, 'receiptFooter maksimal 500 karakter');
    }

    const pool = getSharedPool();
    const storedInBranchSettings = await persistStoreProfileSettings({
      tenantId,
      branchId,
      receiptFooter: normalizedFooter,
    });

    if (!storedInBranchSettings || branchId === null) {
      await pool.query(
        `UPDATE tenants
         SET receipt_footer = $1,
             updated_at = NOW()
         WHERE id = $2`,
        [normalizedFooter, tenantId],
      );
    }

    return jsonOk(
      res,
      {
        tenantId,
        branchId,
        receiptFooter: normalizedFooter,
      },
      'Footer struk berhasil disimpan',
    );
  } catch (error) {
    if (error?.statusCode) {
      return jsonError(res, error.statusCode, error.message);
    }
    console.error('[settingsController.updateTenantReceiptFooter] Error:', error.message);
    return jsonError(res, 500, 'Gagal menyimpan footer struk', error.message);
  }
};

/**
 * POST /api/v1/settings/printer-configs
 * Updates printer routing configuration for a tenant.
 */
const updateTenantPrinterConfigs = async (req, res) => {
  try {
    if (!ensureSettingsUploadAllowed(req)) {
      return jsonError(res, 403, 'Kunci pengaturan tidak valid');
    }

    const tenantId = (
      req.body?.tenantId ||
      req.body?.tenant_id ||
      req.user?.tenantId ||
      req.user?.tenant_id ||
      ''
    )
      .toString()
      .trim();

    if (!tenantId) {
      return jsonError(res, 400, 'tenantId wajib diisi');
    }

    const printerConfigs = normalizePrinterConfigs(
      req.body?.printerConfigs ?? req.body?.printer_configs ?? req.body?.value,
    );

    const pool = getSharedPool();
    const tenantColumns = await getTableColumnSet(pool, 'tenants');
    if (!(tenantColumns instanceof Set) || !tenantColumns.has('printer_configs')) {
      return jsonError(res, 500, 'Kolom printer_configs belum tersedia di tenants');
    }

    await pool.query(
      `UPDATE tenants
       SET printer_configs = $1::jsonb,
           updated_at = NOW()
       WHERE id = $2`,
      [JSON.stringify(printerConfigs), tenantId],
    );

    return jsonOk(res, {
      tenantId,
      printerConfigs,
    }, 'Konfigurasi printer berhasil disimpan');
  } catch (error) {
    console.error('[settingsController.updateTenantPrinterConfigs] Error:', error.message);
    return jsonError(res, 500, 'Gagal menyimpan konfigurasi printer', error.message);
  }
};

module.exports = {
  getSettings,
  getTenantSettings,
  updateTenantQrisImage,
  updateTenantReceiptFooter,
  updateTenantPrinterConfigs,
};
