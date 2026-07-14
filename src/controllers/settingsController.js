const { jsonOk, jsonError } = require('../utils/http');
const { getSharedPool } = require('../middlewares/tenantResolver');
const { getTableColumnSet } = require('../utils/sqlHelpers');

const normalizeOptionalText = (value) => {
  if (value === undefined || value === null) {
    return null;
  }
  const text = value.toString().trim();
  return text || null;
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

const resolveStoreProfileFromStoreSettings = async ({ tenantId, branchId }) => {
  const normalizedTenantId = (tenantId || '').toString().trim();
  if (!normalizedTenantId) {
    return {
      qrisImageUrl: null,
      logoUrl: null,
      storeName: null,
      storeAddress: null,
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
        allowPayAtCashier: true,
        isPaymentProofMandatory: true,
      };
    }

    const supportsBranchColumn = columnSet.has('branch_id');
    const supportsWideColumns =
      columnSet.has('qris_image_url') ||
      columnSet.has('logo_url') ||
      columnSet.has('store_name') ||
      columnSet.has('address');

    if (supportsWideColumns) {
      const whereParts = ['tenant_id = $1'];
      const values = [normalizedTenantId];
      let branchParamIndex = -1;

      if (supportsBranchColumn && branchId !== null) {
        branchParamIndex = values.length + 1;
        whereParts.push(`(branch_id = $${branchParamIndex} OR branch_id IS NULL)`);
        values.push(branchId);
      }

      const orderParts = [];
      if (supportsBranchColumn && branchId !== null && branchParamIndex > 0) {
        orderParts.push(`CASE WHEN branch_id = $${branchParamIndex} THEN 0 WHEN branch_id IS NULL THEN 1 ELSE 2 END`);
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
        ['allow_pay_at_cashier', ['allow_pay_at_cashier']],
        ['is_payment_proof_mandatory', ['is_payment_proof_mandatory', 'enable_qris_ocr']],
        ['enable_qris_ocr', ['enable_qris_ocr']],
      ];

      const whereParts = ['tenant_id = $1'];
      const values = [normalizedTenantId];
      if (supportsBranchColumn && branchId !== null) {
        whereParts.push(`(branch_id = $2 OR branch_id IS NULL)`);
        values.push(branchId);
      }

      const profile = {
        qrisImageUrl: null,
        logoUrl: null,
        storeName: null,
        storeAddress: null,
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
      allowPayAtCashier: true,
      isPaymentProofMandatory: true,
    };
  }

  const pool = getSharedPool();
  const storeProfile = await resolveStoreProfileFromStoreSettings({
    tenantId: normalizedTenantId,
    branchId,
  });

  if (storeProfile.qrisImageUrl) {
    return storeProfile;
  }

  try {
    const tenantResult = await pool.query(
      `SELECT qris_image_url, logo_url, name
       FROM tenants
       WHERE id = $1
       LIMIT 1`,
      [normalizedTenantId],
    );
    const tenantRow = tenantResult.rows?.[0] || null;
    const tenantUrl = (tenantRow?.qris_image_url || '').toString().trim() || null;
    const tenantLogo = (tenantRow?.logo_url || '').toString().trim() || null;
    const tenantName = (tenantRow?.name || '').toString().trim() || null;

    return {
      qrisImageUrl: tenantUrl || storeProfile.qrisImageUrl,
      logoUrl: storeProfile.logoUrl || tenantLogo,
      storeName: storeProfile.storeName || tenantName,
      storeAddress: storeProfile.storeAddress,
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

module.exports = {
  getSettings,
  getTenantSettings,
};
