const { jsonOk, jsonError } = require('../utils/http');
const { getSharedPool } = require('../middlewares/tenantResolver');

const resolveTenantQrisImageUrl = async (tenantId) => {
  const normalizedTenantId = (tenantId || '').toString().trim();
  if (!normalizedTenantId) {
    return null;
  }

  const pool = getSharedPool();

  try {
    const tenantResult = await pool.query(
      `SELECT qris_image_url
       FROM tenants
       WHERE id = $1
       LIMIT 1`,
      [normalizedTenantId],
    );
    const tenantUrl = (tenantResult.rows?.[0]?.qris_image_url || '').toString().trim();
    if (tenantUrl) {
      return tenantUrl;
    }
  } catch (error) {
    console.warn('[settingsController] Tenant QRIS lookup skipped:', error.message);
  }

  try {
    const storeSettingsResult = await pool.query(
      `SELECT COALESCE(value, '') AS qris_image_url
       FROM store_settings
       WHERE tenant_id = $1
         AND key = ANY($2::text[])
       ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
       LIMIT 1`,
      [normalizedTenantId, ['qris_image_url', 'qris_image', 'qris_static_image_url']],
    );
    const settingsUrl = (storeSettingsResult.rows?.[0]?.qris_image_url || '').toString().trim();
    return settingsUrl || null;
  } catch (error) {
    console.warn('[settingsController] store_settings QRIS lookup skipped:', error.message);
    return null;
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
    const webOrderUrl = (process.env.WEB_ORDER_URL || 'https://pos-web-ordering-production.up.railway.app')
      .toString()
      .trim();
    const qrisImageUrl = await resolveTenantQrisImageUrl(tenantId);

    return jsonOk(res, {
      config: {
        web_order_url: webOrderUrl,
        qris_image_url: qrisImageUrl,
        api_version: '1.0.0',
      },
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

    const webOrderUrl = (process.env.WEB_ORDER_URL || 'https://pos-web-ordering-production.up.railway.app')
      .toString()
      .trim();
    const qrisImageUrl = await resolveTenantQrisImageUrl(tenantId);

    return jsonOk(res, {
      tenantId,
      config: {
        web_order_url: webOrderUrl,
        qr_order_base_url: webOrderUrl,
        qris_image_url: qrisImageUrl,
        api_version: '1.0.0',
        description: 'Tenant-specific settings for QR code generation',
      },
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
