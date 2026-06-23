const { jsonOk, jsonError } = require('../utils/http');

/**
 * GET /api/v1/settings
 * Returns application configuration including Web Order URL for Flutter app
 * Used for QR code generation and customer order links
 */
const getSettings = async (req, res) => {
  try {
    const webOrderUrl = (process.env.WEB_ORDER_URL || 'https://pos-web-ordering-production.up.railway.app')
      .toString()
      .trim();

    return jsonOk(res, {
      config: {
        web_order_url: webOrderUrl,
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

    return jsonOk(res, {
      tenantId,
      config: {
        web_order_url: webOrderUrl,
        qr_order_base_url: webOrderUrl,
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
