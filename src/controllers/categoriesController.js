const { jsonOk, jsonError } = require('../utils/http');
const { runSelect, normalizeTenantId } = require('../utils/sqlHelpers');

const normalizeCategoriesQuery = (query = {}) => {
  const normalized = { ...query };

  // sqlHelpers mengenali filter via prefix eq__, bukan field polos.
  const typeRaw = normalized.category_type ?? normalized.type;
  if (typeRaw !== undefined && typeRaw !== null && typeRaw !== '') {
    normalized.eq__category_type = typeRaw;
  }

  delete normalized.category_type;
  delete normalized.type;

  return normalized;
};

const getCategories = async (req, res) => {
  try {
    const tenantId = normalizeTenantId(req.tenant?.tenantId || req.auth?.tenantId);
    const rows = await runSelect(
      req.tenantDb,
      'categories',
      normalizeCategoriesQuery(req.query),
      { tenantId },
    );
    return jsonOk(res, rows);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
};

module.exports = {
  getCategories,
};
