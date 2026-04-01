const { jsonOk, jsonError } = require('../utils/http');
const { runSelect, normalizeTenantId } = require('../utils/sqlHelpers');

const resolveTenantIdFromRequest = (req) => normalizeTenantId(
  req?.user?.tenantId ||
  req?.user?.tenant_id ||
  req?.tenant?.tenantId ||
  req?.auth?.tenantId ||
  req?.auth?.tenant_id,
);

const normalizeCategoriesQuery = (query = {}) => {
  const normalized = { ...query };

  // sqlHelpers mengenali filter via prefix eq__, bukan field polos.
  const typeRaw = normalized.category_type ?? normalized.type;
  if (typeRaw !== undefined && typeRaw !== null && typeRaw !== '') {
    const resolvedType = typeRaw
      .toString()
      .trim()
      .toLowerCase();
    const isExpense = resolvedType === 'expense' || resolvedType === 'pengeluaran';
    normalized.in__category_type = isExpense
      ? 'EXPENSE,expense'
      : 'PRODUCT,product';
  }

  delete normalized.category_type;
  delete normalized.type;

  return normalized;
};

const getCategories = async (req, res) => {
  try {
    const tenantId = resolveTenantIdFromRequest(req);
    if (!tenantId) {
      return jsonError(res, 401, 'Tenant tidak valid');
    }
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
