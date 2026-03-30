const SUBSCRIPTION_ADDON_VALUES = Object.freeze(['service_note']);

const SUBSCRIPTION_ADDON_SET = new Set(SUBSCRIPTION_ADDON_VALUES);

function normalizeSubscriptionAddons(addons) {
  if (!Array.isArray(addons)) {
    return [];
  }

  const normalized = addons
    .filter((item) => typeof item === 'string')
    .map((item) => item.trim())
    .filter((item) => SUBSCRIPTION_ADDON_SET.has(item));

  return Array.from(new Set(normalized));
}

module.exports = {
  SUBSCRIPTION_ADDON_VALUES,
  normalizeSubscriptionAddons,
};
