const { Client } = require('pg');

const DROP_SQL = `
DROP TABLE IF EXISTS
  db_tpp,
  app_users,
  app_versions,
  categories,
  daily_cash,
  expenses,
  manual_shopping_list,
  order_history,
  order_history_items,
  products,
  sales_records,
  saved_carts,
  store_settings,
  suppliers;
`;

const buildTenantDbUrl = (masterUrl) => {
  try {
    const url = new URL(masterUrl);
    url.pathname = '/db_tpp';
    return url.toString();
  } catch (error) {
    throw new Error('MASTER_DB_URL tidak valid');
  }
};

const maskUrl = (rawUrl) => {
  try {
    const url = new URL(rawUrl);
    if (url.password) {
      url.password = '***';
    }
    return url.toString();
  } catch (_error) {
    return '<invalid-url>';
  }
};

const run = async () => {
  const firstArg = process.argv[2];
  if (firstArg === '--help' || firstArg === '-h') {
    console.log('Usage:');
    console.log('  node scripts/railway_cleanup_and_provision.js "MASTER_DB_URL" "Tenant Name"');
    console.log('  npm run railway:fix-tenant -- "MASTER_DB_URL" "Tenant Name"');
    console.log('Env alternatives: MASTER_DB_URL, TENANT_NAME');
    return;
  }

  const masterDbUrl = firstArg || process.env.MASTER_DB_URL;
  const tenantName = process.argv[3] || process.env.TENANT_NAME || 'Tanto Pink Putra';

  if (!masterDbUrl) {
    throw new Error('Gunakan: node scripts/railway_cleanup_and_provision.js "MASTER_DB_URL" "Tenant Name"');
  }

  const tenantDbUrl = buildTenantDbUrl(masterDbUrl);

  console.log('Master DB:', maskUrl(masterDbUrl));
  console.log('Tenant DB:', maskUrl(tenantDbUrl));

  const master = new Client({ connectionString: masterDbUrl, ssl: { rejectUnauthorized: false } });

  await master.connect();

  try {
    console.log('1) Dropping misplaced POS tables in master DB...');
    await master.query(DROP_SQL);

    console.log('2) Creating db_tpp database (skip if already exists)...');
    try {
      await master.query('CREATE DATABASE db_tpp');
      console.log('   db_tpp created.');
    } catch (error) {
      if (error.code === '42P04') {
        console.log('   db_tpp already exists, skipping.');
      } else {
        throw error;
      }
    }

    console.log('3) Updating tenants.db_connection_url in master DB...');
    const updateResult = await master.query(
      'UPDATE tenants SET db_connection_url = $1 WHERE name = $2 RETURNING id, name, db_connection_url',
      [tenantDbUrl, tenantName],
    );

    if (updateResult.rowCount === 0) {
      console.log('   WARNING: tenant row not found. Update manually in Railway Data tab.');
    } else {
      console.log(`   Updated ${updateResult.rowCount} tenant row(s).`);
    }

    console.log('Done. Next step:');
    console.log(`node setup_tenant_db.js "${tenantDbUrl}"`);
  } finally {
    await master.end();
  }
};

run().catch((error) => {
  console.error('FAILED:', error.message);
  process.exit(1);
});
