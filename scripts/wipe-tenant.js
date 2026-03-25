require('dotenv').config();

const { PrismaClient } = require('@prisma/client');

const EXPECTED_TENANT_ID = '96bfc081-2ce8-4a0e-be90-c052f499bed5';

function getCliArg(prefix) {
  return process.argv.find((arg) => arg.startsWith(prefix))?.replace(prefix, '');
}

function getTenantId() {
  return (getCliArg('--tenant-id=') || EXPECTED_TENANT_ID).trim();
}

function getDatabaseUrl() {
  return (
    getCliArg('--db-url=') ||
    process.env.DATABASE_URL_TPP ||
    process.env.DATABASE_URL ||
    ''
  ).trim();
}

function quoteIdentifier(identifier) {
  return `"${String(identifier).replace(/"/g, '""')}"`;
}

async function listPublicTables(prisma) {
  const rows = await prisma.$queryRawUnsafe(`
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name
  `);
  return new Set((rows || []).map((row) => row.table_name));
}

async function countRows(prisma, tableName, whereClause = '') {
  const sql = `SELECT COUNT(*)::int AS count FROM ${quoteIdentifier(tableName)}${whereClause ? ` WHERE ${whereClause}` : ''}`;
  const rows = await prisma.$queryRawUnsafe(sql);
  return rows?.[0]?.count ?? 0;
}

function buildDeleteOperation(prisma, definition, availableTables) {
  if (definition.kind === 'model') {
    const delegate = prisma[definition.delegate];
    if (!delegate || typeof delegate.deleteMany !== 'function') {
      return null;
    }

    return {
      label: definition.label,
      target: definition.modelName,
      promise: delegate.deleteMany(definition.where ? { where: definition.where } : {}),
      getCount: (result) => result?.count ?? 0,
    };
  }

  if (!availableTables.has(definition.tableName)) {
    return null;
  }

  const sql = `DELETE FROM ${quoteIdentifier(definition.tableName)}${definition.whereClause ? ` WHERE ${definition.whereClause}` : ''}`;
  return {
    label: definition.label,
    target: definition.tableName,
    promise: prisma.$executeRawUnsafe(sql),
    getCount: (result) => Number(result || 0),
  };
}

async function main() {
  const tenantId = getTenantId();
  const databaseUrl = getDatabaseUrl();

  if (!databaseUrl) {
    throw new Error('Database URL tidak ditemukan. Kirim --db-url=<TENANT_DB_URL> atau isi DATABASE_URL_TPP.');
  }

  if (tenantId !== EXPECTED_TENANT_ID) {
    throw new Error(`Script sementara ini hanya diizinkan untuk tenant ${EXPECTED_TENANT_ID}. Tenant yang diterima: ${tenantId}`);
  }

  const prisma = new PrismaClient({
    datasources: {
      db: {
        url: databaseUrl,
      },
    },
  });

  try {
    const availableTables = await listPublicTables(prisma);

    const operationDefinitions = [
      {
        kind: 'raw',
        label: 'OrderItem',
        tableName: 'order_history_items',
      },
      {
        kind: 'raw',
        label: 'ManualShoppingList',
        tableName: 'manual_shopping_list',
      },
      {
        kind: 'raw',
        label: 'SavedCart',
        tableName: 'saved_carts',
      },
      {
        kind: 'raw',
        label: 'Order',
        tableName: 'order_history',
      },
      {
        kind: 'raw',
        label: 'TransactionOrReceivable',
        tableName: 'sales_records',
      },
      {
        kind: 'raw',
        label: 'Expense',
        tableName: 'expenses',
      },
      {
        kind: 'raw',
        label: 'DailyCash',
        tableName: 'daily_cash',
      },
      {
        kind: 'raw',
        label: 'PettyCashLog',
        tableName: 'petty_cash_logs',
        whereClause: `tenant_id = '${tenantId.replace(/'/g, "''")}'`,
      },
      {
        kind: 'model',
        label: 'Product',
        modelName: 'Product',
        delegate: 'product',
      },
      {
        kind: 'raw',
        label: 'Category',
        tableName: 'categories',
      },
      {
        kind: 'model',
        label: 'Customer',
        modelName: 'Customer',
        delegate: 'customer',
      },
      {
        kind: 'raw',
        label: 'Supplier',
        tableName: 'suppliers',
      },
      {
        kind: 'raw',
        label: 'SyncQueueOrOfflineQueue',
        tableName: 'sync_queue',
      },
      {
        kind: 'raw',
        label: 'OfflineQueueFallback',
        tableName: 'offline_queue',
      },
    ];

    const operations = operationDefinitions
      .map((definition) => buildDeleteOperation(prisma, definition, availableTables))
      .filter(Boolean);

    const skippedDefinitions = operationDefinitions.filter((definition) => {
      if (definition.kind === 'model') {
        const delegate = prisma[definition.delegate];
        return !delegate || typeof delegate.deleteMany !== 'function';
      }
      return !availableTables.has(definition.tableName);
    });

    if (operations.length === 0) {
      console.log('Tidak ada tabel/model operasional yang cocok untuk dihapus.');
      return;
    }

    console.log(`Mulai wipe tenant demo ${tenantId} pada ${databaseUrl} ...`);

    const results = await prisma.$transaction(operations.map((operation) => operation.promise));

    console.log('Wipe selesai. Ringkasan baris terhapus:');
    operations.forEach((operation, index) => {
      console.log(`- ${operation.label} (${operation.target}): ${operation.getCount(results[index])}`);
    });

    if (skippedDefinitions.length > 0) {
      console.log('Target yang dilewati karena tidak tersedia di schema/table tenant saat ini:');
      skippedDefinitions.forEach((definition) => {
        console.log(`- ${definition.label} (${definition.kind === 'model' ? definition.modelName : definition.tableName})`);
      });
    }

    const validationTargets = [
      'order_history_items',
      'manual_shopping_list',
      'saved_carts',
      'order_history',
      'sales_records',
      'expenses',
      'daily_cash',
      'petty_cash_logs',
      'products',
      'categories',
      'customers',
      'suppliers',
    ].filter((tableName) => availableTables.has(tableName));

    console.log('Sisa data setelah wipe:');
    for (const tableName of validationTargets) {
      const whereClause = tableName === 'petty_cash_logs' ? `tenant_id = '${tenantId.replace(/'/g, "''")}'` : '';
      const remaining = await countRows(prisma, tableName, whereClause);
      console.log(`- ${tableName}: ${remaining}`);
    }
  } finally {
    await prisma.$disconnect();
  }
}

main().catch((error) => {
  console.error('Gagal wipe tenant demo:', error.message || error);
  process.exit(1);
});