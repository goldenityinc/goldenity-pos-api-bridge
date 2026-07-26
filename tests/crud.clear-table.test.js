const test = require('node:test');
const assert = require('node:assert/strict');

const { createCrudController } = require('../src/controllers/crudController');

const buildMockRes = () => ({
  statusCode: 200,
  jsonBody: null,
  status(code) {
    this.statusCode = code;
    return this;
  },
  json(body) {
    this.jsonBody = body;
    return this;
  },
});

const buildMockTenantDb = () => {
  const executedSql = [];

  const client = {
    released: false,
    async query(sql, values = []) {
      executedSql.push({
        sql,
        values,
      });

      const normalizedSql = sql.replace(/\s+/g, ' ').trim().toLowerCase();

      if (normalizedSql === 'begin' || normalizedSql === 'commit' || normalizedSql === 'rollback') {
        return { rows: [], rowCount: 0 };
      }

      if (normalizedSql.includes('from information_schema.columns')) {
        const tableName = (values[0] || '').toString();

        if (values.length >= 2 && values[1] === 'tenant_id') {
          return {
            rows: [{ '?column?': 1 }],
            rowCount: 1,
          };
        }

        if (tableName === 'tables') {
          return {
            rows: [
              { column_name: 'id', data_type: 'bigint', udt_name: 'int8' },
              { column_name: 'tenant_id', data_type: 'text', udt_name: 'text' },
              { column_name: 'status', data_type: 'text', udt_name: 'text' },
            ],
            rowCount: 3,
          };
        }

        if (tableName === 'sales_records') {
          return {
            rows: [
              { column_name: 'id', data_type: 'bigint', udt_name: 'int8' },
              { column_name: 'tenant_id', data_type: 'text', udt_name: 'text' },
              { column_name: 'table_id', data_type: 'bigint', udt_name: 'int8' },
              { column_name: 'status', data_type: 'text', udt_name: 'text' },
              { column_name: 'order_status', data_type: 'text', udt_name: 'text' },
              { column_name: 'transaction_status', data_type: 'text', udt_name: 'text' },
              { column_name: 'payment_status', data_type: 'text', udt_name: 'text' },
              { column_name: 'is_void', data_type: 'boolean', udt_name: 'bool' },
              { column_name: 'void_reason', data_type: 'text', udt_name: 'text' },
              { column_name: 'voided_at', data_type: 'timestamp with time zone', udt_name: 'timestamptz' },
              { column_name: 'updated_at', data_type: 'timestamp with time zone', udt_name: 'timestamptz' },
            ],
            rowCount: 11,
          };
        }
      }

      if (normalizedSql.startsWith('select * from "sales_records" where "table_id" = $1')) {
        return {
          rows: [
            {
              id: 101,
              tenant_id: 'tenant-a',
              table_id: 5,
              status: 'OPEN',
              order_status: 'PENDING',
              payment_status: 'UNPAID',
              is_void: false,
            },
            {
              id: 102,
              tenant_id: 'tenant-a',
              table_id: 5,
              status: 'COMPLETED',
              order_status: 'COMPLETED',
              payment_status: 'PAID',
              is_void: false,
            },
          ],
          rowCount: 2,
        };
      }

      if (normalizedSql.startsWith('update "sales_records" set ')) {
        return {
          rows: [
            {
              id: 101,
              tenant_id: 'tenant-a',
              table_id: 5,
              status: 'CANCELLED',
              order_status: 'CANCELLED',
              transaction_status: 'VOID',
              payment_status: 'CANCELLED',
              is_void: true,
              void_reason: 'TABLE_CLEARED',
            },
          ],
          rowCount: 1,
        };
      }

      if (normalizedSql.startsWith('update "tables" set ')) {
        return {
          rows: [
            {
              id: 5,
              tenant_id: 'tenant-a',
              status: 'AVAILABLE',
            },
          ],
          rowCount: 1,
        };
      }

      throw new Error(`Unexpected SQL in test: ${sql}`);
    },
    release() {
      this.released = true;
    },
  };

  return {
    executedSql,
    async query(sql, values = []) {
      return client.query(sql, values);
    },
    async connect() {
      return client;
    },
    get released() {
      return client.released;
    },
  };
};

test('PATCH /tables/:id cancels unpaid linked orders before clearing table', async () => {
  const tenantDb = buildMockTenantDb();
  const controller = createCrudController('tables');
  const req = {
    params: { id: '5' },
    query: {},
    body: { status: 'Available' },
    user: { tenantId: 'tenant-a' },
    tenantDb,
    headers: {},
  };
  const res = buildMockRes();

  await controller.updateById(req, res);

  assert.equal(res.statusCode, 200);
  assert.equal(res.jsonBody?.success, true);
  assert.equal(res.jsonBody?.data?.status, 'AVAILABLE');
  assert.equal(tenantDb.released, true);

  const normalizedSql = tenantDb.executedSql.map((entry) => (
    entry.sql.replace(/\s+/g, ' ').trim().toLowerCase()
  ));

  const cancelIndex = normalizedSql.findIndex((sql) => sql.startsWith('update "sales_records" set '));
  const tableUpdateIndex = normalizedSql.findIndex((sql) => sql.startsWith('update "tables" set '));

  assert.notEqual(cancelIndex, -1);
  assert.notEqual(tableUpdateIndex, -1);
  assert.ok(cancelIndex < tableUpdateIndex, 'sales_records should be cancelled before tables update');

  const cancelQuery = tenantDb.executedSql[cancelIndex];
  assert.deepEqual(cancelQuery.values[8], [101]);
});
