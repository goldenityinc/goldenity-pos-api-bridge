const test = require('node:test');
const assert = require('node:assert/strict');

const { listRecords } = require('../src/controllers/recordsController');

const buildMockTenantDb = () => {
  const columnsByTable = {
    sales_records: ['id', 'tenant_id', 'status', 'order_status', 'is_void', 'items_json', 'total_profit'],
    sales_record_items: [
      'id',
      'tenant_id',
      'sales_record_id',
      'product_id',
      'product_name',
      'qty',
      'custom_price',
      'note',
      'is_service',
      'product_type',
    ],
    products: ['id', 'tenant_id', 'purchase_price'],
  };

  return {
    async query(sql, values = []) {
      const normalizedSql = sql.replace(/\s+/g, ' ').trim().toLowerCase();

      if (normalizedSql.includes('from information_schema.table_constraints')) {
        return {
          rows: [{ column_name: 'id' }],
          rowCount: 1,
        };
      }

      if (normalizedSql.includes('from information_schema.columns')) {
        const tableName = (values[0] || '').toString();
        const tableColumns = columnsByTable[tableName] || [];

        if (values.length >= 2 && values[1] === 'tenant_id') {
          return {
            rows: tableColumns.includes('tenant_id') ? [{ '?column?': 1 }] : [],
            rowCount: tableColumns.includes('tenant_id') ? 1 : 0,
          };
        }

        return {
          rows: tableColumns.map((columnName) => ({
            column_name: columnName,
            data_type: columnName === 'items_json' ? 'jsonb' : 'text',
            udt_name: columnName === 'items_json' ? 'jsonb' : 'text',
          })),
          rowCount: tableColumns.length,
        };
      }

      if (normalizedSql.startsWith('select') && normalizedSql.includes('from "sales_records"')) {
        return {
          rows: [
            {
              id: 10,
              tenant_id: 'tenant-a',
              status: null,
              order_status: 'VOIDED',
              is_void: true,
              items_json: JSON.stringify([
                {
                  product_id: 'svc-1',
                  product_name: 'Mechanic Service',
                  qty: 1,
                  custom_price: 125000,
                  is_service: true,
                  product_type: 'service',
                },
              ]),
              total_profit: 0,
            },
          ],
          rowCount: 1,
        };
      }

      if (normalizedSql.startsWith('select') && normalizedSql.includes('from sales_record_items')) {
        return {
          rows: [
            {
              sales_record_id: 10,
              product_id: 'svc-1',
              product_name: 'Mechanic Service',
              qty: 1,
              custom_price: 125000,
              note: 'Commissioned service',
              is_service: true,
              product_type: 'service',
            },
          ],
          rowCount: 1,
        };
      }

      if (normalizedSql.startsWith('select id::text as id, purchase_price from products')) {
        return {
          rows: [],
          rowCount: 0,
        };
      }

      throw new Error(`Unexpected SQL in test: ${sql}`);
    },
  };
};

const buildMockRes = () => {
  const headers = {};

  return {
    statusCode: 200,
    jsonBody: null,
    set(values) {
      Object.assign(headers, values || {});
    },
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(body) {
      this.jsonBody = body;
      return this;
    },
    get headers() {
      return headers;
    },
  };
};

test('GET /records/sales_records preserves critical fields and bypasses cache headers', async () => {
  const req = {
    params: { table: 'sales_records' },
    query: {},
    user: { tenantId: 'tenant-a' },
    tenantDb: buildMockTenantDb(),
  };
  const res = buildMockRes();

  await listRecords(req, res);

  assert.equal(res.statusCode, 200);
  assert.equal(res.jsonBody?.success, true);
  assert.ok(Array.isArray(res.jsonBody?.data));
  assert.equal(res.jsonBody.data.length, 1);

  const tx = res.jsonBody.data[0];
  assert.equal(tx.status, 'VOIDED');
  assert.equal(tx.order_status, 'VOIDED');
  assert.equal(tx.is_void, true);

  assert.ok(Array.isArray(tx.items));
  assert.equal(tx.items.length, 1);
  assert.equal(tx.items[0].is_service, true);
  assert.equal(tx.items[0].product_type, 'service');
  assert.equal(tx.items[0].custom_price, 125000);

  assert.equal(res.headers['Cache-Control'], 'no-store, no-cache, must-revalidate, proxy-revalidate');
  assert.equal(res.headers.Pragma, 'no-cache');
  assert.equal(res.headers.Expires, '0');
  assert.equal(res.headers['Surrogate-Control'], 'no-store');
  assert.equal(res.headers['X-Bridge-Cache'], 'bypass');
});
