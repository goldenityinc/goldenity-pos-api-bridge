const express = require('express');
const cors = require('cors');
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY,
);

const jsonOk = (res, data, message = 'Success', status = 200) => {
  res.status(status).json({ success: true, message, data });
};

const jsonError = (res, status, message, error) => {
  res.status(status).json({
    success: false,
    message,
    error: error || null,
  });
};

const toBool = (value, fallback = false) => {
  if (value === undefined) return fallback;
  if (typeof value === 'boolean') return value;
  return value.toString().toLowerCase() == 'true';
};

const normalizeArray = (value) => {
  if (Array.isArray(value)) return value.filter((item) => item !== '');
  if (value === undefined || value === null || value === '') return [];
  return value
    .toString()
    .split(',')
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
};

const parseQueryValue = (value) => {
  if (value === 'true') return true;
  if (value === 'false') return false;
  if (value === 'null') return null;
  if (value !== '' && !Number.isNaN(Number(value))) {
    return Number(value);
  }
  return value;
};

const applyFilters = (builder, query = {}) => {
  let current = builder;
  const orderBy = query.orderBy || query.order;
  const ascending = toBool(query.ascending, true);
  const limit = query.limit ? Number(query.limit) : null;

  for (const [key, rawValue] of Object.entries(query)) {
    if (rawValue === undefined) continue;

    if (key.startsWith('eq__')) {
      current = current.eq(key.slice(4), parseQueryValue(rawValue));
    } else if (key.startsWith('neq__')) {
      current = current.neq(key.slice(5), parseQueryValue(rawValue));
    } else if (key.startsWith('gte__')) {
      current = current.gte(key.slice(5), rawValue);
    } else if (key.startsWith('lte__')) {
      current = current.lte(key.slice(5), rawValue);
    } else if (key.startsWith('ilike__')) {
      current = current.ilike(key.slice(7), rawValue);
    } else if (key.startsWith('in__')) {
      current = current.in(key.slice(4), normalizeArray(rawValue));
    }
  }

  if (orderBy) {
    current = current.order(orderBy, { ascending });
  }

  if (limit && Number.isFinite(limit) && limit > 0) {
    current = current.limit(limit);
  }

  return current;
};

const runSelect = async (table, query = {}) => {
  const select = query.select || '*';
  let request = supabase.from(table).select(select);
  request = applyFilters(request, query);

  if (toBool(query.single)) {
    return request.single();
  }

  if (toBool(query.maybeSingle)) {
    return request.maybeSingle();
  }

  return request;
};

const parseBodyArray = (body) => {
  if (Array.isArray(body)) return body;
  if (Array.isArray(body?.data)) return body.data;
  return null;
};

const parseBodyObject = (body) => {
  if (body && typeof body === 'object' && !Array.isArray(body)) {
    return body.data && typeof body.data === 'object' && !Array.isArray(body.data)
      ? body.data
      : body;
  }
  return {};
};

const createCrudRouter = (table) => {
  const router = express.Router();

  router.get('/', async (req, res) => {
    try {
      const result = await runSelect(table, req.query);
      if (result.error) {
        return jsonError(res, 500, result.error.message, result.error.message);
      }
      return jsonOk(res, result.data);
    } catch (error) {
      return jsonError(res, 500, error.message || 'Internal server error', error.message);
    }
  });

  router.post('/', async (req, res) => {
    try {
      const arrayPayload = parseBodyArray(req.body);
      const payload = arrayPayload || parseBodyObject(req.body);
      const result = await supabase.from(table).insert(payload).select();
      if (result.error) {
        return jsonError(res, 500, result.error.message, result.error.message);
      }
      return jsonOk(res, arrayPayload ? result.data : (result.data?.[0] || null), 'Created', 201);
    } catch (error) {
      return jsonError(res, 500, error.message || 'Internal server error', error.message);
    }
  });

  router.put('/:id', async (req, res) => {
    try {
      const idField = req.query.idField || 'id';
      const payload = parseBodyObject(req.body);
      const result = await supabase
        .from(table)
        .update(payload)
        .eq(idField, req.params.id)
        .select();
      if (result.error) {
        return jsonError(res, 500, result.error.message, result.error.message);
      }
      return jsonOk(res, result.data?.[0] || null, 'Updated');
    } catch (error) {
      return jsonError(res, 500, error.message || 'Internal server error', error.message);
    }
  });

  router.delete('/:id', async (req, res) => {
    try {
      const idField = req.query.idField || 'id';
      const result = await supabase
        .from(table)
        .delete()
        .eq(idField, req.params.id)
        .select();
      if (result.error) {
        return jsonError(res, 500, result.error.message, result.error.message);
      }
      return jsonOk(res, result.data?.[0] || null, 'Deleted');
    } catch (error) {
      return jsonError(res, 500, error.message || 'Internal server error', error.message);
    }
  });

  return router;
};

app.post('/auth/login', async (req, res) => {
  try {
    const { username, password } = req.body;

    const { data, error } = await supabase
      .from('app_users')
      .select('*')
      .eq('username', username)
      .eq('password', password)
      .single();

    if (error || !data) {
      return jsonError(res, 401, 'Login gagal', error?.message || 'Unauthorized');
    }

    return jsonOk(res, {
      user: data,
      token: 'dummy-jwt-token-for-now',
    });
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
});

app.get('/products', async (req, res) => {
  try {
    const result = await runSelect('products', req.query);
    if (result.error) {
      return jsonError(res, 500, result.error.message, result.error.message);
    }
    return jsonOk(res, result.data);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
});

app.get('/categories', async (req, res) => {
  try {
    const result = await runSelect('categories', req.query);
    if (result.error) {
      return jsonError(res, 500, result.error.message, result.error.message);
    }
    return jsonOk(res, result.data);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
});

app.post('/transactions', async (req, res) => {
  try {
    const payload = { ...req.body };

    const { data, error } = await supabase
      .from('sales_records')
      .insert(payload)
      .select()
      .single();

    if (error) {
      return jsonError(res, 500, error.message, error.message);
    }

    return jsonOk(res, data, 'Transaction saved', 201);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
});

app.post('/sync', async (req, res) => {
  try {
    const { table, action, data, id } = req.body;

    if (!table || !action) {
      return jsonError(res, 400, 'table dan action wajib diisi');
    }

    if (action === 'INSERT') {
      const payload = { ...(data || {}) };
      if (typeof payload.id === 'string') {
        delete payload.id;
      }

      const result = await supabase.from(table).insert(payload).select();
      if (result.error) {
        return jsonError(res, 500, result.error.message, result.error.message);
      }

      return jsonOk(res, result.data, 'Sync insert success', 201);
    }

    if (action === 'UPDATE') {
      const result = await supabase.from(table).update(data || {}).eq('id', id).select();
      if (result.error) {
        return jsonError(res, 500, result.error.message, result.error.message);
      }

      return jsonOk(res, result.data, 'Sync update success');
    }

    if (action === 'DELETE') {
      const result = await supabase.from(table).delete().eq('id', id).select();
      if (result.error) {
        return jsonError(res, 500, result.error.message, result.error.message);
      }

      return jsonOk(res, result.data, 'Sync delete success');
    }

    return jsonError(res, 400, `Action tidak didukung: ${action}`);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
});

app.use('/users', createCrudRouter('app_users'));
app.use('/suppliers', createCrudRouter('suppliers'));
app.use('/restock_history', createCrudRouter('restock_history'));
app.use('/order_history', createCrudRouter('order_history'));

app.get('/order_history/items', async (req, res) => {
  try {
    const result = await runSelect('order_history_items', req.query);
    if (result.error) {
      return jsonError(res, 500, result.error.message, result.error.message);
    }
    return jsonOk(res, result.data);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
});

app.post('/order_history/items', async (req, res) => {
  try {
    const payload = parseBodyArray(req.body) || parseBodyObject(req.body);
    const result = await supabase.from('order_history_items').insert(payload).select();
    if (result.error) {
      return jsonError(res, 500, result.error.message, result.error.message);
    }
    return jsonOk(res, result.data, 'Created', 201);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
});

app.put('/order_history/items/archive', async (req, res) => {
  try {
    const { archiveAll = false, productIds = [], manualItemIds = [] } = req.body || {};

    if (archiveAll) {
      const result = await supabase
        .from('order_history_items')
        .update({ is_archived: true })
        .eq('is_archived', false)
        .select();
      if (result.error) {
        return jsonError(res, 500, result.error.message, result.error.message);
      }
      return jsonOk(res, result.data, 'Archived');
    }

    const archived = [];
    const productIdList = normalizeArray(productIds);
    const manualIdList = normalizeArray(manualItemIds);

    if (productIdList.length > 0) {
      const result = await supabase
        .from('order_history_items')
        .update({ is_archived: true })
        .in('product_id', productIdList.map(parseQueryValue))
        .eq('is_archived', false)
        .select();
      if (result.error) {
        return jsonError(res, 500, result.error.message, result.error.message);
      }
      archived.push(...(result.data || []));
    }

    if (manualIdList.length > 0) {
      const result = await supabase
        .from('order_history_items')
        .update({ is_archived: true })
        .in('manual_item_id', manualIdList)
        .eq('is_archived', false)
        .select();
      if (result.error) {
        return jsonError(res, 500, result.error.message, result.error.message);
      }
      archived.push(...(result.data || []));
    }

    return jsonOk(res, archived, 'Archived');
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
});

app.get('/records/:table', async (req, res) => {
  try {
    const result = await runSelect(req.params.table, req.query);
    if (result.error) {
      return jsonError(res, 500, result.error.message, result.error.message);
    }
    return jsonOk(res, result.data);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
});

app.post('/records/:table', async (req, res) => {
  try {
    const arrayPayload = parseBodyArray(req.body);
    const payload = arrayPayload || parseBodyObject(req.body);
    const result = await supabase.from(req.params.table).insert(payload).select();
    if (result.error) {
      return jsonError(res, 500, result.error.message, result.error.message);
    }
    return jsonOk(res, result.data, 'Created', 201);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
});

app.post('/records/:table/upsert', async (req, res) => {
  try {
    const payload = parseBodyArray(req.body) || parseBodyObject(req.body);
    const onConflict = req.body?.onConflict;
    const result = await supabase
      .from(req.params.table)
      .upsert(payload, onConflict ? { onConflict } : undefined)
      .select();
    if (result.error) {
      return jsonError(res, 500, result.error.message, result.error.message);
    }
    return jsonOk(res, result.data, 'Upserted');
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
});

app.put('/records/:table/:id', async (req, res) => {
  try {
    const idField = req.query.idField || 'id';
    const payload = parseBodyObject(req.body);
    const result = await supabase
      .from(req.params.table)
      .update(payload)
      .eq(idField, req.params.id)
      .select();
    if (result.error) {
      return jsonError(res, 500, result.error.message, result.error.message);
    }
    return jsonOk(res, result.data?.[0] || null, 'Updated');
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
});

app.delete('/records/:table/:id', async (req, res) => {
  try {
    const idField = req.query.idField || 'id';
    const result = await supabase
      .from(req.params.table)
      .delete()
      .eq(idField, req.params.id)
      .select();
    if (result.error) {
      return jsonError(res, 500, result.error.message, result.error.message);
    }
    return jsonOk(res, result.data?.[0] || null, 'Deleted');
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
});

app.post('/storage/upload', async (req, res) => {
  try {
    const { bucket, fileName, base64, contentType = 'application/octet-stream', upsert = true } = req.body || {};

    if (!bucket || !fileName || !base64) {
      return jsonError(res, 400, 'bucket, fileName, dan base64 wajib diisi');
    }

    const bytes = Buffer.from(base64, 'base64');
    const uploadResult = await supabase.storage.from(bucket).upload(fileName, bytes, {
      contentType,
      upsert: toBool(upsert, true),
    });

    if (uploadResult.error) {
      return jsonError(res, 500, uploadResult.error.message, uploadResult.error.message);
    }

    const publicUrl = supabase.storage.from(bucket).getPublicUrl(fileName).data.publicUrl;
    return jsonOk(res, { path: uploadResult.data?.path || fileName, url: publicUrl }, 'Uploaded', 201);
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
});

app.delete('/storage/:bucket/:fileName', async (req, res) => {
  try {
    const result = await supabase.storage.from(req.params.bucket).remove([req.params.fileName]);
    if (result.error) {
      return jsonError(res, 500, result.error.message, result.error.message);
    }
    return jsonOk(res, result.data, 'Deleted');
  } catch (error) {
    return jsonError(res, 500, error.message || 'Internal server error', error.message);
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Goldenity API Bridge running on port ${PORT}`);
});
