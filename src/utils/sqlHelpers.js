const toBool = (value, fallback = false) => {
  if (value === undefined) return fallback;
  if (typeof value === 'boolean') return value;
  return value.toString().toLowerCase() === 'true';
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

const isValidIdentifier = (value) => /^[A-Za-z_][A-Za-z0-9_]*$/.test(value);

const quoteIdentifier = (value, label = 'identifier') => {
  if (!isValidIdentifier(value)) {
    throw new Error(`Invalid ${label}: ${value}`);
  }
  return `"${value}"`;
};

const parseSelectClause = (selectValue) => {
  if (!selectValue || selectValue === '*') {
    return '*';
  }

  const columns = selectValue
    .toString()
    .split(',')
    .map((column) => column.trim())
    .filter(Boolean);

  if (columns.length === 0) {
    return '*';
  }

  return columns.map((column) => quoteIdentifier(column, 'column')).join(', ');
};

const createWhereBuilder = (query = {}, startingIndex = 1) => {
  const clauses = [];
  const values = [];
  let parameterIndex = startingIndex;

  for (const [key, rawValue] of Object.entries(query)) {
    if (rawValue === undefined) continue;

    if (key.startsWith('eq__')) {
      const field = quoteIdentifier(key.slice(4), 'column');
      values.push(parseQueryValue(rawValue));
      clauses.push(`${field} = $${parameterIndex}`);
      parameterIndex += 1;
    } else if (key.startsWith('neq__')) {
      const field = quoteIdentifier(key.slice(5), 'column');
      values.push(parseQueryValue(rawValue));
      clauses.push(`${field} <> $${parameterIndex}`);
      parameterIndex += 1;
    } else if (key.startsWith('gte__')) {
      const field = quoteIdentifier(key.slice(5), 'column');
      values.push(parseQueryValue(rawValue));
      clauses.push(`${field} >= $${parameterIndex}`);
      parameterIndex += 1;
    } else if (key.startsWith('lte__')) {
      const field = quoteIdentifier(key.slice(5), 'column');
      values.push(parseQueryValue(rawValue));
      clauses.push(`${field} <= $${parameterIndex}`);
      parameterIndex += 1;
    } else if (key.startsWith('ilike__')) {
      const field = quoteIdentifier(key.slice(7), 'column');
      values.push(rawValue);
      clauses.push(`${field} ILIKE $${parameterIndex}`);
      parameterIndex += 1;
    } else if (key.startsWith('in__')) {
      const field = quoteIdentifier(key.slice(4), 'column');
      const items = normalizeArray(rawValue).map(parseQueryValue);
      if (items.length === 0) {
        clauses.push('1 = 0');
      } else {
        values.push(items);
        clauses.push(`${field} = ANY($${parameterIndex})`);
        parameterIndex += 1;
      }
    }
  }

  return { clauses, values, nextIndex: parameterIndex };
};

const buildSelectQuery = (table, query = {}) => {
  const tableName = quoteIdentifier(table, 'table');
  const selectClause = parseSelectClause(query.select);
  const { clauses, values, nextIndex } = createWhereBuilder(query);
  let sql = `SELECT ${selectClause} FROM ${tableName}`;
  let parameterIndex = nextIndex;

  if (clauses.length > 0) {
    sql += ` WHERE ${clauses.join(' AND ')}`;
  }

  const orderBy = query.orderBy || query.order;
  if (orderBy) {
    const orderColumn = quoteIdentifier(orderBy, 'column');
    const orderDirection = toBool(query.ascending, true) ? 'ASC' : 'DESC';
    sql += ` ORDER BY ${orderColumn} ${orderDirection}`;
  }

  const limit = query.limit ? Number(query.limit) : null;
  if (limit && Number.isFinite(limit) && limit > 0) {
    values.push(limit);
    sql += ` LIMIT $${parameterIndex}`;
  }

  return { sql, values };
};

const buildInsertQuery = (table, payload) => {
  const rows = Array.isArray(payload) ? payload : [payload];

  if (rows.length === 0) {
    throw new Error('Payload cannot be empty');
  }

  const columnSet = new Set();
  for (const row of rows) {
    Object.keys(row || {}).forEach((key) => columnSet.add(key));
  }

  const columns = Array.from(columnSet);
  if (columns.length === 0) {
    throw new Error('Payload must contain at least one field');
  }

  const quotedColumns = columns.map((column) => quoteIdentifier(column, 'column'));
  const values = [];
  const valueGroups = rows.map((row, rowIndex) => {
    const placeholders = columns.map((column, columnIndex) => {
      values.push(row?.[column] ?? null);
      return `$${rowIndex * columns.length + columnIndex + 1}`;
    });
    return `(${placeholders.join(', ')})`;
  });

  return {
    sql: `INSERT INTO ${quoteIdentifier(table, 'table')} (${quotedColumns.join(', ')}) VALUES ${valueGroups.join(', ')} RETURNING *`,
    values,
  };
};

const buildUpdateQuery = (table, payload, idField, idValue) => {
  const entries = Object.entries(payload || {});
  if (entries.length === 0) {
    throw new Error('Payload must contain at least one field');
  }

  const values = [];
  const setClauses = entries.map(([key, value], index) => {
    values.push(value);
    return `${quoteIdentifier(key, 'column')} = $${index + 1}`;
  });

  values.push(parseQueryValue(idValue));

  return {
    sql: `UPDATE ${quoteIdentifier(table, 'table')} SET ${setClauses.join(', ')} WHERE ${quoteIdentifier(idField, 'column')} = $${values.length} RETURNING *`,
    values,
  };
};

const buildDeleteQuery = (table, idField, idValue) => ({
  sql: `DELETE FROM ${quoteIdentifier(table, 'table')} WHERE ${quoteIdentifier(idField, 'column')} = $1 RETURNING *`,
  values: [parseQueryValue(idValue)],
});

const buildUpsertQuery = (table, payload, onConflictValue) => {
  const rows = Array.isArray(payload) ? payload : [payload];
  const conflictColumns = normalizeArray(onConflictValue);

  if (conflictColumns.length === 0) {
    throw new Error('onConflict is required for upsert');
  }

  const insertQuery = buildInsertQuery(table, rows);
  const allColumns = Array.from(
    new Set(rows.flatMap((row) => Object.keys(row || {}))),
  );
  const nonConflictColumns = allColumns.filter((column) => !conflictColumns.includes(column));
  const quotedConflictColumns = conflictColumns.map((column) => quoteIdentifier(column, 'column')).join(', ');
  const updateClause = nonConflictColumns.length > 0
    ? nonConflictColumns
      .map((column) => `${quoteIdentifier(column, 'column')} = EXCLUDED.${quoteIdentifier(column, 'column')}`)
      .join(', ')
    : 'NOTHING';

  return {
    sql: `${insertQuery.sql.replace(/ RETURNING \*$/, '')} ON CONFLICT (${quotedConflictColumns}) DO ${updateClause === 'NOTHING' ? 'NOTHING' : `UPDATE SET ${updateClause}`} RETURNING *`,
    values: insertQuery.values,
  };
};

const runSelect = async (tenantDb, table, query = {}) => {
  const { sql, values } = buildSelectQuery(table, query);
  const result = await tenantDb.query(sql, values);

  if (toBool(query.single)) {
    if (result.rows.length !== 1) {
      throw new Error(result.rows.length === 0 ? 'No rows found' : 'Multiple rows found');
    }
    return result.rows[0];
  }

  if (toBool(query.maybeSingle)) {
    if (result.rows.length > 1) {
      throw new Error('Multiple rows found');
    }
    return result.rows[0] || null;
  }

  return result.rows;
};

module.exports = {
  normalizeArray,
  parseQueryValue,
  parseBodyArray,
  parseBodyObject,
  buildInsertQuery,
  buildUpdateQuery,
  buildDeleteQuery,
  buildUpsertQuery,
  runSelect,
};
