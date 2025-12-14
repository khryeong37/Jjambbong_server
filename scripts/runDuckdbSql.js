#!/usr/bin/env node
/* eslint-disable no-console */

const fs = require('fs');
const path = require('path');
const duckdb = require('duckdb');
const dotenv = require('dotenv');

dotenv.config({ path: path.resolve(__dirname, '../.env') });

const args = parseArgs(process.argv.slice(2));
const dbPath = path.resolve(args.db || process.env.DUCKDB_PATH || path.join(__dirname, '../data/swap.duckdb'));
const sqlDir = requirePathArgument(
  args.sqlDir || process.env.DUCKDB_SQL_DIR,
  'SQL directory (--sqlDir or DUCKDB_SQL_DIR)'
);
const outputDir = path.resolve(args.out || path.join(__dirname, '../data/sql_results'));

async function main() {
  await fs.promises.mkdir(outputDir, { recursive: true });

  const files = (await fs.promises.readdir(sqlDir))
    .filter((file) => file.toLowerCase().endsWith('.sql'))
    .sort();

  if (!files.length) {
    console.warn(`[DuckDB] No SQL files found in ${sqlDir}`);
    return;
  }

  const params = args.params || {};

  const database = new duckdb.Database(dbPath);
  const connection = database.connect();

  try {
    for (const fileName of files) {
      const filePath = path.join(sqlDir, fileName);
      const query = await fs.promises.readFile(filePath, 'utf8');
      const finalQuery = applyParams(query, params);
      const rows = await runQuery(connection, finalQuery);

      const outputPath = path.join(outputDir, `${path.basename(fileName, '.sql')}.json`);
      await fs.promises.writeFile(outputPath, JSON.stringify(rows, null, 2), 'utf8');
      console.log(`[DuckDB] ${fileName}: ${rows.length} rows -> ${outputPath}`);
    }
  } finally {
    connection.close();
  }
}

main().catch((error) => {
  console.error('DuckDB query execution failed:', error);
  process.exit(1);
});

function parseArgs(argv) {
  const result = { params: {} };
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) continue;

    if (token === '--param' || token === '--params') {
      const next = argv[++i];
      if (!next) throw new Error('Missing value for --param key=value pair');
      addParam(result.params, next);
      continue;
    }

    const eqIdx = token.indexOf('=');
    const key = token.slice(2, eqIdx === -1 ? undefined : eqIdx);
    if (key === 'param') {
      const value = eqIdx === -1 ? argv[++i] : token.slice(eqIdx + 1);
      if (!value) throw new Error('Missing value for --param');
      addParam(result.params, value);
      continue;
    }

    const value = eqIdx === -1 ? argv[++i] : token.slice(eqIdx + 1);
    if (value === undefined) {
      throw new Error(`Missing value for argument ${token}`);
    }
    result[key] = value;
  }
  return result;
}

function addParam(container, pair) {
  const [key, ...rest] = pair.split('=');
  if (!key || !rest.length) {
    throw new Error(`Parameter must be in key=value format. Received: ${pair}`);
  }
  container[key] = coerceValue(rest.join('='));
}

function requirePathArgument(value, label) {
  if (!value) {
    throw new Error(`Cannot continue without ${label}.`);
  }
  return path.resolve(value);
}

function applyParams(sql, params) {
  if (!params || Object.keys(params).length === 0) return sql;
  return sql.replace(/:([A-Za-z0-9_]+)/g, (match, key, offset, src) => {
    const prevChar = offset > 0 ? src[offset - 1] : '';
    if (prevChar === ':') {
      return match; // part of ::type or similar
    }
    if (!(key in params)) {
      throw new Error(`Missing value for parameter :${key}`);
    }
    return toSqlLiteral(params[key]);
  });
}

function toSqlLiteral(value) {
  if (value === null) return 'NULL';
  if (typeof value === 'number' && Number.isFinite(value)) return String(value);
  if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE';
  const str = String(value);
  return `'${str.replace(/'/g, "''")}'`;
}

function coerceValue(value) {
  if (value === undefined || value === null) return null;
  const trimmed = String(value).trim();
  if (!trimmed) return null;
  const num = Number(trimmed);
  if (Number.isFinite(num)) return num;
  if (trimmed.toLowerCase() === 'true') return true;
  if (trimmed.toLowerCase() === 'false') return false;
  return trimmed;
}

function runQuery(connection, sql) {
  return new Promise((resolve, reject) => {
    connection.all(sql, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}
