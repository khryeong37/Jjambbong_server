#!/usr/bin/env node
/* eslint-disable no-console */

const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');
const { MongoClient } = require('mongodb');
const duckdb = require('duckdb');
const dotenv = require('dotenv');

dotenv.config({ path: path.resolve(__dirname, '../.env') });

const args = parseArgs(process.argv.slice(2));

const swapCsvPath = requirePathArgument(
  args.swap || process.env.SWAP_CSV_PATH,
  'swap CSV (--swap or SWAP_CSV_PATH)'
);
const atomCsvPath = args.atom || process.env.ATOM_CSV_PATH || null;
const atoneCsvPath = args.atone || process.env.ATONE_CSV_PATH || null;

const duckdbPath =
  args.duckdb || process.env.DUCKDB_PATH || path.resolve(__dirname, '../data/swap.duckdb');

const mongoUri = args.mongo || process.env.MONGODB_URI || null;
const mongoDbName = args.mongoDb || process.env.SWAP_DB_NAME || process.env.MONGODB_DB || null;
const swapCollection = args.swapCollection || process.env.SWAP_COLLECTION_NAME || 'swap_logs';
const atomCollection = args.atomCollection || process.env.ATOM_COLLECTION_NAME || 'atom_base';
const atoneCollection =
  args.atoneCollection || process.env.ATONE_COLLECTION_NAME || 'atone_base';

async function main() {
  console.log('Loading CSV files...');
  const datasets = {
    swap: await loadCsvRecords(swapCsvPath),
    atom: atomCsvPath ? await loadCsvRecords(atomCsvPath) : [],
    atone: atoneCsvPath ? await loadCsvRecords(atoneCsvPath) : [],
  };

  console.log('Rows loaded:', {
    swap: datasets.swap.length,
    atom: datasets.atom.length,
    atone: datasets.atone.length,
  });

  if (mongoUri) {
    await seedMongo(datasets, {
      mongoUri,
      mongoDbName,
      swapCollection,
      atomCollection,
      atoneCollection,
    });
  } else {
    console.warn('[Mongo] MONGODB_URI is not set. Skipping Mongo import.');
  }

  await seedDuckdb({
    duckdbPath,
    swapCsvPath,
    atomCsvPath,
    atoneCsvPath,
  });

  console.log('Data ingestion completed.');
}

main().catch((error) => {
  console.error('Failed to seed databases:', error);
  process.exit(1);
});

function parseArgs(argv) {
  const result = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) continue;
    const next = argv[i + 1];
    if (token.includes('=')) {
      const [key, value] = token.slice(2).split('=');
      result[key] = value;
    } else {
      if (next === undefined) {
        throw new Error(`Missing value for argument ${token}`);
      }
      result[token.slice(2)] = next;
      i += 1;
    }
  }
  return result;
}

function requirePathArgument(value, label) {
  if (!value) {
    throw new Error(`Cannot continue without ${label}.`);
  }
  return value;
}

async function loadCsvRecords(filePath) {
  const absolutePath = path.resolve(filePath);
  const raw = fs.readFileSync(absolutePath, 'utf8');
  const records = parse(raw, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  });
  return records.map((record) => {
    const normalized = {};
    Object.entries(record).forEach(([key, value]) => {
      const cleanKey = key.replace(/^\ufeff/, '').trim();
      normalized[cleanKey] = coerceValue(value);
    });
    return normalized;
  });
}

function coerceValue(value) {
  if (value === undefined || value === null) return null;
  const trimmed = String(value).trim();
  if (!trimmed) return null;
  const num = Number(trimmed);
  return Number.isFinite(num) ? num : trimmed;
}

async function seedMongo(datasets, config) {
  const client = new MongoClient(config.mongoUri);
  await client.connect();
  console.log(`[Mongo] Connected to cluster${config.mongoDbName ? ` (${config.mongoDbName})` : ''}`);
  try {
    const db = config.mongoDbName ? client.db(config.mongoDbName) : client.db();
    await replaceCollection(db, config.swapCollection, datasets.swap);
    await replaceCollection(db, config.atomCollection, datasets.atom);
    await replaceCollection(db, config.atoneCollection, datasets.atone);
  } finally {
    await client.close();
  }
}

async function replaceCollection(db, name, rows) {
  if (!rows.length) {
    console.warn(`[Mongo] Skipping ${name} because there are no rows.`);
    return;
  }
  const collection = db.collection(name);
  await collection.deleteMany({});
  const result = await collection.insertMany(rows);
  console.log(`[Mongo] ${name}: inserted ${result.insertedCount} documents.`);
}

async function seedDuckdb({ duckdbPath, swapCsvPath, atomCsvPath, atoneCsvPath }) {
  if (!duckdbPath) {
    console.warn('[DuckDB] DUCKDB_PATH is not set. Skipping DuckDB ingestion.');
    return;
  }
  const resolvedDbPath = path.resolve(duckdbPath);
  await fs.promises.mkdir(path.dirname(resolvedDbPath), { recursive: true });
  const database = new duckdb.Database(resolvedDbPath);
  const connection = database.connect();

  try {
    await createTableFromCsv(connection, 'swap_data', swapCsvPath);
    if (atomCsvPath) {
      await createTableFromCsv(connection, 'atom_base_information', atomCsvPath);
    }
    if (atoneCsvPath) {
      await createTableFromCsv(connection, 'atone_base_information', atoneCsvPath);
    }
  } finally {
    connection.close();
  }
}

async function createTableFromCsv(connection, tableName, csvPath) {
  if (!csvPath) return;
  const absoluteCsvPath = path.resolve(csvPath);
  const escaped = absoluteCsvPath.replace(/'/g, "''");
  const sql = `
    CREATE OR REPLACE TABLE ${tableName} AS
    SELECT * FROM read_csv_auto('${escaped}', HEADER=TRUE, SAMPLE_SIZE=-1);
  `;
  await runDuckdb(connection, sql);
  console.log(`[DuckDB] ${tableName} refreshed from ${absoluteCsvPath}`);
}

function runDuckdb(connection, sql) {
  return new Promise((resolve, reject) => {
    connection.run(sql, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}
