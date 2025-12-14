const fs = require('fs');
const path = require('path');
const duckdb = require('duckdb');

const DEFAULT_DB_PATH = path.resolve(__dirname, '../data/analytics.duckdb');
const DB_PATH = process.env.DUCKDB_PATH
  ? path.resolve(process.cwd(), process.env.DUCKDB_PATH)
  : DEFAULT_DB_PATH;

let databasePromise = null;

function ensureDatabaseExists() {
  if (!fs.existsSync(DB_PATH)) {
    throw new Error(
      `DuckDB 파일을 찾을 수 없습니다. 경로를 확인해주세요: ${DB_PATH}`
    );
  }
}

function getDatabase() {
  if (!databasePromise) {
    ensureDatabaseExists();
    databasePromise = new Promise((resolve, reject) => {
      const db = new duckdb.Database(DB_PATH, (err) => {
        if (err) {
          reject(err);
        } else {
          resolve(db);
        }
      });
    });
  }
  return databasePromise;
}

function run(sql) {
  return getDatabase().then(
    (db) =>
      new Promise((resolve, reject) => {
        db.all(sql, (err, rows) => {
          if (err) reject(err);
          else resolve(rows);
        });
      })
  );
}

function execute(sql) {
  return getDatabase().then((db) => {
    return new Promise((resolve, reject) => {
      db.run(sql, (err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  });
}


async function ensureViews() {
  // 가격 테이블이 없으면 뷰로 생성 (idempotent)
  const atomView = `
    CREATE OR REPLACE VIEW atom_daily_price AS
    SELECT
      DATE(TRY_STRPTIME(timestamp_converted, '%Y.%m.%d %H:%M')) AS date_kst,
      AVG(marketPrice) AS atom_price_close
    FROM atom_base_information
    WHERE timestamp_converted IS NOT NULL
    GROUP BY 1
    HAVING date_kst IS NOT NULL
    ORDER BY date_kst;
  `;

  const atoneView = `
    CREATE OR REPLACE VIEW atone_daily_price AS
    SELECT
      DATE(TRY_STRPTIME(timestamp_converted, '%Y.%m.%d %H:%M')) AS date_kst,
      AVG(marketPrice) AS atone_price_close
    FROM atone_base_information
    WHERE timestamp_converted IS NOT NULL
    GROUP BY 1
    HAVING date_kst IS NOT NULL
    ORDER BY date_kst;
  `;

  await execute(atomView);
  await execute(atoneView);
}

module.exports = {
  run,
  execute,
  ensureViews,
};
