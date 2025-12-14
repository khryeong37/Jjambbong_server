const { run } = require('./duckdbClient');

const DAY_MS = 24 * 60 * 60 * 1000;

async function fetchChainMarket(chain) {
  const table =
    chain === 'ATOM' ? 'atom_base_information' : 'atone_base_information';
  const rows = await run(`
    SELECT
      marketPrice AS price,
      marketCap,
      tokenBonded,
      tokenSupply,
      CAST(timestamp AS DOUBLE) AS ts
    FROM ${table}
    WHERE marketPrice IS NOT NULL
      AND timestamp IS NOT NULL
    ORDER BY ts DESC
    LIMIT 240
  `);

  if (!rows || rows.length === 0) {
    return null;
  }

  const current = rows[0];
  const latestTs = Number(current.ts);
  const cutoff = latestTs - DAY_MS;
  const past =
    rows.find((row) => Number(row.ts) <= cutoff) || rows[rows.length - 1];

  const change24h =
    past && Number(past.price)
      ? ((Number(current.price) - Number(past.price)) / Number(past.price)) *
        100
      : 0;

  const history = rows
    .slice()
    .reverse()
    .map((row) => ({
      date: new Date(Number(row.ts)).toISOString().split('T')[0],
      price: Number(row.price) || 0,
    }));

  const volume24h = await fetchVolumeForChain(chain, cutoff, latestTs);

  return {
    price: Number(current.price) || 0,
    change24h,
    marketCap: Number(current.marketCap) || 0,
    volume24h,
    history,
  };
}

async function fetchVolumeForChain(chain, startTs, endTs) {
  const targetDenom = chain === 'ATOM' ? 'ATOM' : 'ATONE';
  const rows = await run(`
    SELECT COALESCE(SUM(tx_volume), 0) AS volume
    FROM swap_data
    WHERE CAST(timestamp AS DOUBLE) BETWEEN ${startTs} AND ${endTs}
      AND (
        tokenInDenom1 = '${targetDenom}'
        OR tokenOutDenom1 = '${targetDenom}'
        OR tokenOutDenom2 = '${targetDenom}'
        OR tokenOutDenom3 = '${targetDenom}'
      )
  `);
  if (!rows || rows.length === 0) {
    return 0;
  }
  return Number(rows[0].volume) || 0;
}

async function fetchMarketSnapshot() {
  const [atom, atone] = await Promise.all([
    fetchChainMarket('ATOM'),
    fetchChainMarket('ATONE'),
  ]);
  return { atom, atone };
}

module.exports = {
  fetchMarketSnapshot,
};
