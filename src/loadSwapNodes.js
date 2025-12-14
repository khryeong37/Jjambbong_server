// server/src/loadSwapNodes.js

const { MongoClient } = require('mongodb');
const { run: runDuckdbQuery } = require('./duckdbClient');

// ---------- 공통 유틸 ----------

const parseNumber = (val = '') => {
  const n = Number(val);
  return Number.isFinite(n) ? n : 0;
};

const isAtomDenom = (denom = '') => {
  const upper = denom.toUpperCase();
  return upper.includes('ATOM') && !upper.includes('ONE');
};

const isAtomOneDenom = (denom = '') => {
  const upper = denom.toUpperCase();
  return upper.includes('ATONE') || upper.includes('ATOMONE');
};

// ---------- MongoDB 연결 재사용 ----------

let clientPromise = null;

async function getDbCollection() {
  const uri = process.env.MONGODB_URI;
  if (!uri) return null;

  if (!clientPromise) {
    const client = new MongoClient(uri);
    clientPromise = client.connect();
  }

  const client = await clientPromise;

  const dbName = process.env.SWAP_DB_NAME || 'swap_db';
  const collName = process.env.SWAP_COLLECTION_NAME || 'swap_logs';

  return client.db(dbName).collection(collName);
}

async function fetchDocsFromMongo() {
  const collection = await getDbCollection();
  if (!collection) return [];

  const docs = await collection.find({}).toArray();
  return docs;
}

async function fetchDocsFromDuckdb() {
  const sql = `
    SELECT
      type,
      timestamp,
      timestamp_converted,
      date,
      sender,
      txHash,
      tokenInAmount1,
      tokenInDenom1,
      tokenOutAmount1,
      tokenOutDenom1,
      tokenOutAmount2,
      tokenOutDenom2,
      tokenOutAmount3,
      tokenOutDenom3,
      price_atom,
      price_atone,
      tx_volume
    FROM swap_data
  `;

  const rows = await runDuckdbQuery(sql);
  return rows;
}

function parseTimestamp(value) {
  if (!value || typeof value !== 'string') return null;
  const [datePart, timePart = '00:00'] = value.trim().split(' ');
  const [year, month, day] = (datePart || '').split('.').map((v) => Number(v));
  const [hour, minute] = timePart.split(':').map((v) => Number(v));
  if (!year || !month || !day) return null;

  const pad = (n) => String(n).padStart(2, '0');
  const iso = `${pad(year)}-${pad(month)}-${pad(day)}T${pad(hour || 0)}:${pad(minute || 0)}:00+09:00`;
  const parsed = Date.parse(iso);
  if (Number.isNaN(parsed)) return null;
  return new Date(parsed);
}

function filterDocsByDateRange(docs, dateRange) {
  if (!dateRange?.start && !dateRange?.end) return docs;
  const startMs = dateRange?.start ? new Date(dateRange.start).getTime() : null;
  const endMs = dateRange?.end ? new Date(dateRange.end).getTime() + 86400000 : null;
    return docs.filter((doc) => {
      const ts = parseTimestamp(doc.timestamp_converted || doc.date);
      if (!ts) return false;
      const ms = ts.getTime();
      if (startMs && ms < startMs) return false;
      if (endMs && ms >= endMs) return false;
      return true;
    });
}

// ---------- 메인 함수: CSV 대신 MongoDB에서 읽기 ----------

async function loadSwapNodes(dateRange, options = {}) {
  const includeHistory = options.includeHistory !== false;

  let docs = [];
  try {
    docs = await fetchDocsFromMongo();
  } catch (error) {
    console.warn('[DB] Mongo fetch failed, falling back to DuckDB:', error.message);
    docs = [];
  }

  if (!docs.length) {
    docs = await fetchDocsFromDuckdb();
  }

  const filteredDocs = filterDocsByDateRange(docs, dateRange);
  if (!filteredDocs.length) return [];

  const aggMap = new Map();

  for (const doc of filteredDocs) {
    const parsedTs = parseTimestamp(doc.timestamp_converted || doc.date);
    const sender = doc.sender;
    if (!sender || !parsedTs) continue;
    const timestamp = parsedTs.getTime();
    const date = parsedTs.toISOString().split('T')[0];

    const tokenInAmounts = [
      parseNumber(doc.tokenInAmount1),
      parseNumber(doc.tokenInAmount2),
      parseNumber(doc.tokenInAmount3),
    ];
    const tokenInDenoms = [
      doc.tokenInDenom1?.trim(),
      doc.tokenInDenom2?.trim(),
      doc.tokenInDenom3?.trim(),
    ];
    const tokenOutAmounts = [
      parseNumber(doc.tokenOutAmount1),
      parseNumber(doc.tokenOutAmount2),
      parseNumber(doc.tokenOutAmount3),
    ];
    const tokenOutDenoms = [
      doc.tokenOutDenom1?.trim(),
      doc.tokenOutDenom2?.trim(),
      doc.tokenOutDenom3?.trim(),
    ];

    let inSum = 0;
    let outSum = 0;
    let atomVol = 0;
    let oneVol = 0;

    tokenInAmounts.forEach((amt, idx) => {
      inSum += Math.abs(amt);
      if (isAtomDenom(tokenInDenoms[idx])) atomVol += Math.abs(amt);
      if (isAtomOneDenom(tokenInDenoms[idx])) oneVol += Math.abs(amt);
    });

    tokenOutAmounts.forEach((amt, idx) => {
      outSum += Math.abs(amt);
      if (isAtomDenom(tokenOutDenoms[idx])) atomVol += Math.abs(amt);
      if (isAtomOneDenom(tokenOutDenoms[idx])) oneVol += Math.abs(amt);
    });

    const netFlow = outSum - inSum;
    const allDenoms = [...tokenInDenoms, ...tokenOutDenoms].filter(Boolean);
    const hasAtom = allDenoms.some(isAtomDenom);
    const hasAtomOne = allDenoms.some(isAtomOneDenom);
    const isIBC = hasAtom && hasAtomOne;
    const isStake = inSum === 0 || outSum === 0;
    const txVolume = inSum + outSum;

    const prev = aggMap.get(sender) || {
      sender,
      txCount: 0,
      totalVolume: 0,
      buyVolume: 0,
      sellVolume: 0,
      netFlowSum: 0,
      atomVolume: 0,
      oneVolume: 0,
      lastActive: 0,
      dayFlows: {},
      swapVolume: 0,
      ibcVolume: 0,
      stakeVolume: 0,
    };

    prev.txCount += 1;
    prev.totalVolume += txVolume;
    prev.buyVolume += Math.max(0, netFlow);
    prev.sellVolume += Math.max(0, -netFlow);
    prev.netFlowSum += netFlow;
    prev.atomVolume += atomVol;
    prev.oneVolume += oneVol;
    prev.lastActive = Math.max(prev.lastActive, timestamp);
    prev.dayFlows[date] = (prev.dayFlows[date] || 0) + netFlow;
    prev.swapVolume += isIBC || isStake ? 0 : txVolume;
    prev.ibcVolume += isIBC ? txVolume : 0;
    prev.stakeVolume += isStake && !isIBC ? txVolume : 0;

    aggMap.set(sender, prev);
  }

  const aggregates = Array.from(aggMap.values());
  if (!aggregates.length) return [];

  const maxVol = Math.max(...aggregates.map((a) => a.totalVolume), 1);
  const maxTx = Math.max(...aggregates.map((a) => a.txCount), 1);

  return aggregates.map((a) => {
    const netBuyRatioDenom = a.buyVolume + a.sellVolume || 1;
    const netBuyRatio = (a.buyVolume - a.sellVolume) / netBuyRatioDenom;
    const atomShare = a.totalVolume ? a.atomVolume / a.totalVolume : 0;
    const oneShare = a.totalVolume ? a.oneVolume / a.totalVolume : 0;
    const bias =
      atomShare > oneShare && atomShare >= 0.5
        ? 'ATOM'
        : oneShare > atomShare && oneShare >= 0.5
        ? 'ATOMONE'
        : 'MIXED';

    const scaleScore = Math.min(100, (a.totalVolume / maxVol) * 100);
    const timingScore = 50 + netBuyRatio * 40;

    const dayFlowValues = Object.values(a.dayFlows);
    const flowMean =
      dayFlowValues.length > 0
        ? dayFlowValues.reduce((sum, v) => sum + v, 0) / dayFlowValues.length
        : 0;
    const flowVariance =
      dayFlowValues.length > 0
        ? dayFlowValues.reduce((sum, v) => sum + Math.pow(v - flowMean, 2), 0) /
          dayFlowValues.length
        : 0;
    const flowStdDev = Math.sqrt(flowVariance);
    const consistency =
      flowStdDev > 0
        ? Math.max(0, 1 - flowStdDev / (Math.abs(flowMean) + 1))
        : 0.5;
    const correlationScore = Math.max(
      -1,
      Math.min(1, netBuyRatio * consistency)
    );

    const size = Math.min(
      100,
      Math.max(
        10,
        scaleScore * 0.5 +
          (a.txCount / maxTx) * 40 +
          Math.abs(correlationScore) * 10
      )
    );
    const roi = (a.netFlowSum / 100) * 100;

    const historyArr = Object.entries(a.dayFlows)
      .sort(([d1], [d2]) => (d1 < d2 ? -1 : 1))
      .map(([date, flow], idx) => ({
        date,
        price: 1 + idx * 0.02 + Math.abs(flow) * 0.0001,
        netFlow: flow,
      }));

    const totalComposition =
      a.swapVolume + a.ibcVolume + a.stakeVolume || a.totalVolume;
    const swapPct =
      totalComposition > 0 ? (a.swapVolume / totalComposition) * 100 : 100;
    const ibcPct =
      totalComposition > 0 ? (a.ibcVolume / totalComposition) * 100 : 0;
    const stakePct =
      totalComposition > 0 ? (a.stakeVolume / totalComposition) * 100 : 0;

    return {
      id: a.sender,
      name: a.sender,
      address: a.sender,
      size,
      bias,
      totalVolume: a.totalVolume,
      avgTradeSize: a.totalVolume / a.txCount,
      netBuyRatio,
      txCount: a.txCount,
      atomVolumeShare: atomShare,
      oneVolumeShare: oneShare,
      ibcVolumeShare: totalComposition > 0 ? a.ibcVolume / totalComposition : 0,
      activeDays: Object.keys(a.dayFlows).length,
      lastActiveDate: new Date(a.lastActive).toISOString(),
      timing:
        netBuyRatio > 0.1 ? 'LEADING' : netBuyRatio < -0.1 ? 'LAGGING' : 'SYNC',
      timingScore,
      correlationScore,
      scaleScore,
      roi,
      composition: {
        swap: Math.round(swapPct),
        ibc: Math.round(ibcPct),
        stake: Math.round(stakePct),
      },
      description: 'Derived from MongoDB swap_logs collection.',
      history: includeHistory ? historyArr : undefined,
    };
  });
}

module.exports = { loadSwapNodes };
