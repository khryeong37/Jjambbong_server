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

const simplifyDenom = (denom = '') => {
  if (!denom) return '';
  const cleaned = denom.replace(/^factory\//i, '').replace(/^ibc\//i, 'IBC/').trim();
  return cleaned.toUpperCase();
};

const buildRouteLabel = (tokenInDenoms = [], tokenOutDenoms = []) => {
  const inputs = tokenInDenoms.filter(Boolean).map((d) => simplifyDenom(d));
  const outputs = tokenOutDenoms.filter(Boolean).map((d) => simplifyDenom(d));
  const left = inputs.length ? inputs.join(' + ') : '??';
  const right = outputs.length ? outputs.join(' + ') : '??';
  return `${left} → ${right}`;
};

const createEmptySwapProfile = () => ({
  cross: { count: 0, volume: 0, samples: [] },
  atom: { count: 0, volume: 0, samples: [] },
  atone: { count: 0, volume: 0, samples: [] },
  other: { count: 0, volume: 0, samples: [] },
});

const classifySwapCategory = (denoms = []) => {
  const hasAtom = denoms.some(isAtomDenom);
  const hasAtone = denoms.some(isAtomOneDenom);
  if (hasAtom && hasAtone) return 'cross';
  if (hasAtom) return 'atom';
  if (hasAtone) return 'atone';
  return 'other';
};

const toISODate = (date) => {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return null;
  return date.toISOString().split('T')[0];
};

const createDateSpine = (startISO, endISO) => {
  if (!startISO || !endISO) return [];
  const start = new Date(`${startISO}T00:00:00Z`);
  const end = new Date(`${endISO}T00:00:00Z`);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime()) || start > end) return [];
  const spine = [];
  const cursor = new Date(start);
  while (cursor <= end) {
    spine.push(toISODate(cursor));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return spine;
};

async function fetchDailyPrices(startISO, endISO) {
  if (!startISO || !endISO) return { atom: {}, atone: {} };
  const atomSql = `
    SELECT date_kst AS date, atom_price_close AS price
    FROM atom_daily_price
    WHERE date_kst BETWEEN '${startISO}' AND '${endISO}'
    ORDER BY date_kst
  `;
  const atoneSql = `
    SELECT date_kst AS date, atone_price_close AS price
    FROM atone_daily_price
    WHERE date_kst BETWEEN '${startISO}' AND '${endISO}'
    ORDER BY date_kst
  `;

  const [atomRows, atoneRows] = await Promise.all([
    runDuckdbQuery(atomSql).catch((error) => {
      console.error('[DuckDB] atom_daily_price query failed:', error?.message || error);
      return [];
    }),
    runDuckdbQuery(atoneSql).catch((error) => {
      console.error('[DuckDB] atone_daily_price query failed:', error?.message || error);
      return [];
    }),
  ]);

  const atomMap = {};
  const atoneMap = {};
  atomRows.forEach((row) => {
    if (row?.date) atomMap[row.date] = Number(row.price) || null;
  });
  atoneRows.forEach((row) => {
    if (row?.date) atoneMap[row.date] = Number(row.price) || null;
  });
  return { atom: atomMap, atone: atoneMap };
}

const buildForwardFilledPriceMap = (spine, priceSeries) => {
  if (!spine?.length) return {};
  const map = {};
  let lastAtom = null;
  let lastAtone = null;
  spine.forEach((date) => {
    if (priceSeries.atom?.[date] !== undefined) {
      lastAtom = priceSeries.atom[date];
    }
    if (priceSeries.atone?.[date] !== undefined) {
      lastAtone = priceSeries.atone[date];
    }
    map[date] = {
      atom: Number.isFinite(lastAtom) ? lastAtom : null,
      atone: Number.isFinite(lastAtone) ? lastAtone : null,
    };
  });
  return map;
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

  try {
    const docs = await collection.find({}).toArray();
    return docs;
  } catch (error) {
    console.warn('[Mongo] Failed to fetch swap docs:', error?.message || error);
    return [];
  }
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

  try {
    const rows = await runDuckdbQuery(sql);
    return rows;
  } catch (error) {
    console.error('[DuckDB] swap_data query failed:', error?.message || error);
    throw error;
  }
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
    console.warn('[Mongo] fetch failed, falling back to DuckDB:', error?.message || error);
    docs = [];
  }

  if (!docs.length) {
    docs = await fetchDocsFromDuckdb();
  }

  const filteredDocs = filterDocsByDateRange(docs, dateRange);
  if (!filteredDocs.length) return [];

  const aggMap = new Map();
  let earliestDate = null;
  let latestDate = null;

  for (const doc of filteredDocs) {
    const parsedTs = parseTimestamp(doc.timestamp_converted || doc.date);
    const sender = doc.sender;
    if (!sender || !parsedTs) continue;
    const timestamp = parsedTs.getTime();
    const date = parsedTs.toISOString().split('T')[0];
    if (!earliestDate || date < earliestDate) earliestDate = date;
    if (!latestDate || date > latestDate) latestDate = date;

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
    const priceAtom = parseNumber(doc.price_atom ?? doc.priceAtom);
    const priceAtone = parseNumber(doc.price_atone ?? doc.priceAtone);

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
    const category = classifySwapCategory(allDenoms);
    const routeLabel = buildRouteLabel(tokenInDenoms, tokenOutDenoms);

    const atomOut = tokenOutAmounts.reduce(
      (sum, amt, idx) => sum + (isAtomDenom(tokenOutDenoms[idx]) ? Math.abs(amt) : 0),
      0
    );
    const atomIn = tokenInAmounts.reduce(
      (sum, amt, idx) => sum + (isAtomDenom(tokenInDenoms[idx]) ? Math.abs(amt) : 0),
      0
    );
    const atoneOut = tokenOutAmounts.reduce(
      (sum, amt, idx) => sum + (isAtomOneDenom(tokenOutDenoms[idx]) ? Math.abs(amt) : 0),
      0
    );
    const atoneIn = tokenInAmounts.reduce(
      (sum, amt, idx) => sum + (isAtomOneDenom(tokenInDenoms[idx]) ? Math.abs(amt) : 0),
      0
    );
    const atomNetFlow = atomOut - atomIn;
    const atoneNetFlow = atoneOut - atoneIn;

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
      dayTxCount: {},
      swapVolume: 0,
      ibcVolume: 0,
      stakeVolume: 0,
      categoryStats: createEmptySwapProfile(),
      priceSamples: {},
      coinFlows: { atom: {}, atone: {} },
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
    prev.dayTxCount[date] = (prev.dayTxCount[date] || 0) + 1;
    prev.swapVolume += isIBC || isStake ? 0 : txVolume;
    prev.ibcVolume += isIBC ? txVolume : 0;
    prev.stakeVolume += isStake && !isIBC ? txVolume : 0;
    prev.coinFlows.atom[date] = (prev.coinFlows.atom[date] || 0) + atomNetFlow;
    prev.coinFlows.atone[date] = (prev.coinFlows.atone[date] || 0) + atoneNetFlow;

    const priceSample =
      prev.priceSamples[date] || (prev.priceSamples[date] = { atom: [], atone: [] });
    if (priceAtom > 0) priceSample.atom.push(priceAtom);
    if (priceAtone > 0) priceSample.atone.push(priceAtone);

    const bucket = prev.categoryStats[category];
    bucket.count += 1;
    bucket.volume += txVolume;
    if (routeLabel && bucket.samples.length < 3 && !bucket.samples.includes(routeLabel)) {
      bucket.samples.push(routeLabel);
    }

    aggMap.set(sender, prev);
  }

  const aggregates = Array.from(aggMap.values());
  if (!aggregates.length) return [];

  const maxVol = Math.max(...aggregates.map((a) => a.totalVolume), 1);
  const maxTx = Math.max(...aggregates.map((a) => a.txCount), 1);
  const totalVolumeSum = aggregates.reduce((sum, a) => sum + (a.totalVolume || 0), 0) || 1;

  const resolvedStart = (dateRange?.start && dateRange.start.trim()) || earliestDate;
  const resolvedEnd = (dateRange?.end && dateRange.end.trim()) || latestDate || resolvedStart;
  const spine = resolvedStart && resolvedEnd ? createDateSpine(resolvedStart, resolvedEnd) : [];
  const priceSeries = spine.length ? await fetchDailyPrices(resolvedStart, resolvedEnd) : { atom: {}, atone: {} };
  const priceMap = spine.length ? buildForwardFilledPriceMap(spine, priceSeries) : {};

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
    const weightAtom = a.atomVolume + a.oneVolume > 0 ? a.atomVolume / (a.atomVolume + a.oneVolume) : 0.5;
    const weightAtone = 1 - weightAtom;

    const scaleScore = Math.min(100, (a.totalVolume / maxVol) * 100);
    const shareScore = Math.round(((a.totalVolume || 0) / totalVolumeSum) * 100);

    const sortedDates = Object.keys(a.dayFlows).sort((d1, d2) => (d1 < d2 ? -1 : 1));
    let historyArr = [];
    const historyDates = spine.length ? spine : sortedDates;

    if (historyDates.length) {
      historyArr = historyDates.map((date, idx) => {
        const flow = a.dayFlows[date] ?? 0;
        const priceInfo = priceMap[date] || {};
        let priceAtom = priceInfo.atom;
        let priceAtone = priceInfo.atone;

        if (!Number.isFinite(priceAtom) || !Number.isFinite(priceAtone)) {
          const priceSample = a.priceSamples?.[date] || { atom: [], atone: [] };
          const avgPrice = (values) =>
            values && values.length ? values.reduce((sum, val) => sum + val, 0) / values.length : null;
          if (!Number.isFinite(priceAtom)) priceAtom = avgPrice(priceSample.atom);
          if (!Number.isFinite(priceAtone)) priceAtone = avgPrice(priceSample.atone);
        }

        const unifiedPrice =
          Number.isFinite(priceAtom) && Number.isFinite(priceAtone)
            ? priceAtom * weightAtom + priceAtone * weightAtone
            : Number.isFinite(priceAtom)
            ? priceAtom
            : Number.isFinite(priceAtone)
            ? priceAtone
            : null;

        return {
          date,
          price: unifiedPrice ?? null,
          priceUnified: unifiedPrice ?? null,
          priceAtom: Number.isFinite(priceAtom) ? priceAtom : null,
          priceAtone: Number.isFinite(priceAtone) ? priceAtone : null,
          netFlow: flow,
          netFlowAtom: a.coinFlows.atom[date] ?? 0,
          netFlowAtone: a.coinFlows.atone[date] ?? 0,
          txCount: a.dayTxCount[date] ?? 0,
        };
      });
    } else {
      historyArr = Object.entries(a.dayFlows)
        .sort(([d1], [d2]) => (d1 < d2 ? -1 : 1))
        .map(([date, flow], idx) => ({
          date,
          price: 1 + idx * 0.02 + Math.abs(flow) * 0.0001,
          priceUnified: 1 + idx * 0.02 + Math.abs(flow) * 0.0001,
          priceAtom: null,
          priceAtone: null,
          netFlow: flow,
          netFlowAtom: a.coinFlows.atom[date] ?? 0,
          netFlowAtone: a.coinFlows.atone[date] ?? 0,
          txCount: a.dayTxCount[date] ?? 0,
        }));
    }

    const flowSeriesAtom = historyArr.map((entry) => entry.netFlowAtom ?? null);
    const flowSeriesAtone = historyArr.map((entry) => entry.netFlowAtone ?? null);
    const flowSeriesUnified = historyArr.map(
      (entry) =>
        (entry.netFlowAtom ?? 0) * weightAtom + (entry.netFlowAtone ?? 0) * weightAtone
    );
    const priceSeriesAtom = historyArr.map((entry) => entry.priceAtom ?? null);
    const priceSeriesAtone = historyArr.map((entry) => entry.priceAtone ?? null);
    const priceSeriesUnified = historyArr.map((entry) => entry.price ?? null);

    const lagAtom = computeBestLag(flowSeriesAtom, priceSeriesAtom);
    const lagAtone = computeBestLag(flowSeriesAtone, priceSeriesAtone);
    const lagUnified = computeBestLag(flowSeriesUnified, priceSeriesUnified);

    const timingScore = calculateTimingScore(lagUnified.lag);
    const correlationScore = Number.isFinite(lagUnified.corr) ? lagUnified.corr : 0;
    const flowCorrelationScore = Math.min(100, Math.round(Math.abs(correlationScore) * 100));

    const size = Math.min(
      100,
      Math.max(
        10,
        scaleScore * 0.4 +
          (a.txCount / maxTx) * 40 +
          shareScore * 0.2 +
          timingScore * 0.2 +
          flowCorrelationScore * 0.2
      )
    );
    const roi = (a.netFlowSum / Math.max(1, a.totalVolume)) * 100;

    const categoryStats = a.categoryStats || createEmptySwapProfile();
    const swapProfile = buildSwapProfile(categoryStats, a.txCount);
    const crossVolume = categoryStats.cross?.volume || a.ibcVolume || 0;

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
        classifyTiming(lagUnified.lag) ||
        (netBuyRatio > 0.1 ? 'LEADING' : netBuyRatio < -0.1 ? 'LAGGING' : 'SYNC'),
      timingScore,
      correlationScore,
      scaleScore,
      shareScore,
      flowCorrelationScore,
      roi,
      crossVolume,
      marketSharePct: Math.max(0, (a.totalVolume || 0) / totalVolumeSum),
      swapProfile,
      timingDetail: {
        bestLagUnified: lagUnified.lag ?? null,
        bestLagAtom: lagAtom.lag ?? null,
        bestLagAtone: lagAtone.lag ?? null,
        weightAtom,
        weightAtone,
        correlationAtom: lagAtom.corr ?? null,
        correlationAtone: lagAtone.corr ?? null,
        unifiedCorrelation: lagUnified.corr ?? null,
        sampleSizeAtom: lagAtom.pairs ?? 0,
        sampleSizeAtone: lagAtone.pairs ?? 0,
      },
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

function computeBestLag(flowSeries = [], priceSeries = [], maxLag = 5) {
  if (!flowSeries.length || !priceSeries.length) {
    return { lag: null, corr: null, pairs: 0 };
  }

  const priceReturns = priceSeries.map((value, index) => {
    const current = Number(value);
    const prev = Number(priceSeries[index - 1]);
    if (!Number.isFinite(current) || !Number.isFinite(prev) || prev === 0) {
      return null;
    }
    return (current - prev) / prev;
  });

  let best = { lag: null, corr: null, pairs: 0 };

  for (let lag = -maxLag; lag <= maxLag; lag += 1) {
    if (lag === 0) continue;
    const flows = [];
    const prices = [];
    for (let i = 0; i < priceReturns.length; i += 1) {
      const priceVal = priceReturns[i];
      if (!Number.isFinite(priceVal)) continue;
      const flowIdx = i - lag;
      if (flowIdx < 0 || flowIdx >= flowSeries.length) continue;
      const flowVal = Number(flowSeries[flowIdx]);
      if (!Number.isFinite(flowVal)) continue;
      flows.push(flowVal);
      prices.push(priceVal);
    }
    if (flows.length < 3) continue;
    const corr = computePearson(flows, prices);
    if (!Number.isFinite(corr)) continue;
    if (best.corr === null || Math.abs(corr) > Math.abs(best.corr)) {
      best = { lag, corr, pairs: flows.length };
    }
  }

  return best;
}

function computePearson(x = [], y = []) {
  const n = Math.min(x.length, y.length);
  if (!n) return null;
  const meanX = x.reduce((sum, v) => sum + v, 0) / n;
  const meanY = y.reduce((sum, v) => sum + v, 0) / n;
  let numerator = 0;
  let denomX = 0;
  let denomY = 0;
  for (let i = 0; i < n; i += 1) {
    const dx = x[i] - meanX;
    const dy = y[i] - meanY;
    numerator += dx * dy;
    denomX += dx * dx;
    denomY += dy * dy;
  }
  const denominator = Math.sqrt(denomX * denomY);
  if (denominator === 0) return null;
  return numerator / denominator;
}

function calculateTimingScore(lag) {
  if (!Number.isFinite(lag)) return 48;
  const distance = Math.min(6, Math.abs(lag));
  return Math.max(12, 90 - distance * 12);
}

function classifyTiming(lag) {
  if (!Number.isFinite(lag)) return null;
  if (lag <= -2) return 'LEADING';
  if (lag >= 2) return 'LAGGING';
  return 'SYNC';
}

function buildSwapProfile(stats = createEmptySwapProfile(), totalTx = 0) {
  const result = {};
  Object.entries(stats).forEach(([key, bucket]) => {
    const share = totalTx > 0 ? (bucket.count / totalTx) * 100 : 0;
    result[key] = {
      share,
      count: bucket.count,
      volume: bucket.volume,
      samples: bucket.samples || [],
    };
  });
  return result;
}

module.exports = { loadSwapNodes };
