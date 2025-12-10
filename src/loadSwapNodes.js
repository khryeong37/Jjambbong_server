const fs = require('fs/promises');
const path = require('path');

const DATA_PATH =
  process.env.SWAP_CSV_PATH ||
  path.resolve(__dirname, '../../client/public/data/swap.csv');

const parseNumber = (val = '') => {
  const n = Number(val);
  return Number.isFinite(n) ? n : 0;
};

const parseRow = (row) => {
  const result = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < row.length; i++) {
    const char = row[i];
    const nextChar = row[i + 1];

    if (char === '"') {
      if (inQuotes && nextChar === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      result.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }

  result.push(current.trim());
  return result;
};

const isAtomDenom = (denom = '') => {
  const upper = denom.toUpperCase();
  return upper.includes('ATOM') && !upper.includes('ONE');
};

const isAtomOneDenom = (denom = '') => {
  const upper = denom.toUpperCase();
  return upper.includes('ATONE') || upper.includes('ATOMONE');
};

async function loadSwapNodes(dateRange, options = {}) {
  const includeHistory = options.includeHistory !== false;
  const csv = await fs.readFile(DATA_PATH, 'utf-8');
  const lines = csv.trim().split(/\r?\n/);
  if (lines.length <= 1) return [];

  const dataLines = lines.slice(1);
  const aggMap = new Map();

  const startDate = dateRange?.start ? new Date(dateRange.start).getTime() : 0;
  const endDate = dateRange?.end
    ? new Date(dateRange.end).getTime() + 86400000
    : Infinity;

  for (const line of dataLines) {
    if (!line.trim()) continue;
    const cols = parseRow(line);
    if (cols.length < 16) continue;

    const timestamp = parseNumber(cols[1]);
    if (timestamp < startDate || timestamp >= endDate) continue;

    const sender = cols[2];
    if (!sender) continue;

    const tokenInAmounts = [parseNumber(cols[4]), parseNumber(cols[6]), parseNumber(cols[8])];
    const tokenInDenoms = [cols[5]?.trim(), cols[7]?.trim(), cols[9]?.trim()];
    const tokenOutAmounts = [parseNumber(cols[10]), parseNumber(cols[12]), parseNumber(cols[14])];
    const tokenOutDenoms = [cols[11]?.trim(), cols[13]?.trim(), cols[15]?.trim()];

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
    const date = new Date(timestamp).toISOString().split('T')[0];

    const allDenoms = [...tokenInDenoms, ...tokenOutDenoms].filter(Boolean);
    const hasAtom = allDenoms.some(isAtomDenom);
    const hasAtomOne = allDenoms.some(isAtomOneDenom);
    const isIBC = hasAtom && hasAtomOne;
    const isStake = inSum === 0 || outSum === 0;
    const txVolume = inSum + outSum;

    const prev =
      aggMap.get(sender) ||
      {
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
  if (aggregates.length === 0) return [];

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
      flowStdDev > 0 ? Math.max(0, 1 - flowStdDev / (Math.abs(flowMean) + 1)) : 0.5;
    const correlationScore = Math.max(-1, Math.min(1, netBuyRatio * consistency));

    const size = Math.min(
      100,
      Math.max(10, scaleScore * 0.5 + (a.txCount / maxTx) * 40 + Math.abs(correlationScore) * 10)
    );
    const roi = (a.netFlowSum / 100) * 100;

    const history = Object.entries(a.dayFlows)
      .sort(([d1], [d2]) => (d1 < d2 ? -1 : 1))
      .map(([date, flow], idx) => ({
        date,
        price: 1 + idx * 0.02 + Math.abs(flow) * 0.0001,
        netFlow: flow,
      }));

    const totalComposition = a.swapVolume + a.ibcVolume + a.stakeVolume || a.totalVolume;
    const swapPct = totalComposition > 0 ? (a.swapVolume / totalComposition) * 100 : 100;
    const ibcPct = totalComposition > 0 ? (a.ibcVolume / totalComposition) * 100 : 0;
    const stakePct = totalComposition > 0 ? (a.stakeVolume / totalComposition) * 100 : 0;

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
      timing: netBuyRatio > 0.1 ? 'LEADING' : netBuyRatio < -0.1 ? 'LAGGING' : 'SYNC',
      timingScore,
      correlationScore,
      scaleScore,
      roi,
      composition: {
        swap: Math.round(swapPct),
        ibc: Math.round(ibcPct),
        stake: Math.round(stakePct),
      },
      history,
      description: 'Derived from local swap CSV (server side).',
      history: includeHistory ? history : undefined,
    };
  });
}

module.exports = { loadSwapNodes };
