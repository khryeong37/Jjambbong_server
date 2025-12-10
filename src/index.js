const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
const { connectDB, isDBConnected } = require('./db');
const NodeModel = require('./models/Node');
const { loadSwapNodes } = require('./loadSwapNodes');

dotenv.config();
connectDB();

const app = express();
const port = process.env.PORT || 4000;

app.use(cors());
app.use(express.json());

app.get('/', (_, res) => {
  res.json({ status: 'ok', message: 'Server root reachable' });
});

app.get('/health', (_, res) => {
  res.json({
    status: 'ok',
    dbConnected: isDBConnected(),
    message: 'Server skeleton is running.',
  });
});

app.get('/api/nodes', async (req, res) => {
  try {
    const options = buildFilterOptions(req.query);
    const nodesFromDB = await fetchNodesFromDB(options);
    if (nodesFromDB && nodesFromDB.length > 0) {
      return res.json(nodesFromDB);
    }
    const fallback = await loadSwapNodes(options.dateRange, { includeHistory: false });
    res.json(limitCollection(fallback, options.limit));
  } catch (error) {
    console.error('Failed to load nodes', error);
    res.status(500).json({ error: 'Failed to load nodes' });
  }
});

app.get('/api/nodes/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const node = await fetchNodeDetail(id);
    if (!node) {
      return res.status(404).json({ error: 'Node not found' });
    }
    res.json(node);
  } catch (error) {
    console.error('Failed to load node detail', error);
    res.status(500).json({ error: 'Failed to load node detail' });
  }
});

app.listen(port, () => {
  console.log(`API server ready at http://localhost:${port}`);
});

function limitCollection(collection, limit) {
  if (!limit || collection.length <= limit) return collection;
  return collection.slice(0, limit);
}

function buildFilterOptions(query) {
  const parseRange = (key) => {
    const min = query[`${key}Min`];
    const max = query[`${key}Max`];
    return {
      min: min !== undefined ? Number(min) : undefined,
      max: max !== undefined ? Number(max) : undefined,
    };
  };

  const start = typeof query.start === 'string' ? query.start : undefined;
  const end = typeof query.end === 'string' ? query.end : undefined;
  const limitParam = Number(query.limit);
  const limit = Number.isFinite(limitParam) ? Math.min(Math.max(limitParam, 50), 1000) : 500;

  return {
    dateRange: start || end ? { start, end } : undefined,
    limit,
    ranges: {
      totalVolume: parseRange('totalVolume'),
      avgTradeSize: parseRange('avgTradeSize'),
      netBuyRatio: parseRange('netBuyRatio'),
      txCount: parseRange('txCount'),
      atomVolumeShare: parseRange('atomShare'),
      oneVolumeShare: parseRange('oneShare'),
      ibcVolumeShare: parseRange('ibcShare'),
      activeDays: parseRange('activeDays'),
      size: parseRange('aiiScore'),
      correlationScore: parseRange('correlation'),
    },
    timingType:
      typeof query.timingType === 'string' && query.timingType !== 'ALL'
        ? query.timingType
        : undefined,
    recentActivity: typeof query.recentActivity === 'string' ? query.recentActivity : undefined,
  };
}

async function fetchNodesFromDB(options) {
  if (!isDBConnected()) return null;
  const query = {};

  if (options.dateRange) {
    const dateFilter = {};
    if (options.dateRange.start) {
      dateFilter.$gte = new Date(options.dateRange.start);
    }
    if (options.dateRange.end) {
      dateFilter.$lte = new Date(options.dateRange.end);
    }
    if (Object.keys(dateFilter).length) {
      query.lastActiveDate = dateFilter;
    }
  }

  if (options.recentActivity && options.recentActivity !== 'ALL') {
    const days = Number(options.recentActivity.replace('D', ''));
    if (Number.isFinite(days) && days > 0) {
      const recentDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
      query.lastActiveDate = { ...(query.lastActiveDate || {}), $gte: recentDate };
    }
  }

  const rangeMap = {
    totalVolume: 'totalVolume',
    avgTradeSize: 'avgTradeSize',
    netBuyRatio: 'netBuyRatio',
    txCount: 'txCount',
    atomVolumeShare: 'atomVolumeShare',
    oneVolumeShare: 'oneVolumeShare',
    ibcVolumeShare: 'ibcVolumeShare',
    activeDays: 'activeDays',
    size: 'size',
    correlationScore: 'correlationScore',
  };

  Object.entries(rangeMap).forEach(([rangeKey, field]) => {
    const range = options.ranges?.[rangeKey];
    if (!range) return;
    const conditions = {};
    if (Number.isFinite(range.min)) conditions.$gte = range.min;
    if (Number.isFinite(range.max)) conditions.$lte = range.max;
    if (Object.keys(conditions).length) {
      query[field] = conditions;
    }
  });

  if (options.timingType) {
    query.timing = options.timingType;
  }

  try {
    const docs = await NodeModel.find(query)
      .sort({ size: -1 })
      .limit(options.limit)
      .select(
        'address name size bias totalVolume avgTradeSize netBuyRatio txCount atomVolumeShare oneVolumeShare ibcVolumeShare activeDays lastActiveDate timing correlationScore scaleScore roi description composition'
      )
      .lean();

    return docs.map(normalizeNodeDoc);
  } catch (error) {
    console.error('[DB] Query failed, falling back to CSV:', error.message);
    return null;
  }
}

async function fetchNodeDetail(id) {
  if (isDBConnected()) {
    const node = await NodeModel.findOne({
      $or: [{ _id: id }, { address: id }],
    }).lean();
    if (node) return normalizeNodeDoc(node, true);
  }

  const nodes = await loadSwapNodes(undefined, { includeHistory: true });
  return nodes.find((n) => n.id === id || n.address === id) || null;
}

function normalizeNodeDoc(doc, includeHistory = false) {
  const base = {
    ...doc,
    id: doc.address || String(doc._id),
    lastActiveDate: doc.lastActiveDate ? doc.lastActiveDate.toISOString() : null,
  };

  if (!includeHistory) {
    delete base.history;
  }
  return base;
}
