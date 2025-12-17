const mongoose = require('mongoose');

const CompositionSchema = new mongoose.Schema(
  {
    swap: Number,
    ibc: Number,
    stake: Number,
  },
  { _id: false }
);

const HistorySchema = new mongoose.Schema(
  {
    date: String,
    price: Number,
    priceUnified: Number,
    priceAtom: Number,
    priceAtone: Number,
    netFlow: Number,
    netFlowAtom: Number,
    netFlowAtone: Number,
    txCount: Number,
  },
  { _id: false }
);

const SwapProfileBucketSchema = new mongoose.Schema(
  {
    share: Number,
    count: Number,
    volume: Number,
    samples: [String],
  },
  { _id: false }
);

const SwapProfileSchema = new mongoose.Schema(
  {
    cross: SwapProfileBucketSchema,
    atom: SwapProfileBucketSchema,
    atone: SwapProfileBucketSchema,
    other: SwapProfileBucketSchema,
  },
  { _id: false }
);

const TimingDetailSchema = new mongoose.Schema(
  {
    bestLagUnified: Number,
    bestLagAtom: Number,
    bestLagAtone: Number,
    weightAtom: Number,
    weightAtone: Number,
    correlationAtom: Number,
    correlationAtone: Number,
    unifiedCorrelation: Number,
    sampleSizeAtom: Number,
    sampleSizeAtone: Number,
  },
  { _id: false }
);

const NodeSchema = new mongoose.Schema(
  {
    address: { type: String, index: true },
    name: String,
    size: Number,
    bias: String,
    totalVolume: Number,
    avgTradeSize: Number,
    netBuyRatio: Number,
    txCount: Number,
    atomVolumeShare: Number,
    oneVolumeShare: Number,
    ibcVolumeShare: Number,
    activeDays: Number,
    lastActiveDate: Date,
    timing: String,
    timingScore: Number,
    correlationScore: Number,
    scaleScore: Number,
    shareScore: Number,
    flowCorrelationScore: Number,
    roi: Number,
    crossVolume: Number,
    marketSharePct: Number,
    composition: CompositionSchema,
    swapProfile: SwapProfileSchema,
    timingDetail: TimingDetailSchema,
    history: [HistorySchema],
    description: String,
  },
  {
    collection: 'nodes',
  }
);

NodeSchema.index({ lastActiveDate: -1 });
NodeSchema.index({ bias: 1 });
NodeSchema.index({ size: -1 });

module.exports = mongoose.models.Node || mongoose.model('Node', NodeSchema);
