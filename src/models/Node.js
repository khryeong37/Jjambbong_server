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
    netFlow: Number,
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
    roi: Number,
    composition: CompositionSchema,
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
