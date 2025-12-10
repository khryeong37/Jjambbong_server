const mongoose = require('mongoose');

let isConnected = false;

async function connectDB() {
  if (isConnected) return;
  const uri = process.env.MONGODB_URI;
  if (!uri) {
    console.warn('[DB] MONGODB_URI not set. MongoDB features disabled.');
    return;
  }

  try {
    await mongoose.connect(uri, {
      dbName: process.env.MONGODB_DB || undefined,
    });
    isConnected = true;
    console.log('[DB] Connected to MongoDB');
  } catch (error) {
    console.error('[DB] Mongo connection failed:', error.message);
  }
}

function isDBConnected() {
  return isConnected;
}

module.exports = {
  connectDB,
  isDBConnected,
};
