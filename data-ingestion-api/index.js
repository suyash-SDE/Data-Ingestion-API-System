const express = require('express');
const { v4: uuidv4 } = require('uuid');
const { setTimeout } = require('timers/promises');

const app = express();
app.use(express.json());

// Custom Priority Queue implementation
class PriorityQueue {
  constructor(comparator) {
    this.items = [];
    this.comparator = comparator;
  }

  enqueue(item) {
    this.items.push(item);
    this.items.sort(this.comparator);
  }

  dequeue() {
    return this.items.shift();
  }

  get length() {
    return this.items.length;
  }
}

// In-memory store for ingestion jobs and their statuses
const ingestionJobs = new Map();
const batchStatuses = new Map();

// Priority queue for processing batches
const queue = new PriorityQueue((a, b) => {
  const priorityOrder = { HIGH: 0, MEDIUM: 1, LOW: 2 };
  if (priorityOrder[a.priority] !== priorityOrder[b.priority]) {
    return priorityOrder[a.priority] - priorityOrder[b.priority];
  }
  return a.created_time - b.created_time;
});

// Rate limiting flag
let lastProcessedTime = 0;
const RATE_LIMIT_MS = 5000; // 5 seconds
const BATCH_SIZE = 3;

// Simulate external API call
async function fetchExternalData(id) {
  await setTimeout(1000); // Simulate 1-second delay
  return { id, data: 'processed' };
}

// Process batches with rate limiting
async function processBatches() {
  while (queue.length > 0) {
    const now = Date.now();
    if (now - lastProcessedTime < RATE_LIMIT_MS) {
      await setTimeout(RATE_LIMIT_MS - (now - lastProcessedTime));
    }

    const batch = queue.dequeue();
    const { batch_id, ids, ingestion_id } = batch;

    batchStatuses.set(batch_id, { batch_id, ids, status: 'triggered' });
    updateIngestionStatus(ingestion_id);

    // Process each ID in the batch
    for (const id of ids) {
      try {
        await fetchExternalData(id);
      } catch (error) {
        console.error(`Error processing ID ${id}:`, error);
      }
    }

    batchStatuses.set(batch_id, { batch_id, ids, status: 'completed' });
    updateIngestionStatus(ingestion_id);
    lastProcessedTime = Date.now();
  }
}

// Update ingestion job status based on batch statuses
function updateIngestionStatus(ingestion_id) {
  const job = ingestionJobs.get(ingestion_id);
  if (!job) return;

  const batches = job.batches.map(batch_id => batchStatuses.get(batch_id));
  const allYetToStart = batches.every(b => b.status === 'yet_to_start');
  const anyTriggered = batches.some(b => b.status === 'triggered');
  const allCompleted = batches.every(b => b.status === 'completed');

  job.status = allYetToStart ? 'yet_to_start' : anyTriggered ? 'triggered' : 'completed';
  ingestionJobs.set(ingestion_id, job);
}

// Ingestion API
app.post('/ingest', (req, res) => {
  const { ids, priority = 'LOW' } = req.body;

  // Validate input
  if (!Array.isArray(ids) || ids.length === 0 || !ids.every(id => Number.isInteger(id) && id >= 1 && id <= 1000000007)) {
    return res.status(400).json({ error: 'Invalid IDs' });
  }
  if (!['HIGH', 'MEDIUM', 'LOW'].includes(priority)) {
    return res.status(400).json({ error: 'Invalid priority' });
  }

  const ingestion_id = uuidv4();
  const batches = [];
  const created_time = Date.now();

  // Split IDs into batches of 3
  for (let i = 0; i < ids.length; i += BATCH_SIZE) {
    const batch_ids = ids.slice(i, i + BATCH_SIZE);
    const batch_id = uuidv4();
    batchStatuses.set(batch_id, { batch_id, ids: batch_ids, status: 'yet_to_start' });
    batches.push(batch_id);
    queue.enqueue({ batch_id, ids: batch_ids, priority, created_time, ingestion_id });
  }

  ingestionJobs.set(ingestion_id, { ingestion_id, status: 'yet_to_start', batches, priority, created_time });

  // Start processing if not already running
  if (queue.length === batches.length) {
    processBatches().catch(err => console.error('Error in processBatches:', err));
  }

  res.json({ ingestion_id });
});

// Status API
app.get('/status/:ingestion_id', (req, res) => {
  const { ingestion_id } = req.params;
  const job = ingestionJobs.get(ingestion_id);

  if (!job) {
    return res.status(404).json({ error: 'Ingestion ID not found' });
  }

  const batches = job.batches.map(batch_id => batchStatuses.get(batch_id));
  res.json({
    ingestion_id,
    status: job.status,
    batches
  });
});

// Start server
const PORT = process.env.PORT || 5000;

if (require.main === module) {
  app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
} else {
  module.exports = app; // Export for testing
}
