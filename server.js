const express = require('express');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env') });
const { createClient } = require('redis');
const os = require('os');

// ── Redis (live stats pushed by backdoor backend) ─────────────────
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
let redisClient = null;
let liveCache = null;
let liveCacheAt = 0;
const LIVE_TTL_MS = 2000; // cache for 2 seconds to avoid hammering Redis

async function initRedis() {
  try {
    if (redisClient) return redisClient;
    const client = createClient({ url: REDIS_URL, socket: { keepAlive: 5000 } });
    client.on('error', (err) => console.warn('[Benchmark Backend] Redis error:', err?.message));
    await client.connect();
    redisClient = client;
    console.log('[Benchmark Backend] Redis connected');
    return redisClient;
  } catch (err) {
    console.warn('[Benchmark Backend] Redis connect failed:', err?.message);
    return null;
  }
}

const DEFAULT_LIVE_STATS = {
  cpu_usage_percent: 0,
  system: {
    cpu_cores: os.cpus().length,
    cpu_model: os.cpus()[0]?.model?.trim() || 'Unknown',
    total_mem_bytes: os.totalmem(),
    free_mem_bytes: os.freemem(),
  },
  memory: {
    rss_bytes: 0,
    heap_used_bytes: 0,
    heap_total_bytes: 0,
  },
  uptime_seconds: 0,
  gate: { total: 0, queued: 0, skipped: 0, throttled: 0, cooldown: 0, dropped: 0 },
  workers: { pool_size: 0, busy_ratio: 0, queue_depth: 0 },
};

async function getLiveStats() {
  const now = Date.now();
  if (now - liveCacheAt < LIVE_TTL_MS && liveCache) return liveCache;
  try {
    if (!redisClient) await initRedis();
    if (!redisClient) return DEFAULT_LIVE_STATS;
    const raw = await redisClient.get('godezk:telemetry:live_stats');
    if (!raw) return DEFAULT_LIVE_STATS;
    const parsed = JSON.parse(raw);
    liveCache = parsed;
    liveCacheAt = now;
    return parsed;
  } catch (_) {
    return DEFAULT_LIVE_STATS;
  }
}

const { Pool } = require('pg');

const app = express();
const PORT = Number(process.env.PORT || 3031);

// ── PostgreSQL (read-only pull from the backdoor database) ────────
const pool = new Pool({
  host: process.env.PG_HOST || 'localhost',
  port: Number(process.env.PG_PORT || 5432),
  database: process.env.PG_DATABASE || 'Godezk_medops',
  user: process.env.PG_USER || 'postgres',
  password: process.env.PG_PASSWORD || 'root',
  ssl: process.env.PG_SSL === 'true' ? { rejectUnauthorized: false } : false,
  max: 5,
  connectionTimeoutMillis: 5000,
  idleTimeoutMillis: 30000,
  options: `-c search_path=${process.env.PG_SEARCH_PATH || 'device_workflow_schema,public'}`,
});

// ── Middleware ────────────────────────────────────────────────────
app.use(express.json());
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// ── Health check ──────────────────────────────────────────────────
app.get('/api/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ success: true, status: 'ok' });
  } catch (err) {
    res.status(503).json({ success: false, status: 'db_unreachable', error: err.message });
  }
});

// ── Benchmark Suite — per-component aggregates (24h window by default)
app.get('/api/bench-suite', async (req, res) => {
  try {
    // Window selection: default to last 24 hours
    const hours = Math.max(1, Math.min(parseInt(String(req.query.hours || '24'), 10) || 24, 168)); // 1..168 hours
    const interval = `${hours} hours`;

    // Helper snippets
    const pct = (expr) => `
      AVG(${expr})::float AS avg,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ${expr}::double precision) AS p50,
      PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ${expr}::double precision) AS p95,
      PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY ${expr}::double precision) AS p99,
      COUNT(*)::int AS count
    `;

    // 1) Overall pipeline duration (duration_ms)
    const overallQuery = `
      SELECT
        ${pct('woe.duration_ms')},
        COUNT(*) FILTER (WHERE woe.status = 'failed')::int AS failed
      FROM workflow_org_executions woe
      WHERE woe.started_at IS NOT NULL AND woe.started_at >= NOW() - INTERVAL '${interval}'
    `;

    // 2) FrameWeir (weir_cpu_ms in event context)
    const weirQuery = `
      SELECT ${pct("CAST(woe.context->'event'->>'weir_cpu_ms' AS NUMERIC)")}
      FROM workflow_org_executions woe
      WHERE woe.started_at >= NOW() - INTERVAL '${interval}'
        AND (woe.context->'event'->>'weir_cpu_ms') IS NOT NULL
        AND (woe.context->'event'->>'weir_cpu_ms') <> ''
    `;

    // 3) PreScreener (screener_cpu_ms)
    const screenerQuery = `
      SELECT ${pct("CAST(woe.context->'event'->>'screener_cpu_ms' AS NUMERIC)")}
      FROM workflow_org_executions woe
      WHERE woe.started_at >= NOW() - INTERVAL '${interval}'
        AND (woe.context->'event'->>'screener_cpu_ms') IS NOT NULL
        AND (woe.context->'event'->>'screener_cpu_ms') <> ''
    `;

    // 4) PerceptionGate / Inference Service (ai_time_ms OR inference_ms OR sum of ai_node durations)
    const aiQuery = `
      SELECT ${pct(`COALESCE(
        CAST(woe.context->'event'->>'ai_time_ms' AS NUMERIC),
        CAST(woe.context->'event'->>'inference_ms' AS NUMERIC),
        (
          SELECT SUM(CAST(val->>'duration_ms' AS INT))
          FROM jsonb_each(CASE WHEN jsonb_typeof(woe.context->'node_results') = 'object' THEN woe.context->'node_results' ELSE '{}'::jsonb END) AS t(key, val)
          WHERE val->>'nodeType' = 'ai_node'
        )
      )`)}
      FROM workflow_org_executions woe
      WHERE woe.started_at >= NOW() - INTERVAL '${interval}'
    `;

    // 5) Node-type aggregates from node_results (postgres, notification, minio)
    const nodeStats = (nodeType) => `
      SELECT ${pct("CAST(val->>'duration_ms' AS NUMERIC)")}
      FROM workflow_org_executions woe,
        LATERAL jsonb_each(CASE WHEN jsonb_typeof(woe.context->'node_results') = 'object' THEN woe.context->'node_results' ELSE '{}'::jsonb END) AS t(key, val)
      WHERE woe.started_at >= NOW() - INTERVAL '${interval}' AND val->>'nodeType' = '${nodeType}'
    `;

    // 6) Queue stats (workflow_runner_queue) — schema-flexible
    const queueCurrentSizeQuery = `
      SELECT COUNT(*)::int AS current_size
      FROM workflow_runner_queue
      WHERE status IN ('queued','running')
    `;

    // Discover timestamp columns to build a time series if possible
    const colsRes = await pool.query(`
      SELECT column_name 
      FROM information_schema.columns 
      WHERE table_schema IN (current_schema(), 'public') 
        AND table_name = 'workflow_runner_queue'
    `);
    const colSet = new Set(colsRes.rows.map(r => r.column_name));
    const candidateTs = ['created_at', 'enqueued_at', 'queued_at', 'inserted_at', 'added_at', 'timestamp', 'ts', 'time', 'started_at'];
    const tsCol = candidateTs.find(c => colSet.has(c));
    const hasUpdatedAt = colSet.has('updated_at');

    const queueSeriesQuery = tsCol ? `
      SELECT date_trunc('minute', ${tsCol}) AS ts, COUNT(*)::int AS cnt
      FROM workflow_runner_queue
      WHERE ${tsCol} >= NOW() - INTERVAL '${interval}'
      GROUP BY date_trunc('minute', ${tsCol})
      ORDER BY ts
    ` : null;

    const queueActiveWorkersQuery = hasUpdatedAt ? `
      SELECT COUNT(DISTINCT executor_id)::int AS active_workers
      FROM workflow_runner_queue
      WHERE status = 'running' AND updated_at >= NOW() - INTERVAL '5 minutes'
    ` : `
      SELECT COUNT(DISTINCT executor_id)::int AS active_workers
      FROM workflow_runner_queue
      WHERE status = 'running'
    `;

    const [overallRes, weirRes, screenerRes, aiRes, postgresRes, notificationRes, minioRes, queueCurRes, queueSeriesRes, activeWorkersRes] = await Promise.all([
      pool.query(overallQuery),
      pool.query(weirQuery),
      pool.query(screenerQuery),
      pool.query(aiQuery),
      pool.query(nodeStats('postgres')),
      pool.query(nodeStats('notification')),
      pool.query(nodeStats('minio')),
      pool.query(queueCurrentSizeQuery),
      queueSeriesQuery ? pool.query(queueSeriesQuery) : Promise.resolve({ rows: [] }),
      pool.query(queueActiveWorkersQuery),
    ]);

    // ── RTSP Handler metrics (schema-flexible over context.event)
    const cameraKeyExpr = `COALESCE(
      NULLIF(woe.context->'event'->>'camera_id',''),
      NULLIF(woe.context->'event'->>'device_id',''),
      NULLIF(woe.context->'event'->>'source_id',''),
      NULLIF(woe.context->'event'->>'stream_id',''),
      NULLIF(woe.context->'event'->>'camera',''),
      NULLIF(woe.context->'event'->>'device','')
    )`;

    const rtspFramesQuery = `
      SELECT COUNT(*)::int AS frames_total
      FROM workflow_org_executions woe
      WHERE woe.started_at >= NOW() - INTERVAL '${interval}'
        AND ${cameraKeyExpr} IS NOT NULL
    `;

    const rtspActive5mQuery = `
      SELECT COUNT(DISTINCT ${cameraKeyExpr})::int AS active_5m
      FROM workflow_org_executions woe
      WHERE woe.started_at >= NOW() - INTERVAL '5 minutes'
        AND ${cameraKeyExpr} IS NOT NULL
    `;

    const decodeExpr = `COALESCE(
      NULLIF(woe.context->'event'->>'rtsp_decode_ms','')::numeric,
      NULLIF(woe.context->'event'->>'decode_ms','')::numeric,
      NULLIF(woe.context->'event'->>'ingest_ms','')::numeric,
      NULLIF(woe.context->'event'->>'frame_decode_ms','')::numeric
    )`;
    const decodePresence = `(
      (woe.context->'event'->>'rtsp_decode_ms') IS NOT NULL AND (woe.context->'event'->>'rtsp_decode_ms') <> '' OR
      (woe.context->'event'->>'decode_ms') IS NOT NULL AND (woe.context->'event'->>'decode_ms') <> '' OR
      (woe.context->'event'->>'ingest_ms') IS NOT NULL AND (woe.context->'event'->>'ingest_ms') <> '' OR
      (woe.context->'event'->>'frame_decode_ms') IS NOT NULL AND (woe.context->'event'->>'frame_decode_ms') <> ''
    )`;
    const rtspDecodeQuery = `
      SELECT ${pct(decodeExpr)}
      FROM workflow_org_executions woe
      WHERE woe.started_at >= NOW() - INTERVAL '${interval}' AND ${decodePresence}
    `;

    const encodeExpr = `COALESCE(
      NULLIF(woe.context->'event'->>'base64_encode_ms','')::numeric,
      NULLIF(woe.context->'event'->>'encode_ms','')::numeric,
      NULLIF(woe.context->'event'->>'jpeg_encode_ms','')::numeric
    )`;
    const encodePresence = `(
      (woe.context->'event'->>'base64_encode_ms') IS NOT NULL AND (woe.context->'event'->>'base64_encode_ms') <> '' OR
      (woe.context->'event'->>'encode_ms') IS NOT NULL AND (woe.context->'event'->>'encode_ms') <> '' OR
      (woe.context->'event'->>'jpeg_encode_ms') IS NOT NULL AND (woe.context->'event'->>'jpeg_encode_ms') <> ''
    )`;
    const rtspEncodeQuery = `
      SELECT ${pct(encodeExpr)}
      FROM workflow_org_executions woe
      WHERE woe.started_at >= NOW() - INTERVAL '${interval}' AND ${encodePresence}
    `;

    const rtspFailuresQuery = `
      SELECT COUNT(*)::int AS failures
      FROM workflow_org_executions woe
      WHERE woe.started_at >= NOW() - INTERVAL '${interval}'
        AND woe.status = 'failed'
        AND ${cameraKeyExpr} IS NOT NULL
    `;

    const [rtspFramesRes, rtspActive5mRes, rtspDecodeRes, rtspEncodeRes, rtspFailRes] = await Promise.all([
      pool.query(rtspFramesQuery),
      pool.query(rtspActive5mQuery),
      pool.query(rtspDecodeQuery),
      pool.query(rtspEncodeQuery),
      pool.query(rtspFailuresQuery),
    ]);

    const safe = (row) => row && row.count != null ? {
      avg: Number(row.avg || 0),
      p50: Number(row.p50 || 0),
      p95: Number(row.p95 || 0),
      p99: Number(row.p99 || 0),
      count: Number(row.count || 0)
    } : null;

    const overallRow = overallRes.rows[0] || {};
    const overall = safe(overallRow);
    const failedOverall = Number(overallRow.failed || 0);

    const weir = safe(weirRes.rows[0] || {});
    const screener = safe(screenerRes.rows[0] || {});
    const ai = safe(aiRes.rows[0] || {});
    const postgres = safe(postgresRes.rows[0] || {});
    const notification = safe(notificationRes.rows[0] || {});
    const minio = safe(minioRes.rows[0] || {});

    const currentSize = queueCurRes.rows[0]?.current_size || 0;
    const avgSize = queueSeriesRes.rows && queueSeriesRes.rows.length
      ? Math.round(queueSeriesRes.rows.reduce((s, r) => s + Number(r.cnt || 0), 0) / queueSeriesRes.rows.length)
      : 0;
    const peakSize = queueSeriesRes.rows && queueSeriesRes.rows.length
      ? Math.max(...queueSeriesRes.rows.map((r) => Number(r.cnt || 0)))
      : 0;
    const activeWorkersNow = activeWorkersRes.rows[0]?.active_workers || 0;

    const seconds = hours * 3600;

    // Pull live stats from Redis (backdoor backend origin)
    const live = await getLiveStats();
    const rssMb = Math.round((live?.memory?.rss_bytes || 0) / (1024 * 1024));
    const cpuPct = Math.min(100, Math.max(0, Math.round(live?.cpu_usage_percent || 0)));
    const poolSize = live?.workers?.pool_size || activeWorkersNow || 0;
    const liveQueueDepth = live?.workers?.queue_depth || 0;
    const busyRatio = live?.workers?.busy_ratio || 0;

    res.json({
      success: true,
      window: { hours },
      components: {
        'RtspHandler': {
          latency: { unit: 'ms', ...(safe(rtspDecodeRes.rows[0]) || safe(rtspEncodeRes.rows[0]) || {}) },
          throughput: {
            unit: 'frames',
            perSecond: Number(((rtspFramesRes.rows[0]?.frames_total || 0) / seconds).toFixed(2)),
            perMinute: Number((((rtspFramesRes.rows[0]?.frames_total || 0) / seconds) * 60).toFixed(2)),
            total: rtspFramesRes.rows[0]?.frames_total || 0,
          },
          reliability: { failures: rtspFailRes.rows[0]?.failures || 0, activeCameras5m: rtspActive5mRes.rows[0]?.active_5m || 0 },
          resource: { cpuPct, rssMb }, // show process resource here too
        },
        'Overall Pipeline': {
          latency: { unit: 'ms', ...overall, failed: failedOverall },
          throughput: { unit: 'executions', perSecond: overall ? Number((overall.count / seconds).toFixed(2)) : 0, perMinute: overall ? Number(((overall.count / seconds) * 60).toFixed(2)) : 0, total: overall?.count || 0 },
          resource: { cpuPct, rssMb },
        },
        'FrameWeir': {
          latency: { unit: 'ms', ...weir },
          throughput: { unit: 'frames', perSecond: weir ? Number((weir.count / seconds).toFixed(2)) : 0, perMinute: weir ? Number(((weir.count / seconds) * 60).toFixed(2)) : 0, total: weir?.count || 0 },
        },
        'PreScreener': {
          latency: { unit: 'ms', ...screener },
          throughput: { unit: 'frames', perSecond: screener ? Number((screener.count / seconds).toFixed(2)) : 0, perMinute: screener ? Number(((screener.count / seconds) * 60).toFixed(2)) : 0, total: screener?.count || 0 },
        },
        'PerceptionGate': {
          latency: { unit: 'ms', ...ai },
          throughput: { unit: 'inferences', perSecond: ai ? Number((ai.count / seconds).toFixed(2)) : 0, perMinute: ai ? Number(((ai.count / seconds) * 60).toFixed(2)) : 0, total: ai?.count || 0 },
        },
        'Inference Service': {
          latency: { unit: 'ms', ...ai },
          throughput: { unit: 'inferences', perSecond: ai ? Number((ai.count / seconds).toFixed(2)) : 0, perMinute: ai ? Number(((ai.count / seconds) * 60).toFixed(2)) : 0, total: ai?.count || 0 },
        },
        'workflow_runner_queue': {
          reliability: { failures: failedOverall },
          queue: { currentSize: Math.max(currentSize, liveQueueDepth), avgSize, peakSize },
        },
        'WorkerPool': {
          resource: { activeWorkers: poolSize, cpuPct, rssMb },
          queue: { currentSize: liveQueueDepth, avgSize, peakSize },
        },
        'Database Nodes': {
          latency: { unit: 'ms', ...postgres },
        },
        'Notification Nodes': {
          latency: { unit: 'ms', ...notification },
        },
        'Storage Nodes': {
          latency: { unit: 'ms', ...minio },
        },
      },
      generatedAt: new Date().toISOString(),
    });
  } catch (err) {
    console.error('[Benchmark Backend] bench-suite error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── Benchmark — workflow executions per minute / day / week / month
app.get('/api/benchmark', async (req, res) => {
  try {
    const minuteQuery = `
      SELECT
        to_char(date_trunc('minute', started_at), 'YYYY-MM-DD HH24:MI:00') AS time_bucket,
        COUNT(*)::int AS executions
      FROM workflow_org_executions
      WHERE started_at IS NOT NULL AND started_at >= NOW() - INTERVAL '24 hours'
      GROUP BY date_trunc('minute', started_at)
      ORDER BY date_trunc('minute', started_at) ASC
    `;

    const dayQuery = `
      SELECT
        to_char(date_trunc('day', started_at), 'YYYY-MM-DD') AS time_bucket,
        COUNT(*)::int AS executions
      FROM workflow_org_executions
      WHERE started_at IS NOT NULL AND started_at >= NOW() - INTERVAL '30 days'
      GROUP BY date_trunc('day', started_at)
      ORDER BY date_trunc('day', started_at) ASC
    `;

    const weekQuery = `
      SELECT
        to_char(date_trunc('week', started_at), 'YYYY-MM-DD') AS time_bucket,
        COUNT(*)::int AS executions
      FROM workflow_org_executions
      WHERE started_at IS NOT NULL AND started_at >= NOW() - INTERVAL '12 weeks'
      GROUP BY date_trunc('week', started_at)
      ORDER BY date_trunc('week', started_at) ASC
    `;

    const monthQuery = `
      SELECT
        to_char(date_trunc('month', started_at), 'YYYY-MM') AS time_bucket,
        COUNT(*)::int AS executions
      FROM workflow_org_executions
      WHERE started_at IS NOT NULL AND started_at >= NOW() - INTERVAL '12 months'
      GROUP BY date_trunc('month', started_at)
      ORDER BY date_trunc('month', started_at) ASC
    `;

    const summaryQuery = `
      SELECT
        COUNT(*) FILTER (WHERE started_at >= NOW() - INTERVAL '24 hours')::int AS executions_24h,
        COUNT(*) FILTER (WHERE started_at >= NOW() - INTERVAL '7 days')::int  AS executions_7d,
        COUNT(*) FILTER (WHERE started_at >= NOW() - INTERVAL '30 days')::int AS executions_30d,
        COUNT(*)::int AS executions_total
      FROM workflow_org_executions
      WHERE started_at IS NOT NULL
    `;

    const byWorkflowQuery = `
      SELECT
        wc.name AS workflow_name,
        COUNT(*)::int AS executions
      FROM workflow_org_executions woe
      JOIN workflow_catalog wc ON wc.id = woe.catalog_id
      WHERE woe.started_at >= NOW() - INTERVAL '30 days'
      GROUP BY wc.name
      ORDER BY executions DESC
      LIMIT 10
    `;

    const recentQuery = `
      SELECT
        woe.id AS execution_id,
        wc.name AS workflow_name,
        woe.trigger_event,
        woe.status,
        woe.started_at,
        woe.duration_ms
      FROM workflow_org_executions woe
      JOIN workflow_catalog wc ON wc.id = woe.catalog_id
      WHERE woe.started_at IS NOT NULL
      ORDER BY woe.started_at DESC
      LIMIT 25
    `;

    const [perMinute, perDay, perWeek, perMonth, summary, byWorkflow, recent] = await Promise.all([
      pool.query(minuteQuery),
      pool.query(dayQuery),
      pool.query(weekQuery),
      pool.query(monthQuery),
      pool.query(summaryQuery),
      pool.query(byWorkflowQuery),
      pool.query(recentQuery),
    ]);

    res.json({
      success: true,
      perMinute: perMinute.rows,
      perDay: perDay.rows,
      perWeek: perWeek.rows,
      perMonth: perMonth.rows,
      summary: summary.rows[0],
      byWorkflow: byWorkflow.rows,
      recent: recent.rows,
      generatedAt: new Date().toISOString(),
    });
  } catch (err) {
    console.error('[Benchmark Backend] benchmark error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── Startup ───────────────────────────────────────────────────────
async function start() {
  try {
    await pool.query('SELECT 1');
    console.log('[Benchmark Backend] PostgreSQL connected');
  } catch (err) {
    console.error('[Benchmark Backend] PostgreSQL connection failed:', err.message);
    process.exit(1);
  }

  // Connect Redis in background (non-fatal if unavailable)
  initRedis().catch(() => {});

  const server = app.listen(PORT, '0.0.0.0', () => {
    console.log(`\n=============================================================`);
    console.log(`  Benchmark Backend running on http://localhost:${PORT}`);
    console.log(`=============================================================\n`);
  });

  const shutdown = (signal) => {
    console.log(`[Benchmark Backend] ${signal} received. Shutting down...`);
    server.close(async () => {
      await pool.end();
      try { if (redisClient) await redisClient.disconnect(); } catch (_) {}
      process.exit(0);
    });
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

start().catch((err) => {
  console.error('[Benchmark Backend] Fatal startup error:', err.message);
  process.exit(1);
});
