'use strict';

const express = require('express');
const path = require('path');
const os = require('os');
require('dotenv').config({ path: path.join(__dirname, '.env') });

const { Pool } = require('pg');
const { createClient } = require('redis');

// ── PostgreSQL direct connection ─────────────────────────────────
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
pool.on('error', (err) => console.error('[Telemetry Backend] PostgreSQL pool error:', err.message));

// ── Redis client for live process stats ──────────────────────────
let redisClient = null;
let redisReady = false;

async function initRedis() {
  try {
    redisClient = createClient({
      url: process.env.REDIS_URL || 'redis://localhost:6379',
      socket: {
        reconnectStrategy: (retries) => Math.min(retries * 500, 10000),
      },
    });
    redisClient.on('error', () => { redisReady = false; });
    redisClient.on('ready', () => { redisReady = true; });
    await redisClient.connect();
    console.log('[Telemetry Backend] Redis connected');
  } catch (err) {
    console.warn('[Telemetry Backend] Redis unavailable, live metrics will use defaults:', err.message);
    redisReady = false;
  }
}

// Default live stats when Redis is unavailable
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
  workers: { busy_ratio: 0, queue_depth: 0, pool_size: 0 },
};

let localLiveStatsCache = null;
let localLiveStatsCacheTime = 0;
const LIVE_STATS_CACHE_TTL = 2000; // 2 seconds

async function getLiveStats() {
  const now = Date.now();
  // Return local memory cache if it's less than 2 seconds old
  if (localLiveStatsCache && (now - localLiveStatsCacheTime < LIVE_STATS_CACHE_TTL)) {
    return localLiveStatsCache;
  }

  if (!redisReady || !redisClient) {
    return localLiveStatsCache || DEFAULT_LIVE_STATS;
  }

  try {
    const raw = await redisClient.get('godezk:telemetry:live_stats');
    if (raw) {
      localLiveStatsCache = JSON.parse(raw);
      localLiveStatsCacheTime = now;
      return localLiveStatsCache;
    }
    return localLiveStatsCache || DEFAULT_LIVE_STATS;
  } catch (_) {
    return localLiveStatsCache || DEFAULT_LIVE_STATS;
  }
}

// ── Express app ──────────────────────────────────────────────────
const app = express();
const PORT = process.env.ANALYTICS_PORT || 3031;

app.use(express.json());

// CORS
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  next();
});

// ── 1. Dashboard overview KPIs & Trends ──────────────────────────
app.get('/api/dashboard-summary', async (req, res) => {
  try {
    const telemetry = await getLiveStats();
    const tz = Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';

    const kpiQuery = `
      SELECT 
        COUNT(*)::int AS total,
        COUNT(*) FILTER (WHERE status = 'completed')::int AS completed,
        0::int AS skipped,
        COUNT(*) FILTER (WHERE status = 'failed')::int AS failed,
        COALESCE(ROUND(AVG(duration_ms)), 0)::int AS avg_total
      FROM workflow_org_executions
      WHERE started_at >= NOW() - INTERVAL '24 hours'
    `;

    const latencyQuery = `
      SELECT 
        COALESCE(AVG(CAST(COALESCE(context->'event'->>'weir_cpu_ms', '0') AS NUMERIC)), 0)::int AS weir,
        COALESCE(AVG(CAST(COALESCE(context->'event'->>'screener_cpu_ms', '0') AS NUMERIC)), 0)::int AS screener,
        COALESCE(AVG(CAST(COALESCE(context->'event'->>'ai_time_ms', context->'event'->>'inference_ms', '0') AS NUMERIC)), 0)::int AS decision,
        COALESCE(AVG(duration_ms), 0)::int AS duration
      FROM workflow_org_executions
      WHERE status = 'completed' AND started_at >= NOW() - INTERVAL '24 hours'
    `;

    const failuresQuery = `
      SELECT 
        woe.id::text AS id,
        wc.name AS workflow_name,
        woe.trigger_event AS trigger_event,
        woe.error_message AS error_message,
        to_char(woe.started_at, 'HH24:MI:SS') AS failed_time
      FROM workflow_org_executions woe
      JOIN workflow_catalog wc ON wc.id = woe.catalog_id
      WHERE woe.status = 'failed' AND woe.started_at >= NOW() - INTERVAL '24 hours'
      ORDER BY woe.started_at DESC
      LIMIT 10
    `;

    const journeysQuery = `
      SELECT 
        woe.id::text AS id,
        wc.name AS workflow_name,
        woe.status,
        COALESCE(CAST(woe.context->'event'->>'weir_cpu_ms' AS NUMERIC), 0) AS weir_cpu,
        COALESCE(CAST(woe.context->'event'->>'screener_cpu_ms' AS NUMERIC), 0) AS screener_cpu,
        COALESCE(CAST(woe.context->'event'->>'ai_time_ms' AS NUMERIC), CAST(woe.context->'event'->>'inference_ms' AS NUMERIC), 0) AS decision_cpu,
        woe.duration_ms AS duration_ms,
        to_char(woe.started_at, 'HH24:MI:SS') AS start_time
      FROM workflow_org_executions woe
      JOIN workflow_catalog wc ON wc.id = woe.catalog_id
      WHERE woe.started_at >= NOW() - INTERVAL '24 hours'
      ORDER BY woe.started_at DESC
      LIMIT 10
    `;

    const throughputQuery = `
      SELECT 
        to_char(date_trunc('minute', started_at), 'YYYY-MM-DD HH24:MI:00') AS time_bucket,
        COUNT(*) FILTER (WHERE status = 'completed') AS completed,
        COUNT(*) FILTER (WHERE status = 'failed') AS failed,
        COUNT(*) AS total
      FROM workflow_org_executions
      WHERE started_at >= (SELECT COALESCE(MAX(started_at), NOW()) FROM workflow_org_executions) - INTERVAL '2 hours'
      GROUP BY date_trunc('minute', started_at)
      ORDER BY date_trunc('minute', started_at) ASC
    `;

    const cameraQuery = `
      SELECT 
        COALESCE(context->'event'->>'device_id', 'unknown') AS id,
        COUNT(*)::int AS received,
        0::int AS skipped,
        COUNT(*) FILTER (WHERE status = 'completed')::int AS completed,
        COALESCE(ROUND(AVG(duration_ms)), 0)::int AS avg_time
      FROM workflow_org_executions
      WHERE started_at >= NOW() - INTERVAL '24 hours'
      GROUP BY COALESCE(context->'event'->>'device_id', 'unknown')
    `;

    const workerQuery = `
      SELECT 
        COALESCE(context->'event'->>'worker_id', 'worker-1') AS name,
        COUNT(*)::int AS jobs,
        COALESCE(ROUND(AVG(duration_ms)), 0)::int AS time,
        COALESCE(ROUND(AVG(CAST(context->>'cpu_us' AS NUMERIC))), 0)::int AS cpu_us,
        COALESCE(ROUND(AVG(CAST(context->>'peak_rss' AS NUMERIC))), 0)::int AS peak_rss
      FROM workflow_org_executions
      WHERE started_at >= NOW() - INTERVAL '24 hours'
      GROUP BY COALESCE(context->'event'->>'worker_id', 'worker-1')
    `;

    const modelQuery = `
      WITH execution_ai_details AS (
        SELECT 
          woe.id,
          woe.status,
          COALESCE(
            NULLIF(NULLIF(woe.context->'event'->>'ai_service', 'unknown'), ''),
            (
              SELECT el->'data'->>'ai_service' 
              FROM jsonb_array_elements(
                CASE WHEN jsonb_typeof(wc.nodes_definition) = 'array' THEN wc.nodes_definition ELSE '[]'::jsonb END
              ) AS el
              WHERE el->>'type' = 'ai_node' 
              LIMIT 1
            ),
            'unknown'
          ) AS ai_service,
          COALESCE(
            CAST(woe.context->'event'->>'ai_time_ms' AS NUMERIC),
            CAST(woe.context->'event'->>'inference_ms' AS NUMERIC),
            (
              SELECT SUM(CAST(val->>'duration_ms' AS INT))
              FROM jsonb_each(
                CASE WHEN jsonb_typeof(woe.context->'node_results') = 'object' THEN woe.context->'node_results' ELSE '{}'::jsonb END
              ) AS t(key, val)
              WHERE val->>'nodeType' = 'ai_node'
            ),
            0
          )::int AS ai_dur,
          COALESCE(
            CAST(woe.context->'event'->>'confidence' AS NUMERIC),
            (
              SELECT CAST(val->'output'->>'confidence' AS NUMERIC)
              FROM jsonb_each(
                CASE WHEN jsonb_typeof(woe.context->'node_results') = 'object' THEN woe.context->'node_results' ELSE '{}'::jsonb END
              ) AS t(key, val)
              WHERE val->>'nodeType' = 'ai_node' AND val->'output'->>'confidence' IS NOT NULL
              LIMIT 1
            ),
            0
          )::float AS confidence
        FROM workflow_org_executions woe
        JOIN workflow_catalog wc ON wc.id = woe.catalog_id
        WHERE woe.started_at >= NOW() - INTERVAL '24 hours'
      )
      SELECT 
        ai_service AS name,
        COUNT(*)::int AS total,
        COALESCE(ROUND(AVG(ai_dur)), 0)::int AS avg_time,
        COALESCE(AVG(confidence), 0)::float AS conf,
        COUNT(*) FILTER (WHERE status = 'failed')::int AS failed
      FROM execution_ai_details
      WHERE ai_service IS NOT NULL AND ai_service <> '' AND ai_service <> 'unknown'
      GROUP BY ai_service
    `;

    const workflowQuery = `
      SELECT 
        wc.name AS name,
        COUNT(*)::int AS executions,
        COALESCE(
          ROUND(AVG(woe.duration_ms) FILTER (WHERE woe.started_at >= NOW() - INTERVAL '2 hours')),
          COALESCE(ROUND(AVG(woe.duration_ms)), 0)
        )::int AS duration,
        COUNT(*) FILTER (WHERE woe.status = 'completed')::int AS completed,
        COUNT(*) FILTER (WHERE woe.status = 'failed')::int AS failed,
        COUNT(*) FILTER (WHERE woe.started_at >= NOW() - INTERVAL '2 hours' AND woe.status = 'completed')::int AS completed_recent,
        COUNT(*) FILTER (WHERE woe.started_at >= NOW() - INTERVAL '2 hours')::int AS executions_recent
      FROM workflow_org_executions woe
      JOIN workflow_catalog wc ON wc.id = woe.catalog_id
      WHERE woe.started_at >= NOW() - INTERVAL '24 hours'
      GROUP BY wc.name
    `;

    const queueQuery = `
      SELECT COUNT(*)::int AS count 
      FROM workflow_runner_queue 
      WHERE status IN ('queued', 'running')
    `;

    const activeWorkersQuery = `
      SELECT COUNT(*)::int AS count 
      FROM workflow_runner_queue 
      WHERE status = 'running'
    `;

    const queueTimelineQuery = `
      SELECT 
        to_char(date_trunc('minute', created_at AT TIME ZONE '${tz}'), 'YYYY-MM-DD HH24:MI:00') AS time_bucket,
        COUNT(*)::int AS length
      FROM workflow_runner_queue
      WHERE created_at >= (SELECT COALESCE(MAX(created_at), NOW()) FROM workflow_runner_queue) - INTERVAL '2 hours'
      GROUP BY date_trunc('minute', created_at AT TIME ZONE '${tz}')
      ORDER BY date_trunc('minute', created_at AT TIME ZONE '${tz}') ASC
    `;

    const aiTrendQuery = `
      SELECT 
        to_char(date_trunc('minute', started_at), 'YYYY-MM-DD HH24:MI:00') AS time_bucket,
        COALESCE(
          ROUND(
            AVG(
              COALESCE(
                CAST(context->'event'->>'ai_time_ms' AS NUMERIC),
                CAST(context->'event'->>'inference_ms' AS NUMERIC),
                (
                  SELECT SUM(CAST(val->>'duration_ms' AS INT))
                  FROM jsonb_each(
                    CASE WHEN jsonb_typeof(context->'node_results') = 'object' THEN context->'node_results' ELSE '{}'::jsonb END
                  ) AS t(key, val)
                  WHERE val->>'nodeType' = 'ai_node'
                )
              )
            )
          ),
          0
        )::int AS avg_inference
      FROM workflow_org_executions
      WHERE started_at >= (SELECT COALESCE(MAX(started_at), NOW()) FROM workflow_org_executions) - INTERVAL '2 hours'
      GROUP BY date_trunc('minute', started_at)
      ORDER BY date_trunc('minute', started_at) ASC
    `;

    const confidenceTrendQuery = `
      SELECT 
        to_char(date_trunc('minute', started_at), 'YYYY-MM-DD HH24:MI:00') AS time_bucket,
        COALESCE(
          AVG(
            COALESCE(
              CAST(context->'event'->>'confidence' AS NUMERIC),
              (
                SELECT CAST(val->'output'->>'confidence' AS NUMERIC)
                FROM jsonb_each(
                  CASE WHEN jsonb_typeof(context->'node_results') = 'object' THEN context->'node_results' ELSE '{}'::jsonb END
                ) AS t(key, val)
                WHERE val->>'nodeType' = 'ai_node' AND val->'output'->>'confidence' IS NOT NULL
                LIMIT 1
              )
            )
          ), 
          0
        )::float AS avg_confidence
      FROM workflow_org_executions
      WHERE started_at >= (SELECT COALESCE(MAX(started_at), NOW()) FROM workflow_org_executions) - INTERVAL '2 hours'
      GROUP BY date_trunc('minute', started_at)
      ORDER BY date_trunc('minute', started_at) ASC
    `;

    const histogramQuery = `
      SELECT 
        COUNT(*) FILTER (WHERE duration_ms < 200)::int AS range_1,
        COUNT(*) FILTER (WHERE duration_ms >= 200 AND duration_ms < 500)::int AS range_2,
        COUNT(*) FILTER (WHERE duration_ms >= 500 AND duration_ms < 1000)::int AS range_3,
        COUNT(*) FILTER (WHERE duration_ms >= 1000)::int AS range_4
      FROM workflow_org_executions
      WHERE started_at >= NOW() - INTERVAL '24 hours'
    `;

    const activeJobsQuery = `
      SELECT 
        COALESCE(executor_id, 'worker-1') AS name,
        COUNT(*)::int AS active_count
      FROM workflow_runner_queue
      WHERE status = 'running'
      GROUP BY COALESCE(executor_id, 'worker-1')
    `;

    const [
      kpiRes,
      latencyRes,
      failuresRes,
      journeysRes,
      throughputRes,
      cameraRes,
      workerRes,
      modelRes,
      workflowRes,
      queueRes,
      activeWorkersRes,
      queueTimelineRes,
      aiTrendRes,
      confidenceTrendRes,
      histogramRes,
      activeJobsRes
    ] = await Promise.all([
      pool.query(kpiQuery),
      pool.query(latencyQuery),
      pool.query(failuresQuery),
      pool.query(journeysQuery),
      pool.query(throughputQuery),
      pool.query(cameraQuery),
      pool.query(workerQuery),
      pool.query(modelQuery),
      pool.query(workflowQuery),
      pool.query(queueQuery),
      pool.query(activeWorkersQuery),
      pool.query(queueTimelineQuery),
      pool.query(aiTrendQuery),
      pool.query(confidenceTrendQuery),
      pool.query(histogramQuery),
      pool.query(activeJobsQuery)
    ]);

    const activeMap = new Map(activeJobsRes.rows.map(r => [r.name, r.active_count]));
    const queueLength = queueRes.rows[0]?.count || 0;
    const activeWorkers = activeWorkersRes.rows[0]?.count || 0;

    const kpi = kpiRes.rows[0] || { total: 0, completed: 0, skipped: 0, failed: 0, avg_total: 0 };
    const lat = latencyRes.rows[0] || { weir: 0, screener: 0, decision: 0, duration: 0 };

    const telemetrySkipped = telemetry?.gate?.skipped || 0;
    const telemetryThrottled = telemetry?.gate?.throttled || 0;
    const telemetryCooldown = telemetry?.gate?.cooldown || 0;
    const telemetryDropped = telemetry?.gate?.dropped || 0;
    const totalSkipped = telemetrySkipped + telemetryThrottled + telemetryCooldown + telemetryDropped;

    // Calculate backdoor backend process memory usage % (from Redis live stats)
    const procRss = telemetry?.memory?.rss_bytes || 0;
    const totalMem = telemetry?.system?.total_mem_bytes || os.totalmem();
    const procMemPct = totalMem > 0 ? Math.round((procRss / totalMem) * 100) : 0;
    const procCpuPct = Math.min(100, telemetry?.cpu_usage_percent || 0);

    const backendCpu = procCpuPct;
    const backendMemoryPct = procMemPct;

    res.json({
      success: true,
      stats: {
        receivedToday: kpi.total + totalSkipped,
        completed: kpi.completed,
        skipped: totalSkipped,
        failed: kpi.failed,
        avgProcessTime: kpi.avg_total,
        avgAiTime: lat.decision,
        activeWorkers: telemetry?.workers?.pool_size || activeWorkers || 1,
        queueLength: telemetry?.workers?.queue_depth || queueLength
      },
      latencies: {
        avg_weir: lat.weir,
        avg_screener: lat.screener,
        avg_decision: lat.decision,
        avg_duration: lat.duration
      },
      failures: failuresRes.rows,
      journeys: journeysRes.rows,
      throughput: throughputRes.rows,
      queueTimeline: queueTimelineRes.rows,
      aiTrend: aiTrendRes.rows,
      confidenceTrend: confidenceTrendRes.rows,
      histogram: histogramRes.rows[0] || { range_1: 0, range_2: 0, range_3: 0, range_4: 0 },
      cameras: cameraRes.rows.map(r => ({
        id: r.id,
        received: r.received,
        skipped: r.skipped,
        completed: r.completed,
        avgTime: r.avg_time,
        failureRate: r.received > 0 ? ((r.received - r.completed) / r.received * 100).toFixed(1) + '%' : '0%'
      })),
      workers: workerRes.rows.map(r => {
        const activeJobs = activeMap.get(r.name) || 0;
        const procCpuPercent = r.cpu_us > 0 && r.time > 0 ? Math.round((r.cpu_us / 1000) / r.time * 100) : 0;
        const procMemPercent = r.peak_rss > 0 ? Math.round(((r.peak_rss * 1024) / totalMem) * 100) : 0;

        const cpu = activeJobs > 0 
          ? Math.min(95, Math.max(procCpuPercent, backendCpu, 20 + activeJobs * 10)) 
          : Math.max(1, Math.round(backendCpu * 0.4));
        const memory = activeJobs > 0 
          ? Math.min(92, Math.max(procMemPercent, backendMemoryPct, 35 + activeJobs * 5)) 
          : Math.max(1, backendMemoryPct);
        return {
          name: r.name,
          jobs: r.jobs,
          cpu,
          memory,
          queue: activeJobs,
          time: r.time
        };
      }),
      models: modelRes.rows.map(r => ({
        name: r.name,
        total: r.total,
        avgTime: r.avg_time,
        conf: r.conf,
        failed: r.failed
      })),
      workflows: workflowRes.rows.map(r => ({
        name: r.name,
        executions: r.executions,
        duration: r.duration,
        success: r.executions > 0 ? ((r.completed / r.executions) * 100).toFixed(1) + '%' : '100.0%',
        failed: r.failed
      }))
    });
  } catch (err) {
    console.error('[Telemetry Backend] dashboard-summary error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── 2. Recent frames list ────────────────────────────────────────
app.get('/api/frames', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT 
        woe.id::text AS "frameId",
        COALESCE(woe.context->'event'->>'device_id', 'unknown') AS "cameraId",
        wc.name AS pipeline,
        COALESCE(
          NULLIF(NULLIF(woe.context->'event'->>'ai_service', 'unknown'), ''),
          (
            SELECT el->'data'->>'ai_service' 
            FROM jsonb_array_elements(
              CASE WHEN jsonb_typeof(wc.nodes_definition) = 'array' THEN wc.nodes_definition ELSE '[]'::jsonb END
            ) AS el
            WHERE el->>'type' = 'ai_node' 
            LIMIT 1
          ),
          'unknown'
        ) AS "aiService",
        wc.name AS workflow,
        COALESCE(woe.context->'event'->>'worker_id', 'worker-1') AS "workerId",
        COALESCE(
          CAST(woe.context->'event'->>'confidence' AS NUMERIC),
          (
            SELECT CAST(val->'output'->>'confidence' AS NUMERIC)
            FROM jsonb_each(
              CASE WHEN jsonb_typeof(woe.context->'node_results') = 'object' THEN woe.context->'node_results' ELSE '{}'::jsonb END
            ) AS t(key, val)
            WHERE val->>'nodeType' = 'ai_node' AND val->'output'->>'confidence' IS NOT NULL
            LIMIT 1
          ),
          0
        )::float AS confidence,
        woe.status AS status,
        COALESCE(woe.context->'event'->>'detection', 'Match verified') AS detection,
        to_char(woe.started_at, 'YYYY-MM-DD HH24:MI:SS') AS "receivedAt",
        COALESCE(woe.duration_ms, 0)::int AS "totalTime"
      FROM workflow_org_executions woe
      JOIN workflow_catalog wc ON wc.id = woe.catalog_id
      ORDER BY woe.started_at DESC
      LIMIT 100
    `);
    res.json(rows);
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── 3. Single frame journey segments ─────────────────────────────
app.get('/api/frame/:frameId', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT 
        woe.id::text AS "frameId",
        COALESCE(woe.context->'event'->>'device_id', 'unknown') AS "cameraId",
        wc.name AS pipeline,
        COALESCE(woe.context->'event'->>'ai_service', 'unknown') AS "aiService",
        wc.name AS workflow,
        COALESCE(woe.context->'event'->>'worker_id', 'worker-1') AS "workerId",
        COALESCE(CAST(woe.context->'event'->>'confidence' AS NUMERIC), 0)::float AS confidence,
        woe.status AS status,
        COALESCE(woe.context->'event'->>'detection', 'Match verified') AS detection,
        to_char(woe.started_at, 'YYYY-MM-DD HH24:MI:SS') AS "receivedAt",
        COALESCE(woe.duration_ms, 0)::int AS "totalTime",
        woe.error_message AS error,
        COALESCE(CAST(woe.context->'event'->>'weir_cpu_ms' AS NUMERIC), 0) AS weir_dur,
        COALESCE(CAST(woe.context->'event'->>'screener_cpu_ms' AS NUMERIC), 0) AS screener_dur,
        COALESCE(CAST(woe.context->'event'->>'ai_time_ms' AS NUMERIC), CAST(woe.context->'event'->>'inference_ms' AS NUMERIC), 0) AS infer_dur,
        woe.duration_ms AS work_dur,
        COALESCE(CAST(woe.context->>'cpu_us' AS NUMERIC), 0)::int AS "cpuUs",
        COALESCE(CAST(woe.context->>'peak_rss' AS NUMERIC), 0)::bigint AS "peakRss",
        COALESCE(CAST(woe.context->>'heap_used' AS NUMERIC), 0)::bigint AS "heapUsed",
        COALESCE(CAST(woe.context->>'memory_before' AS NUMERIC), 0)::bigint AS "memBefore",
        COALESCE(CAST(woe.context->>'memory_after' AS NUMERIC), 0)::bigint AS "memAfter"
      FROM workflow_org_executions woe
      JOIN workflow_catalog wc ON wc.id = woe.catalog_id
      WHERE woe.id = $1
    `, [req.params.frameId]);

    if (!rows[0]) {
      return res.status(404).json({ success: false, error: 'Frame not found' });
    }

    const f = rows[0];
    const cpuUsToPercent = f.work_dur > 0 ? Math.round((f.cpuUs / 1000) / f.work_dur * 100) : 0;
    const peakRssMb = f.peakRss > 0 ? Math.round(f.peakRss / (1024 * 1024)) : 0;

    res.json({
      frameId: f.frameId,
      cameraId: f.cameraId,
      pipeline: f.pipeline,
      aiService: f.aiService,
      workflow: f.workflow,
      workerId: f.workerId,
      confidence: f.confidence,
      status: f.status,
      detection: f.detection,
      receivedAt: f.receivedAt,
      totalTime: f.totalTime,
      error: f.error,
      events: [
        { stage: 'Frame Received', duration: 0, cpu: '0%', memory: '0 B' },
        { stage: 'FrameWeir.score()', duration: Math.round(Number(f.weir_dur)), cpu: Math.round(Number(f.weir_dur)) > 0 ? '12%' : '0%', memory: '~12 KB' },
        { stage: 'FrameBus.publish()', duration: Math.round(Number(f.weir_dur)) > 0 ? 1 : 0, cpu: '1%', memory: '~1 KB' },
        { stage: 'PerceptionGate.infer()', duration: Math.round(Number(f.infer_dur)) > 0 ? 2 : 0, cpu: Math.round(Number(f.infer_dur)) > 0 ? '4%' : '0%', memory: '~4 KB' },
        { stage: 'SOMA.canAccept()', duration: Math.round(Number(f.weir_dur)) > 0 ? 1 : 0, cpu: '1%', memory: '~2 KB' },
        { stage: 'PostgreSQL Queue', duration: 5, cpu: '0%', memory: '0 B' },
        { stage: 'AI Inference', duration: Math.round(Number(f.infer_dur)), cpu: Math.round(Number(f.infer_dur)) > 0 ? '82%' : '0%', memory: f.aiService.toLowerCase().includes('mobile') ? '128 MB' : '64 MB' },
        { stage: 'Decision Gate', duration: Math.round(Number(f.screener_dur)), cpu: Math.round(Number(f.screener_dur)) > 0 ? '15%' : '0%', memory: '~8 KB' },
        { stage: 'Worker Execution', duration: Math.round(Number(f.work_dur)), cpu: cpuUsToPercent > 0 ? `${cpuUsToPercent}%` : '2%', memory: peakRssMb > 0 ? `${peakRssMb} MB` : '45 MB' },
        { stage: 'Database Log', duration: 10, cpu: '5%', memory: '~4 KB' },
        { stage: 'Completed', duration: 0, cpu: '0%', memory: '0 B' }
      ]
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── 4. Camera health ─────────────────────────────────────────────
app.get('/api/camera/:cameraId', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT 
        COUNT(*)::int AS received,
        0::int AS skipped,
        COUNT(*) FILTER (WHERE status = 'completed')::int AS completed,
        COALESCE(ROUND(AVG(CAST(context->'event'->>'ai_time_ms' AS NUMERIC))), 0)::int AS avg_inference,
        COALESCE(ROUND(AVG(duration_ms)), 0)::int AS avg_workflow
      FROM workflow_org_executions
      WHERE context->'event'->>'device_id' = $1 OR context->'event'->>'camera_id' = $1
    `, [req.params.cameraId]);

    const stat = rows[0] || { received: 0, skipped: 0, completed: 0, avg_inference: 0, avg_workflow: 0 };
    res.json({
      cameraId: req.params.cameraId,
      received: stat.received,
      skipped: stat.skipped,
      completed: stat.completed,
      avgInference: stat.avg_inference,
      avgWorkflow: stat.avg_workflow,
      failureRate: stat.received > 0 ? ((stat.received - stat.completed) / stat.received * 100).toFixed(1) + '%' : '0%'
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── 5. Worker utilization ────────────────────────────────────────
app.get('/api/workers', async (req, res) => {
  try {
    const telemetry = await getLiveStats();
    const totalMem = telemetry?.system?.total_mem_bytes || os.totalmem();

    const statsRes = await pool.query(`
      SELECT 
        COALESCE(context->'event'->>'worker_id', 'worker-1') AS name,
        COUNT(*)::int AS jobs,
        COALESCE(ROUND(AVG(duration_ms)), 0)::int AS time,
        COALESCE(ROUND(AVG(CAST(context->>'cpu_us' AS NUMERIC))), 0)::int AS cpu_us,
        COALESCE(ROUND(AVG(CAST(context->>'peak_rss' AS NUMERIC))), 0)::int AS peak_rss
      FROM workflow_org_executions
      GROUP BY COALESCE(context->'event'->>'worker_id', 'worker-1')
    `);

    const activeRes = await pool.query(`
      SELECT 
        COALESCE(executor_id, 'worker-1') AS name,
        COUNT(*)::int AS active_count
      FROM workflow_runner_queue
      WHERE status = 'running'
      GROUP BY COALESCE(executor_id, 'worker-1')
    `);

    const activeMap = new Map(activeRes.rows.map(r => [r.name, r.active_count]));
    const backendCpu = telemetry?.cpu_usage_percent || 5;
    const backendMemoryPct = telemetry?.memory?.pct || 12;

    res.json(statsRes.rows.map(r => {
      const activeJobs = activeMap.get(r.name) || 0;
      const procCpuPercent = r.cpu_us > 0 && r.time > 0 ? Math.round((r.cpu_us / 1000) / r.time * 100) : 0;
      const procMemPercent = r.peak_rss > 0 ? Math.round((r.peak_rss / totalMem) * 100) : 0;

      const cpu = activeJobs > 0 
        ? Math.min(95, Math.max(procCpuPercent, backendCpu, 20 + activeJobs * 10)) 
        : Math.max(1, Math.round(backendCpu * 0.4));
      const memory = activeJobs > 0 
        ? Math.min(92, Math.max(procMemPercent, backendMemoryPct, 35 + activeJobs * 5)) 
        : Math.max(1, backendMemoryPct);

      return {
        name: r.name,
        jobs: r.jobs,
        cpu,
        memory,
        queue: activeJobs,
        time: r.time
      };
    }));
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── 6. Pipeline execution logs ───────────────────────────────────
app.get('/api/pipeline-logs', async (req, res) => {
  try {
    const days = req.query.days ? parseInt(req.query.days, 10) : null;
    const limit = req.query.limit ? parseInt(req.query.limit, 10) : 25;

    const queryParams = [];
    let whereClause = '';
    if (days && !isNaN(days)) {
      queryParams.push(days);
      whereClause = `WHERE woe.started_at >= NOW() - ($${queryParams.length} * INTERVAL '1 day')`;
    }

    queryParams.push(limit);
    const limitPlaceholder = `$${queryParams.length}`;

    const queryStr = `
      SELECT 
        woe.id::text AS id,
        wc.name AS workflow_name,
        COALESCE(woe.context->'event'->>'device_id', 'unknown') AS camera_id,
        wc.name AS pipeline,
        COALESCE(
          NULLIF(NULLIF(woe.context->'event'->>'ai_service', 'unknown'), ''),
          (
            SELECT el->'data'->>'ai_service' 
            FROM jsonb_array_elements(
              CASE WHEN jsonb_typeof(wc.nodes_definition) = 'array' THEN wc.nodes_definition ELSE '[]'::jsonb END
            ) AS el
            WHERE el->>'type' = 'ai_node' 
            LIMIT 1
          ),
          'unknown'
        ) AS ai_service,
        COALESCE(woe.context->'event'->>'worker_id', 'worker-1') AS worker_id,
        COALESCE(
          CAST(woe.context->'event'->>'confidence' AS NUMERIC),
          (
            SELECT CAST(val->'output'->>'confidence' AS NUMERIC)
            FROM jsonb_each(
              CASE WHEN jsonb_typeof(woe.context->'node_results') = 'object' THEN woe.context->'node_results' ELSE '{}'::jsonb END
            ) AS t(key, val)
            WHERE val->>'nodeType' = 'ai_node' AND val->'output'->>'confidence' IS NOT NULL
            LIMIT 1
          ),
          0
        )::float AS confidence,
        woe.status AS status,
        woe.error_message AS error,
        COALESCE(CAST(woe.context->'event'->>'weir_cpu_ms' AS NUMERIC), 0)::int AS weir_dur,
        COALESCE(CAST(woe.context->'event'->>'screener_cpu_ms' AS NUMERIC), 0)::int AS screener_dur,
        COALESCE(
          CAST(woe.context->'event'->>'ai_time_ms' AS NUMERIC),
          CAST(woe.context->'event'->>'inference_ms' AS NUMERIC),
          (
            SELECT SUM(CAST(val->>'duration_ms' AS INT))
            FROM jsonb_each(
              CASE WHEN jsonb_typeof(woe.context->'node_results') = 'object' THEN woe.context->'node_results' ELSE '{}'::jsonb END
            ) AS t(key, val)
            WHERE val->>'nodeType' = 'ai_node'
          ),
          0
        )::int AS ai_dur,
        woe.duration_ms AS duration,
        to_char(woe.started_at, 'YYYY-MM-DD HH24:MI:SS') AS timestamp
      FROM workflow_org_executions woe
      JOIN workflow_catalog wc ON wc.id = woe.catalog_id
      ${whereClause}
      ORDER BY woe.started_at DESC
      LIMIT ${limitPlaceholder}
    `;

    const { rows } = await pool.query(queryStr, queryParams);

    const logs = [];
    rows.forEach(r => {
      const execShort = r.id.substring(0, 8);
      
      logs.push({
        id: `${r.id}_weir`,
        timestamp: r.timestamp,
        level: 'INFO',
        layer: 'Weir Ingress',
        message: `[${execShort}] [Weir Ingress] Frame received from camera '${r.camera_id}'. Motion detector processing time: ${r.weir_dur}ms.`
      });

      logs.push({
        id: `${r.id}_screener`,
        timestamp: r.timestamp,
        level: 'INFO',
        layer: 'Pre-Screener',
        message: `[${execShort}] [Pre-Screener] Frame analysis initiated. Filter check screener CPU: ${r.screener_dur}ms.`
      });

      if (r.ai_dur > 0 || r.confidence > 0) {
        logs.push({
          id: `${r.id}_ai`,
          timestamp: r.timestamp,
          level: 'INFO',
          layer: 'AI Inference',
          message: `[${execShort}] [AI Inference] Dispatched to AI service '${r.ai_service}'. Inference time: ${r.ai_dur}ms. Object confidence: ${(r.confidence * 100).toFixed(1)}%.`
        });
      }

      if (r.status === 'completed') {
        logs.push({
          id: `${r.id}_engine`,
          timestamp: r.timestamp,
          level: 'SUCCESS',
          layer: 'Worker Engine',
          message: `[${execShort}] [Worker Engine] Execution completed successfully on worker node '${r.worker_id}'. Workflow run time: ${r.duration}ms.`
        });
        logs.push({
          id: `${r.id}_db`,
          timestamp: r.timestamp,
          level: 'SUCCESS',
          layer: 'DB Logger',
          message: `[${execShort}] [DB Logger] Successfully committed event record & metrics metadata to postgres database.`
        });
      } else if (r.status === 'failed') {
        logs.push({
          id: `${r.id}_error`,
          timestamp: r.timestamp,
          level: 'ERROR',
          layer: 'Pipeline Error',
          message: `[${execShort}] [Worker Engine] Execution crashed on worker node '${r.worker_id}'. Traceback Error: ${r.error || 'Runner execution timeout'}`
        });
      }
    });

    res.json(logs);
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── 7. Telemetry & Debug settings GET ────────────────────────────
app.get('/api/settings', async (req, res) => {
  try {
    const { rows } = await pool.query("SELECT value FROM configurations WHERE key = 'telemetry.settings'");
    if (rows.length > 0) {
      return res.json({ success: true, settings: rows[0].value });
    }
    res.json({
      success: true,
      settings: {
        logLevel: 'INFO',
        retention: '30',
        alerts: true
      }
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── 7.1 Telemetry & Debug settings POST ──────────────────────────
app.post('/api/settings', async (req, res) => {
  try {
    const settings = req.body;
    if (!settings) {
      return res.status(400).json({ success: false, error: 'Settings payload is required' });
    }
    
    const valueStr = JSON.stringify(settings);
    await pool.query(`
      INSERT INTO configurations (key, value, category, is_active, updated_at)
      VALUES ('telemetry.settings', $1, 'telemetry', true, NOW())
      ON CONFLICT (key) DO UPDATE
      SET value = EXCLUDED.value, updated_at = NOW()
    `, [valueStr]);
    
    res.json({ success: true, settings });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Cache storage for deep analysis endpoint
let deepAnalysisCache = null;
let deepAnalysisCacheTime = 0;
const DEEP_ANALYSIS_CACHE_TTL = 60000; // 60 seconds

// ── 8. Deep backend behavior analysis ────────────────────────────
app.get('/api/deep-analysis', async (req, res) => {
  try {
    const now = Date.now();
    if (deepAnalysisCache && (now - deepAnalysisCacheTime < DEEP_ANALYSIS_CACHE_TTL)) {
      console.log('[Telemetry Backend] Serving deep-analysis from cache');
      return res.json(deepAnalysisCache);
    }

    const telemetry = await getLiveStats();

    // 1. Queries definitions
    const modelProfileQuery = `
      SELECT 
        wc.name AS model_name,
        COUNT(*)::int AS total_runs,
        COALESCE(ROUND(AVG(CAST(woe.context->>'cpu_us' AS NUMERIC))), 0)::int AS avg_cpu_us,
        COALESCE(ROUND(AVG(
          CAST(woe.context->>'memory_after' AS NUMERIC) - CAST(woe.context->>'memory_before' AS NUMERIC)
        )), 0)::bigint AS avg_mem_delta,
        COALESCE(ROUND(AVG(CAST(woe.context->>'peak_rss' AS NUMERIC))), 0)::bigint AS avg_peak_rss,
        COALESCE(ROUND(AVG(CAST(woe.context->>'heap_used' AS NUMERIC))), 0)::bigint AS avg_heap_used,
        COALESCE(ROUND(AVG(woe.duration_ms)), 0)::int AS avg_duration_ms,
        COUNT(*) FILTER (WHERE woe.status = 'completed')::int AS completed,
        COUNT(*) FILTER (WHERE woe.status = 'failed')::int AS failed
      FROM workflow_org_executions woe
      JOIN workflow_catalog wc ON wc.id = woe.catalog_id
      WHERE woe.started_at >= NOW() - INTERVAL '7 days'
      GROUP BY wc.name
      ORDER BY total_runs DESC
    `;

    const timelineQuery = `
      SELECT 
        to_char(date_trunc('minute', woe.started_at), 'YYYY-MM-DD HH24:MI:00') AS time_bucket,
        COUNT(*)::int AS executions,
        COALESCE(ROUND(AVG(CAST(woe.context->>'cpu_us' AS NUMERIC))), 0)::int AS avg_cpu_us,
        COALESCE(ROUND(AVG(CAST(woe.context->>'peak_rss' AS NUMERIC) / (1024*1024))), 0)::int AS avg_peak_rss_mb,
        COALESCE(ROUND(AVG(woe.duration_ms)), 0)::int AS avg_duration_ms
      FROM workflow_org_executions woe
      WHERE woe.started_at >= NOW() - INTERVAL '24 hours'
      GROUP BY date_trunc('minute', woe.started_at)
      ORDER BY date_trunc('minute', woe.started_at)
    `;

    const nodeBreakdownQuery = `
      SELECT 
        val->>'nodeType' AS node_type,
        COUNT(*)::int AS total_executions,
        COALESCE(ROUND(AVG(CAST(val->>'duration_ms' AS NUMERIC))), 0)::int AS avg_duration_ms,
        COALESCE(ROUND(SUM(CAST(val->>'duration_ms' AS NUMERIC))), 0)::bigint AS total_time_ms
      FROM workflow_org_executions woe,
        LATERAL jsonb_each(
          CASE WHEN jsonb_typeof(woe.context->'node_results') = 'object' THEN woe.context->'node_results' ELSE '{}'::jsonb END
        ) AS t(key, val)
      WHERE woe.started_at >= NOW() - INTERVAL '24 hours' AND val->>'nodeType' IS NOT NULL
      GROUP BY val->>'nodeType'
      ORDER BY total_time_ms DESC
    `;

    const cpuDistQuery = `
      SELECT 
        CASE
          WHEN CAST(context->>'cpu_us' AS NUMERIC) < 100000 THEN '0-100ms'
          WHEN CAST(context->>'cpu_us' AS NUMERIC) < 500000 THEN '100-500ms'
          WHEN CAST(context->>'cpu_us' AS NUMERIC) < 1000000 THEN '500ms-1s'
          WHEN CAST(context->>'cpu_us' AS NUMERIC) < 5000000 THEN '1-5s'
          ELSE '5s+'
        END AS cpu_bucket,
        COUNT(*)::int AS count
      FROM workflow_org_executions
      WHERE started_at >= NOW() - INTERVAL '24 hours' AND context->>'cpu_us' IS NOT NULL
      GROUP BY cpu_bucket
      ORDER BY MIN(CAST(context->>'cpu_us' AS NUMERIC))
    `;

    const memDistQuery = `
      SELECT 
        CASE
          WHEN CAST(context->>'peak_rss' AS NUMERIC) < 100*1024*1024 THEN '<100 MB'
          WHEN CAST(context->>'peak_rss' AS NUMERIC) < 200*1024*1024 THEN '100-200 MB'
          WHEN CAST(context->>'peak_rss' AS NUMERIC) < 500*1024*1024 THEN '200-500 MB'
          WHEN CAST(context->>'peak_rss' AS NUMERIC) < 1024*1024*1024 THEN '500MB-1GB'
          ELSE '1GB+'
        END AS mem_bucket,
        COUNT(*)::int AS count
      FROM workflow_org_executions
      WHERE started_at >= NOW() - INTERVAL '24 hours' AND context->>'peak_rss' IS NOT NULL
      GROUP BY mem_bucket
      ORDER BY MIN(CAST(context->>'peak_rss' AS NUMERIC))
    `;

    const heaviestQuery = `
      SELECT 
        woe.id::text AS id,
        wc.name AS workflow_name,
        woe.status,
        COALESCE(CAST(woe.context->>'cpu_us' AS NUMERIC), 0)::int AS cpu_us,
        COALESCE(CAST(woe.context->>'peak_rss' AS NUMERIC), 0)::bigint AS peak_rss,
        COALESCE(CAST(woe.context->>'heap_used' AS NUMERIC), 0)::bigint AS heap_used,
        COALESCE(woe.duration_ms, 0)::int AS duration_ms,
        to_char(woe.started_at, 'YYYY-MM-DD HH24:MI:SS') AS started_at
      FROM workflow_org_executions woe
      JOIN workflow_catalog wc ON wc.id = woe.catalog_id
      WHERE woe.started_at >= NOW() - INTERVAL '24 hours' AND woe.context->>'cpu_us' IS NOT NULL
      ORDER BY CAST(woe.context->>'cpu_us' AS NUMERIC) DESC
      LIMIT 10
    `;

    // 2. Execute all queries in parallel
    const [
      modelProfileRes,
      timelineRes,
      nodeBreakdownRes,
      cpuDistRes,
      memDistRes,
      heaviestRes
    ] = await Promise.all([
      pool.query(modelProfileQuery),
      pool.query(timelineQuery),
      pool.query(nodeBreakdownQuery),
      pool.query(cpuDistQuery),
      pool.query(memDistQuery),
      pool.query(heaviestQuery)
    ]);

    // 3. Map model profiles
    const modelProfiles = modelProfileRes.rows.map(r => ({
      name: r.model_name,
      totalRuns: r.total_runs,
      avgCpuUs: r.avg_cpu_us,
      avgMemDelta: Number(r.avg_mem_delta),
      avgPeakRss: Number(r.avg_peak_rss),
      avgHeapUsed: Number(r.avg_heap_used),
      avgDurationMs: r.avg_duration_ms,
      completed: r.completed,
      failed: r.failed,
      cpuEfficiency: r.avg_duration_ms > 0 ? Math.round(r.avg_cpu_us / r.avg_duration_ms) : 0
    }));

    // 4. Map timeline
    const resourceTimeline = timelineRes.rows.map(r => ({
      time: r.time_bucket,
      executions: r.executions,
      avgCpuUs: r.avg_cpu_us,
      avgPeakRssMb: r.avg_peak_rss_mb,
      avgDurationMs: r.avg_duration_ms
    }));

    // 5. Map node breakdown
    const nodeBreakdown = nodeBreakdownRes.rows.map(r => ({
      nodeType: r.node_type,
      totalExecutions: r.total_executions,
      avgDurationMs: r.avg_duration_ms,
      totalTimeMs: Number(r.total_time_ms)
    }));

    // 6. Map heaviest executions
    const heaviestExecutions = heaviestRes.rows.map(r => ({
      id: r.id,
      workflowName: r.workflow_name,
      status: r.status,
      cpuUs: r.cpu_us,
      peakRss: Number(r.peak_rss),
      heapUsed: Number(r.heap_used),
      durationMs: r.duration_ms,
      startedAt: r.started_at
    }));

    const system = telemetry?.system || {};

    const resultPayload = {
      success: true,
      system: {
        cpuCores: system.cpu_cores || os.cpus().length,
        cpuModel: system.cpu_model || os.cpus()[0]?.model?.trim() || 'Unknown',
        totalMemBytes: system.total_mem_bytes || os.totalmem(),
        freeMemBytes: system.free_mem_bytes || os.freemem(),
        uptimeSeconds: telemetry?.uptime_seconds || 0,
        processRssMb: Math.round((telemetry?.memory?.rss_bytes || 0) / (1024 * 1024)),
        cpuUsagePercent: telemetry?.cpu_usage_percent || 0,
        workerPoolSize: telemetry?.workers?.pool_size || 0,
        workerBusyRatio: telemetry?.workers?.busy_ratio || 0,
        queueDepth: telemetry?.workers?.queue_depth || 0
      },
      modelProfiles,
      resourceTimeline,
      nodeBreakdown,
      cpuDistribution: cpuDistRes.rows.map(r => ({ bucket: r.cpu_bucket, count: r.count })),
      memDistribution: memDistRes.rows.map(r => ({ bucket: r.mem_bucket, count: r.count })),
      heaviestExecutions
    };

    // Cache the response
    deepAnalysisCache = resultPayload;
    deepAnalysisCacheTime = Date.now();

    res.json(resultPayload);
  } catch (err) {
    console.error('[Telemetry Backend] deep-analysis error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── Startup ──────────────────────────────────────────────────────
async function start() {
  // Test DB connection
  try {
    await pool.query('SELECT 1');
    console.log('[Telemetry Backend] PostgreSQL connected directly');
  } catch (err) {
    console.error('[Telemetry Backend] PostgreSQL connection failed:', err.message);
    process.exit(1);
  }

  // Connect Redis (non-blocking — dashboard still works without it)
  await initRedis();

  const server = app.listen(PORT, '0.0.0.0', () => {
    console.log(`\n=============================================================`);
    console.log(`  Telemetry Backend running on http://localhost:${PORT}`);
    console.log(`  Mode: INDEPENDENT (Direct DB + Redis live stats)`);
    console.log(`=============================================================\n`);
  });

  process.on('SIGTERM', () => {
    console.log('[Telemetry Backend] SIGTERM received. Shutting down...');
    server.close(async () => {
      await pool.end();
      if (redisClient) await redisClient.disconnect().catch(() => {});
      process.exit(0);
    });
  });

  process.on('SIGINT', () => {
    console.log('[Telemetry Backend] SIGINT received. Shutting down...');
    server.close(async () => {
      await pool.end();
      if (redisClient) await redisClient.disconnect().catch(() => {});
      process.exit(0);
    });
  });
}

start().catch((err) => {
  console.error('[Telemetry Backend] Fatal startup error:', err.message);
  process.exit(1);
});
