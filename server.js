const express = require('express');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env') });

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

  const server = app.listen(PORT, '0.0.0.0', () => {
    console.log(`\n=============================================================`);
    console.log(`  Benchmark Backend running on http://localhost:${PORT}`);
    console.log(`=============================================================\n`);
  });

  const shutdown = (signal) => {
    console.log(`[Benchmark Backend] ${signal} received. Shutting down...`);
    server.close(async () => {
      await pool.end();
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
