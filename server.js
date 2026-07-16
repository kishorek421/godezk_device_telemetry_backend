const express = require('express');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env') });
require('dotenv').config({ path: path.join(__dirname, '../backdoor_backend/.env') });

const BACKEND_URL = process.env.BACKEND_URL || `http://localhost:${process.env.PORT || 8090}`;

function getHttpClient(urlStr) {
  return urlStr.startsWith('https') ? require('https') : require('http');
}

function fetchFromTelemetry(pathStr) {
  return new Promise((resolve, reject) => {
    const url = `${BACKEND_URL}/api/telemetry${pathStr}`;
    console.log(`[Telemetry Backend] Fetching from backdoor backend: ${url}`);
    const client = getHttpClient(url);
    const req = client.get(url, { timeout: 30000 }, (res) => {
      let body = '';
      res.on('data', c => { body += c; });
      res.on('end', () => {
        try {
          if (res.statusCode >= 400) {
            console.error(`[Telemetry Backend] Error response from backdoor backend (Status: ${res.statusCode}) for ${url}`);
            reject(new Error(`Telemetry API error: ${res.statusCode}`));
          } else {
            console.log(`[Telemetry Backend] Successfully fetched telemetry data for ${pathStr}`);
            resolve(JSON.parse(body));
          }
        } catch (err) {
          console.error(`[Telemetry Backend] Failed to parse JSON response from ${url}:`, err.message);
          reject(err);
        }
      });
    });
    req.on('error', (err) => {
      console.error(`[Telemetry Backend] Connection error to backdoor backend ${url}:`, err.message);
      reject(err);
    });
    req.on('timeout', () => {
      req.destroy();
      console.error(`[Telemetry Backend] Request timeout to backdoor backend ${url}`);
      reject(new Error('Telemetry API timeout'));
    });
  });
}

function postToTelemetry(pathStr, bodyObj) {
  return new Promise((resolve, reject) => {
    const url = `${BACKEND_URL}/api/telemetry${pathStr}`;
    console.log(`[Telemetry Backend] Posting to backdoor backend: ${url}`);
    const client = getHttpClient(url);
    const bodyStr = JSON.stringify(bodyObj);
    
    const options = {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(bodyStr)
      },
      timeout: 30000
    };

    const req = client.request(url, options, (res) => {
      let body = '';
      res.on('data', c => { body += c; });
      res.on('end', () => {
        try {
          if (res.statusCode >= 400) {
            console.error(`[Telemetry Backend] Error response from backdoor backend (Status: ${res.statusCode}) for POST ${url}`);
            reject(new Error(`Telemetry API error: ${res.statusCode}`));
          } else {
            console.log(`[Telemetry Backend] Successfully posted telemetry settings data`);
            resolve(JSON.parse(body));
          }
        } catch (err) {
          console.error(`[Telemetry Backend] Failed to parse JSON response from POST ${url}:`, err.message);
          reject(err);
        }
      });
    });
    req.on('error', (err) => {
      console.error(`[Telemetry Backend] Connection error (POST) to backdoor backend ${url}:`, err.message);
      reject(err);
    });
    req.on('timeout', () => {
      req.destroy();
      console.error(`[Telemetry Backend] Request timeout (POST) to backdoor backend ${url}`);
      reject(new Error('Telemetry API timeout'));
    });
    
    req.write(bodyStr);
    req.end();
  });
}

const app = express();
const PORT = process.env.ANALYTICS_PORT || 3031;

app.use(express.json());

// Enable CORS for development
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  next();
});

// 1. Dashboard overview KPIs & Trends (Fetched from Telemetry API)
app.get('/api/dashboard-summary', async (req, res) => {
  try {
    const data = await fetchFromTelemetry('/dashboard-summary');
    res.json(data);
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 2. Recent frames list (Fetched from Telemetry API)
app.get('/api/frames', async (req, res) => {
  try {
    const data = await fetchFromTelemetry('/frames');
    res.json(data);
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 3. Single frame journey segments details (Fetched from Telemetry API)
app.get('/api/frame/:frameId', async (req, res) => {
  try {
    const data = await fetchFromTelemetry(`/frame/${req.params.frameId}`);
    res.json(data);
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 4. Camera health (Fetched from Telemetry API)
app.get('/api/camera/:cameraId', async (req, res) => {
  try {
    const data = await fetchFromTelemetry(`/camera/${req.params.cameraId}`);
    res.json(data);
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 5. Worker utilization (Fetched from Telemetry API)
app.get('/api/workers', async (req, res) => {
  try {
    const data = await fetchFromTelemetry('/workers');
    res.json(data);
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 6. Pipeline execution layer-by-layer logs (Fetched from Telemetry API)
app.get('/api/pipeline-logs', async (req, res) => {
  try {
    const query = req.url.includes('?') ? req.url.substring(req.url.indexOf('?')) : '';
    const data = await fetchFromTelemetry(`/pipeline-logs${query}`);
    res.json(data);
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Telemetry & Debug Configurations settings GET
app.get('/api/settings', async (req, res) => {
  try {
    const data = await fetchFromTelemetry('/settings');
    res.json(data);
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Telemetry & Debug Configurations settings POST
app.post('/api/settings', async (req, res) => {
  try {
    const data = await postToTelemetry('/settings', req.body);
    res.json(data);
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 7. Deep backend behavior analysis (Fetched from Telemetry API)
app.get('/api/deep-analysis', async (req, res) => {
  try {
    const data = await fetchFromTelemetry('/deep-analysis');
    res.json(data);
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(`\n=============================================================`);
  console.log(`Frame Analytics Backend running on http://localhost:${PORT}`);
  console.log(`=============================================================\n`);
});

process.on('SIGTERM', () => {
  console.log('[Telemetry Backend] SIGTERM received. Gracefully closing Express server...');
  server.close(() => {
    console.log('[Telemetry Backend] Express server closed. Exiting process.');
    process.exit(0);
  });
});

process.on('SIGINT', () => {
  console.log('[Telemetry Backend] SIGINT (Ctrl+C) received. Gracefully closing Express server...');
  server.close(() => {
    console.log('[Telemetry Backend] Express server closed. Exiting process.');
    process.exit(0);
  });
});
