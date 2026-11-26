const express = require('express');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

const NORVION_ENDPOINT = 'https://norvionsolution.com/v1/stkpush';
const ACCOUNT_ID = process.env.NORVION_ACCOUNT_ID || '';
const SECRET_KEY = process.env.NORVION_SECRET_KEY || '';
const DEFAULT_CALLBACK_URL = process.env.CALLBACK_URL || '';
const INTERVAL_MS = 4000; // 4 seconds between requests

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// In-memory state for active batch and live UI feed
let jobState = {
  running: false,
  stopRequested: false,
  total: 0,
  processed: 0,
  succeeded: 0,
  failed: 0,
  logs: []
};

function logEvent(level, message, meta = null) {
  const timestamp = new Date().toISOString();
  const entry = { timestamp, level, message, meta };

  // Keep latest 200 entries in memory for the frontend
  jobState.logs.unshift(entry);
  if (jobState.logs.length > 200) jobState.logs.pop();

  // Print to stdout/stderr so Render captures them in real time
  const formatted = `[${timestamp}] [${level.toUpperCase()}] ${message}${
    meta ? ' | ' + JSON.stringify(meta) : ''
  }`;
  if (level === 'error') {
    console.error(formatted);
  } else {
    console.log(formatted);
  }
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Validate Kenyan MSISDN strictly starting with 254 (12 digits total: 2547XXXXXXXX or 2541XXXXXXXX)
function parseAndValidateNumbers(rawInput) {
  const tokens = rawInput
    .split(/[\s,;]+/)
    .map((n) => n.trim())
    .filter(Boolean);

  const valid = [];
  const invalid = [];

  for (const num of tokens) {
    if (/^254[17]\d{8}$/.test(num)) {
      valid.push(num);
    } else {
      invalid.push(num);
    }
  }

  return { valid, invalid };
}

async function sendStkPush({ phone, amount, reference, callbackUrl }) {
  const payload = {
    account_id: ACCOUNT_ID,
    secret_key: SECRET_KEY,
    phone,
    amount: Number(amount),
    reference
  };

  if (callbackUrl) {
    payload.callback_url = callbackUrl;
  }

  const response = await fetch(NORVION_ENDPOINT, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });

  const rawText = await response.text();
  let data;
  try {
    data = JSON.parse(rawText);
  } catch {
    data = { raw: rawText };
  }

  if (!response.ok || data.success === false) {
    const errMsg =
      data.message || data.error || `HTTP ${response.status} from Norvion API`;
    const err = new Error(errMsg);
    err.status = response.status;
    err.responseBody = data;
    throw err;
  }

  return data;
}

async function processQueue(numbers, amount, referencePrefix, callbackUrl) {
  jobState.running = true;
  jobState.stopRequested = false;
  jobState.total = numbers.length;
  jobState.processed = 0;
  jobState.succeeded = 0;
  jobState.failed = 0;

  logEvent('info', `Batch started: ${numbers.length} numbers @ KES ${amount} (4s interval)`);

  for (let i = 0; i < numbers.length; i++) {
    if (jobState.stopRequested) {
      logEvent('warn', `Batch stopped manually after ${jobState.processed}/${jobState.total} requests.`);
      break;
    }

    const phone = numbers[i];
    const reference = `${referencePrefix}-${i + 1}`;

    try {
      const result = await sendStkPush({
        phone,
        amount,
        reference,
        callbackUrl
      });

      jobState.succeeded++;
      logEvent('info', `STK Sent -> ${phone} (${reference})`, {
        transaction_request_id: result?.data?.transaction_request_id,
        checkout_request_id: result?.data?.checkout_request_id,
        status: result?.data?.status
      });
    } catch (err) {
      jobState.failed++;
      logEvent('error', `STK Failed -> ${phone} (${reference}): ${err.message}`, {
        httpStatus: err.status || 'NETWORK_ERROR',
        details: err.responseBody || null
      });
    }

    jobState.processed++;

    // Wait 4 seconds before the next request (skip delay after the final item)
    if (i < numbers.length - 1 && !jobState.stopRequested) {
      await sleep(INTERVAL_MS);
    }
  }

  jobState.running = false;
  logEvent(
    'info',
    `Batch finished. Succeeded: ${jobState.succeeded}, Failed: ${jobState.failed}`
  );
}

// Start a bulk STK batch
app.post('/api/start', (req, res) => {
  if (!ACCOUNT_ID || !SECRET_KEY) {
    logEvent('error', 'Server misconfiguration: NORVION_ACCOUNT_ID or NORVION_SECRET_KEY missing.');
    return res.status(500).json({
      error: 'Server environment variables NORVION_ACCOUNT_ID and NORVION_SECRET_KEY are not set.'
    });
  }

  if (jobState.running) {
    return res.status(409).json({ error: 'A batch is already running. Stop it or wait for completion.' });
  }

  const { numbersRaw, amount, referencePrefix, callbackUrl } = req.body;

  const parsedAmount = Number(amount);
  if (!Number.isInteger(parsedAmount) || parsedAmount < 1 || parsedAmount > 250000) {
    return res.status(400).json({ error: 'Amount must be a whole number between 1 and 250,000 KES.' });
  }

  if (!numbersRaw || typeof numbersRaw !== 'string') {
    return res.status(400).json({ error: 'Please provide phone numbers starting with 254.' });
  }

  const { valid, invalid } = parseAndValidateNumbers(numbersRaw);

  if (invalid.length > 0) {
    logEvent('warn', `Rejected ${invalid.length} invalid number(s)`, { invalid });
  }

  if (valid.length === 0) {
    return res.status(400).json({
      error: 'No valid 254 numbers found. Numbers must match 2547XXXXXXXX or 2541XXXXXXXX.',
      invalid
    });
  }

  const cleanPrefix = (referencePrefix || 'INV').trim().slice(0, 80);
  const cleanCallback = (callbackUrl || DEFAULT_CALLBACK_URL).trim();

  // Run asynchronously in the background
  processQueue(valid, parsedAmount, cleanPrefix, cleanCallback);

  return res.json({
    message: `Queued ${valid.length} valid numbers (4s interval).`,
    validCount: valid.length,
    invalidCount: invalid.length,
    invalid
  });
});

// Stop active batch
app.post('/api/stop', (req, res) => {
  if (!jobState.running) {
    return res.status(400).json({ error: 'No batch is currently running.' });
  }
  jobState.stopRequested = true;
  logEvent('warn', 'Stop requested by user. Finishing current request...');
  res.json({ message: 'Stopping after current request finishes.' });
});

// Poll live status & logs for UI
app.get('/api/status', (req, res) => {
  res.json(jobState);
});

// Webhook receiver so Norvion callbacks also show up in Render logs
app.post('/mpesa/result', (req, res) => {
  logEvent('info', 'Webhook callback received from Norvion', req.body);
  res.status(200).json({ received: true });
});

app.listen(PORT, () => {
  logEvent('info', `Norvion STK Bot server listening on port ${PORT}`);
});
