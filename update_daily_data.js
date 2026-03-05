// ================================================================
// MARKET DATA AUTO-DOWNLOADER (GBPAUD-hardened)
// - Normal fast-path for most symbols
// - Special slow-path for GBPAUD with 7-day micro-chunks & robust retries
// ================================================================

const { getHistoricRates } = require('dukascopy-node');
const fs = require('fs');

const ONE_DAY_MS = 24 * 60 * 60 * 1000;

// ---------- Helpers ----------
function pad2(n) { return String(n).padStart(2, '0'); }
function toISODateUTC(d) {
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
}
function dayStartUTC(date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), 0, 0, 0, 0));
}
function dayEndUTC(date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), 23, 59, 59, 999));
}
function parseISODateUTC(isoYMD) {
  const [y, m, d] = isoYMD.split('-').map(Number);
  return new Date(Date.UTC(y, m - 1, d, 0, 0, 0, 0));
}
function withTimeout(promise, ms, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`Timeout after ${ms}ms: ${label}`)), ms))
  ]);
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function splitRangeInclusive(startISO, endISO, parts) {
  const start = parseISODateUTC(startISO);
  const end   = parseISODateUTC(endISO);
  const totalDays = Math.floor((dayStartUTC(end) - dayStartUTC(start)) / ONE_DAY_MS) + 1; // inclusive
  const base = Math.floor(totalDays / parts);
  const rem  = totalDays % parts;

  const ranges = [];
  let cursor = new Date(start);

  for (let i = 0; i < parts; i++) {
    const length = base + (i < rem ? 1 : 0);
    const fromD = new Date(cursor);
    const toD   = new Date(cursor.getTime() + (length - 1) * ONE_DAY_MS);
    ranges.push([toISODateUTC(fromD), toISODateUTC(toD)]);
    cursor = new Date(toD.getTime() + ONE_DAY_MS);
  }
  return ranges;
}

function sliceIntoFixedDays(startISO, endISO, daysPerChunk) {
  // Returns array of [fromISO, toISO] inclusive windows of given 'daysPerChunk'
  const start = parseISODateUTC(startISO);
  const end   = parseISODateUTC(endISO);
  const ranges = [];

  let cursor = new Date(start);
  while (cursor <= end) {
    const fromD = new Date(cursor);
    const toCandidate = new Date(cursor.getTime() + (daysPerChunk - 1) * ONE_DAY_MS);
    const toD = toCandidate > end ? end : toCandidate;
    ranges.push([toISODateUTC(fromD), toISODateUTC(toD)]);
    cursor = new Date(toD.getTime() + ONE_DAY_MS);
  }
  return ranges;
}

function save(fileName, data) {
  const optimized = data.map(d => [
    Math.round(d.timestamp / 1000),
    d.open, d.high, d.low, d.close,
    d.volume || 0
  ]);
  fs.writeFileSync(`${fileName}.json`, JSON.stringify(optimized));
  const sizeMB = (fs.statSync(`${fileName}.json`).size / 1024 / 1024).toFixed(2);
  console.log(`✅ ${fileName}.json saved — ${optimized.length.toLocaleString()} candles (${sizeMB} MB)`);
  return true;
}

// ---------------------------
// FIXED DATE WINDOW SYSTEM
// ---------------------------
const nowUTC = new Date();
const endDate = new Date(Date.UTC(nowUTC.getUTCFullYear(), nowUTC.getUTCMonth(), nowUTC.getUTCDate()));
endDate.setUTCDate(endDate.getUTCDate() - 2); // end = 2 days ago (full day)
const startDate = new Date(endDate);
startDate.setUTCDate(startDate.getUTCDate() - 91);

const STANDARD_START = toISODateUTC(startDate);
const STANDARD_END   = toISODateUTC(endDate);

// ---------------------------
// SYMBOL SETS
// ---------------------------
const DUKASCOPY_SYMBOLS = [
  // Majors
  { dukascopy: 'eurusd', file: 'EURUSD' },
  { dukascopy: 'gbpusd', file: 'GBPUSD' },
  { dukascopy: 'usdjpy', file: 'USDJPY' },
  { dukascopy: 'audusd', file: 'AUDUSD' },
  { dukascopy: 'usdchf', file: 'USDCHF' },
  { dukascopy: 'usdcad', file: 'USDCAD' },
  { dukascopy: 'nzdusd', file: 'NZDUSD' },
  // Crosses
  { dukascopy: 'gbpjpy', file: 'GBPJPY' },
  { dukascopy: 'eurjpy', file: 'EURJPY' },
  { dukascopy: 'eurgbp', file: 'EURGBP' },
  { dukascopy: 'gbpaud', file: 'GBPAUD' },   // 👈 problematic one
  { dukascopy: 'euraud', file: 'EURAUD' },
  { dukascopy: 'audnzd', file: 'AUDNZD' },
  // Commodities
  { dukascopy: 'xauusd', file: 'XAUUSD' },
  { dukascopy: 'xagusd', file: 'XAGUSD' },
  { dukascopy: 'bcousd', file: 'BCOUSD' },
  { dukascopy: 'lightcmdusd', file: 'USOIL' },
  { dukascopy: 'gasusd', file: 'NATGAS' },
  // Indices
  { dukascopy: 'usa30idxusd', file: 'US30' },
  { dukascopy: 'usatechidxusd', file: 'NAS100' },
  { dukascopy: 'usa500idxusd', file: 'SPX500' },
  { dukascopy: 'deuidxeur', file: 'GER40' },
  { dukascopy: 'gbridxgbp', file: 'UK100' },
  { dukascopy: 'jpnidxjpy', file: 'JPN225' },
  { dukascopy: 'usa2000idxusd', file: 'US2000' }
];

const BINANCE_SYMBOLS = [
  { binance: 'BTCUSDT', file: 'BTCUSD' },
  { binance: 'ETHUSDT', file: 'ETHUSD' },
  { binance: 'SOLUSDT', file: 'SOLUSD' },
  { binance: 'XRPUSDT', file: 'XRPUSD' }
];

// Slow-path list (tune as needed)
const SLOW_SYMBOLS = new Set(['gbpaud']); // add 'euraud','audnzd' if needed

// --------------------------------------------------
// Core Dukascopy fetcher blocks
// --------------------------------------------------
async function getChunkDuka(instrument, fromISO, toISO, timeoutMs) {
  const fromDate = dayStartUTC(parseISODateUTC(fromISO));
  const toDate   = dayEndUTC(parseISODateUTC(toISO));
  const label    = `${instrument.toUpperCase()} [${fromISO} → ${toISO}]`;
  console.log(`   → Fetching ${label}`);
  console.time(`   ⏱ ${label}`);

  const data = await withTimeout(
    getHistoricRates({
      instrument,
      dates: { from: fromDate, to: toDate },
      timeframe: 'm1',
      format: 'json'
    }),
    timeoutMs,
    label
  );

  console.timeEnd(`   ⏱ ${label}`);
  return data || [];
}

// Normal fast-path: full → 2 chunks → 3 chunks
async function fetchDukascopyFast(instrument, startISO, endISO, fileName) {
  const timeoutMs = 75_000;

  // Try full window
  try {
    const all = await getChunkDuka(instrument, startISO, endISO, timeoutMs);
    if (all.length) return save(fileName, all);
    console.log(`   ⚠️ No data in full window, will retry in 2 chunks...`);
  } catch (e) {
    console.log(`   ↻ Full-window failed: ${e.message} → 2 chunks...`);
  }

  // 2 chunks
  try {
    const ranges2 = splitRangeInclusive(startISO, endISO, 2);
    let all = [];
    for (const [f, t] of ranges2) {
      const part = await getChunkDuka(instrument, f, t, timeoutMs);
      all = all.concat(part);
      await sleep(250);
    }
    if (all.length) return save(fileName, all);
    console.log(`   ⚠️ No data in 2-chunk retry, trying 3 chunks...`);
  } catch (e) {
    console.log(`   ↻ 2-chunk failed: ${e.message} → 3 chunks...`);
  }

  // 3 chunks
  try {
    const ranges3 = splitRangeInclusive(startISO, endISO, 3);
    let all3 = [];
    for (const [f, t] of ranges3) {
      const part = await getChunkDuka(instrument, f, t, timeoutMs);
      all3 = all3.concat(part);
      await sleep(250);
    }
    if (all3.length) return save(fileName, all3);
    console.log(`   ⚠️ No data after 3 chunks.`);
  } catch (e) {
    console.log(`   ↻ 3-chunk failed: ${e.message}`);
  }

  return false;
}

// Slow-path specifically for GBPAUD (and others you add)
async function fetchDukascopySlow(instrument, startISO, endISO, fileName) {
  console.log(`🐢 Slow-path enabled for ${instrument.toUpperCase()} — 7-day micro-chunks with retries`);
  const timeoutPerChunkMs = 45_000;
  const maxAttempts = 5;
  const jitter = () => Math.floor(Math.random() * 250);

  // Circuit breaker: don't let one symbol eat the whole run
  const instrumentWallClockLimitMs = 8 * 60 * 1000; // 8 minutes
  const startedAt = Date.now();

  const ranges = sliceIntoFixedDays(startISO, endISO, 7);
  let collected = [];
  let failures  = 0;

  for (const [f, t] of ranges) {
    if (Date.now() - startedAt > instrumentWallClockLimitMs) {
      console.log(`⛔ Circuit breaker: ${instrument.toUpperCase()} exceeded ${instrumentWallClockLimitMs/60000} min — moving on`);
      break;
    }

    let attempt = 0, success = false;
    while (attempt < maxAttempts && !success) {
      attempt++;
      const backoff = Math.pow(2, attempt - 1) * 500 + jitter(); // 0.5s,1s,2s,4s,8s + jitter

      try {
        const part = await getChunkDuka(instrument, f, t, timeoutPerChunkMs);
        if (part && part.length) {
          collected = collected.concat(part);
          success = true;
        } else {
          console.log(`   ⚠️ Empty chunk ${f}→${t} on attempt ${attempt}/${maxAttempts}`);
        }
      } catch (e) {
        console.log(`   ↻ ${instrument.toUpperCase()} ${f}→${t} attempt ${attempt}/${maxAttempts} failed: ${e.message}`);
      }

      if (!success && attempt < maxAttempts) {
        await sleep(backoff);
      }
    }

    if (!success) {
      failures++;
      console.log(`   ❌ Skipping stubborn chunk ${f}→${t} after ${maxAttempts} attempts`);
    }

    // Small pacing between chunks
    await sleep(200);
  }

  if (collected.length) {
    return save(fileName, collected);
  } else {
    console.log(`⚠️ No data saved for ${fileName}; failures=${failures}`);
    return false;
  }
}

// Dispatcher for Dukascopy
async function fetchDukascopy(instrument, startISO, endISO, fileName) {
  try {
    console.log(`⏳ Dukascopy: Updating ${fileName}.json...`);

    if (SLOW_SYMBOLS.has(instrument)) {
      // Dedicated slow-path for GBPAUD
      return await fetchDukascopySlow(instrument, startISO, endISO, fileName);
    } else {
      // Default fast-path for everyone else
      return await fetchDukascopyFast(instrument, startISO, endISO, fileName);
    }

  } catch (err) {
    console.log(`❌ Failed ${fileName}: ${err.message}`);
    return false;
  }
}

// --------------------------------------------------
// BINANCE FETCH (unchanged)
// --------------------------------------------------
async function fetchBinance(symbol, start, end, fileName) {
  try {
    console.log(`⏳ Binance: Updating ${fileName}.json...`);

    const startMs = new Date(start + 'T00:00:00Z').getTime();
    const endMs   = new Date(end   + 'T23:59:59.999Z').getTime();

    let all = [];
    let cursor = startMs;

    while (cursor < endMs) {
      const url =
        `https://api.binance.com/api/v3/klines` +
        `?symbol=${symbol}&interval=1m&startTime=${cursor}&endTime=${endMs}&limit=1000`;

      const res = await fetch(url);
      if (!res.ok) throw new Error(`Binance ${res.status}: ${await res.text()}`);

      const data = await res.json();
      if (!data || data.length === 0) break;

      for (const k of data) {
        all.push([
          Math.round(k[0] / 1000),
          parseFloat(k[1]),
          parseFloat(k[2]),
          parseFloat(k[3]),
          parseFloat(k[4]),
          parseFloat(k[5])
        ]);
      }

      cursor = data[data.length - 1][0] + 60_000;
      await sleep(100);
    }

    return save(fileName, all);
  } catch (err) {
    console.log(`❌ Failed ${fileName}: ${err.message}`);
    return false;
  }
}

// --------------------------------------------------
// RUN SYSTEM
// --------------------------------------------------
async function run() {
  console.log(`🚀 Starting Daily Update: ${STANDARD_START} → ${STANDARD_END}\n`);

  let success = 0, failed = 0;

  for (const s of DUKASCOPY_SYMBOLS) {
    const ok = await fetchDukascopy(s.dukascopy, STANDARD_START, STANDARD_END, s.file);
    ok ? success++ : failed++;
  }

  for (const s of BINANCE_SYMBOLS) {
    const ok = await fetchBinance(s.binance, STANDARD_START, STANDARD_END, s.file);
    ok ? success++ : failed++;
  }

  console.log(`\n🎉 COMPLETE — Success: ${success} | Failures: ${failed}`);
}

run();