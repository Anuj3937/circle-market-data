// ================================================================
//  MARKET DATA AUTO-DOWNLOADER (FIXED VERSION)
//  - Correct date window logic
//  - Timeout wrapper
//  - Retries: full window → 2 chunks → 3 chunks
//  - Works for all Dukascopy/Binance symbols
// ================================================================

const { getHistoricRates } = require('dukascopy-node');
const fs = require('fs');

// ---------------------------
// FIXED DATE WINDOW SYSTEM
// ---------------------------
const endDate = new Date();
endDate.setDate(endDate.getDate() - 2);  // End = 2 days ago

const startDate = new Date(endDate);     // Clone
startDate.setDate(startDate.getDate() - 91); // Start = 91 days before end

const STANDARD_START = startDate.toISOString().split('T')[0];
const STANDARD_END   = endDate.toISOString().split('T')[0];

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
    { dukascopy: 'gbpaud', file: 'GBPAUD' },
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

// Binance markets
const BINANCE_SYMBOLS = [
    { binance: 'BTCUSDT', file: 'BTCUSD' },
    { binance: 'ETHUSDT', file: 'ETHUSD' },
    { binance: 'SOLUSDT', file: 'SOLUSD' },
    { binance: 'XRPUSDT', file: 'XRPUSD' }
];

// --------------------------------------------------
// TIMEOUT WRAPPER (prevents hanging forever)
// --------------------------------------------------
function withTimeout(promise, ms, label) {
    return Promise.race([
        promise,
        new Promise((_, reject) =>
            setTimeout(() => reject(new Error(`Timeout after ${ms}ms: ${label}`)), ms)
        )
    ]);
}

// --------------------------------------------------
// SPLIT DATE RANGE INTO CHUNKS
// --------------------------------------------------
function splitRange(startISO, endISO, parts) {
    const parse = s => new Date(s + "T00:00:00Z");

    const start = parse(startISO).getTime();
    const end   = parse(endISO).getTime();
    const step  = Math.ceil((end - start) / parts);

    const chunks = [];
    let cursor = start;

    for (let i = 0; i < parts; i++) {
        const from = new Date(cursor);
        const to   = new Date(Math.min(end, cursor + step));

        chunks.push([
            from.toISOString().split("T")[0],
            to.toISOString().split("T")[0]
        ]);

        cursor += step + 86400000; // jump 1 day to avoid exact overlap
    }

    return chunks;
}

// --------------------------------------------------
// DUKASCOPY FETCHER (STABLE, FIXED)
// --------------------------------------------------
async function fetchDukascopy(instrument, start, end, fileName) {
    const timeoutMs = 90000;

    async function getChunk(from, to) {
        console.log(`   → Fetching ${instrument.toUpperCase()} [${from} → ${to}]`);
        return withTimeout(
            getHistoricRates({
                instrument,
                dates: { from: new Date(from), to: new Date(to) },
                timeframe: 'm1',
                format: 'json'
            }),
            timeoutMs,
            `${instrument} ${from}→${to}`
        );
    }

    try {
        console.log(`⏳ Dukascopy: Updating ${fileName}.json...`);

        // TRY 1 — FULL WINDOW
        try {
            const data = await getChunk(start, end);
            if (data.length) return save(fileName, data);
        } catch {
            console.log(`   ↻ Full-window failed → retrying in 2 chunks...`);
        }

        // TRY 2 — 2 CHUNKS
        try {
            const ranges = splitRange(start, end, 2);
            let all = [];
            for (const [f, t] of ranges) {
                const part = await getChunk(f, t);
                all = all.concat(part);
            }
            if (all.length) return save(fileName, all);
        } catch {
            console.log(`   ↻ 2-chunk retry failed → retrying in 3 chunks...`);
        }

        // TRY 3 — 3 CHUNKS
        try {
            const ranges = splitRange(start, end, 3);
            let all = [];
            for (const [f, t] of ranges) {
                const part = await getChunk(f, t);
                all = all.concat(part);
            }
            if (all.length) return save(fileName, all);
        } catch {
            console.log(`   ↻ 3-chunk retry also failed.`);
        }

        console.log(`⚠️ No data for ${fileName} after all retries`);
        return false;

    } catch (err) {
        console.log(`❌ Failed ${fileName}: ${err.message}`);
        return false;
    }
}

// --------------------------------------------------
// SAVE OPTIMIZED JSON
// --------------------------------------------------
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

// --------------------------------------------------
// BINANCE FETCH
// --------------------------------------------------
async function fetchBinance(symbol, start, end, fileName) {
    try {
        console.log(`⏳ Binance: Updating ${fileName}.json...`);

        const startMs = new Date(start).getTime();
        const endMs   = new Date(end).getTime();

        let all = [];
        let cursor = startMs;

        while (cursor < endMs) {
            const url =
                `https://api.binance.com/api/v3/klines` +
                `?symbol=${symbol}&interval=1m&startTime=${cursor}&endTime=${endMs}&limit=1000`;

            const res = await fetch(url);
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

            cursor = data[data.length - 1][0] + 60000;
            await new Promise(r => setTimeout(r, 100));
        }

        save(fileName, all);
        return true;

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