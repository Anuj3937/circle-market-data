const { getHistoricRates } = require('dukascopy-node');
const fs = require('fs');

// ═══════════════════════════════════════════════════════════════
// Automated Daily Market Data Downloader
// Fetches the last 90 days of 1-minute data. 
// Overwrites base symbols; leaves Time Machine & Learning Data alone.
// ═══════════════════════════════════════════════════════════════

// Dynamically calculate Rolling 90-Day Window
const endDate = new Date();
const startDate = new Date();
startDate.setDate(endDate.getDate() - 90); 

const STANDARD_START = startDate.toISOString().split('T')[0];
const STANDARD_END = endDate.toISOString().split('T')[0];

const DUKASCOPY_SYMBOLS = [
    // Forex Majors
    { dukascopy: 'eurusd', file: 'EURUSD' },
    { dukascopy: 'gbpusd', file: 'GBPUSD' },
    { dukascopy: 'usdjpy', file: 'USDJPY' },
    { dukascopy: 'audusd', file: 'AUDUSD' },
    { dukascopy: 'usdchf', file: 'USDCHF' },
    { dukascopy: 'usdcad', file: 'USDCAD' },
    { dukascopy: 'nzdusd', file: 'NZDUSD' },
    // Forex Crosses
    { dukascopy: 'gbpjpy', file: 'GBPJPY' },
    { dukascopy: 'eurjpy', file: 'EURJPY' },
    { dukascopy: 'eurgbp', file: 'EURGBP' },
    { dukascopy: 'gbpaud', file: 'GBPAUD' }, 
    { dukascopy: 'euraud', file: 'EURAUD' }, 
    { dukascopy: 'audnzd', file: 'AUDNZD' }, 
    // Commodities
    { dukascopy: 'xauusd', file: 'XAUUSD' },
    { dukascopy: 'xagusd', file: 'XAGUSD' },
    { dukascopy: 'bcousd', file: 'BCOUSD' }, // Brent Oil
    { dukascopy: 'lightcmdusd', file: 'USOIL' }, // WTI Crude
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

async function fetchDukascopy(instrument, start, end, fileName) {
    try {
        console.log(`⏳ Dukascopy: Updating ${fileName}.json...`);
        const data = await getHistoricRates({
            instrument,
            dates: { from: new Date(start), to: new Date(end) },
            timeframe: 'm1',
            format: 'json',
        });
        
        if (!data || data.length === 0) {
            console.log(`⚠️ No data for ${fileName}`);
            return false;
        }
        
        const optimized = data.map(d => [
            Math.round(d.timestamp / 1000), d.open, d.high, d.low, d.close, d.volume || 0
        ]);
        
        fs.writeFileSync(`${fileName}.json`, JSON.stringify(optimized));
        const sizeMB = (fs.statSync(`${fileName}.json`).size / 1024 / 1024).toFixed(2);
        console.log(`✅ ${fileName}.json updated — ${optimized.length.toLocaleString()} candles (${sizeMB} MB)`);
        return true;
    } catch (error) {
        console.error(`❌ Failed ${fileName}: ${error.message}`);
        return false;
    }
}

async function fetchBinance(symbol, start, end, fileName) {
    try {
        console.log(`⏳ Binance: Updating ${fileName}.json...`);
        const startMs = new Date(start).getTime();
        const endMs = new Date(end).getTime();
        let allCandles = [];
        let currentStart = startMs;

        while (currentStart < endMs) {
            const url = `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=1m&startTime=${currentStart}&endTime=${endMs}&limit=1000`;
            const res = await fetch(url);
            if (!res.ok) throw new Error(`Binance ${res.status}: ${await res.text()}`);
            const klines = await res.json();
            if (klines.length === 0) break;

            for (const k of klines) {
                allCandles.push([
                    Math.round(k[0] / 1000),
                    parseFloat(k[1]), parseFloat(k[2]), parseFloat(k[3]), parseFloat(k[4]), parseFloat(k[5])
                ]);
            }
            currentStart = klines[klines.length - 1][0] + 60000;
            await new Promise(r => setTimeout(r, 100)); // Respect Binance 1200 req/min limit
        }

        if (allCandles.length === 0) {
            console.log(`⚠️ No data for ${fileName}`);
            return false;
        }

        fs.writeFileSync(`${fileName}.json`, JSON.stringify(allCandles));
        const sizeMB = (fs.statSync(`${fileName}.json`).size / 1024 / 1024).toFixed(2);
        console.log(`✅ ${fileName}.json updated — ${allCandles.length.toLocaleString()} candles (${sizeMB} MB)`);
        return true;
    } catch (error) {
        console.error(`❌ Failed ${fileName}: ${error.message}`);
        return false;
    }
}

async function run() {
    console.log(`🚀 Starting Daily Update: ${STANDARD_START} to ${STANDARD_END}\n`);
    let success = 0, failed = 0;

    for (const sym of DUKASCOPY_SYMBOLS) {
        const ok = await fetchDukascopy(sym.dukascopy, STANDARD_START, STANDARD_END, sym.file);
        ok ? success++ : failed++;
    }

    for (const sym of BINANCE_SYMBOLS) {
        const ok = await fetchBinance(sym.binance, STANDARD_START, STANDARD_END, sym.file);
        ok ? success++ : failed++;
    }

    console.log(`\n🎉 UPDATE COMPLETE! ✅ ${success} saved | ❌ ${failed} failed`);
}

run();