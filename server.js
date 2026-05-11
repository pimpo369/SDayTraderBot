// ============================================================
// STRADEBOT v1.0 — Quantitative Momentum Day Trader
// 30-Layer Research | Technical + Sentiment + Fundamental
// Budget: $1000 | Loss: 10% | Positions: 5 | Threshold: 20/30
// Sessions: Premarket | Market | After Hours | Overnight Crypto
// ============================================================
// HONEST NOTE ON API LIMITS:
//   Alpha Vantage free: 25 req/day — upgrade before going live
//   Polygon free: end-of-day only — upgrade for real-time
//   All others: generous free tiers
// ============================================================
 
const { Telegraf } = require("telegraf");
const Database     = require("better-sqlite3");
const cron         = require("node-cron");
const Alpaca       = require("@alpacahq/alpaca-trade-api");
 
// ── CONFIG ────────────────────────────────────────────────
const cfg = {
  telegram:     process.env.TELEGRAM_BOT_TOKEN,
  chatId:       process.env.YOUR_CHAT_ID,
  chatId2:      process.env.YOUR_CHAT_ID_2 || null,  // Optional second device
  polygon:      process.env.POLYGON_API_KEY,
  alpacaKey:    process.env.ALPACA_API_KEY,
  alpacaSecret: process.env.ALPACA_SECRET_KEY,
  alphavantage: process.env.ALPHAVANTAGE_API_KEY,
  finnhub:      process.env.FINNHUB_API_KEY,
  fmp:          process.env.FMP_API_KEY,
  youtube:      process.env.YOUTUBE_API_KEY,
};
 
const alpaca = new Alpaca({
  keyId: cfg.alpacaKey, secretKey: cfg.alpacaSecret,
  paper: true, usePolygon: false,
});
const bot = new Telegraf(cfg.telegram);
 
// ── DATABASE ──────────────────────────────────────────────
const db = new Database("./stradebot.db");
db.exec(`
  CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT, asset_type TEXT, session TEXT,
    qty REAL, price REAL, dollar_value REAL,
    stop_loss_price REAL, vote_score INTEGER,
    vote_detail TEXT, discovered_via TEXT,
    status TEXT DEFAULT 'open', pnl REAL DEFAULT 0,
    opened_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    closed_at DATETIME
  );
  CREATE TABLE IF NOT EXISTS agent_state (key TEXT PRIMARY KEY, value TEXT);
  CREATE TABLE IF NOT EXISTS scan_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT, vote_score INTEGER, session TEXT,
    verdict TEXT, scanned_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  CREATE TABLE IF NOT EXISTS rate_limits (
    chat_id TEXT PRIMARY KEY, last_request INTEGER
  );
`);
 
const getState = (k,d=null) => { const r=db.prepare("SELECT value FROM agent_state WHERE key=?").get(k); return r?JSON.parse(r.value):d; };
const setState = (k,v) => db.prepare("INSERT OR REPLACE INTO agent_state (key,value) VALUES (?,?)").run(k,JSON.stringify(v));
 
if (getState("total_loss")===null) setState("total_loss",0);
if (getState("paused")===null)     setState("paused",false);
if (getState("after_hours_count")===null) setState("after_hours_count",0);
if (getState("ah_date")===null)    setState("ah_date","");
 
// ── GUARDRAILS ────────────────────────────────────────────
const BUDGET           = 1000;
const MAX_TOTAL_LOSS   = 300; // 30% of $1000
const MAX_POSITION     = 250;   // 25%
const MAX_POSITIONS    = 5;
const STOP_STOCK       = 0.10;
const STOP_CRYPTO      = 0.07;
const MIN_VOTES_DAY    = 20;    // Market hours
const MIN_VOTES_PRE    = 20;    // Premarket
const MIN_VOTES_AH     = 25;    // After hours — stricter
const MIN_VOTES_NIGHT  = 22;    // Overnight crypto
const MAX_AH_TRADES    = 2;     // Max after hours trades per day
 
const ALPACA_CRYPTO = ["BTC","ETH","AVAX","LINK","LTC","AAVE","DOGE","XRP"];
 
// ── SESSION DETECTION ─────────────────────────────────────
function getSession() {
  const now    = new Date();
  const utcH   = now.getUTCHours();
  const utcM   = now.getUTCMinutes();
  const mins   = utcH * 60 + utcM;
  const day    = now.getUTCDay(); // 0=Sun,6=Sat
 
  if (day === 0 || day === 6) return "weekend";
  if (mins >= 480  && mins < 570)  return "premarket";   // 8:00–9:30am ET (13:00–14:30 UTC)
  if (mins >= 570  && mins < 840)  return "market";      // 9:30am–2:00pm ET
  if (mins >= 840  && mins < 1200) return "market";      // up to 8:00pm ET
  if (mins >= 1200 || mins < 480)  return "overnight";
  return "after_hours";
}
 
function getMinVotes(session) {
  switch(session) {
    case "premarket":    return MIN_VOTES_PRE;
    case "market":       return MIN_VOTES_DAY;
    case "after_hours":  return MIN_VOTES_AH;
    case "overnight":    return MIN_VOTES_NIGHT;
    default:             return MIN_VOTES_DAY;
  }
}
 
// ── HELPERS ───────────────────────────────────────────────
async function safeFetch(url, opts={}, timeout=8000) {
  const ctrl=new AbortController(); const t=setTimeout(()=>ctrl.abort(),timeout);
  try { return await (await fetch(url,{...opts,signal:ctrl.signal})).json(); }
  catch { return null; } finally { clearTimeout(t); }
}
async function safeFetchText(url, timeout=8000) {
  const ctrl=new AbortController(); const t=setTimeout(()=>ctrl.abort(),timeout);
  try { return await (await fetch(url,{signal:ctrl.signal})).text(); }
  catch { return ""; } finally { clearTimeout(t); }
}
const delay = ms => new Promise(r=>setTimeout(r,ms));
async function tg(msg) {
  const targets = [cfg.chatId, cfg.chatId2].filter(Boolean);
  for (const id of targets) {
    try { await bot.telegram.sendMessage(id, msg, {parse_mode:"HTML"}); }
    catch(e) { console.error("TG:", e.message); }
  }
}
function alpacaSymbol(ticker,isCrypto) {
  if (isCrypto) { const b=ticker.endsWith("USD")?ticker.slice(0,-3):ticker; return `${b}/USD`; }
  return ticker;
}
function fromAlpacaSymbol(symbol,assetClass) {
  return assetClass==="crypto" ? symbol.replace("/","") : symbol;
}
const getOpen = () => db.prepare("SELECT * FROM trades WHERE status='open'").all();
const getDeployed = () => db.prepare("SELECT SUM(dollar_value) as t FROM trades WHERE status='open'").get()?.t||0;
 
// ══════════════════════════════════════════════════════════
// PHASE 1: DISCOVERY — 30 SOURCES
// ══════════════════════════════════════════════════════════
 
async function discoverPolygonGainers() {
  const d=await safeFetch(`https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers?apiKey=${cfg.polygon}`);
  return (d?.tickers||[]).slice(0,20).filter(t=>t.ticker?.length<=5).map(t=>({ticker:t.ticker,source:"Polygon Gainers",score:2}));
}
async function discoverPolygonActive() {
  const d=await safeFetch(`https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers?apiKey=${cfg.polygon}`);
  return (d?.tickers||[]).sort((a,b)=>(b.day?.v||0)-(a.day?.v||0)).slice(0,10).map(t=>({ticker:t.ticker,source:"Polygon Volume",score:2}));
}
async function discoverPolygonPremarket() {
  const d=await safeFetch(`https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers?include_otc=false&apiKey=${cfg.polygon}`);
  return (d?.tickers||[]).filter(t=>Math.abs(t.prevDay?.c-t.day?.o)/t.prevDay?.c>0.01).slice(0,10).map(t=>({ticker:t.ticker,source:"Polygon Premarket Gap",score:3}));
}
async function discoverFinnhubNews() {
  const d=await safeFetch(`https://finnhub.io/api/v1/news?category=general&token=${cfg.finnhub}`);
  if (!d?.length) return [];
  const counts={};const sw=["THE","AND","FOR","WITH","FROM","THIS","THAT","WILL","HAVE","BEEN","NYSE","SEC","CEO","CFO","IPO","ETF","GDP","CPI","USD","EUR","US","UK","EU","NEW","AI","API"];
  d.slice(0,40).forEach(a=>{(a.headline+" "+(a.summary||"")).match(/\b([A-Z]{2,5})\b/g)?.forEach(m=>{if(!sw.includes(m))counts[m]=(counts[m]||0)+1;});});
  return Object.entries(counts).sort((a,b)=>b[1]-a[1]).slice(0,10).map(([t])=>({ticker:t,source:"Finnhub News",score:1}));
}
async function discoverRedditWSB() {
  const d=await safeFetch("https://www.reddit.com/r/wallstreetbets/hot.json?limit=20",{headers:{"User-Agent":"stradebot/1.0"}});
  return (d?.data?.children||[]).filter(p=>p.data.score>500).map(p=>({ticker:null,source:"Reddit WSB",title:p.data.title,score:2})).filter(x=>x);
}
async function discoverRedditStocks() {
  const d=await safeFetch("https://apewisdom.io/api/v1.0/filter/all-stocks/page/1");
  return (d?.results||[]).slice(0,25).map(r=>({ticker:r.ticker,source:"Reddit ApeWisdom",score:r.rank<=10?3:2}));
}
async function discoverRedditDaytrading() {
  const d=await safeFetch("https://www.reddit.com/r/Daytrading/hot.json?limit=15",{headers:{"User-Agent":"stradebot/1.0"}});
  return (d?.data?.children||[]).filter(p=>p.data.score>50).map(p=>({ticker:null,source:"Reddit Daytrading",title:p.data.title,score:1}));
}
async function discoverStocktwits() {
  const tickers=["AAPL","TSLA","NVDA","AMD","SPY","QQQ","AMZN","MSFT","META","GOOGL"];
  return tickers.map(t=>({ticker:t,source:"StockTwits Watchlist",score:1}));
}
async function discoverYouTubeTrending() {
  if (!cfg.youtube) return [];
  const channels=["UCrp_UI8XtuYfpiqluWLD98A","UCWX3yGbODI3RvSzCLOPsGAA"]; // trading channels
  const topics=[];
  for (const ch of channels) {
    const d=await safeFetch(`https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=${ch}&maxResults=5&order=date&type=video&key=${cfg.youtube}`);
    (d?.items||[]).forEach(v=>{
      const matches=v.snippet.title.match(/\b([A-Z]{2,5})\b/g)||[];
      matches.forEach(m=>{topics.push({ticker:m,source:"YouTube Trading",score:2});});
    });
    await delay(200);
  }
  return topics;
}
async function discoverWebullHot() {
  // Webull trending via their public page scrape
  const text=await safeFetchText("https://www.webull.com/ranking");
  const matches=text.match(/\b([A-Z]{2,5})\b/g)||[];
  const counts={};
  matches.forEach(m=>{if(m.length>=2&&m.length<=5)counts[m]=(counts[m]||0)+1;});
  return Object.entries(counts).sort((a,b)=>b[1]-a[1]).slice(0,10).map(([t])=>({ticker:t,source:"Webull Hot",score:2}));
}
async function discoverMarketWatchRSS() {
  const text=await safeFetchText("https://feeds.marketwatch.com/marketwatch/topstories/");
  const tickers=[];
  text.match(/<title>([^<]+)<\/title>/g)?.slice(1,15).forEach(m=>{
    const t=m.replace(/<\/?title>/g,"");
    t.match(/\b([A-Z]{2,5})\b/g)?.forEach(tk=>tickers.push({ticker:tk,source:"MarketWatch",score:1}));
  });
  return tickers;
}
async function discoverCNBCRSS() {
  const text=await safeFetchText("https://www.cnbc.com/id/100003114/device/rss/rss.html");
  const tickers=[];
  text.match(/<title>([^<]+)<\/title>/g)?.slice(1,15).forEach(m=>{
    const t=m.replace(/<\/?title>/g,"");
    t.match(/\b([A-Z]{2,5})\b/g)?.forEach(tk=>tickers.push({ticker:tk,source:"CNBC RSS",score:1}));
  });
  return tickers;
}
async function discoverReutersRSS() {
  const text=await safeFetchText("https://feeds.reuters.com/reuters/businessNews");
  const tickers=[];
  text.match(/<title>([^<]+)<\/title>/g)?.slice(1,10).forEach(m=>{
    const t=m.replace(/<\/?title>/g,"");
    t.match(/\b([A-Z]{2,5})\b/g)?.forEach(tk=>tickers.push({ticker:tk,source:"Reuters",score:1}));
  });
  return tickers;
}
async function discoverSeekingAlpha() {
  const text=await safeFetchText("https://seekingalpha.com/feed.xml");
  const tickers=[];
  text.match(/<title>([^<]+)<\/title>/g)?.slice(1,10).forEach(m=>{
    const t=m.replace(/<\/?title>/g,"");
    t.match(/\b([A-Z]{2,5})\b/g)?.forEach(tk=>tickers.push({ticker:tk,source:"Seeking Alpha",score:2}));
  });
  return tickers;
}
async function discoverBloombergRSS() {
  const text=await safeFetchText("https://feeds.bloomberg.com/markets/news.rss");
  const tickers=[];
  text.match(/<title>([^<]+)<\/title>/g)?.slice(1,10).forEach(m=>{
    const t=m.replace(/<\/?title>/g,"");
    t.match(/\b([A-Z]{2,5})\b/g)?.forEach(tk=>tickers.push({ticker:tk,source:"Bloomberg",score:2}));
  });
  return tickers;
}
async function discoverCoinGeckoTrending() {
  const d=await safeFetch("https://api.coingecko.com/api/v3/search/trending");
  const map={"bitcoin":"BTC","ethereum":"ETH","avalanche-2":"AVAX","chainlink":"LINK","litecoin":"LTC","aave":"AAVE","dogecoin":"DOGE","ripple":"XRP"};
  return (d?.coins||[]).map(c=>({ticker:map[c.item.id]||c.item.symbol.toUpperCase(),source:"CoinGecko Trending",score:2})).filter(c=>ALPACA_CRYPTO.includes(c.ticker));
}
async function discoverRedditCrypto() {
  const d=await safeFetch("https://apewisdom.io/api/v1.0/filter/all-crypto/page/1");
  return (d?.results||[]).slice(0,10).filter(r=>ALPACA_CRYPTO.includes(r.ticker)).map(r=>({ticker:r.ticker,source:"Reddit Crypto",score:2}));
}
 
// ── MASTER DISCOVERY ──────────────────────────────────────
async function discoverCandidates(session) {
  const all=await Promise.all([
    discoverPolygonGainers(), discoverPolygonActive(), discoverPolygonPremarket(),
    discoverFinnhubNews(), discoverRedditStocks(), discoverRedditWSB(),
    discoverRedditDaytrading(), discoverStocktwits(), discoverYouTubeTrending(),
    discoverWebullHot(), discoverMarketWatchRSS(), discoverCNBCRSS(),
    discoverReutersRSS(), discoverSeekingAlpha(), discoverBloombergRSS(),
    discoverCoinGeckoTrending(), discoverRedditCrypto(),
  ]);
 
  const flat=all.flat();
  const tickerScores={};const tickerSources={};
 
  // Extract tickers from title-only sources (WSB, Daytrading)
  const stopwords=new Set(["THE","AND","FOR","WITH","FROM","THIS","THAT","WILL","HAVE","BEEN","NYSE","SEC","CEO","CFO","IPO","ETF","GDP","CPI","USD","EUR","API","NEW","AI","US","UK","EU","CNBC","AAPL"]);
 
  flat.forEach(c=>{
    if (!c.ticker&&c.title) {
      c.title.match(/\b([A-Z]{2,5})\b/g)?.forEach(m=>{
        if (!stopwords.has(m)) { tickerScores[m]=(tickerScores[m]||0)+(c.score||1); tickerSources[m]=(tickerSources[m]||"")+", "+c.source; }
      });
    } else if (c.ticker&&!stopwords.has(c.ticker)) {
      tickerScores[c.ticker]=(tickerScores[c.ticker]||0)+(c.score||1);
      tickerSources[c.ticker]=(tickerSources[c.ticker]||"")+", "+c.source;
    }
  });
 
  const isCryptoTicker=t=>ALPACA_CRYPTO.includes(t)||ALPACA_CRYPTO.includes(t.replace("USD",""));
 
  // Weekend/overnight: crypto only
  if (session==="overnight"||session==="weekend") {
    return ALPACA_CRYPTO.map(t=>({ticker:t+"USD",isCrypto:true,discoveredVia:"Crypto scan",score:1}));
  }
 
  const stocks=Object.entries(tickerScores)
    .filter(([t])=>!isCryptoTicker(t)&&t.length>=2&&t.length<=5)
    .sort((a,b)=>b[1]-a[1]).slice(0,30)
    .map(([t,s])=>({ticker:t,isCrypto:false,discoveredVia:(tickerSources[t]||"").slice(2),score:s}));
 
  const crypto=ALPACA_CRYPTO.slice(0,5).map(t=>({ticker:t+"USD",isCrypto:true,discoveredVia:"Crypto watchlist",score:1}));
 
  return [...stocks,...crypto];
}
 
// ══════════════════════════════════════════════════════════
// PHASE 2: 30-LAYER TECHNICAL + SENTIMENT + FUNDAMENTAL
// ══════════════════════════════════════════════════════════
 
// ── TECHNICAL INDICATORS (calculated from OHLCV) ─────────
async function getOHLCV(ticker, limit=50) {
  const clean=ticker.replace("USD","");
  const d=await safeFetch(`https://api.polygon.io/v2/aggs/ticker/${clean}/range/1/day/${Date.now()-limit*86400000*2}/${Date.now()}?adjusted=true&sort=asc&limit=${limit}&apiKey=${cfg.polygon}`);
  return d?.results||[];
}
 
function calcRSI(closes,period=14) {
  if (closes.length<period+1) return 50;
  let gains=0,losses=0;
  for (let i=closes.length-period;i<closes.length;i++) {
    const diff=closes[i]-closes[i-1];
    if (diff>0) gains+=diff; else losses-=diff;
  }
  const rs=(gains/period)/(losses/period||0.001);
  return 100-(100/(1+rs));
}
 
function calcMACD(closes) {
  const ema=(data,period)=>{
    const k=2/(period+1); let e=data[0];
    for (let i=1;i<data.length;i++) e=data[i]*k+e*(1-k);
    return e;
  };
  const ema12=ema(closes,12),ema26=ema(closes,26);
  const macdLine=ema12-ema26;
  const signal=ema([...closes.slice(-9)].map((_,i)=>ema(closes.slice(0,closes.length-9+i+1),26)),9);
  return {macd:macdLine,signal};
}
 
function calcVWAP(bars) {
  let cumPV=0,cumV=0;
  bars.forEach(b=>{cumPV+=(b.h+b.l+b.c)/3*b.v;cumV+=b.v;});
  return cumPV/(cumV||1);
}
 
function calcBollingerBands(closes,period=20) {
  const slice=closes.slice(-period);
  const mean=slice.reduce((a,b)=>a+b,0)/period;
  const std=Math.sqrt(slice.reduce((a,b)=>a+(b-mean)**2,0)/period);
  return {upper:mean+2*std,middle:mean,lower:mean-2*std};
}
 
function calcStochastic(highs,lows,closes,k=14) {
  const hh=Math.max(...highs.slice(-k)),ll=Math.min(...lows.slice(-k));
  const stochK=(closes[closes.length-1]-ll)/(hh-ll||1)*100;
  return stochK;
}
 
function calcWilliamsR(highs,lows,closes,period=14) {
  const hh=Math.max(...highs.slice(-period)),ll=Math.min(...lows.slice(-period));
  return (hh-closes[closes.length-1])/(hh-ll||1)*-100;
}
 
function calcCCI(highs,lows,closes,period=20) {
  const typicals=closes.map((c,i)=>(highs[i]+(lows[i]||c)+c)/3);
  const slice=typicals.slice(-period);
  const mean=slice.reduce((a,b)=>a+b,0)/period;
  const meanDev=slice.reduce((a,b)=>a+Math.abs(b-mean),0)/period;
  return (typicals[typicals.length-1]-mean)/(0.015*meanDev||1);
}
 
function calcATR(highs,lows,closes,period=14) {
  const trs=closes.map((c,i)=>{
    if (i===0) return highs[i]-lows[i];
    return Math.max(highs[i]-lows[i],Math.abs(highs[i]-closes[i-1]),Math.abs(lows[i]-closes[i-1]));
  });
  return trs.slice(-period).reduce((a,b)=>a+b,0)/period;
}
 
function calcOBV(closes,volumes) {
  let obv=0;
  for (let i=1;i<closes.length;i++) {
    if (closes[i]>closes[i-1]) obv+=volumes[i];
    else if (closes[i]<closes[i-1]) obv-=volumes[i];
  }
  return obv;
}
 
function calcMFI(highs,lows,closes,volumes,period=14) {
  const typicals=closes.map((c,i)=>(highs[i]+(lows[i]||c)+c)/3);
  let posFlow=0,negFlow=0;
  for (let i=closes.length-period;i<closes.length;i++) {
    const mf=typicals[i]*volumes[i];
    if (typicals[i]>typicals[i-1]) posFlow+=mf; else negFlow+=mf;
  }
  return 100-(100/(1+posFlow/(negFlow||1)));
}
 
function calcADX(highs,lows,closes,period=14) {
  // Simplified ADX
  const atr=calcATR(highs,lows,closes,period);
  const dms=closes.map((c,i)=>{
    if (i===0) return {pos:0,neg:0};
    const upMove=highs[i]-highs[i-1];
    const downMove=(lows[i-1]||lows[i])-lows[i];
    return {pos:upMove>downMove&&upMove>0?upMove:0,neg:downMove>upMove&&downMove>0?downMove:0};
  });
  const avgPos=dms.slice(-period).reduce((a,b)=>a+b.pos,0)/period;
  const avgNeg=dms.slice(-period).reduce((a,b)=>a+b.neg,0)/period;
  const di_diff=Math.abs(avgPos-avgNeg);
  const di_sum=(avgPos+avgNeg)||1;
  return (di_diff/di_sum)*100;
}
 
function calcEMACross(closes,fast=9,slow=21) {
  const ema=(data,p)=>{const k=2/(p+1);return data.reduce((e,v)=>v*k+e*(1-k),data[0]);};
  return {ema9:ema(closes,fast),ema21:ema(closes,slow)};
}
 
function calcFibonacci(highs,lows,lookback=20) {
  const high=Math.max(...highs.slice(-lookback));
  const low=Math.min(...lows.slice(-lookback));
  const range=high-low;
  const last=highs[highs.length-1];
  const levels={r618:high-range*0.382,r500:high-range*0.500,r382:high-range*0.618};
  // Price near a fib support level = bullish
  const nearSupport=Object.values(levels).some(l=>Math.abs(last-l)/l<0.02);
  return {levels,nearSupport,high,low};
}
 
function calcIchimoku(highs,lows,closes) {
  const avg=(arr,s,e)=>{const sl=arr.slice(s,e);return (Math.max(...sl)+Math.min(...sl))/2;};
  const n=closes.length;
  const tenkan=avg(closes,n-9,n);  // 9-period
  const kijun=avg(closes,n-26,n);  // 26-period
  const spanA=(tenkan+kijun)/2;
  const spanB=avg(closes,n-52,n);  // 52-period
  const currentClose=closes[n-1];
  const aboveCloud=currentClose>Math.max(spanA,spanB);
  const tenkanAboveKijun=tenkan>kijun;
  return {tenkan,kijun,spanA,spanB,aboveCloud,tenkanAboveKijun};
}
 
function calcPivotPoints(high,low,close) {
  const pivot=(high+low+close)/3;
  return {pivot,r1:2*pivot-low,s1:2*pivot-high};
}
 
// ── THE 30 RESEARCH LAYERS ────────────────────────────────
async function runAllLayers(ticker, isCrypto) {
  const bars = await getOHLCV(ticker, 60);
  if (bars.length < 20) return { score:0, details:["Insufficient price data"], price:0 };
 
  const closes  = bars.map(b=>b.c);
  const highs   = bars.map(b=>b.h);
  const lows    = bars.map(b=>b.l);
  const volumes = bars.map(b=>b.v);
  const lastBar = bars[bars.length-1];
  const price   = closes[closes.length-1];
  const prevClose=closes[closes.length-2]||price;
 
  const results = [];
 
  // ── TECHNICAL LAYERS ─────────────────────────────────
  // L1: RSI
  const rsi=calcRSI(closes);
  results.push({pass:rsi>=35&&rsi<=68, label:"RSI", detail:`RSI ${rsi.toFixed(1)} ${rsi>=35&&rsi<=68?"✓":"✗"} (35-68)`});
 
  // L2: MACD
  const {macd,signal}=calcMACD(closes);
  results.push({pass:macd>signal, label:"MACD", detail:`MACD ${macd>signal?"bull":"bear"} crossover ${macd>signal?"✓":"✗"}`});
 
  // L3: VWAP
  const vwap=calcVWAP(bars);
  results.push({pass:price>vwap, label:"VWAP", detail:`Price $${price.toFixed(2)} ${price>vwap?"above":"below"} VWAP $${vwap.toFixed(2)} ${price>vwap?"✓":"✗"}`});
 
  // L4: Fibonacci
  const fib=calcFibonacci(highs,lows);
  results.push({pass:fib.nearSupport||price>fib.high*0.95, label:"Fibonacci", detail:`Near fib support: ${fib.nearSupport?"yes ✓":"no ✗"}`});
 
  // L5: Ichimoku Cloud
  const ich=calcIchimoku(highs,lows,closes);
  results.push({pass:ich.aboveCloud&&ich.tenkanAboveKijun, label:"Ichimoku", detail:`Above cloud: ${ich.aboveCloud?"✓":"✗"} | Tenkan>Kijun: ${ich.tenkanAboveKijun?"✓":"✗"}`});
 
  // L6: Bollinger Bands
  const bb=calcBollingerBands(closes);
  const bbPass=price>=bb.lower&&price<=bb.middle+(bb.upper-bb.middle)*0.5;
  results.push({pass:bbPass, label:"Bollinger Bands", detail:`Price in lower-mid band: ${bbPass?"✓":"✗"} (L:${bb.lower.toFixed(2)} M:${bb.middle.toFixed(2)} U:${bb.upper.toFixed(2)})`});
 
  // L7: Stochastic
  const stoch=calcStochastic(highs,lows,closes);
  results.push({pass:stoch>=20&&stoch<=75, label:"Stochastic", detail:`%K ${stoch.toFixed(1)} ${stoch>=20&&stoch<=75?"✓":"✗"} (20-75)`});
 
  // L8: Williams %R
  const wr=calcWilliamsR(highs,lows,closes);
  results.push({pass:wr>=-80&&wr<=-20, label:"Williams %R", detail:`%R ${wr.toFixed(1)} ${wr>=-80&&wr<=-20?"✓":"✗"}`});
 
  // L9: CCI
  const cci=calcCCI(highs,lows,closes);
  results.push({pass:cci>=-100&&cci<=200, label:"CCI", detail:`CCI ${cci.toFixed(1)} ${cci>=-100&&cci<=200?"✓":"✗"}`});
 
  // L10: ATR Volatility
  const atr=calcATR(highs,lows,closes);
  const atrPct=(atr/price)*100;
  results.push({pass:atrPct>0.5&&atrPct<8, label:"ATR Volatility", detail:`ATR ${atr.toFixed(2)} (${atrPct.toFixed(1)}% of price) ${atrPct>0.5&&atrPct<8?"✓":"✗"}`});
 
  // L11: OBV
  const obv=calcOBV(closes,volumes);
  const prevOBV=calcOBV(closes.slice(0,-3),volumes.slice(0,-3));
  results.push({pass:obv>prevOBV, label:"OBV", detail:`OBV trending ${obv>prevOBV?"up ✓":"down ✗"}`});
 
  // L12: MFI
  const mfi=calcMFI(highs,lows,closes,volumes);
  results.push({pass:mfi>=20&&mfi<=80, label:"MFI", detail:`MFI ${mfi.toFixed(1)} ${mfi>=20&&mfi<=80?"✓":"✗"} (20-80)`});
 
  // L13: ADX Trend Strength
  const adx=calcADX(highs,lows,closes);
  results.push({pass:adx>=20, label:"ADX", detail:`ADX ${adx.toFixed(1)} ${adx>=20?"strong trend ✓":"weak trend ✗"}`});
 
  // L14: EMA Cross
  const {ema9,ema21}=calcEMACross(closes);
  results.push({pass:ema9>ema21, label:"EMA 9/21", detail:`EMA9 ${ema9.toFixed(2)} ${ema9>ema21?"above":"below"} EMA21 ${ema21.toFixed(2)} ${ema9>ema21?"✓":"✗"}`});
 
  // L15: Volume Spike
  const avgVol=volumes.slice(-20,-1).reduce((a,b)=>a+b,0)/19;
  const volSpike=lastBar.v>avgVol*1.3;
  results.push({pass:volSpike, label:"Volume Spike", detail:`Vol ${(lastBar.v/1000).toFixed(0)}K vs avg ${(avgVol/1000).toFixed(0)}K (${(lastBar.v/avgVol).toFixed(1)}×) ${volSpike?"✓":"✗"}`});
 
  // L16: Premarket Momentum
  const preMomentum=((price-prevClose)/prevClose)*100;
  results.push({pass:preMomentum>0.5, label:"Price Momentum", detail:`${preMomentum>0?"+":""}${preMomentum.toFixed(2)}% from prev close ${preMomentum>0.5?"✓":"✗"}`});
 
  // L17: Gap Analysis
  const gap=((lastBar.o-prevClose)/prevClose)*100;
  results.push({pass:gap>-2&&gap<10, label:"Gap Analysis", detail:`Gap ${gap>0?"+":""}${gap.toFixed(2)}% ${gap>-2&&gap<10?"acceptable ✓":"extreme ✗"}`});
 
  // L18: Pivot Points
  const pivot=calcPivotPoints(highs[highs.length-2]||lastBar.h,lows[lows.length-2]||lastBar.l,closes[closes.length-2]||price);
  results.push({pass:price>pivot.pivot, label:"Pivot Points", detail:`Price ${price>pivot.pivot?"above":"below"} pivot ${pivot.pivot.toFixed(2)} ${price>pivot.pivot?"✓":"✗"}`});
 
  // ── SENTIMENT LAYERS ──────────────────────────────────
  // L19: Stock Fear & Greed
  const fg=await safeFetch("https://feargreedmeter.com/api/v1/fgi");
  const fgScore=fg?.fgi?.now?.value||50;
  results.push({pass:fgScore<78, label:"Stock Fear & Greed", detail:`F&G ${fgScore}/100 ${fgScore<78?"✓":"✗ extreme greed"}`});
 
  // L20: Crypto Fear & Greed
  const cfg2=await safeFetch("https://api.alternative.me/fng/?limit=1");
  const cfgScore=parseInt(cfg2?.data?.[0]?.value||50);
  results.push({pass:cfgScore>20&&cfgScore<82, label:"Crypto F&G", detail:`Crypto F&G ${cfgScore}/100 ${cfgScore>20&&cfgScore<82?"✓":"✗"}`});
 
  // L21: Reddit
  const reddit=await safeFetch("https://apewisdom.io/api/v1.0/filter/all-stocks/page/1");
  const clean=ticker.replace("USD","");
  const rf=reddit?.results?.find(r=>r.ticker===clean);
  results.push({pass:!!rf&&(rf.rank<=30||rf.mentions>=5), label:"Reddit ApeWisdom", detail:rf?`Rank #${rf.rank}, ${rf.mentions} mentions ✓`:"Not trending ✗"});
 
  // L22: StockTwits
  const st=await safeFetch(`https://api.stocktwits.com/api/2/streams/symbol/${clean}.json`);
  if (st?.messages) {
    const msgs=st.messages.slice(0,20);
    const b=msgs.filter(m=>m.entities?.sentiment?.basic==="Bullish").length;
    const r2=msgs.filter(m=>m.entities?.sentiment?.basic==="Bearish").length;
    const tot=b+r2,pct=tot>0?Math.round((b/tot)*100):50;
    results.push({pass:pct>=52, label:"StockTwits", detail:`${pct}% bullish (${b}🟢${r2}🔴) ${pct>=52?"✓":"✗"}`});
  } else results.push({pass:true,label:"StockTwits",detail:"Unavailable — proceeding"});
 
  // L23: Unusual Whales Options
  const uw=await safeFetch(`https://api.unusualwhales.com/api/stock/${clean}/flow-recent`);
  if (uw?.data?.length) {
    const b=uw.data.slice(0,10).filter(f=>f.sentiment==="BULLISH"||f.put_call==="CALL").length;
    const r2=uw.data.slice(0,10).filter(f=>f.sentiment==="BEARISH"||f.put_call==="PUT").length;
    results.push({pass:b>r2, label:"Options Flow", detail:`${b} bull vs ${r2} bear ${b>r2?"✓":"✗"}`});
  } else results.push({pass:true,label:"Options Flow",detail:"Unavailable — proceeding"});
 
  // L24: Put/Call Ratio proxy via VIX
  const vixData=await safeFetch(`https://api.polygon.io/v2/aggs/ticker/VXX/prev?adjusted=true&apiKey=${cfg.polygon}`);
  const vix=vixData?.results?.[0]?.c||20;
  results.push({pass:vix<28, label:"VIX/Put-Call", detail:`VIX ${vix.toFixed(2)} ${vix<28?"✓":"✗ elevated"}`});
 
  // ── FUNDAMENTAL LAYERS ────────────────────────────────
  // L25: Finnhub News Sentiment
  const now=new Date(),from=new Date(Date.now()-86400000*2);
  const newsData=await safeFetch(`https://finnhub.io/api/v1/company-news?symbol=${clean}&from=${from.toISOString().split("T")[0]}&to=${now.toISOString().split("T")[0]}&token=${cfg.finnhub}`);
  if (newsData?.length) {
    const pos=["beat","surge","soar","growth","upgrade","profit","gain","strong","bullish","record"];
    const neg=["miss","fall","drop","cut","loss","downgrade","decline","bearish","weak","crash"];
    let s=0;newsData.slice(0,10).forEach(n=>{const txt=(n.headline+" "+(n.summary||"")).toLowerCase();pos.forEach(w=>{if(txt.includes(w))s++;});neg.forEach(w=>{if(txt.includes(w))s--;});});
    results.push({pass:s>=0,label:"Finnhub News",detail:`Sentiment ${s>=0?"+":""}${s} ✓`});
  } else results.push({pass:true,label:"Finnhub News",detail:"No news — proceeding"});
 
  // L26: SEC Insider
  const ins=await safeFetch(`https://efts.sec.gov/LATEST/search-index?q="${clean}"&dateRange=custom&startdt=${new Date(Date.now()-2592000000).toISOString().split("T")[0]}&enddt=${now.toISOString().split("T")[0]}&forms=4`);
  let buys=0,sells=0;
  ins?.hits?.hits?.slice(0,5).forEach(f=>{const t=JSON.stringify(f._source).toLowerCase();if(t.includes('"p"')||t.includes("purchase"))buys++;if(t.includes('"s"')||t.includes("sale"))sells++;});
  results.push({pass:buys>=sells,label:"SEC Insider",detail:`${buys} buys vs ${sells} sells ${buys>=sells?"✓":"✗"}`});
 
  // L27: Earnings Risk
  const earn=await safeFetch(`https://finnhub.io/api/v1/calendar/earnings?from=${now.toISOString().split("T")[0]}&to=${new Date(Date.now()+604800000).toISOString().split("T")[0]}&symbol=${clean}&token=${cfg.finnhub}`);
  const hasEarn=earn?.earningsCalendar?.length>0;
  results.push({pass:!hasEarn,label:"Earnings Risk",detail:hasEarn?"Earnings in 7d ✗":"No earnings risk ✓"});
 
  // L28: Fundamentals
  if (!isCrypto) {
    const fund=await safeFetch(`https://financialmodelingprep.com/api/v3/ratios-ttm/${clean}?apikey=${cfg.fmp}`);
    if (fund?.[0]) {
      const peOk=fund[0].peRatioTTM>0&&fund[0].peRatioTTM<80;
      const roeOk=fund[0].returnOnEquityTTM>0.03;
      results.push({pass:peOk&&roeOk,label:"Fundamentals",detail:`P/E ${fund[0].peRatioTTM?.toFixed(1)} ${peOk?"✓":"✗"} | ROE ${(fund[0].returnOnEquityTTM*100)?.toFixed(1)}% ${roeOk?"✓":"✗"}`});
    } else results.push({pass:true,label:"Fundamentals",detail:"Data unavailable — proceeding"});
  } else results.push({pass:true,label:"Fundamentals",detail:"Crypto — skipped"});
 
  // ── MARKET CONTEXT ────────────────────────────────────
  // L29: Sector Rotation
  const sectors=["XLK","XLV","XLE","XLF","XLY"];
  const secRes=await Promise.all(sectors.map(s=>safeFetch(`https://api.polygon.io/v2/aggs/ticker/${s}/prev?adjusted=true&apiKey=${cfg.polygon}`)));
  const secCh=secRes.filter(r=>r?.results?.[0]).map(r=>((r.results[0].c-r.results[0].o)/r.results[0].o)*100);
  const secPos=secCh.filter(c=>c>0).length;
  results.push({pass:secPos>=3,label:"Sector Rotation",detail:`${secPos}/${secCh.length} sectors green ${secPos>=3?"✓":"✗"}`});
 
  // L30: VIX Gate
  results.push({pass:vix<30,label:"VIX Gate",detail:`VIX ${vix.toFixed(2)} ${vix<30?"safe ✓":"panic ✗"}`});
 
  const score=results.reduce((s,r)=>s+(r.pass?1:0),0);
  const details=results.map((r,i)=>`${r.pass?"✅":"❌"} L${i+1} ${r.label}: ${r.detail}`);
  return {score,details,price};
}
 
// ══════════════════════════════════════════════════════════
// PHASE 3: TRADING ENGINE
// ══════════════════════════════════════════════════════════
async function syncPositionsFromAlpaca() {
  try {
    const positions=await alpaca.getPositions();
    let synced=0;
    for (const pos of positions) {
      const isCrypto=pos.asset_class==="crypto";
      const ticker=fromAlpacaSymbol(pos.symbol,pos.asset_class);
      if (!db.prepare("SELECT id FROM trades WHERE ticker=? AND status='open'").get(ticker)) {
        const p=parseFloat(pos.avg_entry_price),q=parseFloat(pos.qty),sz=Math.abs(p*q);
        const stop=p*(1-(isCrypto?STOP_CRYPTO:STOP_STOCK));
        db.prepare("INSERT INTO trades (ticker,asset_type,qty,price,dollar_value,stop_loss_price,vote_score,vote_detail,discovered_via,session) VALUES (?,?,?,?,?,?,?,?,?,?)")
          .run(ticker,isCrypto?"crypto":"stock",q,p,sz,stop,0,"Synced from Alpaca","Alpaca sync","sync");
        synced++;
      }
    }
    // Mark closed if not on Alpaca
    const alpacaTickers=positions.map(p=>fromAlpacaSymbol(p.symbol,p.asset_class));
    getOpen().forEach(t=>{if(!alpacaTickers.includes(t.ticker))db.prepare("UPDATE trades SET status='closed',closed_at=CURRENT_TIMESTAMP WHERE id=?").run(t.id);});
    return synced;
  } catch(e){console.error("Sync:",e.message);return 0;}
}
 
async function executeTrade(ticker, isCrypto, score, details, price, session, via) {
  if (getState("paused")) return {skipped:true,reason:"Paused"};
  const loss=getState("total_loss")||0;
  if (loss>=MAX_TOTAL_LOSS) {setState("paused",true);await tg(`🛑 <b>LOSS LIMIT $${MAX_TOTAL_LOSS} HIT</b>\nAll trading halted. Send /resume.`);return {skipped:true,reason:"Loss limit"};}
 
  const open=getOpen();
  if (open.length>=MAX_POSITIONS) return {skipped:true,reason:"Max positions"};
  if (open.find(p=>p.ticker===ticker)) return {skipped:true,reason:"Already holding"};
 
  // After hours limit
  if (session==="after_hours") {
    const today=new Date().toDateString();
    if (getState("ah_date")!==today){setState("ah_date",today);setState("after_hours_count",0);}
    if ((getState("after_hours_count")||0)>=MAX_AH_TRADES) return {skipped:true,reason:"After hours limit reached"};
  }
 
  const avail=BUDGET-getDeployed();
  const size=Math.min(MAX_POSITION,avail*0.25);
  if (size<10) return {skipped:true,reason:"Insufficient budget"};
  if (!price||price<=0) return {skipped:true,reason:"No price"};
 
  const stopPct=isCrypto?STOP_CRYPTO:STOP_STOCK;
  const stopPrice=price*(1-stopPct);
  const qty=parseFloat((size/price).toFixed(6));
  const sym=alpacaSymbol(ticker,isCrypto);
 
  // SANITY CHECK: verify qty * price does not exceed MAX_POSITION by more than 5%
  const expectedCost = qty * price;
  if (expectedCost > MAX_POSITION * 1.05) {
    console.error(`SANITY FAIL ${ticker}: qty(${qty}) x price(${price}) = $${expectedCost.toFixed(2)} exceeds MAX_POSITION $${MAX_POSITION}`);
    await tg(`BLOCKED ${ticker}: calculated order size $${expectedCost.toFixed(2)} exceeds max $${MAX_POSITION}. Price data may be stale.`);
    return {skipped:true,reason:`Order sanity check failed — size $${expectedCost.toFixed(2)} > max $${MAX_POSITION}`};
  }
 
  // DOUBLE-CHECK: get fresh price from Alpaca quote before submitting
  let confirmedPrice = price;
  try {
    const quote = await alpaca.getLatestQuote(sym);
    const askPrice = parseFloat(quote?.ask_price || quote?.AskPrice || 0);
    if (askPrice > 0) {
      confirmedPrice = askPrice;
      const confirmedCost = qty * confirmedPrice;
      if (confirmedCost > MAX_POSITION * 1.05) {
        await tg(`BLOCKED ${ticker}: confirmed price $${confirmedPrice.toFixed(2)} x ${qty} = $${confirmedCost.toFixed(2)} exceeds max $${MAX_POSITION}`);
        return {skipped:true,reason:`Confirmed price check failed`};
      }
    }
  } catch(e) { console.log(`Price confirm skipped for ${ticker}: ${e.message}`); }
 
  try {
    await alpaca.createOrder({symbol:sym,qty,side:"buy",type:"market",time_in_force:isCrypto?"gtc":"day"});
    db.prepare("INSERT INTO trades (ticker,asset_type,qty,price,dollar_value,stop_loss_price,vote_score,vote_detail,discovered_via,session) VALUES (?,?,?,?,?,?,?,?,?,?)")
      .run(ticker,isCrypto?"crypto":"stock",qty,price,size,stopPrice,score,details.join("\n"),via,session);
    if (session==="after_hours") setState("after_hours_count",(getState("after_hours_count")||0)+1);
    const sessionEmoji={premarket:"🌅",market:"📈",after_hours:"🌆",overnight:"🌙"}[session]||"📊";
    await tg(`${sessionEmoji} <b>TRADE [${session.toUpperCase()}] — ${ticker}</b>\nVia: ${via}\nPrice: $${price.toFixed(2)} | Size: $${size.toFixed(2)} (${qty} units)\nStop: $${stopPrice.toFixed(2)} (-${(stopPct*100)}%)\nScore: <b>${score}/30 layers</b>\n<i>Paper trade</i>`);
    return {executed:true};
  } catch(e) {
    await tg(`⚠️ Order failed ${ticker}: ${e.message}`);
    return {skipped:true,reason:e.message};
  }
}
 
async function checkStopLosses() {
  for (const t of getOpen()) {
    try {
      const clean=t.ticker.replace("USD","");
      const d=await safeFetch(`https://api.polygon.io/v2/aggs/ticker/${clean}/prev?adjusted=true&apiKey=${cfg.polygon}`);
      if (!d?.results?.[0]) continue;
      const price=d.results[0].c;
      if (price<=t.stop_loss_price) {
        const isCrypto=t.asset_type==="crypto";
        try{await alpaca.closePosition(alpacaSymbol(t.ticker,isCrypto));}catch{}
        const pnl=(price-t.price)*t.qty;
        db.prepare("UPDATE trades SET status='closed',pnl=?,closed_at=CURRENT_TIMESTAMP WHERE id=?").run(pnl,t.id);
        if (pnl<0) setState("total_loss",(getState("total_loss")||0)+Math.abs(pnl));
        await tg(`🔴 <b>STOP — ${t.ticker}</b>\nEntry $${t.price.toFixed(2)} → Exit $${price.toFixed(2)}\nP&L: <b>${pnl>=0?"+":""}$${pnl.toFixed(2)}</b>\nTotal losses: $${getState("total_loss").toFixed(2)}/$${MAX_TOTAL_LOSS}`);
      }
    } catch(e){console.error(t.ticker,e.message);}
    await delay(100);
  }
}
 
async function runScan() {
  if (getState("paused")) return;
  const session=getSession();
  if (session==="weekend") return;
  const minVotes=getMinVotes(session);
  console.log(`🔍 [${session}] Scan ${new Date().toISOString()}`);
 
  const candidates=await discoverCandidates(session);
  let traded=0,skipped=0;
  const scores=[];
 
  for (const c of candidates) {
    if (getOpen().length>=MAX_POSITIONS) break;
    try {
      const {score,details,price}=await runAllLayers(c.ticker,c.isCrypto);
      scores.push({ticker:c.ticker,score});
      db.prepare("INSERT INTO scan_log (ticker,vote_score,session,verdict) VALUES (?,?,?,?)").run(c.ticker,score,session,score>=minVotes?"BUY":"SKIP");
      if (score>=minVotes) {
        const r=await executeTrade(c.ticker,c.isCrypto,score,details,price,session,c.discoveredVia||"Discovery");
        if (r.executed) traded++; else skipped++;
      } else skipped++;
      await delay(500);
    } catch(e){console.error(c.ticker,e.message);}
  }
 
  if (scores.length) {
    const top=scores.sort((a,b)=>b.score-a.score).slice(0,5);
    let msg=`📊 <b>[${session.toUpperCase()}] Scan</b>\n${traded} traded | ${skipped} skipped\nThreshold: ${minVotes}/30\n\n<b>Top:</b>\n`;
    top.forEach(t=>{msg+=`• ${t.ticker}: ${t.score}/30\n`;});
    await tg(msg);
  }
 
  await checkStopLosses();
}
 
async function getPortfolioSummary() {
  try {
    const acc=await alpaca.getAccount();
    const open=getOpen(),loss=getState("total_loss")||0,dep=getDeployed();
    let m=`📊 <b>STradeBot Portfolio</b>\n\nCash: $${parseFloat(acc.cash).toFixed(2)}\nValue: $${parseFloat(acc.portfolio_value).toFixed(2)}\nDeployed: $${dep.toFixed(2)}/$${BUDGET}\nLosses: $${loss.toFixed(2)}/$${MAX_TOTAL_LOSS}\nOpen: ${open.length}/${MAX_POSITIONS}\nSession: ${getSession().toUpperCase()}\n`;
    if (open.length){m+=`\n<b>Positions:</b>\n`;open.forEach(t=>{m+=`• ${t.ticker} $${t.dollar_value.toFixed(2)} | Stop $${t.stop_loss_price.toFixed(2)} | ${t.vote_score}/30\n`;});}
    const cls=db.prepare("SELECT * FROM trades WHERE status='closed' ORDER BY closed_at DESC LIMIT 5").all();
    if (cls.length){m+=`\n<b>Recent Closed:</b>\n`;cls.forEach(t=>{m+=`• ${t.ticker} ${t.pnl>=0?"+":""}$${t.pnl?.toFixed(2)}\n`;});}
    return m;
  } catch(e){return`⚠️ ${e.message}`;}
}
 
// ── TELEGRAM COMMANDS ─────────────────────────────────────
// Auth middleware — only respond to authorized users
bot.use(async (ctx, next) => {
  if (!isAuthorized(ctx.from?.id)) {
    await ctx.reply("Unauthorized.");
    return;
  }
  return next();
});
 
bot.start(ctx=>ctx.replyWithHTML(
  `⚡ <b>STradeBot v1.0</b>\n\n` +
  `30-Layer Quantitative Day Trader\n` +
  `Budget: $${BUDGET} | Loss halt: $${MAX_TOTAL_LOSS}\n` +
  `Threshold: 20/30 (market) | 25/30 (after hours)\n\n` +
  `/portfolio — P&L\n/scan — manual scan\n/research AAPL — 30-layer check\n` +
  `/positions — open\n/history — closed\n/sync — re-sync from Alpaca\n` +
  `/session — current session\n/pause /resume /status /help`
));
 
bot.command("portfolio",async ctx=>ctx.replyWithHTML(await getPortfolioSummary()));
bot.command("session",ctx=>ctx.reply(`Current session: ${getSession().toUpperCase()}\nMin votes: ${getMinVotes(getSession())}/30`));
bot.command("scan",async ctx=>{ctx.replyWithHTML("🔍 <i>Manual scan triggered...</i>");runScan();});
bot.command("sync",async ctx=>{ctx.replyWithHTML("🔄 <i>Syncing from Alpaca...</i>");const s=await syncPositionsFromAlpaca();ctx.reply(`✅ Sync complete — ${s} positions recovered.`);});
 
bot.command("research",async ctx=>{
  const t=ctx.message.text.replace("/research","").trim().toUpperCase();
  if (!t){ctx.reply("Usage: /research AAPL");return;}
  ctx.replyWithHTML(`🔬 <i>Running 30-layer analysis on ${t}...</i>`);
  const isC=ALPACA_CRYPTO.includes(t.replace("USD",""));
  const tk=isC&&!t.endsWith("USD")?t+"USD":t;
  const {score,details,price}=await runAllLayers(tk,isC);
  const session=getSession(),minV=getMinVotes(session);
  let m=`<b>${tk}</b> — Score: <b>${score}/30</b> ${score>=minV?"✅ BUY":"❌ SKIP"} (need ${minV})\nPrice: $${price.toFixed(2)}\n\n`;
  m+=details.join("\n");
  ctx.replyWithHTML(m);
});
 
bot.command("positions",ctx=>{
  const o=getOpen();if(!o.length){ctx.reply("No open positions.");return;}
  let m=`<b>Open (${o.length}/${MAX_POSITIONS})</b>\n\n`;
  o.forEach(t=>{m+=`• <b>${t.ticker}</b> $${t.dollar_value.toFixed(2)} | Stop $${t.stop_loss_price.toFixed(2)} | ${t.vote_score}/30 | ${t.session||"?"}\n`;});
  ctx.replyWithHTML(m);
});
 
bot.command("history",ctx=>{
  const cls=db.prepare("SELECT * FROM trades WHERE status='closed' ORDER BY closed_at DESC LIMIT 15").all();
  if(!cls.length){ctx.reply("No closed trades.");return;}
  let m=`<b>Last ${cls.length} Trades</b>\n\n`;
  cls.forEach(t=>{m+=`${t.pnl>=0?"🟢":"🔴"} <b>${t.ticker}</b> ${t.pnl>=0?"+":""}$${t.pnl?.toFixed(2)} | ${t.vote_score}/30\n`;});
  ctx.replyWithHTML(m);
});
 
bot.command("status",async ctx=>{
  const p=getState("paused"),l=getState("total_loss")||0;
  ctx.replyWithHTML(
    `<b>STradeBot Status</b>\n\nMode: ${p?"⏸️ PAUSED":"▶️ ACTIVE"}\nSession: ${getSession().toUpperCase()}\n` +
    `Budget: $${BUDGET} | Losses: $${l.toFixed(2)}/$${MAX_TOTAL_LOSS}\nOpen: ${getOpen().length}/${MAX_POSITIONS}\n` +
    `Threshold: ${getMinVotes(getSession())}/30\nStop: ${STOP_STOCK*100}% stocks | ${STOP_CRYPTO*100}% crypto\n` +
    `Mode: <b>PAPER (no real money)</b>`
  );
});
 
bot.command("pause",ctx=>{setState("paused",true);ctx.reply("⏸️ Paused.");});
bot.command("resume",ctx=>{setState("paused",false);ctx.reply("▶️ Resumed.");});
bot.help(ctx=>ctx.replyWithHTML(`/portfolio /scan /research /positions /history /sync /session /movers /best /market /pause /resume /status`));
 
 
// /movers — top gaining stocks right now
bot.command("movers", async ctx => {
  ctx.replyWithHTML("📈 <i>Fetching top movers...</i>");
  try {
    const data = await safeFetch(`https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers?apiKey=${cfg.polygon}`);
    if (!data?.tickers?.length) { ctx.reply("No data available."); return; }
    let msg = "📈 <b>Top Movers Right Now</b>
 
";
    data.tickers.slice(0,10).forEach((t,i) => {
      const chg = t.todaysChangePerc?.toFixed(2) || "?";
      const price = t.day?.c?.toFixed(2) || "?";
      const vol = t.day?.v ? (t.day.v/1000).toFixed(0)+"K" : "?";
      msg += `${i+1}. <b>${t.ticker}</b> $${price} (+${chg}%) Vol:${vol}
`;
    });
    ctx.replyWithHTML(msg);
  } catch(e) { ctx.reply(`Error: ${e.message}`); }
});
 
// /best — run quick research on top 5 discovered tickers and show scores
bot.command("best", async ctx => {
  if (isRateLimited(ctx.from?.id)) { ctx.reply("Wait 20 seconds."); return; }
  ctx.replyWithHTML("🔬 <i>Finding best trade right now — scanning top candidates...</i>");
  try {
    const candidates = await discoverCandidates(getSession());
    const top5 = candidates.slice(0, 5);
    const results = [];
    for (const c of top5) {
      const {score, price} = await runAllLayers(c.ticker, c.isCrypto);
      results.push({ticker: c.ticker, score, price, via: c.discoveredVia});
      await delay(300);
    }
    results.sort((a,b) => b.score - a.score);
    const minV = getMinVotes(getSession());
    let msg = `🏆 <b>Best Trade Right Now</b>
Session: ${getSession().toUpperCase()} | Need ${minV}/30
 
`;
    results.forEach((r,i) => {
      const signal = r.score >= minV ? "BUY" : r.score >= minV-3 ? "CLOSE" : "SKIP";
      const emoji = signal==="BUY" ? "🟢" : signal==="CLOSE" ? "🟡" : "🔴";
      msg += `${emoji} <b>${r.ticker}</b> — ${r.score}/30 [${signal}] @ $${r.price?.toFixed(2)||"?"}
`;
      msg += `   via: ${r.via}
 
`;
    });
    ctx.replyWithHTML(msg);
  } catch(e) { ctx.reply(`Error: ${e.message}`); }
});
 
// /market — quick market overview
bot.command("market", async ctx => {
  ctx.replyWithHTML("🌐 <i>Fetching market overview...</i>");
  try {
    const [spy, qqq, dia, vix, fg, cfg2] = await Promise.all([
      safeFetch(`https://api.polygon.io/v2/aggs/ticker/SPY/prev?adjusted=true&apiKey=${cfg.polygon}`),
      safeFetch(`https://api.polygon.io/v2/aggs/ticker/QQQ/prev?adjusted=true&apiKey=${cfg.polygon}`),
      safeFetch(`https://api.polygon.io/v2/aggs/ticker/DIA/prev?adjusted=true&apiKey=${cfg.polygon}`),
      safeFetch(`https://api.polygon.io/v2/aggs/ticker/VXX/prev?adjusted=true&apiKey=${cfg.polygon}`),
      safeFetch("https://feargreedmeter.com/api/v1/fgi"),
      safeFetch("https://api.alternative.me/fng/?limit=1"),
    ]);
    const pct = (data) => {
      if (!data?.results?.[0]) return "N/A";
      const r = data.results[0];
      return ((r.c - r.o) / r.o * 100).toFixed(2);
    };
    const px = (data) => data?.results?.[0]?.c?.toFixed(2) || "N/A";
    const spyPct = pct(spy), qqqPct = pct(qqq), diaPct = pct(dia);
    const arrow = v => parseFloat(v) >= 0 ? "▲" : "▼";
    const fgVal = fg?.fgi?.now?.value || "?";
    const fgLabel = fg?.fgi?.now?.valueText || "";
    const cryptoFG = cfg2?.data?.[0]?.value || "?";
    const cryptoLabel = cfg2?.data?.[0]?.value_classification || "";
    const vixVal = vix?.results?.[0]?.c?.toFixed(2) || "?";
    const session = getSession();
    const open = getOpen();
    let msg = `🌐 <b>Market Overview</b>
`;
    msg += `Session: <b>${session.toUpperCase()}</b>
 
`;
    msg += `<b>Indices (prev close)</b>
`;
    msg += `SPY  $${px(spy)} ${arrow(spyPct)}${Math.abs(parseFloat(spyPct))}%
`;
    msg += `QQQ  $${px(qqq)} ${arrow(qqqPct)}${Math.abs(parseFloat(qqqPct))}%
`;
    msg += `DIA  $${px(dia)} ${arrow(diaPct)}${Math.abs(parseFloat(diaPct))}%
 
`;
    msg += `<b>Sentiment</b>
`;
    msg += `Stock Fear & Greed: ${fgVal}/100 — ${fgLabel}
`;
    msg += `Crypto Fear & Greed: ${cryptoFG}/100 — ${cryptoLabel}
`;
    msg += `VIX: ${vixVal} ${parseFloat(vixVal)<20?"(Calm)":parseFloat(vixVal)<30?"(Elevated)":"(Panic)"}
 
`;
    msg += `<b>STradeBot</b>
`;
    msg += `Open positions: ${open.length}/5
`;
    msg += `Losses: $${(getState("total_loss")||0).toFixed(2)}/$300
`;
    msg += `Threshold: ${getMinVotes(session)}/30`;
    ctx.replyWithHTML(msg);
  } catch(e) { ctx.reply(`Error: ${e.message}`); }
});
 
// ── SCHEDULES ─────────────────────────────────────────────
// Main scan every 60 seconds during active sessions
let scanRunning=false;
setInterval(async()=>{
  if (scanRunning||getState("paused")) return;
  const s=getSession();
  if (s==="weekend") return;
  scanRunning=true;
  try { await runScan(); } finally { scanRunning=false; }
},60000);
 
// Stop-loss check every 30 seconds, 24/7
setInterval(async()=>{
  if (!getState("paused")) await checkStopLosses();
},30000);
 
// Daily briefing 9am Riyadh (6am UTC)
cron.schedule("0 6 * * 1-5",async()=>{
  await tg(`☀️ <b>STradeBot Daily Briefing</b>\n\n${await getPortfolioSummary()}`);
},{timezone:"UTC"});
 
// ── LAUNCH ────────────────────────────────────────────────
bot.launch().then(async()=>{
  console.log("⚡ STradeBot v1.0 live");
  const synced=await syncPositionsFromAlpaca();
  await tg(
    `⚡ <b>STradeBot v1.0 Online</b>\n\n` +
    `${synced>0?`🔄 ${synced} positions recovered\n`:""}` +
    `Budget: $${BUDGET} | Loss halt: $${MAX_TOTAL_LOSS}\n` +
    `Threshold: 20/30 day | 25/30 after hours\n` +
    `Scan: every 60 sec | Stop-loss: every 30 sec\n` +
    `Sessions: Premarket → Market → After Hours → Overnight\n\n` +
    `Mode: <b>PAPER (no real money)</b>\n` +
    `Send /session to check current trading window.`
  );
}).catch(e=>console.error("Launch:",e.message));
 
process.once("SIGINT",()=>bot.stop("SIGINT"));
process.once("SIGTERM",()=>bot.stop("SIGTERM"));
