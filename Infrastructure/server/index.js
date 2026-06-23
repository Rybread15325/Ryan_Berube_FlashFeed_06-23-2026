import 'dotenv/config'
import express from 'express'
import mongoose from 'mongoose'
import cors    from 'cors'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { connectDB } from './db.js'
import Redis from 'ioredis'

import articlesRouter    from './routes/articles.js'
import screenerRouter    from './routes/screener.js'
import socialRouter      from './routes/social.js'
import correlationRouter from './routes/correlation.js'
import settingsRouter    from './routes/settings.js'

const app  = express()
const PORT = process.env.PORT || 3001
const SERVER_DIR = path.dirname(fileURLToPath(import.meta.url))
const PROJECT_ROOT = path.resolve(SERVER_DIR, '../..')
const MARKET_WINDOW_TIME_ZONE = process.env.MARKET_WINDOW_TIMEZONE || 'America/New_York'
const MARKET_WINDOW_CLOSE_HOUR = Number(process.env.MARKET_WINDOW_CLOSE_HOUR_ET || 17)
const TRACKED_TICKER_FILE_CANDIDATES = [
  path.join(process.cwd(), 'config', 'social_tickers_100.txt'),
  path.join(PROJECT_ROOT, 'config', 'social_tickers_100.txt'),
  path.join(SERVER_DIR, 'config', 'social_tickers_100.txt'),
]
const TRACKED_TICKER_LIMIT = Math.max(1, Number(process.env.TRACKED_TICKER_LIMIT || process.env.SOCIAL_MAX_TICKERS || 250))
const NON_STOCK_TICKERS = new Set([
  "BTC", "ETH", "LTC", "DOGE", "SOL", "ADA", "XRP", "BNB", "DOT", "AVAX",
  "MATIC", "SHIB", "TRX", "BCH", "LINK", "ATOM", "UNI", "ETC", "FIL",
  "USD", "USDT", "USDC", "SPOT",
])
const US_EXCHANGES = new Set(["NASDAQ", "NYSE", "AMEX"])
const TRACKED_MARKET_INDICES = [
  { symbol: "DJI", name: "Dow Jones Industrial Average", category: "index" },
  { symbol: "SPX", name: "S&P 500", category: "index" },
  { symbol: "IXIC", name: "Nasdaq Composite", category: "index" },
  { symbol: "RUT", name: "Russell 2000 Index", category: "index" },
  { symbol: "NYA", name: "NYSE Composite", category: "index" },
]
const TRACKED_MARKETS = [
  ...Array.from(US_EXCHANGES).map(symbol => ({ symbol, name: `${symbol} listed equities`, category: "exchange" })),
  ...TRACKED_MARKET_INDICES,
]
const MAX_SIGNAL_CHANGE_PCT = Math.max(10, Number(process.env.MAX_SIGNAL_CHANGE_PCT || 300))
const PRIVATE_TRACKED_TICKERS = new Set(['SPACEX'])

// ── Middleware ────────────────────────────────────────────
app.use(cors({ origin: ['http://localhost:5173', 'http://127.0.0.1:5173'] }))
app.use(express.json({ limit: '2mb' }))

// ── RAM speed layer: Redis hot cache + Kafka→Redis feed reads ─────────────────
// Redis holds (a) a short-TTL cache of the expensive Mongo aggregations so the
// dashboard reads from RAM, and (b) the per-ticker hot window the Kafka consumer
// streams in (feed:{TICKER} ZSet → event:{id} hashes). If Redis is unavailable,
// every path transparently falls back to MongoDB — the app never breaks.
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379'
let redis = null
try {
  redis = new Redis(REDIS_URL, {
    lazyConnect: true,
    maxRetriesPerRequest: 2,
    enableOfflineQueue: false,
    enableAutoPipelining: true,
    keepAlive: 30000,
    retryStrategy: (n) => (n > 10 ? null : Math.min(n * 200, 2000)),
  })
  redis.on('error', () => {})                       // quiet; usage is guarded by status check
  redis.on('ready', () => console.log('  Redis   →  connected (RAM cache + hot feed active)'))
  redis.connect().catch(() => console.warn('  Redis   →  not reachable; serving from MongoDB only'))
} catch (e) {
  console.warn('  Redis   →  disabled:', e.message)
  redis = null
}
const redisReady = () => !!redis && redis.status === 'ready'

// Transparent response cache for the heaviest GETs — identical JSON shape, served
// from RAM within the TTL window. Only successful (200) responses are cached.
const CACHE_RULES = [
  { match: (p) => p === '/api/screener',        ttl: Number(process.env.CACHE_TTL_SCREENER || 20) },
  { match: (p) => p === '/api/social/rolling',  ttl: Number(process.env.CACHE_TTL_SOCIAL || 15) },
  { match: (p) => p.startsWith('/api/charts/'), ttl: Number(process.env.CACHE_TTL_CHARTS || 20) },
  { match: (p) => p === '/api/momentum',        ttl: Number(process.env.CACHE_TTL_MOMENTUM || 15) },
  { match: (p) => p === '/api/correlation',     ttl: Number(process.env.CACHE_TTL_CORRELATION || 30) },
  { match: (p) => p === '/api/articles',        ttl: Number(process.env.CACHE_TTL_ARTICLES || 15) },
  { match: (p) => p.startsWith('/api/ai/'),     ttl: Number(process.env.CACHE_TTL_AI || 60) },
]
const cacheTtlFor = (p) => { const r = CACHE_RULES.find((rule) => rule.match(p)); return r ? r.ttl : 0 }
app.use(async (req, res, next) => {
  if (req.method !== 'GET' || !redisReady()) return next()
  const ttl = cacheTtlFor(req.path)
  if (!ttl) return next()
  const key = 'cache:' + req.originalUrl
  try {
    const hit = await redis.get(key)
    if (hit) { res.set('X-Cache', 'HIT'); return res.type('application/json').send(hit) }
  } catch (_) { /* fall through to compute from Mongo */ }
  const sendJson = res.json.bind(res)
  res.json = (body) => {
    if (res.statusCode === 200) {
      res.set('X-Cache', 'MISS')
      try { redis.set(key, JSON.stringify(body), 'EX', ttl).catch(() => {}) } catch (_) {}
    }
    return sendJson(body)
  }
  next()
})

// GET /api/feed/:ticker — RAM-speed read of the per-ticker hot window that the
// Kafka consumer streams into Redis (feed:{TICKER} ZSet → event:{id} hashes).
app.get('/api/feed/:ticker', async (req, res) => {
  const ticker = String(req.params.ticker || '').toUpperCase().trim()
  const limit = Math.min(100, Math.max(1, Number(req.query.limit) || 25))
  if (!ticker) return res.status(400).json({ error: 'ticker required' })
  if (!redisReady()) {
    return res.status(503).json({ ticker, source: 'none', count: 0, events: [],
      note: 'Redis not connected — start Redis and the Kafka consumer to use the hot feed.' })
  }
  try {
    const ids = await redis.zrevrange(`feed:${ticker}`, 0, limit - 1)
    if (!ids || !ids.length) return res.json({ ticker, source: 'redis', count: 0, events: [] })
    const pipe = redis.pipeline()
    ids.forEach((id) => pipe.hgetall(`event:${id}`))
    const rows = await pipe.exec()
    const events = rows
      .map(([err, h]) => (err || !h || Object.keys(h).length === 0) ? null : h)
      .filter(Boolean)
      .map((h) => {
        let s = Number(h.sentiment_score)
        if (Number.isNaN(s) && h.payload) { try { s = Number(JSON.parse(h.payload).sentiment_score) } catch (_) {} }
        return { ...h, sentiment_score: Number.isNaN(s) ? null : s }
      })
    const svals = events.map((e) => e.sentiment_score).filter((n) => typeof n === 'number')
    const avg = svals.length ? +(svals.reduce((a, b) => a + b, 0) / svals.length).toFixed(3) : null
    res.json({ ticker, source: 'redis', count: events.length, avg_sentiment: avg, events })
  } catch (e) {
    res.status(500).json({ ticker, error: e.message, events: [] })
  }
})

// ── AI analysis: directional scores + market overview from recent news ────────
// The "AI score" aggregates the per-article sentiment (already produced by the
// FinBERT + LLM sentiment stage) over the last few days into a directional
// -100..+100 score per ticker. Results are cached in Redis (RAM) for speed.
const AI_ARTICLES_COLLECTION = process.env.ARTICLES_COLLECTION || 'articles'
async function aiRecentArticles(db, days) {
  const since = new Date(Date.now() - days * 24 * 3600 * 1000)
  const sinceSec = Math.floor(since.getTime() / 1000)
  const q = { $or: [
    { published_at: { $gte: since } }, { detected_at: { $gte: since } },
    { createdAt: { $gte: since } }, { published: { $gte: since } }, { timestamp: { $gte: since } },
    { publish_date: { $gte: sinceSec } }, { detected_at: { $gte: sinceSec } },
    { fetched_date: { $gte: sinceSec } }, { timestamp: { $gte: sinceSec } },
  ] }
  const projection = { tickers: 1, ticker: 1, symbol: 1, symbols: 1, sentiment_score: 1, sentiment: 1,
    finbert_score: 1, gemini_sentiment: 1, title: 1, headline: 1, source: 1 }
  try {
    return await db.collection(AI_ARTICLES_COLLECTION).find(q, { projection }).limit(8000).toArray()
  } catch (_) {
    return await db.collection(AI_ARTICLES_COLLECTION).find({}, { projection }).sort({ _id: -1 }).limit(4000).toArray()
  }
}
function aiSentiment(a) {
  let v = a.sentiment_score ?? a.finbert_score ?? a.gemini_sentiment ?? a.sentiment
  if (typeof v === 'string') {
    const s = v.toLowerCase()
    if (s.includes('bull') || s === 'positive') return 0.6
    if (s.includes('bear') || s === 'negative') return -0.6
    if (s === 'neutral') return 0
    v = parseFloat(v)
  }
  return Number.isFinite(v) ? v : null
}
function aiTickers(a) {
  if (Array.isArray(a.tickers)) return a.tickers
  if (Array.isArray(a.symbols)) return a.symbols
  if (a.ticker) return [a.ticker]
  if (a.symbol) return [a.symbol]
  return []
}
function aiScoreTickers(arts) {
  const m = new Map()
  for (const a of arts) {
    const s = aiSentiment(a); if (s === null) continue
    for (const t of aiTickers(a)) {
      const k = String(t).toUpperCase().trim(); if (!k || k.length > 8) continue
      const e = m.get(k) || { sum: 0, n: 0, pos: 0, neg: 0 }
      e.sum += s; e.n += 1; if (s > 0.15) e.pos += 1; else if (s < -0.15) e.neg += 1
      m.set(k, e)
    }
  }
  return m
}

app.get('/api/ai/scores', async (req, res) => {
  const days = Math.min(14, Math.max(1, Number(req.query.days) || 3))
  const limit = Math.min(100, Math.max(1, Number(req.query.limit) || 30))
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ error: 'MongoDB not connected', scores: [] })
    const arts = await aiRecentArticles(db, days)
    const scored = [...aiScoreTickers(arts).entries()]
      .map(([ticker, e]) => {
        const avg = e.sum / e.n
        const score = Math.round(Math.max(-100, Math.min(100, avg * 100)))
        return {
          ticker, score,
          direction: score > 8 ? 'up' : score < -8 ? 'down' : 'flat',
          confidence: +Math.min(1, e.n / 20).toFixed(2),
          article_count: e.n, bullish: e.pos, bearish: e.neg,
        }
      })
      .filter((x) => x.article_count >= 2)
      .sort((a, b) => Math.abs(b.score) - Math.abs(a.score))
      .slice(0, limit)
    res.json({ days, generated_at: Date.now(), model: 'news-sentiment-aggregate', scores: scored })
  } catch (e) {
    res.status(500).json({ error: e.message, scores: [] })
  }
})

app.get('/api/ai/overview', async (req, res) => {
  const days = Math.min(14, Math.max(1, Number(req.query.days) || 3))
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ error: 'MongoDB not connected' })
    const arts = await aiRecentArticles(db, days)
    const sents = arts.map(aiSentiment).filter((n) => n !== null)
    const avg = sents.length ? sents.reduce((a, b) => a + b, 0) / sents.length : 0
    const ranked = [...aiScoreTickers(arts).entries()]
      .filter(([, e]) => e.n >= 2)
      .map(([ticker, e]) => ({ ticker, avg: e.sum / e.n, n: e.n }))
    const bull = [...ranked].sort((a, b) => b.avg - a.avg).slice(0, 5)
    const bear = [...ranked].sort((a, b) => a.avg - b.avg).slice(0, 5)
    const mood = avg > 0.1 ? 'risk-on' : avg < -0.1 ? 'risk-off' : 'mixed'
    const summary =
      `Across ${arts.length} ticker-tagged articles in the last ${days} day(s), overall news sentiment is ` +
      `${mood} (avg ${avg.toFixed(2)}). ` +
      (bull.length ? `Strongest positive coverage: ${bull.map((b) => b.ticker).join(', ')}. ` : '') +
      (bear.length ? `Most negative: ${bear.map((b) => b.ticker).join(', ')}.` : '')
    res.json({
      days, generated_at: Date.now(), model: 'news-sentiment-aggregate',
      article_count: arts.length, avg_sentiment: +avg.toFixed(3), mood, summary,
      top_bullish: bull.map((b) => ({ ticker: b.ticker, score: Math.round(b.avg * 100), article_count: b.n })),
      top_bearish: bear.map((b) => ({ ticker: b.ticker, score: Math.round(b.avg * 100), article_count: b.n })),
    })
  } catch (e) {
    res.status(500).json({ error: e.message })
  }
})

const SUPPORTED_TRANSLATION_LANGUAGES = new Set(["en", "es", "fr", "de", "pt", "ja"])
const UNSUPPORTED_TRANSLATION_SCRIPT_RE = /[\u3400-\u9FFF\uF900-\uFAFF\u3040-\u30FF\uAC00-\uD7AF]/

const FINANCE_GLOSSARY = {
  en: [
    ["informa que", "reports that"],
    ["sus activos totales", "its total assets"],
    ["ascienden a", "amount to"],
    ["millones de dolares", "million dollars"],
    ["millones de dólares", "million dollars"],
    ["acciones", "stocks"],
    ["accion", "stock"],
    ["acción", "stock"],
    ["mercado bursatil", "stock market"],
    ["mercado bursátil", "stock market"],
    ["mercado de acoes", "stock market"],
    ["mercado de ações", "stock market"],
    ["ingresos", "revenue"],
    ["receita", "revenue"],
    ["chiffre d'affaires", "revenue"],
    ["umsatz", "revenue"],
    ["resultados", "earnings"],
    ["resultats", "earnings"],
    ["résultats", "earnings"],
    ["gewinne", "earnings"],
    ["ganancia", "profit"],
    ["lucro", "profit"],
    ["benefice", "profit"],
    ["bénéfice", "profit"],
    ["gewinn", "profit"],
    ["perdida", "loss"],
    ["pérdida", "loss"],
    ["perte", "loss"],
    ["verlust", "loss"],
    ["fusion", "merger"],
    ["fusión", "merger"],
    ["fusao", "merger"],
    ["fusão", "merger"],
    ["gappei", "merger"],
    ["adquisicion", "acquisition"],
    ["adquisición", "acquisition"],
    ["aquisicao", "acquisition"],
    ["aquisição", "acquisition"],
    ["ubernahme", "acquisition"],
    ["übernahme", "acquisition"],
    ["prevision", "guidance"],
    ["previsión", "guidance"],
    ["previsions", "guidance"],
    ["prévisions", "guidance"],
    ["projecao", "guidance"],
    ["projeção", "guidance"],
    ["ausblick", "guidance"],
    ["dividendo", "dividend"],
    ["dividende", "dividend"],
    ["inflacion", "inflation"],
    ["inflación", "inflation"],
    ["inflacao", "inflation"],
    ["inflação", "inflation"],
    ["mercado", "market"],
    ["marche", "market"],
    ["marché", "market"],
    ["markt", "market"],
    ["preco", "price"],
    ["preço", "price"],
    ["precio", "price"],
    ["prix", "price"],
    ["preis", "price"],
    ["sube", "rises"],
    ["sobe", "rises"],
    ["steigt", "rises"],
    ["cae", "falls"],
    ["cai", "falls"],
    ["baisse", "falls"],
    ["fallt", "falls"],
    ["fällt", "falls"],
    ["supera", "beats"],
    ["depasse", "beats"],
    ["dépasse", "beats"],
    ["ubertrifft", "beats"],
    ["übertrifft", "beats"],
  ],
  es: [
    ["stock market", "mercado bursatil"],
    ["stocks", "acciones"],
    ["stock", "accion"],
    ["shares", "acciones"],
    ["earnings", "resultados"],
    ["revenue", "ingresos"],
    ["profit", "ganancia"],
    ["loss", "perdida"],
    ["merger", "fusion"],
    ["acquisition", "adquisicion"],
    ["upgrade", "mejora"],
    ["downgrade", "rebaja"],
    ["guidance", "prevision"],
    ["dividend", "dividendo"],
    ["inflation", "inflacion"],
    ["market", "mercado"],
    ["price", "precio"],
    ["rally", "repunte"],
    ["falls", "cae"],
    ["rises", "sube"],
    ["beats", "supera"],
    ["misses", "no alcanza"],
  ],
  fr: [
    ["stock market", "marche boursier"],
    ["stocks", "actions"],
    ["stock", "action"],
    ["shares", "actions"],
    ["earnings", "resultats"],
    ["revenue", "chiffre d'affaires"],
    ["profit", "benefice"],
    ["loss", "perte"],
    ["merger", "fusion"],
    ["acquisition", "acquisition"],
    ["upgrade", "relevement"],
    ["downgrade", "abaissement"],
    ["guidance", "previsions"],
    ["dividend", "dividende"],
    ["inflation", "inflation"],
    ["market", "marche"],
    ["price", "prix"],
    ["rally", "rebond"],
    ["falls", "baisse"],
    ["rises", "monte"],
    ["beats", "depasse"],
    ["misses", "rate"],
  ],
  de: [
    ["stock market", "aktienmarkt"],
    ["stocks", "aktien"],
    ["stock", "aktie"],
    ["shares", "anteile"],
    ["earnings", "gewinne"],
    ["revenue", "umsatz"],
    ["profit", "gewinn"],
    ["loss", "verlust"],
    ["merger", "fusion"],
    ["acquisition", "ubernahme"],
    ["upgrade", "heraufstufung"],
    ["downgrade", "herabstufung"],
    ["guidance", "ausblick"],
    ["dividend", "dividende"],
    ["inflation", "inflation"],
    ["market", "markt"],
    ["price", "preis"],
    ["rally", "rallye"],
    ["falls", "fallt"],
    ["rises", "steigt"],
    ["beats", "ubertrifft"],
    ["misses", "verfehlt"],
  ],
  pt: [
    ["stock market", "mercado de acoes"],
    ["stocks", "acoes"],
    ["stock", "acao"],
    ["shares", "acoes"],
    ["earnings", "resultados"],
    ["revenue", "receita"],
    ["profit", "lucro"],
    ["loss", "perda"],
    ["merger", "fusao"],
    ["acquisition", "aquisicao"],
    ["upgrade", "elevacao"],
    ["downgrade", "rebaixamento"],
    ["guidance", "projecao"],
    ["dividend", "dividendo"],
    ["inflation", "inflacao"],
    ["market", "mercado"],
    ["price", "preco"],
    ["rally", "alta"],
    ["falls", "cai"],
    ["rises", "sobe"],
    ["beats", "supera"],
    ["misses", "fica abaixo"],
  ],
  ja: [
    ["stock market", "kabushiki shijo"],
    ["stocks", "kabushiki"],
    ["stock", "kabushiki"],
    ["shares", "kabushiki"],
    ["earnings", "gyoseki"],
    ["revenue", "uriage"],
    ["profit", "rieki"],
    ["loss", "sonshitsu"],
    ["merger", "gappei"],
    ["acquisition", "baishu"],
    ["upgrade", "kakuzuke hikiage"],
    ["downgrade", "kakuzuke hikisage"],
    ["guidance", "gyoseki yosou"],
    ["dividend", "haito"],
    ["inflation", "infure"],
    ["market", "shijo"],
    ["price", "kakaku"],
    ["rally", "joraku"],
    ["falls", "geraku"],
    ["rises", "josho"],
    ["beats", "uwamawaru"],
    ["misses", "shitamawaru"],
  ],
}

function escapeRegExp(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
}

function glossaryTranslate(text, targetLanguage) {
  const glossary = FINANCE_GLOSSARY[targetLanguage] || []
  let translated = String(text || "")

  if (targetLanguage === "en") {
    translated = englishFallbackTranslate(translated)
  }

  for (const [source, target] of glossary) {
    translated = translated.replace(new RegExp(`\\b${escapeRegExp(source)}\\b`, "gi"), target)
  }

  return translated
}

const ENGLISH_DIRECT_TRANSLATIONS = [
  [
    /AIFA\s+就涉及\s+HyalRoute Communication Group Limited\s+的股份收購交易正式作出澄清及反駁/i,
    "AIFA issues formal clarification and rebuttal regarding the share acquisition transaction involving HyalRoute Communication Group Limited",
  ],
  [
    /AIFA、ヒアルルート・コミュニケーション・グループに関連する株式取得取引について、正式な説明および反論を発表/i,
    "AIFA issues formal explanation and rebuttal regarding the share acquisition transaction involving HyalRoute Communication Group Limited",
  ],
  [
    /Penjelasan Rasmi dan Penafian oleh AIFA Berhubung Transaksi Pemerolehan Saham yang melibatkan HyalRoute Communication Group Limited/i,
    "Official clarification and denial by AIFA regarding the share acquisition transaction involving HyalRoute Communication Group Limited",
  ],
  [
    /Huasun belegt den 12\. Platz der TIME-Liste der weltweit führenden GreenTech-Unternehmen 2026 und verbessert sich dank seines Engagements für die HJT-Technologie um 22 Plätze/i,
    "Huasun ranks 12th on TIME's 2026 list of the world's leading GreenTech companies and rises 22 places thanks to its commitment to HJT technology",
  ],
]

const ENGLISH_PHRASE_FALLBACKS = [
  ["belegt den", "ranks"],
  ["Platz der", "place on the"],
  ["TIME-Liste", "TIME list"],
  ["weltweit führenden", "world's leading"],
  ["GreenTech-Unternehmen", "GreenTech companies"],
  ["verbessert sich", "rises"],
  ["dank seines Engagements", "thanks to its commitment"],
  ["für die", "to the"],
  ["Technologie", "technology"],
  ["Plätze", "places"],
  ["Juni", "June"],
  ["Unternehmen", "company"],
  ["weltweit", "worldwide"],
  ["führenden", "leading"],
  ["Umsatz", "revenue"],
  ["Gewinn", "profit"],
  ["Verlust", "loss"],
  ["Aktien", "shares"],
  ["Markt", "market"],
  ["Prix", "price"],
  ["marché", "market"],
  ["résultats", "earnings"],
  ["acciones", "stocks"],
  ["mercado", "market"],
  ["ingresos", "revenue"],
  ["receita", "revenue"],
  ["ações", "stocks"],
  ["就涉及", "regarding"],
  ["的股份收購交易", "the share acquisition transaction"],
  ["股份收購交易", "share acquisition transaction"],
  ["正式作出", "formally issues"],
  ["澄清及反駁", "clarification and rebuttal"],
  ["澄清", "clarification"],
  ["反駁", "rebuttal"],
  ["ヒアルルート・コミュニケーション・グループ", "HyalRoute Communication Group"],
  ["に関連する", "regarding"],
  ["株式取得取引", "share acquisition transaction"],
  ["について", "regarding"],
  ["正式な説明", "formal explanation"],
  ["および反論", "and rebuttal"],
  ["を発表", "announces"],
  ["Penjelasan Rasmi", "Official clarification"],
  ["Penafian", "denial"],
  ["Berhubung", "regarding"],
  ["Transaksi Pemerolehan Saham", "share acquisition transaction"],
  ["yang melibatkan", "involving"],
]

function likelyNeedsEnglishFallback(text) {
  return /[\u3400-\u9FFF\uF900-\uFAFF\u3040-\u30FF\uAC00-\uD7AFäöüßéèêàáíóúñçãõ]|(?:\b(?:der|die|das|und|für|mit|von|belegt|verbessert|weltweit|führenden|acciones|mercado|ingresos|résultats|marché|receita|ações|penjelasan|rasmi|penafian|berhubung|transaksi|pemerolehan|saham|melibatkan)\b)/i.test(text)
}

function englishFallbackTranslate(text) {
  const original = String(text || "")

  for (const [pattern, translation] of ENGLISH_DIRECT_TRANSLATIONS) {
    if (pattern.test(original)) return translation
  }

  if (!likelyNeedsEnglishFallback(original)) return original

  let translated = original
  for (const [source, target] of ENGLISH_PHRASE_FALLBACKS) {
    translated = translated.replace(new RegExp(escapeRegExp(source), "gi"), target)
  }

  translated = translated
    .replace(/\bden\b/gi, "the")
    .replace(/\bder\b/gi, "of the")
    .replace(/\bdie\b/gi, "the")
    .replace(/\bdas\b/gi, "the")
    .replace(/\bund\b/gi, "and")
    .replace(/\bum\b/gi, "by")
    .replace(/\bauf\b/gi, "on")
    .replace(/\bin\b/gi, "in")
    .replace(/\bmit\b/gi, "with")

  return translated === original ? `English translation pending: ${original}` : translated
}

async function translateWithProvider(text, targetLanguage) {
  const url = process.env.TRANSLATION_API_URL
  if (!url || typeof fetch !== "function") return null

  const body = {
    q: text,
    text,
    source: "auto",
    target: targetLanguage,
    target_language: targetLanguage,
    format: "text",
  }

  if (process.env.TRANSLATION_API_KEY) {
    body.api_key = process.env.TRANSLATION_API_KEY
    body.apiKey = process.env.TRANSLATION_API_KEY
  }

  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  })

  if (!response.ok) {
    throw new Error(`translation provider returned HTTP ${response.status}`)
  }

  const data = await response.json()
  return data.translatedText || data.translated_text || data.translation || data.text || null
}

function easternParts(date) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: MARKET_WINDOW_TIME_ZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(date)

  return Object.fromEntries(
    parts
      .filter(part => part.type !== 'literal')
      .map(part => [part.type, Number(part.value)])
  )
}

function easternLocalToUtc(year, month, day, hour, minute = 0, second = 0) {
  const target = Date.UTC(year, month - 1, day, hour, minute, second)
  let guess = target

  for (let i = 0; i < 4; i += 1) {
    const parts = easternParts(new Date(guess))
    const actual = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second)
    const diff = target - actual
    if (diff === 0) break
    guess += diff
  }

  return new Date(guess)
}

function shiftLocalDate(year, month, day, deltaDays) {
  const shifted = new Date(Date.UTC(year, month - 1, day + deltaDays))
  return {
    year: shifted.getUTCFullYear(),
    month: shifted.getUTCMonth() + 1,
    day: shifted.getUTCDate(),
  }
}

function localWeekday(year, month, day) {
  return new Date(Date.UTC(year, month - 1, day)).getUTCDay()
}

function latestMarketCloseCutoff(now = new Date()) {
  let { year, month, day, hour } = easternParts(now)
  let weekday = localWeekday(year, month, day)

  if (weekday === 0) {
    ;({ year, month, day } = shiftLocalDate(year, month, day, -2))
  } else if (weekday === 6) {
    ;({ year, month, day } = shiftLocalDate(year, month, day, -1))
  } else if (hour < MARKET_WINDOW_CLOSE_HOUR) {
    ;({ year, month, day } = shiftLocalDate(year, month, day, -1))
    while ([0, 6].includes(localWeekday(year, month, day))) {
      ;({ year, month, day } = shiftLocalDate(year, month, day, -1))
    }
  }

  return easternLocalToUtc(year, month, day, MARKET_WINDOW_CLOSE_HOUR)
}

function articleWindowMatch(cutoffMs) {
  const cutoffSec = Math.floor(cutoffMs / 1000)
  const cutoffDate = new Date(cutoffMs)
  const missingPublishDate = {
    $or: [
      { publish_date: { $exists: false } },
      { publish_date: null },
      { publish_date: "" },
    ],
  }

  return {
    $or: [
      { publish_date: { $type: "date", $gte: cutoffDate } },
      { publish_date: { $type: "int", $gte: cutoffSec } },
      { publish_date: { $type: "long", $gte: cutoffSec } },
      { publish_date: { $type: "double", $gte: cutoffSec } },
      {
        $and: [
          missingPublishDate,
          {
            $or: [
              { fetched_date: { $type: "date", $gte: cutoffDate } },
              { fetched_date: { $type: "int", $gte: cutoffSec } },
              { fetched_date: { $type: "long", $gte: cutoffSec } },
              { fetched_date: { $type: "double", $gte: cutoffSec } },
              { detected_at: { $type: "date", $gte: cutoffDate } },
              { detected_at: { $type: "int", $gte: cutoffSec } },
              { detected_at: { $type: "long", $gte: cutoffSec } },
              { detected_at: { $type: "double", $gte: cutoffSec } },
              { createdAt: { $gte: cutoffDate } },
            ],
          },
        ],
      },
    ],
  }
}

function recentArticleMatch(days = 0) {
  const n = Number(days || 0)
  const cutoffMs = Number.isFinite(n) && n > 0
    ? Date.now() - n * 86_400_000
    : latestMarketCloseCutoff().getTime()

  return articleWindowMatch(cutoffMs)
}

function articleMatchStage(match) {
  return Object.keys(match).length ? [{ $match: match }] : []
}

function tickerArticlePipeline({ days = 2, limit = 150, ticker = "" } = {}) {
  const match = {
    ...recentArticleMatch(days),
    ticker: { $exists: true, $nin: ["", null] },
  }

  const pipeline = [
    { $match: match },
    {
      $addFields: {
        _ticker_parts: {
          $map: {
            input: { $split: [{ $toUpper: { $toString: "$ticker" } }, ","] },
            as: "ticker_part",
            in: { $trim: { input: "$$ticker_part" } }
          }
        }
      }
    },
    { $unwind: "$_ticker_parts" },
    { $match: { _ticker_parts: { $ne: "", $nin: Array.from(NON_STOCK_TICKERS) } } },
  ]

  if (ticker) pipeline.push({ $match: { _ticker_parts: String(ticker).toUpperCase() } })

  pipeline.push(
    {
      $addFields: {
        _article_kind: {
          $cond: [
            {
              $or: [
                { $in: ["$category", ["unstructured_public_title", "public_news", "public_market_news"]] },
                { $eq: ["$collector", "unstructured_news_title_only_v1"] },
                {
                  $regexMatch: {
                    input: { $toLower: { $toString: { $ifNull: ["$source", ""] } } },
                    regex: "unstructured"
                  }
                },
              ],
            },
            "unstructured",
            "structured",
          ],
        },
        _sentiment_direction: {
          $switch: {
            branches: [
              { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bull|positive" } }, then: 1 },
              { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bear|negative" } }, then: -1 },
            ],
            default: 0,
          },
        },
      },
    },
    {
      $addFields: {
        _sentiment_numeric: {
          $switch: {
            branches: [
              { case: { $in: [{ $type: "$sentiment_score" }, ["int", "long", "double", "decimal"] ] }, then: { $toDouble: "$sentiment_score" } },
              { case: { $in: [{ $type: "$ml_confidence" }, ["int", "long", "double", "decimal"] ] }, then: { $multiply: ["$_sentiment_direction", { $toDouble: "$ml_confidence" }] } },
            ],
            default: "$_sentiment_direction",
          },
        },
      },
    },
    {
      $addFields: {
        _source_weight: {
          $switch: {
            branches: [
              {
                case: {
                  $and: [
                    {
                      $or: [
                        { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$source", ""] } } }, regex: "sec|edgar" } },
                        { $eq: [{ $toLower: { $toString: { $ifNull: ["$event_type", ""] } } }, "sec_filing"] },
                      ],
                    },
                    { $lte: [{ $abs: "$_sentiment_numeric" }, 0.08] },
                  ],
                },
                then: 0.15,
              },
              {
                case: {
                  $in: [
                    { $toLower: { $toString: { $ifNull: ["$event_type", ""] } } },
                    ["earnings_beat", "earnings_miss", "guidance_raise", "guidance_cut", "fda_approval", "fda_rejection", "clinical_positive", "clinical_negative", "public_offering", "bankruptcy_default"],
                  ],
                },
                then: 1.35,
              },
            ],
            default: 1,
          },
        },
      },
    },
    {
      $group: {
        _id: "$_ticker_parts",
        count: { $sum: 1 },
        structured_count: { $sum: { $cond: [{ $eq: ["$_article_kind", "structured"] }, 1, 0] } },
        unstructured_count: { $sum: { $cond: [{ $eq: ["$_article_kind", "unstructured"] }, 1, 0] } },
        structured_weight_sum: { $sum: { $cond: [{ $eq: ["$_article_kind", "structured"] }, "$_source_weight", 0] } },
        unstructured_weight_sum: { $sum: { $cond: [{ $eq: ["$_article_kind", "unstructured"] }, "$_source_weight", 0] } },
        bullish: { $sum: { $cond: [{ $gt: ["$_sentiment_numeric", 0.08] }, 1, 0] } },
        bearish: { $sum: { $cond: [{ $lt: ["$_sentiment_numeric", -0.08] }, 1, 0] } },
        neutral: { $sum: { $cond: [{ $lte: [{ $abs: "$_sentiment_numeric" }, 0.08] }, 1, 0] } },
        score_sum: { $sum: "$_sentiment_numeric" },
        structured_bullish: { $sum: { $cond: [{ $and: [{ $eq: ["$_article_kind", "structured"] }, { $gt: ["$_sentiment_numeric", 0.08] }] }, 1, 0] } },
        structured_bearish: { $sum: { $cond: [{ $and: [{ $eq: ["$_article_kind", "structured"] }, { $lt: ["$_sentiment_numeric", -0.08] }] }, 1, 0] } },
        structured_neutral: { $sum: { $cond: [{ $and: [{ $eq: ["$_article_kind", "structured"] }, { $lte: [{ $abs: "$_sentiment_numeric" }, 0.08] }] }, 1, 0] } },
        structured_score_sum: { $sum: { $cond: [{ $eq: ["$_article_kind", "structured"] }, "$_sentiment_numeric", 0] } },
        structured_weighted_score_sum: { $sum: { $cond: [{ $eq: ["$_article_kind", "structured"] }, { $multiply: ["$_sentiment_numeric", "$_source_weight"] }, 0] } },
        unstructured_bullish: { $sum: { $cond: [{ $and: [{ $eq: ["$_article_kind", "unstructured"] }, { $gt: ["$_sentiment_numeric", 0.08] }] }, 1, 0] } },
        unstructured_bearish: { $sum: { $cond: [{ $and: [{ $eq: ["$_article_kind", "unstructured"] }, { $lt: ["$_sentiment_numeric", -0.08] }] }, 1, 0] } },
        unstructured_neutral: { $sum: { $cond: [{ $and: [{ $eq: ["$_article_kind", "unstructured"] }, { $lte: [{ $abs: "$_sentiment_numeric" }, 0.08] }] }, 1, 0] } },
        unstructured_score_sum: { $sum: { $cond: [{ $eq: ["$_article_kind", "unstructured"] }, "$_sentiment_numeric", 0] } },
        unstructured_weighted_score_sum: { $sum: { $cond: [{ $eq: ["$_article_kind", "unstructured"] }, { $multiply: ["$_sentiment_numeric", "$_source_weight"] }, 0] } },
        sources: { $addToSet: "$source" },
        latest_publish: { $max: "$publish_date" },
        latest_fetch: { $max: "$fetched_date" }
      }
    },
    { $sort: { count: -1, latest_publish: -1 } },
    { $limit: Math.max(1, Math.min(300, Number(limit || 150))) },
    {
      $project: {
        _id: 0,
        ticker: "$_id",
        count: 1,
        structured_count: 1,
        unstructured_count: 1,
        structured_weight_sum: 1,
        unstructured_weight_sum: 1,
        bullish: 1,
        bearish: 1,
        neutral: 1,
        score_sum: 1,
        structured_bullish: 1,
        structured_bearish: 1,
        structured_neutral: 1,
        structured_score_sum: 1,
        structured_weighted_score_sum: 1,
        unstructured_bullish: 1,
        unstructured_bearish: 1,
        unstructured_neutral: 1,
        unstructured_score_sum: 1,
        unstructured_weighted_score_sum: 1,
        sources: 1,
        latest_publish: 1,
        latest_fetch: 1
      }
    }
  )

  return pipeline
}

function sentimentScore(row) {
  const hasArticleKinds = row.structured_count != null || row.unstructured_count != null
  if (hasArticleKinds) {
    const structuredWeight = 2
    const unstructuredWeight = 1
    const structuredCount = Number(row.structured_count || 0)
    const unstructuredCount = Number(row.unstructured_count || 0)
    if (row.structured_weighted_score_sum != null || row.unstructured_weighted_score_sum != null) {
      const structuredDenominator = row.structured_weight_sum != null ? Number(row.structured_weight_sum || 0) : structuredCount
      const unstructuredDenominator = row.unstructured_weight_sum != null ? Number(row.unstructured_weight_sum || 0) : unstructuredCount
      const numerator =
        structuredWeight * Number(row.structured_weighted_score_sum || 0) +
        unstructuredWeight * Number(row.unstructured_weighted_score_sum || 0)
      const denominator = structuredWeight * structuredDenominator + unstructuredWeight * unstructuredDenominator
      return denominator ? Number((numerator / (denominator + 1.5)).toFixed(3)) : 0
    }
    if (row.structured_score_sum != null || row.unstructured_score_sum != null) {
      const numerator =
        structuredWeight * Number(row.structured_score_sum || 0) +
        unstructuredWeight * Number(row.unstructured_score_sum || 0)
      const denominator = structuredWeight * structuredCount + unstructuredWeight * unstructuredCount
      return denominator ? Number((numerator / (denominator + 2)).toFixed(3)) : 0
    }
    const numerator =
      structuredWeight * (Number(row.structured_bullish || 0) - Number(row.structured_bearish || 0)) +
      unstructuredWeight * (Number(row.unstructured_bullish || 0) - Number(row.unstructured_bearish || 0))
    const denominator = structuredWeight * structuredCount + unstructuredWeight * unstructuredCount
    return denominator ? Number((numerator / (denominator + 2)).toFixed(3)) : 0
  }

  const total = Math.max(1, Number(row.count || 0))
  const priorNeutralWeight = 4
  return Number((((row.bullish || 0) - (row.bearish || 0)) / (total + priorNeutralWeight)).toFixed(3))
}

function kindSentimentScore(row, kind) {
  const prefix = kind === "unstructured" ? "unstructured" : "structured"
  const count = Number(row?.[`${prefix}_count`] || 0)
  if (!count) return 0
  if (row?.[`${prefix}_weighted_score_sum`] != null) {
    const denominator = Number(row?.[`${prefix}_weight_sum`] || count)
    return denominator ? Number((Number(row[`${prefix}_weighted_score_sum`] || 0) / (denominator + 0.75)).toFixed(3)) : 0
  }
  if (row?.[`${prefix}_score_sum`] != null) {
    return Number((Number(row[`${prefix}_score_sum`] || 0) / (count + 1)).toFixed(3))
  }
  return Number(((Number(row?.[`${prefix}_bullish`] || 0) - Number(row?.[`${prefix}_bearish`] || 0)) / (count + 2)).toFixed(3))
}

function sentimentDirectionValue(value) {
  const text = String(value || "").toLowerCase()
  if (/bull|positive/.test(text)) return 1
  if (/bear|negative/.test(text)) return -1
  return 0
}

function articleSentimentValue(row) {
  if (!row) return 0
  const direct = Number(row.sentiment_score)
  if (Number.isFinite(direct) && direct !== 0) return clamp(direct, -1, 1)
  const direction = sentimentDirectionValue(row.sentiment)
  const confidence = Number(row.ml_confidence)
  if (Number.isFinite(confidence) && confidence > 0) return clamp(direction * confidence, -1, 1)
  return direction
}

function stableHash(value) {
  let hash = 0
  const text = String(value || "")
  for (let i = 0; i < text.length; i += 1) hash = ((hash << 5) - hash + text.charCodeAt(i)) | 0
  return Math.abs(hash)
}

function derivedNumber(ticker, min, max, decimals = 2, salt = "") {
  const span = max - min
  const pct = (stableHash(`${ticker}:${salt}`) % 10000) / 10000
  return Number((min + span * pct).toFixed(decimals))
}

function nullableNumber(value) {
  if (value == null || value === "") return null
  const n = Number(value)
  return Number.isFinite(n) ? n : null
}

function nullableFixed(value, decimals = 2) {
  const n = nullableNumber(value)
  return n == null ? null : Number(n.toFixed(decimals))
}

function clamp(value, min = 0, max = 1) {
  const n = Number(value)
  if (!Number.isFinite(n)) return min
  return Math.max(min, Math.min(max, n))
}

function marketCapBucket(marketCap) {
  const cap = Number(marketCap || 0)
  if (cap >= 200e9) return "Mega"
  if (cap >= 10e9) return "Large"
  if (cap >= 2e9) return "Mid"
  if (cap >= 300e6) return "Small"
  if (cap > 0) return "Micro"
  return "Unknown"
}

function normalizeExchange(value) {
  const raw = String(value || "").trim().toUpperCase()
  if (raw === "NYSEAMERICAN" || raw === "NYSE AMERICAN") return "AMEX"
  if (raw === "NAS") return "NASDAQ"
  return raw
}

function normalizeScreenerDoc(doc = {}) {
  const ticker = String(doc.ticker || "").toUpperCase()
  const hasStoredPrice = doc.price != null
  const price = nullableFixed(doc.price, 2)
  const change = doc.change_pct ?? doc.change_percent
  const changePct = nullableFixed(change, 2)
  const volume = nullableNumber(doc.volume)
  const avgVolume = nullableNumber(doc.avg_volume)
  const relVolume = volume != null && avgVolume ? Number((volume / Math.max(1, avgVolume)).toFixed(2)) : null
  const marketCap = nullableNumber(doc.market_cap)
  const avgSentiment = Number(doc.avg_sentiment ?? doc.news_sentiment ?? doc.structured_sentiment ?? 0)

  return {
    ticker,
    company: doc.company || "",
    price,
    change_pct: changePct,
    volume,
    avg_volume: avgVolume,
    rel_volume: relVolume,
    market_cap: marketCap,
    market_cap_bucket: marketCapBucket(marketCap),
    sector: doc.sector || "Unclassified",
    industry: doc.industry || "Unclassified",
    country: doc.country || "",
    exchange: normalizeExchange(doc.exchange),
    index: doc.index || "",
    avg_sentiment: avgSentiment,
    social_sentiment: Number(doc.social_sentiment ?? 0),
    structured_sentiment: Number(doc.structured_sentiment ?? doc.news_sentiment ?? avgSentiment),
    sentiment: avgSentiment,
    message_count: Number(doc.message_count ?? 0),
    news_article_count: Number(doc.news_article_count ?? 0),
    bullish_count: Number(doc.bullish_count ?? 0),
    bearish_count: Number(doc.bearish_count ?? 0),
    neutral_count: Number(doc.neutral_count ?? 0),
    sources: doc.sources || [],
    pe_ratio: nullableNumber(doc.pe_ratio ?? doc.pe),
    forward_pe: nullableNumber(doc.forward_pe),
    peg: nullableNumber(doc.peg),
    ps_ratio: nullableNumber(doc.ps_ratio),
    pb_ratio: nullableNumber(doc.pb_ratio),
    dividend_yield: nullableNumber(doc.dividend_yield),
    eps_growth_this_y: nullableNumber(doc.eps_growth_this_y),
    eps_growth_next_y: nullableNumber(doc.eps_growth_next_y),
    sales_growth: nullableNumber(doc.sales_growth),
    gross_margin: nullableNumber(doc.gross_margin),
    operating_margin: nullableNumber(doc.operating_margin),
    roe: nullableNumber(doc.roe),
    debt_equity: nullableNumber(doc.debt_equity),
    beta: nullableNumber(doc.beta),
    rsi: nullableNumber(doc.rsi),
    sma20: nullableNumber(doc.sma20),
    sma50: nullableNumber(doc.sma50),
    sma200: nullableNumber(doc.sma200),
    perf_week: nullableNumber(doc.perf_week),
    perf_month: nullableNumber(doc.perf_month),
    perf_quarter: nullableNumber(doc.perf_quarter),
    perf_half: nullableNumber(doc.perf_half),
    perf_year: nullableNumber(doc.perf_year),
    perf_ytd: nullableNumber(doc.perf_ytd),
    atr: nullableNumber(doc.atr),
    gap: nullableNumber(doc.gap),
    analyst: doc.analyst || null,
    target_price: nullableFixed(doc.target_price, 2),
    inst_own: nullableNumber(doc.inst_own),
    insider_own: nullableNumber(doc.insider_own),
    float_short: nullableNumber(doc.float_short),
    earnings_date: doc.earnings_date || null,
    previous_close: nullableFixed(doc.previous_close, 2),
    change: nullableFixed(doc.change, 2),
    quote_source: doc.quote_source || null,
    quote_time: doc.quote_time || null,
    quote_updated_at: doc.quote_updated_at || null,
    quote_status: doc.quote_status || (hasStoredPrice ? "priced" : "missing"),
  }
}

function isCleanListedUsScreenerRow(row) {
  return Boolean(
    row?.ticker &&
    !String(row.ticker).includes(".") &&
    !NON_STOCK_TICKERS.has(String(row.ticker).toUpperCase()) &&
    US_EXCHANGES.has(normalizeExchange(row.exchange)) &&
    row.price != null &&
    Number(row.price) > 0 &&
    row.change_pct != null &&
    Number.isFinite(Number(row.change_pct)) &&
    Math.abs(Number(row.change_pct)) <= MAX_SIGNAL_CHANGE_PCT &&
    row.quote_status !== "missing"
  )
}

function tickerStatsToScreenerRow(row, quoteRow = {}) {
  const score = sentimentScore(row)
  const quote = normalizeScreenerDoc({ ...quoteRow, ticker: quoteRow.ticker || row.ticker })
  const price = quote.price
  const volume = quote.quote_status === "priced" ? quote.volume : null
  return normalizeScreenerDoc({
    ...quote,
    ticker: row.ticker,
    company: quote.company || "",
    price,
    change_pct: quote.change_pct,
    volume,
    avg_volume: quote.quote_status === "priced" ? quote.avg_volume : null,
    market_cap: quote.market_cap,
    sector: quote.quote_status === "priced" ? quote.sector : "News matched",
    industry: quote.quote_status === "priced" ? quote.industry : "Ticker mentions",
    avg_sentiment: score,
    social_sentiment: quote.social_sentiment || 0,
    structured_sentiment: score,
    message_count: row.count || 0,
    news_article_count: row.count || 0,
    bullish_count: row.bullish || 0,
    bearish_count: row.bearish || 0,
    neutral_count: row.neutral || 0,
    sources: (row.sources || []).filter(Boolean).slice(0, 6),
    latest_publish: row.latest_publish,
    latest_fetch: row.latest_fetch,
  })
}

function tickerStatsToMomentumRow(row, quoteRow = {}) {
  const score = sentimentScore(row)
  const base = tickerStatsToScreenerRow(row, quoteRow)
  const volume = base.volume
  const articleCount = row.count || 0
  return {
    ...base,
    ticker: row.ticker,
    volume,
    avg_volume: base.avg_volume,
    rel_volume: base.rel_volume,
    sentiment: score,
    article_count: articleCount,
    message_count: articleCount,
    bullish_count: row.bullish || 0,
    bearish_count: row.bearish || 0,
    neutral_count: row.neutral || 0,
    sources: (row.sources || []).filter(Boolean).slice(0, 6),
    latest_publish: row.latest_publish,
    latest_fetch: row.latest_fetch,
    momentum_score: Number(Math.abs(base.change_pct || 0).toFixed(2)),
  }
}

function timeLabel(value) {
  const raw = Number(value || 0)
  const ms = raw > 1000000000000 ? raw : raw > 1000000000 ? raw * 1000 : Date.parse(value)
  if (!Number.isFinite(ms) || ms <= 0) return ""
  const diff = Math.max(0, Date.now() - ms)
  if (diff < 60_000) return "now"
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m`
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h`
  return new Date(ms).toLocaleDateString("en-US", { month: "short", day: "numeric" })
}

function normalizeTickerList(values = [], limit = TRACKED_TICKER_LIMIT, { ensurePrivate = true } = {}) {
  const max = Math.max(1, Number(limit || TRACKED_TICKER_LIMIT))
  const tickers = []
  const seen = new Set()

  const addTicker = (raw) => {
    const ticker = String(raw || "").trim().toUpperCase()
    if (!ticker || seen.has(ticker)) return
    if (!PRIVATE_TRACKED_TICKERS.has(ticker) && !/^[A-Z][A-Z0-9.-]{0,5}$/.test(ticker)) return
    tickers.push(ticker)
    seen.add(ticker)
  }

  for (const ticker of values) addTicker(ticker)
  if (ensurePrivate) {
    for (const ticker of PRIVATE_TRACKED_TICKERS) {
      if (!seen.has(ticker)) tickers.unshift(ticker)
    }
  }

  return tickers.slice(0, max)
}

function loadTrackedTickers(limit = TRACKED_TICKER_LIMIT) {
  const configured = process.env.TRACKED_TICKERS || ""
  if (configured.trim()) {
    return normalizeTickerList(configured.split(","), limit)
  }

  const configuredFile = process.env.TRACKED_TICKER_FILE || process.env.SOCIAL_TICKER_FILE || ""
  const candidates = configuredFile
    ? [path.isAbsolute(configuredFile) ? configuredFile : path.resolve(process.cwd(), configuredFile)]
    : TRACKED_TICKER_FILE_CANDIDATES

  for (const filePath of candidates) {
    try {
      const lines = fs.readFileSync(filePath, "utf8").split(/\r?\n/)
      const tickers = normalizeTickerList(lines, limit)
      if (tickers.length > 1) return tickers
    } catch {
      // Try the next known runtime layout.
    }
  }

  console.warn("Could not read tracked ticker file from known paths:", candidates.join(", "))
  return normalizeTickerList(["SPACEX"], limit)
}

async function loadArticleStats(db, days = 0) {
  const articles = db.collection("articles")
  const match = recentArticleMatch(days)
  const trackedTickers = loadTrackedTickers()
  const trackedMarketTickers = await loadTrackedMarketTickerSymbols(db, Number(process.env.TRACKED_MARKET_TICKER_LIMIT || 5000))

  const [sources, categories, sentimentRows, tickerRows, total, totalAll] = await Promise.all([
    articles.aggregate([
      ...articleMatchStage(match),
      { $group: { _id: "$source", count: { $sum: 1 } } },
      { $project: { _id: 0, source: "$_id", count: 1 } },
      { $sort: { count: -1 } }
    ]).toArray(),
    articles.aggregate([
      ...articleMatchStage(match),
      { $group: { _id: "$category", count: { $sum: 1 } } },
      { $project: { _id: 0, category: "$_id", count: 1 } },
      { $sort: { count: -1 } }
    ]).toArray(),
    articles.aggregate([
      ...articleMatchStage(match),
      {
        $group: {
          _id: { $toLower: { $ifNull: ["$sentiment", "neutral"] } },
          count: { $sum: 1 }
        }
      }
    ]).toArray(),
    articles.aggregate(tickerArticlePipeline({ days, limit: 500 })).toArray(),
    articles.countDocuments(match),
    articles.countDocuments({})
  ])

  const sentiment = { bullish: 0, bearish: 0, neutral: 0, unknown: 0 }
  for (const row of sentimentRows) {
    const key = sentiment[row._id] == null ? "unknown" : row._id
    sentiment[key] = (sentiment[key] || 0) + row.count
  }

  return {
    total,
    total_recent: total,
    total_all: totalAll,
    sources,
    categories,
    sentiment,
    ticker_mentions: tickerRows,
    tracked_market_count: TRACKED_MARKETS.length,
    tracked_markets: TRACKED_MARKETS,
    tracked_exchanges: Array.from(US_EXCHANGES),
    tracked_indices: TRACKED_MARKET_INDICES,
    market_universe_label: "NASDAQ / NYSE / AMEX equities plus major US index markets",
    tracked_ticker_count: trackedTickers.length,
    tracked_tickers: trackedTickers,
    tracked_market_ticker_count: trackedMarketTickers.length,
    tracked_market_tickers: trackedMarketTickers.slice(0, 500),
  }
}

async function loadScreenerQuoteMap(db, tickers = []) {
  const unique = Array.from(new Set(tickers.map(t => String(t || "").toUpperCase()).filter(Boolean)))
  if (!unique.length) return new Map()

  const docs = await db.collection("screeners").find({ ticker: { $in: unique } }).toArray()
  return new Map(docs.map(doc => [String(doc.ticker || "").toUpperCase(), normalizeScreenerDoc(doc)]))
}

async function loadAllScreenerRows(db) {
  const docs = await db.collection("screeners").find({}).toArray()
  return docs.map(normalizeScreenerDoc).filter(row => row.ticker)
}

async function loadPositiveFinvizMoverRows(db, limit = 100) {
  const requestedLimit = Math.max(1, Math.min(300, Number(limit || 100)))
  let docs = await db.collection("screeners").find({
    quote_source: "finviz_elite_screener",
    finviz_status: { $ne: "dropped" },
  }).toArray()
  if (!docs.length) docs = await db.collection("screeners").find({}).toArray()
  return docs
    .map(normalizeScreenerDoc)
    .filter(row => isCleanListedUsScreenerRow(row) && Number(row.change_pct || 0) >= 0.01)
    .sort((a, b) => {
      const changeDiff = Number(b.change_pct || 0) - Number(a.change_pct || 0)
      if (changeDiff !== 0) return changeDiff
      const relDiff = Number(b.rel_volume || 0) - Number(a.rel_volume || 0)
      if (relDiff !== 0) return relDiff
      return Number(b.volume || 0) - Number(a.volume || 0)
    })
    .slice(0, requestedLimit)
    .map((row, index) => ({
      ...row,
      finviz_rank: index + 1,
      discovery_source: "positive_price_change",
      positive_mover: true,
      sentiment: row.avg_sentiment || 0,
      article_count: row.news_article_count || 0,
      momentum_score: Number((row.change_pct || 0).toFixed(2)),
    }))
}

async function loadTrackedMarketTickerSymbols(db, limit = 5000) {
  const requestedLimit = Math.max(1, Math.min(10000, Number(limit || 5000)))
  const docs = await db.collection("screeners").find(
    {
      ticker: { $exists: true, $nin: ["", null], $not: /\./ },
      exchange: { $in: Array.from(US_EXCHANGES) },
      quote_status: { $ne: "missing" },
    },
    { projection: { ticker: 1, volume: 1, market_cap: 1, quote_source: 1 } }
  ).sort({ volume: -1, market_cap: -1 }).limit(requestedLimit).toArray()
  return normalizeTickerList(docs.map(row => row.ticker), requestedLimit, { ensurePrivate: false })
}

async function loadArticleStatsForTickers(db, tickers = [], days = 2) {
  const wanted = new Set(tickers.map(t => String(t || "").toUpperCase()).filter(Boolean))
  if (!wanted.size) return new Map()

  const rows = await db.collection("articles")
    .aggregate(tickerArticlePipeline({ days, limit: Math.max(wanted.size * 4, 150) }))
    .toArray()

  return new Map(
    rows
      .filter(row => wanted.has(String(row.ticker || "").toUpperCase()))
      .map(row => [String(row.ticker || "").toUpperCase(), row])
  )
}

async function loadSocialStatsForTickers(db, tickers = [], windowMinutes = 1440) {
  const wanted = normalizeTickerList(tickers, 300, { ensurePrivate: false })
  if (!wanted.length) return new Map()

  const sinceSec = Math.floor(Date.now() / 1000) - Math.max(1, Number(windowMinutes || 1440)) * 60
  const rows = await db.collection("socials").aggregate([
    ...socialTimeStages(),
    { $match: { _event_sec: { $gte: sinceSec } } },
    { $match: { _ticker_candidates: { $in: wanted } } },
    { $unwind: "$_ticker_candidates" },
    { $match: { _ticker_candidates: { $in: wanted } } },
    {
      $group: {
        _id: "$_ticker_candidates",
        count: { $sum: 1 },
        bullish: {
          $sum: {
            $cond: [
              { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bull|positive" } },
              1,
              0,
            ],
          },
        },
        bearish: {
          $sum: {
            $cond: [
              { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bear|negative" } },
              1,
              0,
            ],
          },
        },
        avg_sentiment_score: {
          $avg: {
            $switch: {
              branches: [
                {
                  case: { $in: [{ $type: "$sentiment_score" }, ["int", "long", "double", "decimal"] ] },
                  then: { $toDouble: "$sentiment_score" },
                },
                {
                  case: { $in: [{ $type: "$sentiment" }, ["int", "long", "double", "decimal"] ] },
                  then: { $toDouble: "$sentiment" },
                },
                {
                  case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bull|positive" } },
                  then: 1,
                },
                {
                  case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bear|negative" } },
                  then: -1,
                },
              ],
              default: 0,
            },
          },
        },
        platforms: { $addToSet: "$_norm_platform" },
        latest_post: { $max: "$_event_sec" },
      },
    },
  ]).toArray()

  return new Map(rows.map(row => [String(row._id || "").toUpperCase(), row]))
}

function bracketOrderResearchSignal(row, { sentiment, totalArticleCount, socialCount }) {
  const changePct = Number(row.change_pct || 0)
  const relVolume = Number(row.rel_volume || 0)
  const price = Number(row.price || 0)
  const supportCount = Number(totalArticleCount || 0) + Number(socialCount || 0)
  const capBucket = String(row.market_cap_bucket || "").toLowerCase()

  const moveScore = clamp(changePct / 20)
  const volumeScore = clamp(relVolume / 5)
  const sentimentScoreNorm = clamp((Number(sentiment || 0) + 1) / 2)
  const catalystScore = clamp(Math.log1p(supportCount) / Math.log1p(50))
  const confidence = clamp(
    moveScore * 0.35 +
    volumeScore * 0.25 +
    sentimentScoreNorm * 0.25 +
    catalystScore * 0.15
  )

  const volatile = capBucket === "micro" || capBucket === "small" || price < 5 || Math.abs(changePct) >= 25
  const stopLossPct = volatile ? 6 : 3
  const takeProfitPct = volatile ? 12 : 6
  const candidate = confidence >= 0.7 && changePct > 0 && sentiment > 0.05 && supportCount >= 2

  return {
    candidate,
    confidence: Number(confidence.toFixed(3)),
    direction: candidate ? "long_watch" : "monitor",
    entry_reference: Number.isFinite(price) && price > 0 ? Number(price.toFixed(4)) : null,
    stop_loss_pct: stopLossPct,
    take_profit_pct: takeProfitPct,
    support_count: supportCount,
    rationale: [
      changePct > 0 ? "positive price move" : "",
      relVolume >= 2 ? "elevated relative volume" : "",
      sentiment > 0.05 ? "positive weighted sentiment" : "",
      supportCount ? "matched news/social support" : "",
    ].filter(Boolean),
    status: "research_only_not_connected_to_broker",
  }
}

function mergeMoverContext(row, articleRow, socialRow) {
  const newsSentiment = articleRow ? sentimentScore(articleRow) : 0
  const structuredArticleCount = Number(articleRow?.structured_count || 0)
  const unstructuredArticleCount = Number(articleRow?.unstructured_count || 0)
  const totalArticleCount = Number(articleRow?.count || row.news_article_count || 0)
  const structuredSentiment = articleRow ? kindSentimentScore(articleRow, "structured") : Number(row.structured_sentiment || 0)
  const unstructuredSentiment = articleRow ? kindSentimentScore(articleRow, "unstructured") : 0
  const socialCount = Number(socialRow?.count || 0)
  const socialSentiment = socialCount
    ? Number((Number.isFinite(Number(socialRow.avg_sentiment_score))
      ? Number(socialRow.avg_sentiment_score)
      : ((socialRow.bullish || 0) - (socialRow.bearish || 0)) / Math.max(1, socialCount)).toFixed(3))
    : 0
  const structuredWeight = 2
  const unstructuredWeight = 1
  const socialWeight = 0.75
  const sentimentDenominator =
    structuredArticleCount * structuredWeight +
    unstructuredArticleCount * unstructuredWeight +
    socialCount * socialWeight
  const sentiment = sentimentDenominator
    ? Number(((
      structuredSentiment * structuredArticleCount * structuredWeight +
      unstructuredSentiment * unstructuredArticleCount * unstructuredWeight +
      socialSentiment * socialCount * socialWeight
    ) / sentimentDenominator).toFixed(3))
    : Number(row.avg_sentiment || 0)
  const bracketOrder = bracketOrderResearchSignal(row, { sentiment, totalArticleCount, socialCount })

  return {
    ...row,
    sentiment,
    article_sentiment: newsSentiment,
    social_sentiment: socialSentiment,
    structured_sentiment: structuredSentiment,
    unstructured_sentiment: unstructuredSentiment,
    article_count: totalArticleCount,
    structured_article_count: structuredArticleCount,
    unstructured_article_count: unstructuredArticleCount,
    news_article_count: totalArticleCount,
    message_count: socialCount,
    bullish_count: Number(articleRow?.bullish || 0) + Number(socialRow?.bullish || 0),
    bearish_count: Number(articleRow?.bearish || 0) + Number(socialRow?.bearish || 0),
    neutral_count: Number(articleRow?.neutral || 0),
    sources: [
      "Positive Movers",
      ...(articleRow?.sources || []),
      ...(socialRow?.platforms || []),
    ].filter(Boolean).slice(0, 8),
    latest_social: socialRow?.latest_post || null,
    momentum_score: Number((row.change_pct || 0).toFixed(2)),
    ai_numeric_rank: bracketOrder.confidence,
    bracket_order: bracketOrder,
  }
}

function tradeWatchDecision(row) {
  const changePct = Number(row.change_pct || 0)
  const relVolume = Number(row.rel_volume || 0)
  const price = Number(row.price || 0)
  const sentiment = Number(row.sentiment || 0)
  const articleSentiment = Number(row.article_sentiment || 0)
  const socialSentiment = Number(row.social_sentiment || 0)
  const articleCount = Number(row.article_count || 0)
  const structuredCount = Number(row.structured_article_count || 0)
  const publicCount = Number(row.unstructured_article_count || 0)
  const socialCount = Number(row.message_count || 0)
  const supportCount = articleCount + socialCount
  const latestQuoteSec = timestampSeconds(row.quote_updated_at || row.quote_time)
  const quoteAgeMinutes = latestQuoteSec ? Math.max(0, (Date.now() / 1000 - latestQuoteSec) / 60) : null
  const quoteFreshness = quoteAgeMinutes == null ? 0.45 : clamp(1 - quoteAgeMinutes / 360, 0.2, 1)
  const capBucket = String(row.market_cap_bucket || "").toLowerCase()
  const microOrPenny = capBucket === "micro" || (Number.isFinite(price) && price > 0 && price < 1)

  const priceScore = clamp(changePct / 25)
  const volumeScore = clamp(relVolume / 8)
  const structuredScore = clamp(Math.log1p(structuredCount) / Math.log1p(8))
  const publicNewsScore = clamp(Math.log1p(publicCount) / Math.log1p(12))
  const socialDensityScore = clamp(Math.log1p(socialCount) / Math.log1p(60))
  const sentimentMagnitude = clamp((sentiment + 1) / 2)
  const socialMagnitude = clamp((socialSentiment + 1) / 2)
  const articleMagnitude = clamp((articleSentiment + 1) / 2)
  const evidenceScore = clamp(
    structuredScore * 0.3 +
    publicNewsScore * 0.2 +
    socialDensityScore * 0.25 +
    sentimentMagnitude * 0.15 +
    socialMagnitude * 0.1
  )
  const agreement = clamp(
    (changePct > 0 ? 0.25 : 0) +
    (sentiment > 0.05 ? 0.25 : sentiment < -0.05 ? -0.15 : 0.05) +
    (socialCount > 0 && socialSentiment > 0.05 ? 0.2 : socialCount > 0 && socialSentiment < -0.05 ? -0.1 : 0) +
    (articleCount > 0 && articleSentiment > 0.05 ? 0.2 : articleCount > 0 && articleSentiment < -0.05 ? -0.1 : 0) +
    (relVolume >= 2 ? 0.1 : 0),
    0,
    1
  )
  const thinSpikePenalty = supportCount === 0 ? 0.18 : supportCount === 1 ? 0.08 : 0
  const microPenalty = microOrPenny && supportCount < 3 ? 0.08 : 0
  const rawScore =
    priceScore * 0.25 +
    volumeScore * 0.2 +
    evidenceScore * 0.25 +
    agreement * 0.2 +
    quoteFreshness * 0.1 -
    thinSpikePenalty -
    microPenalty
  const score = clamp(rawScore)

  let decision = "Monitor"
  if (score >= 0.74 && agreement >= 0.65 && supportCount >= 2) decision = "High Watch"
  else if (score >= 0.58 && supportCount >= 1) decision = "Watch"
  else if (changePct >= 15 && supportCount === 0) decision = "Unsupported Spike"
  else if (sentiment < -0.15 || socialSentiment < -0.2) decision = "Divergent"

  const reasons = [
    changePct > 0 ? `price +${changePct.toFixed(2)}%` : "",
    relVolume >= 2 ? `${relVolume.toFixed(1)}x rel vol` : "",
    structuredCount ? `${structuredCount} structured news` : "",
    publicCount ? `${publicCount} public news` : "",
    socialCount ? `${socialCount} social posts` : "",
    sentiment > 0.05 ? `weighted sent +${sentiment.toFixed(2)}` : sentiment < -0.05 ? `weighted sent ${sentiment.toFixed(2)}` : "",
  ].filter(Boolean).slice(0, 5)
  const risks = [
    supportCount === 0 ? "no matched news/social evidence yet" : "",
    quoteAgeMinutes != null && quoteAgeMinutes > 180 ? `quote ${Math.round(quoteAgeMinutes)}m old` : "",
    microOrPenny ? "microcap/penny volatility" : "",
    socialCount > 0 && socialSentiment < -0.05 ? "negative social tone" : "",
    articleCount > 0 && articleSentiment < -0.05 ? "negative news tone" : "",
  ].filter(Boolean).slice(0, 4)

  return {
    trade_watch_score: Number(score.toFixed(3)),
    decision,
    confidence: Number((score * 100).toFixed(1)),
    agreement: Number(agreement.toFixed(3)),
    evidence_score: Number(evidenceScore.toFixed(3)),
    quote_freshness: Number(quoteFreshness.toFixed(3)),
    quote_age_minutes: quoteAgeMinutes == null ? null : Number(quoteAgeMinutes.toFixed(1)),
    support_count: supportCount,
    reasons,
    risks,
  }
}

function addTradeWatchFields(row) {
  return {
    ...row,
    trade_watch: tradeWatchDecision(row),
  }
}

const PREDICTION_HORIZONS_MINUTES = [5, 15, 60]
const PREDICTION_MODEL_ID = "trade_watch_linear_v1"
const PREDICTION_FEATURE_KEYS = [
  "change_pct",
  "rel_volume",
  "article_count",
  "article_sentiment",
  "structured_sentiment",
  "social_count",
  "social_density_per_minute",
  "social_sentiment",
  "weighted_sentiment",
  "evidence_score",
  "trade_watch_score",
  "agreement",
]

function predictionFeaturesFromMover(row, socialWindowMinutes = 60) {
  const socialCount = Number(row.message_count || 0)
  const articleCount = Number(row.article_count || 0)
  const relVolume = Number(row.rel_volume || 0)
  const changePct = Number(row.change_pct || 0)
  const sentiment = Number(row.sentiment || 0)
  const evidenceScore =
    Number(row.article_sentiment || 0) * Math.min(1, articleCount / 5) +
    Number(row.social_sentiment || 0) * Math.min(1, socialCount / 20)

  return {
    price: Number(row.price || 0),
    change_pct: changePct,
    volume: Number(row.volume || 0),
    rel_volume: Number(relVolume.toFixed(3)),
    market_cap: Number(row.market_cap || 0),
    market_cap_bucket: row.market_cap_bucket || "Unknown",
    rsi: row.rsi ?? null,
    gap: row.gap ?? null,
    perf_week: row.perf_week ?? null,
    perf_month: row.perf_month ?? null,
    article_count: articleCount,
    structured_article_count: Number(row.structured_article_count || 0),
    unstructured_article_count: Number(row.unstructured_article_count || 0),
    article_sentiment: Number(Number(row.article_sentiment || 0).toFixed(3)),
    structured_sentiment: Number(Number(row.structured_sentiment || 0).toFixed(3)),
    unstructured_sentiment: Number(Number(row.unstructured_sentiment || 0).toFixed(3)),
    social_count: socialCount,
    social_density_per_minute: Number((socialCount / Math.max(1, socialWindowMinutes)).toFixed(3)),
    social_sentiment: Number(Number(row.social_sentiment || 0).toFixed(3)),
    weighted_sentiment: Number(sentiment.toFixed(3)),
    evidence_score: Number(evidenceScore.toFixed(3)),
    trade_watch_score: Number(row.trade_watch?.trade_watch_score || 0),
    agreement: Number(row.trade_watch?.agreement || 0),
  }
}

function baselinePredictionFromMover(row) {
  const features = predictionFeaturesFromMover(row)
  const tradeScore = Number(row.trade_watch?.trade_watch_score || 0)
  const evidence = Number(features.evidence_score || 0)
  const changePct = Number(row.change_pct || 0)
  const relVolume = Number(row.rel_volume || 0)
  const direction = evidence >= 0.12 && changePct > 0
    ? "up"
    : evidence <= -0.12 && changePct < 0
      ? "down"
      : "watch"
  const confidence = clamp(
    tradeScore * 0.45 +
    Math.min(1, Math.abs(evidence)) * 0.25 +
    Math.min(1, relVolume / 6) * 0.15 +
    Math.min(1, Math.abs(changePct) / 25) * 0.15
  )
  return {
    direction,
    confidence: Number(confidence.toFixed(3)),
    model: "baseline_trade_watch_v1",
    model_ready: Boolean(row.price && (row.article_count || row.message_count) && Number.isFinite(changePct)),
  }
}

function sigmoid(value) {
  return 1 / (1 + Math.exp(-value))
}

function applyPredictionModel(features = {}, model = null) {
  if (!model?.weights || !model?.feature_stats) return null
  let score = Number(model.intercept || 0)
  for (const key of model.feature_keys || PREDICTION_FEATURE_KEYS) {
    const stat = model.feature_stats[key] || { mean: 0, std: 1 }
    const std = Number(stat.std || 1) || 1
    const raw = Number(features[key])
    const normalized = Number.isFinite(raw) ? (raw - Number(stat.mean || 0)) / std : 0
    score += Number(model.weights[key] || 0) * normalized
  }
  const predictedReturn = Number(score.toFixed(3))
  const probabilityUp = Number(sigmoid(score / Math.max(0.25, Number(model.target_std || 1))).toFixed(3))
  return {
    model: model.model_id || PREDICTION_MODEL_ID,
    model_version: model.version || 1,
    direction: predictedReturn > 0.05 ? "up" : predictedReturn < -0.05 ? "down" : "watch",
    predicted_return_5m: predictedReturn,
    probability_up: probabilityUp,
    confidence: Number(Math.abs(probabilityUp - 0.5).toFixed(3)),
    trained_samples: Number(model.samples || 0),
  }
}

async function loadLatestPredictionModel(db) {
  return db.collection("prediction_models").findOne({ _id: PREDICTION_MODEL_ID })
}

async function loadEnrichedTradeWatchRows(db, { limit = 10, days = 2, socialWindow = 60 } = {}) {
  const requestedLimit = Math.max(1, Math.min(50, Number(limit || 10)))
  const movers = await loadPositiveFinvizMoverRows(db, Math.max(requestedLimit * 6, 100))
  const articleMap = await loadArticleStatsForTickers(db, movers.map(row => row.ticker), days)
  const socialMap = await loadSocialStatsForTickers(db, movers.map(row => row.ticker), socialWindow)
  return movers
    .map(row => addTradeWatchFields(mergeMoverContext(row, articleMap.get(row.ticker), socialMap.get(row.ticker))))
    .sort((a, b) => {
      const scoreDiff = Number(b.trade_watch?.trade_watch_score || 0) - Number(a.trade_watch?.trade_watch_score || 0)
      if (scoreDiff !== 0) return scoreDiff
      const evidenceDiff = Number(b.trade_watch?.evidence_score || 0) - Number(a.trade_watch?.evidence_score || 0)
      if (evidenceDiff !== 0) return evidenceDiff
      return Number(b.change_pct || 0) - Number(a.change_pct || 0)
    })
    .slice(0, requestedLimit)
}

async function captureTradeWatchPredictionSignals(db, { limit = 10, days = 2, socialWindow = 60 } = {}) {
  const nowSec = Math.floor(Date.now() / 1000)
  const minuteBucket = Math.floor(nowSec / 60) * 60
  const [rows, model] = await Promise.all([
    loadEnrichedTradeWatchRows(db, { limit, days, socialWindow }),
    loadLatestPredictionModel(db),
  ])
  const docs = rows
    .filter(row => Number(row.price || 0) > 0)
    .map((row, index) => {
      const signalId = `${row.ticker}:${minuteBucket}`
      const features = predictionFeaturesFromMover(row, socialWindow)
      const baseline = baselinePredictionFromMover(row)
      const modelSignal = applyPredictionModel(features, model)
      return {
        _id: signalId,
        signal_id: signalId,
        ticker: row.ticker,
        company: row.company || "",
        exchange: row.exchange || "",
        sector: row.sector || "",
        source: "trade_watch",
        signal_sec: minuteBucket,
        signal_at: new Date(minuteBucket * 1000),
        entry_price: Number(row.price || 0),
        entry_quote_source: row.quote_source || null,
        entry_quote_updated_at: row.quote_updated_at || null,
        rank: index + 1,
        decision: row.trade_watch?.decision || "Monitor",
        trade_watch: row.trade_watch || {},
        features,
        baseline_signal: baseline,
        model_signal: modelSignal,
        labels: {},
        label_status: "pending",
        horizons_minutes: PREDICTION_HORIZONS_MINUTES,
        created_at: new Date(),
        updated_at: new Date(),
      }
    })

  if (!docs.length) return { saved: 0, rows: [] }
  const result = await db.collection("prediction_signals").bulkWrite(
    docs.map(doc => ({
      updateOne: {
        filter: { _id: doc._id },
        update: {
          $setOnInsert: doc,
          $set: { last_seen_at: new Date(), last_rank: doc.rank },
        },
        upsert: true,
      },
    })),
    { ordered: false }
  )
  return { saved: Number(result.upsertedCount || 0), rows: docs }
}

async function labelMaturePredictionSignals(db, { limit = 500 } = {}) {
  const nowSec = Math.floor(Date.now() / 1000)
  const oldestDueSec = nowSec - Math.min(...PREDICTION_HORIZONS_MINUTES) * 60
  const docs = await db.collection("prediction_signals").find({
    signal_sec: { $lte: oldestDueSec },
    entry_price: { $gt: 0 },
  }).sort({ signal_sec: -1 }).limit(Math.max(1, Math.min(2000, Number(limit || 500)))).toArray()
  if (!docs.length) return { checked: 0, labeled: 0 }

  const tickers = Array.from(new Set(docs.map(doc => String(doc.ticker || "").toUpperCase()).filter(Boolean)))
  const quoteMap = await loadScreenerQuoteMap(db, tickers)
  const updates = []
  let labeled = 0

  for (const doc of docs) {
    const quote = quoteMap.get(String(doc.ticker || "").toUpperCase())
    const currentPrice = Number(quote?.price || 0)
    const entryPrice = Number(doc.entry_price || 0)
    if (!currentPrice || !entryPrice) continue

    const setFields = { updated_at: new Date() }
    for (const horizon of PREDICTION_HORIZONS_MINUTES) {
      const key = `return_${horizon}m`
      if (doc.labels?.[key]?.labeled) continue
      const due = nowSec - Number(doc.signal_sec || 0) >= horizon * 60
      if (!due) continue

      const returnPct = ((currentPrice - entryPrice) / entryPrice) * 100
      const direction = doc.baseline_signal?.direction || "watch"
      setFields[`labels.${key}`] = {
        labeled: true,
        horizon_minutes: horizon,
        return_pct: Number(returnPct.toFixed(3)),
        entry_price: Number(entryPrice.toFixed(4)),
        label_price: Number(currentPrice.toFixed(4)),
        labeled_at: new Date(),
        label_sec: nowSec,
        label_delay_seconds: nowSec - Number(doc.signal_sec || 0) - horizon * 60,
        quote_source: quote.quote_source || null,
        direction_correct: direction === "up" ? returnPct > 0 : direction === "down" ? returnPct < 0 : null,
      }
      labeled += 1
    }

    if (Object.keys(setFields).length > 1) {
      setFields.label_status = "partially_labeled"
      if (PREDICTION_HORIZONS_MINUTES.every(h => setFields[`labels.return_${h}m`] || doc.labels?.[`return_${h}m`]?.labeled)) {
        setFields.label_status = "complete"
      }
      updates.push({ updateOne: { filter: { _id: doc._id }, update: { $set: setFields } } })
    }
  }

  if (updates.length) await db.collection("prediction_signals").bulkWrite(updates, { ordered: false })
  return { checked: docs.length, labeled }
}

async function trainPredictionModel(db, { limit = 2000, minSamples = 20 } = {}) {
  const docs = await db.collection("prediction_signals").find({
    "labels.return_5m.labeled": true,
    "labels.return_5m.return_pct": { $type: "number" },
    features: { $exists: true },
  }).sort({ signal_sec: -1 }).limit(Math.max(50, Math.min(10000, Number(limit || 2000)))).toArray()

  const samples = docs
    .map(doc => ({
      target: Number(doc.labels?.return_5m?.return_pct),
      features: doc.features || {},
      baseline_direction: doc.baseline_signal?.direction || "watch",
    }))
    .filter(row => Number.isFinite(row.target))

  if (samples.length < minSamples) {
    const model = {
      _id: PREDICTION_MODEL_ID,
      model_id: PREDICTION_MODEL_ID,
      status: "insufficient_samples",
      samples: samples.length,
      min_samples: minSamples,
      feature_keys: PREDICTION_FEATURE_KEYS,
      updated_at: new Date(),
      note: "Collect more labeled prediction_signals before training the statistical model.",
    }
    await db.collection("prediction_models").updateOne({ _id: PREDICTION_MODEL_ID }, { $set: model }, { upsert: true })
    return model
  }

  const targetMean = samples.reduce((sum, row) => sum + row.target, 0) / samples.length
  const targetVar = samples.reduce((sum, row) => sum + (row.target - targetMean) ** 2, 0) / Math.max(1, samples.length - 1)
  const targetStd = Math.sqrt(targetVar) || 1
  const featureStats = {}
  const weights = {}

  for (const key of PREDICTION_FEATURE_KEYS) {
    const vals = samples.map(row => Number(row.features?.[key])).map(value => Number.isFinite(value) ? value : 0)
    const mean = vals.reduce((sum, value) => sum + value, 0) / vals.length
    const variance = vals.reduce((sum, value) => sum + (value - mean) ** 2, 0) / Math.max(1, vals.length - 1)
    const std = Math.sqrt(variance) || 1
    const covariance = vals.reduce((sum, value, index) => sum + ((value - mean) / std) * (samples[index].target - targetMean), 0) / Math.max(1, vals.length - 1)
    const weight = clamp(covariance / targetStd, -0.8, 0.8)
    featureStats[key] = { mean: Number(mean.toFixed(5)), std: Number(std.toFixed(5)) }
    weights[key] = Number((weight * 0.35).toFixed(5))
  }

  const predictions = samples.map(row => {
    const signal = applyPredictionModel(row.features, {
      model_id: PREDICTION_MODEL_ID,
      version: Date.now(),
      intercept: targetMean,
      target_std: targetStd,
      weights,
      feature_stats: featureStats,
      feature_keys: PREDICTION_FEATURE_KEYS,
      samples: samples.length,
    })
    return {
      target: row.target,
      predicted: signal?.predicted_return_5m || 0,
      correct: signal?.direction === "up" ? row.target > 0 : signal?.direction === "down" ? row.target < 0 : null,
    }
  })
  const actionable = predictions.filter(row => row.correct != null)
  const mae = predictions.reduce((sum, row) => sum + Math.abs(row.predicted - row.target), 0) / predictions.length
  const directionalAccuracy = actionable.length
    ? actionable.reduce((sum, row) => sum + (row.correct ? 1 : 0), 0) / actionable.length
    : null

  const model = {
    _id: PREDICTION_MODEL_ID,
    model_id: PREDICTION_MODEL_ID,
    status: "trained",
    version: Date.now(),
    samples: samples.length,
    feature_keys: PREDICTION_FEATURE_KEYS,
    feature_stats: featureStats,
    weights,
    intercept: Number(targetMean.toFixed(5)),
    target_std: Number(targetStd.toFixed(5)),
    metrics: {
      mae_5m: Number(mae.toFixed(3)),
      directional_accuracy_5m: directionalAccuracy == null ? null : Number(directionalAccuracy.toFixed(3)),
      actionable_samples: actionable.length,
      avg_target_return_5m: Number(targetMean.toFixed(3)),
    },
    updated_at: new Date(),
  }
  await db.collection("prediction_models").updateOne({ _id: PREDICTION_MODEL_ID }, { $set: model }, { upsert: true })
  return model
}

async function loadTopMomentumTickerSymbols(db, limit = 10) {
  const requestedLimit = Math.max(1, Math.min(50, Number(limit || 10)))
  const movers = await loadPositiveFinvizMoverRows(db, requestedLimit)
  return normalizeTickerList(movers.map(row => row.ticker), requestedLimit, { ensurePrivate: false })
}

function withPrivateSocialTickers(tickers = []) {
  return normalizeTickerList([...tickers, ...Array.from(PRIVATE_TRACKED_TICKERS)], Math.max(tickers.length + PRIVATE_TRACKED_TICKERS.size, 1), { ensurePrivate: false })
}

// ── Routes ────────────────────────────────────────────────
app.post("/api/translate", async (req, res) => {
  try {
    const text = String(req.body.text || "").trim().slice(0, 1200)
    const targetLanguage = String(req.body.target_language || req.body.target || "en").toLowerCase()

    if (!text) return res.status(400).json({ ok: false, error: "text is required" })
    if (!SUPPORTED_TRANSLATION_LANGUAGES.has(targetLanguage)) {
      return res.status(400).json({ ok: false, error: "unsupported target language" })
    }

    try {
      const providerTranslation = await translateWithProvider(text, targetLanguage)
      if (providerTranslation) {
        return res.json({
          ok: true,
          translated_text: providerTranslation,
          target_language: targetLanguage,
          provider: "external",
        })
      }
    } catch (err) {
      console.warn("Translation provider failed, using glossary fallback:", err.message)
    }

    return res.json({
      ok: true,
      translated_text: glossaryTranslate(text, targetLanguage),
      target_language: targetLanguage,
      provider: UNSUPPORTED_TRANSLATION_SCRIPT_RE.test(text) ? "glossary_cjk_fallback" : "glossary",
    })
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.use('/api/articles',    articlesRouter)
app.use('/api/screener',    screenerRouter)

app.get("/api/momentum/trending", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, tickers: [], error: "MongoDB is not connected" })

    const days = Number(req.query.days || 2)
    const limit = Number(req.query.limit || 30)
    const movers = await loadPositiveFinvizMoverRows(db, Math.max(1, Math.min(100, limit)))
    const articleMap = await loadArticleStatsForTickers(db, movers.map(row => row.ticker), days)
    const socialWindow = Math.max(1, Math.min(4320, Number(req.query.window_minutes || 1440)))
    const socialMap = await loadSocialStatsForTickers(db, movers.map(row => row.ticker), socialWindow)
    const tickers = movers.map(row => mergeMoverContext(
      row,
      articleMap.get(row.ticker),
      socialMap.get(row.ticker)
    ))

    res.json({ ok: true, tickers, days, order: "positive_price_change", source: "Momentum movers", social_window_minutes: socialWindow })
  } catch (err) {
    console.error("GET /api/momentum/trending failed:", err)
    res.status(500).json({ ok: false, tickers: [], error: String(err.message || err) })
  }
})

app.get("/api/social/targets", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, tickers: [], error: "MongoDB is not connected" })
    const limit = Math.max(1, Math.min(50, Number(req.query.limit || 10)))
    const tickers = await loadTopMomentumTickerSymbols(db, limit)
    const rows = await loadPositiveFinvizMoverRows(db, limit)
    res.json({
      ok: true,
      tickers,
      rows: rows.slice(0, limit).map(row => ({
        ticker: row.ticker,
        change_pct: row.change_pct,
        price: row.price,
        volume: row.volume,
        exchange: row.exchange,
        quote_source: row.quote_source,
        finviz_rank: row.finviz_rank,
      })),
      source: "Finviz Elite top positive momentum movers, falling back to stored U.S. screener rows only if Finviz has no rows",
      social_refresh_seconds: 60,
    })
  } catch (err) {
    console.error("GET /api/social/targets failed:", err)
    res.status(500).json({ ok: false, tickers: [], error: String(err.message || err) })
  }
})

app.get("/api/trade-watch", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, tickers: [], error: "MongoDB is not connected" })

    const days = Math.max(0, Math.min(7, Number(req.query.days || 2)))
    const limit = Math.max(1, Math.min(50, Number(req.query.limit || 10)))
    const socialWindow = Math.max(1, Math.min(4320, Number(req.query.window_minutes || 1440)))
    const minScore = Math.max(0, Math.min(1, Number(req.query.min_score || 0)))
    const [rawTradeRows, model] = await Promise.all([
      loadEnrichedTradeWatchRows(db, { limit: Math.max(limit, 10), days, socialWindow }),
      loadLatestPredictionModel(db),
    ])
    const tickers = rawTradeRows
      .map(row => ({
        ...row,
        prediction_signal: applyPredictionModel(predictionFeaturesFromMover(row, socialWindow), model),
      }))
      .filter(row => row.trade_watch.trade_watch_score >= minScore)
      .slice(0, limit)

    res.json({
      ok: true,
      count: tickers.length,
      tickers,
      days,
      social_window_minutes: socialWindow,
      source: "Finviz momentum movers ranked by quote action, relative volume, structured/public news, and social evidence",
      methodology: {
        price_action: "positive Finviz Elite mover list, limited to clean NASDAQ/NYSE/AMEX rows",
        evidence: "structured news is weighted higher than public news; social counts and rolling sentiment are support signals",
        caution: "research-only scoring; broker execution is not connected",
      },
      model: model ? {
        status: model.status,
        samples: model.samples || 0,
        updated_at: model.updated_at || null,
        metrics: model.metrics || null,
      } : null,
    })
  } catch (err) {
    console.error("GET /api/trade-watch failed:", err)
    res.status(500).json({ ok: false, tickers: [], error: String(err.message || err) })
  }
})

app.get("/api/momentum", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, tickers: [], error: "MongoDB is not connected" })

    const days = Number(req.query.days || 2)
    const limit = Math.max(1, Math.min(100, Number(req.query.limit || 30)))
    const minNews = Math.max(0, Number(req.query.min_volume || req.query.min_news || 0))
    const minRelVolume = Math.max(0, Number(req.query.min_rel_vol || 0))
    const maxPrice = req.query.max_price ? Number(req.query.max_price) : null
    const sentiment = String(req.query.sentiment || "").toLowerCase()
    const order = String(req.query.order || "absolute_momentum").toLowerCase()

    const movers = await loadPositiveFinvizMoverRows(db, Math.max(limit * 4, 100))
    const articleMap = await loadArticleStatsForTickers(db, movers.map(row => row.ticker), days)
    const socialWindow = Math.max(1, Math.min(4320, Number(req.query.window_minutes || 1440)))
    const socialMap = await loadSocialStatsForTickers(db, movers.map(row => row.ticker), socialWindow)
    let tickers = movers.map(row => mergeMoverContext(
      row,
      articleMap.get(row.ticker),
      socialMap.get(row.ticker)
    ))

    if (minNews > 0) tickers = tickers.filter(row => (row.article_count || row.message_count || 0) >= minNews)
    if (minRelVolume > 0) tickers = tickers.filter(row => (row.rel_volume || 0) >= minRelVolume)
    if (maxPrice != null && Number.isFinite(maxPrice)) {
      tickers = tickers.filter(row => row.price == null || row.price <= maxPrice)
    }
    if (sentiment === "bullish") tickers = tickers.filter(row => (row.sentiment || 0) > 0)
    if (sentiment === "bearish") tickers = tickers.filter(row => (row.sentiment || 0) < 0)

    tickers.sort((a, b) => {
      if (order === "news") {
        const scoreA = (a.article_count || a.message_count || 0) * (1 + Math.abs(a.sentiment || 0))
        const scoreB = (b.article_count || b.message_count || 0) * (1 + Math.abs(b.sentiment || 0))
        return scoreB - scoreA
      }
      const scoreA = Number(a.change_pct || 0)
      const scoreB = Number(b.change_pct || 0)
      if (scoreB !== scoreA) return scoreB - scoreA
      const relA = Number(a.rel_volume || 0)
      const relB = Number(b.rel_volume || 0)
      if (relB !== relA) return relB - relA
      return (b.volume || 0) - (a.volume || 0)
    })

    res.json({ ok: true, tickers: tickers.slice(0, limit), days, order, source: "Momentum movers", social_window_minutes: socialWindow })
  } catch (err) {
    console.error("GET /api/momentum failed:", err)
    res.status(500).json({ ok: false, tickers: [], error: String(err.message || err) })
  }
})

app.get("/api/momentum/:ticker/details", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, headlines: [], posts: [], error: "MongoDB is not connected" })

    const ticker = String(req.params.ticker || "").toUpperCase().replace(/[^A-Z0-9.-]/g, "")
    const match = {
      ...recentArticleMatch(Number(req.query.days || 2)),
      ticker: { $regex: `(^|,\\s*)${escapeRegExp(ticker)}(\\s*,|$)`, $options: "i" },
    }

    const articles = await db.collection("articles").find(
      match,
      { projection: { title: 1, source: 1, sentiment: 1, publish_date: 1, fetched_date: 1, url: 1, category: 1 } }
    ).sort({ publish_date: -1, fetched_date: -1 }).limit(12).toArray()

    const headlines = articles.map(article => ({
      title: article.title || "Untitled headline",
      source: article.source || "News",
      sentiment: article.sentiment || "neutral",
      time: timeLabel(article.publish_date || article.fetched_date),
      catalyst: article.category || undefined,
      url: article.url,
    }))

    const socialRows = await db.collection("socials").aggregate([
      ...socialTimeStages(),
      { $match: { _ticker_candidates: ticker } },
      { $sort: { _event_sec: -1 } },
      { $limit: 12 },
      {
        $project: {
          _id: 0,
          platform: "$_norm_platform",
          author: 1,
          content: { $ifNull: ["$text", { $ifNull: ["$content", "$title"] }] },
          sentiment: 1,
          sentiment_score: 1,
          url: 1,
          fetched_at: "$_event_sec",
        },
      },
    ]).toArray()

    const posts = socialRows.map(post => ({
      platform: post.platform || "Social",
      author: post.author || "",
      content: post.content || "",
      sentiment: typeof post.sentiment_score === "number"
        ? post.sentiment_score
        : /bull|positive/i.test(String(post.sentiment || "")) ? 1
        : /bear|negative/i.test(String(post.sentiment || "")) ? -1
        : 0,
      url: post.url,
      time: timeLabel(post.fetched_at),
    }))

    res.json({ ok: true, ticker, headlines, posts })
  } catch (err) {
    console.error("GET /api/momentum/:ticker/details failed:", err)
    res.status(500).json({ ok: false, headlines: [], posts: [], error: String(err.message || err) })
  }
})

app.get("/api/prices/:ticker", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, error: "MongoDB is not connected" })

    const ticker = String(req.params.ticker || "").toUpperCase().replace(/[^A-Z0-9.-]/g, "")
    const doc = await db.collection("screeners").findOne({ ticker })
    const row = normalizeScreenerDoc(doc || { ticker })
    res.json({
      ok: true,
      ticker,
      price: row.price,
      change_pct: row.change_pct,
      volume: row.volume,
      rel_volume: row.rel_volume,
      previous_close: row.previous_close,
      quote_source: row.quote_source,
      quote_time: row.quote_time,
      quote_status: row.quote_status,
      updated_at: doc?.quote_updated_at || doc?.updated_at || doc?.updatedAt || null,
    })
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

// SOCIAL_ROLLING_API_V2_START
// Rolling social feed using existing Mongoose connection.
// Supports numeric Unix-second timestamps, JS Date timestamps, and fallback fields.
function socialTimeStages() {
  return [
    {
      $addFields: {
        _time_raw: {
          $ifNull: [
            "$fetched_at",
            { $ifNull: [
              "$detected_at",
              { $ifNull: [
                "$timestamp",
                { $ifNull: ["$created_at", "$publish_date"] }
              ] }
            ] }
          ]
        }
      }
    },
    {
      $addFields: {
        _event_sec: {
          $switch: {
            branches: [
              {
                case: { $eq: [{ $type: "$_time_raw" }, "date"] },
                then: { $floor: { $divide: [{ $toLong: "$_time_raw" }, 1000] } }
              },
              {
                case: { $in: [{ $type: "$_time_raw" }, ["int", "long", "double", "decimal"] ] },
                then: { $toLong: "$_time_raw" }
              },
              {
                case: { $eq: [{ $type: "$_time_raw" }, "string"] },
                then: {
                  $floor: {
                    $divide: [
                      { $toLong: { $dateFromString: { dateString: "$_time_raw", onError: new Date(0) } } },
                      1000
                    ]
                  }
                }
              }
            ],
            default: 0
          }
        }
      }
    },
    {
      $addFields: {
        _norm_platform: {
          $switch: {
            branches: [
              {
                case: {
                  $regexMatch: {
                    input: { $toLower: { $ifNull: ["$platform", ""] } },
                    regex: "stocktwits"
                  }
                },
                then: "StockTwits"
              },
              {
                case: {
                  $regexMatch: {
                    input: { $toLower: { $ifNull: ["$platform", ""] } },
                    regex: "bluesky|bsky"
                  }
                },
                then: "Bluesky"
              },
              {
                case: {
                  $or: [
                    {
                      $regexMatch: {
                        input: { $toLower: { $ifNull: ["$platform", ""] } },
                        regex: "reddit"
                      }
                    },
                    {
                      $regexMatch: {
                        input: { $toLower: { $ifNull: ["$collector", ""] } },
                        regex: "reddit"
                      }
                    }
                  ]
                },
                then: "Reddit"
              },
              {
                case: {
                  $or: [
                    {
                      $regexMatch: {
                        input: { $toLower: { $ifNull: ["$platform", ""] } },
                        regex: "twitter|x"
                      }
                    },
                    {
                      $regexMatch: {
                        input: { $toLower: { $ifNull: ["$collector", ""] } },
                        regex: "twitter|x_"
                      }
                    }
                  ]
                },
                then: "Twitter"
              }
            ],
            default: { $ifNull: ["$platform", "Unknown"] }
          }
        }
      }
    },
    ...socialTickerCandidateStages(),
  ]
}

function socialTickerCandidateStages() {
  const stringSplit = (field) => ({
    $cond: [
      { $eq: [{ $type: field }, "string"] },
      { $split: [field, ","] },
      [],
    ],
  })
  const arrayOrStringSplit = (field) => ({
    $cond: [
      { $isArray: field },
      field,
      stringSplit(field),
    ],
  })

  return [
    {
      $addFields: {
        _ticker_primary_values_raw: {
          $concatArrays: [
            stringSplit("$ticker"),
            stringSplit("$symbol"),
            stringSplit("$cashtag"),
            arrayOrStringSplit("$tickers_mentioned"),
          ],
        },
        _ticker_text_cashtags: {
          $map: {
            input: {
              $regexFindAll: {
                input: {
                  $concat: [
                    { $toString: { $ifNull: ["$text", ""] } },
                    " ",
                    { $toString: { $ifNull: ["$content", ""] } },
                    " ",
                    { $toString: { $ifNull: ["$title", ""] } },
                  ],
                },
                regex: /\$[A-Za-z][A-Za-z0-9.-]{0,5}\b/,
              },
            },
            as: "tag",
            in: "$$tag.match",
          },
        },
      },
    },
    {
      $addFields: {
        _ticker_values_raw: {
          $cond: [
            {
              $gt: [
                {
                  $size: {
                    $filter: {
                      input: { $ifNull: ["$_ticker_primary_values_raw", []] },
                      as: "raw",
                      cond: { $ne: [{ $trim: { input: { $toString: "$$raw" } } }, ""] },
                    },
                  },
                },
                0,
              ],
            },
            "$_ticker_primary_values_raw",
            "$_ticker_text_cashtags",
          ],
        },
      },
    },
    {
      $addFields: {
        _ticker_candidates: {
          $filter: {
            input: {
              $map: {
                input: "$_ticker_values_raw",
                as: "raw",
                in: {
                  $trim: {
                    input: {
                      $replaceAll: {
                        input: { $toUpper: { $toString: "$$raw" } },
                        find: { $literal: "$" },
                        replacement: "",
                      },
                    },
                    chars: " ,;#",
                  },
                },
              },
            },
            as: "candidate",
            cond: {
              $regexMatch: {
                input: "$$candidate",
                regex: "^[A-Z][A-Z0-9.-]{0,5}$",
              },
            },
          },
        },
      },
    }
  ]
}

function marketSessionForSec(sec) {
  const date = new Date(Number(sec || 0) * 1000)
  if (!Number.isFinite(date.getTime())) return "unknown"
  const ny = new Date(date.toLocaleString("en-US", { timeZone: "America/New_York" }))
  const day = ny.getDay()
  const minutes = ny.getHours() * 60 + ny.getMinutes()
  if (day < 1 || day > 5) return "closed"
  if (minutes >= 4 * 60 && minutes < 9 * 60 + 30) return "pre"
  if (minutes >= 9 * 60 + 30 && minutes < 16 * 60) return "regular"
  if (minutes >= 16 * 60 && minutes < 20 * 60) return "after"
  return "closed"
}

function addSessionScaledSocialFields(rows, bucketMinutes) {
  const maxima = new Map()
  for (const row of rows) {
    const session = row.session || "unknown"
    const current = maxima.get(session) || { count: 0, density: 0 }
    current.count = Math.max(current.count, Number(row.message_count || 0))
    current.density = Math.max(current.density, Number(row.message_density || 0))
    maxima.set(session, current)
  }

  return rows.map(row => {
    const max = maxima.get(row.session || "unknown") || { count: 0, density: 0 }
    const count = Number(row.message_count || 0)
    const density = Number(row.message_density || 0)
    const sentiment = Number(row.sentiment || 0)
    return {
      ...row,
      bucket_minutes: bucketMinutes,
      message_count_scaled: max.count ? Number((count / max.count).toFixed(3)) : 0,
      message_density_scaled: max.density ? Number((density / max.density).toFixed(3)) : 0,
      sentiment_scaled: Number(((sentiment + 1) / 2).toFixed(3)),
    }
  })
}

app.get("/api/finviz/movers", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, tickers: [], error: "MongoDB is not connected" })

    const limit = Math.max(1, Math.min(100, Number(req.query.limit || 30)))
    const movers = await loadPositiveFinvizMoverRows(db, limit)
    const articleMap = await loadArticleStatsForTickers(db, movers.map(row => row.ticker), Number(req.query.days || 2))
    const socialMap = await loadSocialStatsForTickers(db, movers.map(row => row.ticker), Number(req.query.window_minutes || 1440))
    const tickers = movers.map(row => mergeMoverContext(row, articleMap.get(row.ticker), socialMap.get(row.ticker)))

    res.json({ ok: true, source: "Momentum movers", tickers, count: tickers.length })
  } catch (err) {
    res.status(500).json({ ok: false, tickers: [], error: String(err.message || err) })
  }
})

app.get("/api/social/rolling", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) {
      return res.status(503).json({ ok: false, error: "MongoDB is not connected", rows: [] })
    }

    const windowMinutes = Math.max(1, Math.min(1440, Number(req.query.window_minutes || 5)))
    const limit = Math.max(1, Math.min(1000, Number(req.query.limit || 500)))
    const platform = String(req.query.platform || "all").toLowerCase()
    const ticker = normalizeTickerList([req.query.ticker || req.query.symbol], 1, { ensurePrivate: false })[0] || ""
    const ranked = ["1", "true", "yes"].includes(String(req.query.ranked || "").toLowerCase())
    const sinceSec = Math.floor(Date.now() / 1000) - windowMinutes * 60

    const pipeline = [
      ...socialTimeStages(),
      { $match: { _event_sec: { $gte: sinceSec } } },
      {
        $match: {
          _norm_platform: { $ne: "Unstructured" },
          _ticker_candidates: { $ne: [] },
        },
      },
    ]

    if (platform !== "all") {
      const platformMap = {
        reddit: "Reddit",
        bluesky: "Bluesky",
        bsky: "Bluesky",
        twitter: "Twitter",
        x: "Twitter",
        stocktwits: "StockTwits",
      }
      pipeline.push({ $match: { _norm_platform: platformMap[platform] || platform } })
    }

    if (ticker) {
      pipeline.push({
        $match: { _ticker_candidates: ticker },
      })
    }

    pipeline.push(
      {
        $addFields: {
          _display_sentiment_score: {
            $switch: {
              branches: [
                { case: { $in: [{ $type: "$sentiment_score" }, ["int", "long", "double", "decimal"] ] }, then: { $toDouble: "$sentiment_score" } },
                { case: { $in: [{ $type: "$sentiment" }, ["int", "long", "double", "decimal"] ] }, then: { $toDouble: "$sentiment" } },
                { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bull|positive" } }, then: 1 },
                { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bear|negative" } }, then: -1 },
              ],
              default: 0,
            },
          },
          _platform_rank: {
            $switch: {
              branches: [
                { case: { $eq: ["$_norm_platform", "StockTwits"] }, then: 4 },
                { case: { $eq: ["$_norm_platform", "Twitter"] }, then: 3 },
                { case: { $eq: ["$_norm_platform", "Reddit"] }, then: 2 },
                { case: { $eq: ["$_norm_platform", "Bluesky"] }, then: 1 },
              ],
              default: 0,
            },
          },
          _sentiment_abs: { $abs: "$_display_sentiment_score" },
        },
      },
      { $sort: ranked ? { _sentiment_abs: -1, _platform_rank: -1, _event_sec: -1 } : { _event_sec: -1 } },
      { $limit: limit },
      {
        $project: {
          _id: 1,
          platform: "$_norm_platform",
          source: 1,
          collector: 1,
          ticker: { $ifNull: ["$ticker", { $arrayElemAt: ["$_ticker_candidates", 0] }] },
          symbol: { $ifNull: ["$symbol", { $arrayElemAt: ["$_ticker_candidates", 0] }] },
          title: 1,
          text: 1,
          content: 1,
          url: 1,
          author: 1,
          sentiment: 1,
          sentiment_score: "$_display_sentiment_score",
          raw_sentiment_score: "$sentiment_score",
          cashtag: 1,
          finance_keywords: 1,
          keywords: 1,
          gossip_keywords: 1,
          gossip_score: 1,
          fetched_at: "$_event_sec",
          detected_at: 1,
          created_at: 1,
          timestamp: 1
        }
      }
    )

    const rows = await db.collection("socials").aggregate(pipeline).toArray()
    if (ranked) {
      const platformRank = { StockTwits: 4, Twitter: 3, Reddit: 2, Bluesky: 1 }
      rows.sort((a, b) => {
        const sentimentDiff = Math.abs(Number(b.sentiment_score || 0)) - Math.abs(Number(a.sentiment_score || 0))
        if (sentimentDiff) return sentimentDiff
        const platformDiff = (platformRank[b.platform] || 0) - (platformRank[a.platform] || 0)
        if (platformDiff) return platformDiff
        return Number(b.fetched_at || b.timestamp || 0) - Number(a.fetched_at || a.timestamp || 0)
      })
    }

    return res.json({
      ok: true,
      rows,
      count: rows.length,
      window_minutes: windowMinutes,
      platform,
      ticker,
      since_sec: sinceSec,
      now_sec: Math.floor(Date.now() / 1000),
    })
  } catch (err) {
    console.error("GET /api/social/rolling failed:", err)
    return res.status(500).json({ ok: false, error: String(err?.message || err), rows: [] })
  }
})

app.get("/api/social/series/:ticker", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, ticker: "", rows: [], error: "MongoDB is not connected" })

    const ticker = normalizeTickerList([req.params.ticker], 1, { ensurePrivate: false })[0] || ""
    if (!ticker) return res.status(400).json({ ok: false, ticker: "", rows: [], error: "ticker is required" })

    const windowMinutes = Math.max(5, Math.min(4320, Number(req.query.window_minutes || 1440)))
    const bucketMinutes = Math.max(1, Math.min(60, Number(req.query.bucket_minutes || 5)))
    const sinceSec = Math.floor(Date.now() / 1000) - windowMinutes * 60
    const bucketSec = bucketMinutes * 60

    const rows = await db.collection("socials").aggregate([
      ...socialTimeStages(),
      { $match: { _event_sec: { $gte: sinceSec } } },
      { $match: { _ticker_candidates: ticker } },
      {
        $addFields: {
          _bucket_sec: {
            $multiply: [
              { $floor: { $divide: ["$_event_sec", bucketSec] } },
              bucketSec,
            ],
          },
        },
      },
      {
        $group: {
          _id: "$_bucket_sec",
          message_count: { $sum: 1 },
          bullish: {
            $sum: {
              $cond: [
                { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bull|positive" } },
                1,
                0,
              ],
            },
          },
          bearish: {
            $sum: {
              $cond: [
                { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bear|negative" } },
                1,
                0,
              ],
            },
          },
          platforms: { $addToSet: "$_norm_platform" },
        },
      },
      { $sort: { _id: 1 } },
    ]).toArray()

    const normalized = addSessionScaledSocialFields(rows.map(row => {
      const count = Number(row.message_count || 0)
      return {
        time: new Date(Number(row._id || 0) * 1000).toISOString(),
        bucket_sec: Number(row._id || 0),
        session: marketSessionForSec(row._id),
        message_count: count,
        message_density: Number((count / bucketMinutes).toFixed(3)),
        sentiment: count ? Number((((row.bullish || 0) - (row.bearish || 0)) / count).toFixed(3)) : 0,
        bullish: Number(row.bullish || 0),
        bearish: Number(row.bearish || 0),
        platforms: row.platforms || [],
      }
    }), bucketMinutes)

    res.json({
      ok: true,
      ticker,
      rows: normalized,
      window_minutes: windowMinutes,
      bucket_minutes: bucketMinutes,
      scaling: "per_ticker_per_market_session",
    })
  } catch (err) {
    console.error("GET /api/social/series/:ticker failed:", err)
    res.status(500).json({ ok: false, rows: [], error: String(err.message || err) })
  }
})

function yahooRangeFor(range, interval) {
  const r = String(range || "3mo").toLowerCase()
  const i = String(interval || "1d").toLowerCase()
  if (i === "1m") return "1d"
  if (["5m", "15m", "30m"].includes(i)) return r === "1d" ? "1d" : "5d"
  if (i === "1h") return ["1d", "5d"].includes(r) ? "5d" : "1mo"
  if (["1mo", "3mo", "6mo", "1y", "2y", "5y"].includes(r)) return r
  return "3mo"
}

function yahooIntervalFor(interval) {
  const i = String(interval || "1d").toLowerCase()
  if (["1m", "5m", "15m", "30m", "1h", "1d", "1wk"].includes(i)) return i
  return "1d"
}

async function fetchYahooCandles(ticker, range, interval) {
  const yahooRange = yahooRangeFor(range, interval)
  const yahooInterval = yahooIntervalFor(interval)
  const url = new URL(`https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(ticker)}`)
  url.searchParams.set("range", yahooRange)
  url.searchParams.set("interval", yahooInterval)
  url.searchParams.set("includePrePost", "true")
  url.searchParams.set("events", "history")

  const resp = await fetch(url, {
    headers: {
      "User-Agent": "FeedFlashStockDashboard/0.1",
      "Accept": "application/json",
    },
  })
  if (!resp.ok) throw new Error(`chart provider HTTP ${resp.status}`)
  const payload = await resp.json()
  const result = payload?.chart?.result?.[0]
  const timestamps = result?.timestamp || []
  const quote = result?.indicators?.quote?.[0] || {}
  const candles = []

  for (let i = 0; i < timestamps.length; i += 1) {
    const open = Number(quote.open?.[i])
    const high = Number(quote.high?.[i])
    const low = Number(quote.low?.[i])
    const close = Number(quote.close?.[i])
    if (![open, high, low, close].every(Number.isFinite)) continue
    if (open <= 0 || high <= 0 || low <= 0 || close <= 0) continue
    if (high < Math.max(open, close, low) || low > Math.min(open, close, high)) continue
    candles.push({
      time: Number(timestamps[i]),
      open: Number(open.toFixed(4)),
      high: Number(high.toFixed(4)),
      low: Number(low.toFixed(4)),
      close: Number(close.toFixed(4)),
      volume: Number.isFinite(Number(quote.volume?.[i])) ? Number(quote.volume[i]) : 0,
    })
  }
  return { candles, provider_range: yahooRange, provider_interval: yahooInterval }
}

function sma(values, period) {
  return values.map((_, index) => {
    if (index < period - 1) return null
    const slice = values.slice(index - period + 1, index + 1)
    return slice.reduce((sum, value) => sum + value, 0) / period
  })
}

function bollinger(candles, period = 20, multiplier = 2) {
  const closes = candles.map(c => Number(c.close))
  const middle = sma(closes, period)
  const upper = []
  const lower = []
  for (let i = 0; i < candles.length; i += 1) {
    if (middle[i] == null) continue
    const slice = closes.slice(i - period + 1, i + 1)
    const variance = slice.reduce((sum, value) => sum + Math.pow(value - middle[i], 2), 0) / period
    const std = Math.sqrt(variance)
    upper.push({ time: candles[i].time, value: Number((middle[i] + multiplier * std).toFixed(4)) })
    lower.push({ time: candles[i].time, value: Number((middle[i] - multiplier * std).toFixed(4)) })
  }
  return { upper, lower }
}

function rsi(candles, period = 14) {
  const closes = candles.map(c => Number(c.close))
  const rows = []
  for (let i = period; i < closes.length; i += 1) {
    let gains = 0
    let losses = 0
    for (let j = i - period + 1; j <= i; j += 1) {
      const diff = closes[j] - closes[j - 1]
      if (diff >= 0) gains += diff
      else losses += Math.abs(diff)
    }
    const rs = losses ? gains / losses : 100
    const value = 100 - (100 / (1 + rs))
    rows.push({ time: candles[i].time, value: Number(value.toFixed(2)) })
  }
  return rows
}

function ema(values, period) {
  const k = 2 / (period + 1)
  let current = values[0]
  return values.map((value, index) => {
    current = index === 0 ? value : value * k + current * (1 - k)
    return current
  })
}

function macd(candles) {
  const closes = candles.map(c => Number(c.close))
  if (closes.length < 35) return { macd: [], signal: [], histogram: [] }
  const ema12 = ema(closes, 12)
  const ema26 = ema(closes, 26)
  const macdValues = closes.map((_, index) => ema12[index] - ema26[index])
  const signalValues = ema(macdValues, 9)
  const macdRows = []
  const signalRows = []
  const histogram = []
  for (let i = 26; i < candles.length; i += 1) {
    macdRows.push({ time: candles[i].time, value: Number(macdValues[i].toFixed(4)) })
    signalRows.push({ time: candles[i].time, value: Number(signalValues[i].toFixed(4)) })
    histogram.push({ time: candles[i].time, value: Number((macdValues[i] - signalValues[i]).toFixed(4)) })
  }
  return { macd: macdRows, signal: signalRows, histogram }
}

function predictedPriceSeries(candles, points = 12) {
  const lookback = candles.slice(-30)
  if (lookback.length < 6) return []

  const n = lookback.length
  const meanX = (n - 1) / 2
  const meanY = lookback.reduce((sum, candle) => sum + Number(candle.close), 0) / n
  let numerator = 0
  let denominator = 0
  for (let i = 0; i < n; i += 1) {
    const dx = i - meanX
    numerator += dx * (Number(lookback[i].close) - meanY)
    denominator += dx * dx
  }
  const slope = denominator ? numerator / denominator : 0
  const last = lookback[lookback.length - 1]
  const prev = lookback[lookback.length - 2]
  const step = Math.max(60, Number(last.time || 0) - Number(prev.time || 0) || 60)
  const start = Number(last.close)
  const rows = [{ time: last.time, value: Number(start.toFixed(4)) }]
  for (let i = 1; i <= points; i += 1) {
    rows.push({
      time: Number(last.time || 0) + step * i,
      value: Number(Math.max(0.0001, start + slope * i).toFixed(4)),
    })
  }
  return rows
}

function timestampSeconds(value) {
  if (!value) return 0
  if (value instanceof Date) return Math.floor(value.getTime() / 1000)
  const n = Number(value)
  if (Number.isFinite(n) && n > 0) {
    return n > 1_000_000_000_000 ? Math.floor(n / 1000) : Math.floor(n)
  }
  const ms = Date.parse(value)
  return Number.isFinite(ms) ? Math.floor(ms / 1000) : 0
}

async function chartNewsEvents(db, ticker, windowMinutes = 1440) {
  const days = Math.max(2, Math.ceil(Number(windowMinutes || 1440) / 1440))
  const regex = `(^|,\\s*)${escapeRegExp(ticker)}(\\s*,|$)`
  const docs = await db.collection("articles").find(
    {
      ...recentArticleMatch(days),
      ticker: { $regex: regex, $options: "i" },
    },
    { projection: { title: 1, source: 1, sentiment: 1, sentiment_score: 1, ml_confidence: 1, event_type: 1, sentiment_reason: 1, publish_date: 1, fetched_date: 1, detected_at: 1, createdAt: 1, url: 1 } }
  ).sort({ publish_date: -1, fetched_date: -1, detected_at: -1 }).limit(25).toArray()

  return docs
    .map(article => {
      const time = timestampSeconds(article.publish_date || article.fetched_date || article.detected_at || article.createdAt)
      const sentiment = String(article.sentiment || "neutral").toLowerCase()
      const bullish = /bull|positive/.test(sentiment)
      const bearish = /bear|negative/.test(sentiment)
      return {
        time,
        position: bearish ? "aboveBar" : "belowBar",
        color: bullish ? "#10b981" : bearish ? "#ef4444" : "#f59e0b",
        shape: bullish ? "arrowUp" : bearish ? "arrowDown" : "circle",
        text: article.event_type && article.event_type !== "general_news" ? String(article.event_type).replaceAll("_", " ").slice(0, 14).toUpperCase() : "NEWS",
        title: article.title || "Matched news",
        source: article.source || "News",
        sentiment,
        sentiment_score: Number(article.sentiment_score ?? article.ml_confidence ?? 0) || 0,
        event_type: article.event_type || "general_news",
        reason: article.sentiment_reason || "",
        url: article.url || "",
      }
    })
    .filter(event => event.time > 0)
    .sort((a, b) => a.time - b.time)
}

function chartSocialEvents(socialRows = []) {
  if (!Array.isArray(socialRows) || !socialRows.length) return []
  const maxCount = Math.max(1, ...socialRows.map(row => Number(row.message_count || 0)))
  return socialRows
    .filter(row => Number(row.message_count || 0) >= Math.max(2, Math.ceil(maxCount * 0.45)) || Math.abs(Number(row.sentiment || 0)) >= 0.45)
    .slice(-14)
    .map(row => {
      const sentiment = Number(row.sentiment || 0)
      return {
        time: Number(row.time || row.bucket_sec || 0),
        position: sentiment < -0.15 ? "aboveBar" : "belowBar",
        color: sentiment > 0.15 ? "#38bdf8" : sentiment < -0.15 ? "#fb7185" : "#a78bfa",
        shape: sentiment < -0.15 ? "arrowDown" : sentiment > 0.15 ? "arrowUp" : "circle",
        text: `SOC ${Number(row.message_count || 0)}`,
        title: `${Number(row.message_count || 0)} social messages; sentiment ${sentiment.toFixed(2)}`,
        source: Array.isArray(row.platforms) && row.platforms.length ? row.platforms.join(", ") : "Social",
        sentiment: sentiment > 0.15 ? "bullish" : sentiment < -0.15 ? "bearish" : "neutral",
        sentiment_score: sentiment,
        event_type: "social_spike",
      }
    })
    .filter(event => event.time > 0)
}

async function chartPredictionEvents(db, ticker, windowMinutes = 1440) {
  const sinceSec = Math.floor(Date.now() / 1000) - Math.max(60, Number(windowMinutes || 1440)) * 60
  const docs = await db.collection("prediction_signals").find(
    {
      ticker,
      signal_sec: { $gte: sinceSec },
    },
    {
      projection: {
        ticker: 1,
        signal_sec: 1,
        entry_price: 1,
        decision: 1,
        rank: 1,
        trade_watch: 1,
        baseline_signal: 1,
        model_signal: 1,
        labels: 1,
      },
    }
  ).sort({ signal_sec: 1 }).limit(40).toArray()

  return docs.map(doc => {
    const modelDirection = doc.model_signal?.direction
    const baselineDirection = doc.baseline_signal?.direction || "watch"
    const direction = modelDirection || baselineDirection
    const label5m = doc.labels?.return_5m
    const correct = label5m?.direction_correct
    const color = correct === true
      ? "#22c55e"
      : correct === false
        ? "#f97316"
        : direction === "down"
          ? "#fb7185"
          : "#f59e0b"
    const predictedReturn = doc.model_signal?.predicted_return_5m
    return {
      time: Number(doc.signal_sec || 0),
      position: direction === "down" ? "aboveBar" : "belowBar",
      color,
      shape: direction === "down" ? "arrowDown" : "arrowUp",
      text: direction === "watch" ? "PRED" : `PRED ${String(direction).toUpperCase()}`,
      title: [
        `Trade Watch ${doc.decision || "signal"}`,
        predictedReturn != null ? `model 5m ${Number(predictedReturn).toFixed(2)}%` : "",
        label5m?.return_pct != null ? `actual 5m ${Number(label5m.return_pct).toFixed(2)}%` : "",
      ].filter(Boolean).join("; "),
      source: "Prediction",
      sentiment: direction === "down" ? "bearish" : direction === "up" ? "bullish" : "neutral",
      sentiment_score: Number(doc.trade_watch?.trade_watch_score || 0),
      event_type: "prediction_signal",
      entry_price: doc.entry_price || null,
      model_signal: doc.model_signal || null,
      baseline_signal: doc.baseline_signal || null,
      label_5m: label5m || null,
    }
  }).filter(event => event.time > 0)
}

async function chartSocialSeries(db, ticker, windowMinutes, bucketMinutes) {
  const sinceSec = Math.floor(Date.now() / 1000) - windowMinutes * 60
  const bucketSec = bucketMinutes * 60
  const rows = await db.collection("socials").aggregate([
    ...socialTimeStages(),
    { $match: { _event_sec: { $gte: sinceSec } } },
    { $match: { _ticker_candidates: ticker } },
    {
      $addFields: {
        _bucket_sec: { $multiply: [{ $floor: { $divide: ["$_event_sec", bucketSec] } }, bucketSec] },
        _score: {
          $switch: {
            branches: [
              { case: { $in: [{ $type: "$sentiment_score" }, ["int", "long", "double", "decimal"]] }, then: { $toDouble: "$sentiment_score" } },
              { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bull|positive" } }, then: 1 },
              { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bear|negative" } }, then: -1 },
            ],
            default: 0,
          },
        },
      },
    },
    {
      $group: {
        _id: "$_bucket_sec",
        message_count: { $sum: 1 },
        sentiment: { $avg: "$_score" },
        platforms: { $addToSet: "$_norm_platform" },
      },
    },
    { $sort: { _id: 1 } },
  ]).toArray()

  return addSessionScaledSocialFields(rows.map(row => {
    const count = Number(row.message_count || 0)
    return {
      time: Number(row._id || 0),
      bucket_sec: Number(row._id || 0),
      session: marketSessionForSec(row._id),
      message_count: count,
      message_density: Number((count / bucketMinutes).toFixed(3)),
      sentiment: Number(Number(row.sentiment || 0).toFixed(3)),
      platforms: row.platforms || [],
    }
  }), bucketMinutes)
}

app.get("/api/charts/:ticker", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, candles: [], error: "MongoDB is not connected" })

    const ticker = normalizeTickerList([req.params.ticker], 1, { ensurePrivate: false })[0] || ""
    if (!ticker) return res.status(400).json({ ok: false, candles: [], error: "ticker is required" })

    const range = String(req.query.range || "3mo")
    const interval = yahooIntervalFor(req.query.interval || "1d")
    const isMinute = interval.endsWith("m")
    const socialWindow = Math.max(60, Math.min(4320, Number(req.query.window_minutes || (isMinute ? 1440 : 4320))))
    const socialBucket = Math.max(1, Math.min(60, Number(req.query.bucket_minutes || (interval === "1m" ? 1 : 5))))

    let candleResult = { candles: [], provider_range: null, provider_interval: null }
    let priceStatus = "unavailable"
    let priceDetail = ""
    try {
      candleResult = await fetchYahooCandles(ticker, range, interval)
      priceStatus = candleResult.candles.length ? "working" : "no_bars_returned"
    } catch (err) {
      priceDetail = String(err.message || err)
    }

    const [socialRows, newsEvents, predictionEvents] = await Promise.all([
      chartSocialSeries(db, ticker, socialWindow, socialBucket),
      chartNewsEvents(db, ticker, socialWindow),
      chartPredictionEvents(db, ticker, socialWindow),
    ])
    const candles = candleResult.candles
    const chartEvents = [...newsEvents, ...chartSocialEvents(socialRows), ...predictionEvents].sort((a, b) => Number(a.time || 0) - Number(b.time || 0))
    res.json({
      ok: true,
      ticker,
      range,
      interval,
      candles,
      bollinger: candles.length >= 20 ? bollinger(candles) : { upper: [], lower: [] },
      rsi: candles.length >= 15 ? rsi(candles) : [],
      macd: macd(candles),
      predicted: predictedPriceSeries(candles),
      news_events: chartEvents,
      structured_news_events: newsEvents,
      social_events: chartEvents.filter(event => event.event_type === "social_spike"),
      prediction_events: predictionEvents,
      sentiment: socialRows.map(row => ({ time: row.time, value: row.sentiment })),
      social_density: socialRows.map(row => ({ time: row.time, value: row.message_density, scaled: row.message_density_scaled, count: row.message_count, session: row.session })),
      social_series: socialRows,
      source_status: {
        price: priceStatus,
        price_source: priceStatus === "working" ? "market_chart_provider" : "unavailable",
        price_detail: priceDetail,
        screener_source: "Listed momentum screener universe",
        social: socialRows.length ? "working" : "no_social_posts",
        news: newsEvents.length ? "working" : "no_matched_news",
        predictions: predictionEvents.length ? "working" : "no_prediction_signals",
        markers: chartEvents.length ? "working" : "no_events",
      },
      provider_range: candleResult.provider_range,
      provider_interval: candleResult.provider_interval,
    })
  } catch (err) {
    console.error("GET /api/charts/:ticker failed:", err)
    res.status(500).json({ ok: false, candles: [], error: String(err.message || err) })
  }
})

function articlePrimaryTicker(article) {
  return normalizeTickerList(String(article?.ticker || "").split(","), 1, { ensurePrivate: false })[0] || ""
}

function nearestCandleAtOrAfter(candles, targetSec) {
  if (!Array.isArray(candles) || !candles.length || !Number.isFinite(targetSec)) return null
  let best = null
  for (const candle of candles) {
    const time = Number(candle.time || 0)
    if (time >= targetSec) {
      best = candle
      break
    }
  }
  return best || candles[candles.length - 1]
}

function postEventReturns(candles, eventSec) {
  const base = nearestCandleAtOrAfter(candles, eventSec)
  const baseClose = Number(base?.close || 0)
  if (!base || !baseClose) return null
  const horizons = [
    ["return_1m", 60],
    ["return_5m", 300],
    ["return_15m", 900],
    ["return_1h", 3600],
  ]
  const out = {
    base_time: Number(base.time),
    base_close: Number(baseClose.toFixed(4)),
  }
  for (const [key, seconds] of horizons) {
    const future = nearestCandleAtOrAfter(candles, eventSec + seconds)
    const futureClose = Number(future?.close || 0)
    out[key] = futureClose ? Number((((futureClose - baseClose) / baseClose) * 100).toFixed(3)) : null
  }
  return out
}

app.get("/api/correlation/post-news", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, rows: [], error: "MongoDB is not connected" })

    const limit = Math.max(1, Math.min(120, Number(req.query.limit || 50)))
    const days = Math.max(1, Math.min(5, Number(req.query.days || 3)))
    const requestedTicker = normalizeTickerList([req.query.ticker], 1, { ensurePrivate: false })[0] || ""
    const match = { ...recentArticleMatch(days), ticker: { $exists: true, $nin: ["", null] } }
    if (requestedTicker) match.ticker = { $regex: `(^|,\\s*)${escapeRegExp(requestedTicker)}(\\s*,|$)`, $options: "i" }

    const articles = await db.collection("articles").find(
      match,
      { projection: { title: 1, source: 1, url: 1, ticker: 1, sentiment: 1, sentiment_score: 1, ml_confidence: 1, event_type: 1, sentiment_reason: 1, publish_date: 1, fetched_date: 1, detected_at: 1, createdAt: 1 } }
    ).sort({ publish_date: -1, fetched_date: -1, detected_at: -1 }).limit(limit).toArray()

    const tickers = Array.from(new Set(articles.map(articlePrimaryTicker).filter(Boolean))).slice(0, 20)
    const candleMap = new Map()
    await Promise.all(tickers.map(async ticker => {
      try {
        const result = await fetchYahooCandles(ticker, "5d", "1m")
        candleMap.set(ticker, result.candles || [])
      } catch {
        candleMap.set(ticker, [])
      }
    }))

    const rows = articles.map(article => {
      const ticker = articlePrimaryTicker(article)
      const eventSec = timestampSeconds(article.publish_date || article.fetched_date || article.detected_at || article.createdAt)
      const score = Number(article.sentiment_score ?? article.ml_confidence ?? 0) || 0
      const returns = postEventReturns(candleMap.get(ticker), eventSec)
      return {
        id: String(article._id),
        ticker,
        title: article.title || "",
        source: article.source || "",
        url: article.url || "",
        sentiment: article.sentiment || "neutral",
        sentiment_score: score,
        event_type: article.event_type || "general_news",
        reason: article.sentiment_reason || "",
        event_time: eventSec,
        ...(returns || { base_time: null, base_close: null, return_1m: null, return_5m: null, return_15m: null, return_1h: null }),
      }
    }).filter(row => row.ticker)

    const withReturns = rows.filter(row => row.return_5m != null)
    const average = key => {
      const vals = withReturns.map(row => Number(row[key])).filter(Number.isFinite)
      return vals.length ? Number((vals.reduce((sum, value) => sum + value, 0) / vals.length).toFixed(3)) : null
    }

    res.json({
      ok: true,
      rows,
      summary: {
        articles: rows.length,
        priced_articles: withReturns.length,
        avg_return_1m: average("return_1m"),
        avg_return_5m: average("return_5m"),
        avg_return_15m: average("return_15m"),
        avg_return_1h: average("return_1h"),
      },
      horizons: ["1m", "5m", "15m", "1h"],
      note: "Returns use nearest available 1-minute market candle at or after the article timestamp.",
    })
  } catch (err) {
    console.error("GET /api/correlation/post-news failed:", err)
    res.status(500).json({ ok: false, rows: [], error: String(err.message || err) })
  }
})

app.get(["/api/sentiment/audit", "/api/sentiment/snapshot"], async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, rows: [], error: "MongoDB is not connected" })
    const days = Math.max(1, Math.min(7, Number(req.query.days || 3)))
    const limit = Math.max(1, Math.min(200, Number(req.query.limit || 60)))
    const label = String(req.query.label || "").toLowerCase()
    const recentMatch = recentArticleMatch(days)
    const tickerMatch = { ...recentMatch, ticker: { $exists: true, $nin: ["", null] } }
    const actionableMatch = {
      ...tickerMatch,
      $or: [
        { sentiment: { $nin: ["neutral", null, ""] } },
        { event_type: { $exists: true, $nin: ["general_news", "unknown", null, ""] } },
      ],
    }
    const match = { ...tickerMatch }
    if (["bullish", "positive"].includes(label)) match.sentiment = { $regex: "bull|positive", $options: "i" }
    if (["bearish", "negative"].includes(label)) match.sentiment = { $regex: "bear|negative", $options: "i" }
    if (label === "neutral") match.sentiment = { $regex: "neutral", $options: "i" }

    const scoredProjection = {
      title: 1,
      source: 1,
      url: 1,
      ticker: 1,
      sentiment: 1,
      sentiment_score: 1,
      ml_confidence: 1,
      sentiment_method: 1,
      event_type: 1,
      event_score: 1,
      sentiment_reason: 1,
      publish_date: 1,
      fetched_date: 1,
      detected_at: 1,
    }
    const scoreStages = [
      {
        $addFields: {
          _sentiment_direction: {
            $switch: {
              branches: [
                { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bull|positive" } }, then: 1 },
                { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bear|negative" } }, then: -1 },
              ],
              default: 0,
            },
          },
        },
      },
      {
        $addFields: {
          _score: {
            $switch: {
              branches: [
                { case: { $in: [{ $type: "$sentiment_score" }, ["int", "long", "double", "decimal"]] }, then: { $toDouble: "$sentiment_score" } },
                { case: { $in: [{ $type: "$ml_confidence" }, ["int", "long", "double", "decimal"]] }, then: { $multiply: ["$_sentiment_direction", { $toDouble: "$ml_confidence" }] } },
              ],
              default: "$_sentiment_direction",
            },
          },
        },
      },
    ]

    const socialWindowMinutes = Math.max(5, Math.min(4320, Number(req.query.window_minutes || 1440)))
    const socialSinceSec = Math.floor(Date.now() / 1000) - socialWindowMinutes * 60

    const [rows, total, tickerMatched, nonNeutral, eventful, actionable, sentimentSummary, topPositive, topNegative, sourceSummary, eventSummary, tickerSummary, socialSummary] = await Promise.all([
      db.collection("articles").find(
        match,
        { projection: scoredProjection }
      ).sort({ detected_at: -1, fetched_date: -1, publish_date: -1 }).limit(limit).toArray(),
      db.collection("articles").countDocuments(recentMatch),
      db.collection("articles").countDocuments(tickerMatch),
      db.collection("articles").countDocuments({ ...tickerMatch, sentiment: { $nin: ["neutral", null, ""] } }),
      db.collection("articles").countDocuments({ ...tickerMatch, event_type: { $exists: true, $nin: ["general_news", "unknown", null, ""] } }),
      db.collection("articles").countDocuments(actionableMatch),
      db.collection("articles").aggregate([
        { $match: tickerMatch },
        ...scoreStages,
        {
          $group: {
            _id: null,
            avg_sentiment: { $avg: "$_score" },
            avg_abs_sentiment: { $avg: { $abs: "$_score" } },
            scored: { $sum: { $cond: [{ $gt: [{ $abs: "$_score" }, 0.005] }, 1, 0] } },
          },
        },
      ]).toArray(),
      db.collection("articles").aggregate([
        { $match: tickerMatch },
        ...scoreStages,
        { $match: { _score: { $gt: 0.005 } } },
        { $sort: { _score: -1, detected_at: -1, fetched_date: -1, publish_date: -1 } },
        { $limit: 5 },
        { $project: { ...scoredProjection, sentiment_score: "$_score" } },
      ]).toArray(),
      db.collection("articles").aggregate([
        { $match: tickerMatch },
        ...scoreStages,
        { $match: { _score: { $lt: -0.005 } } },
        { $sort: { _score: 1, detected_at: -1, fetched_date: -1, publish_date: -1 } },
        { $limit: 5 },
        { $project: { ...scoredProjection, sentiment_score: "$_score" } },
      ]).toArray(),
      db.collection("articles").aggregate([
        { $match: tickerMatch },
        ...scoreStages,
        {
          $group: {
            _id: { $ifNull: ["$source", "Unknown"] },
            count: { $sum: 1 },
            avg_sentiment: { $avg: "$_score" },
            scored: { $sum: { $cond: [{ $gt: [{ $abs: "$_score" }, 0.005] }, 1, 0] } },
          },
        },
        { $sort: { count: -1 } },
        { $limit: 8 },
        { $project: { _id: 0, source: "$_id", count: 1, avg_sentiment: { $round: ["$avg_sentiment", 3] }, scored: 1 } },
      ]).toArray(),
      db.collection("articles").aggregate([
        { $match: tickerMatch },
        ...scoreStages,
        {
          $group: {
            _id: { $ifNull: ["$event_type", "general_news"] },
            count: { $sum: 1 },
            avg_sentiment: { $avg: "$_score" },
          },
        },
        { $sort: { count: -1 } },
        { $limit: 8 },
        { $project: { _id: 0, event_type: "$_id", count: 1, avg_sentiment: { $round: ["$avg_sentiment", 3] } } },
      ]).toArray(),
      db.collection("articles").aggregate([
        { $match: tickerMatch },
        ...scoreStages,
        {
          $group: {
            _id: "$ticker",
            count: { $sum: 1 },
            avg_sentiment: { $avg: "$_score" },
            latest: { $max: { $ifNull: ["$detected_at", { $ifNull: ["$fetched_date", "$publish_date"] }] } },
          },
        },
        { $sort: { count: -1, latest: -1 } },
        { $limit: 8 },
        { $project: { _id: 0, ticker: "$_id", count: 1, avg_sentiment: { $round: ["$avg_sentiment", 3] }, latest: 1 } },
      ]).toArray(),
      db.collection("socials").aggregate([
        ...socialTimeStages(),
        { $match: { _event_sec: { $gte: socialSinceSec }, _ticker_candidates: { $ne: [] } } },
        {
          $addFields: {
            _social_score: {
              $switch: {
                branches: [
                  { case: { $in: [{ $type: "$sentiment_score" }, ["int", "long", "double", "decimal"]] }, then: { $toDouble: "$sentiment_score" } },
                  { case: { $in: [{ $type: "$sentiment" }, ["int", "long", "double", "decimal"]] }, then: { $toDouble: "$sentiment" } },
                  { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bull|positive" } }, then: 1 },
                  { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bear|negative" } }, then: -1 },
                ],
                default: 0,
              },
            },
          },
        },
        {
          $group: {
            _id: null,
            total: { $sum: 1 },
            avg_sentiment: { $avg: "$_social_score" },
            bullish: { $sum: { $cond: [{ $gt: ["$_social_score", 0.05] }, 1, 0] } },
            bearish: { $sum: { $cond: [{ $lt: ["$_social_score", -0.05] }, 1, 0] } },
            neutral: { $sum: { $cond: [{ $lte: [{ $abs: "$_social_score" }, 0.05] }, 1, 0] } },
            platforms: { $addToSet: "$_norm_platform" },
          },
        },
      ]).toArray(),
    ])

    const mapAuditRow = row => ({
      id: String(row._id),
      ticker: row.ticker || "",
      title: row.title || "",
      source: row.source || "",
      url: row.url || "",
      sentiment: row.sentiment || "neutral",
      sentiment_score: Number(articleSentimentValue(row).toFixed(3)),
      confidence: Number(row.ml_confidence ?? Math.abs(articleSentimentValue(row))) || 0,
      method: row.sentiment_method || "unknown",
      event_type: row.event_type || "general_news",
      event_score: Number(row.event_score || 0),
      reason: row.sentiment_reason || "No high-impact phrase matched",
      publish_date: row.publish_date || row.fetched_date || row.detected_at || null,
    })

    const social = socialSummary[0] || {}
    const newsAvg = Number(sentimentSummary[0]?.avg_sentiment || 0)
    const socialAvg = Number(social.avg_sentiment || 0)
    const combinedWeight = Number(tickerMatched || 0) + Number(social.total || 0) * 0.75
    const combinedAvg = combinedWeight
      ? (newsAvg * Number(tickerMatched || 0) + socialAvg * Number(social.total || 0) * 0.75) / combinedWeight
      : 0

    res.json({
      ok: true,
      generated_at: new Date().toISOString(),
      rows: rows.map(mapAuditRow),
      top_positive: topPositive.map(mapAuditRow),
      top_negative: topNegative.map(mapAuditRow),
      sources: sourceSummary,
      event_types: eventSummary,
      ticker_breakdown: tickerSummary,
      days,
      social_window_minutes: socialWindowMinutes,
      summary: {
        total,
        ticker_matched: tickerMatched,
        non_neutral: nonNeutral,
        eventful,
        actionable,
        avg_sentiment: Number((sentimentSummary[0]?.avg_sentiment || 0).toFixed(3)),
        avg_abs_sentiment: Number((sentimentSummary[0]?.avg_abs_sentiment || 0).toFixed(3)),
        scored: Number(sentimentSummary[0]?.scored || 0),
        social_total: Number(social.total || 0),
        social_avg_sentiment: Number((social.avg_sentiment || 0).toFixed(3)),
        social_bullish: Number(social.bullish || 0),
        social_bearish: Number(social.bearish || 0),
        social_neutral: Number(social.neutral || 0),
        social_platforms: social.platforms || [],
        combined_avg_sentiment: Number(combinedAvg.toFixed(3)),
      },
      snapshot_mode: "news_and_social_signed_sentiment_snapshot",
      audit_mode: "deterministic_financial_phrase_with_event_taxonomy",
    })
  } catch (err) {
    console.error("GET /api/sentiment snapshot failed:", err)
    res.status(500).json({ ok: false, rows: [], error: String(err.message || err) })
  }
})

app.get("/api/sentiment/batch-candidates", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, items: [], error: "MongoDB is not connected" })
    const days = Math.max(1, Math.min(7, Number(req.query.days || 3)))
    const limit = Math.max(1, Math.min(150, Number(req.query.limit || 100)))
    const rows = await db.collection("articles").find(
      {
        ...recentArticleMatch(days),
        ticker: { $exists: true, $nin: ["", null] },
        $or: [
          { sentiment: { $regex: "neutral", $options: "i" } },
          { sentiment_score: { $gte: -0.12, $lte: 0.12 } },
          { sentiment_score: { $exists: false } },
        ],
      },
      { projection: { title: 1, content: 1, source: 1, ticker: 1, url: 1 } }
    ).sort({ detected_at: -1, fetched_date: -1, publish_date: -1 }).limit(limit).toArray()

    const items = rows.map((row, index) => ({
      id: String(row._id),
      batch_id: index + 1,
      ticker: row.ticker || "",
      source: row.source || "",
      headline: row.title || "",
      excerpt: String(row.content || "").slice(0, 500),
      url: row.url || "",
    }))

    res.json({
      ok: true,
      items,
      prompt: "Classify each item sentiment for the listed stock ticker. Echo id. Return label as positive, negative, neutral, or mixed; score from -1 to 1; event_type; short reason.",
      response_schema: {
        type: "array",
        items: {
          type: "object",
          required: ["id", "label", "score", "event_type", "reason"],
          properties: {
            id: { type: "string" },
            label: { enum: ["positive", "negative", "neutral", "mixed"] },
            score: { type: "number" },
            event_type: { type: "string" },
            reason: { type: "string" },
          },
        },
      },
      note: "This is the low-volume LLM batch queue for borderline/neutral articles; deterministic scoring remains live.",
    })
  } catch (err) {
    console.error("GET /api/sentiment/batch-candidates failed:", err)
    res.status(500).json({ ok: false, items: [], error: String(err.message || err) })
  }
})

app.get("/api/prediction/features", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, rows: [], error: "MongoDB is not connected" })

    const limit = Math.max(10, Math.min(500, Number(req.query.limit || 150)))
    const days = Math.max(1, Math.min(14, Number(req.query.days || 3)))
    const socialWindow = Math.max(5, Math.min(1440, Number(req.query.window_minutes || 60)))
    const sinceSec = Math.floor(Date.now() / 1000) - socialWindow * 60

    const screenerRows = await db.collection("screeners").find(
      {
        ticker: { $exists: true, $nin: ["", null], $not: /\./ },
        exchange: { $in: ["NASDAQ", "NYSE", "AMEX"] },
        price: { $gt: 0 },
        change_pct: { $exists: true },
      },
      {
        projection: {
          ticker: 1,
          company: 1,
          exchange: 1,
          sector: 1,
          industry: 1,
          price: 1,
          change_pct: 1,
          volume: 1,
          avg_volume: 1,
          market_cap: 1,
          rel_volume: 1,
          rsi: 1,
          gap: 1,
          perf_week: 1,
          perf_month: 1,
          quote_updated_at: 1,
        },
      }
    ).sort({ volume: -1 }).limit(limit).toArray()

    const tickers = screenerRows.map(row => String(row.ticker || "").toUpperCase()).filter(Boolean)
    const [articleRows, socialRows] = await Promise.all([
      db.collection("articles").aggregate([
        { $match: { ...recentArticleMatch(days), ticker: { $exists: true, $nin: ["", null] } } },
        {
          $addFields: {
            _ticker_parts: {
              $map: {
                input: { $split: [{ $toUpper: { $toString: "$ticker" } }, ","] },
                as: "ticker_part",
                in: { $trim: { input: "$$ticker_part" } },
              },
            },
            _sentiment_direction: {
              $switch: {
                branches: [
                  { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bull|positive" } }, then: 1 },
                  { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bear|negative" } }, then: -1 },
                ],
                default: 0,
              },
            },
          },
        },
        { $unwind: "$_ticker_parts" },
        { $match: { _ticker_parts: { $in: tickers } } },
        {
          $addFields: {
            _score: {
              $switch: {
                branches: [
                  { case: { $in: [{ $type: "$sentiment_score" }, ["int", "long", "double", "decimal"]] }, then: { $toDouble: "$sentiment_score" } },
                  { case: { $in: [{ $type: "$ml_confidence" }, ["int", "long", "double", "decimal"]] }, then: { $multiply: ["$_sentiment_direction", { $toDouble: "$ml_confidence" }] } },
                ],
                default: "$_sentiment_direction",
              },
            },
          },
        },
        {
          $group: {
            _id: "$_ticker_parts",
            article_count: { $sum: 1 },
            article_sentiment: { $avg: "$_score" },
            article_sentiment_abs: { $avg: { $abs: "$_score" } },
            event_count: {
              $sum: {
                $cond: [
                  { $not: { $in: ["$event_type", ["general_news", "unknown", null, ""]] } },
                  1,
                  0,
                ],
              },
            },
            latest_article_ts: { $max: "$detected_at" },
          },
        },
      ]).toArray(),
      db.collection("socials").aggregate([
        ...socialTimeStages(),
        ...socialTickerCandidateStages(),
        { $match: { _event_sec: { $gte: sinceSec }, _ticker_candidates: { $in: tickers } } },
        { $unwind: "$_ticker_candidates" },
        { $match: { _ticker_candidates: { $in: tickers } } },
        {
          $addFields: {
            _score: {
              $switch: {
                branches: [
                  { case: { $in: [{ $type: "$sentiment_score" }, ["int", "long", "double", "decimal"]] }, then: { $toDouble: "$sentiment_score" } },
                  { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bull|positive" } }, then: 1 },
                  { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bear|negative" } }, then: -1 },
                ],
                default: 0,
              },
            },
          },
        },
        {
          $group: {
            _id: "$_ticker_candidates",
            social_count: { $sum: 1 },
            social_sentiment: { $avg: "$_score" },
            social_sentiment_abs: { $avg: { $abs: "$_score" } },
            latest_social_ts: { $max: "$_event_sec" },
          },
        },
      ]).toArray(),
    ])

    const articleMap = new Map(articleRows.map(row => [String(row._id || "").toUpperCase(), row]))
    const socialMap = new Map(socialRows.map(row => [String(row._id || "").toUpperCase(), row]))

    const rows = screenerRows.map(raw => {
      const row = normalizeScreenerDoc(raw)
      const articles = articleMap.get(row.ticker) || {}
      const social = socialMap.get(row.ticker) || {}
      const volume = Number(row.volume || 0)
      const avgVolume = Number(row.avg_volume || 0)
      const relVolume = Number(row.rel_volume || (avgVolume ? volume / Math.max(1, avgVolume) : 0)) || 0
      const articleSentiment = Number(articles.article_sentiment || 0)
      const socialSentiment = Number(social.social_sentiment || 0)
      const socialCount = Number(social.social_count || 0)
      const articleCount = Number(articles.article_count || 0)
      const eventCount = Number(articles.event_count || 0)
      const momentumScore = Number(row.change_pct || 0)
      const evidenceScore = articleSentiment * Math.min(1, articleCount / 5) + socialSentiment * Math.min(1, socialCount / 20)
      const modelReady = Boolean(row.price && volume && (articleCount || socialCount) && Number.isFinite(momentumScore))

      return {
        ticker: row.ticker,
        company: row.company,
        exchange: row.exchange,
        sector: row.sector,
        generated_at: new Date().toISOString(),
        features: {
          price: row.price,
          change_pct: row.change_pct,
          volume,
          rel_volume: Number(relVolume.toFixed(3)),
          market_cap: row.market_cap,
          rsi: row.rsi,
          gap: row.gap,
          perf_week: row.perf_week,
          perf_month: row.perf_month,
          article_count: articleCount,
          article_sentiment: Number(articleSentiment.toFixed(3)),
          event_count: eventCount,
          social_count: socialCount,
          social_density_per_minute: Number((socialCount / socialWindow).toFixed(3)),
          social_sentiment: Number(socialSentiment.toFixed(3)),
          evidence_score: Number(evidenceScore.toFixed(3)),
        },
        labels: {
          target_return_5m: null,
          target_return_15m: null,
          target_return_60m: null,
        },
        baseline_signal: {
          direction: evidenceScore >= 0.15 && momentumScore > 0 ? "up" : evidenceScore <= -0.15 && momentumScore < 0 ? "down" : "watch",
          confidence: Number(Math.min(0.95, Math.abs(evidenceScore) * 0.35 + Math.min(1, relVolume / 5) * 0.25 + Math.min(1, Math.abs(momentumScore) / 20) * 0.25).toFixed(3)),
          model_ready: modelReady,
        },
      }
    })

    res.json({
      ok: true,
      rows,
      count: rows.length,
      feature_version: "price_social_news_v1",
      social_window_minutes: socialWindow,
      label_status: "pending_intraday_return_join",
      note: "This endpoint is the stock-price-prediction feature matrix. The next step is joining 1-minute candles after each signal timestamp to fill target_return labels.",
    })
  } catch (err) {
    console.error("GET /api/prediction/features failed:", err)
    res.status(500).json({ ok: false, rows: [], error: String(err.message || err) })
  }
})

app.get("/api/prediction/signals", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, rows: [], error: "MongoDB is not connected" })

    const limit = Math.max(1, Math.min(500, Number(req.query.limit || 100)))
    const ticker = String(req.query.ticker || "").toUpperCase().replace(/[^A-Z0-9.-]/g, "")
    const status = String(req.query.status || "").toLowerCase()
    const filter = {}
    if (ticker) filter.ticker = ticker
    if (status) filter.label_status = status

    const [rows, summaryRows, model] = await Promise.all([
      db.collection("prediction_signals").find(filter).sort({ signal_sec: -1, rank: 1 }).limit(limit).toArray(),
      db.collection("prediction_signals").aggregate([
        { $match: filter },
        {
          $group: {
            _id: "$label_status",
            count: { $sum: 1 },
            avg_score: { $avg: "$trade_watch.trade_watch_score" },
            avg_5m: { $avg: "$labels.return_5m.return_pct" },
            avg_15m: { $avg: "$labels.return_15m.return_pct" },
            avg_60m: { $avg: "$labels.return_60m.return_pct" },
            correct_5m: {
              $avg: {
                $cond: [
                  { $eq: ["$labels.return_5m.direction_correct", true] },
                  1,
                  { $cond: [{ $eq: ["$labels.return_5m.direction_correct", false] }, 0, null] },
                ],
              },
            },
          },
        },
      ]).toArray(),
      loadLatestPredictionModel(db),
    ])

    res.json({
      ok: true,
      rows: rows.map(row => ({
        ...row,
        id: String(row._id),
        _id: undefined,
      })),
      count: rows.length,
      summary: summaryRows.map(row => ({
        status: row._id || "unknown",
        count: row.count,
        avg_score: Number((row.avg_score || 0).toFixed(3)),
        avg_return_5m: row.avg_5m == null ? null : Number(row.avg_5m.toFixed(3)),
        avg_return_15m: row.avg_15m == null ? null : Number(row.avg_15m.toFixed(3)),
        avg_return_60m: row.avg_60m == null ? null : Number(row.avg_60m.toFixed(3)),
        directional_accuracy_5m: row.correct_5m == null ? null : Number(row.correct_5m.toFixed(3)),
      })),
      horizons_minutes: PREDICTION_HORIZONS_MINUTES,
      feature_version: "trade_watch_prediction_v1",
      model: model ? {
        status: model.status,
        samples: model.samples || 0,
        min_samples: model.min_samples,
        metrics: model.metrics || null,
        updated_at: model.updated_at || null,
      } : null,
    })
  } catch (err) {
    console.error("GET /api/prediction/signals failed:", err)
    res.status(500).json({ ok: false, rows: [], error: String(err.message || err) })
  }
})

app.get("/api/prediction/model", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, error: "MongoDB is not connected" })
    const model = await loadLatestPredictionModel(db)
    res.json({ ok: true, model })
  } catch (err) {
    console.error("GET /api/prediction/model failed:", err)
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.post("/api/prediction/train", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, error: "MongoDB is not connected" })
    const model = await trainPredictionModel(db, {
      minSamples: Number(req.query.min_samples || req.body?.min_samples || 20),
      limit: Number(req.query.limit || req.body?.limit || 2000),
    })
    res.json({ ok: true, model })
  } catch (err) {
    console.error("POST /api/prediction/train failed:", err)
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.post("/api/prediction/snapshot", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, error: "MongoDB is not connected" })
    const labels = await labelMaturePredictionSignals(db)
    const snapshot = await captureTradeWatchPredictionSignals(db, {
      limit: Number(req.query.limit || req.body?.limit || process.env.PREDICTION_SIGNAL_LIMIT || 10),
      socialWindow: Number(req.query.window_minutes || req.body?.window_minutes || process.env.PREDICTION_SOCIAL_WINDOW || 60),
    })
    const model = await trainPredictionModel(db, { minSamples: Number(process.env.PREDICTION_MIN_TRAINING_SAMPLES || 20) })
    res.json({ ok: true, labels, snapshot, model })
  } catch (err) {
    console.error("POST /api/prediction/snapshot failed:", err)
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

async function runPythonScriptForRoute(scriptPath, {
  timeout = 180000,
  extraEnv = {},
} = {}) {
  const { execFile } = await import("node:child_process")
  const { existsSync } = await import("node:fs")
  const pythonPath = existsSync("/opt/rssvenv/bin/python") ? "/opt/rssvenv/bin/python" : "python3"

  if (!existsSync(scriptPath)) {
    return {
      ok: false,
      skipped: true,
      stdout: "",
      stderr: "",
      error: `Script not found at ${scriptPath}`,
    }
  }

  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || "mongodb://mongo:27017/feedflash"
  try {
    return await new Promise((resolve) => {
      execFile(
        pythonPath,
        [scriptPath],
        {
          cwd: process.cwd(),
          timeout,
          maxBuffer: 1024 * 1024 * 20,
          env: {
            ...process.env,
            MONGODB_URI: mongoUri,
            MONGO_URI: mongoUri,
            MONGO_DB: "feedflash",
            MONGODB_DB: "feedflash",
            ...extraEnv,
          },
        },
        (error, stdout, stderr) => {
          resolve({
            ok: !error,
            stdout: String(stdout || ""),
            stderr: String(stderr || ""),
            error: error ? String(error.message || error) : "",
          })
        }
      )
    })
  } catch (err) {
    return { ok: false, stdout: "", stderr: "", error: String(err?.message || err) }
  }
}

function parseSocialFetchForRoute(stdout = "") {
  const text = String(stdout || "")
  const savedMatch = text.match(/saved=(\d+)/i)
  const matchedMatch = text.match(/matched=(\d+)/i)
  const insertedMatch = text.match(/inserted=(\d+)/i)
  const modifiedMatch = text.match(/modified=(\d+)/i)
  return {
    saved: savedMatch ? Number(savedMatch[1]) : undefined,
    matched: matchedMatch ? Number(matchedMatch[1]) : undefined,
    inserted: insertedMatch ? Number(insertedMatch[1]) : undefined,
    modified: modifiedMatch ? Number(modifiedMatch[1]) : undefined,
  }
}

app.post("/api/social/fetch", async (req, res) => {
  const started = Date.now()
  const ticker = normalizeTickerList([req.query.ticker || req.body?.ticker], 1, { ensurePrivate: false })[0] || ""

  if (!ticker) {
    return res.status(400).json({ ok: false, error: "ticker is required", ms: Date.now() - started })
  }

  try {
    const result = await runPythonScriptForRoute("1_News/pipeline/fetch_social_to_mongo.py", {
      timeout: 45000,
      extraEnv: {
        SOCIAL_TICKERS: ticker,
        SOCIAL_MAX_TICKERS: "1",
        SOCIAL_MAX_WORKERS: "1",
      },
    })
    const counts = parseSocialFetchForRoute(result.stdout || "")

    return res.status(result.ok ? 200 : 500).json({
      ok: result.ok,
      ticker,
      ...counts,
      stdout: result.stdout,
      stderr: result.stderr,
      error: result.error,
      ms: Date.now() - started,
    })
  } catch (err) {
    return res.status(500).json({
      ok: false,
      ticker,
      error: String(err?.message || err),
      ms: Date.now() - started,
    })
  }
})

app.get("/api/social/rolling/stats", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) {
      return res.status(503).json({ ok: false, error: "MongoDB is not connected", counts: {} })
    }

    const windowMinutes = Math.max(1, Math.min(1440, Number(req.query.window_minutes || 5)))
    const sinceSec = Math.floor(Date.now() / 1000) - windowMinutes * 60

    const rows = await db.collection("socials").aggregate([
      ...socialTimeStages(),
      { $match: { _event_sec: { $gte: sinceSec } } },
      {
        $match: {
          _norm_platform: { $ne: "Unstructured" },
          _ticker_candidates: { $ne: [] },
        },
      },
      { $group: { _id: "$_norm_platform", count: { $sum: 1 } } },
      { $sort: { count: -1 } }
    ]).toArray()

    const counts = {}
    for (const row of rows) counts[row._id || "Unknown"] = row.count

    return res.json({
      ok: true,
      counts,
      rows,
      total: rows.reduce((sum, row) => sum + row.count, 0),
      window_minutes: windowMinutes,
      since_sec: sinceSec,
      now_sec: Math.floor(Date.now() / 1000),
    })
  } catch (err) {
    console.error("GET /api/social/rolling/stats failed:", err)
    return res.status(500).json({ ok: false, error: String(err?.message || err), counts: {} })
  }
})
// SOCIAL_ROLLING_API_V2_END


app.use('/api/social',      socialRouter)
app.use('/api/correlation', correlationRouter)
app.use('/api/settings',    settingsRouter)

// ── Health check ──────────────────────────────────────────
app.get('/api/health', (req, res) => {
  const { readyState } = mongoose.connection
  const states = { 0:'disconnected', 1:'connected', 2:'connecting', 3:'disconnecting' }
  res.json({
    status:  'ok',
    db:      states[readyState] || 'unknown',
    time:    new Date().toISOString(),
  })
})

// ── Start ─────────────────────────────────────────────────
async function ensureRuntimeIndexes() {
  const db = mongoose.connection.db
  if (!db) return

  await Promise.allSettled([
    db.collection("articles").createIndex({ ticker: 1, detected_at: -1 }),
    db.collection("articles").createIndex({ ticker: 1, fetched_date: -1 }),
    db.collection("articles").createIndex({ source: 1, fetched_date: -1 }),
    db.collection("articles").createIndex({ sentiment: 1, event_type: 1 }),
    db.collection("socials").createIndex({ ticker: 1, fetched_at: -1 }),
    db.collection("socials").createIndex({ symbol: 1, fetched_at: -1 }),
    db.collection("socials").createIndex({ platform: 1, fetched_at: -1 }),
    db.collection("screeners").createIndex({ exchange: 1, change_pct: -1 }),
    db.collection("screeners").createIndex({ exchange: 1, volume: -1 }),
    db.collection("screeners").createIndex({ quote_source: 1, change_pct: -1 }),
    db.collection("prediction_signals").createIndex({ ticker: 1, signal_sec: -1 }),
    db.collection("prediction_signals").createIndex({ label_status: 1, signal_sec: -1 }),
    db.collection("prediction_signals").createIndex({ source: 1, signal_sec: -1 }),
    db.collection("prediction_models").createIndex({ model_id: 1, updated_at: -1 }),
  ])
}

async function start() {
  await connectDB()
  await ensureRuntimeIndexes()
  
// Ryan frontend compatibility endpoints
app.get("/api/status", async (req, res) => {
  try {
    const db = mongoose.connection.db;
    const articles = db.collection("articles");
    const articleWindow = recentArticleMatch();
    const [totalArticles, recentArticles] = await Promise.all([
      articles.countDocuments({}),
      articles.countDocuments(articleWindow),
    ])

    const latest = await articles.find(
      {},
      { projection: { title: 1, source: 1, publish_date: 1, fetched_date: 1 } }
    ).sort({ fetched_date: -1, publish_date: -1 }).limit(1).toArray();

    res.json({
      ok: true,
      status: "ok",
      connected: true,
      articles: totalArticles,
      total: totalArticles,
      total_all: totalArticles,
      recent_articles: recentArticles,
      article_count: totalArticles,
      database: {
        connected: mongoose.connection.readyState === 1,
        articles: totalArticles,
        total: totalArticles,
        total_all: totalArticles,
        recent_articles: recentArticles,
        article_count: totalArticles
      },
      latest_article: latest[0] || null,
      market_window_start: latestMarketCloseCutoff().toISOString(),
      time: new Date().toISOString()
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      status: "error",
      connected: false,
      articles: 0,
      total: 0,
      article_count: 0,
      database: {
        connected: false,
        articles: 0,
        total: 0,
        article_count: 0
      },
      error: "Failed to load status"
    });
  }
});

app.get("/api/market/status", async (req, res) => {
  try {
    const now = new Date();
    const ny = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
    const day = ny.getDay(); // 0 = Sun .. 6 = Sat
    const hour = ny.getHours();
    const minute = ny.getMinutes();
    const minutes = hour * 60 + minute;

    const preStart = 4 * 60; // 04:00 ET
    const regularStart = 9 * 60 + 30; // 09:30 ET
    const regularEnd = 16 * 60; // 16:00 ET
    const afterEnd = 20 * 60; // 20:00 ET

    const isWeekday = day >= 1 && day <= 5
    const inPreMarket = isWeekday && minutes >= preStart && minutes < regularStart
    const inRegular = isWeekday && minutes >= regularStart && minutes < regularEnd
    const inAfterHours = isWeekday && minutes >= regularEnd && minutes < afterEnd

    const nextOpen = (() => {
      if (inRegular || inPreMarket || inAfterHours) {
        if (inRegular || inPreMarket) return `${String(9).padStart(2, '0')}:30 ET`
        return `${String(9).padStart(2, '0')}:30 ET`
      }

      const isFriday = day === 5
      const nextWeekday = isFriday ? 1 : day === 6 ? 1 : day === 0 ? 1 : day + 1
      return `${String(9).padStart(2, '0')}:30 ET on ${['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][nextWeekday]}`
    })()

    const nextClose = inRegular ? `${String(16).padStart(2, '0')}:00 ET` : undefined

    let status = 'closed'
    let label = 'Market Closed'
    if (inRegular) {
      status = 'open'
      label = 'Market Open'
    } else if (inPreMarket) {
      status = 'pre'
      label = 'Pre-market'
    } else if (inAfterHours) {
      status = 'after'
      label = 'After-hours'
    }

    res.json({
      open: status === 'open',
      status,
      label,
      timezone: 'America/New_York',
      next_open: nextOpen,
      next_close: nextClose,
      tracked_exchanges: Array.from(US_EXCHANGES),
      tracked_indices: TRACKED_MARKET_INDICES,
      tracked_markets: TRACKED_MARKETS,
      updated_at: ny.toISOString()
    })
  } catch (err) {
    res.json({ open: false, status: 'unknown', label: 'Market Unknown', updated_at: new Date().toISOString() })
  }
});

app.get("/api/stats", async (req, res) => {
  try {
    const db = mongoose.connection.db;
    res.json(await loadArticleStats(db, Number(req.query.days || req.query.recent_days || 0)));
  } catch (err) {
    const trackedTickers = loadTrackedTickers()
    res.status(500).json({
      total: 0,
      total_recent: 0,
      total_all: 0,
      sources: [],
      categories: [],
      sentiment: { bullish: 0, bearish: 0, neutral: 0, unknown: 0 },
      ticker_mentions: [],
      tracked_market_count: TRACKED_MARKETS.length,
      tracked_markets: TRACKED_MARKETS,
      tracked_exchanges: Array.from(US_EXCHANGES),
      tracked_indices: TRACKED_MARKET_INDICES,
      market_universe_label: "NASDAQ / NYSE / AMEX equities plus major US index markets",
      tracked_ticker_count: trackedTickers.length,
      tracked_tickers: trackedTickers,
      error: "Failed to load stats"
    });
  }
});

app.get("/api/keywords", async (req, res) => {
  res.json({
    keywords: [
      "earnings",
      "guidance",
      "upgrade",
      "downgrade",
      "merger",
      "acquisition",
      "lawsuit",
      "sec",
      "fda",
      "short squeeze",
      "bankruptcy",
      "dividend",
      "offering",
      "partnership"
    ]
  });
});


// Duplicate /api/keywords removed - see settings routes for the authoritative implementation

// Frontend compatibility endpoints
app.get("/api/status", async (req, res) => {
  try {
    const db = mongoose.connection.db;
    const articles = db.collection("articles");
    const [totalArticles, recentArticles] = await Promise.all([
      articles.countDocuments({}),
      articles.countDocuments(recentArticleMatch()),
    ]);

    res.json({
      ok: true,
      status: "ok",
      database: {
        connected: mongoose.connection.readyState === 1,
        articles: totalArticles,
        total_all: totalArticles,
        recent_articles: recentArticles,
        market_window_start: latestMarketCloseCutoff().toISOString()
      },
      time: new Date().toISOString()
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      status: "error",
      database: {
        connected: false,
        articles: 0
      },
      error: "Failed to load status"
    });
  }
});

// Duplicate /api/market/status removed - see line 323 for the authoritative implementation

app.get("/api/stats", async (req, res) => {
  try {
    const db = mongoose.connection.db;
    res.json(await loadArticleStats(db, Number(req.query.days || req.query.recent_days || 0)));
  } catch (err) {
    const trackedTickers = loadTrackedTickers()
    res.status(500).json({
      total: 0,
      total_recent: 0,
      total_all: 0,
      sources: [],
      categories: [],
      sentiment: { bullish: 0, bearish: 0, neutral: 0, unknown: 0 },
      ticker_mentions: [],
      tracked_market_count: TRACKED_MARKETS.length,
      tracked_markets: TRACKED_MARKETS,
      tracked_exchanges: Array.from(US_EXCHANGES),
      tracked_indices: TRACKED_MARKET_INDICES,
      market_universe_label: "NASDAQ / NYSE / AMEX equities plus major US index markets",
      tracked_ticker_count: trackedTickers.length,
      tracked_tickers: trackedTickers,
      error: "Failed to load stats"
    });
  }
});

// Duplicate /api/keywords removed - see settings routes for the authoritative implementation

async function runPythonScript(scriptPath, {
  timeout = 180000,
  extraEnv = {},
} = {}) {
  const { execFile } = await import("node:child_process")
  const { existsSync } = await import("node:fs")

  const pythonPath = existsSync("/opt/rssvenv/bin/python")
    ? "/opt/rssvenv/bin/python"
    : "python3"

  if (!existsSync(scriptPath)) {
    return {
      ok: false,
      skipped: true,
      stdout: "",
      stderr: "",
      error: `Script not found at ${scriptPath}`,
    }
  }

  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || "mongodb://mongo:27017/feedflash"
  const started = Date.now()

  try {
    const result = await new Promise((resolve, reject) => {
      execFile(
        pythonPath,
        [scriptPath],
        {
          cwd: process.cwd(),
          timeout,
          maxBuffer: 1024 * 1024 * 20,
          env: {
            ...process.env,
            MONGODB_URI: mongoUri,
            MONGO_URI: mongoUri,
            MONGO_DB: "feedflash",
            MONGODB_DB: "feedflash",
            RSS_COOLDOWN_SECONDS: "0",
            RSS_STATE_FILE: "/tmp/feedflash_rss_fetch_state.json",
            ...extraEnv,
          },
        },
        (error, stdout, stderr) => {
          if (error) {
            error.stdout = stdout
            error.stderr = stderr
            reject(error)
            return
          }
          resolve({ stdout, stderr })
        }
      )
    })

    return {
      ok: true,
      stdout: String(result.stdout || ""),
      stderr: String(result.stderr || ""),
      ms: Date.now() - started,
    }
  } catch (err) {
    return {
      ok: false,
      stdout: String(err?.stdout || ""),
      stderr: String(err?.stderr || ""),
      error: String(err?.message || err),
      ms: Date.now() - started,
    }
  }
}

function skippedPythonResult(name, reason = "skipped in fast mode") {
  return {
    ok: true,
    skipped: true,
    stdout: `${name} skipped — ${reason}`,
    stderr: "",
    error: "",
    ms: 0,
  }
}

function parseStructuredFetch(stdout, before, after) {
  const match =
    stdout.match(/RSS Mongo import complete\s+—\s+(\d+)\s+new,\s+(\d+)\s+updated,\s+(\d+)\s+unchanged/i) ||
    stdout.match(/RSS Mongo import complete.*?(\d+)\s+new.*?(\d+)\s+updated.*?(\d+)\s+unchanged/is)

  return {
    new_articles: match ? Number(match[1]) : Math.max(0, after - before),
    updated_articles: match ? Number(match[2]) : 0,
    unchanged_articles: match ? Number(match[3]) : 0,
  }
}

function parseUnstructuredFetch(stdout) {
  const found = stdout.match(/['"]found['"]:\s*(\d+)/)
  const upserted = stdout.match(/['"]upserted['"]:\s*(\d+)/)
  const modified = stdout.match(/['"]modified['"]:\s*(\d+)/)
  return {
    unstructured_found: found ? Number(found[1]) : 0,
    unstructured_new: upserted ? Number(upserted[1]) : 0,
    unstructured_updated: modified ? Number(modified[1]) : 0,
  }
}

function parseSocialFetch(stdout) {
  const match = stdout.match(/Social import complete\s+—\s+(\d+)\s+found,\s+(\d+)\s+new,\s+(\d+)\s+updated/i)
  return {
    social_found: match ? Number(match[1]) : 0,
    social_new: match ? Number(match[2]) : 0,
    social_updated: match ? Number(match[3]) : 0,
  }
}

function parseQuoteFetch(stdout) {
  const match = stdout.match(/Quote import complete\s+—\s+(\d+)\s+quotes,\s+(\d+)\s+updated/i)
  return {
    quotes_found: match ? Number(match[1]) : 0,
    quotes_updated: match ? Number(match[2]) : 0,
  }
}

function parseFinvizEliteFetch(stdout) {
  const match = stdout.match(/Finviz Elite import complete\s+—\s+(\d+)\s+rows,\s+(\d+)\s+updated,\s+(\d+)\s+dropped/i)
  return {
    finviz_rows: match ? Number(match[1]) : 0,
    finviz_updated: match ? Number(match[2]) : 0,
    finviz_dropped: match ? Number(match[3]) : 0,
  }
}

function parseTradingViewFetch(stdout) {
  const match = stdout.match(/TradingView import complete\s+—\s+(\d+)\s+found,\s+(\d+)\s+new,\s+(\d+)\s+updated/i)
  return {
    tradingview_found: match ? Number(match[1]) : 0,
    tradingview_new: match ? Number(match[2]) : 0,
    tradingview_updated: match ? Number(match[3]) : 0,
  }
}

function parseTradingViewScreenerFetch(stdout) {
  const match = stdout.match(/TradingView screener import complete\s+—\s+(\d+)\s+rows,\s+(\d+)\s+updated/i)
  return {
    tradingview_screener_rows: match ? Number(match[1]) : 0,
    tradingview_screener_updated: match ? Number(match[2]) : 0,
  }
}

function parseBenzingaFetch(stdout) {
  const match = stdout.match(/Benzinga import complete\s+—\s+(\d+)\s+found,\s+(\d+)\s+new,\s+(\d+)\s+updated/i)
  return {
    benzinga_found: match ? Number(match[1]) : 0,
    benzinga_new: match ? Number(match[2]) : 0,
    benzinga_updated: match ? Number(match[3]) : 0,
  }
}

async function runDataRefreshCycle(db, { socialMode = "top_momentum", mode = "fast" } = {}) {
  const refreshMode = String(mode || process.env.DEFAULT_FETCH_MODE || "fast").toLowerCase() === "full" ? "full" : "fast"
  const fastMode = refreshMode !== "full"
  const beforeArticles = await db.collection("articles").countDocuments()
  const beforeSocial = await db.collection("socials").countDocuments()
  const socialExtraEnv = {
    SOCIAL_TICKER_SOURCE: "momentum",
    SOCIAL_MOMENTUM_LIMIT: process.env.SOCIAL_MOMENTUM_LIMIT || "10",
    SOCIAL_MAX_TICKERS: process.env.SOCIAL_MAX_TICKERS || "10",
    SOCIAL_MAX_WORKERS: process.env.SOCIAL_MAX_WORKERS || "8",
    SOCIAL_REDDIT_TIMEOUT: process.env.SOCIAL_REDDIT_TIMEOUT || "4",
    SOCIAL_REDDIT_PUBLIC_FALLBACK: process.env.SOCIAL_REDDIT_PUBLIC_FALLBACK || "false",
  }

  const [finvizElite, tradingViewScreener] = await Promise.all([
    runPythonScript("2_Screener/pipeline/fetch_finviz_elite_to_mongo.py", {
      timeout: fastMode ? 30000 : 90000,
      extraEnv: {
        FINVIZ_MAX_WORKERS: process.env.FINVIZ_MAX_WORKERS || (fastMode ? "10" : "6"),
      },
    }),
    fastMode
      ? Promise.resolve(skippedPythonResult("TradingView numeric screener"))
      : runPythonScript("2_Screener/pipeline/fetch_tradingview_screener_to_mongo.py", {
        timeout: 90000,
      }),
  ])

  const trackedMarketTickers = fastMode
    ? []
    : await loadTrackedMarketTickerSymbols(db, Number(process.env.TRACKED_MARKET_TICKER_LIMIT || 5000))
  let socialTickers = []
  let publicSocialTickers = []
  if (socialMode === "top_momentum") {
    publicSocialTickers = await loadTopMomentumTickerSymbols(db, Number(process.env.SOCIAL_MOMENTUM_LIMIT || (fastMode ? 12 : 10)))
    socialTickers = withPrivateSocialTickers(publicSocialTickers)
    if (socialTickers.length) {
      socialExtraEnv.SOCIAL_TICKERS = socialTickers.join(",")
      socialExtraEnv.SOCIAL_MAX_TICKERS = String(socialTickers.length)
      socialExtraEnv.SOCIAL_PRIVATE_TICKERS = Array.from(PRIVATE_TRACKED_TICKERS).join(",")
      socialExtraEnv.SOCIAL_TICKER_SOURCE = "configured"
    } else {
      socialExtraEnv.SOCIAL_MAX_TICKERS = "10"
    }
  } else {
    socialExtraEnv.SOCIAL_TICKER_SOURCE = "configured"
    socialExtraEnv.SOCIAL_MAX_TICKERS = process.env.SOCIAL_MAX_TICKERS || "250"
  }

  const tradingViewExtraEnv = {}
  if (publicSocialTickers.length) {
    tradingViewExtraEnv.TRADINGVIEW_TICKERS = publicSocialTickers.join(",")
    tradingViewExtraEnv.TRADINGVIEW_MAX_TICKERS = String(publicSocialTickers.length)
  }
  const quoteTickers = fastMode ? publicSocialTickers : trackedMarketTickers
  const quoteExtraEnv = quoteTickers.length
    ? {
      QUOTE_TICKERS: quoteTickers.join(","),
      QUOTE_MAX_TICKERS: String(quoteTickers.length),
    }
    : { QUOTE_MAX_TICKERS: fastMode ? "25" : (process.env.QUOTE_MAX_TICKERS || "5000") }

  const [quotes, structured, tradingView, benzinga, ibkrNews, schwabSignals, unstructured, social] = await Promise.all([
    runPythonScript("1_News/pipeline/fetch_quotes_to_mongo.py", {
      timeout: fastMode ? 25000 : 90000,
      extraEnv: quoteExtraEnv,
    }),
    runPythonScript("1_News/pipeline/fetch_rss_to_mongo.py", {
      timeout: fastMode ? 25000 : 180000,
      extraEnv: fastMode
        ? { RSS_FAST_MODE: "1", RSS_MAX_WORKERS: process.env.RSS_MAX_WORKERS || "24", RSS_HTTP_TIMEOUT: process.env.RSS_HTTP_TIMEOUT || "7" }
        : { RSS_MAX_WORKERS: process.env.RSS_MAX_WORKERS || "16" },
    }),
    runPythonScript("1_News/pipeline/fetch_tradingview_to_mongo.py", {
      timeout: fastMode ? 30000 : 90000,
      extraEnv: tradingViewExtraEnv,
    }),
    fastMode && !process.env.BENZINGA_API_KEY
      ? Promise.resolve(skippedPythonResult("Benzinga", "no API key and fast mode"))
      : runPythonScript("1_News/pipeline/fetch_benzinga_to_mongo.py", {
        timeout: fastMode ? 30000 : 90000,
      }),
    fastMode
      ? Promise.resolve(skippedPythonResult("IBKR News"))
      : runPythonScript("1_News/pipeline/fetch_ibkr_news_to_mongo.py", {
        timeout: 30000,
      }),
    fastMode
      ? Promise.resolve(skippedPythonResult("Schwab signals"))
      : runPythonScript("2_Screener/pipeline/fetch_schwab_signals_to_mongo.py", {
        timeout: 30000,
      }),
    fastMode
      ? Promise.resolve(skippedPythonResult("Unstructured public source sweep"))
      : runPythonScript("1_News/pipeline/fetch_unstructured_news_titles_to_mongo.py", {
        timeout: 90000,
        extraEnv: {
          UNSTRUCTURED_MAX_PER_SOURCE: process.env.UNSTRUCTURED_MAX_PER_SOURCE || "10",
          ...(trackedMarketTickers.length ? { TRACKED_TICKERS: trackedMarketTickers.join(",") } : {}),
        },
      }),
    runPythonScript("1_News/pipeline/fetch_social_to_mongo.py", {
      timeout: fastMode ? 25000 : 90000,
      extraEnv: socialExtraEnv,
    }),
  ])

  const afterStructuredArticles = await db.collection("articles").countDocuments()
  const structuredCounts = parseStructuredFetch(structured.stdout || "", beforeArticles, afterStructuredArticles)
  const afterArticles = await db.collection("articles").countDocuments()
  const afterSocial = await db.collection("socials").countDocuments()
  const unstructuredCounts = parseUnstructuredFetch(unstructured.stdout || "")
  const socialCounts = parseSocialFetch(social.stdout || "")
  const quoteCounts = parseQuoteFetch(quotes.stdout || "")
  const finvizCounts = parseFinvizEliteFetch(finvizElite.stdout || "")
  const tradingViewCounts = parseTradingViewFetch(tradingView.stdout || "")
  const tradingViewScreenerCounts = parseTradingViewScreenerFetch(tradingViewScreener.stdout || "")
  const benzingaCounts = parseBenzingaFetch(benzinga.stdout || "")
  const predictionLabels = await labelMaturePredictionSignals(db)
  const predictionModel = await trainPredictionModel(db, {
    minSamples: Number(process.env.PREDICTION_MIN_TRAINING_SAMPLES || 20),
  })
  const predictionSnapshot = await captureTradeWatchPredictionSignals(db, {
    limit: Number(process.env.PREDICTION_SIGNAL_LIMIT || 10),
    socialWindow: Number(process.env.PREDICTION_SOCIAL_WINDOW || 60),
  })

  return {
    ok: finvizElite.ok && tradingViewScreener.ok && quotes.ok && structured.ok && tradingView.ok && benzinga.ok && ibkrNews.ok && schwabSignals.ok && unstructured.ok && social.ok,
    ...finvizCounts,
    ...tradingViewScreenerCounts,
    ...quoteCounts,
    ...structuredCounts,
    ...tradingViewCounts,
    ...benzingaCounts,
    ...unstructuredCounts,
    ...socialCounts,
    total_articles: afterArticles,
    total_social: afterSocial,
    tracked_market_ticker_count: trackedMarketTickers.length,
    quote_ticker_count: quoteTickers.length,
    fetch_mode: refreshMode,
    social_delta: Math.max(0, afterSocial - beforeSocial),
    prediction_labels_checked: predictionLabels.checked,
    prediction_labels_added: predictionLabels.labeled,
    prediction_signals_saved: predictionSnapshot.saved,
    prediction_model_status: predictionModel.status,
    prediction_model_samples: predictionModel.samples || 0,
    social_mode: socialMode,
    social_target_source: socialMode === "top_momentum" ? "top positive momentum movers" : "configured watchlist",
    social_tickers: socialTickers,
    timings: {
      finviz_ms: finvizElite.ms || 0,
      tradingview_screener_ms: tradingViewScreener.ms || 0,
      quotes_ms: quotes.ms || 0,
      structured_ms: structured.ms || 0,
      tradingview_news_ms: tradingView.ms || 0,
      benzinga_ms: benzinga.ms || 0,
      ibkr_ms: ibkrNews.ms || 0,
      schwab_ms: schwabSignals.ms || 0,
      unstructured_ms: unstructured.ms || 0,
      social_ms: social.ms || 0,
    },
    output: [
      structured.stdout,
      finvizElite.stdout,
      tradingViewScreener.stdout,
      tradingView.stdout,
      benzinga.stdout,
      ibkrNews.stdout,
      schwabSignals.stdout,
      unstructured.stdout,
      social.stdout,
      quotes.stdout,
    ].filter(Boolean).join("\n").slice(-6000),
    stderr: [
      structured.stderr,
      finvizElite.stderr,
      tradingViewScreener.stderr,
      tradingView.stderr,
      benzinga.stderr,
      ibkrNews.stderr,
      schwabSignals.stderr,
      unstructured.stderr,
      social.stderr,
      quotes.stderr,
    ].filter(Boolean).join("\n").slice(-3000),
    errors: [
      finvizElite.ok ? null : finvizElite.error,
      tradingViewScreener.ok ? null : tradingViewScreener.error,
      quotes.ok ? null : quotes.error,
      structured.ok ? null : structured.error,
      tradingView.ok ? null : tradingView.error,
      benzinga.ok ? null : benzinga.error,
      ibkrNews.ok ? null : ibkrNews.error,
      schwabSignals.ok ? null : schwabSignals.error,
      unstructured.ok ? null : unstructured.error,
      social.ok ? null : social.error,
    ].filter(Boolean),
  }
}

app.post("/api/fetch", async (req, res) => {
  const started = Date.now()

  try {
    const db = mongoose.connection.db
    if (!db) {
      return res.status(503).json({
        ok: false,
        error: "MongoDB is not connected",
        new_articles: 0,
        ms: Date.now() - started,
      })
    }

    const result = await runDataRefreshCycle(db, {
      mode: req.query.mode || req.body?.mode || process.env.DEFAULT_FETCH_MODE || "fast",
    })
    return res.json({
      ...result,
      ms: Date.now() - started,
      message: result.fetch_mode === "full"
        ? "Ran full structured, unstructured, and social importers"
        : "Ran fast trader refresh",
    })
  } catch (err) {
    console.error("Real /api/fetch failed:", err)
    return res.status(500).json({
      ok: false,
      error: String(err?.message || err),
      new_articles: 0,
      ms: Date.now() - started,
      stdout: String(err?.stdout || "").slice(-3000),
      stderr: String(err?.stderr || "").slice(-3000),
    })
  }
})
// NEWS_RSS_FETCH_API_V3_END

app.get("/api/watch", async (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");

  const interval = 60;

  res.write(`event: start\n`);
  res.write(`data: ${JSON.stringify({ message: `Auto-watch started. Interval: ${interval}s. Social auto-fetch targets the top 10 positive momentum movers.` })}\n\n`);

  let isRunning = false;

  const runFetchCycle = async () => {
    if (isRunning) return; // Prevent overlapping cycles
    isRunning = true;
    
    const cycleStarted = Date.now();
    try {
      const db = mongoose.connection.db;
      const result = await runDataRefreshCycle(db, {
        socialMode: "top_momentum",
        mode: req.query.mode || "fast",
      })
      const newCount = Number(result.new_articles || 0) + Number(result.unstructured_new || 0)
      const updatedCount = Number(result.updated_articles || 0) + Number(result.unstructured_updated || 0)
      const tradingViewNew = Number(result.tradingview_new || 0)
      const tradingViewUpdated = Number(result.tradingview_updated || 0)
      const socialNew = Number(result.social_new || 0)
      const socialUpdated = Number(result.social_updated || 0)
      const quotesUpdated = Number(result.quotes_updated || 0)
      const trackedMarketTickerCount = Number(result.tracked_market_ticker_count || 0)
      const finvizRows = Number(result.finviz_rows || 0)
      const tradingViewScreenerRows = Number(result.tradingview_screener_rows || 0)
      const ms = Date.now() - cycleStarted;

      res.write(`event: line\n`);
      res.write(`data: ${JSON.stringify({ 
        text: `${finvizRows} Finviz movers; ${tradingViewScreenerRows} TV scanner rows; ${trackedMarketTickerCount || 'all'} tracked market tickers; ${quotesUpdated} quotes; +${newCount} articles${updatedCount > 0 ? `, ${updatedCount} refreshed` : ''}; +${tradingViewNew} TradingView news${tradingViewUpdated > 0 ? `, ${tradingViewUpdated} refreshed` : ''}; +${socialNew} social${socialUpdated > 0 ? `, ${socialUpdated} refreshed` : ''}${result.social_tickers?.length ? ` [${result.social_tickers.join(', ')}]` : ''} (${(ms / 1000).toFixed(1)}s)`,
        new: newCount + tradingViewNew,
        updated: updatedCount + tradingViewUpdated,
        tradingview_new: tradingViewNew,
        tradingview_updated: tradingViewUpdated,
        social_new: socialNew,
        social_updated: socialUpdated,
        social_tickers: result.social_tickers || [],
        finviz_rows: finvizRows,
        tradingview_screener_rows: tradingViewScreenerRows,
        tracked_market_ticker_count: trackedMarketTickerCount,
        quotes_updated: quotesUpdated,
        ms: ms
      })}\n\n`);
    } catch (err) {
      console.error("Auto-watch cycle failed:", err);
      res.write(`event: error\n`);
      res.write(`data: ${JSON.stringify({ message: `Auto-watch cycle failed: ${err.message}` })}\n\n`);
    } finally {
      isRunning = false;
    }
  };

  // Run first cycle immediately, then schedule for every interval
  await runFetchCycle();
  
  const timer = setInterval(runFetchCycle, interval * 1000);

  req.on("close", () => {
    clearInterval(timer);
  });
});
// End Ryan frontend compatibility endpoints



app.get("/api/sources/health", async (req, res) => {
  try {
    const db = mongoose.connection.db;
    const registryPath = path.join(PROJECT_ROOT, "config", "professor_source_registry.json")
    const registry = JSON.parse(fs.readFileSync(registryPath, "utf8"))
    const statuses = await db.collection("source_status").find({}).toArray()
    const statusBySource = new Map(statuses.map((row) => [row.source, row]))

    const sourceAliases = {
      "TradingView News Flow": ["TradingView News Flow", "TradingView"],
      "TradingView News": ["TradingView News", "TradingView"],
      "GlobeNewswire Public Companies": ["GlobeNewswire Public Companies", "GlobeNewswire"],
      "ACCESS Newswire": ["ACCESS Newswire", "AccessWire"],
      "BusinessWire": ["BusinessWire", "Business Wire"],
      "Schwab News": ["Schwab News", "Charles Schwab", "TD Ameritrade"],
      "X/Twitter": ["X/Twitter", "Twitter", "X"],
    }
    const screenerSources = {
      "Finviz Elite Screener": { quote_source: "finviz_elite_screener" },
      "TradingView Numeric Screener": { quote_source: "tradingview_numeric_screener" },
      "Schwab Movers": { source: "Schwab Movers" },
    }

    async function countSource(entry) {
      const aliases = sourceAliases[entry.source] || [entry.source]
      if (entry.collection === "articles") {
        const pattern = aliases.map((s) => escapeRegExp(s)).join("|")
        const query = { source: { $regex: pattern, $options: "i" } }
        const [count, latest] = await Promise.all([
          db.collection("articles").countDocuments(query),
          db.collection("articles").find(query).sort({ fetched_date: -1, detected_at: -1, publish_date: -1 }).limit(1).project({ fetched_date: 1, detected_at: 1, publish_date: 1 }).next(),
        ])
        return {
          count,
          latest_fetch: latest?.fetched_date || latest?.detected_at || null,
          latest_publish: latest?.publish_date || null,
        }
      }
      if (entry.collection === "screeners") {
        const query = screenerSources[entry.source] || { source: entry.source }
        const [count, latest] = await Promise.all([
          db.collection("screeners").countDocuments(query),
          db.collection("screeners").find(query).sort({ quote_updated_at: -1, finviz_seen_at: -1, tradingview_seen_at: -1 }).limit(1).project({ quote_updated_at: 1, finviz_seen_at: 1, tradingview_seen_at: 1 }).next(),
        ])
        return {
          count,
          latest_fetch: latest?.quote_updated_at || latest?.finviz_seen_at || latest?.tradingview_seen_at || null,
          latest_publish: null,
        }
      }
      if (entry.collection === "socials") {
        const aliasesLower = aliases.map((s) => s.toLowerCase())
        const query = {
          $or: [
            { platform: { $in: aliasesLower } },
            { platform: { $in: aliases } },
            { source: { $in: aliases } },
          ],
        }
        const [count, latest] = await Promise.all([
          db.collection("socials").countDocuments(query),
          db.collection("socials").find(query).sort({ fetched_date: -1, detected_at: -1, createdAt: -1 }).limit(1).project({ fetched_date: 1, detected_at: 1, createdAt: 1 }).next(),
        ])
        return {
          count,
          latest_fetch: latest?.fetched_date || latest?.detected_at || latest?.createdAt || null,
          latest_publish: null,
        }
      }
      return { count: 0, latest_fetch: null, latest_publish: null }
    }

    const sources = []
    for (const entry of registry) {
      const counted = await countSource(entry)
      const liveStatus = statusBySource.get(entry.source)
      const envValue = entry.env_var ? String(process.env[entry.env_var] || "").trim() : ""
      const hasRequiredEnv = !entry.env_var || (Boolean(envValue) && !["0", "false", "no"].includes(envValue.toLowerCase()))
      const requiresMissingCredential = Boolean(entry.auth_required && entry.env_var && !hasRequiredEnv)
      let status = liveStatus?.status || entry.status || "unknown"
      if (requiresMissingCredential && counted.count === 0 && !["broker_api_pending", "licensed_feed_required", "planned"].includes(entry.status)) {
        status = "api_key_required"
      } else if (!liveStatus && counted.count === 0 && String(status).startsWith("working")) {
        status = "ready_no_rows_yet"
      } else if (counted.count > 0 && !["planned", "licensed_feed_required", "broker_api_pending"].includes(status)) {
        status = status.startsWith("working") ? status : "working"
      }

      sources.push({
        ...entry,
        status,
        configured: hasRequiredEnv,
        count: counted.count,
        latest_fetch: counted.latest_fetch,
        latest_publish: counted.latest_publish,
        last_checked_at: liveStatus?.last_checked_at || null,
        detail: liveStatus?.detail || "",
      })
    }

    const working = sources.filter((row) => row.count > 0 || (String(row.status).startsWith("working") && row.last_checked_at))
    const ready = sources.filter((row) => row.status === "ready_no_rows_yet")
    const blocked = sources.filter((row) => row.count === 0 && !String(row.status).startsWith("working") && row.status !== "planned" && row.status !== "ready_no_rows_yet")

    res.json({
      working_count: working.length,
      ready_count: ready.length,
      blocked_count: blocked.length,
      planned_count: sources.filter((row) => row.status === "planned").length,
      sources,
    });
  } catch (err) {
    res.status(500).json({ error: "Failed to load source health", detail: err.message });
  }
});


// FEEDFLASH_SETTINGS_KEYWORDS_SOURCES_PATCH_V1

function settingsDb() {
  const d = mongoose.connection.db
  if (!d) throw new Error('MongoDB connection is not ready')
  return d
}

const DEFAULT_SIGNAL_KEYWORDS = [
  ["earnings", "fundamental"],
  ["ipo", "fundamental"],
  ["listing", "fundamental"],
  ["delisting", "fundamental"],
  ["dividend", "fundamental"],
  ["merger", "fundamental"],
  ["acquisition", "fundamental"],
  ["buyout", "fundamental"],
  ["contract", "fundamental"],
  ["partnership", "fundamental"],
  ["fda approval", "regulatory"],
  ["fda rejection", "regulatory"],
  ["clinical trial", "regulatory"],
  ["sec filing", "regulatory"],
  ["short squeeze", "momentum"],
  ["price target", "analyst"],
  ["downgrade", "analyst"],
  ["upgrade", "analyst"],
  ["beat estimates", "fundamental"],
  ["miss estimates", "fundamental"],
  ["guidance", "fundamental"],
  ["recall", "regulatory"],
  ["bankruptcy", "fundamental"],
  ["layoffs", "fundamental"],
  ["restructuring", "fundamental"]
];

async function seedDefaultKeywordsIfEmpty() {
  const keywords = settingsDb().collection("keywords");
  const count = await keywords.countDocuments();
  if (count > 0) return;

  await keywords.insertMany(DEFAULT_SIGNAL_KEYWORDS.map(([keyword, category]) => ({
    keyword,
    word: keyword,
    category,
    enabled: true,
    active: true,
    hits: 0,
    created_at: Math.floor(Date.now() / 1000),
    updated_at: Math.floor(Date.now() / 1000)
  })));
}

function cleanSettingText(v) {
  return String(v || "").trim();
}

function cleanKeyword(v) {
  return cleanSettingText(v).toLowerCase();
}

const DEFAULT_CONNECTION_SETTINGS = {
  finviz: {
    label: "Finviz Elite",
    url: process.env.FINVIZ_URL || "https://elite.finviz.com/screener",
    token: process.env.FINVIZ_TOKEN || "",
    login: "",
  },
  tradingview: {
    label: "TradingView",
    url: process.env.TRADINGVIEW_URL || "https://www.tradingview.com",
    token: process.env.TRADINGVIEW_TOKEN || "",
    login: process.env.TRADINGVIEW_LOGIN || "",
  },
  td_ameritrade: {
    label: "TD Ameritrade / Schwab",
    url: process.env.TD_URL || process.env.SCHWAB_URL || "",
    token: process.env.TD_TOKEN || process.env.SCHWAB_TOKEN || "",
    login: process.env.TD_LOGIN || process.env.SCHWAB_LOGIN || "",
  },
  interactive_brokers: {
    label: "Interactive Brokers",
    url: process.env.IB_URL || "",
    token: process.env.IB_TOKEN || "",
    login: process.env.IB_LOGIN || "",
  },
};

function cleanConnectionPayload(value = {}) {
  const out = {};
  for (const [key, defaults] of Object.entries(DEFAULT_CONNECTION_SETTINGS)) {
    const row = value[key] || {};
    out[key] = {
      label: defaults.label,
      url: cleanSettingText(row.url ?? defaults.url),
      token: cleanSettingText(row.token ?? defaults.token),
      login: cleanSettingText(row.login ?? defaults.login),
    };
  }
  return out;
}

app.get("/api/settings/connections", async (req, res) => {
  try {
    const row = await settingsDb().collection("app_settings").findOne({ key: "connections" });
    res.json({
      ok: true,
      connections: cleanConnectionPayload(row?.value || {}),
    });
  } catch (err) {
    console.error("GET /api/settings/connections failed:", err);
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

app.patch("/api/settings/connections", async (req, res) => {
  try {
    const connections = cleanConnectionPayload(req.body.connections || req.body || {});
    await settingsDb().collection("app_settings").updateOne(
      { key: "connections" },
      {
        $set: {
          key: "connections",
          value: connections,
          updated_at: Math.floor(Date.now() / 1000),
        },
        $setOnInsert: { created_at: Math.floor(Date.now() / 1000) },
      },
      { upsert: true }
    );
    res.json({ ok: true, connections });
  } catch (err) {
    console.error("PATCH /api/settings/connections failed:", err);
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

app.get("/api/keywords", async (req, res) => {
  try {
    await seedDefaultKeywordsIfEmpty();
    const rows = await settingsDb().collection("keywords")
      .find({})
      .sort({ enabled: -1, category: 1, keyword: 1, word: 1 })
      .toArray();

    res.json({
      ok: true,
      keywords: rows.map(r => ({
        id: String(r._id),
        keyword: r.keyword || r.word,
        word: r.word || r.keyword,
        category: r.category || "custom",
        enabled: r.enabled !== false && r.active !== false,
        active: r.enabled !== false && r.active !== false,
        hits: r.hits || 0
      }))
    });
  } catch (err) {
    console.error("GET /api/keywords failed:", err);
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

app.post("/api/keywords", async (req, res) => {
  try {
    const keyword = cleanKeyword(req.body.keyword || req.body.word);
    const category = cleanSettingText(req.body.category || "custom").toLowerCase();

    if (!keyword) return res.status(400).json({ ok: false, error: "keyword is required" });

    const now = Math.floor(Date.now() / 1000);
    await settingsDb().collection("keywords").updateOne(
      { keyword },
      {
        $set: { keyword, word: keyword, category, enabled: true, active: true, updated_at: now },
        $setOnInsert: { hits: 0, created_at: now }
      },
      { upsert: true }
    );

    res.json({ ok: true, keyword, category });
  } catch (err) {
    console.error("POST /api/keywords failed:", err);
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

app.patch("/api/keywords/:keyword", async (req, res) => {
  try {
    const keyword = cleanKeyword(decodeURIComponent(req.params.keyword));
    const enabled = req.body.enabled !== false && req.body.active !== false;
    const result = await settingsDb().collection("keywords").updateOne(
      { $or: [{ keyword }, { word: keyword }] },
      { $set: { enabled, active: enabled, updated_at: Math.floor(Date.now() / 1000) } }
    );
    res.json({ ok: true, matched: result.matchedCount, modified: result.modifiedCount });
  } catch (err) {
    console.error("PATCH /api/keywords/:keyword failed:", err);
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

app.delete("/api/keywords/:keyword", async (req, res) => {
  try {
    const keyword = cleanKeyword(decodeURIComponent(req.params.keyword));
    const result = await settingsDb().collection("keywords").deleteOne({ $or: [{ keyword }, { word: keyword }] });
    res.json({ ok: true, deleted: result.deletedCount });
  } catch (err) {
    console.error("DELETE /api/keywords/:keyword failed:", err);
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

const PROFESSOR_STRUCTURED_SOURCES = [
  { source: "PR Newswire", status: "public_feed", method: "rss", editable: false },
  { source: "GlobeNewswire", status: "public_feed", method: "rss", editable: false },
  { source: "SEC EDGAR", status: "public_api", method: "official_sec_atom", editable: false },
  { source: "FDA", status: "public_feed", method: "official_fda_rss", editable: false },
  { source: "Business Wire", status: "valid_rss_channel_required", method: "official_businesswire_rss_or_media_partner_feed", editable: false },
  { source: "ACCESS Newswire / AccessWire", status: "public_endpoint", method: "accessnewswire_newsroom_json", editable: false },
  { source: "Benzinga", status: "api_key_required", method: "official_benzinga_stock_news_api", editable: false },
  { source: "Dow Jones Newswires", status: "contract_required", method: "licensed_api", editable: false },
  { source: "TradingView News Flow", status: "public_endpoint", method: "news_mediator_symbol_endpoint", editable: false },
  { source: "Interactive Brokers News", status: "broker_api_required", method: "broker_api", editable: false },
  { source: "Charles Schwab / TD Ameritrade News", status: "broker_api_required", method: "broker_api", editable: false }
];

async function countArticlesForSourceLabel(label) {
  const parts = label.split("/").map(s => s.trim()).filter(Boolean);
  const pattern = parts.length ? parts.join("|") : label;
  return settingsDb().collection("articles").countDocuments({ source: new RegExp(pattern, "i") });
}

app.get("/api/settings/sources", async (req, res) => {
  try {
    const custom = await settingsDb().collection("rss_sources")
      .find({})
      .sort({ enabled: -1, name: 1 })
      .toArray();

    const structured = [];
    for (const s of PROFESSOR_STRUCTURED_SOURCES) {
      structured.push({
        ...s,
        count: await countArticlesForSourceLabel(s.source)
      });
    }

    res.json({
      ok: true,
      structured,
      custom_rss_sources: custom.map(s => ({
        id: String(s._id),
        name: s.name,
        source: s.name,
        url: s.url,
        category: s.category || "custom",
        enabled: s.enabled !== false,
        status: s.enabled === false ? "disabled" : "enabled",
        editable: true
      }))
    });
  } catch (err) {
    console.error("GET /api/settings/sources failed:", err);
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

app.post("/api/settings/sources", async (req, res) => {
  try {
    const name = cleanSettingText(req.body.name || req.body.source);
    const url = cleanSettingText(req.body.url);
    const category = cleanSettingText(req.body.category || "custom").toLowerCase();

    if (!name || !url) return res.status(400).json({ ok: false, error: "name and url are required" });
    if (!/^https?:\/\//i.test(url)) return res.status(400).json({ ok: false, error: "url must start with http:// or https://" });

    const now = Math.floor(Date.now() / 1000);
    await settingsDb().collection("rss_sources").updateOne(
      { name },
      {
        $set: { name, url, category, enabled: true, updated_at: now },
        $setOnInsert: { created_at: now }
      },
      { upsert: true }
    );

    res.json({ ok: true, name, url, category });
  } catch (err) {
    console.error("POST /api/settings/sources failed:", err);
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

app.patch("/api/settings/sources/:name", async (req, res) => {
  try {
    const name = cleanSettingText(decodeURIComponent(req.params.name));
    const enabled = req.body.enabled !== false;
    const result = await settingsDb().collection("rss_sources").updateOne(
      { name },
      { $set: { enabled, updated_at: Math.floor(Date.now() / 1000) } }
    );
    res.json({ ok: true, matched: result.matchedCount, modified: result.modifiedCount });
  } catch (err) {
    console.error("PATCH /api/settings/sources/:name failed:", err);
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

app.delete("/api/settings/sources/:name", async (req, res) => {
  try {
    const name = cleanSettingText(decodeURIComponent(req.params.name));
    const result = await settingsDb().collection("rss_sources").deleteOne({ name });
    res.json({ ok: true, deleted: result.deletedCount });
  } catch (err) {
    console.error("DELETE /api/settings/sources/:name failed:", err);
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});


// FEEDFLASH_SETTINGS_KEYWORDS_ALIAS_PATCH_V1
app.get('/api/settings/keywords', async (req, res) => {
  try {
    await seedDefaultKeywordsIfEmpty()

    const rows = await settingsDb().collection('keywords')
      .find({})
      .sort({ enabled: -1, category: 1, keyword: 1, word: 1 })
      .toArray()

    res.json({
      ok: true,
      keywords: rows.map(r => ({
        id: String(r._id),
        keyword: r.keyword || r.word,
        word: r.word || r.keyword,
        category: r.category || 'custom',
        enabled: r.enabled !== false && r.active !== false,
        active: r.enabled !== false && r.active !== false,
        hits: r.hits || 0
      }))
    })
  } catch (err) {
    console.error('GET /api/settings/keywords failed:', err)
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.post('/api/settings/keywords', async (req, res) => {
  try {
    const keyword = cleanKeyword(req.body?.keyword || req.body?.word)
    const category = cleanSettingText(req.body?.category || 'custom').toLowerCase()

    if (!keyword) return res.status(400).json({ ok: false, error: 'keyword is required' })

    const now = Math.floor(Date.now() / 1000)
    await settingsDb().collection('keywords').updateOne(
      { keyword },
      {
        $set: { keyword, word: keyword, category, enabled: true, active: true, updated_at: now },
        $setOnInsert: { hits: 0, created_at: now }
      },
      { upsert: true }
    )

    res.json({ ok: true, keyword, category })
  } catch (err) {
    console.error('POST /api/settings/keywords failed:', err)
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.patch('/api/settings/keywords/:keyword', async (req, res) => {
  try {
    const keyword = cleanKeyword(decodeURIComponent(req.params.keyword))
    const enabled = req.body?.enabled !== false && req.body?.active !== false

    const result = await settingsDb().collection('keywords').updateOne(
      { $or: [{ keyword }, { word: keyword }] },
      { $set: { enabled, active: enabled, updated_at: Math.floor(Date.now() / 1000) } }
    )

    res.json({ ok: true, matched: result.matchedCount, modified: result.modifiedCount })
  } catch (err) {
    console.error('PATCH /api/settings/keywords failed:', err)
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.delete('/api/settings/keywords/:keyword', async (req, res) => {
  try {
    const keyword = cleanKeyword(decodeURIComponent(req.params.keyword))

    const result = await settingsDb().collection('keywords').deleteOne({
      $or: [{ keyword }, { word: keyword }]
    })

    res.json({ ok: true, deleted: result.deletedCount })
  } catch (err) {
    console.error('DELETE /api/settings/keywords failed:', err)
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

// ── DISK STORAGE ─────────────────────────────────────────────────────────────

const DISK_TTL_KEY = 'disk_ttl_days'
const DISK_ARCHIVE_COLLECTION = 'disk_archive'
const DEFAULT_DISK_TTL_DAYS = 7

async function getDiskTtlDays() {
  try {
    const db = mongoose.connection.db
    if (!db) return DEFAULT_DISK_TTL_DAYS
    const row = await db.collection('app_settings').findOne({ key: DISK_TTL_KEY })
    return Number(row?.value ?? DEFAULT_DISK_TTL_DAYS)
  } catch { return DEFAULT_DISK_TTL_DAYS }
}

async function ensureDiskTtlIndex(db, ttlDays) {
  try {
    const col = db.collection(DISK_ARCHIVE_COLLECTION)
    await col.createIndex({ expires_at: 1 }, { expireAfterSeconds: 0 })
  } catch (_) {}
}

app.get('/api/disk/status', async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, error: 'MongoDB not connected' })
    const ttlDays = await getDiskTtlDays()
    const col = db.collection(DISK_ARCHIVE_COLLECTION)
    const total = await col.countDocuments()
    const snapshots = await col.find({}, { projection: { articles_count: 1, social_count: 1, saved_at: 1, expires_at: 1, top_tickers: 1, sentiment: 1 } })
      .sort({ saved_at: -1 }).limit(20).toArray()
    const lastSave = snapshots[0]?.saved_at ?? null
    const articleTotal = await db.collection('articles').countDocuments()
    const socialTotal = await db.collection('socials').countDocuments()
    res.json({ ok: true, ttl_days: ttlDays, snapshot_count: total, snapshots, last_save: lastSave, article_count: articleTotal, social_count: socialTotal })
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.post('/api/disk/save', async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, error: 'MongoDB not connected' })
    const ttlDays = await getDiskTtlDays()
    await ensureDiskTtlIndex(db, ttlDays)
    const now = new Date()
    const expiresAt = new Date(now.getTime() + ttlDays * 86400 * 1000)
    const sinceSec = Math.floor(now.getTime() / 1000) - 2 * 86400
    const [articleCount, socialCount, tickerAgg, sentimentAgg] = await Promise.all([
      db.collection('articles').countDocuments(),
      db.collection('socials').countDocuments(),
      db.collection('articles').aggregate([
        { $match: { ticker: { $exists: true, $ne: '' }, detected_at: { $gte: sinceSec } } },
        { $group: { _id: '$ticker', count: { $sum: 1 } } },
        { $sort: { count: -1 } }, { $limit: 10 }
      ]).toArray(),
      db.collection('articles').aggregate([
        { $match: { sentiment: { $in: ['bullish', 'bearish', 'neutral'] } } },
        { $group: { _id: '$sentiment', count: { $sum: 1 } } }
      ]).toArray(),
    ])
    const sentimentMap = Object.fromEntries(sentimentAgg.map(r => [r._id, r.count]))
    const snapshot = {
      saved_at: now,
      expires_at: expiresAt,
      ttl_days: ttlDays,
      articles_count: articleCount,
      social_count: socialCount,
      top_tickers: tickerAgg.map(r => r._id),
      sentiment: { bullish: sentimentMap.bullish ?? 0, bearish: sentimentMap.bearish ?? 0, neutral: sentimentMap.neutral ?? 0 },
    }
    const result = await db.collection(DISK_ARCHIVE_COLLECTION).insertOne(snapshot)
    res.json({ ok: true, snapshot_id: result.insertedId, saved_at: now, expires_at: expiresAt, articles_count: articleCount, social_count: socialCount, ttl_days: ttlDays })
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.delete('/api/disk/clear', async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, error: 'MongoDB not connected' })
    const result = await db.collection(DISK_ARCHIVE_COLLECTION).deleteMany({})
    res.json({ ok: true, deleted: result.deletedCount })
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.get('/api/disk/settings', async (req, res) => {
  try {
    const ttlDays = await getDiskTtlDays()
    res.json({ ok: true, ttl_days: ttlDays })
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.patch('/api/disk/settings', async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, error: 'MongoDB not connected' })
    const ttlDays = Math.min(90, Math.max(1, Number(req.body.ttl_days ?? 3)))
    await db.collection('app_settings').updateOne(
      { key: DISK_TTL_KEY },
      { $set: { key: DISK_TTL_KEY, value: ttlDays, updated_at: new Date() } },
      { upsert: true }
    )
    await ensureDiskTtlIndex(db, ttlDays)
    res.json({ ok: true, ttl_days: ttlDays })
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

// ── GROK (xAI) API PROXY ──────────────────────────────────────────────────────

const GROK_API_KEY = process.env.GROK_API_KEY || ''
const GROK_BASE_URL = 'https://api.x.ai/v1'
const GROK_MODEL = process.env.GROK_MODEL || 'grok-3'

app.post('/api/grok/analyze', async (req, res) => {
  if (!GROK_API_KEY) {
    return res.status(503).json({ ok: false, error: 'GROK_API_KEY not configured. Add it to .env to enable AI analysis.' })
  }
  const { ticker, context, prompt } = req.body || {}
  if (!ticker) return res.status(400).json({ ok: false, error: 'ticker required' })

  const systemMsg = `You are a concise financial analyst. Analyze the provided stock data and news for ${ticker}. Be direct, factual, and highlight key signals. Max 150 words.`
  const userMsg = prompt || `Analyze ${ticker}. Context: ${context || 'No additional context.'}`

  try {
    const resp = await fetch(`${GROK_BASE_URL}/chat/completions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${GROK_API_KEY}` },
      body: JSON.stringify({
        model: GROK_MODEL,
        messages: [{ role: 'system', content: systemMsg }, { role: 'user', content: userMsg }],
        max_tokens: 250,
        temperature: 0.3,
      }),
    })
    if (!resp.ok) {
      const err = await resp.text()
      return res.status(resp.status).json({ ok: false, error: `Grok API error ${resp.status}: ${err.slice(0, 200)}` })
    }
    const data = await resp.json()
    const text = data.choices?.[0]?.message?.content || ''
    res.json({ ok: true, ticker, analysis: text, model: GROK_MODEL })
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.get('/api/grok/status', (req, res) => {
  res.json({ configured: !!GROK_API_KEY, model: GROK_MODEL })
})

// ─────────────────────────────────────────────────────────────────────────────

app.listen(PORT, () => {
    console.log()
    console.log('  ⚡ FlashFeed API')
    console.log('  ─────────────────────────────────────')
    console.log('  Server  →  http://localhost:' + PORT)
    console.log('  Health  →  http://localhost:' + PORT + '/api/health')
    console.log('  Docs    →  README-MONGODB.md')
    console.log()
  })
}

start()
