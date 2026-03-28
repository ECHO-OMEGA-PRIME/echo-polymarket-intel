import { Hono } from 'hono';
import { cors } from 'hono/cors';

interface Env {
  DB: D1Database;
  CACHE: KVNamespace;
  TELEGRAM: Fetcher;
  SHARED_BRAIN: Fetcher;
  ANALYTICS: AnalyticsEngineDataset;
  ECHO_API_KEY: string;
}

const app = new Hono<{ Bindings: Env }>();

const log = (level: string, msg: string, data?: Record<string, unknown>) =>
  console.log(JSON.stringify({ ts: new Date().toISOString(), level, worker: 'echo-polymarket-intel', msg, ...data }));

// ── In-memory rate limiting (120 reads / 30 writes per IP per minute) ──
const RL_MAP = new Map<string, { c: number; t: number }>();
function isRateLimited(ip: string, isWrite: boolean): boolean {
  const limit = isWrite ? 30 : 120;
  const key = `${ip}:${isWrite ? 'w' : 'r'}`;
  const now = Date.now();
  const entry = RL_MAP.get(key);
  if (!entry || now - entry.t > 60000) {
    RL_MAP.set(key, { c: 1, t: now });
    if (RL_MAP.size > 5000) { const first = RL_MAP.keys().next().value; if (first) RL_MAP.delete(first); }
    return false;
  }
  entry.c++;
  return entry.c > limit;
}

// ── Auth middleware ──────────────────────────────────────────────────────────
function authRequired(c: any, next: any) {
  const key = c.req.header('X-Echo-API-Key') || c.req.query('key');
  if (!key || key !== c.env.ECHO_API_KEY) {
    return c.json({ error: 'Unauthorized' }, 401);
  }
  return next();
}

app.use('*', cors({ origin: ['https://echo-op.com', 'https://echo-prime-tech.vercel.app', 'https://echo-prime.tech'], credentials: true }));
// Rate limiting middleware
app.use('*', async (c, next) => {
  const path = new URL(c.req.url).pathname;
  if (path === '/health' || c.req.method === 'OPTIONS') return next();
  const ip = c.req.header('cf-connecting-ip') || 'unknown';
  const isWrite = ['POST', 'PUT', 'PATCH', 'DELETE'].includes(c.req.method);
  if (isRateLimited(ip, isWrite)) {
    log('warn', 'Rate limited', { ip, method: c.req.method, path });
    return c.json({ error: 'Rate limited' }, 429);
  }
  return next();
});
// Security headers middleware
app.use('*', async (c, next) => {
  await next();
  c.res.headers.set('X-Content-Type-Options', 'nosniff');
  c.res.headers.set('X-Frame-Options', 'DENY');
  c.res.headers.set('X-XSS-Protection', '1; mode=block');
  c.res.headers.set('Referrer-Policy', 'strict-origin-when-cross-origin');
  c.res.headers.set('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
});


// ── Root ──────────────────────────────────────────────────────────────────
app.get('/', (c) => c.json({
  service: 'echo-polymarket-intel',
  version: '1.1.0',
  status: 'operational',
  description: 'Prediction Markets Intelligence — track 113+ markets, watchlist alerts, crude oil correlation, AI analysis',
  endpoints: { health: '/health', dashboard: '/dashboard', markets: '/markets', alerts: '/alerts', oil: '/oil', stats: '/stats' },
}));

// ── Health ──────────────────────────────────────────────────────────────────
app.get('/health', async (c) => {
  try {
    const [markets, watchlist, alerts, oil] = await Promise.all([
      c.env.DB.prepare('SELECT COUNT(*) as cnt FROM markets').first<{ cnt: number }>(),
      c.env.DB.prepare('SELECT COUNT(*) as cnt FROM markets WHERE watchlist = 1').first<{ cnt: number }>(),
      c.env.DB.prepare("SELECT COUNT(*) as cnt FROM alerts WHERE created_at > datetime('now', '-24 hours')").first<{ cnt: number }>(),
      c.env.DB.prepare("SELECT COUNT(*) as cnt FROM crude_oil_prices WHERE recorded_at > datetime('now', '-24 hours')").first<{ cnt: number }>(),
    ]);
    return c.json({ status: 'healthy', worker: 'echo-polymarket-intel', version: '1.1.0', markets: markets?.cnt || 0, watchlist: watchlist?.cnt || 0, alerts_24h: alerts?.cnt || 0, oil_points: oil?.cnt || 0, timestamp: new Date().toISOString() });
  } catch (e) {
    return c.json({ status: 'degraded', error: String(e) }, 500);
  }
});

// ── Polymarket API helpers ──────────────────────────────────────────────────
const GAMMA_BASE = 'https://gamma-api.polymarket.com';

async function fetchPolymarkets(limit = 100, offset = 0): Promise<any[]> {
  const url = `${GAMMA_BASE}/markets?limit=${limit}&offset=${offset}&active=true&closed=false&order=volume24hr&ascending=false`;
  const res = await fetch(url, { headers: { 'User-Agent': 'EchoPolymarketIntel/1.0' } });
  if (!res.ok) throw new Error(`Polymarket API ${res.status}: ${await res.text()}`);
  return res.json();
}

async function fetchCrudeOilPrice(): Promise<{ wti: number; brent: number; wti_change: number; brent_change: number } | null> {
  // Source 1: Yahoo Finance v8 spark API (free, no auth, reliable)
  try {
    const symbols = 'CL=F,BZ=F'; // WTI and Brent futures
    const res = await fetch(`https://query1.finance.yahoo.com/v8/finance/spark?symbols=${symbols}&range=1d&interval=1d`, {
      headers: { 'User-Agent': 'Mozilla/5.0' },
    });
    if (res.ok) {
      const data: any = await res.json();
      let wti = 0, brent = 0, wtiChange = 0, brentChange = 0;
      // Yahoo returns: { "CL=F": { close: [price], chartPreviousClose: prevPrice } }
      for (const sym of ['CL=F', 'BZ=F']) {
        const entry = data?.[sym];
        if (!entry) continue;
        const close = Array.isArray(entry.close) ? entry.close[entry.close.length - 1] : (entry.close || 0);
        const prevClose = entry.chartPreviousClose || entry.previousClose || close;
        const changePct = prevClose > 0 ? ((close - prevClose) / prevClose) * 100 : 0;
        if (sym === 'CL=F') { wti = close; wtiChange = changePct; }
        if (sym === 'BZ=F') { brent = close; brentChange = changePct; }
      }
      if (wti > 0 || brent > 0) {
        log('info', 'Oil prices from Yahoo Finance', { wti, brent, wtiChange, brentChange });
        return { wti, brent, wti_change: wtiChange, brent_change: brentChange };
      }
    }
  } catch (e) {
    log('warn', 'Yahoo Finance API failed, trying fallback', { error: String(e) });
  }

  // Source 2: Yahoo Finance v7 quote endpoint (alternate)
  try {
    const res = await fetch('https://query1.finance.yahoo.com/v7/finance/quote?symbols=CL=F,BZ=F&fields=regularMarketPrice,regularMarketChangePercent', {
      headers: { 'User-Agent': 'Mozilla/5.0' },
    });
    if (res.ok) {
      const data: any = await res.json();
      const quotes = data?.quoteResponse?.result || [];
      let wti = 0, brent = 0, wtiChange = 0, brentChange = 0;
      for (const q of quotes) {
        if (q.symbol === 'CL=F') { wti = q.regularMarketPrice || 0; wtiChange = q.regularMarketChangePercent || 0; }
        if (q.symbol === 'BZ=F') { brent = q.regularMarketPrice || 0; brentChange = q.regularMarketChangePercent || 0; }
      }
      if (wti > 0 || brent > 0) return { wti, brent, wti_change: wtiChange, brent_change: brentChange };
    }
  } catch (e) {
    log('warn', 'Yahoo v7 fallback also failed', { error: String(e) });
  }

  // Source 3: Marketstack free tier
  try {
    const res = await fetch('https://api.marketstack.com/v1/eod/latest?access_key=free&symbols=CL.COMM,BZ.COMM', {
      headers: { 'User-Agent': 'EchoPolymarketIntel/1.0' },
    });
    if (res.ok) {
      const data: any = await res.json();
      if (data?.data?.length) {
        let wti = 0, brent = 0;
        for (const d of data.data) {
          if (d.symbol?.includes('CL')) wti = d.close || 0;
          if (d.symbol?.includes('BZ')) brent = d.close || 0;
        }
        if (wti > 0 || brent > 0) return { wti, brent, wti_change: 0, brent_change: 0 };
      }
    }
  } catch (e) {
    log('warn', 'All oil price sources failed', { error: String(e) });
  }

  return null;
}

// ── Watchlist matching ──────────────────────────────────────────────────────
async function matchWatchlist(db: D1Database, question: string): Promise<{ matched: boolean; tags: string[] }> {
  const rules = await db.prepare('SELECT keyword, tag FROM watchlist_rules WHERE active = 1').all<{ keyword: string; tag: string }>();
  const tags: string[] = [];
  const qLower = question.toLowerCase();
  for (const rule of rules.results || []) {
    const kw = rule.keyword.toLowerCase();
    // Use word boundary matching to avoid "Oilers" matching "oil"
    const regex = new RegExp(`\\b${kw.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
    if (regex.test(qLower)) {
      if (!tags.includes(rule.tag)) tags.push(rule.tag);
    }
  }
  return { matched: tags.length > 0, tags };
}

// ── Alert detection ─────────────────────────────────────────────────────────
interface PriceShift {
  market_id: string;
  question: string;
  outcome_index: number;
  old_price: number;
  new_price: number;
  shift_pct: number;
  tags: string[];
}

async function detectShifts(db: D1Database, market: any, currentPrices: number[], tags: string[]): Promise<PriceShift[]> {
  const shifts: PriceShift[] = [];
  const thresholds = { watchlist: 0.05, normal: 0.10 }; // 5% for watchlist, 10% for others
  const isWatchlist = tags.length > 0;
  const threshold = isWatchlist ? thresholds.watchlist : thresholds.normal;

  for (let i = 0; i < currentPrices.length; i++) {
    // Get the last recorded price for this outcome
    const lastPrice = await db.prepare(
      'SELECT price FROM price_history WHERE market_id = ? AND outcome_index = ? ORDER BY recorded_at DESC LIMIT 1'
    ).bind(market.id, i).first<{ price: number }>();

    if (lastPrice && lastPrice.price > 0) {
      const shift = Math.abs(currentPrices[i] - lastPrice.price) / lastPrice.price;
      if (shift >= threshold) {
        shifts.push({
          market_id: market.id,
          question: market.question,
          outcome_index: i,
          old_price: lastPrice.price,
          new_price: currentPrices[i],
          shift_pct: shift * 100,
          tags,
        });
      }
    }
  }
  return shifts;
}

// ── Telegram notification ───────────────────────────────────────────────────
async function notifyTelegram(env: Env, message: string): Promise<void> {
  try {
    await env.TELEGRAM.fetch('https://telegram/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        channel: 'polymarket-intel',
        message,
        parse_mode: 'HTML',
      }),
    });
  } catch (e) {
    log('error', 'Telegram notification failed', { error: String(e) });
  }
}

// ── Shared Brain feed ───────────────────────────────────────────────────────
async function feedSharedBrain(env: Env, content: string, importance: number = 6): Promise<void> {
  try {
    await env.SHARED_BRAIN.fetch('https://brain/ingest', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        content,
        source: 'echo-polymarket-intel',
        importance,
        tags: ['polymarket', 'intelligence', 'prediction-markets'],
      }),
    });
  } catch (e) {
    log('error', 'Shared Brain feed failed', { error: String(e) });
  }
}

// ── Analytics ───────────────────────────────────────────────────────────────
function track(env: Env, event: string, data?: Record<string, number>) {
  try {
    env.ANALYTICS?.writeDataPoint({
      blobs: ['echo-polymarket-intel', event],
      doubles: data ? Object.values(data) : [],
      indexes: ['echo-polymarket-intel'],
    });
  } catch (e) {
    // Analytics is non-critical
  }
}

// ══════════════════════════════════════════════════════════════════════════════
// CRON HANDLER — Main polling loop
// ══════════════════════════════════════════════════════════════════════════════
async function cronHandler(env: Env): Promise<void> {
  log('info', 'Cron triggered — polling Polymarket');
  const startTime = Date.now();
  let marketsProcessed = 0;
  let alertsCreated = 0;
  let watchlistMatches = 0;

  try {
    // Fetch top markets by 24h volume
    const markets = await fetchPolymarkets(100);
    log('info', `Fetched ${markets.length} markets from Polymarket`);

    // Process each market
    for (const market of markets) {
      if (!market.id || !market.question || !market.outcomePrices) continue;

      // Polymarket returns these as JSON strings, not arrays
      let rawPrices: string[] = [];
      if (typeof market.outcomePrices === 'string') {
        try { rawPrices = JSON.parse(market.outcomePrices); } catch { continue; }
      } else if (Array.isArray(market.outcomePrices)) {
        rawPrices = market.outcomePrices;
      }
      const prices: number[] = rawPrices.map((p: string) => parseFloat(p) || 0);

      let outcomes: string[] = ['Yes', 'No'];
      if (typeof market.outcomes === 'string') {
        try { outcomes = JSON.parse(market.outcomes); } catch { /* keep default */ }
      } else if (Array.isArray(market.outcomes)) {
        outcomes = market.outcomes;
      }

      // Check watchlist
      const { matched, tags } = await matchWatchlist(env.DB, market.question);
      if (matched) watchlistMatches++;

      // Detect shifts before updating
      const shifts = await detectShifts(env.DB, market, prices, tags);

      // Upsert market
      await env.DB.prepare(`
        INSERT INTO markets (id, question, slug, category, end_date, outcomes, current_prices, volume, volume_24hr, liquidity, best_bid, best_ask, last_trade_price, watchlist, watchlist_tags, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(id) DO UPDATE SET
          current_prices = excluded.current_prices,
          volume = excluded.volume,
          volume_24hr = excluded.volume_24hr,
          liquidity = excluded.liquidity,
          best_bid = excluded.best_bid,
          best_ask = excluded.best_ask,
          last_trade_price = excluded.last_trade_price,
          watchlist = excluded.watchlist,
          watchlist_tags = excluded.watchlist_tags,
          last_updated = datetime('now')
      `).bind(
        market.id,
        market.question,
        market.slug || null,
        market.category || null,
        market.endDate || null,
        JSON.stringify(outcomes),
        JSON.stringify(prices),
        market.volume || 0,
        market.volume24hr || 0,
        market.liquidity || 0,
        market.bestBid != null ? parseFloat(market.bestBid) : null,
        market.bestAsk != null ? parseFloat(market.bestAsk) : null,
        market.lastTradePrice != null ? parseFloat(market.lastTradePrice) : null,
        matched ? 1 : 0,
        matched ? JSON.stringify(tags) : null,
      ).run();

      // Record price history for all outcomes
      const historyStmts = prices.map((price, i) =>
        env.DB.prepare(
          'INSERT INTO price_history (market_id, outcome_index, price, volume, volume_24hr) VALUES (?, ?, ?, ?, ?)'
        ).bind(market.id, i, price, market.volume || 0, market.volume24hr || 0)
      );
      if (historyStmts.length > 0) {
        await env.DB.batch(historyStmts);
      }

      // Process alerts for shifts
      for (const shift of shifts) {
        const direction = shift.new_price > shift.old_price ? '📈' : '📉';
        const severity = shift.shift_pct >= 15 ? 'critical' : shift.shift_pct >= 8 ? 'warning' : 'info';
        const outcomeName = outcomes[shift.outcome_index] || `Outcome ${shift.outcome_index}`;

        const message = `${direction} <b>${shift.question}</b>\n` +
          `Outcome: ${outcomeName}\n` +
          `${(shift.old_price * 100).toFixed(1)}% → ${(shift.new_price * 100).toFixed(1)}% (${shift.shift_pct >= 0 ? '+' : ''}${shift.shift_pct.toFixed(1)}%)\n` +
          `Tags: ${shift.tags.join(', ') || 'general'}`;

        await env.DB.prepare(
          'INSERT INTO alerts (market_id, alert_type, severity, message, old_price, new_price, shift_pct, metadata) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
        ).bind(
          shift.market_id, 'shift', severity, message,
          shift.old_price, shift.new_price, shift.shift_pct,
          JSON.stringify({ outcome_index: shift.outcome_index, tags: shift.tags })
        ).run();
        alertsCreated++;

        // Notify on warning+ severity
        if (severity === 'warning' || severity === 'critical') {
          await notifyTelegram(env, `🔮 POLYMARKET INTEL\n${message}`);
          await feedSharedBrain(env, `Polymarket shift: ${shift.question} — ${outcomeName} moved ${shift.shift_pct.toFixed(1)}% (${(shift.old_price * 100).toFixed(1)}% → ${(shift.new_price * 100).toFixed(1)}%). Tags: ${shift.tags.join(', ')}`, severity === 'critical' ? 8 : 6);
        }
      }

      marketsProcessed++;
    }

    // ── Crude oil price tracking ──────────────────────────────────────────
    const oilPrices = await fetchCrudeOilPrice();
    if (oilPrices) {
      const stmts = [];
      if (oilPrices.wti > 0) {
        stmts.push(env.DB.prepare(
          'INSERT INTO crude_oil_prices (source, price, change_pct) VALUES (?, ?, ?)'
        ).bind('wti', oilPrices.wti, oilPrices.wti_change));
      }
      if (oilPrices.brent > 0) {
        stmts.push(env.DB.prepare(
          'INSERT INTO crude_oil_prices (source, price, change_pct) VALUES (?, ?, ?)'
        ).bind('brent', oilPrices.brent, oilPrices.brent_change));
      }
      if (stmts.length > 0) await env.DB.batch(stmts);
      log('info', 'Crude oil prices recorded', { wti: oilPrices.wti, brent: oilPrices.brent });

      // ── Cross-correlation: oil markets vs crude price ──────────────────
      await computeOilCorrelations(env, oilPrices);
    }

    const elapsed = Date.now() - startTime;
    log('info', 'Cron complete', { marketsProcessed, alertsCreated, watchlistMatches, elapsed_ms: elapsed });
    track(env, 'cron_complete', { markets: marketsProcessed, alerts: alertsCreated, watchlist: watchlistMatches, elapsed });

    // Prune old price history (keep 7 days)
    await env.DB.prepare("DELETE FROM price_history WHERE recorded_at < datetime('now', '-7 days')").run();
    await env.DB.prepare("DELETE FROM crude_oil_prices WHERE recorded_at < datetime('now', '-30 days')").run();

  } catch (e) {
    log('error', 'Cron failed', { error: String(e) });
    track(env, 'cron_error');
  }
}

// ── Oil correlation engine ──────────────────────────────────────────────────
async function computeOilCorrelations(env: Env, currentOil: { wti: number; brent: number }): Promise<void> {
  // Get watchlist markets tagged with 'oil' or 'geopolitical'
  const oilMarkets = await env.DB.prepare(
    "SELECT id, question, current_prices, watchlist_tags FROM markets WHERE watchlist = 1 AND (watchlist_tags LIKE '%oil%' OR watchlist_tags LIKE '%geopolitical%')"
  ).all<{ id: string; question: string; current_prices: string; watchlist_tags: string }>();

  if (!oilMarkets.results?.length) return;

  // Get recent oil price trend (last 24 data points = ~4 hours at 10min intervals)
  const oilHistory = await env.DB.prepare(
    "SELECT price FROM crude_oil_prices WHERE source = 'wti' ORDER BY recorded_at DESC LIMIT 24"
  ).all<{ price: number }>();

  if (!oilHistory.results?.length || oilHistory.results.length < 3) return;

  const oilPrices = oilHistory.results.map(r => r.price).reverse();
  const oilTrend = oilPrices[oilPrices.length - 1] - oilPrices[0]; // positive = rising

  for (const market of oilMarkets.results) {
    // Get this market's Yes-price history (same timeframe)
    const marketHistory = await env.DB.prepare(
      "SELECT price FROM price_history WHERE market_id = ? AND outcome_index = 0 ORDER BY recorded_at DESC LIMIT 24"
    ).all<{ price: number }>();

    if (!marketHistory.results?.length || marketHistory.results.length < 3) continue;

    const marketPrices = marketHistory.results.map(r => r.price).reverse();
    const corr = pearsonCorrelation(oilPrices.slice(0, marketPrices.length), marketPrices);

    let signal = 'neutral';
    if (corr > 0.5 && oilTrend > 0) signal = 'bullish';
    else if (corr > 0.5 && oilTrend < 0) signal = 'bearish';
    else if (corr < -0.5 && oilTrend > 0) signal = 'bearish';
    else if (corr < -0.5 && oilTrend < 0) signal = 'bullish';

    const analysis = `Correlation: ${corr.toFixed(3)} | Oil trend: ${oilTrend > 0 ? 'UP' : 'DOWN'} ($${Math.abs(oilTrend).toFixed(2)}) | Signal: ${signal.toUpperCase()}`;

    await env.DB.prepare(
      'INSERT INTO correlations (market_id, crude_source, correlation_score, signal, analysis) VALUES (?, ?, ?, ?, ?)'
    ).bind(market.id, 'wti', corr, signal, analysis).run();

    // Alert on strong signals
    if (Math.abs(corr) > 0.7 && signal !== 'neutral') {
      const prices = JSON.parse(market.current_prices) as number[];
      const msg = `🛢️ <b>OIL-MARKET CORRELATION</b>\n` +
        `Market: ${market.question}\n` +
        `Yes: ${(prices[0] * 100).toFixed(1)}% | Oil: $${currentOil.wti.toFixed(2)}\n` +
        `Correlation: ${corr.toFixed(3)} | Signal: <b>${signal.toUpperCase()}</b>\n` +
        `${analysis}`;

      await env.DB.prepare(
        "INSERT INTO alerts (market_id, alert_type, severity, message, metadata) VALUES (?, 'oil_correlation', 'warning', ?, ?)"
      ).bind(market.id, msg, JSON.stringify({ correlation: corr, signal, oil_wti: currentOil.wti })).run();

      await notifyTelegram(env, msg);
      await feedSharedBrain(env, `Oil-market correlation detected: ${market.question} (corr=${corr.toFixed(3)}, signal=${signal}, WTI=$${currentOil.wti.toFixed(2)})`, 7);
    }
  }
}

function pearsonCorrelation(x: number[], y: number[]): number {
  const n = Math.min(x.length, y.length);
  if (n < 3) return 0;
  const xs = x.slice(0, n);
  const ys = y.slice(0, n);
  const meanX = xs.reduce((a, b) => a + b, 0) / n;
  const meanY = ys.reduce((a, b) => a + b, 0) / n;
  let num = 0, denX = 0, denY = 0;
  for (let i = 0; i < n; i++) {
    const dx = xs[i] - meanX;
    const dy = ys[i] - meanY;
    num += dx * dy;
    denX += dx * dx;
    denY += dy * dy;
  }
  const den = Math.sqrt(denX * denY);
  return den === 0 ? 0 : num / den;
}

// ══════════════════════════════════════════════════════════════════════════════
// API ROUTES
// ══════════════════════════════════════════════════════════════════════════════

// ── Dashboard — public summary ──────────────────────────────────────────────
app.get('/dashboard', async (c) => {
  const [watchlist, recentAlerts, oilPrice, stats] = await Promise.all([
    c.env.DB.prepare(
      "SELECT id, question, current_prices, outcomes, volume_24hr, watchlist_tags, last_updated FROM markets WHERE watchlist = 1 ORDER BY volume_24hr DESC LIMIT 25"
    ).all(),
    c.env.DB.prepare(
      "SELECT market_id, alert_type, severity, message, shift_pct, created_at FROM alerts ORDER BY created_at DESC LIMIT 20"
    ).all(),
    c.env.DB.prepare(
      "SELECT source, price, change_pct, recorded_at FROM crude_oil_prices ORDER BY recorded_at DESC LIMIT 2"
    ).all(),
    c.env.DB.prepare(
      "SELECT (SELECT COUNT(*) FROM markets) as total_markets, (SELECT COUNT(*) FROM markets WHERE watchlist=1) as watchlist_count, (SELECT COUNT(*) FROM alerts WHERE created_at > datetime('now','-24 hours')) as alerts_24h, (SELECT COUNT(*) FROM price_history WHERE recorded_at > datetime('now','-24 hours')) as datapoints_24h"
    ).first(),
  ]);

  // Parse prices for display
  const watchlistMarkets = (watchlist.results || []).map((m: any) => ({
    ...m,
    current_prices: JSON.parse(m.current_prices),
    outcomes: JSON.parse(m.outcomes),
    watchlist_tags: m.watchlist_tags ? JSON.parse(m.watchlist_tags) : [],
  }));

  return c.json({
    watchlist: watchlistMarkets,
    recent_alerts: recentAlerts.results || [],
    crude_oil: oilPrice.results || [],
    stats: stats || {},
    timestamp: new Date().toISOString(),
  });
});

// ── Markets list ────────────────────────────────────────────────────────────
app.get('/markets', async (c) => {
  const watchlistOnly = c.req.query('watchlist') === '1';
  const tag = c.req.query('tag');
  const limit = Math.min(parseInt(c.req.query('limit') || '50'), 200);
  const offset = parseInt(c.req.query('offset') || '0');

  let query = 'SELECT * FROM markets';
  const conditions: string[] = [];
  const binds: any[] = [];

  if (watchlistOnly) conditions.push('watchlist = 1');
  if (tag) {
    conditions.push("watchlist_tags LIKE ?");
    binds.push(`%${tag}%`);
  }

  if (conditions.length) query += ' WHERE ' + conditions.join(' AND ');
  query += ' ORDER BY volume_24hr DESC LIMIT ? OFFSET ?';
  binds.push(limit, offset);

  const stmt = c.env.DB.prepare(query);
  const result = await (binds.length ? stmt.bind(...binds) : stmt).all();

  const markets = (result.results || []).map((m: any) => ({
    ...m,
    current_prices: JSON.parse(m.current_prices),
    outcomes: JSON.parse(m.outcomes),
    watchlist_tags: m.watchlist_tags ? JSON.parse(m.watchlist_tags) : [],
  }));

  return c.json({ markets, count: markets.length });
});

// ── Single market detail ────────────────────────────────────────────────────
app.get('/markets/:id', async (c) => {
  const id = c.req.param('id');
  const [market, history, alerts, correlations] = await Promise.all([
    c.env.DB.prepare('SELECT * FROM markets WHERE id = ?').bind(id).first(),
    c.env.DB.prepare(
      'SELECT outcome_index, price, volume, recorded_at FROM price_history WHERE market_id = ? ORDER BY recorded_at DESC LIMIT 200'
    ).bind(id).all(),
    c.env.DB.prepare(
      'SELECT * FROM alerts WHERE market_id = ? ORDER BY created_at DESC LIMIT 20'
    ).bind(id).all(),
    c.env.DB.prepare(
      'SELECT * FROM correlations WHERE market_id = ? ORDER BY calculated_at DESC LIMIT 10'
    ).bind(id).all(),
  ]);

  if (!market) return c.json({ error: 'Market not found' }, 404);

  return c.json({
    market: {
      ...market,
      current_prices: JSON.parse(market.current_prices as string),
      outcomes: JSON.parse(market.outcomes as string),
      watchlist_tags: market.watchlist_tags ? JSON.parse(market.watchlist_tags as string) : [],
    },
    price_history: history.results || [],
    alerts: alerts.results || [],
    correlations: correlations.results || [],
  });
});

// ── Alerts ──────────────────────────────────────────────────────────────────
app.get('/alerts', async (c) => {
  const severity = c.req.query('severity');
  const limit = Math.min(parseInt(c.req.query('limit') || '50'), 200);
  const since = c.req.query('since'); // ISO date

  let query = 'SELECT a.*, m.question FROM alerts a LEFT JOIN markets m ON a.market_id = m.id';
  const conditions: string[] = [];
  const binds: any[] = [];

  if (severity) { conditions.push('a.severity = ?'); binds.push(severity); }
  if (since) { conditions.push('a.created_at >= ?'); binds.push(since); }

  if (conditions.length) query += ' WHERE ' + conditions.join(' AND ');
  query += ' ORDER BY a.created_at DESC LIMIT ?';
  binds.push(limit);

  const result = await c.env.DB.prepare(query).bind(...binds).all();
  return c.json({ alerts: result.results || [], count: result.results?.length || 0 });
});

// ── Crude oil data ──────────────────────────────────────────────────────────
app.get('/oil', async (c) => {
  const days = Math.min(parseInt(c.req.query('days') || '7'), 30);
  const [prices, correlations] = await Promise.all([
    c.env.DB.prepare(
      `SELECT source, price, change_pct, recorded_at FROM crude_oil_prices WHERE recorded_at > datetime('now', '-${days} days') ORDER BY recorded_at DESC`
    ).all(),
    c.env.DB.prepare(
      "SELECT c.*, m.question FROM correlations c LEFT JOIN markets m ON c.market_id = m.id WHERE c.calculated_at > datetime('now', '-24 hours') ORDER BY ABS(c.correlation_score) DESC LIMIT 20"
    ).all(),
  ]);

  return c.json({
    prices: prices.results || [],
    correlations: correlations.results || [],
    count: prices.results?.length || 0,
  });
});

// ── Watchlist management (auth required) ────────────────────────────────────
app.get('/watchlist/rules', authRequired, async (c) => {
  const rules = await c.env.DB.prepare('SELECT * FROM watchlist_rules ORDER BY tag, keyword').all();
  return c.json({ rules: rules.results || [] });
});

app.post('/watchlist/rules', authRequired, async (c) => {
  const { keyword, tag } = await c.req.json<{ keyword: string; tag: string }>();
  if (!keyword || !tag) return c.json({ error: 'keyword and tag required' }, 400);
  await c.env.DB.prepare('INSERT INTO watchlist_rules (keyword, tag) VALUES (?, ?)').bind(keyword, tag).run();
  return c.json({ ok: true, keyword, tag });
});

app.delete('/watchlist/rules/:id', authRequired, async (c) => {
  const id = c.req.param('id');
  await c.env.DB.prepare('DELETE FROM watchlist_rules WHERE id = ?').bind(id).run();
  return c.json({ ok: true });
});

// ── Manual poll trigger (auth required) ─────────────────────────────────────
app.post('/poll', authRequired, async (c) => {
  await cronHandler(c.env);
  return c.json({ ok: true, message: 'Poll completed' });
});

// ── Stats ───────────────────────────────────────────────────────────────────
app.get('/stats', async (c) => {
  const stats = await c.env.DB.prepare(`
    SELECT
      (SELECT COUNT(*) FROM markets) as total_markets,
      (SELECT COUNT(*) FROM markets WHERE watchlist = 1) as watchlist_markets,
      (SELECT COUNT(*) FROM alerts) as total_alerts,
      (SELECT COUNT(*) FROM alerts WHERE created_at > datetime('now', '-24 hours')) as alerts_24h,
      (SELECT COUNT(*) FROM alerts WHERE severity = 'critical') as critical_alerts,
      (SELECT COUNT(*) FROM price_history) as total_datapoints,
      (SELECT COUNT(*) FROM crude_oil_prices) as oil_datapoints,
      (SELECT COUNT(*) FROM correlations) as total_correlations,
      (SELECT MAX(recorded_at) FROM price_history) as last_poll
  `).first();

  return c.json({ stats, timestamp: new Date().toISOString() });
});

// ── Export handler ──────────────────────────────────────────────────────────
export default {
  fetch: app.fetch,
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    ctx.waitUntil(cronHandler(env));
  },
};
