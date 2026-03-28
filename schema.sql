-- Polymarket Intelligence Schema

CREATE TABLE IF NOT EXISTS markets (
  id TEXT PRIMARY KEY,
  question TEXT NOT NULL,
  slug TEXT,
  category TEXT,
  end_date TEXT,
  outcomes TEXT NOT NULL, -- JSON array of outcome labels
  current_prices TEXT NOT NULL, -- JSON array of current prices
  volume REAL DEFAULT 0,
  volume_24hr REAL DEFAULT 0,
  liquidity REAL DEFAULT 0,
  best_bid REAL,
  best_ask REAL,
  last_trade_price REAL,
  watchlist INTEGER DEFAULT 0, -- 1 if on our watchlist
  watchlist_tags TEXT, -- JSON array: oil, geopolitical, crypto, etc.
  first_seen TEXT NOT NULL DEFAULT (datetime('now')),
  last_updated TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS price_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  market_id TEXT NOT NULL,
  outcome_index INTEGER NOT NULL DEFAULT 0,
  price REAL NOT NULL,
  volume REAL,
  volume_24hr REAL,
  recorded_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (market_id) REFERENCES markets(id)
);

CREATE INDEX IF NOT EXISTS idx_price_history_market ON price_history(market_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_price_history_time ON price_history(recorded_at);

CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  market_id TEXT NOT NULL,
  alert_type TEXT NOT NULL, -- shift, spike, watchlist_hit, oil_correlation
  severity TEXT NOT NULL DEFAULT 'info', -- info, warning, critical
  message TEXT NOT NULL,
  old_price REAL,
  new_price REAL,
  shift_pct REAL,
  metadata TEXT, -- JSON: extra context
  notified INTEGER DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (market_id) REFERENCES markets(id)
);

CREATE INDEX IF NOT EXISTS idx_alerts_time ON alerts(created_at);
CREATE INDEX IF NOT EXISTS idx_alerts_unnotified ON alerts(notified, created_at);

CREATE TABLE IF NOT EXISTS crude_oil_prices (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL, -- wti, brent
  price REAL NOT NULL,
  change_pct REAL,
  recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_crude_time ON crude_oil_prices(recorded_at);

CREATE TABLE IF NOT EXISTS correlations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  market_id TEXT NOT NULL,
  crude_source TEXT NOT NULL,
  correlation_score REAL, -- -1 to 1
  signal TEXT, -- bullish, bearish, neutral
  analysis TEXT,
  calculated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (market_id) REFERENCES markets(id)
);

CREATE TABLE IF NOT EXISTS watchlist_rules (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  keyword TEXT NOT NULL,
  tag TEXT NOT NULL, -- oil, geopolitical, crypto, financial, tech
  active INTEGER DEFAULT 1
);

-- Default watchlist rules
INSERT OR IGNORE INTO watchlist_rules (id, keyword, tag) VALUES
  (1, 'oil', 'oil'),
  (2, 'crude', 'oil'),
  (3, 'petroleum', 'oil'),
  (4, 'OPEC', 'oil'),
  (5, 'barrel', 'oil'),
  (6, 'drilling', 'oil'),
  (7, 'pipeline', 'oil'),
  (8, 'energy', 'oil'),
  (9, 'war', 'geopolitical'),
  (10, 'invasion', 'geopolitical'),
  (11, 'sanction', 'geopolitical'),
  (12, 'NATO', 'geopolitical'),
  (13, 'Iran', 'geopolitical'),
  (14, 'Russia', 'geopolitical'),
  (15, 'China', 'geopolitical'),
  (16, 'Taiwan', 'geopolitical'),
  (17, 'Ukraine', 'geopolitical'),
  (18, 'Middle East', 'geopolitical'),
  (19, 'Israel', 'geopolitical'),
  (20, 'Bitcoin', 'crypto'),
  (21, 'Ethereum', 'crypto'),
  (22, 'crypto', 'crypto'),
  (23, 'SEC', 'financial'),
  (24, 'Federal Reserve', 'financial'),
  (25, 'interest rate', 'financial'),
  (26, 'inflation', 'financial'),
  (27, 'recession', 'financial'),
  (28, 'tariff', 'financial'),
  (29, 'trade war', 'financial'),
  (30, 'Trump', 'geopolitical'),
  (31, 'election', 'geopolitical'),
  (32, 'GDP', 'financial'),
  (33, 'natural gas', 'oil'),
  (34, 'Permian', 'oil'),
  (35, 'shale', 'oil'),
  (36, 'refinery', 'oil'),
  (37, 'gasoline', 'oil'),
  (38, 'Saudi', 'oil'),
  (39, 'Venezuela', 'oil'),
  (40, 'nuclear', 'geopolitical');
