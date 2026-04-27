// db.js — Camada de persistência SQLite (FASE 1: anti-duplicidade Bling↔ML)
const Database = require('better-sqlite3');
const path = require('path');

const DB_PATH = path.join(__dirname, 'dados.sqlite');
const db = new Database(DB_PATH);

// WAL mode pra concorrência segura (writer não bloqueia readers)
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');

// Schema
db.exec(`
  CREATE TABLE IF NOT EXISTS produtos_publicados (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bling_id TEXT NOT NULL,
    mlb_id TEXT NOT NULL UNIQUE,
    titulo TEXT,
    preco REAL,
    publicado_em TEXT NOT NULL,
    status TEXT DEFAULT 'active',
    vendas INTEGER DEFAULT 0,
    cliques INTEGER DEFAULT 0,
    metricas_atualizado_em TEXT,
    criado_em TEXT DEFAULT (datetime('now')),
    atualizado_em TEXT DEFAULT (datetime('now'))
  );

  CREATE INDEX IF NOT EXISTS idx_bling_id ON produtos_publicados(bling_id);
  CREATE INDEX IF NOT EXISTS idx_status ON produtos_publicados(status);
  CREATE INDEX IF NOT EXISTS idx_mlb_id ON produtos_publicados(mlb_id);
`);

module.exports = { db };
