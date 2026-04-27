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

// FASE 1.6 - FRENTE D: Tabela de plataformas pendentes
db.exec(`
  CREATE TABLE IF NOT EXISTS plataformas_pendentes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    marca TEXT NOT NULL,
    modelo TEXT NOT NULL,
    ano_inicial INTEGER,
    ano_final INTEGER,
    qtd_produtos INTEGER DEFAULT 1,
    primeiro_visto_em TEXT NOT NULL,
    ultimo_visto_em TEXT NOT NULL,
    titulo_exemplo TEXT,
    mlb_exemplo TEXT,
    revisado INTEGER DEFAULT 0,
    UNIQUE(marca, modelo)
  );
  CREATE INDEX IF NOT EXISTS idx_pendentes_revisado ON plataformas_pendentes(revisado);
  CREATE INDEX IF NOT EXISTS idx_pendentes_qtd ON plataformas_pendentes(qtd_produtos DESC);
`);

const _stmtRegistrarPendente = db.prepare(`
  INSERT INTO plataformas_pendentes
    (marca, modelo, ano_inicial, ano_final, primeiro_visto_em, ultimo_visto_em, titulo_exemplo, mlb_exemplo)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  ON CONFLICT(marca, modelo) DO UPDATE SET
    qtd_produtos = qtd_produtos + 1,
    ultimo_visto_em = excluded.ultimo_visto_em,
    ano_inicial = MIN(ano_inicial, excluded.ano_inicial),
    ano_final = MAX(ano_final, excluded.ano_final)
`);

const _stmtListarPendentes = db.prepare(`
  SELECT * FROM plataformas_pendentes
  WHERE revisado = 0
  ORDER BY qtd_produtos DESC, ultimo_visto_em DESC
`);

const _stmtMarcarRevisado = db.prepare(`
  UPDATE plataformas_pendentes SET revisado = 1 WHERE marca = ? AND modelo = ?
`);

function registrarPendente(marca, modelo, anoIni, anoFim, tituloExemplo, mlbExemplo) {
  const agora = new Date().toISOString();
  return _stmtRegistrarPendente.run(
    marca, modelo, anoIni || null, anoFim || null,
    agora, agora, tituloExemplo || '', mlbExemplo || ''
  );
}

function listarPendentes() {
  return _stmtListarPendentes.all();
}

function marcarRevisado(marca, modelo) {
  return _stmtMarcarRevisado.run(marca, modelo);
}

module.exports = {
  db,
  // FASE 1.6 - Frente D
  registrarPendente,
  listarPendentes,
  marcarRevisado,
};
