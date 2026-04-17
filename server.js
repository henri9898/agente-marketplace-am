/**
 * Agente ML — servidor local
 * Serve o dashboard estático E atua como proxy OAuth2 para a API do Mercado Livre.
 * Sem dependências externas — usa só módulos nativos do Node.
 */
const http = require('http');
const fs   = require('fs');
const path = require('path');
const url  = require('url');
const crypto = require('crypto');

// Porta: em produção (Render, Heroku, etc) vem de process.env.PORT;
// localmente cai pra 3000.
const PORT = parseInt(process.env.PORT, 10) || 3000;
const ROOT = __dirname;
const ENV_FILE = path.join(ROOT, '.env');
const USERS_FILE = path.join(ROOT, 'users.json');

// ---------- usuários (persistência em disco, senha hasheada) ----------
function loadUsersFile(){
  try { return JSON.parse(fs.readFileSync(USERS_FILE,'utf8')); } catch(e){ return []; }
}
function saveUsersFile(users){
  fs.writeFileSync(USERS_FILE, JSON.stringify(users, null, 2));
}
function hashPassword(password, salt){
  const s = salt || crypto.randomBytes(16).toString('hex');
  const h = crypto.scryptSync(password, s, 64).toString('hex');
  return { salt:s, hash:h };
}
function verifyPassword(password, salt, hash){
  try {
    const h = crypto.scryptSync(password, salt, 64).toString('hex');
    return crypto.timingSafeEqual(Buffer.from(h,'hex'), Buffer.from(hash,'hex'));
  } catch(e){ return false; }
}
function readBody(req){
  return new Promise((resolve,reject)=>{
    let data=''; req.on('data',c=>data+=c);
    req.on('end',()=>{ try{ resolve(data?JSON.parse(data):{}); }catch(e){ reject(e); } });
    req.on('error',reject);
  });
}

// ---------- mini dotenv ----------
function loadEnv() {
  if (!fs.existsSync(ENV_FILE)) return {};
  const out = {};
  fs.readFileSync(ENV_FILE, 'utf8').split(/\r?\n/).forEach(line => {
    const m = line.match(/^\s*([A-Z_][A-Z0-9_]*)\s*=\s*(.*)\s*$/);
    if (m) out[m[1]] = m[2].replace(/^["']|["']$/g, '');
  });
  return out;
}
function saveEnv(patch) {
  const env = { ...loadEnv(), ...patch };
  const body = Object.entries(env).map(([k,v]) => `${k}=${v ?? ''}`).join('\n');
  fs.writeFileSync(ENV_FILE, body);
}
let env = loadEnv();

// ============================================================
// PERSISTÊNCIA DE TOKENS (tokens.json) — auto-refresh em server
// Fica lado a lado com o .env (não remove nada, complementa)
// ============================================================
const TOKENS_FILE = path.join(ROOT, 'tokens.json');

function loadTokens() {
  try {
    if (fs.existsSync(TOKENS_FILE)) {
      return JSON.parse(fs.readFileSync(TOKENS_FILE, 'utf8'));
    }
  } catch (e) {
    console.error('[tokens] load error:', e.message);
  }
  return {};
}

function saveTokens(patch) {
  try {
    const existing = loadTokens();
    const updated = { ...existing, ...patch, updated_at: new Date().toISOString() };
    fs.writeFileSync(TOKENS_FILE, JSON.stringify(updated, null, 2));
    // Garante permissão 600 (dono leitura/escrita; outros nada) no Linux
    try { fs.chmodSync(TOKENS_FILE, 0o600); } catch(_){}
    return updated;
  } catch (e) {
    console.error('[tokens] save error:', e.message);
    return null;
  }
}

// Converte payload ML → formato canônico do tokens.json
function persistMLTokens(data) {
  if (!data || !data.access_token) return null;
  const expiresIn = Number(data.expires_in) || 21600; // default 6h
  return saveTokens({
    ml_access_token:     data.access_token,
    ml_refresh_token:    data.refresh_token || loadTokens().ml_refresh_token,
    ml_expires_in:       expiresIn,
    ml_token_expires_at: new Date(Date.now() + expiresIn * 1000).toISOString(),
    ml_user_id:          data.user_id != null ? String(data.user_id) : (loadTokens().ml_user_id || null),
  });
}
function persistBlingTokens(data) {
  if (!data || !data.access_token) return null;
  const expiresIn = Number(data.expires_in) || 21600;
  return saveTokens({
    bling_access_token:     data.access_token,
    bling_refresh_token:    data.refresh_token || loadTokens().bling_refresh_token,
    bling_expires_in:       expiresIn,
    bling_token_expires_at: new Date(Date.now() + expiresIn * 1000).toISOString(),
  });
}

// ---------- refreshMLToken ----------
async function refreshMLToken() {
  const tokens = loadTokens();
  const currentEnv = loadEnv();
  const refresh = tokens.ml_refresh_token || currentEnv.ML_REFRESH_TOKEN || '';
  if (!refresh) {
    console.log('[ml-refresh] sem refresh_token — precisa reconectar via OAuth');
    return null;
  }
  const clientId     = process.env.ML_CLIENT_ID     || currentEnv.ML_CLIENT_ID     || '3688973136843575';
  const clientSecret = process.env.ML_CLIENT_SECRET || currentEnv.ML_CLIENT_SECRET || '';
  if (!clientSecret) {
    console.log('[ml-refresh] ML_CLIENT_SECRET ausente');
    return null;
  }
  try {
    console.log('[ml-refresh] 🔄 renovando token ML...');
    const r = await fetch('https://api.mercadolibre.com/oauth/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json' },
      body: new URLSearchParams({
        grant_type:    'refresh_token',
        client_id:     clientId,
        client_secret: clientSecret,
        refresh_token: refresh,
      }),
    });
    const data = await r.json().catch(() => ({}));
    if (!data.access_token) {
      console.error('[ml-refresh] ❌ falhou:', data.error_description || data.error || data.message || 'sem access_token');
      return null;
    }
    const saved = persistMLTokens(data);
    // Espelha também no .env (compat com o resto do server que lê de .env)
    saveEnv({
      ML_ACCESS_TOKEN:     data.access_token,
      ML_REFRESH_TOKEN:    data.refresh_token || refresh,
      ML_TOKEN_EXPIRES_AT: String(Date.now() + (data.expires_in || 21600) * 1000),
    });
    const hoursLeft = ((data.expires_in || 21600) / 3600).toFixed(1);
    console.log(`[ml-refresh] ✅ ok · expira em ${hoursLeft}h`);
    return saved;
  } catch (err) {
    console.error('[ml-refresh] exceção:', err.message);
    return null;
  }
}

// ---------- refreshBlingToken ----------
async function refreshBlingToken() {
  const tokens = loadTokens();
  const currentEnv = loadEnv();
  const refresh = tokens.bling_refresh_token || '';
  if (!refresh) return null;
  const clientId     = process.env.BLING_CLIENT_ID     || currentEnv.BLING_CLIENT_ID     || '';
  const clientSecret = process.env.BLING_CLIENT_SECRET || currentEnv.BLING_CLIENT_SECRET || '';
  if (!clientId || !clientSecret) {
    console.log('[bling-refresh] credenciais ausentes no .env');
    return null;
  }
  try {
    console.log('[bling-refresh] 🔄 renovando token Bling...');
    const credentials = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
    const r = await fetch('https://www.bling.com.br/Api/v3/oauth/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept':       '1.0',
        'Authorization': 'Basic ' + credentials,
      },
      body: new URLSearchParams({
        grant_type:    'refresh_token',
        refresh_token: refresh,
      }),
    });
    const data = await r.json().catch(() => ({}));
    if (!data.access_token) {
      console.error('[bling-refresh] ❌ falhou:', data.error_description || data.error || 'sem access_token');
      return null;
    }
    const saved = persistBlingTokens(data);
    console.log('[bling-refresh] ✅ ok');
    return saved;
  } catch (err) {
    console.error('[bling-refresh] exceção:', err.message);
    return null;
  }
}

// ---------- Timer: verifica a cada 5h + on-boot ----------
const REFRESH_INTERVAL_MS = 5 * 60 * 60 * 1000; // 5h
async function checkAndRefresh() {
  const tokens = loadTokens();
  const now = Date.now();
  // ML: renova se expira em <1.5h (token dura 6h, margem de 1.5h pra compensar clock skew e erros)
  if (tokens.ml_token_expires_at) {
    const expAt = new Date(tokens.ml_token_expires_at).getTime();
    const hoursLeft = (expAt - now) / 3600000;
    if (hoursLeft < 1.5) {
      console.log(`[ml-refresh] ⏰ token expira em ${hoursLeft.toFixed(2)}h, renovando...`);
      await refreshMLToken();
    }
  }
  // Bling: renova se expira em <0.5h
  if (tokens.bling_token_expires_at) {
    const expAt = new Date(tokens.bling_token_expires_at).getTime();
    const hoursLeft = (expAt - now) / 3600000;
    if (hoursLeft < 0.5) {
      console.log(`[bling-refresh] ⏰ token expira em ${hoursLeft.toFixed(2)}h, renovando...`);
      await refreshBlingToken();
    }
  }
}
setInterval(checkAndRefresh, REFRESH_INTERVAL_MS);

// Migração on-boot: se o .env tem ML_ACCESS_TOKEN mas tokens.json está vazio,
// semeia o tokens.json com os valores do .env (só na primeira execução).
function seedTokensFromEnv() {
  const tokens = loadTokens();
  const e = loadEnv();
  if (!tokens.ml_access_token && e.ML_ACCESS_TOKEN) {
    const expAt = e.ML_TOKEN_EXPIRES_AT ? Number(e.ML_TOKEN_EXPIRES_AT) : (Date.now() + 21600*1000);
    const expiresIn = Math.max(60, Math.floor((expAt - Date.now()) / 1000));
    saveTokens({
      ml_access_token:     e.ML_ACCESS_TOKEN,
      ml_refresh_token:    e.ML_REFRESH_TOKEN || null,
      ml_expires_in:       expiresIn,
      ml_token_expires_at: new Date(expAt).toISOString(),
      ml_user_id:          e.ML_USER_ID || null,
    });
    console.log('[tokens] semeado ML a partir do .env');
  }
}

// Check on-boot: 3s depois do start, valida e refresha se necessário
setTimeout(async () => {
  seedTokensFromEnv();
  const tokens = loadTokens();
  if (tokens.ml_refresh_token) {
    const expAt = new Date(tokens.ml_token_expires_at || 0);
    if (expAt.getTime() < Date.now() + 3600000) { // se já expirou OU expira em <1h
      console.log('[boot] 🔄 renovando ML na inicialização');
      await refreshMLToken();
    } else {
      console.log(`[boot] ✅ token ML válido até ${expAt.toLocaleString()}`);
    }
  }
  if (tokens.bling_refresh_token) {
    const expAt = new Date(tokens.bling_token_expires_at || 0);
    if (expAt.getTime() < Date.now() + 1800000) { // <30 min
      console.log('[boot] 🔄 renovando Bling na inicialização');
      await refreshBlingToken();
    }
  }
}, 3000);

// ---------- helpers ----------
const MIME = {
  '.html':'text/html; charset=utf-8', '.js':'application/javascript',
  '.css':'text/css', '.svg':'image/svg+xml', '.png':'image/png',
  '.ico':'image/x-icon', '.json':'application/json; charset=utf-8',
};
function send(res, status, body, type='application/json; charset=utf-8') {
  res.writeHead(status, { 'Content-Type': type, 'Access-Control-Allow-Origin':'*' });
  res.end(typeof body === 'string' ? body : JSON.stringify(body));
}
function serveStatic(req, res) {
  let f = decodeURIComponent(req.url.split('?')[0]);
  if (f === '/') f = '/index.html';
  const full = path.join(ROOT, f);
  if (!full.startsWith(ROOT)) return send(res, 403, 'forbidden', 'text/plain');
  fs.readFile(full, (err, data) => {
    if (err) return send(res, 404, 'not found', 'text/plain');
    res.writeHead(200, { 'Content-Type': MIME[path.extname(full)] || 'application/octet-stream' });
    res.end(data);
  });
}

// ---------- OAuth e API ML ----------
async function refreshAccessToken() {
  env = loadEnv();
  if (!env.ML_REFRESH_TOKEN) throw new Error('refresh_token ausente — faça /login primeiro');
  const r = await fetch('https://api.mercadolibre.com/oauth/token', {
    method:'POST',
    headers:{'Content-Type':'application/x-www-form-urlencoded','Accept':'application/json'},
    body: new URLSearchParams({
      grant_type:'refresh_token',
      client_id: env.ML_CLIENT_ID,
      client_secret: env.ML_CLIENT_SECRET,
      refresh_token: env.ML_REFRESH_TOKEN,
    }),
  });
  const data = await r.json();
  if (!r.ok) throw new Error('refresh falhou: ' + JSON.stringify(data));
  saveEnv({
    ML_ACCESS_TOKEN: data.access_token,
    ML_REFRESH_TOKEN: data.refresh_token || env.ML_REFRESH_TOKEN,
    ML_USER_ID: String(data.user_id || env.ML_USER_ID || ''),
    ML_TOKEN_EXPIRES_AT: String(Date.now() + (data.expires_in * 1000)),
  });
  persistMLTokens(data);  // persistência adicional em tokens.json
  env = loadEnv();
  return env.ML_ACCESS_TOKEN;
}

async function getToken() {
  env = loadEnv();
  const exp = parseInt(env.ML_TOKEN_EXPIRES_AT || '0', 10);
  if (!env.ML_ACCESS_TOKEN || Date.now() > exp - 60_000) return refreshAccessToken();
  return env.ML_ACCESS_TOKEN;
}

async function ml(pathname, opts={}) {
  const token = await getToken();
  const r = await fetch('https://api.mercadolibre.com' + pathname, {
    ...opts,
    headers: { 'Authorization':'Bearer '+token, 'Accept':'application/json', ...(opts.headers||{}) },
  });
  const body = await r.json().catch(() => ({}));
  if (!r.ok) throw Object.assign(new Error('ML error ' + r.status), { status:r.status, body });
  return body;
}

// ---------- rotas ----------
const server = http.createServer(async (req, res) => {
  const u = url.parse(req.url, true);
  try {
    // Status de conexão (pro front saber se está conectado)
    if (u.pathname === '/api/status') {
      env = loadEnv();
      const connected = !!(env.ML_ACCESS_TOKEN && env.ML_USER_ID);
      return send(res, 200, {
        connected,
        hasCredentials: !!(env.ML_CLIENT_ID && env.ML_CLIENT_SECRET),
        userId: env.ML_USER_ID || null,
        tokenExpiresAt: env.ML_TOKEN_EXPIRES_AT ? parseInt(env.ML_TOKEN_EXPIRES_AT,10) : null,
      });
    }

    // Inicia OAuth
    if (u.pathname === '/login') {
      env = loadEnv();
      const clientId    = process.env.ML_CLIENT_ID    || env.ML_CLIENT_ID;
      const redirectUri = process.env.ML_REDIRECT_URI || env.ML_REDIRECT_URI || 'http://localhost:3000/callback';
      if (!clientId) return send(res, 400, 'Preencha ML_CLIENT_ID em .env', 'text/plain');
      const auth = 'https://auth.mercadolivre.com.br/authorization?' + new URLSearchParams({
        response_type:'code',
        client_id: clientId,
        redirect_uri: redirectUri,
      });
      res.writeHead(302, { Location: auth }); return res.end();
    }

    // Callback OAuth
    if (u.pathname === '/callback') {
      const code = u.query.code;
      if (!code) return send(res, 400, 'Sem "code" na URL', 'text/plain');
      env = loadEnv();
      const clientId     = process.env.ML_CLIENT_ID     || env.ML_CLIENT_ID;
      const clientSecret = process.env.ML_CLIENT_SECRET || env.ML_CLIENT_SECRET;
      const redirectUri  = process.env.ML_REDIRECT_URI  || env.ML_REDIRECT_URI || 'http://localhost:3000/callback';
      const r = await fetch('https://api.mercadolibre.com/oauth/token', {
        method:'POST',
        headers:{'Content-Type':'application/x-www-form-urlencoded','Accept':'application/json'},
        body: new URLSearchParams({
          grant_type:'authorization_code',
          client_id: clientId,
          client_secret: clientSecret,
          code,
          redirect_uri: redirectUri,
        }),
      });
      const data = await r.json();
      if (!r.ok) return send(res, 400, data);
      saveEnv({
        ML_ACCESS_TOKEN: data.access_token,
        ML_REFRESH_TOKEN: data.refresh_token,
        ML_USER_ID: String(data.user_id),
        ML_TOKEN_EXPIRES_AT: String(Date.now() + data.expires_in * 1000),
      });
      persistMLTokens(data);  // persistência adicional em tokens.json
      res.writeHead(302, { Location:'/?connected=1' }); return res.end();
    }

    // ============= AUTENTICAÇÃO DO PAINEL (persistência em disco) =============
    if (u.pathname === '/api/auth/signup' && req.method === 'POST') {
      const body = await readBody(req);
      const { name, email, password, store } = body || {};
      if (!name || !email || !password) return send(res, 400, { error:'name, email, password obrigatórios' });
      if (password.length < 6) return send(res, 400, { error:'Senha precisa ter no mínimo 6 caracteres' });
      if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return send(res, 400, { error:'Email inválido' });
      const users = loadUsersFile();
      if (users.find(u => u.email.toLowerCase() === email.toLowerCase())) {
        return send(res, 409, { error:'Já existe uma conta com este email' });
      }
      const { salt, hash } = hashPassword(password);
      const newUser = { name, email:email.trim(), store:store||'', salt, hash, createdAt:new Date().toISOString() };
      users.push(newUser);
      saveUsersFile(users);
      return send(res, 200, { ok:true, user:{ name:newUser.name, email:newUser.email, store:newUser.store } });
    }

    if (u.pathname === '/api/auth/login' && req.method === 'POST') {
      const body = await readBody(req);
      const { email, password } = body || {};
      if (!email || !password) return send(res, 400, { error:'email e password obrigatórios' });
      const users = loadUsersFile();
      const found = users.find(u => u.email.toLowerCase() === email.trim().toLowerCase());
      if (!found || !verifyPassword(password, found.salt, found.hash)) {
        return send(res, 401, { error:'Email ou senha incorretos' });
      }
      // gera token simples (uso local — não é JWT real mas serve pra sessão)
      const token = crypto.randomBytes(24).toString('hex');
      return send(res, 200, {
        ok:true,
        token,
        user:{ name:found.name, email:found.email, store:found.store, createdAt:found.createdAt }
      });
    }

    // lista de usuários (sem senhas) — útil pra debug
    if (u.pathname === '/api/auth/users') {
      const users = loadUsersFile().map(u => ({ name:u.name, email:u.email, store:u.store, createdAt:u.createdAt }));
      return send(res, 200, { count:users.length, users });
    }

    // ============= FIPE — Parallelum (sem auth) =============
    // marcas?tipo=carros|motos|caminhoes
    if (u.pathname === '/api/fipe/marcas') {
      const tipo = u.query.tipo || 'carros';
      const r = await fetch(`https://parallelum.com.br/fipe/api/v1/${tipo}/marcas`);
      return send(res, r.status, await r.json());
    }
    // modelos?tipo=&marca=
    if (u.pathname === '/api/fipe/modelos') {
      const tipo = u.query.tipo || 'carros';
      const marca = u.query.marca;
      if (!marca) return send(res, 400, { error:'marca obrigatória' });
      const r = await fetch(`https://parallelum.com.br/fipe/api/v1/${tipo}/marcas/${marca}/modelos`);
      return send(res, r.status, await r.json());
    }
    // anos?tipo=&marca=&modelo=
    if (u.pathname === '/api/fipe/anos') {
      const tipo = u.query.tipo || 'carros';
      const { marca, modelo } = u.query;
      if (!marca || !modelo) return send(res, 400, { error:'marca e modelo obrigatórios' });
      const r = await fetch(`https://parallelum.com.br/fipe/api/v1/${tipo}/marcas/${marca}/modelos/${modelo}/anos`);
      return send(res, r.status, await r.json());
    }
    // preco?tipo=&marca=&modelo=&ano=
    if (u.pathname === '/api/fipe/preco') {
      const tipo = u.query.tipo || 'carros';
      const { marca, modelo, ano } = u.query;
      if (!marca || !modelo || !ano) return send(res, 400, { error:'marca, modelo, ano obrigatórios' });
      const r = await fetch(`https://parallelum.com.br/fipe/api/v1/${tipo}/marcas/${marca}/modelos/${modelo}/anos/${ano}`);
      return send(res, r.status, await r.json());
    }

    // ============= ENDPOINTS PÚBLICOS (sem OAuth) =============
    // Busca pública no ML Brasil
    if (u.pathname === '/api/public/search') {
      const q = encodeURIComponent(u.query.q || 'amortecedor');
      const limit = Math.min(parseInt(u.query.limit||'20',10), 50);
      const r = await fetch(`https://api.mercadolibre.com/sites/MLB/search?q=${q}&limit=${limit}`);
      return send(res, r.status, await r.json());
    }
    // Detalhes de um item público
    if (u.pathname.startsWith('/api/public/item/')) {
      const id = u.pathname.split('/').pop();
      const r = await fetch(`https://api.mercadolibre.com/items/${id}`);
      return send(res, r.status, await r.json());
    }
    // Tendências de busca (top termos por categoria)
    if (u.pathname === '/api/public/trends') {
      const cat = u.query.category || 'MLB1747'; // Autopeças
      const r = await fetch(`https://api.mercadolibre.com/trends/MLB/${cat}`);
      return send(res, r.status, await r.json());
    }
    // Categorias nível 1 do Brasil
    if (u.pathname === '/api/public/categories') {
      const r = await fetch('https://api.mercadolibre.com/sites/MLB/categories');
      return send(res, r.status, await r.json());
    }

    // Proxy: dados do vendedor
    if (u.pathname === '/api/me') return send(res, 200, await ml('/users/me'));

    // Anúncios ativos do vendedor
    if (u.pathname === '/api/listings') {
      env = loadEnv();
      const limit = Math.min(parseInt(u.query.limit||'50',10), 50);
      const search = await ml(`/users/${env.ML_USER_ID}/items/search?status=active&limit=${limit}`);
      const ids = (search.results || []).slice(0, limit);
      if (!ids.length) return send(res, 200, { count: 0, items: [] });
      const multi = await ml(`/items?ids=${ids.join(',')}&attributes=id,title,price,available_quantity,sold_quantity,status,health,permalink,thumbnail,listing_type_id,category_id`);
      const items = multi.map(x => x.body).filter(Boolean);
      return send(res, 200, { count: search.paging?.total || items.length, items });
    }

    // Visitas/vendas agregadas do vendedor (últimos 30d)
    if (u.pathname === '/api/metrics') {
      env = loadEnv();
      const visits = await ml(`/users/${env.ML_USER_ID}/items_visits?last=30&unit=day`).catch(()=>null);
      return send(res, 200, { visits });
    }

    // Categorias (teste rápido)
    if (u.pathname === '/api/categories') return send(res, 200, await ml('/sites/MLB/categories'));

    // ============= ML OAuth — FLUXO SEMI-MANUAL (redirect = google.com) =============
    // POST /api/ml/token — troca code por access_token + refresh_token + user info
    if (u.pathname === '/api/ml/token' && req.method === 'POST') {
      env = loadEnv();
      const body = await readBody(req);
      const code = (body.code || '').trim();
      if (!code) return send(res, 400, { success:false, error:'code obrigatório' });

      const clientId     = process.env.ML_CLIENT_ID     || env.ML_CLIENT_ID     || '3688973136843575';
      const clientSecret = process.env.ML_CLIENT_SECRET || env.ML_CLIENT_SECRET || 'wFVvYKcAFoaLedYEfUmnKnUN9vYQMcXW';
      const redirectUri  = process.env.ML_REDIRECT_URI  || env.ML_REDIRECT_URI  || 'https://www.google.com';

      try {
        const r = await fetch('https://api.mercadolibre.com/oauth/token', {
          method: 'POST',
          headers: { 'Content-Type':'application/x-www-form-urlencoded', 'Accept':'application/json' },
          body: new URLSearchParams({
            grant_type:    'authorization_code',
            client_id:     clientId,
            client_secret: clientSecret,
            code,
            redirect_uri:  redirectUri,
          }),
        });
        const data = await r.json().catch(() => ({}));
        if (!data.access_token) {
          // Erros comuns: invalid_grant (code expirou/usado), invalid_client, redirect_uri_mismatch
          const msg = data.error === 'invalid_grant'
            ? 'Code expirado ou já usado — gere um novo clicando em "Autorizar" novamente.'
            : (data.message || data.error_description || data.error || 'Erro ao obter token');
          return send(res, 200, { success:false, error: msg, raw: data });
        }

        // Persiste no .env para os proxies autenticados do server continuarem funcionando
        saveEnv({
          ML_ACCESS_TOKEN:     data.access_token,
          ML_REFRESH_TOKEN:    data.refresh_token,
          ML_USER_ID:          String(data.user_id),
          ML_TOKEN_EXPIRES_AT: String(Date.now() + (data.expires_in || 21600) * 1000),
        });
        persistMLTokens(data);  // persistência adicional em tokens.json

        // Busca dados do usuário ML
        let nickname = null, email = null, reputation = null;
        try {
          const ur = await fetch('https://api.mercadolibre.com/users/me', {
            headers: { 'Authorization': 'Bearer ' + data.access_token }
          });
          if (ur.ok) {
            const u2 = await ur.json();
            nickname   = u2.nickname || null;
            email      = u2.email || null;
            reputation = u2.seller_reputation || null;
          }
        } catch(_){}

        return send(res, 200, {
          success:       true,
          access_token:  data.access_token,
          refresh_token: data.refresh_token,
          expires_in:    data.expires_in,
          user_id:       data.user_id,
          nickname,
          email,
          seller_reputation: reputation,
        });
      } catch(err) {
        return send(res, 200, { success:false, error: err.message });
      }
    }

    // POST /api/ml/refresh — usa refresh_token pra renovar o access_token
    if (u.pathname === '/api/ml/refresh' && req.method === 'POST') {
      env = loadEnv();
      const body = await readBody(req);
      const refreshToken = (body.refresh_token || env.ML_REFRESH_TOKEN || '').trim();
      if (!refreshToken) return send(res, 400, { success:false, error:'refresh_token obrigatório' });

      const clientId     = process.env.ML_CLIENT_ID     || env.ML_CLIENT_ID     || '3688973136843575';
      const clientSecret = process.env.ML_CLIENT_SECRET || env.ML_CLIENT_SECRET || 'wFVvYKcAFoaLedYEfUmnKnUN9vYQMcXW';

      try {
        const r = await fetch('https://api.mercadolibre.com/oauth/token', {
          method: 'POST',
          headers: { 'Content-Type':'application/x-www-form-urlencoded', 'Accept':'application/json' },
          body: new URLSearchParams({
            grant_type:    'refresh_token',
            client_id:     clientId,
            client_secret: clientSecret,
            refresh_token: refreshToken,
          }),
        });
        const data = await r.json().catch(() => ({}));
        if (data.access_token) {
          saveEnv({
            ML_ACCESS_TOKEN:     data.access_token,
            ML_REFRESH_TOKEN:    data.refresh_token || refreshToken,
            ML_TOKEN_EXPIRES_AT: String(Date.now() + (data.expires_in || 21600) * 1000),
          });
          persistMLTokens(data);  // persistência adicional em tokens.json
          return send(res, 200, { success:true, ...data });
        }
        return send(res, 200, { success:false, error: data.error_description || data.message || data.error || 'Falha no refresh', raw: data });
      } catch(err) {
        return send(res, 200, { success:false, error: err.message });
      }
    }

    // GET /api/ml/me — testa token + retorna dados do usuário
    // Aceita Bearer no header OU cai pro ML_ACCESS_TOKEN do .env
    if (u.pathname === '/api/ml/me' && req.method === 'GET') {
      env = loadEnv();
      const hdr = req.headers['authorization'] || req.headers['Authorization'] || '';
      const token = hdr.replace(/^Bearer\s+/i, '') || env.ML_ACCESS_TOKEN || '';
      if (!token) return send(res, 200, { connected:false, error:'sem token' });

      try {
        const r = await fetch('https://api.mercadolibre.com/users/me', {
          headers: { 'Authorization': 'Bearer ' + token }
        });
        const data = await r.json().catch(() => ({}));
        if (!r.ok || data.error) return send(res, 200, { connected:false, error: data.message || data.error || 'token inválido' });
        return send(res, 200, {
          connected: true,
          nickname:  data.nickname,
          user_id:   data.id,
          email:     data.email,
          seller_reputation: data.seller_reputation || null,
          site_id:   data.site_id,
          country_id: data.country_id,
        });
      } catch(err) {
        return send(res, 200, { connected:false, error: err.message });
      }
    }

    // ============= ML DATA — endpoints autenticados via Bearer (localStorage) =============
    // Helper local: extrai Bearer do header ou cai pro .env
    const getBearer = () => {
      env = loadEnv();
      const hdr = req.headers['authorization'] || req.headers['Authorization'] || '';
      return hdr.replace(/^Bearer\s+/i, '') || env.ML_ACCESS_TOKEN || '';
    };
    const authedFetch = async (url, token) => {
      const r = await fetch(url, { headers: { 'Authorization': 'Bearer ' + token } });
      const data = await r.json().catch(() => ({}));
      return { ok: r.ok, status: r.status, data };
    };

    // 1) GET /api/ml/items — anúncios ativos do vendedor (com detalhes em lotes de 20)
    if (u.pathname === '/api/ml/items' && req.method === 'GET') {
      const token = getBearer();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      try {
        const me = await authedFetch('https://api.mercadolibre.com/users/me', token);
        if (!me.ok || !me.data.id) return send(res, me.status || 401, { success:false, error: me.data.message || 'Token inválido' });

        const limit = Math.min(parseInt(u.query.limit||'50',10), 50);
        const search = await authedFetch(
          `https://api.mercadolibre.com/users/${me.data.id}/items/search?status=active&limit=${limit}`, token
        );
        if (!search.ok) return send(res, search.status, { success:false, error: search.data.message || 'Falha ao buscar items' });

        const itemIds = search.data.results || [];
        let allItems = [];
        for (let i = 0; i < itemIds.length; i += 20) {
          const batch = itemIds.slice(i, i + 20).join(',');
          if (!batch) continue;
          const det = await authedFetch(`https://api.mercadolibre.com/items?ids=${batch}`, token);
          if (Array.isArray(det.data)) {
            allItems = allItems.concat(det.data.map(d => d.body).filter(Boolean));
          }
        }

        return send(res, 200, {
          success: true,
          total: search.data.paging?.total || 0,
          items: allItems.map(item => ({
            id: item.id,
            title: item.title,
            price: item.price,
            currency: item.currency_id,
            status: item.status,
            permalink: item.permalink,
            thumbnail: item.thumbnail,
            sold_quantity: item.sold_quantity,
            available_quantity: item.available_quantity,
            listing_type: item.listing_type_id,
            condition: item.condition,
            category_id: item.category_id,
            health: item.health,
            date_created: item.date_created,
          })),
        });
      } catch(err) {
        return send(res, 200, { success:false, error: err.message });
      }
    }

    // 2) GET /api/ml/orders — vendas recentes do vendedor
    if (u.pathname === '/api/ml/orders' && req.method === 'GET') {
      const token = getBearer();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      try {
        const me = await authedFetch('https://api.mercadolibre.com/users/me', token);
        if (!me.ok || !me.data.id) return send(res, me.status || 401, { success:false, error: me.data.message || 'Token inválido' });

        const limit = Math.min(parseInt(u.query.limit||'50',10), 50);
        const orders = await authedFetch(
          `https://api.mercadolibre.com/orders/search?seller=${me.data.id}&sort=date_desc&limit=${limit}`, token
        );
        if (!orders.ok) return send(res, orders.status, { success:false, error: orders.data.message || 'Falha ao buscar orders' });

        return send(res, 200, {
          success: true,
          total: orders.data.paging?.total || 0,
          orders: (orders.data.results || []).map(order => ({
            id: order.id,
            status: order.status,
            total: order.total_amount,
            currency: order.currency_id,
            date: order.date_created,
            buyer: order.buyer?.nickname || null,
            items: (order.order_items || []).map(oi => ({
              title: oi.item?.title,
              quantity: oi.quantity,
              unit_price: oi.unit_price,
            })),
          })),
        });
      } catch(err) {
        return send(res, 200, { success:false, error: err.message });
      }
    }

    // 3) GET /api/ml/visits/:itemId — visitas de um item (últimos 30d)
    if (u.pathname.startsWith('/api/ml/visits/') && req.method === 'GET') {
      const token = getBearer();
      const itemId = u.pathname.split('/').pop();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      if (!itemId) return send(res, 400, { success:false, error:'itemId obrigatório' });
      try {
        const v = await authedFetch(
          `https://api.mercadolibre.com/items/${itemId}/visits/time_window?last=30&unit=day`, token
        );
        if (!v.ok) return send(res, v.status, { success:false, error: v.data.message || 'Falha' });
        return send(res, 200, { success:true, visits: v.data });
      } catch(err) {
        return send(res, 200, { success:false, error: err.message });
      }
    }

    // 4) GET /api/ml/questions — perguntas pendentes (UNANSWERED)
    if (u.pathname === '/api/ml/questions' && req.method === 'GET') {
      const token = getBearer();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      try {
        const me = await authedFetch('https://api.mercadolibre.com/users/me', token);
        if (!me.ok || !me.data.id) return send(res, me.status || 401, { success:false, error: me.data.message || 'Token inválido' });

        const limit = Math.min(parseInt(u.query.limit||'50',10), 50);
        const qs = await authedFetch(
          `https://api.mercadolibre.com/questions/search?seller_id=${me.data.id}&status=UNANSWERED&sort_fields=date_created&sort_types=DESC&limit=${limit}`,
          token
        );
        if (!qs.ok) return send(res, qs.status, { success:false, error: qs.data.message || 'Falha' });

        return send(res, 200, {
          success: true,
          total: qs.data.total || 0,
          questions: (qs.data.questions || []).map(q => ({
            id: q.id,
            text: q.text,
            item_id: q.item_id,
            date: q.date_created,
            status: q.status,
          })),
        });
      } catch(err) {
        return send(res, 200, { success:false, error: err.message });
      }
    }

    // 5) GET /api/ml/dashboard — resumo consolidado (user + items + orders)
    if (u.pathname === '/api/ml/dashboard' && req.method === 'GET') {
      const token = getBearer();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      try {
        const me = await authedFetch('https://api.mercadolibre.com/users/me', token);
        if (!me.ok || !me.data.id) return send(res, me.status || 401, { success:false, error: me.data.message || 'Token inválido' });

        const [items, orders] = await Promise.all([
          authedFetch(`https://api.mercadolibre.com/users/${me.data.id}/items/search?status=active`, token),
          authedFetch(`https://api.mercadolibre.com/orders/search?seller=${me.data.id}&sort=date_desc&limit=10`, token),
        ]);

        return send(res, 200, {
          success: true,
          user: {
            id:                me.data.id,
            nickname:          me.data.nickname,
            email:             me.data.email,
            seller_reputation: me.data.seller_reputation,
            status:            me.data.status,
          },
          items: {
            total_active: items.data?.paging?.total || 0,
          },
          orders: {
            total: orders.data?.paging?.total || 0,
            recent: (orders.data?.results || []).slice(0, 5).map(o => ({
              id:     o.id,
              total:  o.total_amount,
              status: o.status,
              date:   o.date_created,
            })),
          },
        });
      } catch(err) {
        return send(res, 200, { success:false, error: err.message });
      }
    }

    // ============= BLING ERP — OAuth + API =============
    // Helpers locais
    const blingCreds = () => {
      env = loadEnv();
      return {
        clientId:     process.env.BLING_CLIENT_ID     || env.BLING_CLIENT_ID     || '',
        clientSecret: process.env.BLING_CLIENT_SECRET || env.BLING_CLIENT_SECRET || '',
        redirectUri:  process.env.BLING_REDIRECT_URI  || env.BLING_REDIRECT_URI  || 'http://localhost:3000/callback/bling',
      };
    };
    const getBlingBearer = () => {
      const hdr = req.headers['authorization'] || req.headers['Authorization'] || '';
      return hdr.replace(/^Bearer\s+/i, '');
    };
    const blingFetch = async (url, token, method='GET', body=null) => {
      const opts = {
        method,
        headers: { 'Authorization': 'Bearer ' + token, 'Accept': 'application/json' },
      };
      if (body) { opts.headers['Content-Type'] = 'application/json'; opts.body = JSON.stringify(body); }
      const r = await fetch(url, opts);
      const data = await r.json().catch(() => ({}));
      return { ok: r.ok, status: r.status, data };
    };

    // GET /api/bling/config-status — sinaliza pro front se o app está configurado
    if (u.pathname === '/api/bling/config-status' && req.method === 'GET') {
      const { clientId, redirectUri } = blingCreds();
      return send(res, 200, {
        configured: !!clientId,
        client_id_preview: clientId ? (clientId.slice(0,4) + '…' + clientId.slice(-2)) : null,
        redirect_uri: redirectUri,
      });
    }

    // GET /api/bling/authorize — retorna a URL de autorização OAuth
    if (u.pathname === '/api/bling/authorize' && req.method === 'GET') {
      const { clientId, redirectUri } = blingCreds();
      if (!clientId) return send(res, 400, { success:false, error:'BLING_CLIENT_ID não configurado no servidor (.env)' });
      const state = crypto.randomBytes(8).toString('hex');
      const url = 'https://www.bling.com.br/Api/v3/oauth/authorize?' + new URLSearchParams({
        response_type: 'code',
        client_id:     clientId,
        redirect_uri:  redirectUri,
        state,
      }).toString();
      return send(res, 200, { success:true, url, state });
    }

    // POST /api/bling/token — troca authorization_code por access_token
    if (u.pathname === '/api/bling/token' && req.method === 'POST') {
      const { clientId, clientSecret, redirectUri } = blingCreds();
      if (!clientId || !clientSecret) return send(res, 400, { success:false, error:'Bling não configurado no servidor' });
      const body = await readBody(req);
      const code = (body.code || '').trim();
      if (!code) return send(res, 400, { success:false, error:'code obrigatório' });

      try {
        const credentials = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
        const r = await fetch('https://www.bling.com.br/Api/v3/oauth/token', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept':       '1.0',
            'Authorization': 'Basic ' + credentials,
          },
          body: new URLSearchParams({
            grant_type:   'authorization_code',
            code,
            redirect_uri: redirectUri,
          }),
        });
        const data = await r.json().catch(() => ({}));
        if (!data.access_token) {
          const msg = data.error === 'invalid_grant'
            ? 'Code expirado ou já usado — gere um novo clicando em "Autorizar no Bling" novamente.'
            : (data.error_description || data.error || 'Erro ao obter token Bling');
          return send(res, 200, { success:false, error: msg, raw: data });
        }
        persistBlingTokens(data);  // persistência + futuro auto-refresh
        return send(res, 200, {
          success: true,
          access_token:  data.access_token,
          refresh_token: data.refresh_token,
          expires_in:    data.expires_in,
          token_type:    data.token_type,
          scope:         data.scope || '',
        });
      } catch(err) {
        return send(res, 200, { success:false, error: err.message });
      }
    }

    // POST /api/bling/refresh — renova access_token via refresh_token
    if (u.pathname === '/api/bling/refresh' && req.method === 'POST') {
      const { clientId, clientSecret } = blingCreds();
      if (!clientId || !clientSecret) return send(res, 400, { success:false, error:'Bling não configurado no servidor' });
      const body = await readBody(req);
      const refreshToken = (body.refresh_token || '').trim();
      if (!refreshToken) return send(res, 400, { success:false, error:'refresh_token obrigatório' });
      try {
        const credentials = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
        const r = await fetch('https://www.bling.com.br/Api/v3/oauth/token', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept':       '1.0',
            'Authorization': 'Basic ' + credentials,
          },
          body: new URLSearchParams({
            grant_type:    'refresh_token',
            refresh_token: refreshToken,
          }),
        });
        const data = await r.json().catch(() => ({}));
        if (!data.access_token) return send(res, 200, { success:false, error: data.error_description || data.error || 'Falha no refresh', raw: data });
        persistBlingTokens(data);  // persiste o token renovado
        return send(res, 200, { success:true, ...data });
      } catch(err) {
        return send(res, 200, { success:false, error: err.message });
      }
    }

    // GET /callback/bling — callback humano (redirect do Bling)
    // Mostra o code na tela pro usuário copiar de volta pro painel
    if (u.pathname === '/callback/bling' && req.method === 'GET') {
      const code = u.query.code || '';
      const err  = u.query.error || '';
      const html = `<!doctype html>
<html><head><meta charset="utf-8"><title>Bling OAuth Callback</title>
<style>body{font-family:sans-serif;background:#0a0a0a;color:#e5e7eb;padding:40px;max-width:720px;margin:0 auto}
.card{background:#141414;border:1px solid #222;border-radius:12px;padding:24px;margin-top:20px}
.code{background:#0e0e0e;border:1px solid #00A7FF;color:#86efac;padding:12px 16px;border-radius:8px;word-break:break-all;font-family:monospace;font-size:14px;user-select:all}
.err{background:rgba(220,38,38,.15);border:1px solid #dc2626;color:#fca5a5;padding:14px;border-radius:8px}
h1{color:#00A7FF}.btn{background:#00A7FF;color:#fff;padding:10px 16px;border-radius:8px;text-decoration:none;display:inline-block;margin-top:14px;font-weight:700}</style>
</head><body>
<h1>🔌 Bling OAuth</h1>
${err ? `<div class="err"><b>Erro:</b> ${err}<br>${u.query.error_description||''}</div>` :
  code ? `<div class="card">
    <p>✅ Autorização recebida. Copie o código abaixo e cole no painel AM (aba 🔌 Integrações):</p>
    <div class="code">${code}</div>
    <a href="/" class="btn">Voltar ao painel →</a>
  </div>` :
  `<div class="err">Sem code na URL — tente autorizar novamente.</div>`}
</body></html>`;
      res.writeHead(200, { 'Content-Type':'text/html; charset=utf-8' });
      return res.end(html);
    }

    // GET /api/bling/produtos — lista paginada de produtos
    if (u.pathname === '/api/bling/produtos' && req.method === 'GET') {
      const token = getBlingBearer();
      if (!token) return send(res, 200, { success:false, error:'Token Bling não fornecido' });
      const pagina = Math.max(1, parseInt(u.query.page || u.query.pagina || '1', 10));
      const limite = Math.min(100, Math.max(1, parseInt(u.query.limit || u.query.limite || '100', 10)));
      try {
        const r = await blingFetch(`https://www.bling.com.br/Api/v3/produtos?pagina=${pagina}&limite=${limite}`, token);
        if (!r.ok) return send(res, 200, { success:false, error: r.data.error?.description || 'Falha ao listar produtos', status:r.status });
        const list = Array.isArray(r.data.data) ? r.data.data : [];
        return send(res, 200, {
          success: true,
          total: list.length,
          pagina,
          produtos: list.map(p => ({
            id:              p.id,
            nome:            p.nome,
            codigo:          p.codigo,
            preco:           p.preco,
            precoCusto:      p.precoCusto,
            situacao:        p.situacao,
            tipo:            p.tipo,
            formato:         p.formato,
            unidade:         p.unidade,
            pesoLiquido:     p.pesoLiquido,
            pesoBruto:       p.pesoBruto,
            gtin:            p.gtin,
            gtinEmbalagem:   p.gtinEmbalagem,
            marca:           p.marca,
            descricaoCurta:  p.descricaoCurta,
            estoqueSaldo:    p.estoque?.saldoVirtualTotal ?? null,
          })),
        });
      } catch(err) {
        return send(res, 200, { success:false, error: err.message });
      }
    }

    // GET /api/bling/produtos/:id/estoques — estoque do produto
    if (u.pathname.match(/^\/api\/bling\/produtos\/\d+\/estoques$/) && req.method === 'GET') {
      const token = getBlingBearer();
      if (!token) return send(res, 200, { success:false, error:'Token Bling não fornecido' });
      const id = u.pathname.split('/')[4];
      try {
        const r = await blingFetch(`https://www.bling.com.br/Api/v3/estoques/produtos/${id}`, token);
        return send(res, 200, { success: r.ok, estoque: r.data.data || null, raw: r.ok ? null : r.data });
      } catch(err) {
        return send(res, 200, { success:false, error: err.message });
      }
    }

    // GET /api/bling/produtos/:id — detalhe de um produto
    if (u.pathname.match(/^\/api\/bling\/produtos\/\d+$/) && req.method === 'GET') {
      const token = getBlingBearer();
      if (!token) return send(res, 200, { success:false, error:'Token Bling não fornecido' });
      const id = u.pathname.split('/').pop();
      try {
        const r = await blingFetch(`https://www.bling.com.br/Api/v3/produtos/${id}`, token);
        return send(res, 200, { success: r.ok, produto: r.data.data || null, raw: r.ok ? null : r.data });
      } catch(err) {
        return send(res, 200, { success:false, error: err.message });
      }
    }

    // ============= FLUXO COMPLETO: BLING → ML =============
    // POST /api/publicar — puxa 1 produto do Bling, otimiza, publica no ML
    if (u.pathname === '/api/publicar' && req.method === 'POST') {
      const body = await readBody(req);
      const { produtoId, blingToken, mlToken } = body || {};
      if (!produtoId || !blingToken || !mlToken) {
        return send(res, 400, { success:false, error:'produtoId, blingToken e mlToken obrigatórios' });
      }
      try {
        // 1) Detalhe do produto no Bling
        const pr = await blingFetch(`https://www.bling.com.br/Api/v3/produtos/${produtoId}`, blingToken);
        if (!pr.ok || !pr.data.data) return send(res, 200, { success:false, error:'Produto não encontrado no Bling', raw: pr.data });
        const produto = pr.data.data;

        // 2) Predição de categoria no ML
        let categoryId = 'MLB1747'; // fallback autopeças
        try {
          const cr = await fetch(`https://api.mercadolibre.com/sites/MLB/domain_discovery/search?q=${encodeURIComponent(produto.nome || '')}`, {
            headers: { 'Authorization': 'Bearer ' + mlToken }
          });
          const cj = await cr.json().catch(() => []);
          if (Array.isArray(cj) && cj[0]?.category_id) categoryId = cj[0].category_id;
        } catch(_) {}

        // 3) Atributos obrigatórios
        let requiredAttrs = [];
        try {
          const ar = await fetch(`https://api.mercadolibre.com/categories/${categoryId}/attributes`, {
            headers: { 'Authorization': 'Bearer ' + mlToken }
          });
          const attrs = await ar.json().catch(() => []);
          if (Array.isArray(attrs)) {
            requiredAttrs = attrs
              .filter(a => a.tags && a.tags.required)
              .map(a => ({
                id: a.id,
                value_name:
                  a.id === 'BRAND'      ? (produto.marca || 'Genérico') :
                  a.id === 'GTIN'       ? (produto.gtin || '') :
                  a.id === 'SELLER_SKU' ? (produto.codigo || '') :
                  (a.values?.[0]?.name || ''),
              }))
              .filter(a => a.value_name);
          }
        } catch(_) {}

        // 4) Título otimizado (ML limita 60)
        let titulo = (produto.nome || '').trim();
        if (titulo.length > 60) titulo = titulo.slice(0, 57) + '...';

        // 5) Body do anúncio
        const imagens = (produto.midia?.imagens?.internas || [])
          .map(img => ({ source: img.link }))
          .filter(i => i.source);
        const estoque = produto.estoque?.saldoVirtualTotal ?? 1;
        const anuncio = {
          title:       titulo,
          category_id: categoryId,
          price:       Number(produto.preco) || 0,
          currency_id: 'BRL',
          available_quantity: Math.max(1, Number(estoque) || 1),
          buying_mode:  'buy_it_now',
          listing_type_id: 'gold_special',
          condition:    'new',
          description:  { plain_text: (produto.descricaoCurta || titulo + ' — produto novo, com garantia. Envio rápido para todo Brasil.').slice(0, 50000) },
          attributes:   requiredAttrs,
          pictures:     imagens,
        };

        // 6) POST /items no ML
        const mr = await fetch('https://api.mercadolibre.com/items', {
          method: 'POST',
          headers: { 'Authorization': 'Bearer ' + mlToken, 'Content-Type': 'application/json' },
          body: JSON.stringify(anuncio),
        });
        const mj = await mr.json().catch(() => ({}));
        if (mj.id) {
          return send(res, 200, {
            success: true,
            message: 'Anúncio publicado com sucesso',
            ml_item_id: mj.id,
            permalink:  mj.permalink,
            title:      mj.title,
            price:      mj.price,
            status:     mj.status,
            category_id: categoryId,
          });
        }
        return send(res, 200, {
          success: false,
          error:   mj.message || mj.error || 'Erro ao publicar no ML',
          details: mj.cause || mj,
        });
      } catch(err) {
        return send(res, 200, { success:false, error: err.message });
      }
    }

    // POST /api/publicar/lote — publica vários com throttle 1s entre cada
    if (u.pathname === '/api/publicar/lote' && req.method === 'POST') {
      const body = await readBody(req);
      const { produtoIds, blingToken, mlToken } = body || {};
      if (!Array.isArray(produtoIds) || produtoIds.length === 0) {
        return send(res, 400, { success:false, error:'produtoIds (array) obrigatório' });
      }
      if (!blingToken || !mlToken) return send(res, 400, { success:false, error:'blingToken e mlToken obrigatórios' });

      const base = `http://127.0.0.1:${PORT}`;
      const resultados = [];
      for (let i = 0; i < produtoIds.length; i++) {
        const produtoId = produtoIds[i];
        try {
          const r = await fetch(`${base}/api/publicar`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ produtoId, blingToken, mlToken }),
          });
          const j = await r.json().catch(() => ({ success:false, error:'JSON inválido' }));
          resultados.push({ produtoId, ...j });
        } catch(err) {
          resultados.push({ produtoId, success:false, error: err.message });
        }
        // throttle 1s pro rate-limit ML (exceto no último)
        if (i < produtoIds.length - 1) await new Promise(r => setTimeout(r, 1000));
      }
      return send(res, 200, {
        success: true,
        total:      produtoIds.length,
        publicados: resultados.filter(r => r.success).length,
        erros:      resultados.filter(r => !r.success).length,
        resultados,
      });
    }

    // ============= TOKEN MANAGEMENT (ML + Bling) =============
    // GET /api/tokens/status — estado consolidado dos tokens (pro indicador do front)
    if (u.pathname === '/api/tokens/status' && req.method === 'GET') {
      const tokens = loadTokens();
      const now = Date.now();

      const mlExpAt = tokens.ml_token_expires_at ? new Date(tokens.ml_token_expires_at).getTime() : null;
      const blExpAt = tokens.bling_token_expires_at ? new Date(tokens.bling_token_expires_at).getTime() : null;
      const hoursLeft = (exp) => exp ? Math.max(0, (exp - now) / 3600000) : null;

      return send(res, 200, {
        ml: {
          connected:     !!tokens.ml_access_token,
          expires_at:    tokens.ml_token_expires_at || null,
          expires_in_ms: mlExpAt ? Math.max(0, mlExpAt - now) : null,
          hours_left:    hoursLeft(mlExpAt) != null ? Number(hoursLeft(mlExpAt).toFixed(2)) : null,
          auto_refresh:  !!tokens.ml_refresh_token,
          user_id:       tokens.ml_user_id || null,
          expired:       mlExpAt ? (mlExpAt - now <= 0) : false,
          near_expiry:   mlExpAt ? (mlExpAt - now < 3600000) : false, // <1h
        },
        bling: {
          connected:     !!tokens.bling_access_token,
          expires_at:    tokens.bling_token_expires_at || null,
          expires_in_ms: blExpAt ? Math.max(0, blExpAt - now) : null,
          hours_left:    hoursLeft(blExpAt) != null ? Number(hoursLeft(blExpAt).toFixed(2)) : null,
          auto_refresh:  !!tokens.bling_refresh_token,
          expired:       blExpAt ? (blExpAt - now <= 0) : false,
          near_expiry:   blExpAt ? (blExpAt - now < 1800000) : false, // <30min
        },
        updated_at: tokens.updated_at || null,
      });
    }

    // POST /api/tokens/refresh-ml — força refresh do ML (via server)
    if (u.pathname === '/api/tokens/refresh-ml' && req.method === 'POST') {
      const saved = await refreshMLToken();
      if (saved) {
        return send(res, 200, {
          success: true,
          message: 'Token ML renovado',
          expires_at: saved.ml_token_expires_at,
          user_id:    saved.ml_user_id,
        });
      }
      return send(res, 200, { success:false, error: 'Falha no refresh — reconecte na aba 🔌 Integrações' });
    }

    // POST /api/tokens/refresh-bling — força refresh do Bling
    if (u.pathname === '/api/tokens/refresh-bling' && req.method === 'POST') {
      const saved = await refreshBlingToken();
      if (saved) {
        return send(res, 200, { success:true, expires_at: saved.bling_token_expires_at });
      }
      return send(res, 200, { success:false, error:'Falha no refresh Bling — reconecte' });
    }

    // GET /api/tokens/ml — fallback: front pega access_token do server
    // (útil quando o localStorage está vazio ou o server renovou em background)
    if (u.pathname === '/api/tokens/ml' && req.method === 'GET') {
      const tokens = loadTokens();
      if (tokens.ml_access_token) {
        return send(res, 200, {
          success:      true,
          access_token: tokens.ml_access_token,
          expires_at:   tokens.ml_token_expires_at,
          user_id:      tokens.ml_user_id,
        });
      }
      return send(res, 200, { success:false });
    }

    // GET /api/tokens/bling — idem pro Bling
    if (u.pathname === '/api/tokens/bling' && req.method === 'GET') {
      const tokens = loadTokens();
      if (tokens.bling_access_token) {
        return send(res, 200, {
          success:      true,
          access_token: tokens.bling_access_token,
          expires_at:   tokens.bling_token_expires_at,
        });
      }
      return send(res, 200, { success:false });
    }

    // Estáticos
    return serveStatic(req, res);
  } catch (e) {
    console.error('[erro]', e.message, e.body || '');
    return send(res, e.status || 500, { error: e.message, detail: e.body || null });
  }
});

server.listen(PORT, () => {
  const env_name = process.env.NODE_ENV === 'production' ? 'PRODUÇÃO' : 'LOCAL';
  console.log(`\n🟢 Agente ML [${env_name}] — porta ${PORT}`);
  if (process.env.NODE_ENV !== 'production') {
    console.log(`   Acesse: http://localhost:${PORT}`);
    console.log(`   Fluxo: preencha .env → acesse /login → autorize → volta conectado\n`);
  } else {
    console.log(`   Listening on :${PORT} (production)\n`);
  }
});
