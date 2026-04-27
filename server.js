// ============================================================
// BOOT: carrega .env pra process.env (ANTHROPIC_API_KEY, ADMIN_SECRET,
// ML_CLIENT_ID/SECRET, BLING_*, etc.). Tenta dotenv primeiro; se não
// estiver instalado, faz parse manual — sem dependência externa.
// ============================================================
try { require('dotenv').config(); } catch(e) {
  try {
    const _fs   = require('fs');
    const _path = require('path');
    const _envPath = _path.join(__dirname, '.env');
    if (_fs.existsSync(_envPath)) {
      const _envContent = _fs.readFileSync(_envPath, 'utf8');
      let _count = 0;
      _envContent.split(/\r?\n/).forEach(line => {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith('#')) return;
        const eq = trimmed.indexOf('=');
        if (eq <= 0) return;
        const key = trimmed.slice(0, eq).trim();
        let val = trimmed.slice(eq + 1).trim();
        // Remove aspas nas pontas (simples ou duplas)
        if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
          val = val.slice(1, -1);
        }
        if (key && !(key in process.env)) { // não sobrescreve vars já setadas pelo sistema
          process.env[key] = val;
          _count++;
        }
      });
      console.log('[boot] ✅ .env carregado (' + _count + ' variáveis)');
    }
  } catch(e2) { /* silencioso — se não der, cai nos defaults hardcoded */ }
}

// ============================================================
// CLOUDFLARE WORKERS PROXY — contorna bloqueio do ML contra o IP do VPS
// ============================================================
// O ML rate-limita/bloqueia o IP do VPS. Solução: as chamadas pra
// api.mercadolibre.com passam por um Cloudflare Worker (IP limpo).
// Ativado automaticamente quando ML_PROXY_URL está no .env.
// Deploy do Worker: copiar o código abaixo em dash.cloudflare.com →
// Workers → Create, configurar route ml-proxy.agentemarkt.com/*,
// e setar ML_PROXY_URL=https://ml-proxy.agentemarkt.com no .env.
//
// ===== WORKER CODE (COPIAR NO CLOUDFLARE DASHBOARD) =====
// const ALLOWED_ORIGINS = ['https://agentemarkt.com'];
// const ML_API_BASE     = 'https://api.mercadolibre.com';
// const PROXY_SECRET    = 'agente-ml-proxy-2026'; // alinhado com ML_PROXY_SECRET do server
//
// addEventListener('fetch', event => {
//   event.respondWith(handleRequest(event.request));
// });
//
// async function handleRequest(request) {
//   if (request.method === 'OPTIONS') {
//     return new Response(null, { headers: {
//       'Access-Control-Allow-Origin':  '*',
//       'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, PATCH',
//       'Access-Control-Allow-Headers': 'Authorization, Content-Type, X-Proxy-Secret',
//       'Access-Control-Max-Age':       '86400',
//     }});
//   }
//   if (request.headers.get('X-Proxy-Secret') !== PROXY_SECRET) {
//     return new Response(JSON.stringify({error:'Unauthorized proxy access'}),
//       { status: 403, headers: {'Content-Type':'application/json'} });
//   }
//   try {
//     const u = new URL(request.url);
//     const mlUrl = ML_API_BASE + (u.pathname.startsWith('/') ? u.pathname : '/' + u.pathname) + u.search;
//     const headers = new Headers();
//     for (const [k, v] of request.headers) {
//       if (k.toLowerCase() !== 'x-proxy-secret' && k.toLowerCase() !== 'host') headers.set(k, v);
//     }
//     headers.set('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36');
//     headers.set('Accept', 'application/json');
//     const mlResponse = await fetch(mlUrl, {
//       method: request.method,
//       headers: headers,
//       body:    (request.method !== 'GET' && request.method !== 'HEAD') ? await request.text() : undefined,
//     });
//     const rh = new Headers(mlResponse.headers);
//     rh.set('Access-Control-Allow-Origin', '*');
//     rh.set('X-Proxy-Status', 'ok');
//     return new Response(mlResponse.body, { status: mlResponse.status, headers: rh });
//   } catch (error) {
//     return new Response(JSON.stringify({error:'Proxy error', message:error.message}),
//       { status: 502, headers: {'Content-Type':'application/json'} });
//   }
// }
// ===== FIM DO WORKER CODE =====

const ML_PROXY_URL    = process.env.ML_PROXY_URL    || '';
const ML_PROXY_SECRET = process.env.ML_PROXY_SECRET || 'agente-ml-proxy-2026';
const ML_USE_PROXY    = !!ML_PROXY_URL;

if (ML_USE_PROXY) {
  console.log(`[boot] 🌐 ML Proxy ativo: ${ML_PROXY_URL}`);
} else {
  console.log('[boot] ⚠️ ML Proxy não configurado — chamadas diretas pra api.mercadolibre.com');
}

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

// FASE 1 — Persistência SQLite (anti-duplicidade Bling↔ML).
// Requer better-sqlite3 instalado: npm install better-sqlite3
const { db } = require('./db.js');

// ============================================================
// FASE 1.5 — Extração de dados do título do produto Bling
// ============================================================
const COMPATIBILIDADES_ML = require('./compatibilidades_ml.json');

// Aliases comuns no Bling para nome ML canônico
const ALIASES_MARCA_BLING = {
  'mercedes':       'Mercedes-Benz',
  'mercedes-benz':  'Mercedes-Benz',
  'mercedez':       'Mercedes-Benz',
  'vw':             'Volkswagen',
  'volkswagem':     'Volkswagen',
  'volkswagen':     'Volkswagen',
  'gm':             'Chevrolet',
  'chevrolet':      'Chevrolet',
  'chevy':          'Chevrolet',
  'renault':        'Renault',
  'honda':          'Honda',
  'hyundai':        'Hyundai',
  'nissan':         'Nissan',
  'jeep':           'Jeep',
  'fiat':           'Fiat',
  'ford':           'Ford',
  'toyota':         'Toyota',
};

function normalizarNomeMarca(marcaBling) {
  if (!marcaBling) return null;
  const lower = String(marcaBling).toLowerCase().trim();
  if (ALIASES_MARCA_BLING[lower]) return ALIASES_MARCA_BLING[lower];
  // Tenta match direto com BRAND do mapa
  for (const nome of Object.keys(COMPATIBILIDADES_ML.BRAND)) {
    if (nome.toLowerCase() === lower) return nome;
  }
  return null;
}

function normalizarCor(cor) {
  if (!cor) return null;
  const map = {
    'Branca':    'Branco',
    'Preta':     'Preto',
    'Vermelha':  'Vermelho',
    'Amarela':   'Amarelo',
    'Dourada':   'Dourado',
    'Prateado':  'Prata',
  };
  return map[cor] || cor;
}

/**
 * Extrai dados estruturados do nome/título do produto Bling.
 * @param {string} nome - Título do produto (ex: "Capô Renault Kwid 2017 a 2025 Cinza")
 * @param {string} marcaBling - Marca do produto Bling (opcional, usado como prioridade)
 * @returns {object} { marca, modelo, posicao, lado, cor, anoInicial, anoFinal }
 */
function extrairDadosDoTitulo(nome, marcaBling) {
  if (!nome) return {};
  const txt = String(nome);
  const txtLower = txt.toLowerCase();

  // 1) Marca: prioridade marcaBling, fallback regex no título
  let marca = normalizarNomeMarca(marcaBling);
  if (!marca) {
    for (const nomeMarca of Object.keys(COMPATIBILIDADES_ML.BRAND)) {
      const re = new RegExp(`\\b${nomeMarca.toLowerCase().replace(/-/g, '\\-')}\\b`, 'i');
      if (re.test(txtLower)) {
        marca = nomeMarca;
        break;
      }
    }
  }

  // 2) Modelo: busca dentro dos modelos da marca encontrada
  let modelo = null;
  if (marca) {
    const brandId = COMPATIBILIDADES_ML.BRAND[marca];
    const modelosDaMarca = COMPATIBILIDADES_ML.MODEL[brandId] || {};
    // Ordena por tamanho do nome DESC para match de "Sandero Stepway" antes de "Sandero"
    const nomesModelos = Object.keys(modelosDaMarca).sort((a, b) => b.length - a.length);
    for (const nomeModelo of nomesModelos) {
      const escapado = nomeModelo.replace(/[-\/\.]/g, '\\$&');
      const re = new RegExp(`\\b${escapado}\\b`, 'i');
      if (re.test(txt)) {
        modelo = nomeModelo;
        break;
      }
    }
  }

  // 3) Posição (Traseira/Dianteira)
  let posicao = null;
  if (/\btraseir[oa]\b/i.test(txt))      posicao = 'Traseira';
  else if (/\bdianteir[oa]\b/i.test(txt)) posicao = 'Dianteira';

  // 4) Lado (Esquerda/Direita)
  let lado = null;
  if (/\bdireit[oa]\b/i.test(txt))        lado = 'Direita';
  else if (/\besquerd[oa]\b/i.test(txt))  lado = 'Esquerda';

  // 5) Cor
  const coresPossiveis = [
    'Branca', 'Branco', 'Preta', 'Preto', 'Cinza', 'Prata', 'Prateado',
    'Azul', 'Vermelha', 'Vermelho', 'Verde', 'Amarela', 'Amarelo',
    'Bege', 'Marrom', 'Dourada', 'Dourado',
  ];
  let cor = null;
  for (const c of coresPossiveis) {
    const re = new RegExp(`\\b${c}\\b`, 'i');
    if (re.test(txt)) { cor = c; break; }
  }
  cor = normalizarCor(cor);

  // 6) Anos — múltiplos padrões
  let anoInicial = null, anoFinal = null;
  // Padrão "2017 a 2025" ou "2017 até 2025"
  let m = txt.match(/(\b\d{4}\b)\s*(?:a|até|ate)\s*(\b\d{4}\b)/i);
  if (m) { anoInicial = parseInt(m[1], 10); anoFinal = parseInt(m[2], 10); }
  // Padrão "2017-2025" ou "2017/2025"
  if (!anoInicial) {
    m = txt.match(/(\b\d{4}\b)\s*[-\/]\s*(\b\d{4}\b)/);
    if (m) { anoInicial = parseInt(m[1], 10); anoFinal = parseInt(m[2], 10); }
  }
  // Padrão "2017 2018 2019 2020 ..." (lista de anos)
  if (!anoInicial) {
    const anos = txt.match(/\b(20\d{2})\b/g);
    if (anos && anos.length >= 2) {
      const ints = anos.map(a => parseInt(a, 10)).sort((a, b) => a - b);
      anoInicial = ints[0];
      anoFinal = ints[ints.length - 1];
    } else if (anos && anos.length === 1) {
      anoInicial = parseInt(anos[0], 10);
      anoFinal = anoInicial;
    }
  }
  // Sanity check: anos no range razoável
  if (anoInicial && (anoInicial < 2000 || anoInicial > 2030)) anoInicial = null;
  if (anoFinal && (anoFinal < 2000 || anoFinal > 2030)) anoFinal = null;
  // Garante anoInicial <= anoFinal
  if (anoInicial && anoFinal && anoInicial > anoFinal) {
    [anoInicial, anoFinal] = [anoFinal, anoInicial];
  }

  return {
    marca:      marca      || null,
    modelo:     modelo     || null,
    posicao:    posicao    || null,
    lado:       lado       || null,
    cor:        cor        || null,
    anoInicial: anoInicial || null,
    anoFinal:   anoFinal   || null,
  };
}
// ============================================================
// FIM Fase 1.5 — Extração
// ============================================================

// ============================================================
// FASE 1.6 - FRENTE A: Detecção de tipo de peça via categoria ML
// ============================================================

// Lista de category_ids ML que correspondem a peças de CARROCERIA/ESTRUTURAL
// (peças que NÃO precisam de PART_NUMBER OEM)
const CATEGORIAS_CARROCERIA = new Set([
  // Capôs e tampas
  'MLB101762', 'MLB101763', 'MLB45307',
  // Portas
  'MLB101764', 'MLB101765', 'MLB101766',
  // Para-choques
  'MLB63801', 'MLB101767', 'MLB101768', 'MLB101769',
  // Paralamas
  'MLB101770', 'MLB101771', 'MLB101772',
  // Retrovisores
  'MLB101773', 'MLB101774', 'MLB101775',
  // Faróis e lanternas
  'MLB101776', 'MLB101777', 'MLB101778',
  // Grades e molduras
  'MLB101779', 'MLB101780',
  // Categoria genérica fallback - tratada como carroceria por segurança
  'MLB1747',
]);

function ehCategoriaCarroceria(categoryId) {
  if (!categoryId || typeof categoryId !== 'string') return false;
  return CATEGORIAS_CARROCERIA.has(categoryId);
}

function ehCodigoInternoBling(codigo) {
  if (!codigo || typeof codigo !== 'string') return true; // sem código = interno
  const c = codigo.trim().toUpperCase();
  const padroesInternos = [
    /^PD\//,             // PD/123
    /^BLG\//,            // BLG/456
    /^COSMOS/,           // COSMOS-XXX
    /^PEC[AÇ]A/,         // PECA-789, PEÇA-XXX
    /^[A-Z]{2,4}\/\d+$/, // 2-4 letras + barra + números (formato Bling padrão)
  ];
  return padroesInternos.some(re => re.test(c));
}

function decidirPartNumber(categoryId, codigoBling) {
  if (ehCategoriaCarroceria(categoryId)) {
    return { enviarPartNumber: false, partNumber: null, motivo: 'carroceria_sem_codigo' };
  }
  if (ehCodigoInternoBling(codigoBling)) {
    return { enviarPartNumber: false, partNumber: null, motivo: 'codigo_interno_bling_descartado' };
  }
  return { enviarPartNumber: true, partNumber: codigoBling.trim(), motivo: 'mecanica_com_codigo_oem' };
}
// ============================================================
// FIM Frente A
// ============================================================

// ============================================================
// FASE 1.5 — Criação de compatibilidades veiculares no ML
// ============================================================

/**
 * Cria compatibilidades veiculares para um item ML após sua publicação.
 * Esta função NUNCA propaga erros — falha de compatibilidade não pode quebrar
 * o fluxo principal de publicação.
 *
 * @param {string} mlbId - ID do item ML (ex: "MLB123456789")
 * @param {object} dadosTitulo - Saída da função extrairDadosDoTitulo
 * @param {string} mlToken - Token Bearer do ML
 * @returns {object} { sucesso, motivo, compatibilidadesCriadas }
 */
async function criarCompatibilidades(mlbId, dadosTitulo, mlToken) {
  const log = (msg) => console.log(`[COMPAT ${mlbId}] ${msg}`);
  const PROXY_URL    = process.env.ML_PROXY_URL    || 'https://ml-proxy.agentemarkt.com';
  const PROXY_SECRET = process.env.ML_PROXY_SECRET || 'agente-ml-proxy-2026';

  try {
    if (!mlbId || !mlToken) {
      log('SKIP: mlbId ou mlToken faltando');
      return { sucesso: false, motivo: 'parametros_invalidos', compatibilidadesCriadas: 0 };
    }
    if (!dadosTitulo || !dadosTitulo.marca || !dadosTitulo.modelo) {
      log(`SKIP: marca/modelo nao extraidos (marca=${dadosTitulo?.marca}, modelo=${dadosTitulo?.modelo})`);
      return { sucesso: false, motivo: 'sem_marca_modelo', compatibilidadesCriadas: 0 };
    }

    // Resolve BRAND value_id
    const brandId = COMPATIBILIDADES_ML.BRAND[dadosTitulo.marca];
    if (!brandId) {
      log(`SKIP: marca "${dadosTitulo.marca}" nao esta no mapa`);
      return { sucesso: false, motivo: 'marca_nao_mapeada', compatibilidadesCriadas: 0 };
    }

    // Resolve MODEL value_id
    const modelosDaMarca = COMPATIBILIDADES_ML.MODEL[brandId] || {};
    const modelId = modelosDaMarca[dadosTitulo.modelo];
    if (!modelId) {
      log(`SKIP: modelo "${dadosTitulo.modelo}" nao esta no mapa para marca "${dadosTitulo.marca}"`);
      return { sucesso: false, motivo: 'modelo_nao_mapeado', compatibilidadesCriadas: 0 };
    }

    // Resolve VEHICLE_YEAR value_ids — gera 1 entrada por ano no range
    const anoIni = dadosTitulo.anoInicial || 2018;
    const anoFim = dadosTitulo.anoFinal   || anoIni;
    const yearMap = COMPATIBILIDADES_ML.VEHICLE_YEAR;
    const familias = [];
    for (let ano = anoIni; ano <= anoFim; ano++) {
      const yearId = yearMap[String(ano)];
      if (!yearId) {
        log(`Aviso: ano ${ano} nao mapeado, pulando`);
        continue;
      }
      familias.push({
        domain_id: COMPATIBILIDADES_ML.domain_id,
        creation_source: 'DEFAULT',
        attributes: [
          { id: 'BRAND',        value_id: brandId },
          { id: 'MODEL',        value_id: modelId },
          { id: 'VEHICLE_YEAR', value_id: yearId  },
        ],
      });
    }

    if (familias.length === 0) {
      log('SKIP: nenhum ano valido encontrado');
      return { sucesso: false, motivo: 'sem_anos_validos', compatibilidadesCriadas: 0 };
    }

    log(`Tentando criar ${familias.length} compatibilidade(s) [${dadosTitulo.marca} ${dadosTitulo.modelo} ${anoIni}-${anoFim}]`);

    // POST /items/{mlb}/compatibilities
    const url = `${PROXY_URL}/items/${mlbId}/compatibilities`;
    const body = { products_families: familias };

    const resp = await fetch(url, {
      method: 'POST',
      headers: {
        'Authorization':  `Bearer ${mlToken}`,
        'X-Proxy-Secret': PROXY_SECRET,
        'Content-Type':   'application/json',
      },
      body: JSON.stringify(body),
    });

    const respText = await resp.text();
    let respJson = null;
    try { respJson = JSON.parse(respText); } catch (e) { /* não é JSON */ }

    if (resp.ok) {
      log(`OK: ${familias.length} compatibilidade(s) criadas`);
      return { sucesso: true, motivo: 'criadas', compatibilidadesCriadas: familias.length };
    }

    log(`ERRO ${resp.status}: ${respText.substring(0, 300)}`);

    // Fallback: se o ML disse que não encontrou produtos, tenta endpoint de exceção
    const mensagemErro = (respJson?.message || respText || '').toLowerCase();
    if (
      resp.status === 400 &&
      (mensagemErro.includes('no products') ||
       mensagemErro.includes('not found') ||
       mensagemErro.includes('product families'))
    ) {
      log('Tentando fallback /compatibilities/exception');
      const urlException = `${PROXY_URL}/items/${mlbId}/compatibilities/exception`;
      const bodyException = {
        comment: `Veiculo ${dadosTitulo.marca} ${dadosTitulo.modelo} ${anoIni}-${anoFim} nao disponivel no catalogo ML`,
      };
      try {
        const respEx = await fetch(urlException, {
          method: 'POST',
          headers: {
            'Authorization':  `Bearer ${mlToken}`,
            'X-Proxy-Secret': PROXY_SECRET,
            'Content-Type':   'application/json',
          },
          body: JSON.stringify(bodyException),
        });
        if (respEx.ok) {
          log('OK: exception registrada');
          return { sucesso: true, motivo: 'exception', compatibilidadesCriadas: 0 };
        }
        const exText = await respEx.text();
        log(`Exception falhou ${respEx.status}: ${exText.substring(0, 200)}`);
      } catch (eEx) {
        log(`Exception erro: ${eEx.message}`);
      }
    }

    return { sucesso: false, motivo: `http_${resp.status}`, compatibilidadesCriadas: 0 };
  } catch (err) {
    // NUNCA propaga erro — apenas loga
    log(`ERRO INESPERADO: ${err.message}`);
    return { sucesso: false, motivo: 'excecao_javascript', compatibilidadesCriadas: 0 };
  }
}
// ============================================================
// FIM Fase 1.5 — Compatibilidades
// ============================================================

// Porta: em produção (Render, Heroku, etc) vem de process.env.PORT;
// localmente cai pra 3000.
const PORT = parseInt(process.env.PORT, 10) || 3000;
const ROOT = __dirname;
const ENV_FILE = path.join(ROOT, '.env');
const USERS_FILE = path.join(ROOT, 'users.json');

// ============================================================
// RATE LIMITER GLOBAL ML — evita HTTP 403/429 por excesso de requests
// ML permite 1500 req/min por seller; usamos 800 (margem de segurança)
// ============================================================
const mlRateLimiter = {
  requests: [],        // timestamps das últimas requisições
  maxPerMinute: 800,
  paused: false,
  totalHoje: 0,
  bloqueios: 0,
  _dayStamp: new Date().toISOString().slice(0,10),

  registrar() {
    const agora = Date.now();
    // reseta contador diário
    const hoje = new Date().toISOString().slice(0,10);
    if (hoje !== this._dayStamp) { this.totalHoje = 0; this._dayStamp = hoje; }
    this.requests.push(agora);
    this.totalHoje++;
    this.requests = this.requests.filter(t => agora - t < 60000);
    if (this.requests.length >= this.maxPerMinute) {
      this.paused = true;
      this.bloqueios++;
      console.log(`⚠️ [rate-limiter] PAUSADO! ${this.requests.length}/${this.maxPerMinute} req/min — aguardando 60s`);
      setTimeout(() => {
        this.paused = false;
        this.requests = [];
        console.log('✅ [rate-limiter] Retomado — contador zerado');
      }, 60000);
    }
  },

  podeFazer() {
    if (this.paused) return false;
    const agora = Date.now();
    this.requests = this.requests.filter(t => agora - t < 60000);
    return this.requests.length < this.maxPerMinute;
  },

  status() {
    const agora = Date.now();
    this.requests = this.requests.filter(t => agora - t < 60000);
    return {
      reqUltimoMinuto: this.requests.length,
      limite: this.maxPerMinute,
      percentual: Math.round((this.requests.length / this.maxPerMinute) * 100),
      pausado: this.paused,
      totalHoje: this.totalHoje,
      bloqueios: this.bloqueios,
    };
  },
};

// Wrapper seguro pra fetch do ML — mesma interface de fetch(), mas bloqueia se estourar limite
// Chamadas pra localhost, Bling, FIPE etc NÃO passam pelo rate limiter (só tráfego ML)
// Wrapper que torna o fetch tolerante a respostas vazias / erros de rede.
// Garante que callers que fazem `await r.json()` nunca crashem.
// Retorna sempre um Response-like com { ok, status, statusText, headers, json(), text() }.
function _fakeResponse({ ok, status, statusText, payload, fromCache }) {
  const jsonBody = typeof payload === 'string' ? payload : JSON.stringify(payload);
  return {
    ok, status, statusText,
    fromCache: !!fromCache,
    headers: { get: () => null },
    json: async () => { try { return JSON.parse(jsonBody); } catch(_) { return payload || {}; } },
    text: async () => jsonBody,
  };
}

// ============================================================
// CACHE ML — última resposta boa por URL (TTL 5min, máx 100 entradas)
// Cai em cache quando o ML retorna 429/vazio pro VPS.
// ============================================================
const mlCache = {
  dados: {}, // { url: { data, timestamp } }
  TTL: 5 * 60 * 1000,

  get(url) {
    const entry = this.dados[url];
    if (entry && (Date.now() - entry.timestamp) < this.TTL) {
      console.log(`📦 [cache] HIT: ${url.substring(0, 80)}`);
      return entry.data;
    }
    return null;
  },

  set(url, data) {
    this.dados[url] = { data, timestamp: Date.now() };
    const keys = Object.keys(this.dados);
    if (keys.length > 100) {
      // remove o mais antigo
      let oldestKey = keys[0];
      let oldestTs = this.dados[oldestKey].timestamp;
      for (const k of keys) {
        if (this.dados[k].timestamp < oldestTs) { oldestKey = k; oldestTs = this.dados[k].timestamp; }
      }
      delete this.dados[oldestKey];
    }
  },

  stats() {
    return { entradas: Object.keys(this.dados).length, ttl_ms: this.TTL };
  },
};

// Headers que parecem navegador — alguns CDNs/edge tratam bot diferente
const _browserHeaders = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Accept': 'application/json',
  'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
};

// Sinaliza se um 429 ou resposta vazia deve tentar cache.
// Evita cache pra endpoints sensíveis (oauth).
function _cacheable(url, method) {
  if ((method || 'GET').toUpperCase() !== 'GET') return false;
  if (!/mercadolibre\.com|mercadolivre\.com/.test(url)) return false;
  if (/\/oauth\/token/.test(url)) return false;
  return true;
}

// Se proxy Cloudflare está ativo E a URL é do ML, reescreve pra passar pelo Worker.
// Retorna { url, options } com X-Proxy-Secret injetado. Logs de diagnóstico ficam aqui
// em vez de no _safeFetch pra não poluir cada retry.
function _applyMLProxy(url, options) {
  if (!ML_USE_PROXY) return { url, options };
  const u = String(url);
  if (!u.startsWith('https://api.mercadolibre.com')) return { url, options };

  const proxyUrl = u.replace('https://api.mercadolibre.com', ML_PROXY_URL);
  const proxiedOptions = { ...options };
  const hdrs = { ...(options && options.headers) };
  hdrs['X-Proxy-Secret'] = ML_PROXY_SECRET;
  proxiedOptions.headers = hdrs;
  return { url: proxyUrl, options: proxiedOptions };
}

async function _rawFetch(url, options) {
  // Merge de headers: defaults de navegador primeiro, options por cima (pra Authorization etc sobrescrever)
  const merged = { ...options, headers: { ..._browserHeaders, ...(options && options.headers) } };
  // Aplica proxy se ativo (reescreve URL + injeta X-Proxy-Secret)
  const routed = _applyMLProxy(url, merged);
  const resp = await fetch(routed.url, routed.options);
  const text = await resp.text().catch(() => '');
  return { resp, text };
}

async function _safeFetch(url, options = {}) {
  const method = (options.method || 'GET').toUpperCase();
  const cacheable = _cacheable(url, method);

  const tryParse = (t) => { try { return JSON.parse(t); } catch(_) { return null; } };

  const degraded = (status, reason) => _fakeResponse({
    ok: false,
    status: status || 0,
    statusText: reason,
    payload: { error: reason, message: reason, hint: 'ML não retornou body — possível rate-limit no IP do VPS' },
  });

  try {
    const { resp, text } = await _rawFetch(url, options);

    // Resposta vazia → tenta cache, senão retry 1x com headers diferentes, senão degrada
    const isEmpty = !text || !text.trim();
    if (isEmpty || resp.status === 429) {
      const reason = resp.status === 429 ? 'ml_rate_limited_upstream' : 'ml_empty_response';

      // 1) fallback cache
      if (cacheable) {
        const cached = mlCache.get(url);
        if (cached != null) {
          return _fakeResponse({
            ok: true, status: 200, statusText: 'cached',
            payload: cached, fromCache: true,
          });
        }
      }

      // 2) retry único após 3s com headers levemente diferentes
      await new Promise(r => setTimeout(r, 3000));
      try {
        const altOptions = {
          ...options,
          headers: {
            ..._browserHeaders,
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
            'Accept-Language': 'en-US,en;q=0.9',
            ...(options.headers || {}),
          },
        };
        // Reaplica proxy no retry também
        const routed2 = _applyMLProxy(url, altOptions);
        const r2 = await fetch(routed2.url, routed2.options);
        const t2 = await r2.text().catch(() => '');
        if (t2 && t2.trim() && r2.status !== 429) {
          const parsed2 = tryParse(t2);
          if (r2.ok && cacheable && parsed2 != null) mlCache.set(url, parsed2);
          return _fakeResponse({
            ok: r2.ok, status: r2.status, statusText: r2.statusText,
            payload: parsed2 != null ? parsed2 : t2,
          });
        }
      } catch(_) { /* cai pro degraded */ }

      return degraded(resp.status, reason);
    }

    // Resposta normal com body
    const parsed = tryParse(text);
    if (resp.ok && cacheable && parsed != null) mlCache.set(url, parsed);
    return _fakeResponse({
      ok: resp.ok,
      status: resp.status,
      statusText: resp.statusText,
      payload: parsed != null ? parsed : text,
    });
  } catch (err) {
    // Erro de rede (DNS, timeout, reset, etc.) → tenta cache
    if (cacheable) {
      const cached = mlCache.get(url);
      if (cached != null) {
        return _fakeResponse({
          ok: true, status: 200, statusText: 'cached-on-network-error',
          payload: cached, fromCache: true,
        });
      }
    }
    return _fakeResponse({
      ok: false,
      status: 0,
      statusText: err.message || 'network_error',
      payload: { error: 'network_error', message: err.message || 'network_error' },
    });
  }
}

async function mlFetch(url, options = {}) {
  const u = String(url);
  if (u.includes('mercadolibre.com') || u.includes('mercadolivre.com')) {
    if (!mlRateLimiter.podeFazer()) {
      console.log(`🚫 [rate-limiter] Bloqueado: ${u.slice(0, 80)}...`);
      // Rate limiter INTERNO estourado — tenta cache antes de falhar
      const cached = _cacheable(u, options.method) ? mlCache.get(u) : null;
      if (cached != null) {
        return _fakeResponse({
          ok: true, status: 200, statusText: 'cached-on-rate-limit-local',
          payload: cached, fromCache: true,
        });
      }
      return _fakeResponse({
        ok: false,
        status: 429,
        statusText: 'Too Many Requests (rate-limiter interno)',
        payload: { error: 'Rate limit interno — aguarde', message: 'rate_limit_local' },
      });
    }
    mlRateLimiter.registrar();
  }
  return _safeFetch(url, options);
}

// Log periódico do rate limiter (a cada 15 min, só se houve atividade)
setInterval(() => {
  const st = mlRateLimiter.status();
  if (st.reqUltimoMinuto > 0 || st.bloqueios > 0) {
    console.log(`📊 [rate-limiter] ${st.reqUltimoMinuto}/${st.limite} req/min (${st.percentual}%) | hoje: ${st.totalHoje} | bloqueios: ${st.bloqueios}${st.pausado ? ' | PAUSADO' : ''}`);
  }
}, 15 * 60 * 1000);

// ============================================================
// FETCH COM RETRY — VPS às vezes recebe resposta vazia/timeout da API ML
// 3 tentativas · timeout 10s · backoff progressivo (2s, 4s, 6s)
// Uso obrigatório no OAuth (callback + refresh) onde resposta vazia
// causava "Unexpected end of JSON input"
// ============================================================
async function fetchComRetry(url, options = {}, tentativas = 3) {
  for (let i = 0; i < tentativas; i++) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 10000); // 10s

      const resp = await fetch(url, { ...options, signal: controller.signal });
      clearTimeout(timeout);

      const text = await resp.text();
      if (!text || text.trim() === '') {
        console.log(`[retry] tentativa ${i+1}/${tentativas}: resposta vazia, tentando novamente...`);
        await new Promise(r => setTimeout(r, 2000 * (i + 1)));
        continue;
      }

      try {
        return { ok: resp.ok, status: resp.status, data: JSON.parse(text) };
      } catch (e) {
        console.log(`[retry] tentativa ${i+1}/${tentativas}: JSON inválido: ${text.substring(0, 100)}`);
        await new Promise(r => setTimeout(r, 2000 * (i + 1)));
        continue;
      }
    } catch (e) {
      console.log(`[retry] tentativa ${i+1}/${tentativas}: erro: ${e.message}`);
      await new Promise(r => setTimeout(r, 2000 * (i + 1)));
      continue;
    }
  }
  return { ok: false, status: 0, data: null, error: 'Todas as tentativas falharam' };
}

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
    // fetchComRetry evita "Unexpected end of JSON input" em picos do VPS
    const result = await fetchComRetry('https://api.mercadolibre.com/oauth/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json' },
      body: new URLSearchParams({
        grant_type:    'refresh_token',
        client_id:     clientId,
        client_secret: clientSecret,
        refresh_token: refresh,
      }),
    });
    const data = result.data || {};
    if (!result.ok || !data.access_token) {
      console.error('[ml-refresh] ❌ falhou:', result.error || data.error_description || data.error || data.message || 'sem access_token');
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

// ============================================================
// COMPATIBILIDADE AUTO — stats + monitor de 6h
// ============================================================
const compatStats = {
  tentativas: 0,
  sucessos:   0,
  falhas:     0,
  ultima:     null,
  ultimaVerificacao: null,
  historico:  [], // últimas 50 resoluções: { ts, itemId, metodo, resumo }
};

// Verifica se token ML está válido (tem + não expirou) — guarda compartilhado dos monitores
function tokenMLValido() {
  const t = loadTokens();
  if (!t.ml_access_token) return false;
  if (t.ml_token_expires_at) {
    const exp = new Date(t.ml_token_expires_at).getTime();
    if (Date.now() > exp) return false; // expirou — não faz requisição
  }
  return true;
}

async function rodarMonitorCompat() {
  if (!tokenMLValido()) return; // sem token ou expirado — silencioso
  if (!mlRateLimiter.podeFazer()) { console.log('⏸️ [compat-monitor] pulando — rate limit atingido'); return; }
  const tokens = loadTokens();
  try {
    console.log('🚗 [compat-monitor] verificando pendências...');
    const r = await fetch(`http://127.0.0.1:${PORT}/api/ml/compat/auto-resolver-todos`, {
      method:'POST',
      headers:{ 'Authorization':'Bearer '+tokens.ml_access_token, 'Content-Type':'application/json' },
    });
    const data = await r.json().catch(()=>({}));
    compatStats.ultimaVerificacao = new Date().toISOString();
    if (data.total > 0) {
      console.log(`🚗 [compat-monitor] ${data.resolvidos}/${data.total} resolvidas automaticamente`);
    } else {
      console.log('🚗 [compat-monitor] nenhum anúncio pendente');
    }
  } catch(err) {
    console.error('🚗 [compat-monitor] erro:', err.message);
  }
}
// A cada 12h (reduzido de 6h pra aliviar rate limit)
setInterval(rodarMonitorCompat, 12 * 60 * 60 * 1000);
// Primeira verificação 2min depois do boot (antes era 30s)
setTimeout(rodarMonitorCompat, 2 * 60 * 1000);

// ============================================================
// SAC AUTOMÁTICO — stats, regras, monitor 10min
// ============================================================
const sacStats = {
  tentativas:   0,
  respondidas:  0,
  falhas:       0,
  ultima:       null,
  ultimaVerif:  null,
  historico:    [], // últimas 50 { ts, questionId, pergunta, categoria, enviada }
};

// Regras de resposta pra autopeças — 10 categorias + fallback
const SAC_REGRAS = [
  { categoria: 'compatibilidade',
    keywords: ['serve','compatível','compativel','encaixa','funciona','cabe','adapta','meu carro','meu veículo','meu veiculo'],
    base: (ctx) => ctx.compatTexto
      ? `Olá! Este produto é compatível com os seguintes veículos: ${ctx.compatTexto}. Por favor, verifique a tabela completa de compatibilidade no anúncio para confirmar se atende ao seu veículo. Qualquer dúvida, estamos à disposição!`
      : `Olá! Por favor, verifique a tabela de compatibilidade no anúncio — lá estão listados todos os veículos compatíveis com esta peça. Se o seu veículo estiver na lista, pode comprar com segurança! Qualquer dúvida, estamos à disposição.`,
  },
  { categoria: 'estoque',
    keywords: ['tem','disponível','disponivel','estoque','pronta entrega','tem disponível'],
    base: () => `Olá! Sim, produto disponível em estoque e pronta entrega! Após a confirmação do pagamento, enviamos em até 24h úteis. Pode comprar com segurança!`,
  },
  { categoria: 'frete',
    keywords: ['frete','envio','entrega','prazo','quanto tempo','dias','chega','demora'],
    base: () => `Olá! O prazo de envio é calculado automaticamente pelo Mercado Envios e aparece no anúncio conforme o seu CEP. Enviamos em até 24h úteis após a confirmação do pagamento. Compre e acompanhe o rastreamento pela plataforma!`,
  },
  { categoria: 'garantia',
    keywords: ['garantia','garante','defeito','troca','devolver','devolução'],
    base: () => `Olá! Este produto possui garantia do fabricante. Caso apresente qualquer defeito, você pode solicitar a devolução ou troca diretamente pela plataforma do Mercado Livre com total segurança. Pode comprar tranquilo!`,
  },
  { categoria: 'nota_fiscal',
    keywords: ['nota fiscal','nf','nfe','nota','cnpj','cpf'],
    base: () => `Olá! Sim, emitimos nota fiscal em todas as vendas. A NF é enviada automaticamente para o e-mail cadastrado na plataforma. Pode comprar com segurança!`,
  },
  { categoria: 'desconto',
    keywords: ['desconto','menor preço','preço','negocia','negociar','mais barato','valor'],
    base: () => `Olá! O preço anunciado já é o melhor que conseguimos oferecer com a qualidade garantida. Aproveite que o produto está com estoque disponível e compre agora! Qualquer dúvida estamos à disposição.`,
  },
  { categoria: 'parcelamento',
    keywords: ['parcela','parcelo','parcelamento','cartão','cartao','crédito','credito','vezes','12x'],
    base: () => `Olá! Sim, aceitamos parcelamento em até 12x sem juros pelo Mercado Pago! Basta selecionar a opção de pagamento na hora da compra. Pode comprar com segurança!`,
  },
  { categoria: 'original',
    keywords: ['original','paralelo','genuíno','genuino','qualidade','marca'],
    base: () => `Olá! Este é um produto de alta qualidade, conforme especificado no anúncio. Todos os detalhes de marca e especificações estão na ficha técnica. Garantia do fabricante inclusa. Pode comprar com confiança!`,
  },
  { categoria: 'kit',
    keywords: ['kit','vem','acompanha','inclui','conteúdo','peças','unidade','quantidade','par','jogo'],
    base: () => `Olá! O conteúdo do kit está descrito na ficha técnica e na descrição do anúncio. Confira os detalhes e, se restar alguma dúvida, estamos à disposição!`,
  },
  { categoria: 'saudacao',
    keywords: ['bom dia','boa tarde','boa noite','olá','oi','ola'],
    base: () => `Olá! Seja bem-vindo à nossa loja! Como posso te ajudar? Se tiver dúvidas sobre compatibilidade, estoque ou envio, estamos à disposição. Pode comprar com segurança!`,
  },
];

function categorizarPerguntaSAC(pergunta, compatTexto) {
  const low = String(pergunta || '').toLowerCase();
  for (const regra of SAC_REGRAS) {
    if (regra.keywords.some(kw => low.includes(kw))) {
      return { categoriaDetectada: regra.categoria, respostaGerada: limparRespostaSAC(regra.base({ compatTexto })) };
    }
  }
  return {
    categoriaDetectada: 'fallback',
    respostaGerada: limparRespostaSAC(`Olá! Obrigado pelo seu interesse. Todas as informações sobre o produto, incluindo compatibilidade, especificações e envio, estão disponíveis na descrição e ficha técnica do anúncio. Caso precise de algo específico, estamos à disposição!`),
  };
}

// Limpa resposta: remove links externos, telefones, emails, WhatsApp (proibido pelo ML) + trunca 2000
function limparRespostaSAC(texto) {
  let t = String(texto || '')
    .replace(/https?:\/\/\S+/gi, '')                                   // URLs
    .replace(/(?:whats?app|wpp|zap)\S*/gi, '')                          // WhatsApp
    .replace(/\b\d{2,5}[\s.-]?\d{3,5}[\s.-]?\d{4}\b/g, '')              // telefones
    .replace(/[\w._%+-]+@[\w.-]+\.[A-Za-z]{2,}/g, '')                   // emails
    .replace(/\s+/g, ' ')
    .trim();
  if (t.length > 2000) t = t.slice(0, 1997) + '...';
  return t;
}

async function rodarMonitorSAC() {
  if (!tokenMLValido()) return;
  if (!mlRateLimiter.podeFazer()) { console.log('⏸️ [sac-monitor] pulando — rate limit atingido'); return; }
  const tokens = loadTokens();
  try {
    const r = await fetch(`http://127.0.0.1:${PORT}/api/ml/sac/pendentes`, {
      headers:{ 'Authorization':'Bearer '+tokens.ml_access_token }});
    const d = await r.json().catch(()=>({}));
    sacStats.ultimaVerif = new Date().toISOString();
    if (d.success && d.total > 0) {
      console.log(`💬 [sac-monitor] ${d.total} perguntas pendentes — respondendo...`);
      const autoR = await fetch(`http://127.0.0.1:${PORT}/api/ml/sac/auto-responder-todos`, {
        method:'POST',
        headers:{ 'Authorization':'Bearer '+tokens.ml_access_token, 'Content-Type':'application/json' },
        body: JSON.stringify({ modoAutomatico:true }),
      });
      const autoD = await autoR.json().catch(()=>({}));
      if (autoD.respondidas > 0) {
        console.log(`💬 [sac-monitor] ${autoD.respondidas}/${autoD.total} respondidas automaticamente ✅`);
      }
    }
  } catch(err) {
    // silencioso (ML pode estar momentaneamente fora)
  }
}
// Frequência reduzida: 10min → 30min (evita consumo de requests e 429)
setInterval(rodarMonitorSAC, 30 * 60 * 1000);
// Primeira verificação 2min após boot (antes eram 45s)
setTimeout(rodarMonitorSAC, 2 * 60 * 1000);

// ============================================================
// ESTOQUE — stats + monitor 30min (só alerta, não força sync sem mapa Bling↔ML)
// ============================================================
const estoqueStats = {
  atualizacoes:   0,
  ultimoSync:     null,
  ultimaVerif:    null,
  semEstoque:     0,
  estoqueBaixo:   0,
  historico:      [], // últimas 50 { ts, itemId, acao, quantidade, sku? }
};

async function rodarMonitorEstoque() {
  if (!tokenMLValido()) return;
  if (!mlRateLimiter.podeFazer()) { console.log('⏸️ [estoque-monitor] pulando — rate limit atingido'); return; }
  const tokens = loadTokens();
  try {
    const r = await fetch(`http://127.0.0.1:${PORT}/api/estoque/ml/todos`, {
      headers:{ 'Authorization':'Bearer '+tokens.ml_access_token }});
    const d = await r.json().catch(()=>({}));
    estoqueStats.ultimaVerif = new Date().toISOString();
    if (d.success) {
      estoqueStats.semEstoque   = d.resumo?.semEstoque || 0;
      estoqueStats.estoqueBaixo = d.resumo?.estoqueBaixo || 0;
      if (estoqueStats.semEstoque > 0 || estoqueStats.estoqueBaixo > 0) {
        console.log(`📦 [estoque-monitor] ${estoqueStats.semEstoque} sem estoque · ${estoqueStats.estoqueBaixo} estoque baixo`);
      }
    }
  } catch(err) { /* silencioso */ }
}
// Frequência reduzida: 30min → 2h (estoque muda raramente)
setInterval(rodarMonitorEstoque, 2 * 60 * 60 * 1000);
// Primeira verificação 2min após boot (antes era 60s)
setTimeout(rodarMonitorEstoque, 2 * 60 * 1000);

// ============================================================
// AUTO-CONFIGURAR WEBHOOKS NO BOOT — tenta 1x 3min após subir.
// Se a app ML não permitir (comum), log cai gracioso.
// ============================================================
setTimeout(async () => {
  try {
    const resp = await fetch(`http://127.0.0.1:${PORT}/api/webhooks/configurar`, { method: 'POST' });
    const data = await resp.json().catch(() => ({}));
    if (data.success) {
      console.log('🔔 [boot] Webhooks configurados automaticamente ✅');
    } else if (data.instrucoes) {
      console.log('🔔 [boot] Webhooks não configurados via API (normal) — configure manualmente no DevCenter');
    }
  } catch (e) { /* silencioso */ }
}, 3 * 60 * 1000);

// ============================================================
// TESTE DE PROXY ML NO BOOT — só se configurado (5s após subir)
// ============================================================
if (ML_USE_PROXY) {
  setTimeout(async () => {
    try {
      const resp = await fetch(ML_PROXY_URL + '/sites/MLB', {
        headers: { 'X-Proxy-Secret': ML_PROXY_SECRET },
      });
      if (resp.ok) {
        console.log('🌐 [boot] Proxy ML funcionando ✅');
      } else {
        console.log(`🌐 [boot] Proxy ML respondeu com status: ${resp.status}`);
      }
    } catch (e) {
      console.log('🌐 [boot] Proxy ML indisponível:', e.message);
    }
  }, 5000);
}

// ============================================================
// WEBHOOKS ML — processamento assíncrono (chamado por /webhooks)
// Stats em global.webhookStats · vendas em global.vendasRecentes
// ============================================================
global.webhookStats   = global.webhookStats   || { total:0, porTopic:{}, historico:[], ultimoRecebido:null };
global.vendasRecentes = global.vendasRecentes || [];
global.publicacoesHoje = global.publicacoesHoje || { count:0, items:[], data: new Date().toDateString() };

// ============================================================
// MENSAGENS PÓS-VENDA — templates + histórico + configuração
// ============================================================
global.mensagensPosVenda = global.mensagensPosVenda || [];
global.msgAutoConfig     = global.msgAutoConfig     || { venda_confirmada:true, produto_enviado:true, produto_entregue:true };
global.msgEnviadas       = global.msgEnviadas       || new Set(); // deduplica por "orderId:tipo"

// Histórico de ajustes de preço (acompanhamento de concorrência)
global.ajustesPreco = global.ajustesPreco || [];

// Fila de publicação com retry (últimas 50 tentativas)
global.filaPublicacao = global.filaPublicacao || [];

// ============================================================
// ERROS DE INTEGRAÇÃO — painel centralizado
// ============================================================
global.errosIntegracao = global.errosIntegracao || [];
function registrarErro(origem, mensagem, detalhes = null) {
  global.errosIntegracao.unshift({
    id: Date.now() + Math.floor(Math.random() * 1000),
    origem,  // 'publicacao' | 'sac' | 'estoque' | 'pedido' | 'webhook' | 'auth'
    mensagem: String(mensagem || ''),
    detalhes,
    data: new Date().toISOString(),
    resolvido: false,
  });
  if (global.errosIntegracao.length > 200) {
    global.errosIntegracao = global.errosIntegracao.slice(0, 200);
  }
  console.error(`❌ [erro-${origem}] ${mensagem}`);
}

// ============================================================
// COMPLIANCE ML — validadores aplicados em toda saída pro ML
// (respostas SAC, mensagens pós-venda, títulos de anúncios)
// ============================================================

/**
 * Sanitiza respostas que vão pro ML. Remove telefone, email, WhatsApp,
 * URLs, redes sociais, CPF/CNPJ, marketplaces concorrentes e pedidos
 * de contato externo. Se > 3 violações, substitui por resposta segura.
 */
function validarRespostaML(texto) {
  const proibidos = {
    telefone:       /(\+?\d{2,3}[\s.-]?\d{4,5}[\s.-]?\d{4})/g,
    celular:        /(\d{2}[\s.-]?9\d{4}[\s.-]?\d{4})/g,
    telefoneFixo:   /\(\d{2}\)\s*\d{4}-\d{4}/g,
    whatsapp:       /(whats\s*app|wpp|zap\s*zap|\bzap\b|\bwhats\b)/gi,
    email:          /[\w.+-]+@[\w.-]+\.\w{2,}/g,
    url:            /(https?:\/\/[^\s]+|www\.[^\s]+|bit\.ly|goo\.gl|t\.me|tinyurl)/gi,
    cpf:            /\d{3}\.\d{3}\.\d{3}-\d{2}/g,
    cnpj:           /\d{2}\.\d{3}\.\d{3}\/\d{4}-\d{2}/g,
    instagram:      /(instagram|\binsta\b|@[a-z_][a-z0-9_.]{2,})/gi,
    facebook:       /(facebook|fb\.com|fb\.me)/gi,
    site:           /(\.com\.br|\.com\b|\.net\b|\.org\b|\.io\b|\.shop\b|\.store\b)/gi,
    concorrente:    /(shopee|amazon|aliexpress|shein|magalu|magazine\s+luiza|americanas|casas\s+bahia|olx|enjoei)/gi,
    marketplace:    /(compre\s+fora|fora\s+d[oa]\s+mercado|meu\s+site|minha\s+loja|loja\s+virtual)/gi,
    dados_pessoais: /(meu\s+telefone|meu\s+numero|meu\s+nu?mero|me\s+chama|liga\s+pra|manda\s+mensagem\s+pra|me\s+liga)/gi,
  };

  let limpo = String(texto || '');
  const violacoes = [];

  for (const [tipo, regex] of Object.entries(proibidos)) {
    regex.lastIndex = 0;
    if (regex.test(limpo)) {
      violacoes.push(tipo);
      regex.lastIndex = 0;
      limpo = limpo.replace(regex, '[REMOVIDO]');
    }
  }

  if (violacoes.length > 0) {
    console.log(`🛡️ [compliance] Violações detectadas: ${violacoes.join(', ')}`);
    try { registrarErro('compliance', `Resposta continha: ${violacoes.join(', ')}`, { textoOriginal: String(texto).substring(0, 120) }); } catch(_){}
    if (violacoes.length >= 3) {
      limpo = 'Olá! Todas as informações sobre o produto estão na descrição e ficha técnica do anúncio. Pode comprar com segurança pelo Mercado Livre!';
    } else {
      // limpa tags [REMOVIDO] duplicadas e espaços
      limpo = limpo.replace(/\[REMOVIDO\](\s*\[REMOVIDO\])+/g, '[REMOVIDO]').replace(/\s{2,}/g, ' ').trim();
    }
  }

  return { texto: limpo, violacoes, limpo: violacoes.length === 0 };
}

/**
 * Remove termos proibidos de títulos de anúncio (réplica/cópia/WhatsApp/
 * concorrentes/urgência falsa) e garante 60 chars.
 */
function validarTituloML(titulo) {
  const termosProibidos = [
    'réplica','replica','cópia','copia','imitação','imitacao',
    'semelhante','similar','genérico','generico','paralelo',
    'inspirado','tipo','modelo de','estilo',
    'whatsapp','whats','zap','telefone','tel:','ligue',
    'email','e-mail','@','instagram','facebook',
    'shopee','amazon','aliexpress','shein','magalu',
    'americanas','casas bahia','olx',
    'grátis','gratis','brinde','promoção relâmpago',
    'últimas unidades','ultimas unidades','só hoje','só amanhã',
  ];

  let limpo = String(titulo || '');
  const encontrados = [];

  for (const termo of termosProibidos) {
    const escaped = termo.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const regex = new RegExp(escaped, 'gi');
    if (regex.test(limpo)) {
      encontrados.push(termo);
      regex.lastIndex = 0;
      limpo = limpo.replace(regex, '');
    }
  }
  limpo = limpo.replace(/\s{2,}/g, ' ').trim().substring(0, 60).trim();

  if (encontrados.length > 0) {
    console.log(`🛡️ [compliance] Termos proibidos removidos do título: ${encontrados.join(', ')}`);
    try { registrarErro('compliance', `Título continha termos proibidos: ${encontrados.join(', ')}`, { tituloOriginal: String(titulo).substring(0, 80) }); } catch(_){}
  }
  return { titulo: limpo, termosRemovidos: encontrados, valido: encontrados.length === 0 };
}

/**
 * Detecta produtos proibidos (peças USADAS de segurança — freio/suspensão/direção/airbag
 * — ou produtos de segurança que exigem certificação especial).
 * Retorna array de strings; 🚫 = erro bloqueante, ⚠️ = aviso.
 */
function validarProdutoProibido(produto) {
  const alertas = [];
  const produtoSeguranca = [
    { regex: /(airbag|air\s*bag)/i,                                 msg: 'Airbag — verificar se é novo e certificado' },
    { regex: /(cinto\s*de?\s*segurança|cinto\s*seguranca)/i,        msg: 'Cinto de segurança — verificar se é novo' },
    { regex: /(vidro\s*(de)?\s*segurança|vidro\s*blindado)/i,       msg: 'Vidro de segurança — pode exigir certificação especial' },
  ];

  const cond = String(produto.condicao || '').toLowerCase();
  if (cond === 'used' || cond === 'usado') {
    const categoriasSeguranca = ['freio','suspensão','suspensao','direção','direcao','airbag'];
    for (const cat of categoriasSeguranca) {
      if (String(produto.titulo || '').toLowerCase().includes(cat)) {
        alertas.push(`🚫 PROIBIDO: ${cat} USADO não pode ser vendido no ML (regra de segurança)`);
      }
    }
  }
  for (const item of produtoSeguranca) {
    if (item.regex.test(String(produto.titulo || ''))) {
      alertas.push(`⚠️ ${item.msg}`);
    }
  }
  return alertas;
}

// ============================================================
// TEMPLATES DE MENSAGEM EDITÁVEIS — persistidos em msg-templates.json
// ============================================================
const TEMPLATES_FILE = path.join(ROOT, 'msg-templates.json');
global.msgTemplates = global.msgTemplates || null;
(function loadTemplatesFromDisk(){
  try {
    if (fs.existsSync(TEMPLATES_FILE)) {
      global.msgTemplates = JSON.parse(fs.readFileSync(TEMPLATES_FILE, 'utf8'));
      console.log('[boot] ✅ Templates de mensagem carregados');
    }
  } catch (e) { /* cai nos defaults */ }
})();

// ============================================================
// CONFIG DE ESTOQUE — reserva de segurança, alerta etc.
// ============================================================
global.estoqueConfig = global.estoqueConfig || {
  reservaSeguranca:    0,  // unidades reservadas (não vão pro ML)
  alertaBaixo:         3,  // alerta quando estoque <= X
  pausarQuandoZero:    true,
  reativarQuandoVoltar:true,
};

// ============================================================
// CONFIG DO AGENTE — estratégias de top sellers 2026
// (primeiras vendas, listagem automática, frete grátis, etc.)
// ============================================================
global.agenteConfig = global.agenteConfig || {
  modoPrimeirasVendas:   false, // true = margem baixa pra construir reputação
  markupNormal:          2.5,   // multiplicador normal (~60% margem)
  markupPrimeirasVendas: 1.5,   // multiplicador baixo (~33% margem)
  vendasParaSairDoModo:  10,    // sair do modo após X vendas
  scoreMinimo:           60,
  margemSegurancaBling:  0.10,  // 10% acima do preço Bling — protege contra cadastro errado
  limiteDiario:          10,
  preferirPremium:       true,  // preferir Premium quando preço ≥ mínimo
  freteGratisMinimo:     79,    // preço mínimo pra frete grátis (ML 2026)
};

// ============================================================
// NOMES POPULARES — traduz termo técnico pro que cliente procura
// (top sellers usam nomes que as pessoas realmente digitam na busca)
// ============================================================
const nomesPopulares = {
  'módulo inflador':       'bolsa airbag',
  'coxim do motor':        'coxim motor',
  'bieleta estabilizadora':'bieleta barra estabilizadora',
  'terminal de direção':   'terminal ponteira direção',
  'pivô de suspensão':     'pivô suspensão dianteira',
  'cilindro mestre':       'cilindro mestre freio',
  'bomba combustível':     'bomba elétrica combustível',
  'sensor oxigênio':       'sonda lambda',
  'catalisador':           'catalisador escapamento',
  'junta cabeçote':        'junta do cabeçote',
  'retentor virabrequim':  'retentor dianteiro virabrequim',
  'kit distribuição':      'kit correia dentada',
  'tensor correia':        'tensor esticador correia',
};
function aplicarNomesPopulares(titulo) {
  let resultado = String(titulo || '');
  for (const [tecnico, popular] of Object.entries(nomesPopulares)) {
    const escaped = tecnico.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const regex = new RegExp(escaped, 'gi');
    if (regex.test(resultado)) resultado = resultado.replace(regex, popular);
  }
  return resultado.substring(0, 60);
}

const mensagensPosVenda = {
  venda_confirmada: (comprador, item) =>
    `Olá ${comprador}! 😊\n\nObrigado pela sua compra de "${item}"!\n\nEstamos preparando seu pedido com todo cuidado. Enviaremos em até 24h úteis após a confirmação do pagamento.\n\nQualquer dúvida, estamos à disposição!\n\nEquipe Agente Marketplace`,

  produto_enviado: (comprador, item, rastreio) =>
    `Olá ${comprador}! 📦\n\nSeu pedido "${item}" acabou de ser ENVIADO!\n\n${rastreio ? '📍 Rastreamento: ' + rastreio + '\n\n' : ''}Acompanhe a entrega pelo app do Mercado Livre.\n\nBoa compra!\nEquipe Agente Marketplace`,

  produto_entregue: (comprador, item) =>
    `Olá ${comprador}! 🎉\n\nSeu pedido "${item}" foi entregue!\n\nEsperamos que esteja tudo perfeito! Se puder, avalie sua compra — sua opinião é muito importante pra nós.\n\n⭐ Avalie pelo app do Mercado Livre\n\nObrigado pela confiança!\nEquipe Agente Marketplace`
};

// ============================================================
// CATÁLOGO SIMULADO DE AUTOPEÇAS — placeholder até Bling conectar
// ============================================================
const catalogoSimulado = [
  {
    id: 'SIM001',
    sku: 'PAST-FREIO-GOL-G5',
    titulo: 'Pastilha de Freio Dianteira Gol G5 G6 Voyage Saveiro',
    descricao: 'Pastilha de freio dianteira de alta performance. Fabricada com materiais de primeira linha, garante frenagem segura e silenciosa. Compatível com Gol G5, G6, Voyage e Saveiro.',
    preco_custo: 35.00,
    estoque: 50,
    marca: 'Frasle',
    categoria_ml: 'MLB120316',
    condicao: 'new',
    imagens: ['https://http2.mlstatic.com/D_NQ_NP_placeholder.jpg'],
    compatibilidade: ['Volkswagen Gol G5 2008-2012','Volkswagen Gol G6 2012-2016','Volkswagen Voyage 2008-2016','Volkswagen Saveiro 2010-2016'],
    peso_g: 450,
    ean: '7890000000001',
    modelo: 'PD/123',
    inmetro: 'REG 012345/2024',
    altura_cm: 5,
    largura_cm: 12,
    comprimento_cm: 15,
    ativo: true,
  },
  {
    id: 'SIM002',
    sku: 'AMORT-TRAS-CIVIC',
    titulo: 'Amortecedor Traseiro Honda Civic 2012 a 2016',
    descricao: 'Amortecedor traseiro para Honda Civic. Qualidade premium com garantia de fábrica. Instalação simples, encaixe perfeito.',
    preco_custo: 120.00,
    estoque: 25,
    marca: 'Monroe',
    categoria_ml: 'MLB120316',
    condicao: 'new',
    imagens: ['https://http2.mlstatic.com/D_NQ_NP_placeholder.jpg'],
    compatibilidade: ['Honda Civic 2012','Honda Civic 2013','Honda Civic 2014','Honda Civic 2015','Honda Civic 2016'],
    peso_g: 1200,
    ean: '7890000000002',
    modelo: 'AM/456',
    inmetro: 'REG 023456/2024',
    altura_cm: 45,
    largura_cm: 12,
    comprimento_cm: 12,
    ativo: true,
  },
  {
    id: 'SIM003',
    sku: 'DISCO-FREIO-COROLLA',
    titulo: 'Disco de Freio Dianteiro Toyota Corolla 2009 a 2019',
    descricao: 'Disco de freio dianteiro ventilado. Alta durabilidade e resistência ao calor. Instalação direta sem adaptações.',
    preco_custo: 85.00,
    estoque: 30,
    marca: 'Fremax',
    categoria_ml: 'MLB120316',
    condicao: 'new',
    imagens: ['https://http2.mlstatic.com/D_NQ_NP_placeholder.jpg'],
    compatibilidade: ['Toyota Corolla 2009-2014','Toyota Corolla 2015-2019'],
    peso_g: 2800,
    ean: '7890000000003',
    modelo: 'BD/789',
    inmetro: 'REG 034567/2024',
    altura_cm: 30,
    largura_cm: 30,
    comprimento_cm: 6,
    ativo: true,
  },
  {
    id: 'SIM004',
    sku: 'FILTRO-OLEO-HB20',
    titulo: 'Filtro de Óleo Hyundai HB20 1.0 1.6 2012 a 2022',
    descricao: 'Filtro de óleo de alta filtragem. Retém impurezas e prolonga a vida útil do motor. Troca fácil e rápida.',
    preco_custo: 18.00,
    estoque: 100,
    marca: 'Tecfil',
    categoria_ml: 'MLB120316',
    condicao: 'new',
    imagens: ['https://http2.mlstatic.com/D_NQ_NP_placeholder.jpg'],
    compatibilidade: ['Hyundai HB20 1.0 2012-2022','Hyundai HB20 1.6 2012-2022','Hyundai HB20S 2013-2022'],
    peso_g: 250,
    modelo: 'FO/012',
    ativo: true,
  },
  {
    id: 'SIM005',
    sku: 'VELA-IGN-ONIX',
    titulo: 'Jogo de Velas de Ignição Chevrolet Onix Prisma 1.0 1.4',
    descricao: 'Jogo com 4 velas de ignição iridium. Melhor desempenho e economia de combustível. Durabilidade de até 60.000 km.',
    preco_custo: 65.00,
    estoque: 40,
    marca: 'NGK',
    categoria_ml: 'MLB120316',
    condicao: 'new',
    imagens: ['https://http2.mlstatic.com/D_NQ_NP_placeholder.jpg'],
    compatibilidade: ['Chevrolet Onix 1.0 2012-2019','Chevrolet Onix 1.4 2012-2019','Chevrolet Prisma 1.0 2013-2019','Chevrolet Prisma 1.4 2013-2019'],
    peso_g: 200,
    modelo: 'VI/345',
    ativo: true,
  },
];

// ============================================================
// ADAPTADOR: Produto Bling → Schema interno (compatível com SIMs)
// ============================================================
function adaptarProdutoBlingParaSimulado(blingProduto) {
  if (!blingProduto || !blingProduto.id) return null;

  // Limpa HTML da descrição
  const stripHtml = (s) => String(s || '')
    .replace(/<[^>]*>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/\s+/g, ' ')
    .trim();

  // Imagens internas (S3 do Bling) + externas + URLs avulsas
  const imagensInternas = (blingProduto.midia?.imagens?.internas || [])
    .map(i => i.link).filter(Boolean);
  const imagensExternas = (blingProduto.midia?.imagens?.externas || [])
    .map(i => i.link).filter(Boolean);
  const imagensURL = (blingProduto.midia?.imagens?.imagensURL || []).filter(Boolean);
  // ML aceita máximo 12 fotos por anúncio
  const imagens = [...imagensInternas, ...imagensExternas, ...imagensURL].slice(0, 12);

  // Marca pode vir como string ou objeto
  const marca = typeof blingProduto.marca === 'string'
    ? blingProduto.marca
    : (blingProduto.marca?.nome || '');

  // Estoque pode vir em vários formatos
  const estoque = blingProduto.estoque?.saldoVirtualTotal
    ?? blingProduto.estoqueSaldo
    ?? 0;

  // Peso em gramas (Bling devolve em kg)
  const pesoG = Math.round((blingProduto.pesoBruto || blingProduto.pesoLiquido || 0) * 1000);

  // Dimensões em cm (Bling: dimensoes.altura/largura/profundidade)
  const altura      = blingProduto.dimensoes?.altura       || 0;
  const largura     = blingProduto.dimensoes?.largura      || 0;
  const comprimento = blingProduto.dimensoes?.profundidade || 0;

  return {
    id:               String(blingProduto.id),
    bling_id:         blingProduto.id,
    sku:              blingProduto.codigo || '',
    titulo:           blingProduto.nome || '',
    descricao:        stripHtml(blingProduto.descricaoComplementar || blingProduto.descricaoCurta || ''),
    descricaoCurta:   stripHtml(blingProduto.descricaoCurta || ''),
    preco:            Number(blingProduto.preco) || 0,
    preco_custo:      Number(blingProduto.precoCusto) || Number(blingProduto.fornecedor?.precoCusto) || 0,
    estoque:          Number(estoque) || 0,
    marca:            marca,
    categoria_ml:     'MLB1747', // fallback autopeças, ML auto-detecta depois
    condicao:         'new',
    imagens:          imagens,
    tem_video:        !!(blingProduto.midia?.video?.url),
    compatibilidade:  [], // Bling não tem nativo, vem do título/descrição
    peso_g:           pesoG,
    ean:              blingProduto.gtin || '',
    modelo:           blingProduto.codigo || '',
    inmetro:          '', // Bling não cadastra inmetro
    altura_cm:        altura,
    largura_cm:       largura,
    comprimento_cm:   comprimento,
    ativo:            blingProduto.situacao === 'A',
    situacao:         blingProduto.situacao || 'I',
    fonte:            'bling',
  };
}

// ============================================================
// QUALIFICAÇÃO: 13 critérios → selo + pendências
// ============================================================
function qualificarProduto(produto) {
  if (!produto) {
    return {
      selo: 'BLOQUEADO', score: 0,
      pendencias: ['produto_inexistente'],
      obrigatorios_ok: 0, recomendados_ok: 0, premium_ok: 0,
    };
  }

  const pend = [];
  let obrig = 0, recom = 0, prem = 0;

  // ===== OBRIGATÓRIOS (8) =====
  if (produto.imagens && produto.imagens.length >= 1) obrig++; else pend.push('sem_foto');
  if (produto.titulo && produto.titulo.length >= 15 && produto.titulo.length <= 60) obrig++;
  else pend.push(produto.titulo ? `titulo_tamanho_invalido(${produto.titulo.length})` : 'sem_titulo');
  if (produto.preco > 0) obrig++; else pend.push('sem_preco_venda');
  if (produto.preco_custo > 0 && produto.preco > produto.preco_custo) obrig++;
  else pend.push(produto.preco_custo > 0 ? 'preco_menor_que_custo' : 'sem_preco_custo');
  if (produto.estoque > 0) obrig++; else pend.push('sem_estoque');
  if (produto.marca && String(produto.marca).trim()) obrig++; else pend.push('sem_marca');
  if (produto.ativo === true) obrig++; else pend.push('produto_inativo_bling');
  if (produto.peso_g > 0) obrig++; else pend.push('sem_peso');

  // ===== RECOMENDADOS (4) =====
  if (produto.imagens && produto.imagens.length >= 3) recom++; else pend.push('menos_de_3_fotos');
  if (produto.descricao && produto.descricao.length >= 100) recom++; else pend.push('descricao_curta');
  if (produto.ean && String(produto.ean).length >= 8) recom++; else pend.push('sem_ean');
  if (produto.altura_cm > 0 && produto.largura_cm > 0 && produto.comprimento_cm > 0) recom++;
  else pend.push('sem_dimensoes');

  // ===== PREMIUM (1) =====
  if (produto.tem_video) prem++;

  // Selo
  let selo;
  if (obrig === 8 && recom === 4 && prem === 1) selo = 'OURO';
  else if (obrig === 8 && recom >= 3)            selo = 'PRONTO';
  else if (obrig === 8)                          selo = 'PUBLICAVEL';
  else                                           selo = 'BLOQUEADO';

  return {
    selo,
    score:               obrig + recom + prem,
    score_max:           13,
    obrigatorios_ok:     obrig,
    obrigatorios_total:  8,
    recomendados_ok:     recom,
    recomendados_total:  4,
    premium_ok:          prem,
    premium_total:       1,
    pendencias:          pend,
    pronto_para_publicar: ['OURO', 'PRONTO', 'PUBLICAVEL'].includes(selo),
  };
}

// Tradução de pendências em mensagens humanas
const PENDENCIAS_LEGIVEIS = {
  sem_foto:                 { texto: 'Sem fotos cadastradas',                                           como_resolver: 'Bling > Produto > Imagens > Adicionar' },
  sem_titulo:               { texto: 'Sem nome/título',                                                  como_resolver: 'Bling > Produto > Dados > Nome' },
  titulo_tamanho_invalido:  { texto: 'Título com tamanho fora de 15-60 caracteres',                      como_resolver: 'Bling > Produto > Dados > Nome (encurtar/alongar)' },
  sem_preco_venda:          { texto: 'Preço de venda zerado',                                            como_resolver: 'Bling > Produto > Dados > Preço' },
  preco_menor_que_custo:    { texto: 'Preço de venda menor ou igual ao custo (prejuízo)',                como_resolver: 'Bling > Produto > Dados > Preço' },
  sem_preco_custo:          { texto: 'Preço de custo não cadastrado',                                    como_resolver: 'Bling > Produto > Dados > Preço de custo' },
  sem_estoque:              { texto: 'Sem estoque',                                                      como_resolver: 'Bling > Produto > Estoque > Adicionar' },
  sem_marca:                { texto: 'Marca não preenchida',                                             como_resolver: 'Bling > Produto > Dados > Marca' },
  produto_inativo_bling:    { texto: 'Produto inativo no Bling',                                         como_resolver: 'Bling > Produto > Situação > Ativar' },
  sem_peso:                 { texto: 'Peso bruto não cadastrado (frete não calcula)',                    como_resolver: 'Bling > Produto > Dados > Peso bruto' },
  menos_de_3_fotos:         { texto: 'Menos de 3 fotos (anúncio fica fraco)',                            como_resolver: 'Bling > Produto > Imagens > Adicionar mais' },
  descricao_curta:          { texto: 'Descrição com menos de 100 caracteres',                            como_resolver: 'Bling > Produto > Descrição complementar' },
  sem_ean:                  { texto: 'Sem código de barras (EAN/GTIN)',                                  como_resolver: 'Bling > Produto > Dados > GTIN' },
  sem_dimensoes:            { texto: 'Sem dimensões (frete pode não calcular)',                          como_resolver: 'Bling > Produto > Dados > Altura/Largura/Profundidade' },
};
function explicarPendencia(codigo) {
  const base = String(codigo).split('(')[0]; // remove parâmetros tipo titulo_tamanho_invalido(8)
  return PENDENCIAS_LEGIVEIS[base] || { texto: codigo, como_resolver: 'Verificar cadastro no Bling' };
}

// ============================================================
// HELPER: Lê token Bling do tokens.json (fallback do header)
// ============================================================
function lerTokenBlingDoArquivo() {
  try {
    const arquivoTokens = path.join(ROOT, 'tokens.json');
    if (!fs.existsSync(arquivoTokens)) return null;
    const tokens = JSON.parse(fs.readFileSync(arquivoTokens, 'utf8'));
    return tokens.bling_access_token || null;
  } catch (e) {
    console.warn('[bling] tokens.json não pôde ser lido:', e.message);
    return null;
  }
}

// ============================================================
// FASE 1 — Anti-duplicidade Bling-ML via SQLite
// Regra: duplicação só se justifica quando produto VENDE.
//   - Sem rendimento >24h → permite republicar (e pausa o velho)
//   - Com rendimento → NÃO publica novo (canibaliza vendas)
//   - <24h no ar → NÃO publica novo (aguarda dados)
// Rendimento = vendas > 0 OU cliques > 50.
// ============================================================

const _stmtInserirPublicacao = db.prepare(`
  INSERT INTO produtos_publicados (bling_id, mlb_id, titulo, preco, publicado_em, status)
  VALUES (?, ?, ?, ?, datetime('now'), 'active')
  ON CONFLICT(mlb_id) DO UPDATE SET
    titulo = excluded.titulo,
    preco = excluded.preco,
    atualizado_em = datetime('now')
`);

const _stmtBuscarPorBling = db.prepare(`
  SELECT mlb_id, titulo, preco, publicado_em, status, vendas, cliques, metricas_atualizado_em,
    julianday('now') - julianday(publicado_em) AS dias_no_ar
  FROM produtos_publicados
  WHERE bling_id = ? AND status IN ('active', 'under_review')
  ORDER BY publicado_em ASC
`);

const _stmtAtualizarMetricas = db.prepare(`
  UPDATE produtos_publicados
  SET vendas = ?, cliques = ?, metricas_atualizado_em = datetime('now'), atualizado_em = datetime('now')
  WHERE mlb_id = ?
`);

const _stmtAtualizarStatus = db.prepare(`
  UPDATE produtos_publicados
  SET status = ?, atualizado_em = datetime('now')
  WHERE mlb_id = ?
`);

const _stmtListarComDuplicatas = db.prepare(`
  SELECT bling_id, COUNT(*) AS qtd
  FROM produtos_publicados
  WHERE status IN ('active', 'under_review')
  GROUP BY bling_id
  HAVING qtd > 1
`);

function registrarPublicacaoBlingML(blingId, mlbId, titulo, preco) {
  try {
    _stmtInserirPublicacao.run(String(blingId), String(mlbId), titulo || '', Number(preco) || 0);
    return true;
  } catch (err) {
    console.error('[mapeamento] erro inserindo:', err.message);
    return false;
  }
}

async function buscarRendimentoMLB(mlbId, mlToken) {
  // Retorna { vendas, cliques, dias_no_ar, status } ou null
  try {
    const proxy = process.env.ML_PROXY_URL || 'https://ml-proxy.agentemarkt.com';
    const proxySecret = process.env.ML_PROXY_SECRET || 'agente-ml-proxy-2026';
    const headers = {
      'Authorization': 'Bearer ' + mlToken,
      'X-Proxy-Secret': proxySecret,
    };

    const itemResp = await fetch(`${proxy}/items/${mlbId}?attributes=id,sold_quantity,date_created,status`, { headers });
    if (!itemResp.ok) return null;
    const item = await itemResp.json();

    const dataPublicacao = new Date(item.date_created);
    const diasNoAr = Math.floor((Date.now() - dataPublicacao.getTime()) / (1000 * 60 * 60 * 24));

    let cliques = 0;
    try {
      const visitsResp = await fetch(`${proxy}/items/${mlbId}/visits/time_window?last=30&unit=day`, { headers });
      if (visitsResp.ok) {
        const visits = await visitsResp.json();
        cliques = (visits.results || []).reduce((sum, day) => sum + (day.total || 0), 0);
      }
    } catch (_) {}

    return {
      vendas: Number(item.sold_quantity) || 0,
      cliques,
      dias_no_ar: diasNoAr,
      status: item.status,
    };
  } catch (err) {
    console.error(`[rendimento ${mlbId}]:`, err.message);
    return null;
  }
}

function temRendimento(metricas) {
  if (!metricas) return false;
  return metricas.vendas > 0 || metricas.cliques > 50;
}

async function checarDuplicidadeBling(blingId, mlToken) {
  // Retorna { jaPublicado, mlbsAtivos, comRendimento, podeRepublicar, motivo, mlbsParaPausar }
  const registros = _stmtBuscarPorBling.all(String(blingId));
  if (registros.length === 0) {
    return { jaPublicado: false, mlbsAtivos: [], comRendimento: [], podeRepublicar: true, motivo: 'primeiro_anuncio', mlbsParaPausar: [] };
  }

  const ativos = [];
  const comRend = [];
  for (const r of registros) {
    // Cache válido se atualizado <6h, senão busca fresh do ML
    const cacheValido = r.metricas_atualizado_em
      && (Date.now() - new Date(r.metricas_atualizado_em).getTime()) < 6 * 60 * 60 * 1000;
    let metricas;
    if (cacheValido) {
      metricas = { vendas: r.vendas, cliques: r.cliques, dias_no_ar: Math.floor(r.dias_no_ar), status: r.status };
    } else {
      const fresh = await buscarRendimentoMLB(r.mlb_id, mlToken);
      if (!fresh) continue;
      _stmtAtualizarMetricas.run(fresh.vendas, fresh.cliques, r.mlb_id);
      if (fresh.status !== r.status) _stmtAtualizarStatus.run(fresh.status, r.mlb_id);
      metricas = fresh;
    }
    if (metricas.status === 'active' || metricas.status === 'under_review') {
      ativos.push({ mlb_id: r.mlb_id, ...metricas });
      if (temRendimento(metricas)) comRend.push({ mlb_id: r.mlb_id, ...metricas });
    }
  }

  if (ativos.length === 0) {
    return { jaPublicado: true, mlbsAtivos: [], comRendimento: [], podeRepublicar: true, motivo: 'todos_pausados_ou_fechados', mlbsParaPausar: [] };
  }

  if (comRend.length > 0) {
    return { jaPublicado: true, mlbsAtivos: ativos, comRendimento: comRend, podeRepublicar: false, motivo: 'tem_mlb_com_rendimento', mlbsParaPausar: [] };
  }

  const algumComProva = ativos.some(a => a.dias_no_ar >= 1);
  if (!algumComProva) {
    return { jaPublicado: true, mlbsAtivos: ativos, comRendimento: [], podeRepublicar: false, motivo: 'aguardando_dados_24h', mlbsParaPausar: [] };
  }

  // Tem ativos sem rendimento >24h — permite republicar e marca os velhos pra pausar
  return { jaPublicado: true, mlbsAtivos: ativos, comRendimento: [], podeRepublicar: true, motivo: 'sem_rendimento_apos_24h', mlbsParaPausar: ativos.map(a => a.mlb_id) };
}

async function pausarMLB(mlbId, mlToken) {
  try {
    const proxy = process.env.ML_PROXY_URL || 'https://ml-proxy.agentemarkt.com';
    const proxySecret = process.env.ML_PROXY_SECRET || 'agente-ml-proxy-2026';
    const resp = await fetch(`${proxy}/items/${mlbId}`, {
      method: 'PUT',
      headers: {
        'Authorization': 'Bearer ' + mlToken,
        'X-Proxy-Secret': proxySecret,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ status: 'paused' }),
    });
    if (resp.ok) {
      _stmtAtualizarStatus.run('paused', mlbId);
      return true;
    }
    return false;
  } catch (err) {
    console.error(`[pausar ${mlbId}]:`, err.message);
    return false;
  }
}

// Score 0-100 pra decidir se vale publicar
// Marcas premium de fabricantes de autopeças (alta qualidade reconhecida)
const MARCAS_FABRICANTE_PREMIUM = ['Frasle','Monroe','Fremax','Tecfil','NGK','Bosch','Continental','Nakata',
  'Cofap','Magneti Marelli','Valeo','Mahle','Mann-Filter','SKF','Gates','Denso','Delphi','Akebono'];
// Montadoras (peças originais geralmente confiáveis)
const MONTADORAS = ['Volkswagen','VW','Fiat','Chevrolet','GM','Ford','Hyundai','Toyota','Honda','Renault',
  'Nissan','Jeep','Peugeot','Citroen','Mitsubishi','Audi','BMW','Mercedes','Mercedes-Benz','Kia','Suzuki','Volvo','Land Rover'];

function scoreProdutoSimulado(p) {
  let score = 0;
  const cfg = global.agenteConfig || {};
  const markup = cfg.modoPrimeirasVendas ? (cfg.markupPrimeirasVendas || 1.5) : (cfg.markupNormal || 2.5);
  const margemSeg = (cfg.margemSegurancaBling != null) ? cfg.margemSegurancaBling : 0.10;

  // PREÇO: se Bling tem preço cadastrado válido, respeita (com margem de segurança).
  // Senão (raro), aplica markup sobre custo.
  const precoBlingValido = p.preco > 0 && p.preco > (p.preco_custo || 0);
  const precoBase = precoBlingValido ? p.preco : ((p.preco_custo || 0) * markup);
  const precoVenda = Math.ceil(precoBase * (1 + margemSeg));

  // Margem real (vs custo)
  const custo = p.preco_custo || 0;
  const margem = custo > 0 ? ((precoVenda - custo) / precoVenda) * 100 : 0;

  // ===== MARGEM (até +30) =====
  if (margem >= 40) score += 30;
  else if (margem >= 25) score += 20;
  else if (margem >= 15) score += 10;

  // ===== ESTOQUE (até +20) — tabela ajustada pra peças únicas/usadas =====
  if (p.estoque >= 50) score += 20;
  else if (p.estoque >= 10) score += 15;
  else if (p.estoque >= 3) score += 10;
  else if (p.estoque >= 1) score += 8;  // peça única ainda vale

  // ===== COMPATIBILIDADE (até +20) =====
  const nc = p.compatibilidade?.length || 0;
  if (nc >= 5) score += 20;
  else if (nc >= 3) score += 15;
  else if (nc >= 1) score += 10;

  // ===== MARCA (5/12/15) =====
  if (MARCAS_FABRICANTE_PREMIUM.includes(p.marca)) score += 15;
  else if (MONTADORAS.includes(p.marca)) score += 12;
  else score += 5;

  // ===== IMAGENS (até +15) =====
  if (p.imagens?.length >= 5) score += 15;
  else if (p.imagens?.length >= 3) score += 12;
  else if (p.imagens?.length >= 1) score += 8;

  // ===== FRETE GRÁTIS (-5/+10) =====
  const minFrete = cfg.freteGratisMinimo || 79;
  if (precoVenda >= minFrete) score += 10;
  else score -= 5;

  // ===== TICKET ALTO (BÔNUS — até +10) =====
  if (precoVenda >= 1000) score += 10;
  else if (precoVenda >= 300) score += 5;

  return {
    score: Math.max(0, Math.min(score, 100)),
    precoVenda,
    precoBase,
    precoBlingValido,
    margem,
    markup,
    margemSegurancaAplicada: margemSeg,
  };
}

// ============================================================
// EXPLICA SCORE — detalhamento passo a passo
// "Como a IA está vendo este produto"
// ============================================================
function explicarScore(p) {
  if (!p) return null;
  const cfg = global.agenteConfig || {};
  const markup = cfg.modoPrimeirasVendas ? (cfg.markupPrimeirasVendas || 1.5) : (cfg.markupNormal || 2.5);
  const margemSeg = (cfg.margemSegurancaBling != null) ? cfg.margemSegurancaBling : 0.10;
  const minFrete = cfg.freteGratisMinimo || 79;

  // Preço (mesma lógica de scoreProdutoSimulado)
  const precoBlingValido = p.preco > 0 && p.preco > (p.preco_custo || 0);
  const precoBase = precoBlingValido ? p.preco : ((p.preco_custo || 0) * markup);
  const precoVenda = Math.ceil(precoBase * (1 + margemSeg));
  const custo = p.preco_custo || 0;
  const margem = custo > 0 ? ((precoVenda - custo) / precoVenda) * 100 : 0;

  const itens = [];
  let total = 0;

  // Origem do preço (informativo, sem pontos)
  itens.push({
    criterio: precoBlingValido ? 'Preço base do Bling' : 'Preço calculado (custo × markup)',
    valor:    `R$ ${precoBase.toFixed(2)}`,
    pontos:   0,
    info:     true,
  });
  itens.push({
    criterio: `Margem segurança (+${(margemSeg*100).toFixed(0)}%)`,
    valor:    `R$ ${precoVenda.toFixed(2)}`,
    pontos:   0,
    info:     true,
  });

  // Margem
  if (margem >= 40)      { itens.push({ criterio:'Margem ≥ 40%',      valor: margem.toFixed(1)+'%', pontos: 30 }); total += 30; }
  else if (margem >= 25) { itens.push({ criterio:'Margem 25-40%',     valor: margem.toFixed(1)+'%', pontos: 20 }); total += 20; }
  else if (margem >= 15) { itens.push({ criterio:'Margem 15-25%',     valor: margem.toFixed(1)+'%', pontos: 10 }); total += 10; }
  else                   { itens.push({ criterio:'Margem < 15%',      valor: margem.toFixed(1)+'%', pontos: 0  }); }

  // Estoque (alinhado com scoreProdutoSimulado: 1-2 = 8 pontos)
  if (p.estoque >= 50)       { itens.push({ criterio:'Estoque ≥ 50',           valor: p.estoque, pontos: 20 }); total += 20; }
  else if (p.estoque >= 10)  { itens.push({ criterio:'Estoque 10-50',          valor: p.estoque, pontos: 15 }); total += 15; }
  else if (p.estoque >= 3)   { itens.push({ criterio:'Estoque 3-10',           valor: p.estoque, pontos: 10 }); total += 10; }
  else if (p.estoque >= 1)   { itens.push({ criterio:'Estoque 1-2 (peça única)', valor: p.estoque, pontos: 8  }); total += 8;  }
  else                       { itens.push({ criterio:'Sem estoque',            valor: 0,         pontos: 0  }); }

  // Compatibilidade
  const nc = p.compatibilidade?.length || 0;
  if (nc >= 5)      { itens.push({ criterio:'5+ compatibilidades',  valor: nc, pontos: 20 }); total += 20; }
  else if (nc >= 3) { itens.push({ criterio:'3-5 compatibilidades', valor: nc, pontos: 15 }); total += 15; }
  else if (nc >= 1) { itens.push({ criterio:'1-3 compatibilidades', valor: nc, pontos: 10 }); total += 10; }
  else              { itens.push({ criterio:'Sem compatibilidade',  valor: 0,  pontos: 0  }); }

  // Marca (3 níveis)
  if (MARCAS_FABRICANTE_PREMIUM.includes(p.marca)) {
    itens.push({ criterio:'Marca fabricante premium', valor: p.marca, pontos: 15 });
    total += 15;
  } else if (MONTADORAS.includes(p.marca)) {
    itens.push({ criterio:'Marca de montadora', valor: p.marca, pontos: 12 });
    total += 12;
  } else {
    itens.push({ criterio:'Marca outras', valor: p.marca || '-', pontos: 5 });
    total += 5;
  }

  // Imagens (progressivo)
  const ni = p.imagens?.length || 0;
  if (ni >= 5)       { itens.push({ criterio:'5+ imagens', valor: ni, pontos: 15 }); total += 15; }
  else if (ni >= 3)  { itens.push({ criterio:'3-4 imagens', valor: ni, pontos: 12 }); total += 12; }
  else if (ni >= 1)  { itens.push({ criterio:'1-2 imagens', valor: ni, pontos: 8  }); total += 8;  }
  else               { itens.push({ criterio:'Sem imagem',   valor: 0,  pontos: 0  }); }

  // Frete grátis
  if (precoVenda >= minFrete) { itens.push({ criterio:`Frete grátis (≥R$${minFrete})`,    valor:'R$'+precoVenda.toFixed(2), pontos: 10 }); total += 10; }
  else                        { itens.push({ criterio:`Sem frete grátis (<R$${minFrete})`, valor:'R$'+precoVenda.toFixed(2), pontos: -5 }); total -= 5;  }

  // Ticket alto (bônus)
  if (precoVenda >= 1000)    { itens.push({ criterio:'Ticket alto (≥R$1000)', valor:'R$'+precoVenda.toFixed(2), pontos: 10 }); total += 10; }
  else if (precoVenda >= 300){ itens.push({ criterio:'Ticket médio (≥R$300)', valor:'R$'+precoVenda.toFixed(2), pontos: 5  }); total += 5;  }

  return {
    score:            Math.max(0, Math.min(total, 100)),
    score_max:        100,
    preco_custo:      custo,
    preco_base:       +precoBase.toFixed(2),
    preco_venda:      precoVenda,
    margem:           +margem.toFixed(1),
    markup:           markup,
    margem_seguranca: `${(margemSeg*100).toFixed(0)}%`,
    detalhamento:     itens,
    recomendacao:     total >= 80 ? '🟢🟢 EXCELENTE' : total >= 60 ? '🟢 PUBLICAR' : total >= 40 ? '🟡 REVISAR' : '🔴 NÃO PUBLICAR',
  };
}

// Rotaciona contador diário à meia-noite
function resetPublicacoesSeNovoDia() {
  const hoje = new Date().toDateString();
  if (global.publicacoesHoje.data !== hoje) {
    global.publicacoesHoje = { count:0, items:[], data: hoje };
  }
}

async function processarWebhookML(notification, headers) {
  if (!notification || typeof notification !== 'object') return;
  const { resource, topic, user_id, attempts, sent, received } = notification;
  if (!topic || !resource) return;

  // Log IP (útil se o usuário quiser whitelistar)
  const ip = headers?.['x-forwarded-for']?.split(',')[0]?.trim() || headers?.['x-real-ip'] || 'direto';
  console.log(`🔔 Webhook [${topic}]: ${resource} · ip=${ip} · attempts=${attempts||1}`);

  // Stats
  global.webhookStats.total++;
  global.webhookStats.porTopic[topic] = (global.webhookStats.porTopic[topic] || 0) + 1;
  global.webhookStats.ultimoRecebido = new Date().toISOString();
  const entry = { topic, resource, user_id, recebido: new Date().toISOString(), processado: false };
  global.webhookStats.historico.unshift(entry);
  if (global.webhookStats.historico.length > 100) global.webhookStats.historico = global.webhookStats.historico.slice(0, 100);

  const tokens = loadTokens();
  const mlToken = tokens.ml_access_token;
  if (!mlToken) {
    console.log('🔔 sem token ML — evento registrado mas não processado');
    return;
  }

  try {
    switch (topic) {
      // ----- Nova pergunta: auto-responde em tempo real via SAC -----
      case 'questions': {
        const qResp = await mlFetch(`https://api.mercadolibre.com${resource}`, {
          headers:{ 'Authorization':'Bearer '+mlToken }});
        const question = await qResp.json();
        if (question.status === 'UNANSWERED' && question.text) {
          const base = `http://127.0.0.1:${PORT}`;
          const usarIA = !!process.env.ANTHROPIC_API_KEY;
          const rotaResposta = usarIA ? '/api/ml/sac/ia-responder' : '/api/ml/sac/auto-responder';
          const autoR = await fetch(`${base}${rotaResposta}`, {
            method:'POST',
            headers:{ 'Authorization':'Bearer '+mlToken, 'Content-Type':'application/json' },
            body: JSON.stringify({ questionId: question.id, pergunta: question.text, itemId: question.item_id }),
          });
          const autoD = await autoR.json();
          if (autoD.success) {
            const sendR = await fetch(`${base}/api/ml/sac/responder`, {
              method:'POST',
              headers:{ 'Authorization':'Bearer '+mlToken, 'Content-Type':'application/json' },
              body: JSON.stringify({ questionId: question.id, texto: autoD.respostaGerada }),
            });
            const sendD = await sendR.json();
            const logTag = usarIA ? '🧠' : '💬';
            const logCat = autoD.fonte || autoD.categoriaDetectada || 'ia';
            console.log(`🔔${logTag} RT: "${(question.text||'').slice(0,50)}..." → ${logCat} → ${sendD.success ? '✅' : '❌'}`);
            sacStats.tentativas++;
            if (sendD.success) {
              sacStats.respondidas++;
              sacStats.historico.unshift({
                ts: new Date().toISOString(), questionId: question.id,
                pergunta: (question.text||'').slice(0,80),
                categoria: autoD.categoriaDetectada || autoD.fonte || 'ia', enviada: true, viaWebhook: true,
              });
              sacStats.historico = sacStats.historico.slice(0, 50);
            } else { sacStats.falhas++; }
            sacStats.ultima = new Date().toISOString();
          }
        }
        break;
      }

      // ----- Pedido: detecta venda paga e registra -----
      case 'orders_v2': {
        const oR = await mlFetch(`https://api.mercadolibre.com${resource}`, {
          headers:{ 'Authorization':'Bearer '+mlToken }});
        const order = await oR.json();
        if (order.status === 'paid') {
          console.log(`🔔🛒💰 VENDA CONFIRMADA! #${order.id} — R$ ${Number(order.total_amount||0).toFixed(2)}`);
          global.vendasRecentes.unshift({
            orderId:  order.id,
            valor:    order.total_amount,
            data:     new Date().toISOString(),
            items:    (order.order_items || []).map(i => ({
              titulo:     i.item?.title,
              quantidade: i.quantity,
              preco:      i.unit_price,
            })),
            comprador: order.buyer?.nickname || 'N/A',
          });
          if (global.vendasRecentes.length > 50) global.vendasRecentes = global.vendasRecentes.slice(0, 50);

          // Mensagem pós-venda automática: "obrigado pela compra"
          if (global.msgAutoConfig.venda_confirmada !== false) {
            const dedupKey = `${order.id}:venda_confirmada`;
            if (!global.msgEnviadas.has(dedupKey)) {
              global.msgEnviadas.add(dedupKey);
              try {
                await fetch(`http://127.0.0.1:${PORT}/api/ml/mensagem/enviar`, {
                  method: 'POST',
                  headers: { 'Authorization': 'Bearer ' + mlToken, 'Content-Type': 'application/json' },
                  body: JSON.stringify({ orderId: order.id, tipo: 'venda_confirmada' }),
                });
              } catch(e) { console.error('✉️ [pós-venda] erro auto venda_confirmada:', e.message); }
            }
          }
        } else {
          console.log(`🔔🛒 Pedido #${order.id} status=${order.status}`);
        }
        break;
      }

      // ----- Pagamento -----
      case 'payments': {
        const pR = await mlFetch(`https://api.mercadolibre.com${resource}`, {
          headers:{ 'Authorization':'Bearer '+mlToken }});
        const payment = await pR.json();
        console.log(`🔔💳 Pag ${payment.id}: ${payment.status} · R$${payment.transaction_amount}`);
        break;
      }

      // ----- Item alterado (status/estoque/preço) -----
      case 'items': {
        const iR = await mlFetch(`https://api.mercadolibre.com${resource}`, {
          headers:{ 'Authorization':'Bearer '+mlToken }});
        const item = await iR.json();
        console.log(`🔔📦 ${item.id}: status=${item.status}, estoque=${item.available_quantity}, preço=R$${item.price}`);
        if (item.status === 'under_review') {
          console.log(`🔔🚨 ALERTA: ${item.id} sob revisão do ML!`);
        }
        break;
      }

      // ----- Envio -----
      case 'shipments': {
        const sR = await mlFetch(`https://api.mercadolibre.com${resource}`, {
          headers:{ 'Authorization':'Bearer '+mlToken, 'x-format-new':'true' }});
        const shipment = await sR.json();
        console.log(`🔔🚚 Envio ${shipment.id}: ${shipment.status}/${shipment.substatus || '-'}`);

        // Mensagem pós-venda automática para shipped / delivered
        const tipoMsg = shipment.status === 'shipped' ? 'produto_enviado'
                      : shipment.status === 'delivered' ? 'produto_entregue'
                      : null;
        if (tipoMsg && global.msgAutoConfig[tipoMsg] !== false) {
          try {
            const orderSearch = await mlFetch(
              `https://api.mercadolibre.com/orders/search?seller=${user_id}&shipping.id=${shipment.id}`,
              { headers:{ 'Authorization':'Bearer '+mlToken } }
            );
            const orderData = await orderSearch.json();
            const orderId = orderData?.results?.[0]?.id;
            if (orderId) {
              const dedupKey = `${orderId}:${tipoMsg}`;
              if (!global.msgEnviadas.has(dedupKey)) {
                global.msgEnviadas.add(dedupKey);
                await fetch(`http://127.0.0.1:${PORT}/api/ml/mensagem/enviar`, {
                  method: 'POST',
                  headers: { 'Authorization': 'Bearer ' + mlToken, 'Content-Type': 'application/json' },
                  body: JSON.stringify({ orderId, tipo: tipoMsg }),
                });
              }
            } else {
              console.log(`✉️ [pós-venda] ${tipoMsg}: não achei orderId para shipment ${shipment.id}`);
            }
          } catch(e) { console.error(`✉️ [pós-venda] erro auto ${tipoMsg}:`, e.message); }
        }
        break;
      }

      // ----- Mensagem pós-venda -----
      case 'messages': {
        console.log(`🔔✉️ msg: ${resource}`);
        break;
      }

      // ----- Reclamação -----
      case 'claims': {
        console.log(`🔔⚠️ RECLAMAÇÃO: ${resource}`);
        break;
      }

      default:
        console.log(`🔔 topic desconhecido [${topic}]: ${resource}`);
    }
    entry.processado = true;
    entry.topic_detail = topic;
  } catch (err) {
    console.error(`🔔 erro processando [${topic}]:`, err.message);
    entry.processado = false;
    entry.erro = err.message;
  }
}

// ============================================================
// PRECIFICAÇÃO — helpers (calcular, simular). Replica dos top sellers.
// Fórmula: Preço = (Custo + Embalagem + Frete + TaxaFixa) / (1 - comissão - margem - imposto)
// ============================================================
const ML_COMISSOES_2026 = { classico: 0.13, premium: 0.17 };
const ML_TAXA_FIXA_ABAIXO_79 = 6.75;
const ML_SUBSIDIO_FRETE = 0.5; // ML subsidia 50% se MercadoLíder + preço > R$79
const ML_TABELA_FRETE_2026 = [
  { ate: 0.3,  valor: 15.90 },
  { ate: 0.5,  valor: 18.90 },
  { ate: 1,    valor: 22.90 },
  { ate: 2,    valor: 28.90 },
  { ate: 5,    valor: 35.90 },
  { ate: 10,   valor: 45.90 },
  { ate: 30,   valor: 65.90 },
  { ate: 9999, valor: 99.90 },
];
const _r2 = n => Math.round((Number(n) || 0) * 100) / 100;
const _r1 = n => Math.round((Number(n) || 0) * 10) / 10;
function _freteVendedor(pesoKg, freteGratis) {
  const f = ML_TABELA_FRETE_2026.find(x => pesoKg <= x.ate)?.valor || 99.90;
  return { freteTotal: f, custoVendedor: freteGratis ? f * (1 - ML_SUBSIDIO_FRETE) : 0 };
}

function calcularPrecoTopSeller(input) {
  const {
    custo,
    margemDesejada = 20,
    tipoAnuncio    = 'premium',
    pesoKg         = 1,
    imposto        = 0,
    custoEmbalagem = 2,
    freteGratis    = true,
  } = input || {};

  if (!custo || custo <= 0) return { success:false, error:'Custo inválido' };

  const comissaoML     = ML_COMISSOES_2026[tipoAnuncio] ?? ML_COMISSOES_2026.premium;
  const margemDecimal  = margemDesejada / 100;
  const impostoDecimal = imposto / 100;

  const { freteTotal, custoVendedor: custFreteVendedor } = _freteVendedor(pesoKg, freteGratis);
  const custoBase = Number(custo) + Number(custoEmbalagem) + custFreteVendedor;

  const divisor = 1 - comissaoML - margemDecimal - impostoDecimal;
  if (divisor <= 0) {
    return {
      success:false,
      error:'Margem + comissão + imposto excedem 100%. Reduza a margem.',
      dica:'Top sellers trabalham com margem entre 15-25% em autopeças',
    };
  }

  let precoVenda = custoBase / divisor;
  let taxaFixa = 0;
  if (precoVenda < 79) {
    taxaFixa = ML_TAXA_FIXA_ABAIXO_79;
    precoVenda = (custoBase + taxaFixa) / divisor;
  }
  // Arredondamento psicológico (,90)
  precoVenda = Math.ceil(precoVenda) - 0.10;
  if (precoVenda < custoBase + 1) precoVenda = Math.ceil(custoBase / divisor) + 0.90;

  const comissaoValor = precoVenda * comissaoML;
  const impostoValor  = precoVenda * impostoDecimal;
  const lucroLiquido  = precoVenda - custo - custoEmbalagem - comissaoValor - taxaFixa - impostoValor - custFreteVendedor;
  const margemReal    = (lucroLiquido / precoVenda) * 100;

  let nivel, emoji, alerta;
  if (lucroLiquido < 0)       { nivel='prejuizo';  emoji='🚨'; alerta='PREJUÍZO! Não publique neste preço. Aumente o preço ou reduza custos.'; }
  else if (margemReal < 5)    { nivel='critico';   emoji='🔴'; alerta='Margem crítica (<5%). Top sellers evitam operar abaixo de 15%.'; }
  else if (margemReal < 15)   { nivel='apertado';  emoji='🟡'; alerta='Margem apertada. Considere criar kit pra diluir custos fixos.'; }
  else if (margemReal < 25)   { nivel='saudavel';  emoji='🟢'; alerta='Margem saudável! Dentro do padrão dos top sellers.'; }
  else                        { nivel='excelente'; emoji='💎'; alerta='Margem excelente! Produto muito lucrativo.'; }

  const dicas = [];
  if (precoVenda < 79)             dicas.push('💡 Preço < R$79 = taxa fixa R$6,75. Considere criar KIT pra passar de R$79.');
  if (tipoAnuncio === 'classico')  dicas.push('💡 Top sellers usam PREMIUM (17% comissão mas 3x mais visibilidade + parcela 12x).');
  if (!freteGratis)                dicas.push('💡 ML prioriza anúncios com frete grátis. Embuta o frete no preço.');
  if (margemReal > 10 && margemReal < 15) dicas.push('💡 Margem OK mas apertada. Se concorrente baixar preço, sua margem vira pó.');

  // Preço mínimo (margem 0% — ponto de equilíbrio)
  const precoMinimo = _r2((custoBase + taxaFixa) / (1 - comissaoML - impostoDecimal));

  // Preço sugerido pra kit (2 unidades — dilui taxa fixa e frete)
  const kitCustoBase = (custo * 2) + custoEmbalagem + custFreteVendedor;
  let precoKit = kitCustoBase / divisor;
  if (precoKit < 79) precoKit = (kitCustoBase + ML_TAXA_FIXA_ABAIXO_79) / divisor;
  precoKit = Math.ceil(precoKit) - 0.10;
  const kitTaxa = precoKit < 79 ? ML_TAXA_FIXA_ABAIXO_79 : 0;
  const lucroKit = precoKit - (custo * 2) - custoEmbalagem - (precoKit * comissaoML) - kitTaxa - (precoKit * impostoDecimal) - custFreteVendedor;
  const margemKit = (lucroKit / precoKit) * 100;

  return {
    success: true,
    precoSugerido: _r2(precoVenda),
    precoMinimo,
    precoKit: { preco: _r2(precoKit), lucro: _r2(lucroKit), margem: _r1(margemKit) + '%' },
    breakdown: {
      custoProduto:  _r2(custo),
      custoEmbalagem,
      custoFrete:    _r2(custFreteVendedor),
      freteTotal:    freteTotal,
      subsidioML:    _r2(freteTotal * ML_SUBSIDIO_FRETE),
      comissaoML:    { percentual: (comissaoML*100)+'%', valor: _r2(comissaoValor) },
      taxaFixa,
      imposto:       { percentual: imposto+'%', valor: _r2(impostoValor) },
      custoTotal:    _r2(custo + custoEmbalagem + comissaoValor + taxaFixa + impostoValor + custFreteVendedor),
      lucroLiquido:  _r2(lucroLiquido),
      margemReal:    _r1(margemReal) + '%',
    },
    status: { nivel, emoji, alerta },
    dicas,
    tipoAnuncio,
    comparativo: {
      classico: {
        preco: _r2(Math.ceil((custoBase / Math.max(0.01, 1 - 0.13 - margemDecimal - impostoDecimal))) - 0.10),
        comissao: '13%',
      },
      premium: {
        preco: _r2(Math.ceil((custoBase / Math.max(0.01, 1 - 0.17 - margemDecimal - impostoDecimal))) - 0.10),
        comissao: '17%',
      },
    },
  };
}

function simularLucroVenda(input) {
  const {
    precoVenda, custo,
    tipoAnuncio    = 'premium',
    pesoKg         = 1,
    freteGratis    = true,
    imposto        = 0,
    custoEmbalagem = 2,
  } = input || {};

  if (!precoVenda || precoVenda <= 0) return { success:false, error:'Preço de venda inválido' };
  if (!custo || custo < 0)            return { success:false, error:'Custo inválido' };

  const comissaoML    = ML_COMISSOES_2026[tipoAnuncio] ?? ML_COMISSOES_2026.premium;
  const comissaoValor = precoVenda * comissaoML;
  const taxaFixa      = precoVenda < 79 ? ML_TAXA_FIXA_ABAIXO_79 : 0;
  const impostoValor  = precoVenda * (imposto / 100);
  const { custoVendedor: custoFrete } = _freteVendedor(pesoKg, freteGratis);

  const lucroLiquido = precoVenda - custo - custoEmbalagem - comissaoValor - taxaFixa - impostoValor - custoFrete;
  const margemReal   = (lucroLiquido / precoVenda) * 100;

  let status='verde', emoji='🟢';
  if      (lucroLiquido < 0)     { status='prejuizo'; emoji='🚨'; }
  else if (margemReal   < 5)     { status='vermelho'; emoji='🔴'; }
  else if (margemReal   < 15)    { status='amarelo';  emoji='🟡'; }
  else if (margemReal  >= 25)    { status='diamante'; emoji='💎'; }

  return {
    success: true,
    precoVenda: _r2(precoVenda),
    custo: _r2(custo),
    descontos: {
      comissaoML: { percentual: (comissaoML*100)+'%', valor: _r2(comissaoValor) },
      taxaFixa,
      imposto:    { percentual: imposto+'%', valor: _r2(impostoValor) },
      embalagem:  custoEmbalagem,
      frete:      _r2(custoFrete),
    },
    lucroLiquido: _r2(lucroLiquido),
    margemReal:   _r1(margemReal) + '%',
    status, emoji,
    veredicto: lucroLiquido < 0 ? '🚨 PREJUÍZO! NÃO VENDA neste preço!' :
               margemReal < 5  ? '🔴 Margem muito baixa — aumente o preço' :
               margemReal < 15 ? '🟡 Margem apertada — cuidado com concorrência' :
               margemReal < 25 ? '🟢 Margem saudável — padrão top seller' :
                                 '💎 Margem excelente — produto muito lucrativo',
  };
}

// ============================================================
// GERADOR SEO — helpers (títulos, descrições, anti-duplicidade)
// Regras ML: máx 60 chars, PMME (Produto+Marca+Modelo+Especificação),
// palavra-chave no início, sem preço/promoção/caps/especiais,
// sem palavras repetidas, hífen para separar informações.
// ============================================================
function _limparNomeBling(nome) {
  return String(nome || '')
    .replace(/\b(p[çc]|und|un|par|jg|kit|cx)\b\.?/gi, '')
    .replace(/[★♦●◆■□▪▫]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function _detectarPeca(nome) {
  const pecas = {
    'farol':'Farol','lanterna':'Lanterna','para-choque':'Para-Choque',
    'parachoque':'Para-Choque','paragolpe':'Para-Choque',
    'pastilha':'Pastilha de Freio','disco':'Disco de Freio',
    'amortecedor':'Amortecedor','mola':'Mola','bandeja':'Bandeja',
    'terminal':'Terminal de Direção','bieleta':'Bieleta',
    'pivô':'Pivô','pivo':'Pivô','rolamento':'Rolamento',
    'correia':'Correia','bomba':'Bomba','filtro':'Filtro',
    'vela':'Vela de Ignição','bobina':'Bobina','sensor':'Sensor',
    'retrovisor':'Retrovisor','espelho':'Espelho Retrovisor',
    'vidro':'Vidro','maçaneta':'Maçaneta','fechadura':'Fechadura',
    'radiador':'Radiador','condensador':'Condensador',
    'compressor':'Compressor','evaporador':'Evaporador',
    'embreagem':'Kit Embreagem','volante':'Volante Motor',
    'junta':'Junta','retentor':'Retentor','coxim':'Coxim',
    'barra':'Barra Estabilizadora','calha':'Calha de Chuva',
    'palheta':'Palheta Limpador','limpador':'Palheta Limpador',
    'bateria':'Bateria','alternador':'Alternador','motor de partida':'Motor de Partida',
  };
  const low = String(nome || '').toLowerCase();
  for (const [chave, valor] of Object.entries(pecas)) {
    if (low.includes(chave)) return valor;
  }
  return _limparNomeBling(nome).split(/\s+/).slice(0, 3).join(' ') || 'Peça';
}

function _detectarVeiculo(nome, veiculosCompativeis) {
  if (veiculosCompativeis && veiculosCompativeis.length > 0) {
    const v = veiculosCompativeis[0];
    return `${v.marca || ''} ${v.modelo || ''} ${v.anos || ''}`.replace(/\s+/g,' ').trim();
  }
  const modelos = ['gol','voyage','saveiro','polo','fox','onix','prisma','cobalt','spin',
    'tracker','cruze','uno','palio','siena','strada','argo','mobi','toro','cronos',
    'ka','fiesta','focus','ecosport','hb20','creta','corolla','hilux','etios',
    'civic','fit','hr-v','city','kwid','sandero','logan','duster','kicks',
    'renegade','compass','l200','pajero'];
  const low = String(nome || '').toLowerCase();
  for (const m of modelos) {
    const re = new RegExp(`\\b${m.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')}\\b`);
    if (re.test(low)) return m.charAt(0).toUpperCase() + m.slice(1);
  }
  return '';
}

function _detectarAnos(nome) {
  const s = String(nome || '');
  const range = s.match(/(\d{4})\s*(?:a|até|à|-)\s*(\d{4})/);
  if (range) return `${range[1]}-${range[2]}`;
  const uni = s.match(/\b(20[0-3]\d|19\d{2})\b/);
  return uni ? uni[1] : '';
}

// Remove palavras repetidas, limpa espaços, trunca em 60 preservando palavra
function _finalizarTitulo(raw) {
  const seen = new Set();
  const out = [];
  for (const p of String(raw).replace(/\s+/g,' ').trim().split(' ')) {
    const k = p.toLowerCase();
    if (k && !seen.has(k)) { seen.add(k); out.push(p); }
  }
  let t = out.join(' ');
  if (t.length > 60) {
    t = t.substring(0, 60);
    const lastSpace = t.lastIndexOf(' ');
    if (lastSpace > 40) t = t.substring(0, lastSpace);
  }
  return t.trim();
}

function gerarTitulosSEO({ nome, marca, partNumber, categoria, veiculosCompativeis }) {
  const nomeLimpo = _limparNomeBling(nome);
  const peca = _detectarPeca(nomeLimpo);
  const marcaStr = (marca || '').trim();
  const veiculo = _detectarVeiculo(nomeLimpo, veiculosCompativeis);
  const anos = _detectarAnos(nomeLimpo) || (veiculosCompativeis?.[0]?.anos || '');

  // A — SEO: Peça + Marca + Modelo + Anos
  const tA = _finalizarTitulo(`${peca} ${marcaStr} ${veiculo} ${anos}`);
  // B — Aplicação: Para Veículo + Peça + Marca
  const tB = _finalizarTitulo(
    veiculo
      ? `Para ${veiculo} ${anos} - ${peca} ${marcaStr}`
      : `${peca} ${marcaStr} Original ${anos}`
  );
  // C — Benefício: Peça Original + Marca + Veículo + Anos
  const tC = _finalizarTitulo(
    veiculo
      ? `${peca} Original ${marcaStr} ${veiculo} ${anos}`
      : `${peca} ${marcaStr} Novo Original ${anos}`
  );

  return [
    { variacao:'A', foco:'SEO (Produto + Marca + Modelo)',    titulo:tA, chars:tA.length },
    { variacao:'B', foco:'Aplicação (Para Veículo)',          titulo:tB, chars:tB.length },
    { variacao:'C', foco:'Benefício (Original + Garantia)',   titulo:tC, chars:tC.length },
  ];
}

function gerarDescricoesSEO({ nome, marca, partNumber, especificacoes, veiculosCompativeis }) {
  const compatTexto = veiculosCompativeis?.length
    ? veiculosCompativeis.map(v => `${v.marca||''} ${v.modelo||''} ${v.anos||''}`.trim()).join(', ')
    : 'Consulte compatibilidade';

  const descA = [
    `${String(nome||'').toUpperCase()}`,
    '',
    'ESPECIFICAÇÕES TÉCNICAS:',
    `• Marca: ${marca || 'Consulte'}`,
    partNumber ? `• Part Number: ${partNumber}` : '',
    `• Condição: Novo`,
    especificacoes ? `• ${especificacoes}` : '',
    '',
    'COMPATIBILIDADE:',
    `• ${compatTexto}`,
    '',
    'GARANTIA E ENVIO:',
    '• Produto novo na caixa',
    '• Garantia do fabricante',
    '• Nota fiscal inclusa',
    '• Envio rápido para todo Brasil',
    '',
    'Em caso de dúvidas sobre compatibilidade, consulte a tabela de veículos compatíveis no anúncio.',
  ].filter(Boolean).join('\n');

  const descB = [
    `🔧 ${nome || ''}`,
    '',
    `Peça ${marca || 'de qualidade'} com garantia do fabricante.`,
    '',
    '✅ Por que comprar conosco?',
    '• Peça original/equivalente de alta qualidade',
    '• Encaixe perfeito — sem adaptações',
    '• Nota fiscal em todas as compras',
    '• Envio rápido e seguro para todo Brasil',
    '• Suporte pós-venda via chat',
    '',
    `📋 Compatível com: ${compatTexto}`,
    '',
    partNumber ? `📦 Part Number: ${partNumber}` : '',
    '',
    'Não arrisque com peças de procedência duvidosa.',
    'Compre com segurança e garantia!',
  ].filter(Boolean).join('\n');

  const descC = [
    `${nome || ''} - ${marca || 'Original'}`,
    partNumber ? `Part Number: ${partNumber}` : '',
    `Condição: Novo, na caixa`,
    `Compatibilidade: ${compatTexto}`,
    `Garantia do fabricante`,
    `Nota fiscal inclusa`,
    `Envio em até 24h úteis`,
    '',
    'Consulte a tabela de compatibilidade para confirmar se serve no seu veículo.',
  ].filter(Boolean).join('\n');

  return [
    { variacao:'A', foco:'Técnica (Especificações)',  descricao:descA, chars:descA.length },
    { variacao:'B', foco:'Comercial (Benefícios)',    descricao:descB, chars:descB.length },
    { variacao:'C', foco:'Direta (Objetiva)',         descricao:descC, chars:descC.length },
  ];
}

// Similaridade de Jaccard entre dois títulos (0..1)
function _jaccard(a, b) {
  const wa = new Set(String(a).toLowerCase().trim().split(/\s+/).filter(Boolean));
  const wb = new Set(String(b).toLowerCase().trim().split(/\s+/).filter(Boolean));
  if (wa.size === 0 && wb.size === 0) return 1;
  const inter = [...wa].filter(w => wb.has(w)).length;
  const uni   = new Set([...wa, ...wb]).size;
  return uni === 0 ? 0 : inter / uni;
}

async function verificarDuplicidadeSEO(titulo, token) {
  const meResp = await mlFetch('https://api.mercadolibre.com/users/me', {
    headers:{ 'Authorization':'Bearer '+token },
  });
  const me = await meResp.json();
  const itemsResp = await mlFetch(
    `https://api.mercadolibre.com/users/${me.id}/items/search?status=active&limit=50`,
    { headers:{ 'Authorization':'Bearer '+token }}
  );
  const itemsData = await itemsResp.json().catch(()=>({}));
  const itemIds = itemsData.results || [];
  if (itemIds.length === 0) {
    return { duplicado:false, similaridade:0, tituloSimilar:null, aviso:'✅ Primeiro anúncio — sem duplicidade' };
  }
  const batch = itemIds.slice(0, 20).join(',');
  const detResp = await mlFetch(`https://api.mercadolibre.com/items?ids=${batch}&attributes=title`, {
    headers:{ 'Authorization':'Bearer '+token },
  });
  const det = await detResp.json().catch(()=>[]);
  const titulos = Array.isArray(det) ? det.map(d => d.body?.title || '').filter(Boolean) : [];
  let maxSim = 0, tituloSimilar = '';
  for (const ex of titulos) {
    const sim = _jaccard(titulo, ex);
    if (sim > maxSim) { maxSim = sim; tituloSimilar = ex; }
  }
  const duplicado = maxSim > 0.6;
  return {
    duplicado,
    similaridade: Math.round(maxSim * 100),
    tituloSimilar: duplicado ? tituloSimilar : null,
    aviso: duplicado
      ? `⚠️ Título ${Math.round(maxSim*100)}% similar a "${tituloSimilar}" — ML pode penalizar`
      : `✅ Título único (${Math.round(maxSim*100)}% máx de similaridade)`,
  };
}

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
  const r = await mlFetch('https://api.mercadolibre.com/oauth/token', {
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
  const r = await mlFetch('https://api.mercadolibre.com' + pathname, {
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
    // ============= PWA MANIFEST =============
    if (u.pathname === '/manifest.json' && req.method === 'GET') {
      return send(res, 200, {
        name: 'Agente Marketplace',
        short_name: 'AM',
        description: 'Agente autônomo de autopeças para Mercado Livre',
        start_url: '/',
        scope: '/',
        display: 'standalone',
        orientation: 'portrait',
        background_color: '#0a0a1a',
        theme_color: '#16a34a',
        lang: 'pt-BR',
        icons: [
          { src: '/icon-192.png', sizes: '192x192', type: 'image/png', purpose: 'any maskable' },
          { src: '/icon-512.png', sizes: '512x512', type: 'image/png', purpose: 'any maskable' },
        ],
      });
    }

    // ============= WEBHOOK ML — PRIORIDADE MÁXIMA =============
    // Responde 200 em < 500ms e processa em background.
    // URL pública: https://agentemarkt.com/webhooks
    if (u.pathname === '/webhooks' && req.method === 'POST') {
      const notification = await readBody(req).catch(() => ({}));
      // Responde ANTES de processar — requisito ML (< 500ms ou topic é desativado)
      send(res, 200, '');
      setImmediate(() => processarWebhookML(notification, req.headers).catch(e => {
        console.error('🔔 erro async webhook:', e.message);
      }));
      return;
    }

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
      const redirectUri = process.env.ML_REDIRECT_URI || env.ML_REDIRECT_URI || 'https://agentemarkt.com/callback';
      if (!clientId) return send(res, 400, 'Preencha ML_CLIENT_ID em .env', 'text/plain');
      const auth = 'https://auth.mercadolivre.com.br/authorization?' + new URLSearchParams({
        response_type:'code',
        client_id: clientId,
        redirect_uri: redirectUri,
      });
      res.writeHead(302, { Location: auth }); return res.end();
    }

    // Callback OAuth — recebe ?code=TG-xxx, troca por access_token, salva e redireciona pro painel
    if (u.pathname === '/callback' && req.method === 'GET') {
      const code  = u.query.code;
      const error = u.query.error;
      // Se veio erro do ML (usuário cancelou/app inválido), redireciona com mensagem
      if (error) {
        const msg = encodeURIComponent(u.query.error_description || error);
        res.writeHead(302, { Location:`/?ml_error=${msg}` });
        return res.end();
      }
      if (!code) return send(res, 400, 'Sem "code" na URL — autorize primeiro em /login', 'text/plain');
      env = loadEnv();
      const clientId     = process.env.ML_CLIENT_ID     || env.ML_CLIENT_ID;
      const clientSecret = process.env.ML_CLIENT_SECRET || env.ML_CLIENT_SECRET;
      const redirectUri  = process.env.ML_REDIRECT_URI  || env.ML_REDIRECT_URI || 'https://agentemarkt.com/callback';
      if (!clientId || !clientSecret) {
        return send(res, 500, 'ML_CLIENT_ID/ML_CLIENT_SECRET não configurados no .env do servidor', 'text/plain');
      }
      try {
        // fetchComRetry: 3 tentativas · timeout 10s · backoff progressivo
        // Resolve "Unexpected end of JSON input" quando o VPS recebe resposta vazia do ML
        const result = await fetchComRetry('https://api.mercadolibre.com/oauth/token', {
          method: 'POST',
          headers: { 'Content-Type':'application/x-www-form-urlencoded', 'Accept':'application/json' },
          body: new URLSearchParams({
            grant_type:   'authorization_code',
            client_id:    clientId,
            client_secret:clientSecret,
            code,
            redirect_uri: redirectUri,
          }),
        });
        if (!result.ok || !result.data?.access_token) {
          const msg = encodeURIComponent(result.data?.message || result.data?.error || result.error || 'Falha ao trocar code por token');
          console.error('[ml/callback] ❌', result.data || result.error);
          res.writeHead(302, { Location:`/?ml_error=${msg}` });
          return res.end();
        }
        const data = result.data;
        saveEnv({
          ML_ACCESS_TOKEN:     data.access_token,
          ML_REFRESH_TOKEN:    data.refresh_token,
          ML_USER_ID:          String(data.user_id),
          ML_TOKEN_EXPIRES_AT: String(Date.now() + data.expires_in * 1000),
        });
        persistMLTokens(data);  // persistência adicional em tokens.json
        console.log(`[ml/callback] ✅ conectado · user_id=${data.user_id}`);
        res.writeHead(302, { Location:'/?connected=1&ml_user_id=' + encodeURIComponent(String(data.user_id)) });
        return res.end();
      } catch(err) {
        console.error('[ml/callback] exceção:', err.message);
        const msg = encodeURIComponent(err.message);
        res.writeHead(302, { Location:`/?ml_error=${msg}` });
        return res.end();
      }
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
      const r = await mlFetch(`https://api.mercadolibre.com/sites/MLB/search?q=${q}&limit=${limit}`);
      return send(res, r.status, await r.json());
    }
    // Detalhes de um item público
    if (u.pathname.startsWith('/api/public/item/')) {
      const id = u.pathname.split('/').pop();
      const r = await mlFetch(`https://api.mercadolibre.com/items/${id}`);
      return send(res, r.status, await r.json());
    }
    // Tendências de busca (top termos por categoria)
    if (u.pathname === '/api/public/trends') {
      const cat = u.query.category || 'MLB1747'; // Autopeças
      const r = await mlFetch(`https://api.mercadolibre.com/trends/MLB/${cat}`);
      return send(res, r.status, await r.json());
    }
    // Categorias nível 1 do Brasil
    if (u.pathname === '/api/public/categories') {
      const r = await mlFetch('https://api.mercadolibre.com/sites/MLB/categories');
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

    // ============= ML OAuth — FLUXO SEMI-MANUAL (fallback: cola o code manualmente) =============
    // POST /api/ml/token — troca code por access_token + refresh_token + user info
    if (u.pathname === '/api/ml/token' && req.method === 'POST') {
      env = loadEnv();
      const body = await readBody(req);
      const code = (body.code || '').trim();
      if (!code) return send(res, 400, { success:false, error:'code obrigatório' });

      const clientId     = process.env.ML_CLIENT_ID     || env.ML_CLIENT_ID     || '3688973136843575';
      const clientSecret = process.env.ML_CLIENT_SECRET || env.ML_CLIENT_SECRET || 'wFVvYKcAFoaLedYEfUmnKnUN9vYQMcXW';
      const redirectUri  = process.env.ML_REDIRECT_URI  || env.ML_REDIRECT_URI  || 'https://agentemarkt.com/callback';

      try {
        const r = await mlFetch('https://api.mercadolibre.com/oauth/token', {
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
          const ur = await mlFetch('https://api.mercadolibre.com/users/me', {
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
        const r = await mlFetch('https://api.mercadolibre.com/oauth/token', {
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
        const r = await mlFetch('https://api.mercadolibre.com/users/me', {
          headers: { 'Authorization': 'Bearer ' + token }
        });
        const data = await r.json().catch(() => ({}));
        if (!r.ok || data.error) {
          // Distingue: (1) ML rate-limitou o IP do VPS  (2) token inválido  (3) rede
          const code = data.message || data.error || '';
          const cause =
            /rate_limit_local/i.test(code)          ? 'rate_limit_local'
          : /ml_rate_limited_upstream/i.test(code)  ? 'ml_rate_limited_upstream'
          : /ml_empty_response/i.test(code)         ? 'ml_empty_response'
          : /network_error/i.test(code)             ? 'network_error'
          : r.status === 429                         ? 'ml_rate_limited_upstream'
          : (r.status === 401 || r.status === 403)   ? 'token_invalido'
          : 'ml_error';
          const mensagem =
            cause === 'ml_rate_limited_upstream' ? 'ML rate-limitou este IP do servidor (HTTP 429) — tente de novo em alguns minutos'
          : cause === 'ml_empty_response'        ? 'ML respondeu vazio — possível bloqueio de rede no VPS'
          : cause === 'rate_limit_local'         ? 'Rate limiter interno ativado — aguarde'
          : cause === 'network_error'            ? 'Erro de rede ao falar com o ML'
          : cause === 'token_invalido'           ? 'Token inválido — reconecte'
          : (data.message || data.error || 'Erro ao consultar ML');
          return send(res, 200, { connected:false, error: mensagem, cause, status: r.status });
        }
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
        return send(res, 200, { connected:false, error: err.message, cause: 'exception' });
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
      // URLs passadas aqui são sempre https://api.mercadolibre.com/* — mlFetch aplica rate limiter
      const r = await mlFetch(url, { headers: { 'Authorization': 'Bearer ' + token } });
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

    // ============= GESTÃO DE PEDIDOS (Orders + Shipping + SLA) =============
    // Lista detalhada, stats, detalhes com envio/SLA, etiqueta PDF, ready_to_ship

    // Stats — precisa vir ANTES do :orderId pra não ser capturado pelo match genérico
    if (u.pathname === '/api/ml/pedidos/stats/resumo' && req.method === 'GET') {
      const token = getBearer();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      try {
        const me = await authedFetch('https://api.mercadolibre.com/users/me', token);
        if (!me.ok || !me.data.id) return send(res, me.status || 401, { success:false, error: me.data.message || 'Token inválido' });

        const desde = new Date();
        desde.setDate(desde.getDate() - 30);
        const desdeStr = desde.toISOString();

        const data = await authedFetch(
          `https://api.mercadolibre.com/orders/search?seller=${me.data.id}&order.date_created.from=${desdeStr}&sort=date_desc&limit=50`,
          token
        );
        if (!data.ok) return send(res, data.status, { success:false, error: data.data.message || 'Falha' });
        const pedidos = data.data.results || [];
        const hoje = new Date().toDateString();
        const pedidosHoje = pedidos.filter(p => new Date(p.date_created).toDateString() === hoje);

        return send(res, 200, {
          success: true,
          ultimos30dias: {
            total:        data.data.paging?.total || 0,
            pagos:        pedidos.filter(p => p.status === 'paid').length,
            enviados:     pedidos.filter(p => p.shipping?.status === 'shipped').length,
            cancelados:   pedidos.filter(p => p.status === 'cancelled').length,
            receitaTotal: pedidos.reduce((s, p) => s + (p.total_amount || 0), 0),
          },
          hoje: {
            total:   pedidosHoje.length,
            receita: pedidosHoje.reduce((s, p) => s + (p.total_amount || 0), 0),
          },
        });
      } catch (err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // Lista de pedidos recentes (?status=paid|confirmed|cancelled)
    if (u.pathname === '/api/ml/pedidos' && req.method === 'GET') {
      const token = getBearer();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      try {
        const me = await authedFetch('https://api.mercadolibre.com/users/me', token);
        if (!me.ok || !me.data.id) return send(res, me.status || 401, { success:false, error: me.data.message || 'Token inválido' });

        const status = u.query.status || '';
        let url = `https://api.mercadolibre.com/orders/search?seller=${me.data.id}&sort=date_desc&limit=50`;
        if (status) url += `&order.status=${encodeURIComponent(status)}`;

        const orders = await authedFetch(url, token);
        if (!orders.ok) return send(res, orders.status, { success:false, error: orders.data.message || 'Falha ao buscar pedidos' });

        const pedidos = (orders.data.results || []).map(order => ({
          id:             order.id,
          status:         order.status,
          statusDetail:   order.status_detail,
          dataCriacao:    order.date_created,
          dataFechamento: order.date_closed,
          valorTotal:     order.total_amount,
          moeda:          order.currency_id,
          comprador: {
            id:       order.buyer?.id,
            nickname: order.buyer?.nickname,
          },
          itens: (order.order_items || []).map(item => ({
            titulo:         item.item?.title,
            itemId:         item.item?.id,
            quantidade:     item.quantity,
            precoUnitario:  item.unit_price,
            sku:            item.item?.seller_sku || item.item?.seller_custom_field,
          })),
          envio: {
            id:     order.shipping?.id,
            status: order.shipping?.status || null,
          },
          tags:   order.tags || [],
          fraude: (order.tags || []).includes('fraud_risk_detected'),
        }));

        return send(res, 200, {
          success: true,
          total:   orders.data.paging?.total || 0,
          pedidos,
          resumo: {
            pagos:       pedidos.filter(p => p.status === 'paid').length,
            confirmados: pedidos.filter(p => p.status === 'confirmed').length,
            cancelados:  pedidos.filter(p => p.status === 'cancelled').length,
            valorTotal:  pedidos.reduce((s, p) => s + (p.valorTotal || 0), 0),
          },
        });
      } catch (err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // Detalhes de um pedido (com envio + SLA)
    if (u.pathname.startsWith('/api/ml/pedidos/') && req.method === 'GET') {
      const token = getBearer();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      try {
        const orderId = u.pathname.replace('/api/ml/pedidos/', '');
        if (!orderId || orderId.includes('/')) return send(res, 400, { success:false, error:'orderId inválido' });

        const orderR = await authedFetch(`https://api.mercadolibre.com/orders/${orderId}`, token);
        if (!orderR.ok) return send(res, orderR.status, { success:false, error: orderR.data.message || 'Pedido não encontrado' });
        const order = orderR.data;

        // Envio
        let envio = null;
        if (order.shipping?.id) {
          try {
            const shipResp = await mlFetch(`https://api.mercadolibre.com/shipments/${order.shipping.id}`, {
              headers: { 'Authorization': 'Bearer ' + token, 'x-format-new': 'true' },
            });
            envio = await shipResp.json();
          } catch(_){}
        }

        // SLA (prazo máximo de despacho)
        let sla = null;
        if (order.shipping?.id) {
          try {
            const slaResp = await mlFetch(`https://api.mercadolibre.com/shipments/${order.shipping.id}/sla`, {
              headers: { 'Authorization': 'Bearer ' + token },
            });
            sla = await slaResp.json();
          } catch(_){}
        }

        return send(res, 200, {
          success: true,
          pedido: {
            id:          order.id,
            status:      order.status,
            dataCriacao: order.date_created,
            valorTotal:  order.total_amount,
            comprador: {
              id:       order.buyer?.id,
              nickname: order.buyer?.nickname,
            },
            itens: (order.order_items || []).map(item => ({
              titulo:        item.item?.title,
              itemId:        item.item?.id,
              quantidade:    item.quantity,
              precoUnitario: item.unit_price,
            })),
            envio: envio ? {
              id:           envio.id,
              status:       envio.status,
              substatus:    envio.substatus,
              tipo:         envio.logistic_type,
              dataEnvio:    envio.date_first_printed,
              dataEntrega:  envio.status_history?.date_delivered,
              rastreamento: envio.tracking_number,
              metodo:       envio.shipping_option?.name,
            } : null,
            sla: sla ? {
              status:      sla.status,
              prazoMaximo: sla.expected_date,
              servico:     sla.service,
            } : null,
            tags:   order.tags || [],
            fraude: (order.tags || []).includes('fraud_risk_detected'),
          },
        });
      } catch (err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // Etiqueta PDF
    if (u.pathname.match(/^\/api\/ml\/envios\/[^/]+\/etiqueta$/) && req.method === 'GET') {
      const token = getBearer();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      try {
        const shipmentId = u.pathname.split('/')[4];
        const labelResp = await mlFetch(
          `https://api.mercadolibre.com/shipment_labels?shipment_ids=${shipmentId}&response_type=pdf`,
          { headers: { 'Authorization': 'Bearer ' + token } }
        );
        if (labelResp.ok) {
          const buffer = await labelResp.arrayBuffer();
          res.writeHead(200, {
            'Content-Type':        'application/pdf',
            'Content-Disposition': `inline; filename="etiqueta-${shipmentId}.pdf"`,
            'Access-Control-Allow-Origin': '*',
          });
          return res.end(Buffer.from(buffer));
        }
        return send(res, 200, { success:false, error:'Etiqueta não disponível' });
      } catch (err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // Marcar envio como pronto pra despachar
    if (u.pathname.match(/^\/api\/ml\/envios\/[^/]+\/despachar$/) && req.method === 'POST') {
      const token = getBearer();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      try {
        const shipmentId = u.pathname.split('/')[4];
        const response = await mlFetch(
          `https://api.mercadolibre.com/shipments/${shipmentId}/process/ready_to_ship`,
          { method: 'POST', headers: { 'Authorization': 'Bearer ' + token } }
        );
        const data = await response.json().catch(() => ({}));
        if (response.ok) {
          console.log(`🚚 Envio ${shipmentId} marcado como PRONTO PRA DESPACHAR ✅`);
          return send(res, 200, { success: true, message: 'Marcado como pronto pra despachar!' });
        }
        return send(res, 200, { success: false, error: data.message || 'Erro ao despachar', details: data });
      } catch (err) { return send(res, 200, { success:false, error: err.message }); }
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
            // Bug #2 — `precoCusto` está em `fornecedor.precoCusto` na API Bling v3, não na raiz.
            // Sem este fallback, o front sempre recebia `undefined`.
            precoCusto:      p.precoCusto || p.fornecedor?.precoCusto || p.fornecedor?.precoCompra || null,
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
          const cr = await mlFetch(`https://api.mercadolibre.com/sites/MLB/domain_discovery/search?q=${encodeURIComponent(produto.nome || '')}`, {
            headers: { 'Authorization': 'Bearer ' + mlToken }
          });
          const cj = await cr.json().catch(() => []);
          if (Array.isArray(cj) && cj[0]?.category_id) categoryId = cj[0].category_id;
        } catch(_) {}

        // 3) Atributos obrigatórios
        let requiredAttrs = [];
        try {
          const ar = await mlFetch(`https://api.mercadolibre.com/categories/${categoryId}/attributes`, {
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

        // 4) Título + descrição otimizados via SEO (AUTOMÁTICO — IA faz tudo)
        // Se o cliente mandou tituloOtimizado, respeita; senão gera.
        let titulo = (body.tituloOtimizado || produto.nome || '').trim();
        let descricaoPlain = null;
        let seoInfo = null;
        if (!body.tituloOtimizado) {
          try {
            const seoCompleto = await fetch(`http://127.0.0.1:${PORT}/api/seo/gerar-completo`, {
              method:'POST',
              headers:{ 'Authorization':'Bearer '+mlToken, 'Content-Type':'application/json' },
              body: JSON.stringify({
                nome: produto.nome,
                marca: produto.marca?.nome || produto.marca || null,
                partNumber: produto.codigo || null,
                categoria: categoryId,
                especificacoes: produto.descricaoCurta || null,
                veiculosCompativeis: body.veiculosCompativeis || [],
              }),
            });
            const seoData = await seoCompleto.json().catch(()=>({}));
            if (seoData.success && seoData.recomendacao?.titulo?.titulo) {
              titulo = seoData.recomendacao.titulo.titulo;
              descricaoPlain = seoData.recomendacao.descricao?.descricao || null;
              seoInfo = {
                titulo_variacao: seoData.recomendacao.titulo.variacao,
                descricao_variacao: seoData.recomendacao.descricao?.variacao,
                motivo: seoData.recomendacao.motivo,
                duplicidade: seoData.duplicidade,
                todos_titulos: seoData.titulos,
              };
              console.log(`📝 SEO ${titulo} (${titulo.length}ch) · ${seoInfo.motivo}`);
            }
          } catch(err) {
            console.error('📝 SEO erro:', err.message);
          }
        }
        if (titulo.length > 60) titulo = titulo.slice(0, 57) + '...';

        // 4.1) Precificação top-seller — bloqueia prejuízo, alerta margem crítica
        let precoFinal = Number(produto.preco) || 0;
        let precificacaoInfo = null;
        // Bug #1 — `precoCusto` NÃO existe na raiz do produto Bling v3.
        // Está em `produto.fornecedor.precoCusto` (ou `precoCompra` como fallback).
        // Antes: caía em `Number(produto.preco)` e tratava preço de venda como custo,
        // causando "PREJUÍZO" falso em todo produto com fornecedor cadastrado.
        const custoBase = Number(produto.precoCusto)
                       || Number(produto.fornecedor?.precoCusto)
                       || Number(produto.fornecedor?.precoCompra)
                       || 0;
        const pricingCfg = body.pricingConfig || {};
        const bloquearPrejuizo = pricingCfg.bloquearPrejuizo !== false; // default true
        if (custoBase > 0) {
          try {
            const calc = calcularPrecoTopSeller({
              custo: custoBase,
              margemDesejada: pricingCfg.margemDesejada ?? 20,
              tipoAnuncio:    pricingCfg.tipoAnuncio    ?? 'premium',
              pesoKg:         Number(produto.pesoBruto || produto.pesoLiq || pricingCfg.pesoKg) || 1,
              imposto:        pricingCfg.imposto        ?? 0,
              custoEmbalagem: pricingCfg.custoEmbalagem ?? 2,
              freteGratis:    pricingCfg.freteGratis    !== false,
            });
            if (calc.success) {
              precificacaoInfo = calc;
              // Se usuário pediu preço sugerido, usa; senão mantém do Bling
              if (pricingCfg.usarPrecoSugerido) precoFinal = calc.precoSugerido;
              console.log(`💰 Preço: R$${precoFinal.toFixed(2)} (custo R$${custoBase.toFixed(2)}, margem ${calc.breakdown.margemReal}, ${calc.status.emoji} ${calc.status.nivel})`);
              // Simula com o preço final pra ver se passa
              const sim = simularLucroVenda({
                precoVenda: precoFinal, custo: custoBase,
                tipoAnuncio:    pricingCfg.tipoAnuncio    ?? 'premium',
                pesoKg:         Number(produto.pesoBruto || produto.pesoLiq || pricingCfg.pesoKg) || 1,
                imposto:        pricingCfg.imposto        ?? 0,
                custoEmbalagem: pricingCfg.custoEmbalagem ?? 2,
                freteGratis:    pricingCfg.freteGratis    !== false,
              });
              if (sim.success && sim.lucroLiquido < 0 && bloquearPrejuizo) {
                return send(res, 200, {
                  success: false,
                  error: `🚨 PREJUÍZO bloqueado: preço R$${precoFinal.toFixed(2)} gera lucro ${sim.lucroLiquido}. Use preço ≥ R$${calc.precoMinimo}`,
                  precificacao: { atual: sim, sugerido: calc },
                });
              }
              if (sim.success && sim.status === 'vermelho') {
                console.warn(`🔴 MARGEM CRÍTICA (${sim.margemReal}) — publicando mesmo assim · ${produto.nome}`);
              }
            }
          } catch(err) {
            console.error('💰 precificação erro:', err.message);
          }
        }

        // 5) Body do anúncio
        const imagens = (produto.midia?.imagens?.internas || [])
          .map(img => ({ source: img.link }))
          .filter(i => i.source);
        const estoque = produto.estoque?.saldoVirtualTotal ?? 1;
        const descFinal = descricaoPlain
          || produto.descricaoCurta
          || (titulo + ' — produto novo, com garantia. Envio rápido para todo Brasil.');
        const anuncio = {
          title:       titulo,
          category_id: categoryId,
          price:       precoFinal,
          currency_id: 'BRL',
          available_quantity: Math.max(1, Number(estoque) || 1),
          buying_mode:  'buy_it_now',
          listing_type_id: 'gold_special',
          condition:    'new',
          description:  { plain_text: String(descFinal).slice(0, 50000) },
          attributes:   requiredAttrs,
          pictures:     imagens,
        };

        // 6) POST /items no ML
        const mr = await mlFetch('https://api.mercadolibre.com/items', {
          method: 'POST',
          headers: { 'Authorization': 'Bearer ' + mlToken, 'Content-Type': 'application/json' },
          body: JSON.stringify(anuncio),
        });
        const mj = await mr.json().catch(() => ({}));
        if (mj.id) {
          // 🚗 Dispara compatibilidade automática em background (3s depois do ML indexar)
          setTimeout(async () => {
            try {
              console.log(`🚗 Resolvendo compatibilidade para ${mj.id}...`);
              const cr = await fetch(`http://127.0.0.1:${PORT}/api/ml/compat/auto-resolver`, {
                method:'POST',
                headers:{ 'Authorization':'Bearer '+mlToken, 'Content-Type':'application/json' },
                body: JSON.stringify({ itemId: mj.id }),
              });
              const cd = await cr.json().catch(()=>({}));
              if (cd.sucesso) {
                console.log(`🚗 ${mj.id} → compat ✅ via ${cd.metodo}${cd.resumo?` (${cd.resumo})`:''}`);
              } else {
                console.log(`🚗 ${mj.id} → compat ⚠️ pendente`);
              }
            } catch(err) {
              console.error(`🚗 ${mj.id} → erro compat:`, err.message);
            }
          }, 3000);

          return send(res, 200, {
            success: true,
            message: 'Anúncio publicado com sucesso',
            ml_item_id: mj.id,
            permalink:  mj.permalink,
            title:      mj.title,
            price:      mj.price,
            status:     mj.status,
            category_id: categoryId,
            compatibilidade: 'auto-resolvendo em background',
            precificacao: precificacaoInfo ? {
              precoUsado:    precoFinal,
              precoSugerido: precificacaoInfo.precoSugerido,
              precoMinimo:   precificacaoInfo.precoMinimo,
              margem:        precificacaoInfo.breakdown.margemReal,
              lucroLiquido:  precificacaoInfo.breakdown.lucroLiquido,
              status:        precificacaoInfo.status,
            } : null,
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

    // POST /api/tokens/manual-connect — reconecta manualmente colando tokens obtidos externamente
    // (emergência: quando o VPS não consegue trocar code por token e o usuário faz no PC dele)
    // Protegida por X-Admin-Secret (default 'agente-marketplace-2026' — override via ADMIN_SECRET no .env)
    if (u.pathname === '/api/tokens/manual-connect' && req.method === 'POST') {
      try {
        const adminSecret = process.env.ADMIN_SECRET || loadEnv().ADMIN_SECRET || 'agente-marketplace-2026';
        const providedSecret = req.headers['x-admin-secret'] || req.headers['X-Admin-Secret'];
        if (providedSecret !== adminSecret) {
          return send(res, 403, { success:false, error:'Acesso negado — X-Admin-Secret inválido ou ausente' });
        }

        const body = await readBody(req).catch(() => ({}));
        const { access_token, refresh_token, expires_in, user_id } = body || {};

        if (!access_token) {
          return send(res, 200, { success:false, error:'access_token obrigatório' });
        }

        const expSeconds = Number(expires_in) || 21600;
        const expiresAt = new Date(Date.now() + expSeconds * 1000).toISOString();

        // Persiste no tokens.json (e via saveTokens, que já aplica chmod 600)
        const saved = saveTokens({
          ml_access_token:     access_token,
          ml_refresh_token:    refresh_token || loadTokens().ml_refresh_token || '',
          ml_expires_in:       expSeconds,
          ml_token_expires_at: expiresAt,
          ml_user_id:          user_id != null ? String(user_id) : (loadTokens().ml_user_id || null),
        });

        // Espelha no .env pra compat com rotas legadas que leem de env
        saveEnv({
          ML_ACCESS_TOKEN:     access_token,
          ML_REFRESH_TOKEN:    refresh_token || loadEnv().ML_REFRESH_TOKEN || '',
          ML_USER_ID:          String(user_id || loadEnv().ML_USER_ID || ''),
          ML_TOKEN_EXPIRES_AT: String(Date.now() + expSeconds * 1000),
        });

        console.log(`[tokens] ✅ Token salvo manualmente — user_id=${user_id || 'N/A'} · expira ${expiresAt}`);
        return send(res, 200, {
          success: true,
          message: 'Token salvo com sucesso!',
          expires_at: expiresAt,
          user_id: saved?.ml_user_id || user_id || null,
        });
      } catch (error) {
        return send(res, 200, { success:false, error: error.message });
      }
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

    // ============= COMPATIBILIDADE AUTOPEÇAS (AUTO via IA) =============
    // Domínio ML: MLB-CARS_AND_VANS · O usuário NÃO cadastra manualmente.
    // Helper: pega o token ML do header Authorization OU do tokens.json
    const getMlToken = () => {
      const h = req.headers.authorization;
      if (h && /^Bearer\s+/i.test(h)) return h.replace(/^Bearer\s+/i,'').trim();
      return loadTokens().ml_access_token || null;
    };

    // ETAPA 1 — Sugestões do próprio ML pro anúncio
    if (u.pathname === '/api/ml/compat/sugestoes' && req.method === 'POST') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      try {
        const { itemId, productId } = await readBody(req);
        const body = {
          domain_id: 'MLB-CARS_AND_VANS',
          site_id:   'MLB',
          filter:    'SUGGESTED',
          item_id:   itemId,
        };
        if (productId) body.secondary_product_id = productId;
        const r = await mlFetch('https://api.mercadolibre.com/catalog_compatibilities/products_search/chunks', {
          method:'POST',
          headers:{ 'Authorization':'Bearer '+token, 'Content-Type':'application/json' },
          body: JSON.stringify(body),
        });
        const data = await r.json().catch(()=>({}));
        return send(res, 200, { success:true, sugestoes: data.results || [], total: data.paging?.total || 0 });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // ETAPA 2 — Buscar produto no catálogo ML pelo part number
    if (u.pathname.startsWith('/api/ml/catalogo/buscar/') && req.method === 'GET') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      const partNumber = decodeURIComponent(u.pathname.replace('/api/ml/catalogo/buscar/',''));
      try {
        const r = await mlFetch(`https://api.mercadolibre.com/products/search?status=active&site_id=MLB&q=${encodeURIComponent(partNumber)}`, {
          headers:{ 'Authorization':'Bearer '+token },
        });
        const data = await r.json().catch(()=>({}));
        return send(res, 200, { success:true, produtos: data.results || [] });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // ETAPA 3 — Marcas do domínio
    if (u.pathname === '/api/ml/compat/marcas' && req.method === 'POST') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      try {
        const r = await mlFetch('https://api.mercadolibre.com/catalog_domains/MLB-CARS_AND_VANS/attributes/BRAND/top_values', {
          method:'POST',
          headers:{ 'Authorization':'Bearer '+token, 'Content-Type':'application/json' },
          body: JSON.stringify({}),
        });
        const data = await r.json().catch(()=>({}));
        return send(res, 200, { success:true, marcas: data });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // Modelos de uma marca
    if (u.pathname === '/api/ml/compat/modelos' && req.method === 'POST') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      try {
        const { brandId } = await readBody(req);
        const r = await mlFetch('https://api.mercadolibre.com/catalog_domains/MLB-CARS_AND_VANS/attributes/MODEL/top_values', {
          method:'POST',
          headers:{ 'Authorization':'Bearer '+token, 'Content-Type':'application/json' },
          body: JSON.stringify({ known_attributes: [{ id:'BRAND', value_id: brandId }] }),
        });
        const data = await r.json().catch(()=>({}));
        return send(res, 200, { success:true, modelos: data });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // ETAPA 4 — Cadastrar compatibilidade manualmente (fallback)
    if (u.pathname === '/api/ml/compat/cadastrar' && req.method === 'POST') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      try {
        const { itemId, products, productsFamilies, universal } = await readBody(req);
        let body;
        if (universal) {
          body = { products: [], products_families: [], products_group: [], universal: true };
        } else {
          body = {
            products: (products || []).map(p => ({ id: p.id })),
            products_families: (productsFamilies || []).map(pf => ({
              domain_id: 'MLB-CARS_AND_VANS',
              attributes: pf.attributes,
            })),
          };
        }
        const r = await mlFetch(`https://api.mercadolibre.com/items/${itemId}/compatibilities`, {
          method:'POST',
          headers:{ 'Authorization':'Bearer '+token, 'Content-Type':'application/json' },
          body: JSON.stringify(body),
        });
        const data = await r.json().catch(()=>({}));
        return send(res, 200, r.ok ? { success:true, data } : { success:false, error: data.message, details: data });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // ETAPA 5 — Listar compatibilidades de um anúncio
    if (u.pathname.startsWith('/api/ml/compat/listar/') && req.method === 'GET') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      const itemId = u.pathname.replace('/api/ml/compat/listar/','');
      try {
        const r = await mlFetch(`https://api.mercadolibre.com/items/${itemId}/compatibilities`, {
          headers:{ 'Authorization':'Bearer '+token },
        });
        const data = await r.json().catch(()=>({}));
        return send(res, 200, { success:true, compatibilidades: data });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // ETAPA 6 — Listar anúncios com compatibilidade PENDENTE
    if (u.pathname === '/api/ml/compat/pendentes' && req.method === 'GET') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      try {
        const meResp = await mlFetch('https://api.mercadolibre.com/users/me', {
          headers:{ 'Authorization':'Bearer '+token },
        });
        const me = await meResp.json();
        const itemsResp = await mlFetch(`https://api.mercadolibre.com/users/${me.id}/items/search?tags=incomplete_compatibilities&status=active`, {
          headers:{ 'Authorization':'Bearer '+token },
        });
        const itemsData = await itemsResp.json();
        return send(res, 200, { success:true, total: itemsData.paging?.total || 0, items: itemsData.results || [] });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // =========================================================
    // AUTO-RESOLVER — a IA faz TUDO sozinha pra 1 anúncio
    // =========================================================
    if (u.pathname === '/api/ml/compat/auto-resolver' && req.method === 'POST') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      try {
        const { itemId } = await readBody(req);
        const resultados = { itemId, etapas: [], sucesso: false };

        // 1) Detalhes do anúncio
        const itemResp = await mlFetch(`https://api.mercadolibre.com/items/${itemId}`, {
          headers:{ 'Authorization':'Bearer '+token },
        });
        const item = await itemResp.json();
        resultados.etapas.push({ etapa:'Buscar anúncio', status:'ok', titulo: item.title });
        if (item.catalog_product_id) {
          resultados.etapas.push({ etapa:'Produto do catálogo encontrado', id: item.catalog_product_id });
        }

        // 2) Sugestões do ML
        const sugResp = await mlFetch('https://api.mercadolibre.com/catalog_compatibilities/products_search/chunks', {
          method:'POST',
          headers:{ 'Authorization':'Bearer '+token, 'Content-Type':'application/json' },
          body: JSON.stringify({
            domain_id: 'MLB-CARS_AND_VANS',
            site_id:   'MLB',
            filter:    'SUGGESTED',
            item_id:   itemId,
            ...(item.catalog_product_id ? { secondary_product_id: item.catalog_product_id } : {}),
          }),
        });
        const sugData = await sugResp.json().catch(()=>({}));
        const sugestoes = sugData.results || [];
        resultados.etapas.push({ etapa:'Sugestões do ML', total: sugestoes.length });

        // 3) Aplica sugestões (até 200)
        if (sugestoes.length > 0) {
          const productsToAdd = sugestoes.slice(0, 200).map(s => ({ id: s.id }));
          const cadR = await mlFetch(`https://api.mercadolibre.com/items/${itemId}/compatibilities`, {
            method:'POST',
            headers:{ 'Authorization':'Bearer '+token, 'Content-Type':'application/json' },
            body: JSON.stringify({ products: productsToAdd }),
          });
          const cadD = await cadR.json().catch(()=>({}));
          if (cadR.ok) {
            resultados.sucesso = true;
            resultados.metodo  = 'sugestoes';
            resultados.aplicadas = productsToAdd.length;
            resultados.etapas.push({ etapa:'Compatibilidades aplicadas via sugestões', total: productsToAdd.length });
          } else {
            resultados.etapas.push({ etapa:'Erro ao aplicar sugestões', erro: cadD.message });
          }
        }

        // 4) Fallback: analisar TÍTULO
        if (!resultados.sucesso) {
          const titulo = (item.title || '').toLowerCase();
          const marcasML = {
            'volkswagen':'60249','vw':'60249','gol':'60249','voyage':'60249',
            'saveiro':'60249','polo':'60249','fox':'60249','up':'60249',
            'ford':'66432','fiesta':'66432','focus':'66432','ka':'66432','ecosport':'66432',
            'chevrolet':'58955','gm':'58955','onix':'58955','prisma':'58955',
            'cobalt':'58955','spin':'58955','tracker':'58955','cruze':'58955',
            'fiat':'67781','uno':'67781','palio':'67781','siena':'67781',
            'strada':'67781','argo':'67781','mobi':'67781','toro':'67781','cronos':'67781',
            'hyundai':'60376','hb20':'60376','creta':'60376','tucson':'60376',
            'toyota':'60557','corolla':'60557','hilux':'60557','etios':'60557','yaris':'60557',
            'honda':'60304','civic':'60304','fit':'60304','hr-v':'60304','city':'60304',
            'renault':'9909','kwid':'9909','sandero':'9909','logan':'9909','duster':'9909',
            'peugeot':'60279','208':'60279','308':'60279',
            'citroen':'60094','c3':'60094','c4':'60094',
            'nissan':'60421','kicks':'60421','march':'60421','versa':'60421',
            'jeep':'60333','renegade':'60333','compass':'60333',
            'mitsubishi':'60413','l200':'60413','pajero':'60413','outlander':'60413',
          };
          const modelosConhecidos = {
            'gol':'Gol','voyage':'Voyage','saveiro':'Saveiro','polo':'Polo',
            'fox':'Fox','up':'Up!','golf':'Golf','jetta':'Jetta','amarok':'Amarok',
            'onix':'Onix','prisma':'Prisma','cobalt':'Cobalt','spin':'Spin',
            'tracker':'Tracker','cruze':'Cruze','montana':'Montana','corsa':'Corsa',
            'celta':'Celta','s10':'S10',
            'uno':'Uno','palio':'Palio','siena':'Siena','strada':'Strada',
            'argo':'Argo','mobi':'Mobi','toro':'Toro','cronos':'Cronos',
            'ka':'Ka','fiesta':'Fiesta','focus':'Focus','ecosport':'EcoSport',
            'ranger':'Ranger','fusion':'Fusion',
            'hb20':'HB20','creta':'Creta','tucson':'Tucson','ix35':'ix35',
            'corolla':'Corolla','hilux':'Hilux','etios':'Etios','yaris':'Yaris',
            'civic':'Civic','fit':'Fit','hr-v':'HR-V','city':'City',
            'kwid':'Kwid','sandero':'Sandero','logan':'Logan','duster':'Duster',
            'kicks':'Kicks','march':'March','versa':'Versa',
            'renegade':'Renegade','compass':'Compass',
            'l200':'L200','pajero':'Pajero','outlander':'Outlander',
          };
          let marcaDetectada = null, modeloDetectado = null, anosDetectados = [];
          for (const [pal, valueId] of Object.entries(marcasML)) {
            const re = new RegExp(`\\b${pal.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')}\\b`);
            if (re.test(titulo)) { marcaDetectada = { palavra: pal, valueId }; break; }
          }
          for (const [pal, nome] of Object.entries(modelosConhecidos)) {
            const re = new RegExp(`\\b${pal.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')}\\b`);
            if (re.test(titulo)) { modeloDetectado = { palavra: pal, nome }; break; }
          }
          const rangeMatch = titulo.match(/(\d{4})\s*(?:a|até|à|-)\s*(\d{4})/);
          if (rangeMatch) {
            const ini = parseInt(rangeMatch[1],10), fim = parseInt(rangeMatch[2],10);
            if (ini >= 1970 && fim <= 2035 && fim >= ini && fim - ini <= 30) {
              for (let a = ini; a <= fim; a++) anosDetectados.push(String(a));
            }
          } else {
            const ind = titulo.match(/\b(19\d{2}|20[0-3]\d)\b/g);
            if (ind) anosDetectados = [...new Set(ind)];
          }

          resultados.etapas.push({
            etapa:'Análise do título',
            marca:  marcaDetectada?.palavra || 'não encontrada',
            modelo: modeloDetectado?.nome   || 'não encontrado',
            anos:   anosDetectados.length ? anosDetectados : 'nenhum',
          });

          if (marcaDetectada && modeloDetectado) {
            const families = [];
            if (anosDetectados.length > 0) {
              for (const ano of anosDetectados) {
                families.push({
                  domain_id:'MLB-CARS_AND_VANS',
                  attributes:[
                    { id:'BRAND', value_id: marcaDetectada.valueId },
                    { id:'MODEL', value_name: modeloDetectado.nome },
                    { id:'YEAR',  value_name: ano },
                  ],
                });
              }
            } else {
              families.push({
                domain_id:'MLB-CARS_AND_VANS',
                attributes:[
                  { id:'BRAND', value_id: marcaDetectada.valueId },
                  { id:'MODEL', value_name: modeloDetectado.nome },
                ],
              });
            }
            const cadR = await mlFetch(`https://api.mercadolibre.com/items/${itemId}/compatibilities`, {
              method:'POST',
              headers:{ 'Authorization':'Bearer '+token, 'Content-Type':'application/json' },
              body: JSON.stringify({ products_families: families }),
            });
            const cadD = await cadR.json().catch(()=>({}));
            if (cadR.ok) {
              resultados.sucesso = true;
              resultados.metodo  = 'titulo';
              resultados.aplicadas = families.length;
              resultados.resumo = `${modeloDetectado.nome}${anosDetectados.length?` ${anosDetectados[0]}-${anosDetectados[anosDetectados.length-1]}`:''}`;
              resultados.etapas.push({ etapa:'Compatibilidade via análise de título', total: families.length });
            } else {
              resultados.etapas.push({ etapa:'Erro ao cadastrar via título', erro: cadD.message });
            }
          }
        }

        // 5) Universal (parafusos, óleos, genéricos)
        if (!resultados.sucesso) {
          const titulo = (item.title || '').toLowerCase();
          const termos = ['universal','genérico','generico','parafuso','arruela','presilha',
            'grampo','abraçadeira','fita','cola','adesivo','limpador','desengripante',
            'óleo','oleo','fluido','aditivo'];
          if (termos.some(t => titulo.includes(t))) {
            const univR = await mlFetch(`https://api.mercadolibre.com/items/${itemId}/compatibilities`, {
              method:'POST',
              headers:{ 'Authorization':'Bearer '+token, 'Content-Type':'application/json' },
              body: JSON.stringify({ products:[], products_families:[], products_group:[], universal:true }),
            });
            if (univR.ok) {
              resultados.sucesso = true;
              resultados.metodo  = 'universal';
              resultados.etapas.push({ etapa:'Marcado como UNIVERSAL' });
            } else {
              const eD = await univR.json().catch(()=>({}));
              resultados.etapas.push({ etapa:'Erro ao marcar universal', erro: eD.message });
            }
          } else {
            resultados.etapas.push({ etapa:'Não foi possível resolver automaticamente — precisa revisão manual' });
          }
        }

        // Contabiliza na stats
        compatStats.tentativas++;
        if (resultados.sucesso) compatStats.sucessos++; else compatStats.falhas++;
        compatStats.ultima = new Date().toISOString();
        if (resultados.sucesso) {
          compatStats.historico.unshift({
            ts: compatStats.ultima, itemId, metodo: resultados.metodo,
            resumo: resultados.resumo || (resultados.metodo === 'universal' ? 'universal' : null),
          });
          compatStats.historico = compatStats.historico.slice(0, 50);
        }

        return send(res, 200, resultados);
      } catch(err) {
        return send(res, 200, { sucesso:false, error: err.message });
      }
    }

    // AUTO-RESOLVER TODOS os pendentes
    if (u.pathname === '/api/ml/compat/auto-resolver-todos' && req.method === 'POST') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      try {
        const meResp = await mlFetch('https://api.mercadolibre.com/users/me', {
          headers:{ 'Authorization':'Bearer '+token },
        });
        const me = await meResp.json();
        const pendResp = await mlFetch(`https://api.mercadolibre.com/users/${me.id}/items/search?tags=incomplete_compatibilities&status=active&limit=50`, {
          headers:{ 'Authorization':'Bearer '+token },
        });
        const pendData = await pendResp.json().catch(()=>({}));
        const itemIds = pendData.results || [];
        const base = `http://127.0.0.1:${PORT}`;
        const resultados = [];
        for (let i = 0; i < itemIds.length; i++) {
          const itemId = itemIds[i];
          try {
            const r = await fetch(`${base}/api/ml/compat/auto-resolver`, {
              method:'POST',
              headers:{ 'Authorization':'Bearer '+token, 'Content-Type':'application/json' },
              body: JSON.stringify({ itemId }),
            });
            const rd = await r.json().catch(()=>({ sucesso:false, error:'JSON inválido' }));
            resultados.push({ itemId, ...rd });
          } catch(e) {
            resultados.push({ itemId, sucesso:false, error: e.message });
          }
          if (i < itemIds.length - 1) await new Promise(r => setTimeout(r, 1000));
        }
        return send(res, 200, {
          success: true,
          total:     itemIds.length,
          resolvidos: resultados.filter(r => r.sucesso).length,
          falhas:     resultados.filter(r => !r.sucesso).length,
          detalhes:   resultados,
        });
      } catch(err) {
        return send(res, 200, { success:false, error: err.message });
      }
    }

    // Stats + histórico pro dashboard
    if (u.pathname === '/api/ml/compat/stats' && req.method === 'GET') {
      return send(res, 200, { success:true, ...compatStats });
    }

    // ============= SEO — GERADOR DE TÍTULOS + DESCRIÇÕES + ANTI-DUPLICIDADE =============
    // Regras ML autopeças: máx 60 chars, técnica PMME, peça+marca+modelo+spec,
    // sem preço/promoção/caps/caracteres especiais/palavras repetidas

    // 3 variações de título
    if (u.pathname === '/api/seo/gerar-titulos' && req.method === 'POST') {
      try {
        const { nome, marca, partNumber, categoria, veiculosCompativeis } = await readBody(req);
        const titulos = gerarTitulosSEO({ nome, marca, partNumber, categoria, veiculosCompativeis });
        return send(res, 200, { success:true, titulos });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // 3 variações de descrição
    if (u.pathname === '/api/seo/gerar-descricoes' && req.method === 'POST') {
      try {
        const { nome, marca, partNumber, especificacoes, veiculosCompativeis } = await readBody(req);
        const descricoes = gerarDescricoesSEO({ nome, marca, partNumber, especificacoes, veiculosCompativeis });
        return send(res, 200, { success:true, descricoes });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // Anti-duplicidade — similaridade Jaccard com meus anúncios ativos
    if (u.pathname === '/api/seo/verificar-duplicidade' && req.method === 'POST') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      try {
        const { titulo } = await readBody(req);
        const resultado = await verificarDuplicidadeSEO(titulo, token);
        return send(res, 200, { success:true, ...resultado });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // Gera tudo de uma vez: 3 títulos + 3 descrições + checagem duplicidade + recomendação
    if (u.pathname === '/api/seo/gerar-completo' && req.method === 'POST') {
      try {
        const token = getMlToken();
        const { nome, marca, partNumber, categoria, especificacoes, veiculosCompativeis } = await readBody(req);
        const titulos = gerarTitulosSEO({ nome, marca, partNumber, categoria, veiculosCompativeis });
        const descricoes = gerarDescricoesSEO({ nome, marca, partNumber, especificacoes, veiculosCompativeis });

        let duplicidade = { duplicado:false, similaridade:0, tituloSimilar:null, aviso:'anti-duplicidade não executada (sem token)' };
        if (token && titulos[0]) {
          try { duplicidade = await verificarDuplicidadeSEO(titulos[0].titulo, token); } catch(_) {}
        }

        // Se título A é duplicado, tenta B; se B também, tenta C
        let recTitulo = titulos[0];
        let motivo = 'Título A é único — usando como principal';
        if (duplicidade.duplicado && titulos[1]) {
          const dupB = await verificarDuplicidadeSEO(titulos[1].titulo, token).catch(()=>({duplicado:false}));
          if (!dupB.duplicado) {
            recTitulo = titulos[1];
            motivo = 'Título A era duplicado — usando variação B';
          } else if (titulos[2]) {
            recTitulo = titulos[2];
            motivo = 'Títulos A e B eram duplicados — usando variação C';
          }
        }

        return send(res, 200, {
          success: true,
          titulos,
          descricoes,
          duplicidade,
          recomendacao: {
            titulo: recTitulo,
            descricao: descricoes[0],
            motivo,
          },
        });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // ============= PRECIFICAÇÃO INTELIGENTE — NÍVEL TOP SELLER =============
    // Calcula lucro líquido por SKU incluindo comissão, taxa fixa, frete, imposto, embalagem

    if (u.pathname === '/api/precificacao/calcular' && req.method === 'POST') {
      try {
        const body = await readBody(req);
        const r = calcularPrecoTopSeller(body);
        return send(res, 200, r);
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    if (u.pathname === '/api/precificacao/simular' && req.method === 'POST') {
      try {
        const body = await readBody(req);
        const r = simularLucroVenda(body);
        return send(res, 200, r);
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // ============= SAC AUTOMÁTICO — PERGUNTAS ML =============
    // TMR baixo = ML prioriza anúncios. Top sellers respondem em < 2 min.

    // Listar perguntas NÃO respondidas
    if (u.pathname === '/api/ml/sac/pendentes' && req.method === 'GET') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      try {
        const meResp = await mlFetch('https://api.mercadolibre.com/users/me', {
          headers:{ 'Authorization':'Bearer '+token },
        });
        const me = await meResp.json();
        const qResp = await mlFetch(
          `https://api.mercadolibre.com/questions/search?seller_id=${me.id}&status=UNANSWERED&api_version=4&sort_fields=date_created&sort_types=DESC`,
          { headers:{ 'Authorization':'Bearer '+token }}
        );
        const qData = await qResp.json().catch(()=>({}));
        return send(res, 200, {
          success: true,
          total: qData.total || 0,
          perguntas: (qData.questions || []).map(q => ({
            id:          q.id,
            texto:       q.text,
            itemId:      q.item_id,
            data:        q.date_created,
            compradorId: q.from?.id,
            status:      q.status,
          })),
        });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // Listar TODAS (respondidas e não)
    if (u.pathname === '/api/ml/sac/todas' && req.method === 'GET') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      try {
        const meResp = await mlFetch('https://api.mercadolibre.com/users/me', {
          headers:{ 'Authorization':'Bearer '+token },
        });
        const me = await meResp.json();
        const qResp = await mlFetch(
          `https://api.mercadolibre.com/questions/search?seller_id=${me.id}&api_version=4&sort_fields=date_created&sort_types=DESC&limit=50`,
          { headers:{ 'Authorization':'Bearer '+token }}
        );
        const qData = await qResp.json().catch(()=>({}));
        return send(res, 200, {
          success: true,
          total: qData.total || 0,
          perguntas: (qData.questions || []).map(q => ({
            id:       q.id,
            texto:    q.text,
            resposta: q.answer?.text || null,
            itemId:   q.item_id,
            data:     q.date_created,
            status:   q.status,
          })),
        });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // Responder uma pergunta (manual)
    if (u.pathname === '/api/ml/sac/responder' && req.method === 'POST') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      try {
        const { questionId, texto } = await readBody(req);
        if (!questionId || !texto) return send(res, 400, { success:false, error:'questionId e texto obrigatórios' });
        // Dupla camada: limparRespostaSAC (legado) + validarRespostaML (compliance rigoroso)
        let textoSafe = limparRespostaSAC(texto);
        const compl = validarRespostaML(textoSafe);
        textoSafe = compl.texto;
        if (compl.violacoes.length > 0) console.log(`🛡️ [compliance] /sac/responder sanitizado: ${compl.violacoes.join(', ')}`);
        const r = await mlFetch('https://api.mercadolibre.com/answers', {
          method:'POST',
          headers:{ 'Authorization':'Bearer '+token, 'Content-Type':'application/json' },
          body: JSON.stringify({ question_id: questionId, text: textoSafe }),
        });
        const d = await r.json().catch(()=>({}));
        return send(res, 200, r.ok
          ? { success:true, message:'Resposta enviada!', data:d }
          : { success:false, error: d.message || 'Erro ao responder', details:d }
        );
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // Gerar resposta automática pra UMA pergunta (preview)
    if (u.pathname === '/api/ml/sac/auto-responder' && req.method === 'POST') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      try {
        const { questionId, pergunta, itemId } = await readBody(req);
        if (!pergunta) return send(res, 400, { success:false, error:'pergunta obrigatória' });
        let itemTitle = '', itemPrice = 0, compatTexto = '';
        if (itemId) {
          try {
            const ir = await mlFetch(`https://api.mercadolibre.com/items/${itemId}`, {
              headers:{ 'Authorization':'Bearer '+token }});
            const it = await ir.json();
            itemTitle = it.title || ''; itemPrice = it.price || 0;
          } catch(_) {}
          try {
            const cr = await mlFetch(`https://api.mercadolibre.com/items/${itemId}/compatibilities`, {
              headers:{ 'Authorization':'Bearer '+token }});
            const cd = await cr.json();
            if (cd.products?.length > 0) {
              compatTexto = cd.products.slice(0, 10)
                .map(p => p.catalog_product_name || p.id).join(', ');
            }
          } catch(_) {}
        }
        const { respostaGerada, categoriaDetectada } = categorizarPerguntaSAC(pergunta, compatTexto);
        // Compliance: sanitiza a resposta gerada pelo template antes de devolver
        const compl = validarRespostaML(respostaGerada);
        if (compl.violacoes.length > 0) console.log(`🛡️ [compliance] /sac/auto-responder sanitizado: ${compl.violacoes.join(', ')}`);
        return send(res, 200, {
          success: true,
          questionId, pergunta, categoriaDetectada,
          respostaGerada: compl.texto,
          complianceViolacoes: compl.violacoes,
          itemTitle, preview: true,
        });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // Responder TODAS pendentes (auto)
    if (u.pathname === '/api/ml/sac/auto-responder-todos' && req.method === 'POST') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      try {
        const { modoAutomatico = false, horarioComercial = false } = await readBody(req);
        // Horário comercial: 8h-18h local Brasil
        if (horarioComercial) {
          const h = new Date().getHours();
          if (h < 8 || h >= 18) {
            return send(res, 200, { success:true, total:0, mensagem:'Fora do horário comercial (8h-18h) — aguardando' });
          }
        }
        const base = `http://127.0.0.1:${PORT}`;
        const pendR = await fetch(`${base}/api/ml/sac/pendentes`, {
          headers:{ 'Authorization':'Bearer '+token }});
        const pendD = await pendR.json().catch(()=>({}));
        if (!pendD.success || pendD.total === 0) {
          return send(res, 200, { success:true, total:0, mensagem:'Nenhuma pergunta pendente ✅' });
        }
        const resultados = [];
        const usarIA = !!process.env.ANTHROPIC_API_KEY;
        const rotaResposta = usarIA ? '/api/ml/sac/ia-responder' : '/api/ml/sac/auto-responder';
        for (const p of pendD.perguntas) {
          const autoR = await fetch(`${base}${rotaResposta}`, {
            method:'POST',
            headers:{ 'Authorization':'Bearer '+token, 'Content-Type':'application/json' },
            body: JSON.stringify({ questionId:p.id, pergunta:p.texto, itemId:p.itemId }),
          });
          const autoD = await autoR.json();

          if (modoAutomatico && autoD.success) {
            const sendR = await fetch(`${base}/api/ml/sac/responder`, {
              method:'POST',
              headers:{ 'Authorization':'Bearer '+token, 'Content-Type':'application/json' },
              body: JSON.stringify({ questionId:p.id, texto: autoD.respostaGerada }),
            });
            const sendD = await sendR.json();
            const cat = autoD.categoriaDetectada || autoD.fonte || 'ia';
            resultados.push({
              questionId: p.id, pergunta: p.texto,
              categoria:  cat,
              resposta:   autoD.respostaGerada,
              enviada:    sendD.success,
              erro:       sendD.error || null,
            });
            const logTag = usarIA ? '🧠 [ia-sac]' : '💬 SAC';
            console.log(`${logTag}: "${(p.texto||'').slice(0,50)}..." → ${cat} → ${sendD.success?'✅ Respondida':'❌ Erro'}`);
            // Stats
            sacStats.tentativas++;
            if (sendD.success) {
              sacStats.respondidas++;
              sacStats.historico.unshift({
                ts: new Date().toISOString(), questionId: p.id,
                pergunta: (p.texto||'').slice(0,80),
                categoria: cat, enviada: true,
              });
              sacStats.historico = sacStats.historico.slice(0, 50);
            } else {
              sacStats.falhas++;
            }
          } else {
            resultados.push({
              questionId: p.id, pergunta: p.texto,
              categoria:  autoD.categoriaDetectada || autoD.fonte || 'ia',
              respostaSugerida: autoD.respostaGerada,
              preview: true,
            });
          }
          await new Promise(r => setTimeout(r, 500));
        }
        sacStats.ultima = new Date().toISOString();
        return send(res, 200, {
          success: true,
          total:        resultados.length,
          respondidas:  resultados.filter(r => r.enviada).length,
          previews:     resultados.filter(r => r.preview).length,
          detalhes:     resultados,
        });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // ============= SAC IA GENERATIVA (Claude API) =============
    // Upgrade do SAC: gera respostas personalizadas via Claude API.
    // Fallback: se Claude API falhar ou sem ANTHROPIC_API_KEY → usa template do auto-responder.
    if (u.pathname === '/api/ml/sac/ia-responder' && req.method === 'POST') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      try {
        const { questionId, pergunta, itemId } = await readBody(req);
        if (!pergunta) return send(res, 400, { success:false, error:'pergunta obrigatória' });

        // 1. Dados completos do anúncio
        let itemData = {};
        if (itemId) {
          try {
            const itemResp = await mlFetch(`https://api.mercadolibre.com/items/${itemId}`, {
              headers: { 'Authorization': 'Bearer ' + token }
            });
            itemData = await itemResp.json();
          } catch(_) {}
        }

        // 2. Compatibilidades
        let compatibilidades = '';
        if (itemId) {
          try {
            const compatResp = await mlFetch(`https://api.mercadolibre.com/items/${itemId}/compatibilities`, {
              headers: { 'Authorization': 'Bearer ' + token }
            });
            const compatData = await compatResp.json();
            if (compatData.products && compatData.products.length > 0) {
              compatibilidades = compatData.products.slice(0, 15)
                .map(p => p.catalog_product_name || p.id).join(', ');
            }
          } catch(_) {}
        }

        // 3. Descrição
        let descricao = '';
        if (itemId) {
          try {
            const descResp = await mlFetch(`https://api.mercadolibre.com/items/${itemId}/description`, {
              headers: { 'Authorization': 'Bearer ' + token }
            });
            const descData = await descResp.json();
            descricao = (descData.plain_text || descData.text || '').substring(0, 500);
          } catch(_) {}
        }

        // 4. Prompt
        const systemPrompt = `Você é um vendedor profissional de autopeças no Mercado Livre.
Responda a pergunta do cliente de forma clara, cordial e profissional.

REGRAS OBRIGATÓRIAS:
- Máximo 350 caracteres (resposta CURTA e direta)
- NUNCA incluir links, URLs, telefones, emails ou WhatsApp
- NUNCA mencionar outros marketplaces ou lojas
- NUNCA combinar venda fora do Mercado Livre
- Tom profissional e acolhedor
- Sempre convidar pra compra ("pode comprar com segurança!")
- Se não souber a resposta, dizer "consulte a descrição do anúncio"
- Responder em português do Brasil`;

        const userPrompt = `DADOS DO ANÚNCIO:
Título: ${itemData.title || 'N/A'}
Preço: R$ ${itemData.price || 'N/A'}
Estoque: ${itemData.available_quantity || 'N/A'} unidades
Condição: ${itemData.condition === 'new' ? 'Novo' : 'Usado'}
Compatibilidades: ${compatibilidades || 'Não informado'}
Descrição: ${descricao || 'Não disponível'}

PERGUNTA DO CLIENTE:
"${pergunta}"

Responda de forma curta (máximo 350 caracteres), profissional e convidando pra compra.`;

        // 5. Claude API
        let respostaIA = null;
        let fonte = 'template-fallback';
        if (process.env.ANTHROPIC_API_KEY) {
          try {
            const claudeResp = await fetch('https://api.anthropic.com/v1/messages', {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
                'x-api-key': process.env.ANTHROPIC_API_KEY,
                'anthropic-version': '2023-06-01'
              },
              body: JSON.stringify({
                model: 'claude-sonnet-4-20250514',
                max_tokens: 300,
                system: systemPrompt,
                messages: [{ role: 'user', content: userPrompt }]
              })
            });
            if (claudeResp.ok) {
              const claudeData = await claudeResp.json();
              respostaIA = claudeData.content?.[0]?.text || null;
              if (respostaIA) fonte = 'claude-api';
            } else {
              console.error('🧠 [ia-sac] Claude API HTTP', claudeResp.status);
            }
          } catch(e) {
            console.error('🧠 [ia-sac] Erro Claude API:', e.message);
          }
        }

        // 6. Fallback: template existente
        if (!respostaIA) {
          console.log('🧠 [ia-sac] Claude API indisponível — usando fallback template');
          try {
            const fallbackResp = await fetch(`http://127.0.0.1:${PORT}/api/ml/sac/auto-responder`, {
              method: 'POST',
              headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
              body: JSON.stringify({ questionId, pergunta, itemId })
            });
            const fallbackData = await fallbackResp.json();
            respostaIA = fallbackData.respostaGerada;
          } catch(e) {
            console.error('🧠 [ia-sac] Erro fallback template:', e.message);
          }
        }

        // 7. Limpar resposta (regras ML) — dupla camada: legado + compliance
        if (typeof limparRespostaSAC === 'function' && respostaIA) {
          respostaIA = limparRespostaSAC(respostaIA);
        }
        if (respostaIA && respostaIA.length > 2000) {
          respostaIA = respostaIA.substring(0, 1997) + '...';
        }
        let complianceViolacoes = [];
        if (respostaIA) {
          const compl = validarRespostaML(respostaIA);
          respostaIA = compl.texto;
          complianceViolacoes = compl.violacoes;
          if (complianceViolacoes.length > 0) {
            console.log(`🛡️ [compliance] /sac/ia-responder sanitizado: ${complianceViolacoes.join(', ')}`);
          }
        }

        return send(res, 200, {
          success: true,
          questionId,
          pergunta,
          respostaGerada: respostaIA,
          fonte,
          complianceViolacoes,
          itemTitle: itemData.title || '',
          preview: true,
        });
      } catch (err) {
        return send(res, 200, { success: false, error: err.message });
      }
    }

    // Status da IA generativa
    if (u.pathname === '/api/ml/sac/ia-status' && req.method === 'GET') {
      const hasKey = !!process.env.ANTHROPIC_API_KEY;
      return send(res, 200, {
        success: true,
        iaGenerativa: hasKey,
        modelo: hasKey ? 'claude-sonnet-4-20250514' : null,
        fallback: 'templates (10 categorias)',
        custoEstimado: '~R$ 0,03 por resposta',
        instrucoes: hasKey ? null : {
          passo1: 'Obter API key em console.anthropic.com',
          passo2: 'Adicionar no .env do servidor: ANTHROPIC_API_KEY=sk-ant-xxx',
          passo3: 'Reiniciar o servidor: pm2 restart agente-am',
        },
      });
    }

    // ============= AGENTE AUTÔNOMO — publicação automática =============

    // Catálogo disponível pra publicar
    if (u.pathname === '/api/agente/catalogo' && req.method === 'GET') {
      const fonte = process.env.BLING_CLIENT_ID ? 'bling' : 'simulado';
      const produtos = catalogoSimulado
        .filter(p => p.ativo && p.estoque > 0)
        .map(p => {
          const { score, precoVenda, margem } = scoreProdutoSimulado(p);
          return { ...p, score, precoVenda, margem: +margem.toFixed(1) };
        });
      return send(res, 200, {
        success: true,
        fonte,
        total: produtos.length,
        produtos,
      });
    }

    // Score de um produto
    if (u.pathname.startsWith('/api/agente/score/') && req.method === 'GET') {
      const produtoId = u.pathname.replace('/api/agente/score/', '');
      const produto = catalogoSimulado.find(p => p.id === produtoId);
      if (!produto) return send(res, 200, { success:false, error:'Produto não encontrado' });
      const { score, precoVenda, margem } = scoreProdutoSimulado(produto);
      return send(res, 200, {
        success: true,
        produto: produto.titulo,
        score,
        detalhes: {
          margem:          margem.toFixed(1) + '%',
          estoque:         produto.estoque,
          compatibilidades: produto.compatibilidade?.length || 0,
          marca:           produto.marca,
          precoVendaEstimado: +precoVenda.toFixed(2),
        },
        recomendacao: score >= 70 ? '🟢 PUBLICAR' : score >= 40 ? '🟡 REVISAR' : '🔴 NÃO PUBLICAR',
      });
    }

    // ============================================================
    // GET /api/agente/produtos-qualificados — lista Bling com selos
    // Query: ?pagina=1&limite=100&selo=PRONTO (filtra por selo)
    // ============================================================
    if (u.pathname === '/api/agente/produtos-qualificados' && req.method === 'GET') {
      let blingToken = (req.headers['authorization'] || '').replace(/^Bearer\s+/i, '');
      if (!blingToken) blingToken = lerTokenBlingDoArquivo();
      if (!blingToken) {
        return send(res, 200, { success:false, error:'Token Bling não disponível' });
      }
      const pagina = Math.max(1, parseInt(u.query.pagina || '1', 10));
      const limite = Math.min(100, Math.max(1, parseInt(u.query.limite || '100', 10)));
      const filtroSelo = (u.query.selo || '').toUpperCase() || null;

      try {
        // 1) Lista paginada (campos básicos)
        const lr = await fetch(`https://www.bling.com.br/Api/v3/produtos?pagina=${pagina}&limite=${limite}`,
          { headers: { 'Authorization': 'Bearer ' + blingToken } });
        const lj = await lr.json().catch(() => ({}));
        if (!lr.ok || !Array.isArray(lj.data)) {
          return send(res, 200, { success:false, error:'Bling não retornou lista', raw: lj });
        }

        // 2) Pra cada produto, busca detalhe (pra ter midia.imagens) + qualifica.
        // Sequencial pra não estourar rate limit do Bling (~3 req/s).
        const resultado = [];
        const contadores = { OURO: 0, PRONTO: 0, PUBLICAVEL: 0, BLOQUEADO: 0 };
        for (const item of lj.data) {
          try {
            const dr = await fetch(`https://www.bling.com.br/Api/v3/produtos/${item.id}`,
              { headers: { 'Authorization': 'Bearer ' + blingToken } });
            const dj = await dr.json().catch(() => ({}));
            const adapt = dj.data ? adaptarProdutoBlingParaSimulado(dj.data) : null;
            const qual = qualificarProduto(adapt);
            const scoreV = adapt ? scoreProdutoSimulado(adapt) : { score: 0, precoVenda: 0, margem: 0 };
            contadores[qual.selo] = (contadores[qual.selo] || 0) + 1;
            if (!filtroSelo || qual.selo === filtroSelo) {
              resultado.push({
                id:                    item.id,
                codigo:                item.codigo || adapt?.sku || '',
                nome:                  item.nome   || adapt?.titulo || '',
                preco:                 item.preco  || adapt?.preco || 0,
                estoque:               item.estoqueSaldo ?? adapt?.estoque ?? 0,
                selo:                  qual.selo,
                qualificacao_score:    `${qual.score}/${qual.score_max}`,
                score_venda:           scoreV.score,                       // 0-100, usado pra ranking
                preco_venda_estimado:  +scoreV.precoVenda.toFixed(2),
                margem:                +scoreV.margem.toFixed(1),
                obrigatorios:          `${qual.obrigatorios_ok}/${qual.obrigatorios_total}`,
                recomendados:          `${qual.recomendados_ok}/${qual.recomendados_total}`,
                pendencias_count:      qual.pendencias.length,
                pronto_para_publicar:  qual.pronto_para_publicar,
              });
            }
            // Throttle leve pra não estourar rate limit
            await new Promise(r => setTimeout(r, 350));
          } catch (e) {
            console.warn(`[qualificar] falha em ${item.id}:`, e.message);
          }
        }

        // Ordena por: pronto_para_publicar primeiro, depois score_venda decrescente.
        // Bloqueados ficam no fim mesmo com score alto.
        resultado.sort((a, b) => {
          if (a.pronto_para_publicar !== b.pronto_para_publicar) {
            return a.pronto_para_publicar ? -1 : 1;
          }
          return b.score_venda - a.score_venda;
        });

        return send(res, 200, {
          success:     true,
          pagina,
          limite,
          filtro_selo: filtroSelo,
          totais:      contadores,
          retornados:  resultado.length,
          ordenado_por:'pronto_para_publicar DESC, score_venda DESC',
          produtos:    resultado,
        });
      } catch (err) {
        return send(res, 200, { success:false, error: err.message });
      }
    }

    // ============================================================
    // FASE 1 — GET /api/agente/mapeamento
    // Lista total de produtos únicos publicados, total de anúncios ativos,
    // e detalhamento das duplicatas detectadas.
    // ============================================================
    if (u.pathname === '/api/agente/mapeamento' && req.method === 'GET') {
      try {
        const total = db.prepare(`SELECT COUNT(DISTINCT bling_id) AS n FROM produtos_publicados`).get().n;
        const totalAtivos = db.prepare(`SELECT COUNT(*) AS n FROM produtos_publicados WHERE status IN ('active','under_review')`).get().n;
        const duplicatas = _stmtListarComDuplicatas.all();
        const stmtMlbsAtivos = db.prepare(`
          SELECT mlb_id, titulo, preco, status, vendas, cliques, publicado_em
          FROM produtos_publicados
          WHERE bling_id = ? AND status IN ('active','under_review')
        `);
        const duplicatasDetalhadas = duplicatas.map(d => {
          const mlbs = stmtMlbsAtivos.all(d.bling_id);
          return { bling_id: d.bling_id, qtd: d.qtd, mlbs };
        });
        return send(res, 200, {
          success: true,
          total_produtos_unicos: total,
          total_anuncios_ativos: totalAtivos,
          total_com_duplicata: duplicatas.length,
          duplicatas: duplicatasDetalhadas,
        });
      } catch (err) {
        return send(res, 500, { success: false, error: err.message });
      }
    }

    // ============================================================
    // FASE 1 — POST /api/agente/limpar-duplicatas
    // Body: { dryRun: true|false } — default true
    // Aplica regra de rendimento:
    //   - Tem com rendimento + tem sem rendimento → pausa os sem
    //   - Nenhum com rendimento → mantém o mais antigo, pausa o resto
    //   - Todos com rendimento → não faz nada
    // ============================================================
    if (u.pathname === '/api/agente/limpar-duplicatas' && req.method === 'POST') {
      try {
        const { dryRun = true } = await readBody(req);
        const mlToken = getMlToken();
        if (!mlToken) return send(res, 200, { success: false, error: 'Token ML não disponível' });

        const duplicatas = _stmtListarComDuplicatas.all();
        const acoes = [];
        const stmtMlbsDup = db.prepare(`
          SELECT mlb_id, titulo, vendas, cliques, status, publicado_em,
            julianday('now') - julianday(publicado_em) AS dias_no_ar
          FROM produtos_publicados
          WHERE bling_id = ? AND status IN ('active','under_review')
        `);

        for (const dup of duplicatas) {
          const mlbs = stmtMlbsDup.all(dup.bling_id);

          // Atualiza métricas frescas pra cada
          const metricas = [];
          for (const m of mlbs) {
            const fresh = await buscarRendimentoMLB(m.mlb_id, mlToken);
            const efetivo = fresh || { vendas: m.vendas, cliques: m.cliques, dias_no_ar: Math.floor(m.dias_no_ar), status: m.status };
            if (fresh) {
              _stmtAtualizarMetricas.run(fresh.vendas, fresh.cliques, m.mlb_id);
              if (fresh.status !== m.status) _stmtAtualizarStatus.run(fresh.status, m.mlb_id);
            }
            metricas.push({ mlb_id: m.mlb_id, titulo: m.titulo, ...efetivo });
            await new Promise(r => setTimeout(r, 300)); // throttle ML
          }

          const comRend = metricas.filter(m => temRendimento(m));
          const semRend = metricas.filter(m => !temRendimento(m) && (m.status === 'active' || m.status === 'under_review'));

          if (comRend.length > 0 && semRend.length > 0) {
            // Mantém com rendimento, pausa sem rendimento
            for (const c of comRend) {
              acoes.push({ bling_id: dup.bling_id, mlb_id: c.mlb_id, acao: 'MANTIDO (rendimento)', vendas: c.vendas, cliques: c.cliques });
            }
            for (const s of semRend) {
              if (dryRun) {
                acoes.push({ bling_id: dup.bling_id, mlb_id: s.mlb_id, acao: 'PAUSARIA (dry-run)', motivo: 'sem_rendimento_outro_tem' });
              } else {
                const ok = await pausarMLB(s.mlb_id, mlToken);
                acoes.push({ bling_id: dup.bling_id, mlb_id: s.mlb_id, acao: ok ? 'PAUSADO' : 'ERRO_PAUSAR', motivo: 'sem_rendimento_outro_tem' });
                await new Promise(r => setTimeout(r, 500));
              }
            }
          } else if (comRend.length === 0 && semRend.length > 1) {
            // Nenhum com rendimento — mantém o mais antigo, pausa os outros
            semRend.sort((a, b) => b.dias_no_ar - a.dias_no_ar);
            const manter = semRend[0];
            const pausar = semRend.slice(1);
            acoes.push({ bling_id: dup.bling_id, mlb_id: manter.mlb_id, acao: 'MANTIDO (mais antigo)', dias_no_ar: manter.dias_no_ar });
            for (const p of pausar) {
              if (dryRun) {
                acoes.push({ bling_id: dup.bling_id, mlb_id: p.mlb_id, acao: 'PAUSARIA (dry-run)', motivo: 'duplicata_sem_rendimento' });
              } else {
                const ok = await pausarMLB(p.mlb_id, mlToken);
                acoes.push({ bling_id: dup.bling_id, mlb_id: p.mlb_id, acao: ok ? 'PAUSADO' : 'ERRO_PAUSAR', motivo: 'duplicata_sem_rendimento' });
                await new Promise(r => setTimeout(r, 500));
              }
            }
          }
          // Se TODOS têm rendimento, não faz nada — todos ficam
        }

        return send(res, 200, {
          success: true,
          dryRun,
          total_duplicatas: duplicatas.length,
          total_acoes: acoes.length,
          acoes,
        });
      } catch (err) {
        return send(res, 500, { success: false, error: err.message });
      }
    }

    // ============================================================
    // FASE 1.5 — POST /api/agente/recompatibilizar/:mlbId
    // Recupera/refaz compatibilidade veicular de anúncio existente.
    // Body opcional: { titulo?, marca? } — se omitido, busca do ML.
    // ============================================================
    if (u.pathname.startsWith('/api/agente/recompatibilizar/') && req.method === 'POST') {
      try {
        const mlbId = u.pathname.replace('/api/agente/recompatibilizar/', '').trim();
        if (!mlbId || !mlbId.startsWith('MLB')) {
          return send(res, 400, { success: false, erro: 'mlbId invalido' });
        }

        const body = await readBody(req).catch(() => ({}));
        const { titulo, marca } = body || {};

        const mlToken = loadTokens().ml_access_token;
        if (!mlToken) {
          return send(res, 500, { success: false, erro: 'token ML nao disponivel' });
        }

        let dadosCompat;
        if (titulo) {
          // Usa titulo passado no body
          dadosCompat = extrairDadosDoTitulo(titulo, marca || null);
        } else {
          // Busca o item no ML pra pegar o titulo
          const PROXY_URL    = process.env.ML_PROXY_URL    || 'https://ml-proxy.agentemarkt.com';
          const PROXY_SECRET = process.env.ML_PROXY_SECRET || 'agente-ml-proxy-2026';
          const respItem = await fetch(`${PROXY_URL}/items/${mlbId}`, {
            headers: {
              'Authorization':  `Bearer ${mlToken}`,
              'X-Proxy-Secret': PROXY_SECRET,
            },
          });
          if (!respItem.ok) {
            return send(res, respItem.status, { success: false, erro: 'erro ao buscar item ML' });
          }
          const item = await respItem.json();
          dadosCompat = extrairDadosDoTitulo(item.title, null);
        }

        const resultado = await criarCompatibilidades(mlbId, dadosCompat, mlToken);
        return send(res, 200, {
          success: true,
          mlbId,
          dadosExtraidos: dadosCompat,
          resultado,
        });
      } catch (err) {
        console.error('[RECOMPATIBILIZAR] erro:', err);
        return send(res, 500, { success: false, erro: err.message });
      }
    }

    // ============================================================
    // POST /api/agente/atualizar-preco — atualiza preço de anúncio existente no ML
    // Body: { mlbId, novoPreco?, recalcularDoBling?, blingId?, reativar? }
    // - novoPreco: usa esse valor direto.
    // - recalcularDoBling + blingId: busca preço atual do Bling, aplica margem segurança.
    // - reativar: além de mudar preço, faz status:active.
    // ============================================================
    if (u.pathname === '/api/agente/atualizar-preco' && req.method === 'POST') {
      const body = await readBody(req);
      const { mlbId, novoPreco, recalcularDoBling, blingId, reativar } = body || {};
      if (!mlbId) return send(res, 200, { success:false, error:'mlbId obrigatório' });

      let mlToken = (req.headers['authorization'] || '').replace(/^Bearer\s+/i, '');
      if (!mlToken) mlToken = getMlToken();
      if (!mlToken) return send(res, 200, { success:false, error:'Token ML não disponível' });

      let precoFinal = Number(novoPreco) || 0;

      // Recalcular do Bling se solicitado
      if (recalcularDoBling && blingId) {
        let blingToken = (req.headers['x-bling-token'] || '').replace(/^Bearer\s+/i, '');
        if (!blingToken) blingToken = lerTokenBlingDoArquivo();
        if (!blingToken) return send(res, 200, { success:false, error:'Token Bling não disponível' });

        try {
          const r = await fetch(`https://www.bling.com.br/Api/v3/produtos/${blingId}`,
            { headers: { 'Authorization': 'Bearer ' + blingToken } });
          const j = await r.json().catch(() => ({}));
          if (!r.ok || !j.data) return send(res, 200, { success:false, error:'Produto Bling não encontrado' });
          const adapt = adaptarProdutoBlingParaSimulado(j.data);
          const sc = scoreProdutoSimulado(adapt);
          precoFinal = sc.precoVenda;
        } catch (err) {
          return send(res, 200, { success:false, error:'Falha ao buscar Bling: ' + err.message });
        }
      }

      if (precoFinal <= 0) return send(res, 200, { success:false, error:'Preço inválido' });

      // PUT no ML pra atualizar (via mlFetch — passa pelo proxy automaticamente se ML_USE_PROXY)
      try {
        const updateBody = { price: precoFinal };
        if (reativar) updateBody.status = 'active';

        const upResp = await mlFetch(`https://api.mercadolibre.com/items/${mlbId}`, {
          method: 'PUT',
          headers: {
            'Authorization': 'Bearer ' + mlToken,
            'Content-Type':  'application/json',
          },
          body: JSON.stringify(updateBody),
        });
        const upData = await upResp.json().catch(() => ({}));
        if (!upResp.ok || upData.error) {
          return send(res, 200, { success:false, error:'ML rejeitou atualização', raw: upData });
        }
        console.log(`💰 [agente] Preço atualizado: ${mlbId} → R$ ${precoFinal.toFixed(2)}${reativar ? ' (reativado)' : ''}`);
        return send(res, 200, {
          success:    true,
          mlbId,
          preco_novo: precoFinal,
          status:     upData.status || 'desconhecido',
          permalink:  upData.permalink || null,
          mensagem:   `✅ Preço atualizado para R$ ${precoFinal.toFixed(2)}${reativar ? ' (reativado)' : ''}`,
        });
      } catch (err) {
        return send(res, 200, { success:false, error: err.message });
      }
    }

    // ============================================================
    // GET /api/agente/explicar/:id — passo a passo: como a IA vê o produto
    // ============================================================
    if (u.pathname.startsWith('/api/agente/explicar/') && req.method === 'GET') {
      const id = u.pathname.replace('/api/agente/explicar/', '');
      let blingToken = (req.headers['authorization'] || '').replace(/^Bearer\s+/i, '');
      if (!blingToken) blingToken = lerTokenBlingDoArquivo();
      if (!blingToken) return send(res, 200, { success:false, error:'Token Bling não disponível' });

      try {
        const r = await fetch(`https://www.bling.com.br/Api/v3/produtos/${id}`,
          { headers: { 'Authorization': 'Bearer ' + blingToken } });
        const j = await r.json().catch(() => ({}));
        if (!r.ok || !j.data) return send(res, 200, { success:false, error:'Produto não encontrado', raw: j });

        const adapt = adaptarProdutoBlingParaSimulado(j.data);
        const qual = qualificarProduto(adapt);
        const scoreV = explicarScore(adapt);
        const cfg = global.agenteConfig || {};
        const scoreMin = cfg.scoreMinimo || 60;

        // Decisão final
        let decisao;
        if (!qual.pronto_para_publicar) {
          decisao = { acao: 'NÃO PUBLICAR', razao: 'Falta requisito obrigatório (qualificação)', detalhes: qual.pendencias };
        } else if (scoreV.score < scoreMin) {
          decisao = { acao: 'NÃO PUBLICAR', razao: `Score (${scoreV.score}) abaixo do mínimo (${scoreMin})`, detalhes: 'Veja itens negativos no detalhamento' };
        } else if (scoreV.score >= 80) {
          decisao = { acao: 'PUBLICAR PRIORITÁRIO', razao: `Score ${scoreV.score} (excelente)`, detalhes: 'Top do ranking, publicar primeiro' };
        } else {
          decisao = { acao: 'PUBLICAR', razao: `Score ${scoreV.score} ≥ mínimo ${scoreMin}`, detalhes: 'Publicação normal' };
        }

        return send(res, 200, {
          success: true,
          produto: {
            id:           adapt.id,
            codigo:       adapt.sku,
            titulo:       adapt.titulo,
            marca:        adapt.marca,
            preco_bling:  adapt.preco,
            preco_custo:  adapt.preco_custo,
            estoque:      adapt.estoque,
            n_imagens:    adapt.imagens.length,
          },
          passo_1_qualificacao: {
            selo:                 qual.selo,
            score:                `${qual.score}/${qual.score_max}`,
            obrigatorios:         `${qual.obrigatorios_ok}/${qual.obrigatorios_total}`,
            recomendados:         `${qual.recomendados_ok}/${qual.recomendados_total}`,
            pendencias:           qual.pendencias.map(c => ({ codigo: c, ...explicarPendencia(c) })),
            pronto_para_publicar: qual.pronto_para_publicar,
          },
          passo_2_score_venda:  scoreV,
          passo_3_config_agente: {
            score_minimo:           scoreMin,
            markup_aplicado:        scoreV.markup,
            modo_primeiras_vendas:  !!cfg.modoPrimeirasVendas,
            preferir_premium:       cfg.preferirPremium !== false,
            frete_gratis_minimo:    cfg.freteGratisMinimo || 79,
          },
          passo_4_decisao: decisao,
          link_bling: `https://www.bling.com.br/produtos.editar.php?id=${adapt.bling_id}`,
        });
      } catch (err) {
        return send(res, 200, { success:false, error: err.message });
      }
    }

    // ============================================================
    // GET /api/agente/pendencias/:id — diagnóstico detalhado
    // ============================================================
    if (u.pathname.startsWith('/api/agente/pendencias/') && req.method === 'GET') {
      const id = u.pathname.replace('/api/agente/pendencias/', '');
      let blingToken = (req.headers['authorization'] || '').replace(/^Bearer\s+/i, '');
      if (!blingToken) blingToken = lerTokenBlingDoArquivo();
      if (!blingToken) return send(res, 200, { success:false, error:'Token Bling não disponível' });

      try {
        const r = await fetch(`https://www.bling.com.br/Api/v3/produtos/${id}`,
          { headers: { 'Authorization': 'Bearer ' + blingToken } });
        const j = await r.json().catch(() => ({}));
        if (!r.ok || !j.data) return send(res, 200, { success:false, error:'Produto não encontrado', raw: j });
        const adapt = adaptarProdutoBlingParaSimulado(j.data);
        const qual  = qualificarProduto(adapt);
        const detalhes = qual.pendencias.map(c => ({ codigo: c, ...explicarPendencia(c) }));
        return send(res, 200, {
          success: true,
          produto: {
            id:             adapt.id,
            codigo:         adapt.sku,
            titulo:         adapt.titulo,
            marca:          adapt.marca,
            preco:          adapt.preco,
            preco_custo:    adapt.preco_custo,
            estoque:        adapt.estoque,
            n_imagens:      adapt.imagens.length,
            peso_g:         adapt.peso_g,
            tem_dimensoes:  adapt.altura_cm > 0 && adapt.largura_cm > 0 && adapt.comprimento_cm > 0,
          },
          qualificacao:        qual,
          pendencias_legiveis: detalhes,
          link_bling:          `https://www.bling.com.br/produtos.editar.php?id=${adapt.bling_id}`,
        });
      } catch (err) {
        return send(res, 200, { success:false, error: err.message });
      }
    }

    // ============================================================
    // POST /api/agente/publicar-fila — publica todos PRONTO/OURO em lote
    // Body: { pagina?, limite?, max?, somenteSelo?, dryRun? }
    // ============================================================
    if (u.pathname === '/api/agente/publicar-fila' && req.method === 'POST') {
      const body = await readBody(req).catch(() => ({}));
      const pagina      = body.pagina || 1;
      const limite      = Math.min(100, body.limite || 100);
      const max         = body.max || 10; // máx publicações por chamada (segurança)
      const somenteSelo = body.somenteSelo || ['OURO', 'PRONTO']; // padrão: só os melhores
      const dryRun      = body.dryRun !== false; // default true (preview)

      let blingToken = (req.headers['authorization'] || '').replace(/^Bearer\s+/i, '');
      if (!blingToken) blingToken = lerTokenBlingDoArquivo();
      if (!blingToken) return send(res, 200, { success:false, error:'Token Bling não disponível' });

      try {
        // 1) Pega lista qualificada
        const qResp = await fetch(`http://127.0.0.1:${PORT}/api/agente/produtos-qualificados?pagina=${pagina}&limite=${limite}`,
          { headers: { 'Authorization': 'Bearer ' + blingToken } });
        const qData = await qResp.json().catch(() => ({}));
        if (!qData.success) return send(res, 200, { success:false, error:'Falha ao qualificar', raw: qData });

        // 2) Filtra elegíveis
        const elegiveis = (qData.produtos || [])
          .filter(p => somenteSelo.includes(p.selo))
          .slice(0, max);

        const resultados = [];
        for (const p of elegiveis) {
          if (dryRun) {
            resultados.push({ id: p.id, titulo: p.nome, selo: p.selo, status: 'preview-only' });
            continue;
          }
          // Publica chamando o próprio handler
          try {
            const pubResp = await fetch(`http://127.0.0.1:${PORT}/api/agente/publicar`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body:    JSON.stringify({ produtoId: String(p.id), modoTeste: false }),
            });
            const pubData = await pubResp.json().catch(() => ({}));
            resultados.push({
              id:      p.id,
              titulo:  p.nome,
              selo:    p.selo,
              status:  pubData.success ? 'publicado' : 'falhou',
              mlb_id:  pubData.mlbId || null,
              erro:    pubData.error || null,
            });
            // Throttle entre publicações
            await new Promise(r => setTimeout(r, 1500));
          } catch (e) {
            resultados.push({ id: p.id, titulo: p.nome, status: 'erro', erro: e.message });
          }
        }

        return send(res, 200, {
          success:     true,
          dryRun,
          totais:      qData.totais,
          elegiveis:   elegiveis.length,
          processados: resultados.length,
          publicados:  resultados.filter(r => r.status === 'publicado').length,
          falhas:      resultados.filter(r => r.status === 'falhou' || r.status === 'erro').length,
          resultados,
        });
      } catch (err) {
        return send(res, 200, { success:false, error: err.message });
      }
    }

    // PUBLICAR UM PRODUTO (preview/real) — aceita SIMs e IDs Bling
    if (u.pathname === '/api/agente/publicar' && req.method === 'POST') {
      const token = getMlToken();
      try {
        const { produtoId, modoTeste = true, ignorarQualificacao = false } = await readBody(req);
        let produto;
        let qualificacao = null;

        // FASE 1 — Anti-duplicidade Bling↔ML (só faz sentido pra produtos Bling reais)
        const _produtoIdBling = String(produtoId || '');
        let _mlbsParaPausarDepois = [];
        if (!modoTeste && _produtoIdBling && !_produtoIdBling.startsWith('SIM')) {
          try {
            const _check = await checarDuplicidadeBling(_produtoIdBling, token);
            if (!_check.podeRepublicar) {
              return send(res, 200, {
                success: false,
                error: '⛔ Produto já publicado',
                motivo: _check.motivo,
                mlb_existente: _check.mlbsAtivos.map(m => m.mlb_id),
                detalhe: _check.motivo === 'tem_mlb_com_rendimento'
                  ? 'Já tem MLB com rendimento — duplicar canibaliza vendas'
                  : 'Publicado há <24h — aguardando dados pra decidir',
              });
            }
            _mlbsParaPausarDepois = _check.mlbsParaPausar || [];
          } catch (err) {
            console.error('[anti-dup] erro:', err.message);
            // Em erro de checagem, deixa publicar (fail-open). Logado pra investigar.
          }
        }

        // Decide fonte: SIM* = simulado, qualquer outra coisa = Bling
        if (String(produtoId).startsWith('SIM')) {
          produto = catalogoSimulado.find(p => p.id === produtoId);
          if (!produto) return send(res, 200, { success:false, error:'Produto SIM não encontrado' });
        } else {
          // Busca no Bling (token híbrido: header > tokens.json)
          let blingToken = (req.headers['authorization'] || '').replace(/^Bearer\s+/i, '');
          if (!blingToken) blingToken = lerTokenBlingDoArquivo();
          if (!blingToken) {
            return send(res, 200, { success:false, error:'Token Bling não disponível. Conecte o Bling em Config.' });
          }

          const blingFetchLocal = async (url, tk) => {
            const r = await fetch(url, { headers: { 'Authorization': 'Bearer ' + tk, 'Accept': 'application/json' } });
            const data = await r.json().catch(() => ({}));
            return { ok: r.ok, status: r.status, data };
          };

          const r = await blingFetchLocal(`https://www.bling.com.br/Api/v3/produtos/${produtoId}`, blingToken);
          if (!r.ok || !r.data?.data) {
            return send(res, 200, { success:false, error:'Produto não encontrado no Bling', bling_id: produtoId, raw_status: r.status });
          }
          produto = adaptarProdutoBlingParaSimulado(r.data.data);
          if (!produto) return send(res, 200, { success:false, error:'Adaptação Bling→ML falhou' });

          // MUDANÇA 1 — Auto-detecta categoria-folha do ML pelo nome do produto.
          // Necessário porque MLB1747 (default Bling) é categoria-pai e ML
          // só aceita categorias-folha (leaf) na publicação.
          if (token) {
            try {
              const cr = await fetch(
                `https://api.mercadolibre.com/sites/MLB/domain_discovery/search?q=${encodeURIComponent(produto.titulo || '')}`,
                { headers: { 'Authorization': 'Bearer ' + token } }
              );
              const cj = await cr.json().catch(() => []);
              if (Array.isArray(cj) && cj[0]?.category_id) {
                console.log(`🔍 [agente] Categoria detectada: ${cj[0].category_id} (${cj[0].domain_name || 'sem domain'}) para "${produto.titulo}"`);
                produto.categoria_ml = cj[0].category_id;
              } else {
                console.warn(`⚠️ [agente] domain_discovery não retornou categoria pra "${produto.titulo}" — mantendo ${produto.categoria_ml}`);
              }
            } catch (e) {
              console.warn(`⚠️ [agente] domain_discovery falhou: ${e.message}`);
            }
          }

          // Qualificação obrigatória pra produto Bling
          qualificacao = qualificarProduto(produto);
          if (!qualificacao.pronto_para_publicar && !ignorarQualificacao) {
            const pendsLegiveis = qualificacao.pendencias.map(c => ({ codigo: c, ...explicarPendencia(c) }));
            return send(res, 200, {
              success: false,
              error:   `🚫 Produto BLOQUEADO: ${qualificacao.pendencias.length} pendências obrigatórias`,
              produto: { id: produto.id, titulo: produto.titulo, marca: produto.marca },
              qualificacao,
              pendencias_legiveis: pendsLegiveis,
              dica: 'Resolva as pendências no Bling ou use ignorarQualificacao:true (não recomendado)',
            });
          }

          // MUDANÇA 2 — Aplica scoreMinimo da config global do agente.
          // (regras de margem, estoque, marca, frete grátis — definidas em scoreProdutoSimulado)
          const cfgAgente = global.agenteConfig || {};
          const scoreMin  = cfgAgente.scoreMinimo || 60;
          const scoreCalc = scoreProdutoSimulado(produto);
          if (scoreCalc.score < scoreMin && !ignorarQualificacao) {
            return send(res, 200, {
              success: false,
              error:   `📉 Score de venda (${scoreCalc.score}) abaixo do mínimo (${scoreMin}). Produto não recomendado pra publicação.`,
              produto: { id: produto.id, titulo: produto.titulo, marca: produto.marca },
              score:         scoreCalc.score,
              score_minimo:  scoreMin,
              detalhe_score: explicarScore(produto),
              dica: 'Aumente preço/estoque, mude pra marca top, ou use ignorarQualificacao:true',
            });
          }
        }

        const { score, precoVenda } = scoreProdutoSimulado(produto);

        // GAP 8 — aplica reserva de segurança
        const reserva = (global.estoqueConfig?.reservaSeguranca) || 0;
        const estoqueParaML = Math.max(0, (produto.estoque || 0) - reserva);

        // Compliance: sanitiza título removendo termos proibidos (réplica/zap/concorrente/etc)
        const tituloCompl = validarTituloML(produto.titulo || '');
        // Aplica nomes populares (top sellers: "bolsa airbag" em vez de "módulo inflador")
        const tituloFinal = aplicarNomesPopulares(tituloCompl.titulo);
        if (!tituloCompl.valido) {
          console.log(`🛡️ [compliance] Título sanitizado antes de publicar: removidos ${tituloCompl.termosRemovidos.join(', ')}`);
        }
        if (tituloFinal !== tituloCompl.titulo) {
          console.log(`🤖 [agente] Nomes populares aplicados no título`);
        }

        // Estratégia Clássico vs Premium — baseado em preço e config
        const cfgAg  = global.agenteConfig || {};
        const minFrete = cfgAg.freteGratisMinimo || 79;
        const preferPrem = cfgAg.preferirPremium !== false;
        const listingType = (preferPrem && precoVenda >= minFrete) ? 'gold_special' : 'gold_pro';
        const freteGratis = precoVenda >= minFrete;
        console.log(`🤖 [agente] Listagem: ${listingType === 'gold_special' ? 'PREMIUM' : 'CLÁSSICO'} (R$ ${precoVenda.toFixed(2)}${freteGratis ? ' + frete grátis' : ''})`);

        // FASE 1.6 - FRENTE A: decisão inteligente de PART_NUMBER
        const decisaoPartNumber = decidirPartNumber(produto.categoria_ml, produto.sku);
        console.log(`[FASE1.6-A] PART_NUMBER decisão: ${decisaoPartNumber.motivo} (categoria=${produto.categoria_ml}, codigo="${produto.sku}")`);
        // Pra Frente B usar na descrição
        const codigoPecaDescricao = decisaoPartNumber.enviarPartNumber
          ? decisaoPartNumber.partNumber
          : 'Sem Código';

        const payload = {
          title: tituloFinal.substring(0, 60),
          category_id: produto.categoria_ml,
          price: +precoVenda.toFixed(2),
          currency_id: 'BRL',
          available_quantity: Math.min(estoqueParaML, 50),
          buying_mode: 'buy_it_now',
          condition: produto.condicao,
          listing_type_id: listingType,
          description: { plain_text: produto.descricao },
          pictures: produto.imagens.map(url => ({ source: url })),
          shipping: { mode: 'me2', local_pick_up: false, free_shipping: freteGratis },
          seller_custom_field: produto.sku,
          attributes: [
            { id: 'BRAND',              value_name: produto.marca },
            { id: 'ITEM_CONDITION',     value_id:   '2230284' },
            { id: 'MODEL',              value_name: String(produto.modelo || produto.sku || 'Padrão') },
            { id: 'POWER_SUPPLY_TYPES', value_name: 'Mecânico' },
          ],
        };

        // FASE 1.6 - FRENTE A: PART_NUMBER apenas pra mecânica com código OEM real
        if (decisaoPartNumber.enviarPartNumber) {
          payload.attributes.push({ id: 'PART_NUMBER', value_name: decisaoPartNumber.partNumber });
        }

        // PROBLEMA 1 — Categoria pode ter migrado (ex: MLB180634 → MLB120316).
        // Consulta o endpoint da categoria; se o ML retornar outra ID, atualiza payload.
        try {
          const catCheck = await mlFetch(`https://api.mercadolibre.com/categories/${payload.category_id}`);
          const catData = await catCheck.json().catch(() => ({}));
          if (catCheck.ok && catData && catData.id && catData.id !== payload.category_id) {
            console.log(`🔄 [agente] Categoria migrada: ${payload.category_id} → ${catData.id}`);
            payload.category_id = catData.id;
          }
        } catch (_) { /* segue com a categoria atual */ }

        // GAP 1 + Problema 2 — auto-preenchimento de atributos obrigatórios da categoria.
        // Também adiciona INMETRO_CERTIFICATION (Problema 3) APENAS se a categoria aceitar.
        if (payload.category_id && token) {
          try {
            const attrResp = await mlFetch(
              `https://api.mercadolibre.com/categories/${payload.category_id}/attributes`,
              { headers: { 'Authorization': 'Bearer ' + token } }
            );
            if (attrResp.ok) {
              const attrs = await attrResp.json().catch(() => []);
              const lista = Array.isArray(attrs) ? attrs : [];
              const allIds = new Set(lista.map(a => a.id));
              const obrigatorios = lista.filter(a => a.tags?.required);
              for (const attr of obrigatorios) {
                if (payload.attributes.some(a => a.id === attr.id)) continue; // já temos
                // FASE 1.6 - FRENTE A: respeita decisão sobre PART_NUMBER (carroceria não envia)
                if (attr.id === 'PART_NUMBER') {
                  if (decisaoPartNumber.enviarPartNumber) {
                    payload.attributes.push({ id: 'PART_NUMBER', value_name: decisaoPartNumber.partNumber });
                  } else {
                    console.log(`[FASE1.6-A] Auto-fill PART_NUMBER suprimido: ${decisaoPartNumber.motivo}`);
                  }
                  continue;
                }
                if (attr.id === 'GTIN' && produto.ean) {
                  payload.attributes.push({ id: 'GTIN', value_name: String(produto.ean) });
                } else if (attr.id === 'SELLER_SKU' && produto.sku) {
                  payload.attributes.push({ id: 'SELLER_SKU', value_name: String(produto.sku) });
                } else if (attr.id === 'PACKAGE_WEIGHT' && produto.peso_g) {
                  payload.attributes.push({ id: 'PACKAGE_WEIGHT', value_name: String(produto.peso_g) + ' g' });
                } else if (attr.id === 'ALPHANUMERIC_MODEL') {
                  payload.attributes.push({ id: 'ALPHANUMERIC_MODEL', value_name: String(produto.modelo || produto.sku || 'Padrão') });
                } else if (attr.id === 'MANUFACTURER') {
                  payload.attributes.push({ id: 'MANUFACTURER', value_name: String(produto.marca || '') });
                } else if (attr.id === 'POWER_SUPPLY_TYPES' && Array.isArray(attr.values) && attr.values.length > 0) {
                  // Usa primeiro valor permitido (cobre quando "Mecânico" não está na lista)
                  payload.attributes.push({ id: 'POWER_SUPPLY_TYPES', value_id: attr.values[0].id });
                } else if (Array.isArray(attr.values) && attr.values.length > 0) {
                  payload.attributes.push({ id: attr.id, value_id: attr.values[0].id });
                } else {
                  // Sem `values` na definição: cai no produto.modelo / sku / 'Padrão'
                  payload.attributes.push({
                    id: attr.id,
                    value_name: String(produto.modelo || produto.sku || 'Padrão'),
                  });
                }
                console.log(`📋 [agente] Atributo obrigatório adicionado: ${attr.id}`);
              }

              // PROBLEMA 3 — INMETRO só entra se a categoria aceitar
              if (produto.inmetro && allIds.has('INMETRO_CERTIFICATION')
                  && !payload.attributes.some(a => a.id === 'INMETRO_CERTIFICATION')) {
                payload.attributes.push({ id: 'INMETRO_CERTIFICATION', value_name: String(produto.inmetro) });
                console.log(`📋 [agente] Inmetro aceito pela categoria — adicionado`);
              }

              // Sanity: se a categoria NÃO aceita MODEL/POWER_SUPPLY_TYPES, remove o que pré-adicionamos
              for (const presetId of ['MODEL', 'POWER_SUPPLY_TYPES']) {
                if (!allIds.has(presetId)) {
                  payload.attributes = payload.attributes.filter(a => a.id !== presetId);
                }
              }
            }
          } catch (e) {
            console.log('[agente] ⚠️ Atributos da categoria não puderam ser lidos:', e.message);
          }
        }

        if (modoTeste) {
          console.log(`🤖 [agente] MODO TESTE: ${produto.titulo} → R$ ${precoVenda.toFixed(2)} (não publicado)`);
          return send(res, 200, {
            success: true,
            modoTeste: true,
            mensagem: 'Preview da publicação (não publicado no ML)',
            payload,
            score,
            produto: {
              titulo: produto.titulo,
              preco:  precoVenda,
              estoque: produto.estoque,
              sku:    produto.sku,
            },
          });
        }

        // Validação pré-publicação (só em modo real) — se tem erro crítico, não publica
        try {
          const valResp = await fetch(`http://127.0.0.1:${PORT}/api/agente/validar`, {
            method: 'POST',
            headers: { 'Authorization': 'Bearer ' + (token || ''), 'Content-Type': 'application/json' },
            body: JSON.stringify({ produtoId }),
          });
          const valData = await valResp.json().catch(() => ({}));
          if (valData && Array.isArray(valData.erros) && valData.erros.length > 0) {
            console.log(`🤖 [agente] ❌ VALIDAÇÃO FALHOU (${valData.erros.length} erros) — não publicado`);
            return send(res, 200, {
              success: false,
              error: 'Validação falhou — corrija os erros antes de publicar',
              erros: valData.erros,
              avisos: valData.avisos || [],
            });
          }
        } catch (_) { /* se validador fora, segue publicação pra não travar fluxo */ }

        if (!token) return send(res, 200, { success:false, error:'sem token ML — necessário pra publicar de verdade' });

        const pubResp = await mlFetch('https://api.mercadolibre.com/items', {
          method: 'POST',
          headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        const pubData = await pubResp.json().catch(() => ({}));

        if (pubResp.ok) {
          console.log(`🤖 [agente] ✅ PUBLICADO: ${produto.titulo} → ${pubData.id} (R$ ${precoVenda.toFixed(2)})`);
          resetPublicacoesSeNovoDia();
          global.publicacoesHoje.count++;
          global.publicacoesHoje.items.push({
            mlbId: pubData.id,
            titulo: produto.titulo,
            preco: precoVenda,
            hora: new Date().toISOString(),
          });

          // FASE 1 — Registra mapeamento Bling→ML no SQLite (só pra produtos Bling reais)
          if (pubData.id && _produtoIdBling && !_produtoIdBling.startsWith('SIM')) {
            registrarPublicacaoBlingML(_produtoIdBling, pubData.id, produto.titulo, precoVenda);
            // Fire-and-forget: pausa os antigos sem rendimento
            for (const mlbAntigo of _mlbsParaPausarDepois) {
              pausarMLB(mlbAntigo, token).then(ok => {
                if (ok) console.log(`[anti-dup] MLB ${mlbAntigo} pausado (sem rendimento, substituído por ${pubData.id})`);
              }).catch(() => {});
            }

            // FASE 1.5 — Tenta criar compatibilidades veiculares (não bloqueante).
            // Falha aqui NUNCA pode quebrar o fluxo de publicação.
            try {
              const dadosCompat = extrairDadosDoTitulo(produto.titulo, produto.marca);
              console.log(`[FASE 1.5] Dados extraidos do titulo:`, JSON.stringify(dadosCompat));
              const resultadoCompat = await criarCompatibilidades(pubData.id, dadosCompat, token);
              console.log(`[FASE 1.5] Resultado compat:`, JSON.stringify(resultadoCompat));
            } catch (errCompat) {
              console.log(`[FASE 1.5] Erro ignorado em compatibilidade:`, errCompat.message);
              // Não propaga — fluxo principal continua normal
            }
          }

          return send(res, 200, {
            success: true,
            modoTeste: false,
            mlbId: pubData.id,
            permalink: pubData.permalink,
            titulo: produto.titulo,
            preco: precoVenda,
            score,
            mensagem: `✅ Publicado com sucesso! ID: ${pubData.id}`,
          });
        }
        console.error(`🤖 [agente] ❌ ERRO ao publicar: ${pubData.message || JSON.stringify(pubData.cause)}`);
        return send(res, 200, { success: false, error: pubData.message, cause: pubData.cause, details: pubData });
      } catch (err) {
        return send(res, 200, { success: false, error: err.message });
      }
    }

    // PUBLICAR LOTE
    if (u.pathname === '/api/agente/publicar-lote' && req.method === 'POST') {
      const token = getMlToken();
      try {
        const { limiteDiario = 10, scoreMinimo = 60, modoTeste = true } = await readBody(req);
        resetPublicacoesSeNovoDia();
        const restante = limiteDiario - global.publicacoesHoje.count;
        if (restante <= 0) {
          return send(res, 200, {
            success: true,
            mensagem: `Limite diário atingido (${limiteDiario}/${limiteDiario}). Volta amanhã!`,
            publicacoesHoje: global.publicacoesHoje,
          });
        }

        const comScore = catalogoSimulado
          .filter(p => p.ativo && p.estoque > 0)
          .map(p => {
            const { score, precoVenda } = scoreProdutoSimulado(p);
            return { ...p, score, precoVenda };
          })
          .filter(p => p.score >= scoreMinimo)
          .sort((a, b) => b.score - a.score);

        const aPublicar = comScore.slice(0, restante);
        const resultados = [];
        const base = `http://127.0.0.1:${PORT}`;

        for (const p of aPublicar) {
          try {
            const pubResp = await fetch(`${base}/api/agente/publicar`, {
              method: 'POST',
              headers: { 'Authorization': 'Bearer ' + (token || ''), 'Content-Type': 'application/json' },
              body: JSON.stringify({ produtoId: p.id, modoTeste }),
            });
            const pubData = await pubResp.json();
            resultados.push({
              produto: p.titulo,
              score: p.score,
              preco: p.precoVenda,
              ...pubData,
            });
            await new Promise(r => setTimeout(r, 2000));
          } catch (e) {
            resultados.push({ produto: p.titulo, success: false, error: e.message });
          }
        }

        console.log(`🤖 [agente] Lote: ${resultados.filter(r => r.success).length}/${aPublicar.length} publicados (${modoTeste ? 'TESTE' : 'REAL'})`);

        return send(res, 200, {
          success: true,
          modoTeste,
          totalDisponivel: comScore.length,
          tentativas: aPublicar.length,
          publicados: resultados.filter(r => r.success).length,
          limiteRestante: Math.max(limiteDiario - global.publicacoesHoje.count, 0),
          resultados,
        });
      } catch (err) {
        return send(res, 200, { success: false, error: err.message });
      }
    }

    // Status do agente
    if (u.pathname === '/api/agente/status' && req.method === 'GET') {
      resetPublicacoesSeNovoDia();
      return send(res, 200, {
        success: true,
        fonte:           process.env.BLING_CLIENT_ID ? 'bling' : 'simulado',
        iaGenerativa:   !!process.env.ANTHROPIC_API_KEY,
        publicacoesHoje: global.publicacoesHoje,
        catalogoTotal:   catalogoSimulado.filter(p => p.ativo).length,
        comEstoque:      catalogoSimulado.filter(p => p.ativo && p.estoque > 0).length,
        rateLimiter:     mlRateLimiter.status(),
        agenteConfig:    global.agenteConfig,
      });
    }

    // ============================================================
    // CONFIG DO AGENTE — estratégias editáveis (top sellers 2026)
    // ============================================================
    if (u.pathname === '/api/agente/config' && req.method === 'GET') {
      return send(res, 200, { success:true, config: global.agenteConfig });
    }
    if (u.pathname === '/api/agente/config' && req.method === 'POST') {
      try {
        const body = await readBody(req).catch(() => ({}));
        const campos = ['modoPrimeirasVendas','markupNormal','markupPrimeirasVendas',
          'vendasParaSairDoModo','scoreMinimo','limiteDiario','preferirPremium','freteGratisMinimo',
          'margemSegurancaBling'];
        for (const c of campos) {
          if (body[c] !== undefined) {
            if (typeof global.agenteConfig[c] === 'boolean') global.agenteConfig[c] = !!body[c];
            else global.agenteConfig[c] = Number(body[c]);
          }
        }
        console.log(`🤖 [agente] Config atualizada:`, global.agenteConfig);
        return send(res, 200, { success:true, config: global.agenteConfig });
      } catch (error) {
        return send(res, 200, { success:false, error: error.message });
      }
    }

    // ============================================================
    // SUGESTÃO DE KITS — combinações que ativam frete grátis (≥ R$ 79)
    // ============================================================
    if (u.pathname === '/api/agente/sugerir-kits' && req.method === 'GET') {
      try {
        const produtos = catalogoSimulado.filter(p => p.ativo && p.estoque > 0);
        const cfgKit = global.agenteConfig || {};
        const minFreteKit = cfgKit.freteGratisMinimo || 79;
        const markupKit = cfgKit.modoPrimeirasVendas ? (cfgKit.markupPrimeirasVendas || 1.5) : (cfgKit.markupNormal || 2.5);
        // Kit tem markup ligeiramente menor — aumenta apelo e volume
        const markupDoKit = Math.max(1.5, markupKit - 0.2);
        const kits = [];

        const combinacoes = [
          { tipo:'freio',      itens:['pastilha','disco'],                       nome:'Kit Freio Completo' },
          { tipo:'suspensao',  itens:['amortecedor','mola','batente','coifa'],   nome:'Kit Suspensão' },
          { tipo:'motor',      itens:['filtro','vela','correia'],                nome:'Kit Revisão Motor' },
          { tipo:'embreagem',  itens:['disco','platô','plato','rolamento'],      nome:'Kit Embreagem Completo' },
        ];

        for (const combo of combinacoes) {
          const itensKit = produtos.filter(p =>
            combo.itens.some(item => String(p.titulo || '').toLowerCase().includes(item))
          );
          if (itensKit.length >= 2) {
            const custoTotal    = itensKit.reduce((s, p) => s + (p.preco_custo || 0), 0);
            const precoKit      = custoTotal * markupDoKit;
            const precoSeparado = itensKit.reduce((s, p) => s + ((p.preco_custo || 0) * markupKit), 0);
            const economia      = precoSeparado - precoKit;
            kits.push({
              nome: combo.nome,
              tipo: combo.tipo,
              itens: itensKit.map(p => ({ id:p.id, titulo:p.titulo, custo:p.preco_custo })),
              custoTotal:       parseFloat(custoTotal.toFixed(2)),
              precoKit:         parseFloat(precoKit.toFixed(2)),
              precoSeparado:    parseFloat(precoSeparado.toFixed(2)),
              economiaCliente:  parseFloat(economia.toFixed(2)),
              margemKit:        parseFloat(((precoKit - custoTotal) / precoKit * 100).toFixed(1)),
              freteGratis:      precoKit >= minFreteKit,
              listingType:      precoKit >= minFreteKit ? 'Premium' : 'Clássico',
              recomendacao:     precoKit >= minFreteKit
                                ? '✅ Kit ativa frete grátis — alta conversão!'
                                : `⚠️ Considere adicionar mais itens pra chegar em R$ ${minFreteKit}`,
            });
          }
        }

        // Sugere pares pra produtos abaixo do piso de frete grátis
        const abaixoPiso = produtos.filter(p => (p.preco_custo || 0) * markupKit < minFreteKit);
        const jaCombinado = new Set();
        for (const p of abaixoPiso) {
          const complementares = produtos.filter(q => {
            if (q.id === p.id) return false;
            const combinado = (p.preco_custo + q.preco_custo) * markupKit;
            return combinado >= minFreteKit && combinado <= 200;
          });
          if (complementares.length > 0) {
            const melhor = complementares[0];
            const chave = [p.id, melhor.id].sort().join('|');
            if (jaCombinado.has(chave)) continue;
            jaCombinado.add(chave);
            const custoTotal = p.preco_custo + melhor.preco_custo;
            const precoKit = custoTotal * markupDoKit;
            kits.push({
              nome: `Kit ${String(p.titulo).split(' ')[0]} + ${String(melhor.titulo).split(' ')[0]}`,
              tipo: 'custom',
              itens: [
                { id:p.id,      titulo:p.titulo,      custo:p.preco_custo },
                { id:melhor.id, titulo:melhor.titulo, custo:melhor.preco_custo },
              ],
              custoTotal:  parseFloat(custoTotal.toFixed(2)),
              precoKit:    parseFloat(precoKit.toFixed(2)),
              freteGratis: precoKit >= minFreteKit,
              margemKit:   parseFloat(((precoKit - custoTotal) / precoKit * 100).toFixed(1)),
              recomendacao:'💡 Sugestão: combinar pra ativar frete grátis',
            });
          }
        }

        return send(res, 200, {
          success: true,
          total: kits.length,
          kits,
          config: { minFreteKit, markupKit, markupDoKit },
          regra: `Kits com preço ≥ R$ ${minFreteKit} ativam frete grátis e convertem mais`,
        });
      } catch (error) {
        return send(res, 200, { success:false, error: error.message });
      }
    }

    // ============================================================
    // VALIDAÇÃO PRÉ-PUBLICAÇÃO — checa erros críticos + avisos + atributos ML
    // ============================================================
    if (u.pathname === '/api/agente/validar' && req.method === 'POST') {
      try {
        const token = getBearer() || getMlToken() || '';
        const { produtoId } = await readBody(req).catch(() => ({}));
        const produto = catalogoSimulado.find(p => p.id === produtoId);
        if (!produto) return send(res, 200, { success:false, error:'Produto não encontrado' });

        const erros  = [];
        const avisos = [];

        if (!produto.titulo || produto.titulo.length < 10)
          erros.push('Título muito curto (mínimo 10 caracteres)');
        if (produto.titulo && produto.titulo.length > 60)
          erros.push('Título muito longo (máximo 60 caracteres ML)');
        if (!produto.preco_custo || produto.preco_custo <= 0)
          erros.push('Preço de custo inválido');
        if (!produto.estoque || produto.estoque <= 0)
          erros.push('Sem estoque');
        if (!produto.categoria_ml)
          erros.push('Categoria ML não definida');
        if (!produto.descricao || produto.descricao.length < 20)
          erros.push('Descrição muito curta (mínimo 20 caracteres)');
        if (!produto.marca)
          avisos.push('Marca não informada — ML pode rejeitar');
        if (!produto.imagens || produto.imagens.length === 0)
          erros.push('Sem imagens — ML exige pelo menos 1');
        if (produto.imagens && produto.imagens.some(img => /placeholder/i.test(String(img))))
          avisos.push('Imagem placeholder detectada — substitua por foto real');
        if (!produto.compatibilidade || produto.compatibilidade.length === 0)
          avisos.push('Sem compatibilidade — recomendado pra autopeças');

        // RISCO 3 — Fotos: fundo branco (aviso) + mín 3 (aviso) + máx 10 (erro)
        if (produto.imagens && produto.imagens.length > 0) {
          avisos.push('📷 Garanta que a PRIMEIRA foto tem fundo BRANCO (obrigatório ML — anúncio pode ser moderado)');
          if (produto.imagens.length < 3) {
            avisos.push('📷 Recomendado ter pelo menos 3 fotos (ML prioriza anúncios com mais fotos)');
          }
          if (produto.imagens.length > 10) {
            erros.push('📷 Máximo 10 fotos por anúncio (ML não aceita mais)');
          }
        }

        // RISCO 4 — Produtos proibidos (peças usadas de segurança) + alertas
        const alertasProibidos = validarProdutoProibido(produto);
        for (const a of alertasProibidos) {
          if (a.startsWith('🚫')) erros.push(a);
          else avisos.push(a);
        }

        // RISCO 2 — Termos proibidos no título (réplica/whatsapp/concorrentes)
        const tituloCheck = validarTituloML(produto.titulo || '');
        if (!tituloCheck.valido) {
          avisos.push(`📝 Título contém termos que serão removidos: ${tituloCheck.termosRemovidos.join(', ')}`);
        }

        // ESTRATÉGIA 3 — Preço mínimo pra frete grátis (regra dos top sellers)
        const cfgVal = global.agenteConfig || {};
        const markupVal = cfgVal.modoPrimeirasVendas ? (cfgVal.markupPrimeirasVendas || 1.5) : (cfgVal.markupNormal || 2.5);
        const precoVendaVal = (produto.preco_custo || 0) * markupVal;
        const minFreteVal = cfgVal.freteGratisMinimo || 79;
        if (produto.preco_custo > 0 && precoVendaVal < minFreteVal) {
          avisos.push(`💡 Preço R$ ${precoVendaVal.toFixed(2)} não ativa frete grátis (mínimo R$ ${minFreteVal}). Considere criar um kit ou ajustar margem.`);
          const margemComFreteGratis = ((minFreteVal - produto.preco_custo) / minFreteVal) * 100;
          if (margemComFreteGratis >= 15) {
            avisos.push(`💰 Se subir pra R$ ${minFreteVal.toFixed(2)} (margem ${margemComFreteGratis.toFixed(1)}%), ativa frete grátis e converte muito mais.`);
          }
        }

        // GAP 2 — Inmetro obrigatório para autopeças (categorias comuns)
        const categoriasAutopeças = ['MLB120316','MLB180634','MLB449571','MLB180635','MLB455239','MLB455227',
          'MLB1747','MLB6312','MLB6316','MLB6320','MLB6308','MLB6328'];
        const ehAutopeca = produto.categoria_ml && categoriasAutopeças.some(c =>
          produto.categoria_ml?.startsWith(c.substring(0, 6))
        );
        if (ehAutopeca && !produto.inmetro) {
          avisos.push('⚠️ Autopeças exigem certificação Inmetro — adicione o registro');
        }

        // Atributos obrigatórios da categoria (só se token disponível)
        if (produto.categoria_ml && token) {
          try {
            const catResp = await mlFetch(
              `https://api.mercadolibre.com/categories/${produto.categoria_ml}/attributes`,
              { headers: { 'Authorization': 'Bearer ' + token } }
            );
            const attrs = await catResp.json().catch(() => []);
            if (Array.isArray(attrs)) {
              const obrigatorios = attrs.filter(a => a.tags?.required || a.tags?.catalog_required);
              for (const attr of obrigatorios) {
                if (attr.id !== 'BRAND' && attr.id !== 'ITEM_CONDITION') {
                  avisos.push(`Atributo obrigatório "${attr.name}" (${attr.id}) pode ser exigido pelo ML`);
                }
              }
            }
          } catch (_) { /* ignora — ML pode estar 429 */ }
        }

        const valido = erros.length === 0;
        return send(res, 200, {
          success: true,
          valido,
          erros,
          avisos,
          produto: produto.titulo,
          score: valido ? 'Pronto pra publicar ✅' : 'Corrigir erros antes ❌',
        });
      } catch (error) {
        return send(res, 200, { success:false, error: error.message });
      }
    }

    // ============================================================
    // FILA DE PUBLICAÇÃO COM RETRY — até 3 tentativas com delay progressivo
    // ============================================================
    if (u.pathname === '/api/agente/publicar-com-retry' && req.method === 'POST') {
      try {
        const token = getBearer() || '';
        const body = await readBody(req).catch(() => ({}));
        const produtoId  = body.produtoId;
        const modoTeste  = body.modoTeste !== false;
        const maxRetries = Math.min(Number(body.maxRetries) || 3, 5);

        let tentativa = 0;
        let resultado = null;
        const base = `http://127.0.0.1:${PORT}`;

        while (tentativa < maxRetries) {
          tentativa++;
          console.log(`🤖 [agente] Tentativa ${tentativa}/${maxRetries}: ${produtoId}`);

          const pubResp = await fetch(`${base}/api/agente/publicar`, {
            method: 'POST',
            headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
            body: JSON.stringify({ produtoId, modoTeste }),
          });
          resultado = await pubResp.json().catch(() => ({ success:false, error:'resposta inválida' }));

          if (resultado && resultado.success) {
            console.log(`🤖 [agente] ✅ Sucesso na tentativa ${tentativa}`);
            break;
          }
          // Validação não deve tentar de novo — bloqueio intencional
          if (resultado && Array.isArray(resultado.erros) && resultado.erros.length > 0) {
            console.log(`🤖 [agente] ⛔ Validação bloqueou — não faz retry`);
            break;
          }
          console.log(`🤖 [agente] ❌ Falha tentativa ${tentativa}: ${resultado?.error || 'erro'}`);

          if (tentativa < maxRetries) {
            const delay = 5000 * tentativa;
            console.log(`🤖 [agente] Aguardando ${delay/1000}s antes de tentar novamente...`);
            await new Promise(r => setTimeout(r, delay));
          }
        }

        global.filaPublicacao.unshift({
          produtoId,
          tentativas: tentativa,
          sucesso:    !!(resultado && resultado.success),
          erro:       resultado?.error || null,
          erros:      resultado?.erros || null,
          mlbId:      resultado?.mlbId || null,
          titulo:     resultado?.titulo || resultado?.produto?.titulo || null,
          modoTeste,
          data:       new Date().toISOString(),
        });
        if (global.filaPublicacao.length > 50) {
          global.filaPublicacao = global.filaPublicacao.slice(0, 50);
        }

        return send(res, 200, { ...(resultado || {}), tentativas: tentativa, maxRetries });
      } catch (error) {
        return send(res, 200, { success:false, error: error.message });
      }
    }

    if (u.pathname === '/api/agente/fila' && req.method === 'GET') {
      return send(res, 200, {
        success: true,
        fila: global.filaPublicacao || [],
        total: (global.filaPublicacao || []).length,
      });
    }

    // ============================================================
    // PEDIDOS URGENTES — SLA de despacho próximo do vencimento
    // ============================================================
    if (u.pathname === '/api/ml/pedidos/urgentes' && req.method === 'GET') {
      const token = getBearer();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      try {
        const base = `http://127.0.0.1:${PORT}`;
        const pedidosResp = await fetch(`${base}/api/ml/pedidos?status=paid`, {
          headers: { 'Authorization': 'Bearer ' + token },
        });
        const pedidosData = await pedidosResp.json();
        if (!pedidosData.success) return send(res, 200, pedidosData);

        const urgentes = [];
        for (const pedido of pedidosData.pedidos || []) {
          if (!pedido.envio?.id) continue;
          try {
            const slaResp = await mlFetch(
              `https://api.mercadolibre.com/shipments/${pedido.envio.id}/sla`,
              { headers: { 'Authorization': 'Bearer ' + token } }
            );
            const sla = await slaResp.json().catch(() => ({}));
            if (slaResp.ok && sla && sla.date) {
              const prazoMs = new Date(sla.date).getTime() - Date.now();
              const prazoHoras = Math.max(0, prazoMs / 3600000);
              urgentes.push({
                ...pedido,
                prazoHoras:   parseFloat(prazoHoras.toFixed(1)),
                prazoStatus:  prazoHoras < 6 ? '🚨 URGENTE' : prazoHoras < 12 ? '⚠️ Atenção' : '✅ OK',
                slaData:      sla.date,
              });
            }
          } catch (_) {}
          await new Promise(r => setTimeout(r, 300));
        }

        urgentes.sort((a, b) => a.prazoHoras - b.prazoHoras);
        return send(res, 200, {
          success:  true,
          total:    urgentes.length,
          urgentes,
          alertas:  urgentes.filter(u => u.prazoHoras < 6).length,
          atencao:  urgentes.filter(u => u.prazoHoras >= 6 && u.prazoHoras < 12).length,
        });
      } catch (error) {
        return send(res, 200, { success:false, error: error.message });
      }
    }

    // ============================================================
    // ESCALAR ANÚNCIOS TOP — gera variações de título pra ocupar busca
    // ============================================================
    if (u.pathname === '/api/ml/performance/escalar' && req.method === 'POST') {
      const token = getBearer();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      try {
        const body = await readBody(req).catch(() => ({}));
        const itemId    = body.itemId;
        const modoTeste = body.modoTeste !== false;
        if (!itemId) return send(res, 200, { success:false, error:'itemId obrigatório' });

        const itemResp = await mlFetch(`https://api.mercadolibre.com/items/${itemId}`, {
          headers: { 'Authorization': 'Bearer ' + token },
        });
        const item = await itemResp.json().catch(() => ({}));
        if (!item.id) return send(res, 200, { success:false, error: item.message || 'Anúncio não encontrado' });

        const tituloBase = String(item.title || '');
        const variacoesRaw = [
          ('Kit ' + tituloBase).substring(0, 60),
          tituloBase.replace(/Dianteiro|Traseiro/i, (m) => (m.toLowerCase() === 'dianteiro' ? 'Diant.' : 'Tras.')).substring(0, 60),
          (tituloBase + ' Premium').substring(0, 60),
        ];
        // Compliance: filtra termos proibidos de cada variação
        const variacoesLimpas = variacoesRaw.map(v => validarTituloML(v).titulo);
        const variacoes = [...new Set(variacoesLimpas.filter(v => v && v !== tituloBase))];

        if (modoTeste) {
          return send(res, 200, {
            success: true,
            modoTeste: true,
            itemId,
            original: tituloBase,
            variacoes,
            mensagem: 'Preview de variações (não publicado)',
          });
        }

        const resultados = [];
        for (const titulo of variacoes) {
          try {
            const payload = {
              title: titulo,
              category_id: item.category_id,
              price: item.price,
              currency_id: 'BRL',
              available_quantity: Math.min(item.available_quantity || 1, 10),
              buying_mode: 'buy_it_now',
              condition: item.condition,
              listing_type_id: item.listing_type_id,
              description: { plain_text: item.description?.plain_text || item.title },
              pictures: (item.pictures || []).map(p => ({ source: p.url || p.source })),
              shipping: item.shipping,
            };
            const pubResp = await mlFetch('https://api.mercadolibre.com/items', {
              method: 'POST',
              headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
              body: JSON.stringify(payload),
            });
            const pubData = await pubResp.json().catch(() => ({}));
            resultados.push({
              titulo,
              success: pubResp.ok,
              mlbId:   pubData.id || null,
              error:   pubData.message || null,
            });
            console.log(`🤖 [escalar] ${pubResp.ok ? '✅' : '❌'} ${titulo.substring(0, 40)}...`);
            await new Promise(r => setTimeout(r, 2000));
          } catch (e) {
            resultados.push({ titulo, success:false, error: e.message });
          }
        }
        return send(res, 200, {
          success:   true,
          original:  tituloBase,
          variacoes: resultados.filter(r => r.success).length,
          detalhes:  resultados,
        });
      } catch (error) {
        return send(res, 200, { success:false, error: error.message });
      }
    }

    // ============================================================
    // UPLOAD DE IMAGEM — recebe base64 e envia pro ML (opcionalmente vincula a item)
    // ============================================================
    if (u.pathname === '/api/ml/imagens/upload' && req.method === 'POST') {
      const token = getBearer();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      try {
        const body = await readBody(req).catch(() => ({}));
        const imageBase64 = body.imageBase64;
        const itemId     = body.itemId || null;
        if (!imageBase64) return send(res, 200, { success:false, error:'Imagem base64 obrigatória' });

        const uploadResp = await mlFetch('https://api.mercadolibre.com/pictures/items/upload', {
          method: 'POST',
          headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
          body: JSON.stringify({ file: imageBase64 }),
        });
        const uploadData = await uploadResp.json().catch(() => ({}));

        if (!uploadResp.ok || !uploadData.id) {
          return send(res, 200, { success:false, error: uploadData.message || 'Erro no upload', details: uploadData });
        }

        // Vincula ao anúncio se informado
        if (itemId) {
          try {
            const linkResp = await mlFetch(`https://api.mercadolibre.com/items/${itemId}/pictures`, {
              method: 'POST',
              headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
              body: JSON.stringify({ id: uploadData.id }),
            });
            if (linkResp.ok) {
              console.log(`📷 Imagem ${uploadData.id} vinculada ao ${itemId}`);
            }
          } catch (_) {}
        }
        return send(res, 200, {
          success: true,
          pictureId: uploadData.id,
          url: (uploadData.variations?.[0]?.url) || uploadData.secure_url || uploadData.url || null,
          itemId: itemId || null,
        });
      } catch (error) {
        return send(res, 200, { success:false, error: error.message });
      }
    }

    // Excluir pergunta (spam/concorrente)
    if (u.pathname.startsWith('/api/ml/sac/excluir/') && req.method === 'DELETE') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      try {
        const qid = u.pathname.replace('/api/ml/sac/excluir/', '');
        const r = await mlFetch(`https://api.mercadolibre.com/questions/${qid}`, {
          method:'DELETE', headers:{ 'Authorization':'Bearer '+token }});
        return send(res, 200, r.ok ? { success:true } : { success:false, error:'Erro ao excluir' });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // Stats pro dashboard
    if (u.pathname === '/api/ml/sac/stats' && req.method === 'GET') {
      return send(res, 200, { success:true, ...sacStats });
    }

    // ============= SYNC ESTOQUE BLING ↔ ML =============
    // ML pausa automaticamente quando available_quantity=0 e reativa quando >0

    // Proxy: estoque de um produto no Bling
    if (u.pathname.match(/^\/api\/estoque\/bling\/\d+$/) && req.method === 'GET') {
      const token = getMlToken(); // usa mesmo helper pra passar auth adiante
      try {
        const id = u.pathname.split('/')[4];
        const r = await fetch(`http://127.0.0.1:${PORT}/api/bling/produtos/${id}/estoques`, {
          headers:{ 'Authorization':'Bearer '+(req.headers.authorization?.replace(/^Bearer\s+/i,'').trim() || '') },
        });
        const d = await r.json().catch(()=>({}));
        return send(res, 200, { success:true, estoque:d });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // Atualizar estoque do anúncio ML
    if (u.pathname.match(/^\/api\/estoque\/ml\/MLB\d+$/) && req.method === 'PUT') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      try {
        const itemId = u.pathname.split('/').pop();
        const { quantidade } = await readBody(req);
        if (typeof quantidade !== 'number' || quantidade < 0) return send(res, 400, { success:false, error:'quantidade inválida' });
        const r = await mlFetch(`https://api.mercadolibre.com/items/${itemId}`, {
          method:'PUT',
          headers:{ 'Authorization':'Bearer '+token, 'Content-Type':'application/json' },
          body: JSON.stringify({ available_quantity: quantidade }),
        });
        const d = await r.json().catch(()=>({}));
        if (r.ok) {
          const acao = quantidade === 0 ? 'PAUSADO (sem estoque)' : `atualizado para ${quantidade}`;
          console.log(`📦 Estoque ML: ${itemId} → ${acao}`);
          estoqueStats.atualizacoes++;
          estoqueStats.historico.unshift({ ts:new Date().toISOString(), itemId, acao, quantidade });
          estoqueStats.historico = estoqueStats.historico.slice(0, 50);
          return send(res, 200, { success:true, message:`Estoque ${acao}`, data:d });
        }
        return send(res, 200, { success:false, error: d.message || 'Erro ao atualizar', details:d });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // Pausar anúncio manualmente
    if (u.pathname.match(/^\/api\/estoque\/ml\/MLB\d+\/pausar$/) && req.method === 'PUT') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      try {
        const itemId = u.pathname.split('/')[4];
        const r = await mlFetch(`https://api.mercadolibre.com/items/${itemId}`, {
          method:'PUT',
          headers:{ 'Authorization':'Bearer '+token, 'Content-Type':'application/json' },
          body: JSON.stringify({ status:'paused' }),
        });
        const d = await r.json().catch(()=>({}));
        if (r.ok) console.log(`⏸️ Anúncio ${itemId} PAUSADO manualmente`);
        return send(res, 200, r.ok ? { success:true, message:'Anúncio pausado' } : { success:false, error:d.message });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // Reativar anúncio manualmente
    if (u.pathname.match(/^\/api\/estoque\/ml\/MLB\d+\/reativar$/) && req.method === 'PUT') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      try {
        const itemId = u.pathname.split('/')[4];
        const r = await mlFetch(`https://api.mercadolibre.com/items/${itemId}`, {
          method:'PUT',
          headers:{ 'Authorization':'Bearer '+token, 'Content-Type':'application/json' },
          body: JSON.stringify({ status:'active' }),
        });
        const d = await r.json().catch(()=>({}));
        if (r.ok) console.log(`▶️ Anúncio ${itemId} REATIVADO`);
        return send(res, 200, r.ok ? { success:true, message:'Anúncio reativado' } : { success:false, error:d.message });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // Listar todos os anúncios ML com estoque atual + resumo
    if (u.pathname === '/api/estoque/ml/todos' && req.method === 'GET') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      try {
        const limiteBaixo = parseInt(u.query.limiteBaixo, 10) || 3;
        const meR = await mlFetch('https://api.mercadolibre.com/users/me', {
          headers:{ 'Authorization':'Bearer '+token }});
        const me = await meR.json();
        const itR = await mlFetch(`https://api.mercadolibre.com/users/${me.id}/items/search?limit=50`, {
          headers:{ 'Authorization':'Bearer '+token }});
        const itD = await itR.json().catch(()=>({}));
        const itemIds = itD.results || [];
        if (itemIds.length === 0) return send(res, 200, { success:true, itens:[], total:0 });
        const batch = itemIds.slice(0, 20).join(',');
        const dR = await mlFetch(`https://api.mercadolibre.com/items?ids=${batch}&attributes=id,title,available_quantity,status,sub_status,price`, {
          headers:{ 'Authorization':'Bearer '+token }});
        const det = await dR.json().catch(()=>[]);
        const itens = (Array.isArray(det) ? det : []).map(d => {
          const body = d.body || {};
          const subStatus = body.sub_status || [];
          const estoque = body.available_quantity || 0;
          return {
            id:      body.id,
            titulo:  body.title,
            estoque,
            preco:   body.price || 0,
            status:  body.status,
            subStatus,
            pausadoPorEstoque: subStatus.includes('out_of_stock'),
            alerta: estoque <= limiteBaixo && estoque > 0 && body.status === 'active' ? '⚠️ Estoque baixo!' :
                    estoque === 0 ? '🚨 Sem estoque' : null,
          };
        });
        const semEstoque   = itens.filter(i => i.estoque === 0).length;
        const estoqueBaixo = itens.filter(i => i.estoque > 0 && i.estoque <= limiteBaixo).length;
        const ativos       = itens.filter(i => i.status === 'active').length;
        const pausados     = itens.filter(i => i.status === 'paused').length;
        return send(res, 200, {
          success: true, itens, total: itens.length,
          resumo: {
            ativos, pausados, semEstoque, estoqueBaixo,
            alerta: semEstoque > 0 ? `🚨 ${semEstoque} anúncios sem estoque!`
                 : estoqueBaixo > 0 ? `⚠️ ${estoqueBaixo} anúncios com estoque baixo`
                 : '✅ Estoque saudável em todos os anúncios',
          },
        });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // SYNC Bling → ML (lote)
    if (u.pathname === '/api/estoque/sync' && req.method === 'POST') {
      const mlToken = getMlToken();
      if (!mlToken) return send(res, 401, { success:false, error:'sem token ML' });
      try {
        const { mapeamentos } = await readBody(req);
        if (!Array.isArray(mapeamentos) || mapeamentos.length === 0) {
          return send(res, 400, { success:false, error:'mapeamentos (array) obrigatório' });
        }
        const resultados = [];
        for (const map of mapeamentos) {
          try {
            const mlR = await mlFetch(`https://api.mercadolibre.com/items/${map.mlItemId}?attributes=id,available_quantity,status`, {
              headers:{ 'Authorization':'Bearer '+mlToken }});
            const mlIt = await mlR.json();
            const estoqueML    = mlIt.available_quantity || 0;
            const estoqueBling = Number(map.estoqueBling) || 0;
            if (estoqueBling !== estoqueML) {
              const upR = await mlFetch(`https://api.mercadolibre.com/items/${map.mlItemId}`, {
                method:'PUT',
                headers:{ 'Authorization':'Bearer '+mlToken, 'Content-Type':'application/json' },
                body: JSON.stringify({ available_quantity: estoqueBling }),
              });
              const acao = estoqueBling === 0
                ? '⏸️ PAUSADO (sem estoque)'
                : (estoqueML === 0 && estoqueBling > 0)
                  ? '▶️ REATIVADO'
                  : `📦 ${estoqueML} → ${estoqueBling}`;
              console.log(`📦 Sync: ${map.sku || map.mlItemId} ${acao}`);
              resultados.push({ sku:map.sku, mlItemId:map.mlItemId, estoqueAnterior:estoqueML, estoqueNovo:estoqueBling, acao, sucesso:upR.ok });
              if (upR.ok) {
                estoqueStats.atualizacoes++;
                estoqueStats.historico.unshift({ ts:new Date().toISOString(), itemId:map.mlItemId, acao, quantidade: estoqueBling, sku:map.sku });
                estoqueStats.historico = estoqueStats.historico.slice(0, 50);
              }
            } else {
              resultados.push({ sku:map.sku, mlItemId:map.mlItemId, estoque:estoqueML, acao:'✅ Sincronizado', sucesso:true });
            }
            await new Promise(r => setTimeout(r, 500));
          } catch(err) {
            resultados.push({ sku:map.sku, mlItemId:map.mlItemId, sucesso:false, error:err.message });
          }
        }
        const atualizados   = resultados.filter(r => r.estoqueNovo !== undefined).length;
        const sincronizados = resultados.filter(r => r.acao === '✅ Sincronizado').length;
        estoqueStats.ultimoSync = new Date().toISOString();
        return send(res, 200, {
          success: true, total: resultados.length, atualizados, sincronizados,
          detalhes: resultados,
          resumo: atualizados > 0
            ? `📦 ${atualizados} estoque(s) atualizado(s), ${sincronizados} já sincronizado(s)`
            : `✅ Todos os ${sincronizados} estoques já estão sincronizados`,
        });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // Stats pro dashboard
    if (u.pathname === '/api/estoque/stats' && req.method === 'GET') {
      return send(res, 200, { success:true, ...estoqueStats });
    }

    // ============= RATE LIMITER — status =============
    if (u.pathname === '/api/rate-limiter/status' && req.method === 'GET') {
      return send(res, 200, {
        success: true,
        ...mlRateLimiter.status(),
        monitores: {
          compatibilidade: '12h',
          sac:             '30min',
          estoque:         '2h',
          tokenRefresh:    '1h',
        },
      });
    }

    // ============================================================
    // PROXY CLOUDFLARE — status e teste (contorna bloqueio ML no VPS)
    // ============================================================
    if (u.pathname === '/api/proxy/status' && req.method === 'GET') {
      if (!ML_USE_PROXY) {
        return send(res, 200, {
          success: true,
          proxyAtivo: false,
          mensagem: 'Proxy não configurado — adicione ML_PROXY_URL no .env',
          instrucoes: {
            passo1: 'Criar Worker no Cloudflare Dashboard (dash.cloudflare.com → Workers → Create)',
            passo2: 'Colar o código que está comentado no topo do server.js',
            passo3: 'Configurar route: ml-proxy.agentemarkt.com/*',
            passo4: 'Adicionar ML_PROXY_URL=https://ml-proxy.agentemarkt.com no .env do servidor',
            passo5: 'Reiniciar o servidor',
          },
        });
      }
      try {
        const testResp = await fetch(ML_PROXY_URL + '/sites/MLB', {
          headers: {
            'X-Proxy-Secret': ML_PROXY_SECRET,
            'Accept': 'application/json',
          },
        });
        const data = await testResp.json().catch(() => null);
        return send(res, 200, {
          success:      testResp.ok,
          proxyAtivo:   true,
          proxyUrl:     ML_PROXY_URL,
          status:       testResp.status,
          mlRespondeu:  !!data,
          teste: data ? 'ML respondeu via proxy ✅' : 'ML não respondeu',
        });
      } catch (error) {
        return send(res, 200, {
          success: false,
          proxyAtivo: true,
          proxyUrl: ML_PROXY_URL,
          error: error.message,
        });
      }
    }

    // ============= WEBHOOKS ML — stats, vendas, missed_feeds =============
    if (u.pathname === '/api/webhooks/status' && req.method === 'GET') {
      return send(res, 200, {
        success: true,
        callbackUrl: 'https://agentemarkt.com/webhooks',
        stats: global.webhookStats || { total:0, porTopic:{}, historico:[], ultimoRecebido:null },
        vendasRecentes: (global.vendasRecentes || []).slice(0, 20),
        topicsAtivos: ['items','questions','orders_v2','payments','shipments','messages','claims'],
        instrucoes: {
          passo1: 'Ir em https://developers.mercadolivre.com.br/devcenter',
          passo2: 'Editar o app (Client ID: 3688973136843575)',
          passo3: 'No campo "Notifications Callback URL" colocar: https://agentemarkt.com/webhooks',
          passo4: 'Marcar os topics: items, questions, orders_v2, payments, shipments',
          passo5: 'Salvar',
        },
      });
    }

    // Notificações perdidas (se o server ficou fora do ar)
    if (u.pathname === '/api/webhooks/perdidas' && req.method === 'GET') {
      const token = getMlToken();
      if (!token) return send(res, 401, { success:false, error:'sem token ML' });
      try {
        const appId = process.env.ML_CLIENT_ID || (loadEnv().ML_CLIENT_ID) || '3688973136843575';
        const r = await mlFetch(`https://api.mercadolibre.com/missed_feeds?app_id=${appId}`, {
          headers:{ 'Authorization':'Bearer '+token }});
        const d = await r.json().catch(()=>({}));
        return send(res, 200, { success:true, perdidas:d });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // Vendas alimentadas pelo topic orders_v2
    if (u.pathname === '/api/webhooks/vendas' && req.method === 'GET') {
      const vendas = global.vendasRecentes || [];
      // Vendas de hoje
      const hoje = new Date(); hoje.setHours(0,0,0,0);
      const doDia = vendas.filter(v => new Date(v.data) >= hoje);
      const totalHoje = doDia.reduce((s, v) => s + Number(v.valor || 0), 0);
      return send(res, 200, {
        success: true,
        vendas,
        total: vendas.length,
        hoje: { count: doDia.length, valor: totalHoje },
      });
    }

    if (u.pathname === '/api/precificacao/calcular-lote' && req.method === 'POST') {
      try {
        const { produtos, margemDesejada=20, tipoAnuncio='premium', imposto=0 } = await readBody(req);
        const resultados = [];
        for (const p of (produtos||[])) {
          const r = calcularPrecoTopSeller({
            custo: p.custo || p.preco,
            margemDesejada, tipoAnuncio,
            pesoKg: p.pesoKg || p.peso || 1,
            imposto,
            custoEmbalagem: p.custoEmbalagem || 2,
            freteGratis: true,
          });
          resultados.push({ nome:p.nome, sku:p.sku||p.codigo, ...r });
        }
        const saudaveis = resultados.filter(r => r.status?.nivel === 'saudavel' || r.status?.nivel === 'excelente').length;
        const apertados = resultados.filter(r => r.status?.nivel === 'apertado').length;
        const criticos  = resultados.filter(r => r.status?.nivel === 'critico' || r.status?.nivel === 'prejuizo').length;
        return send(res, 200, {
          success: true,
          produtos: resultados,
          resumo: {
            total: resultados.length, saudaveis, apertados, criticos,
            alertaGeral: criticos > 0
              ? `🚨 ${criticos} produtos com margem crítica! Revise antes de publicar.`
              : apertados > 0
                ? `🟡 ${apertados} produtos com margem apertada. Considere criar kits.`
                : `🟢 Todos os ${saudaveis} produtos com margem saudável!`,
          },
        });
      } catch(err) { return send(res, 200, { success:false, error: err.message }); }
    }

    // ============================================================
    // CONFIG & WEBHOOKS — painel unificado de setup do sistema
    // ============================================================

    // POST /api/webhooks/configurar — tenta registrar callback+topics via API ML
    // Se a app ML não permite edição via API (comum), retorna instruções manuais.
    if (u.pathname === '/api/webhooks/configurar' && req.method === 'POST') {
      try {
        const tokens = loadTokens();
        const token = tokens.ml_access_token;
        if (!token) return send(res, 200, { success:false, error:'Token ML não disponível' });

        const appId = process.env.ML_CLIENT_ID || loadEnv().ML_CLIENT_ID || '3688973136843575';
        const callbackUrl = 'https://agentemarkt.com/webhooks';
        const topics = ['items', 'questions', 'orders_v2', 'payments', 'shipments'];

        const configResp = await mlFetch(`https://api.mercadolibre.com/applications/${appId}`, {
          method: 'PUT',
          headers: {
            'Authorization': 'Bearer ' + token,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            notifications_callback_url: callbackUrl,
            notifications_topics: topics,
          }),
        });
        const data = await configResp.json().catch(() => ({}));

        if (configResp.ok) {
          console.log('🔔 [webhooks] Configurados com sucesso via API!');
          return send(res, 200, { success:true, message:'Webhooks configurados!', callback:callbackUrl, topics, data });
        }
        return send(res, 200, {
          success: false,
          error: data.message || 'Não foi possível configurar via API (normal — muitos apps ML não permitem)',
          instrucoes: {
            passo1: 'Acesse https://developers.mercadolivre.com.br/devcenter',
            passo2: `Edite o app ${appId}`,
            passo3: `Callback URL: ${callbackUrl}`,
            passo4: `Topics: ${topics.join(', ')}`,
            passo5: 'Salve',
          },
          details: data,
        });
      } catch (error) {
        return send(res, 200, { success:false, error: error.message });
      }
    }

    // POST /api/config/anthropic-key — salva ANTHROPIC_API_KEY no .env (requer X-Admin-Secret)
    if (u.pathname === '/api/config/anthropic-key' && req.method === 'POST') {
      try {
        const adminSecret = process.env.ADMIN_SECRET || loadEnv().ADMIN_SECRET || 'agente-marketplace-2026';
        const providedSecret = req.headers['x-admin-secret'] || req.headers['X-Admin-Secret'];
        if (providedSecret !== adminSecret) {
          return send(res, 403, { success:false, error:'Acesso negado — X-Admin-Secret inválido ou ausente' });
        }

        const body = await readBody(req).catch(() => ({}));
        const apiKey = (body && body.apiKey) || '';
        if (!apiKey || !apiKey.startsWith('sk-ant-')) {
          return send(res, 200, { success:false, error:'API key inválida — deve começar com sk-ant-' });
        }

        saveEnv({ ANTHROPIC_API_KEY: apiKey });
        process.env.ANTHROPIC_API_KEY = apiKey;
        console.log('🧠 [config] ANTHROPIC_API_KEY configurada com sucesso!');
        return send(res, 200, { success:true, message:'API key configurada! IA generativa ativada.' });
      } catch (error) {
        return send(res, 200, { success:false, error: error.message });
      }
    }

    // GET /api/config/status — resumo de todas as integrações (pro painel Setup)
    if (u.pathname === '/api/config/status' && req.method === 'GET') {
      try {
        const toks = loadTokens();
        const currentEnv = loadEnv();
        const mlExpiresAt = toks.ml_token_expires_at ? new Date(toks.ml_token_expires_at) : null;
        const mlValid = !!(toks.ml_access_token && mlExpiresAt && mlExpiresAt > new Date());

        return send(res, 200, {
          success: true,
          ml: {
            connected:   !!toks.ml_access_token,
            tokenValido: mlValid,
            expires_at:  toks.ml_token_expires_at || null,
            user_id:     toks.ml_user_id || null,
            auto_refresh: !!toks.ml_refresh_token,
          },
          bling: {
            configured: !!(process.env.BLING_CLIENT_ID || currentEnv.BLING_CLIENT_ID),
            connected:  !!toks.bling_access_token,
          },
          ia: {
            configured: !!(process.env.ANTHROPIC_API_KEY || currentEnv.ANTHROPIC_API_KEY),
            modelo: (process.env.ANTHROPIC_API_KEY || currentEnv.ANTHROPIC_API_KEY) ? 'claude-sonnet-4-20250514' : null,
          },
          webhooks: {
            urlConfigurada: 'https://agentemarkt.com/webhooks',
            recebendo: (global.webhookStats?.total || 0) > 0,
            total: global.webhookStats?.total || 0,
          },
          cache: mlCache.stats(),
          rateLimiter: mlRateLimiter.status(),
          proxy: {
            ativo:  ML_USE_PROXY,
            url:    ML_PROXY_URL || null,
            secret: ML_USE_PROXY ? '***configurado***' : null,
          },
          admin: {
            // nunca expõe o secret — só indica se o default tá sendo usado
            usando_secret_default: !process.env.ADMIN_SECRET && !currentEnv.ADMIN_SECRET,
          },
        });
      } catch (error) {
        return send(res, 200, { success:false, error: error.message });
      }
    }

    // ============================================================
    // MODO DEMO — dados simulados realistas pra demonstração
    // Ativado quando: (a) usuário liga toggle no header
    //                 (b) ML rate-limita o VPS (429)
    // ============================================================
    if ((u.pathname === '/api/demo/dados' || u.pathname === '/api/ml/dashboard-demo') && req.method === 'GET') {
      const anunciosDemo = [
        { id:'MLB001', titulo:'Pastilha de Freio Dianteira Gol G5 G6 Frasle',   preco:87.50,  vendas:23, visitas:412, ctr:5.6, estoque:45, status:'active', classe:'top',   perfScore:92 },
        { id:'MLB002', titulo:'Amortecedor Traseiro Honda Civic Monroe',        preco:289.90, vendas:15, visitas:287, ctr:5.2, estoque:18, status:'active', classe:'top',   perfScore:88 },
        { id:'MLB003', titulo:'Disco de Freio Dianteiro Toyota Corolla Fremax', preco:212.50, vendas:12, visitas:198, ctr:6.1, estoque:25, status:'active', classe:'top',   perfScore:85 },
        { id:'MLB004', titulo:'Filtro de Óleo Hyundai HB20 Tecfil',             preco:42.90,  vendas:31, visitas:523, ctr:5.9, estoque:89, status:'active', classe:'top',   perfScore:95 },
        { id:'MLB005', titulo:'Jogo Velas Ignição Onix Prisma NGK Iridium',     preco:162.50, vendas:8,  visitas:156, ctr:5.1, estoque:34, status:'active', classe:'top',   perfScore:78 },
        { id:'MLB006', titulo:'Kit Embreagem Palio Siena 1.0 LUK',              preco:459.90, vendas:5,  visitas:89,  ctr:5.6, estoque:12, status:'active', classe:'medio', perfScore:62 },
        { id:'MLB007', titulo:'Bomba Água Celta Corsa 1.0 Urba',                preco:78.90,  vendas:4,  visitas:112, ctr:3.6, estoque:22, status:'active', classe:'medio', perfScore:55 },
        { id:'MLB008', titulo:'Correia Dentada Fiat Uno Fire Gates',            preco:34.90,  vendas:3,  visitas:87,  ctr:3.4, estoque:56, status:'active', classe:'medio', perfScore:48 },
        { id:'MLB009', titulo:'Pivô Suspensão Dianteira Civic Viemar',          preco:124.90, vendas:2,  visitas:67,  ctr:3.0, estoque:15, status:'active', classe:'medio', perfScore:42 },
        { id:'MLB010', titulo:'Farol Direito HB20 2016-2019 Arteb',             preco:389.90, vendas:0,  visitas:23,  ctr:0,   estoque:8,  status:'active', classe:'ruim',  perfScore:18, diasSemVenda:18 },
      ];
      const agora = Date.now();
      const pedidosDemo = [
        { id:2000008779, status:'paid',      valorTotal:289.90, dataCriacao:new Date(agora - 2*3600000).toISOString(),  comprador:{ nickname:'JOAO***' },   itens:[{ titulo:'Amortecedor Traseiro Honda Civic Monroe', quantidade:1, precoUnitario:289.90 }], envio:{ id:'SHP001' }, fraude:false },
        { id:2000008780, status:'shipped',   valorTotal:175.00, dataCriacao:new Date(agora - 24*3600000).toISOString(), comprador:{ nickname:'MARIA***' },  itens:[{ titulo:'Pastilha de Freio Dianteira Gol G5 G6 Frasle', quantidade:2, precoUnitario:87.50 }], envio:{ id:'SHP002', rastreamento:'BR987654321' }, fraude:false },
        { id:2000008781, status:'delivered', valorTotal:212.50, dataCriacao:new Date(agora - 72*3600000).toISOString(), comprador:{ nickname:'PEDRO***' },  itens:[{ titulo:'Disco de Freio Dianteiro Toyota Corolla Fremax', quantidade:1, precoUnitario:212.50 }], envio:{ id:'SHP003' }, fraude:false },
        { id:2000008782, status:'paid',      valorTotal:459.90, dataCriacao:new Date(agora - 1*3600000).toISOString(),  comprador:{ nickname:'ANA***' },    itens:[{ titulo:'Kit Embreagem Palio Siena 1.0 LUK', quantidade:1, precoUnitario:459.90 }], envio:{ id:'SHP004' }, fraude:false },
        { id:2000008783, status:'paid',      valorTotal:42.90,  dataCriacao:new Date(agora - 0.5*3600000).toISOString(),comprador:{ nickname:'CARLOS***' }, itens:[{ titulo:'Filtro de Óleo Hyundai HB20 Tecfil', quantidade:1, precoUnitario:42.90 }], envio:{ id:'SHP005' }, fraude:false },
      ];
      const dadosDemo = {
        dashboard: {
          produtos: 47, anunciosAtivos: 32, vendas30d: 156, receita30d: 28750.00,
          publicadosHoje: 3, limiteDiario: 15, vendasHoje: 8, receitaHoje: 1890.00, ticketMedio: 184.29,
        },
        vendasDiarias: Array.from({length:30}, (_, i) => ({
          dia: `D${i+1}`,
          vendas:  Math.floor(Math.random() * 8) + 1,
          receita: Math.floor(Math.random() * 2000) + 500,
        })),
        ctrDiario: Array.from({length:30}, (_, i) => ({
          dia: `D${i+1}`,
          ctr: parseFloat((Math.random() * 3 + 0.5).toFixed(1)),
        })),
        classificacao: { top: 7, medio: 12, ruim: 8, novo: 5 },
        anuncios: anunciosDemo,
        pedidos: pedidosDemo,
        sac: {
          pendentes: 2, respondidas: 48,
          perguntas: [
            { id:'Q001', texto:'Serve no Gol G5 2012 1.0?',        itemId:'MLB001', data:new Date(agora - 300000).toISOString(), status:'UNANSWERED' },
            { id:'Q002', texto:'Tem disponível pra entrega imediata?', itemId:'MLB002', data:new Date(agora - 600000).toISOString(), status:'UNANSWERED' },
          ],
        },
        estoque: {
          resumo: { ativos: 28, pausados: 2, semEstoque: 2, estoqueBaixo: 3, alerta: '⚠️ 3 anúncios com estoque baixo' },
        },
        webhooks: {
          total: 347,
          porTopic: { questions: 98, orders_v2: 156, items: 52, payments: 23, shipments: 18 },
          ultimoRecebido: new Date(agora - 180000).toISOString(),
        },
        reputacao: {
          level: 'gold', powerSeller: 'platinum',
          positivas: 98.5, neutras: 1.0, negativas: 0.5, totalVendas: 1247,
        },
        mensagens: [
          { orderId:2000008780, tipo:'venda_confirmada', comprador:'MARIA***',  item:'Pastilha Freio', enviada:new Date(agora - 24*3600000).toISOString(), sucesso:true },
          { orderId:2000008781, tipo:'produto_enviado',  comprador:'PEDRO***',  item:'Disco Freio',    enviada:new Date(agora - 48*3600000).toISOString(), sucesso:true },
          { orderId:2000008781, tipo:'produto_entregue', comprador:'PEDRO***',  item:'Disco Freio',    enviada:new Date(agora - 24*3600000).toISOString(), sucesso:true },
        ],
      };

      if (u.pathname === '/api/demo/dados') {
        return send(res, 200, { success:true, demo:true, ...dadosDemo });
      }

      // dashboard-demo: mesmo formato do /api/ml/dashboard real, pro front trocar sem if/else
      return send(res, 200, {
        success: true,
        demo: true,
        connected: true,
        user: {
          id: 2947005156,
          nickname: 'AGENTE_MARKETPLACE',
          seller_reputation: {
            level_id: dadosDemo.reputacao.level,
            power_seller_status: dadosDemo.reputacao.powerSeller,
            transactions: {
              total: dadosDemo.reputacao.totalVendas,
              ratings: {
                positive: dadosDemo.reputacao.positivas / 100,
                neutral:  dadosDemo.reputacao.neutras   / 100,
                negative: dadosDemo.reputacao.negativas / 100,
              },
            },
          },
        },
        items:  { total: dadosDemo.dashboard.anunciosAtivos, results: dadosDemo.anuncios.map(a => a.id) },
        orders: { total: dadosDemo.dashboard.vendas30d,      results: dadosDemo.pedidos },
        metrics: dadosDemo.dashboard,
      });
    }

    // ============================================================
    // AJUSTE AUTOMÁTICO DE PREÇO — concorrência, ajuste manual e em lote
    // ============================================================

    // GET /api/ml/preco/concorrencia/:itemId — top 5 concorrentes + análise de posição
    if (u.pathname.startsWith('/api/ml/preco/concorrencia/') && req.method === 'GET') {
      const token = getBearer();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      try {
        const itemId = u.pathname.split('/').pop();
        if (!itemId) return send(res, 400, { success:false, error:'itemId obrigatório' });

        const itemResp = await mlFetch(`https://api.mercadolibre.com/items/${itemId}`, {
          headers: { 'Authorization': 'Bearer ' + token }
        });
        const item = await itemResp.json();
        if (!itemResp.ok || !item.id) {
          return send(res, 200, { success:false, error: item.message || 'Anúncio não encontrado' });
        }

        const query = encodeURIComponent(String(item.title || '').substring(0, 40));
        const searchResp = await mlFetch(
          `https://api.mercadolibre.com/sites/MLB/search?q=${query}&category=${item.category_id || ''}&sort=price_asc&limit=10`,
          { headers: { 'Authorization': 'Bearer ' + token } }
        );
        const searchData = await searchResp.json();

        const meResp = await mlFetch('https://api.mercadolibre.com/users/me', {
          headers: { 'Authorization': 'Bearer ' + token }
        });
        const me = await meResp.json();

        const concorrentes = (searchData.results || [])
          .filter(r => r.seller?.id !== me.id)
          .slice(0, 5)
          .map(r => ({
            id: r.id,
            titulo: r.title,
            preco: r.price,
            vendedor: r.seller?.nickname,
            vendas: r.sold_quantity || 0,
            frete_gratis: r.shipping?.free_shipping || false,
            tipo_listagem: r.listing_type_id,
          }));

        const menorPreco = concorrentes.length > 0 ? Math.min(...concorrentes.map(c => c.price)) : null;
        const precoMedio = concorrentes.length > 0 ? concorrentes.reduce((s, c) => s + c.price, 0) / concorrentes.length : null;

        let posicao = 'desconhecida';
        let sugestao = null;
        if (menorPreco && item.price) {
          if (item.price <= menorPreco) {
            posicao = '🟢 Mais barato';
            sugestao = 'Você já tem o melhor preço! Considere subir um pouco pra aumentar margem.';
          } else if (item.price <= menorPreco * 1.1) {
            posicao = '🟡 Competitivo';
            sugestao = `Você está ${((item.price / menorPreco - 1) * 100).toFixed(0)}% acima do mais barato (R$ ${menorPreco.toFixed(2)}). Está bom!`;
          } else {
            posicao = '🔴 Caro';
            sugestao = `Você está ${((item.price / menorPreco - 1) * 100).toFixed(0)}% acima do mais barato (R$ ${menorPreco.toFixed(2)}). Considere baixar.`;
          }
        }

        return send(res, 200, {
          success: true,
          meuAnuncio: {
            id: item.id,
            titulo: item.title,
            preco: item.price,
            categoria: item.category_id,
          },
          concorrentes,
          analise: {
            menorPreco,
            precoMedio: precoMedio ? parseFloat(precoMedio.toFixed(2)) : null,
            meuPreco: item.price,
            posicao,
            sugestao,
            diferencaParaMaisBarato: menorPreco ? parseFloat((item.price - menorPreco).toFixed(2)) : null,
          },
        });
      } catch (error) {
        return send(res, 200, { success:false, error: error.message });
      }
    }

    // PUT /api/ml/preco/ajustar/:itemId — altera preço e registra no histórico
    if (u.pathname.startsWith('/api/ml/preco/ajustar/') && req.method === 'PUT') {
      const token = getBearer();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      try {
        const itemId = u.pathname.split('/').pop();
        if (!itemId) return send(res, 400, { success:false, error:'itemId obrigatório' });
        const body = await readBody(req).catch(() => ({}));
        const novoPreco = Number(body.novoPreco);
        const motivo = body.motivo || 'ajuste manual';

        if (!novoPreco || novoPreco <= 0) {
          return send(res, 200, { success:false, error:'Preço inválido' });
        }

        const itemResp = await mlFetch(
          `https://api.mercadolibre.com/items/${itemId}?attributes=id,title,price`,
          { headers: { 'Authorization': 'Bearer ' + token } }
        );
        const item = await itemResp.json();
        if (!itemResp.ok || !item.id) {
          return send(res, 200, { success:false, error: item.message || 'Anúncio não encontrado' });
        }
        const precoAnterior = item.price;

        const updateResp = await mlFetch(`https://api.mercadolibre.com/items/${itemId}`, {
          method: 'PUT',
          headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
          body: JSON.stringify({ price: novoPreco }),
        });
        const updateData = await updateResp.json().catch(() => ({}));

        if (updateResp.ok) {
          console.log(`💰 [preço] ${itemId}: R$ ${precoAnterior} → R$ ${novoPreco} (${motivo})`);
          global.ajustesPreco.unshift({
            itemId,
            titulo: item.title,
            precoAnterior,
            novoPreco,
            motivo,
            data: new Date().toISOString(),
          });
          if (global.ajustesPreco.length > 50) global.ajustesPreco = global.ajustesPreco.slice(0, 50);
          return send(res, 200, { success:true, precoAnterior, novoPreco, motivo });
        } else {
          return send(res, 200, { success:false, error: updateData.message || 'Erro ao ajustar preço', details: updateData });
        }
      } catch (error) {
        return send(res, 200, { success:false, error: error.message });
      }
    }

    // POST /api/ml/preco/ajuste-automatico — analisa/ajusta em lote (modo teste default)
    if (u.pathname === '/api/ml/preco/ajuste-automatico' && req.method === 'POST') {
      const token = getBearer();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      try {
        const body = await readBody(req).catch(() => ({}));
        const percentualAbaixo = Number.isFinite(+body.percentualAbaixo) ? +body.percentualAbaixo : 5;
        const margemMinima = Number.isFinite(+body.margemMinima) ? +body.margemMinima : 15;
        const modoTeste = body.modoTeste !== false; // default true

        const meResp = await mlFetch('https://api.mercadolibre.com/users/me', {
          headers: { 'Authorization': 'Bearer ' + token }
        });
        const me = await meResp.json();
        if (!meResp.ok || !me.id) return send(res, 200, { success:false, error:'Token inválido' });

        const itemsResp = await mlFetch(
          `https://api.mercadolibre.com/users/${me.id}/items/search?status=active&limit=20`,
          { headers: { 'Authorization': 'Bearer ' + token } }
        );
        const itemsData = await itemsResp.json();
        const itemIds = itemsData.results || [];

        const resultados = [];
        const base = `http://127.0.0.1:${PORT}`;

        for (const itemId of itemIds.slice(0, 10)) {
          try {
            const concResp = await fetch(`${base}/api/ml/preco/concorrencia/${itemId}`, {
              headers: { 'Authorization': 'Bearer ' + token }
            });
            const concData = await concResp.json();

            if (!concData.success || !concData.analise?.menorPreco) {
              resultados.push({ itemId, acao: '⚪ Sem concorrentes encontrados' });
              await new Promise(r => setTimeout(r, 1000));
              continue;
            }

            const menorConc = concData.analise.menorPreco;
            const meuPreco  = concData.analise.meuPreco;
            const precoIdeal = parseFloat((menorConc * (1 - percentualAbaixo / 100)).toFixed(2));

            // Estimativa simples: custo = 40% do preço atual
            const custoEstimado = meuPreco * 0.4;
            const margemNova = ((precoIdeal - custoEstimado) / precoIdeal) * 100;

            if (margemNova < margemMinima) {
              resultados.push({
                itemId, titulo: concData.meuAnuncio.titulo,
                meuPreco, menorConc, precoIdeal,
                acao: `🔴 Margem insuficiente (${margemNova.toFixed(0)}% < ${margemMinima}%) — não ajustar`
              });
              await new Promise(r => setTimeout(r, 1000));
              continue;
            }

            if (meuPreco <= precoIdeal) {
              resultados.push({
                itemId, titulo: concData.meuAnuncio.titulo,
                meuPreco, menorConc,
                acao: '🟢 Já está competitivo — sem ajuste necessário'
              });
              await new Promise(r => setTimeout(r, 1000));
              continue;
            }

            if (modoTeste) {
              resultados.push({
                itemId, titulo: concData.meuAnuncio.titulo,
                meuPreco, menorConc, precoIdeal,
                acao: `🟡 Ajustaria: R$ ${meuPreco} → R$ ${precoIdeal} (-${percentualAbaixo}% do concorrente)`,
                modoTeste: true
              });
            } else {
              const ajResp = await fetch(`${base}/api/ml/preco/ajustar/${itemId}`, {
                method: 'PUT',
                headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
                body: JSON.stringify({ novoPreco: precoIdeal, motivo: `auto: -${percentualAbaixo}% do concorrente` })
              });
              const ajData = await ajResp.json();
              resultados.push({
                itemId, titulo: concData.meuAnuncio.titulo,
                precoAnterior: meuPreco, novoPreco: precoIdeal,
                acao: ajData.success ? `✅ Ajustado: R$ ${meuPreco} → R$ ${precoIdeal}` : '❌ Erro ao ajustar'
              });
            }

            await new Promise(r => setTimeout(r, 1000));
          } catch(e) {
            resultados.push({ itemId, acao: '❌ Erro', error: e.message });
          }
        }

        return send(res, 200, {
          success: true,
          modoTeste,
          config: { percentualAbaixo, margemMinima },
          total: resultados.length,
          ajustados: resultados.filter(r => r.acao?.includes('Ajustado')).length,
          resultados
        });
      } catch (error) {
        return send(res, 200, { success:false, error: error.message });
      }
    }

    // GET /api/ml/preco/historico — últimos 50 ajustes
    if (u.pathname === '/api/ml/preco/historico' && req.method === 'GET') {
      return send(res, 200, {
        success: true,
        ajustes: global.ajustesPreco || [],
        total: (global.ajustesPreco || []).length,
      });
    }

    // ============================================================
    // MENSAGENS PÓS-VENDA — envio, histórico e configuração
    // ============================================================

    // POST /api/ml/mensagem/enviar — envia mensagem pós-venda (venda_confirmada | produto_enviado | produto_entregue)
    if (u.pathname === '/api/ml/mensagem/enviar' && req.method === 'POST') {
      const token = getBearer();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      try {
        const { orderId, tipo } = await readBody(req).catch(() => ({}));
        if (!orderId || !tipo) return send(res, 200, { success:false, error:'orderId e tipo são obrigatórios' });

        const template = mensagensPosVenda[tipo];
        if (!template) return send(res, 200, { success:false, error:'Tipo inválido' });
        // Template editável tem precedência — usa o do global.msgTemplates se existir
        const templateEditavel = global.msgTemplates && global.msgTemplates[tipo];

        const orderResp = await mlFetch(`https://api.mercadolibre.com/orders/${orderId}`, {
          headers: { 'Authorization': 'Bearer ' + token }
        });
        const order = await orderResp.json();
        if (!orderResp.ok || !order.id) {
          return send(res, 200, { success:false, error: order.message || 'Pedido não encontrado' });
        }

        const meResp = await mlFetch('https://api.mercadolibre.com/users/me', {
          headers: { 'Authorization': 'Bearer ' + token }
        });
        const me = await meResp.json();
        if (!meResp.ok || !me.id) {
          return send(res, 200, { success:false, error:'Token inválido' });
        }

        const comprador = order.buyer?.first_name || order.buyer?.nickname || 'Cliente';
        const item = order.order_items?.[0]?.item?.title || 'seu produto';
        const packId = order.pack_id || order.id;

        // Rastreio só para produto_enviado
        let rastreio = '';
        if (tipo === 'produto_enviado' && order.shipping?.id) {
          try {
            const shipResp = await mlFetch(`https://api.mercadolibre.com/shipments/${order.shipping.id}`, {
              headers: { 'Authorization': 'Bearer ' + token, 'x-format-new': 'true' }
            });
            const ship = await shipResp.json();
            rastreio = ship.tracking_number || '';
          } catch(e) {}
        }

        // Se tem template editável, substitui placeholders; senão usa função hardcoded
        let texto = templateEditavel
          ? String(templateEditavel)
              .replace(/\{comprador\}/g, comprador)
              .replace(/\{item\}/g, item)
              .replace(/\{rastreio\}/g, rastreio ? '📍 Rastreamento: ' + rastreio + '\n\n' : '')
          : template(comprador, item, rastreio);

        // Compliance: sanitiza antes de mandar pro ML
        const complMsg = validarRespostaML(texto);
        texto = complMsg.texto;
        if (complMsg.violacoes.length > 0) {
          console.log(`🛡️ [compliance] /mensagem/enviar sanitizada: ${complMsg.violacoes.join(', ')}`);
        }

        const msgResp = await mlFetch(
          `https://api.mercadolibre.com/messages/packs/${packId}/sellers/${me.id}`,
          {
            method: 'POST',
            headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
            body: JSON.stringify({ text: texto })
          }
        );
        const msgData = await msgResp.json().catch(() => ({}));

        if (msgResp.ok) {
          console.log(`✉️ [pós-venda] ${tipo}: pedido ${orderId} → ${comprador} ✅`);
          global.mensagensPosVenda.unshift({
            orderId, tipo, comprador, item,
            enviada: new Date().toISOString(),
            sucesso: true,
          });
          if (global.mensagensPosVenda.length > 50) {
            global.mensagensPosVenda = global.mensagensPosVenda.slice(0, 50);
          }
          return send(res, 200, { success:true, mensagem: texto, tipo, orderId });
        } else {
          console.error(`✉️ [pós-venda] ❌ ${tipo}: ${msgData.message || JSON.stringify(msgData)}`);
          global.mensagensPosVenda.unshift({
            orderId, tipo, comprador, item,
            enviada: new Date().toISOString(),
            sucesso: false,
            erro: msgData.message || 'Erro ao enviar',
          });
          if (global.mensagensPosVenda.length > 50) {
            global.mensagensPosVenda = global.mensagensPosVenda.slice(0, 50);
          }
          return send(res, 200, { success:false, error: msgData.message || 'Erro ao enviar', details: msgData });
        }
      } catch (error) {
        return send(res, 200, { success:false, error: error.message });
      }
    }

    // GET /api/ml/mensagem/historico — últimas 50 mensagens enviadas
    if (u.pathname === '/api/ml/mensagem/historico' && req.method === 'GET') {
      return send(res, 200, {
        success: true,
        mensagens: global.mensagensPosVenda || [],
        total: (global.mensagensPosVenda || []).length,
      });
    }

    // GET /api/ml/mensagem/config — status das mensagens automáticas
    if (u.pathname === '/api/ml/mensagem/config' && req.method === 'GET') {
      return send(res, 200, {
        success: true,
        autoEnviar: {
          venda_confirmada: global.msgAutoConfig?.venda_confirmada ?? true,
          produto_enviado:  global.msgAutoConfig?.produto_enviado  ?? true,
          produto_entregue: global.msgAutoConfig?.produto_entregue ?? true,
        },
        templates: Object.keys(mensagensPosVenda),
      });
    }

    // POST /api/ml/mensagem/config — atualiza flags de auto-envio
    if (u.pathname === '/api/ml/mensagem/config' && req.method === 'POST') {
      try {
        const body = await readBody(req).catch(() => ({}));
        if (!global.msgAutoConfig) global.msgAutoConfig = {};
        if (body.venda_confirmada !== undefined) global.msgAutoConfig.venda_confirmada = !!body.venda_confirmada;
        if (body.produto_enviado  !== undefined) global.msgAutoConfig.produto_enviado  = !!body.produto_enviado;
        if (body.produto_entregue !== undefined) global.msgAutoConfig.produto_entregue = !!body.produto_entregue;
        return send(res, 200, { success:true, config: global.msgAutoConfig });
      } catch(e) {
        return send(res, 200, { success:false, error: e.message });
      }
    }

    // ============================================================
    // GAP 1 — ATRIBUTOS OBRIGATÓRIOS DA CATEGORIA ML
    // ============================================================
    if (u.pathname.startsWith('/api/ml/categoria/') && u.pathname.endsWith('/atributos') && req.method === 'GET') {
      const token = getBearer();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      try {
        const categoryId = u.pathname.split('/')[4];
        const catResp = await mlFetch(
          `https://api.mercadolibre.com/categories/${categoryId}/attributes`,
          { headers: { 'Authorization': 'Bearer ' + token } }
        );
        if (!catResp.ok) {
          return send(res, 200, { success:false, error:'Categoria não encontrada' });
        }
        const attrs = await catResp.json().catch(() => []);
        const obrigatorios = (Array.isArray(attrs) ? attrs : []).filter(a => a.tags?.required || a.tags?.catalog_required);
        const recomendados = (Array.isArray(attrs) ? attrs : []).filter(a => !a.tags?.required && !a.tags?.catalog_required && a.tags?.recommended);
        const opcionais    = (Array.isArray(attrs) ? attrs : []).filter(a => !a.tags?.required && !a.tags?.catalog_required && !a.tags?.recommended);

        return send(res, 200, {
          success: true,
          categoryId,
          total: Array.isArray(attrs) ? attrs.length : 0,
          obrigatorios: obrigatorios.map(a => ({
            id: a.id,
            nome: a.name,
            tipo: a.value_type,
            valores: a.values?.slice(0, 20)?.map(v => ({ id: v.id, nome: v.name })) || [],
            unidade: a.units?.map(u => u.id) || null,
          })),
          recomendados: recomendados.map(a => ({ id: a.id, nome: a.name, tipo: a.value_type })),
          totalObrigatorios: obrigatorios.length,
          totalRecomendados: recomendados.length,
          totalOpcionais:    opcionais.length,
        });
      } catch (error) {
        registrarErro('publicacao', 'Falha buscando atributos da categoria', { error: error.message });
        return send(res, 200, { success:false, error: error.message });
      }
    }

    // ============================================================
    // GAP 3 — EDIÇÃO EM MASSA (preço / estoque / título / status)
    // ============================================================
    if (u.pathname === '/api/ml/anuncios/editar-massa' && req.method === 'PUT') {
      const token = getBearer();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      try {
        const body = await readBody(req).catch(() => ({}));
        const alteracoes = Array.isArray(body.alteracoes) ? body.alteracoes : [];
        if (alteracoes.length === 0) {
          return send(res, 200, { success:false, error:'Nenhuma alteração informada' });
        }

        const resultados = [];
        for (const alt of alteracoes.slice(0, 50)) {
          try {
            const mlBody = {};
            if (alt.preco   != null)   mlBody.price              = Number(alt.preco);
            if (alt.estoque !== undefined) mlBody.available_quantity = Number(alt.estoque);
            if (alt.titulo) {
              // Compliance: sanitiza título antes de aplicar no ML
              const tc = validarTituloML(String(alt.titulo));
              if (!tc.valido) console.log(`🛡️ [compliance] editar-massa ${alt.itemId}: removidos ${tc.termosRemovidos.join(', ')}`);
              mlBody.title = tc.titulo.substring(0, 60);
            }
            if (alt.status)            mlBody.status             = alt.status;

            const updateResp = await mlFetch(`https://api.mercadolibre.com/items/${alt.itemId}`, {
              method: 'PUT',
              headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
              body: JSON.stringify(mlBody),
            });
            const upData = await updateResp.json().catch(() => ({}));
            resultados.push({
              itemId: alt.itemId,
              success: updateResp.ok,
              alteracoes: Object.keys(mlBody),
              error: updateResp.ok ? null : (upData.message || 'erro'),
            });
            if (!updateResp.ok) registrarErro('publicacao', `Falha edição massa ${alt.itemId}`, upData);
            await new Promise(r => setTimeout(r, 500));
          } catch (e) {
            resultados.push({ itemId: alt.itemId, success:false, error: e.message });
            registrarErro('publicacao', `Exceção edição massa ${alt.itemId}`, { error: e.message });
          }
        }

        console.log(`📝 [massa] ${resultados.filter(r => r.success).length}/${resultados.length} alterados`);
        return send(res, 200, {
          success: true,
          total:   resultados.length,
          sucesso: resultados.filter(r => r.success).length,
          falhas:  resultados.filter(r => !r.success).length,
          resultados,
        });
      } catch (error) {
        return send(res, 200, { success:false, error: error.message });
      }
    }

    // ============================================================
    // GAP 4 — ETIQUETAS EM LOTE (PDF único com todos os envios pendentes)
    // ============================================================
    if (u.pathname === '/api/ml/envios/etiquetas-lote' && req.method === 'GET') {
      const token = getBearer();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      try {
        const base = `http://127.0.0.1:${PORT}`;
        const pedidosResp = await fetch(`${base}/api/ml/pedidos?status=paid`, {
          headers: { 'Authorization': 'Bearer ' + token },
        });
        const pedidosData = await pedidosResp.json().catch(() => ({}));
        const shipmentIds = (pedidosData.pedidos || [])
          .filter(p => p.envio?.id)
          .map(p => p.envio.id);

        if (shipmentIds.length === 0) {
          return send(res, 200, { success:false, error:'Nenhum envio pendente pra gerar etiqueta' });
        }

        const ids = shipmentIds.join(',');
        const labelResp = await mlFetch(
          `https://api.mercadolibre.com/shipment_labels?shipment_ids=${ids}&response_type=pdf`,
          { headers: { 'Authorization': 'Bearer ' + token } }
        );
        if (labelResp.ok) {
          // _safeFetch retorna fakeResponse — pego o raw via text()+base64? Não dá.
          // Melhor: bypass do _safeFetch fazendo fetch direto (só aqui, pra PDF binário)
          const rawResp = await fetch(
            `https://api.mercadolibre.com/shipment_labels?shipment_ids=${ids}&response_type=pdf`,
            { headers: { 'Authorization': 'Bearer ' + token } }
          );
          if (!rawResp.ok) {
            return send(res, 200, { success:false, error:'Erro ao gerar etiquetas em lote' });
          }
          const buffer = await rawResp.arrayBuffer();
          res.writeHead(200, {
            'Content-Type':        'application/pdf',
            'Content-Disposition': 'inline; filename="etiquetas-lote.pdf"',
            'Access-Control-Allow-Origin': '*',
          });
          console.log(`🏷️ [etiquetas] Lote gerado: ${shipmentIds.length} etiquetas`);
          return res.end(Buffer.from(buffer));
        }
        return send(res, 200, { success:false, error:'Erro ao gerar etiquetas em lote' });
      } catch (error) {
        registrarErro('pedido', 'Falha etiquetas em lote', { error: error.message });
        return send(res, 200, { success:false, error: error.message });
      }
    }

    // ============================================================
    // GAP 5 — PREÇO POR TIPO DE LISTAGEM (Clássico vs Premium)
    // ============================================================
    if (u.pathname === '/api/ml/preco/calcular-por-listagem' && req.method === 'POST') {
      try {
        const body = await readBody(req).catch(() => ({}));
        const custo = Number(body.custo) || 0;
        const margemDesejada = Number(body.margemDesejada) || 20;
        if (custo <= 0) return send(res, 200, { success:false, error:'custo inválido' });

        const comissoes = {
          classico: { taxa: 0.13, nome: 'Clássico', frete_gratis: false },
          premium:  { taxa: 0.18, nome: 'Premium',  frete_gratis: true  },
        };
        const resultados = {};
        for (const [tipo, config] of Object.entries(comissoes)) {
          const denominador = 1 - config.taxa - (margemDesejada / 100);
          if (denominador <= 0) continue;
          const precoVenda = custo / denominador;
          const comissaoReais = precoVenda * config.taxa;
          const lucro = precoVenda - custo - comissaoReais;
          const margemReal = (lucro / precoVenda) * 100;
          resultados[tipo] = {
            nome: config.nome,
            precoVenda:      parseFloat(precoVenda.toFixed(2)),
            comissao:        parseFloat(comissaoReais.toFixed(2)),
            comissaoPercent: (config.taxa * 100).toFixed(1) + '%',
            lucro:           parseFloat(lucro.toFixed(2)),
            margemReal:      parseFloat(margemReal.toFixed(1)),
            freteGratis:     config.frete_gratis,
          };
        }
        const c = resultados.classico, p = resultados.premium;
        return send(res, 200, {
          success: true,
          custo, margemDesejada,
          classico: c, premium: p,
          diferencaPreco: p && c ? parseFloat((p.precoVenda - c.precoVenda).toFixed(2)) : null,
          recomendacao: p && p.precoVenda >= 79
            ? '✅ Premium recomendado (frete grátis ativo)'
            : '⚠️ Clássico pode ser melhor (produto barato)',
        });
      } catch (error) {
        return send(res, 200, { success:false, error: error.message });
      }
    }

    // ============================================================
    // GAP 6 — TEMPLATES EDITÁVEIS (GET lista · PUT salva)
    // ============================================================
    if (u.pathname === '/api/ml/mensagem/templates' && req.method === 'GET') {
      // Defaults a partir das funções hardcoded (dá pro usuário ver/editar)
      if (!global.msgTemplates) {
        global.msgTemplates = {
          venda_confirmada: 'Olá {comprador}! 😊\n\nObrigado pela sua compra de "{item}"!\n\nEstamos preparando seu pedido com todo cuidado. Enviaremos em até 24h úteis.\n\nQualquer dúvida, estamos à disposição!\n\nEquipe Agente Marketplace',
          produto_enviado:  'Olá {comprador}! 📦\n\nSeu pedido "{item}" acabou de ser ENVIADO!\n\n{rastreio}Acompanhe a entrega pelo app do Mercado Livre.\n\nBoa compra!\nEquipe Agente Marketplace',
          produto_entregue: 'Olá {comprador}! 🎉\n\nSeu pedido "{item}" foi entregue!\n\nEsperamos que esteja tudo perfeito! Se puder, avalie sua compra — sua opinião é muito importante pra nós.\n\n⭐ Avalie pelo app do Mercado Livre\n\nObrigado pela confiança!\nEquipe Agente Marketplace',
        };
      }
      return send(res, 200, { success:true, templates: global.msgTemplates, placeholders:['{comprador}','{item}','{rastreio}'] });
    }
    if (u.pathname === '/api/ml/mensagem/templates' && req.method === 'PUT') {
      try {
        const adminSecret = process.env.ADMIN_SECRET || loadEnv().ADMIN_SECRET || 'agente-marketplace-2026';
        const providedSecret = req.headers['x-admin-secret'] || req.headers['X-Admin-Secret'];
        if (providedSecret !== adminSecret) {
          return send(res, 403, { success:false, error:'Acesso negado — X-Admin-Secret inválido' });
        }
        const body = await readBody(req).catch(() => ({}));

        // RISCO 5 — valida/sanitiza cada template antes de salvar.
        // Se tem violações, recusa o salvamento inteiro e avisa o usuário.
        for (const tipo of ['venda_confirmada','produto_enviado','produto_entregue']) {
          if (body[tipo]) {
            const v = validarRespostaML(body[tipo]);
            if (v.violacoes.length > 0) {
              return send(res, 200, {
                success: false,
                error: `Template "${tipo}" contém conteúdo proibido pelo ML: ${v.violacoes.join(', ')}`,
                violacoes: v.violacoes,
                campo: tipo,
              });
            }
          }
        }

        if (!global.msgTemplates) global.msgTemplates = {};
        if (body.venda_confirmada) global.msgTemplates.venda_confirmada = String(body.venda_confirmada);
        if (body.produto_enviado)  global.msgTemplates.produto_enviado  = String(body.produto_enviado);
        if (body.produto_entregue) global.msgTemplates.produto_entregue = String(body.produto_entregue);

        try {
          fs.writeFileSync(TEMPLATES_FILE, JSON.stringify(global.msgTemplates, null, 2));
          try { fs.chmodSync(TEMPLATES_FILE, 0o600); } catch(_){}
        } catch (e) { /* não bloqueia — fica só em memória */ }

        console.log('✉️ [templates] Atualizados com sucesso');
        return send(res, 200, { success:true, templates: global.msgTemplates });
      } catch (error) {
        return send(res, 200, { success:false, error: error.message });
      }
    }

    // ============================================================
    // COMPLIANCE STATUS — resumo das regras ativas
    // ============================================================
    if (u.pathname === '/api/compliance/status' && req.method === 'GET') {
      return send(res, 200, {
        success: true,
        regras: {
          sac: {
            sanitizacao: true,
            validacaoIA: true,
            limiteCaracteres: 2000,
            bloqueioTelefone: true,
            bloqueioEmail: true,
            bloqueioWhatsApp: true,
            bloqueioLinks: true,
            bloqueioConcorrentes: true,
            tmrAlvo: '< 30 minutos',
          },
          publicacao: {
            tituloMax60: true,
            validacaoTermosProibidos: true,
            validacaoInmetro: true,
            antiDuplicidade: true,
            validacaoFotoFundoBranco: 'aviso (não automático)',
            fichaTenicaCompleta: true,
            filtroProibidos: true,
          },
          mensagens: {
            sanitizacaoTemplates: true,
            semLinksExternos: true,
            semDadosPessoais: true,
            semConcorrentes: true,
          },
          conta: {
            multiConta: false,
            cancelamentoMonitorado: 'futuro',
            reputacaoMonitorada: true,
          },
        },
        violacoesRegistradas: (global.errosIntegracao || []).filter(e => e.origem === 'compliance').length,
        ultimaVerificacao: new Date().toISOString(),
      });
    }

    // ============================================================
    // GAP 7 — ERROS DE INTEGRAÇÃO (listar / resolver)
    // ============================================================
    if (u.pathname === '/api/erros' && req.method === 'GET') {
      const erros = global.errosIntegracao || [];
      const naoResolvidos = erros.filter(e => !e.resolvido);
      return send(res, 200, {
        success: true,
        total: erros.length,
        naoResolvidos: naoResolvidos.length,
        erros: erros.slice(0, 50),
        porOrigem: {
          publicacao: erros.filter(e => e.origem === 'publicacao').length,
          sac:        erros.filter(e => e.origem === 'sac').length,
          estoque:    erros.filter(e => e.origem === 'estoque').length,
          pedido:     erros.filter(e => e.origem === 'pedido').length,
          webhook:    erros.filter(e => e.origem === 'webhook').length,
          auth:       erros.filter(e => e.origem === 'auth').length,
        },
      });
    }
    if (u.pathname.startsWith('/api/erros/') && u.pathname.endsWith('/resolver') && req.method === 'POST') {
      const idStr = u.pathname.split('/')[3];
      const id = parseInt(idStr, 10);
      const erro = (global.errosIntegracao || []).find(e => e.id === id);
      if (erro) {
        erro.resolvido = true;
        erro.resolvidoEm = new Date().toISOString();
        return send(res, 200, { success:true });
      }
      return send(res, 200, { success:false, error:'Erro não encontrado' });
    }

    // ============================================================
    // GAP 8 — CONFIG DE ESTOQUE (reserva de segurança + alertas)
    // ============================================================
    if (u.pathname === '/api/estoque/config' && req.method === 'GET') {
      return send(res, 200, { success:true, config: global.estoqueConfig });
    }
    if (u.pathname === '/api/estoque/config' && req.method === 'POST') {
      try {
        const body = await readBody(req).catch(() => ({}));
        if (body.reservaSeguranca     !== undefined) global.estoqueConfig.reservaSeguranca    = Math.max(0, parseInt(body.reservaSeguranca, 10) || 0);
        if (body.alertaBaixo          !== undefined) global.estoqueConfig.alertaBaixo         = Math.max(0, parseInt(body.alertaBaixo, 10) || 0);
        if (body.pausarQuandoZero     !== undefined) global.estoqueConfig.pausarQuandoZero    = !!body.pausarQuandoZero;
        if (body.reativarQuandoVoltar !== undefined) global.estoqueConfig.reativarQuandoVoltar = !!body.reativarQuandoVoltar;
        console.log(`📦 [estoque] Config: reserva=${global.estoqueConfig.reservaSeguranca}, alerta=${global.estoqueConfig.alertaBaixo}`);
        return send(res, 200, { success:true, config: global.estoqueConfig });
      } catch (error) {
        return send(res, 200, { success:false, error: error.message });
      }
    }

    // ============================================================
    // MONITORAMENTO DE PERFORMANCE + CLASSIFICAÇÃO AUTOMÁTICA
    // ============================================================

    // GET /api/ml/performance — visitas, vendas, CTR e classificação (TOP/MÉDIO/RUIM)
    if (u.pathname === '/api/ml/performance' && req.method === 'GET') {
      const token = getBearer();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      try {
        const meResp = await mlFetch('https://api.mercadolibre.com/users/me', {
          headers: { 'Authorization': 'Bearer ' + token }
        });
        const me = await meResp.json();
        if (!meResp.ok || !me.id) {
          return send(res, meResp.status || 401, { success:false, error: me.message || 'Token inválido' });
        }

        const itemsResp = await mlFetch(
          `https://api.mercadolibre.com/users/${me.id}/items/search?limit=50`,
          { headers: { 'Authorization': 'Bearer ' + token } }
        );
        const itemsData = await itemsResp.json();
        const itemIds = itemsData.results || [];

        if (itemIds.length === 0) {
          return send(res, 200, {
            success: true,
            anuncios: [],
            total: 0,
            classificacao: { top: [], medio: [], ruim: [] },
            resumo: { top:0, medio:0, ruim:0, totalVendas:0, totalVisitas:0, ctrMedio:'0.00' }
          });
        }

        // Buscar detalhes em batch (máx 20 por chamada)
        const batch = itemIds.slice(0, 20).join(',');
        const detailsResp = await mlFetch(
          `https://api.mercadolibre.com/items?ids=${batch}&attributes=id,title,price,available_quantity,sold_quantity,status,date_created,listing_type_id,health`,
          { headers: { 'Authorization': 'Bearer ' + token } }
        );
        const details = await detailsResp.json();

        // Janela de visitas: últimos 30 dias
        const desde = new Date();
        desde.setDate(desde.getDate() - 30);
        const desdeStr = desde.toISOString().split('T')[0];
        const ateStr = new Date().toISOString().split('T')[0];

        const anuncios = [];
        for (const d of (details || [])) {
          const item = d && d.body;
          if (!item) continue;

          let visitas = 0;
          try {
            const visitResp = await mlFetch(
              `https://api.mercadolibre.com/visits/items/${item.id}?date_from=${desdeStr}&date_to=${ateStr}`,
              { headers: { 'Authorization': 'Bearer ' + token } }
            );
            const visitData = await visitResp.json();
            visitas = visitData.total_visits || 0;
          } catch(e) {}

          const vendas = item.sold_quantity || 0;
          const ctr = visitas > 0 ? ((vendas / visitas) * 100).toFixed(2) : '0.00';
          const diasAtivo = Math.floor((Date.now() - new Date(item.date_created).getTime()) / (1000*60*60*24));

          // Score de performance (0-100)
          let perfScore = 0;
          if (vendas >= 10) perfScore += 40;
          else if (vendas >= 5) perfScore += 30;
          else if (vendas >= 1) perfScore += 20;

          if (visitas >= 100) perfScore += 20;
          else if (visitas >= 30) perfScore += 15;
          else if (visitas >= 10) perfScore += 10;

          if (parseFloat(ctr) >= 5) perfScore += 20;
          else if (parseFloat(ctr) >= 2) perfScore += 15;
          else if (parseFloat(ctr) >= 0.5) perfScore += 10;

          if (item.available_quantity > 0) perfScore += 10;
          if (item.status === 'active') perfScore += 10;

          anuncios.push({
            id: item.id,
            titulo: item.title,
            preco: item.price,
            status: item.status,
            estoque: item.available_quantity,
            vendas30d: vendas,
            visitas30d: visitas,
            ctr: parseFloat(ctr),
            diasAtivo,
            perfScore: Math.min(perfScore, 100),
            listingType: item.listing_type_id
          });

          await new Promise(r => setTimeout(r, 300));
        }

        // Classificar: Top 20%, Médio 30%, Ruim 50%
        anuncios.sort((a, b) => b.perfScore - a.perfScore);
        const totalAnuncios = anuncios.length;
        const topLimit = Math.ceil(totalAnuncios * 0.2);
        const medioLimit = Math.ceil(totalAnuncios * 0.5);

        const classificacao = {
          top: anuncios.slice(0, topLimit).map(a => ({ ...a, classe: '🟢 TOP — escalar' })),
          medio: anuncios.slice(topLimit, medioLimit).map(a => ({ ...a, classe: '🟡 MÉDIO — otimizar' })),
          ruim: anuncios.slice(medioLimit).map(a => ({
            ...a,
            classe: '🔴 RUIM — pausar',
            sugestao: a.diasAtivo >= 14 && a.vendas30d === 0 ? '⚠️ 14+ dias sem venda — considerar pausar' : null
          }))
        };

        return send(res, 200, {
          success: true,
          total: totalAnuncios,
          anuncios,
          classificacao,
          resumo: {
            top: classificacao.top.length,
            medio: classificacao.medio.length,
            ruim: classificacao.ruim.length,
            totalVendas: anuncios.reduce((s, a) => s + a.vendas30d, 0),
            totalVisitas: anuncios.reduce((s, a) => s + a.visitas30d, 0),
            ctrMedio: anuncios.length > 0 ? (anuncios.reduce((s, a) => s + a.ctr, 0) / anuncios.length).toFixed(2) : '0.00'
          }
        });
      } catch (error) {
        return send(res, 200, { success: false, error: error.message });
      }
    }

    // POST /api/ml/performance/pausar-ruins — pausa anúncios ruins (modoTeste por padrão)
    if (u.pathname === '/api/ml/performance/pausar-ruins' && req.method === 'POST') {
      const token = getBearer();
      if (!token) return send(res, 200, { success:false, error:'Token não fornecido' });
      try {
        const body = await readBody(req).catch(() => ({}));
        const diasSemVenda = Number.isFinite(+body.diasSemVenda) ? +body.diasSemVenda : 14;
        const modoTeste = body.modoTeste !== false; // default true

        // Reusa a rota /api/ml/performance localmente
        const perfResp = await fetch(`http://localhost:${PORT}/api/ml/performance`, {
          headers: { 'Authorization': 'Bearer ' + token }
        });
        const perfData = await perfResp.json();

        if (!perfData.success) return send(res, 200, perfData);

        const aPausar = (perfData.classificacao?.ruim || []).filter(
          a => a.diasAtivo >= diasSemVenda && a.vendas30d === 0 && a.status === 'active'
        );

        const resultados = [];
        for (const anuncio of aPausar) {
          if (modoTeste) {
            resultados.push({
              id: anuncio.id,
              titulo: anuncio.titulo,
              acao: '⏸️ Seria pausado (modo teste)',
              modoTeste: true
            });
            console.log(`🤖 [perf] TESTE: pausaria ${anuncio.id} (${anuncio.diasAtivo}d sem venda)`);
          } else {
            try {
              const pauseResp = await mlFetch(`https://api.mercadolibre.com/items/${anuncio.id}`, {
                method: 'PUT',
                headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
                body: JSON.stringify({ status: 'paused' })
              });
              resultados.push({
                id: anuncio.id,
                titulo: anuncio.titulo,
                acao: pauseResp.ok ? '⏸️ PAUSADO' : '❌ Erro ao pausar',
                modoTeste: false
              });
              console.log(`🤖 [perf] ${pauseResp.ok ? '⏸️ PAUSADO' : '❌ ERRO'}: ${anuncio.id} (${anuncio.diasAtivo}d sem venda)`);
            } catch(e) {
              resultados.push({ id: anuncio.id, titulo: anuncio.titulo, acao: '❌ Erro', error: e.message });
            }
            await new Promise(r => setTimeout(r, 500));
          }
        }

        return send(res, 200, {
          success: true,
          modoTeste,
          candidatos: aPausar.length,
          pausados: resultados.filter(r => r.acao.includes('PAUSADO')).length,
          resultados
        });
      } catch (error) {
        return send(res, 200, { success: false, error: error.message });
      }
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
