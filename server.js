/**
 * 圣诞树之夜 · Christmas Room
 * 单进程多人房间服务：账号注册登录 + WebSocket 实时房间
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const zlib = require('zlib');
const mqtt = require('mqtt');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT || 8080);
const ROOT = __dirname;
const PUBLIC_DIR = path.join(ROOT, 'public');
const DATA_DIR = path.join(ROOT, 'data');
const ACCOUNTS_FILE = path.join(DATA_DIR, 'accounts.json');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

/* ------------------------------------------------------------------ 世界常量 */
const WORLD = { w: 2200, h: 1500 };
const TREE = { x: 1100, y: 700, r: 150 };
const COFFIN = { x: 360, y: 1240, r: 46 };
const CAULDRON = { x: 1900, y: 380, r: 52 };
const SPAWN = { x: 1100, y: 1010 };

/* ------------------------------------------------------------------ 账号存储 */
let accounts = {};
try { accounts = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf8')); } catch (e) { accounts = {}; }
let saveTimer = null;
function saveAccounts() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    fs.writeFile(ACCOUNTS_FILE, JSON.stringify(accounts, null, 1), () => {});
  }, 300);
}
function hashPass(pass, salt) {
  return crypto.scryptSync(String(pass), salt, 32).toString('hex');
}
const SESSIONS_FILE = path.join(DATA_DIR, 'sessions.json');
let sessions = new Map(); // token -> accountKey
try {
  const raw = JSON.parse(fs.readFileSync(SESSIONS_FILE, 'utf8'));
  Object.keys(raw).forEach(k => sessions.set(k, raw[k]));
} catch (e) {
  sessions = new Map();
}
function saveSessions() {
  const obj = {};
  for (const [k, v] of sessions) obj[k] = v;
  fs.writeFile(SESSIONS_FILE, JSON.stringify(obj), () => {});
}
function newSession(key) {
  const token = crypto.randomBytes(24).toString('hex');
  sessions.set(token, key);
  if (sessions.size > 500) { const first = sessions.keys().next().value; sessions.delete(first); }
  saveSessions();
  return token;
}

/* ------------------------------------------------------------------ 物品定义 */
const ITEMS = {
  pumpkin: { name: '彩球', slot: 'hat', throwable: true, effect: 'pumpkin', color: '#e23b3b' },
  hat:     { name: '圣诞帽', slot: 'hat', throwable: true, effect: 'hat', color: '#d8303f' },
  skull:   { name: '雪人面具', slot: 'mask', throwable: true, effect: 'skull', color: '#eef6ff' },
  cape:    { name: '红披风', slot: 'cape', throwable: true, effect: 'cape', color: '#b52633' },
  candy:   { name: '糖果', slot: null, throwable: true, effect: 'candy', color: '#ff4d6d' },
  pie:     { name: '姜饼人', slot: null, throwable: true, effect: 'pie', color: '#c98a4b' },
  bone:    { name: '冰锥', slot: null, throwable: true, effect: 'bone', color: '#bfe6ff' },
  spider:  { name: '雪球', slot: null, throwable: true, effect: 'spider', color: '#eaf6ff' },
  potion:  { name: '热可可', slot: null, throwable: true, effect: 'potion', color: '#d9a06b' },
  candle:  { name: '星星灯', slot: null, throwable: true, effect: 'candle', color: '#ffd76b' }
};
const SPAWN_POOL = ['pumpkin','hat','skull','cape','candy','candy','candy','pie','bone','spider','potion','candle'];

/* ------------------------------------------------------------------ 场景装饰 */
const DECOR = (() => {
  const list = [];
  const add = (type, x, y, s, extra) => list.push(Object.assign({ type, x, y, s: s || 1 }, extra || {}));
  // 雪人与拐杖糖
  const graves = [[300,380],[470,270],[1720,1180],[1880,1040],[760,1290],[1500,240],[560,560],[1760,600],[250,760],[1980,880],[1260,1330],[380,1010]];
  graves.forEach(([x, y], i) => add(i % 3 === 0 ? 'cross' : 'grave', x, y, 1 + (i % 2) * 0.2, { rot: (i % 4) * 0.06 - 0.09 }));
  // 冰灯
  [[820,300],[1450,300],[820,1140],[1450,1140],[390,900],[1960,740],[1100,360]].forEach(([x, y]) => add('lantern', x, y, 1));
  // 礼物堆
  [[1000,1180],[1250,380],[1990,640],[230,640],[1640,1350],[520,880],[1500,860]].forEach(([x, y], i) => add('pumpkins', x, y, 1 + (i % 2) * 0.15, { rot: i * 0.4 }));
  // 挂雪的枯树
  [[150,250],[2080,1320],[2030,980],[150,1150],[2100,560]].forEach(([x, y]) => add('deadtree', x, y, 1));
  // 礼物盒、彩球桶、拐杖糖、雪痕
  [[680,1040],[1620,1080],[1180,480],[330,560],[1880,1160],[1280,1200]].forEach(([x, y], i) => add(i % 2 ? 'crate' : 'barrel', x, y, 1 + (i % 3) * 0.12, { rot: i * 0.3 }));
  [[960,940],[1560,1000],[560,430],[1820,760],[420,1180]].forEach(([x, y]) => add('candles', x, y, 1));
  [[90,180],[2110,180],[90,1400],[2110,1400],[520,150],[1680,1450]].forEach(([x, y], i) => add('web', x, y, 1.1, { rot: i * 0.5 }));
  return list;
})();

/* ------------------------------------------------------------------ 任务池 */
const TASK_POOL = [
  { id: 'candy',  text: '收集 {n} 颗糖果', target: 5, stat: 'candyPicked' },
  { id: 'hit',    text: '用道具砸中别人 {n} 次', target: 3, stat: 'hits' },
  { id: 'punch',  text: '近身互殴命中 {n} 次', target: 3, stat: 'punches' },
  { id: 'chat',   text: '在房间里说 {n} 句话', target: 1, stat: 'chats' },
  { id: 'ride',   text: '骑到别人背上 {n} 次', target: 1, stat: 'rides' },
  { id: 'coffin', text: '钻进礼盒躲 {n} 次', target: 1, stat: 'hides' },
  { id: 'potion', text: '喝下 {n} 杯热可可', target: 1, stat: 'potions' },
  { id: 'wear',   text: '穿戴 {n} 件圣诞装扮', target: 2, stat: 'worn' },
  { id: 'survive',text: '累计活过 {n} 秒不死', target: 60, stat: 'aliveSeconds' }
];
function rollTasks() {
  const pool = TASK_POOL.slice().sort(() => Math.random() - 0.5);
  return pool.slice(0, 3).map(t => ({ id: t.id, text: t.text.replace('{n}', t.target), target: t.target, stat: t.stat, done: false, prog: 0 }));
}

/* ------------------------------------------------------------------ 房间状态 */
const state = {
  players: new Map(),      // id -> player
  items: [],
  projectiles: [],
  mode: { halloween: false, until: 0, next: Date.now() + 90000 },
  itemSeq: 1,
  projSeq: 1,
  events: []
};

function clamp(v, a, b) { return v < a ? a : v > b ? b : v; }
function dist(a, b, c, d) { return Math.hypot(a - c, b - d); }
function randItemType() { return SPAWN_POOL[Math.floor(Math.random() * SPAWN_POOL.length)]; }

function spawnItem(type, x, y) {
  const it = { id: state.itemSeq++, type, x, y, born: Date.now() };
  state.items.push(it);
  itemsRev++;
  return it;
}
let itemsRev = 0;
function randomFloorPos() {
  for (let i = 0; i < 40; i++) {
    const x = 70 + Math.random() * (WORLD.w - 140);
    const y = 70 + Math.random() * (WORLD.h - 140);
    if (dist(x, y, TREE.x, TREE.y) > TREE.r + 40 && dist(x, y, COFFIN.x, COFFIN.y) > 70) return { x, y };
  }
  return { x: 300, y: 300 };
}
function seedItems(n) {
  for (let i = 0; i < n; i++) { const p = randomFloorPos(); spawnItem(randItemType(), p.x, p.y); }
}
seedItems(34);

function getTasksFor(p) {
  // 完成任务后自动补新任务
  p.tasks.forEach(t => {
    if (!t.done && t.prog >= t.target) {
      t.done = true;
      p.candy += 6;
      p.completed = (p.completed || 0) + 1;
      pushEvent(`${p.name} 完成了任务「${t.text}」，获得 6 颗糖`);
      setTimeout(() => {
        if (state.players.has(p.id) && p.tasks.every(x => x.done)) p.tasks = rollTasks();
      }, 900);
    }
  });
}
function bump(p, stat, amount) {
  if (!p || !p.tasks) return;
  p.tasks.forEach(t => { if (!t.done && t.stat === stat) t.prog += amount; });
  getTasksFor(p);
}
function pushEvent(text) {
  state.events.push({ text, ts: Date.now() });
  broadcast({ t: 'event', text });
}

/* ------------------------------------------------------------------ 玩家 */
const MAX_SPEED = 250; // px/s
function makePlayer(id, name, avatar, x, y) {
  return {
    id, name, avatar,
    x, y, dir: 1,
    hp: 100, maxHp: 100, candy: 0, crown: false,
    held: null, wear: { hat: null, mask: null, cape: null },
    riding: null, rider: null, carry: null,
    dead: false, ghostUntil: 0, hiddenUntil: 0,
    stickyUntil: 0, scaredUntil: 0, lastHit: 0, lastDamage: 0,
    anim: 'idle', animUntil: 0, emote: null,
    tasks: rollTasks(), completed: 0,
    lastChat: 0, aliveSince: Date.now(), joinedAt: Date.now(),
    lastMoveAt: Date.now()
  };
}

function publicPlayer(p, withTasks) {
  const o = {
    id: p.id, name: p.name, avatar: p.avatar,
    x: Math.round(p.x), y: Math.round(p.y), dir: p.dir,
    hp: Math.round(p.hp), candy: p.candy, crown: p.crown,
    held: p.held ? p.held.type : null,
    wear: p.wear,
    riding: p.riding, rider: p.rider,
    dead: p.dead, hidden: Date.now() < p.hiddenUntil,
    sticky: Date.now() < p.stickyUntil, scared: Date.now() < p.scaredUntil,
    anim: p.anim, emote: p.emote && { kind: p.emote.kind, until: p.emote.until }
  };
  if (withTasks) o.tasks = p.tasks.map(t => ({ text: t.text, prog: Math.min(t.prog, t.target), target: t.target, done: t.done }));
  return o;
}

/* ------------------------------------------------------------------ 广播 */
const sockets = new Map(); // playerId -> ws
function send(ws, msg) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg)); }
function broadcast(msg) {
  const raw = JSON.stringify(msg);
  for (const ws of sockets.values()) { if (ws.readyState === 1) ws.send(raw); }
}

/* ------------------------------------------------------------------ 物理与交互 */
function removeHeld(p) {
  const held = p.held;
  p.held = null;
  return held;
}

function applyEffect(target, owner, type, proj) {
  const now = Date.now();
  const def = ITEMS[type] || {};
  switch (def.effect) {
    case 'candy':
      target.candy += 2;
      target.anim = 'happy'; target.animUntil = now + 400;
      send(sockets.get(owner && owner.id), { t: 'toast', text: `糖果砸到 ${target.name}，对方 +2 糖` });
      break;
    case 'pie':
      target.hp -= 8; target.stickyUntil = now + 2500;
      break;
    case 'pumpkin':
      target.hp -= 10;
      if (!target.wear.hat) { target.wear.hat = 'pumpkin'; if (target.id) bump(target, 'worn', 1); }
      break;
    case 'hat':
      target.hp -= 4;
      if (!target.wear.hat) { target.wear.hat = 'hat'; bump(target, 'worn', 1); }
      break;
    case 'skull':
      target.hp -= 6;
      if (!target.wear.mask) { target.wear.mask = 'skull'; bump(target, 'worn', 1); }
      break;
    case 'cape':
      target.hp -= 4;
      if (!target.wear.cape) { target.wear.cape = 'cape'; bump(target, 'worn', 1); }
      break;
    case 'bone':
      target.hp -= 14;
      break;
    case 'spider':
      target.hp -= 6; target.scaredUntil = now + 2000;
      send(sockets.get(target.id), { t: 'shake' });
      break;
    case 'potion':
      target.hp = Math.min(target.maxHp, target.hp + 22);
      break;
    case 'candle':
      target.hp -= 5; target.scaredUntil = now + 1200;
      break;
  }
  target.lastDamage = now;
  if (owner && owner !== target) {
    owner.lastHit = now;
    bump(owner, 'hits', 1);
  }
  if (target.hp <= 0 && !target.dead) killPlayer(target);
}

function killPlayer(p) {
  p.dead = true;
  p.hp = 0;
  p.ghostUntil = Date.now() + 4500;
  p.riding = null;
  if (p.rider) { const r = state.players.get(p.rider); if (r) r.riding = null; p.rider = null; }
  p.hiddenUntil = 0;
  p.carry = null;
  pushEvent(`${p.name} 被雪夜吞掉了… 4 秒后在圣诞树下复活`);
}

function respawn(p) {
  const a = Math.random() * Math.PI * 2;
  p.x = SPAWN.x + Math.cos(a) * 120;
  p.y = SPAWN.y + Math.sin(a) * 90;
  p.hp = p.maxHp; p.dead = false;
  p.stickyUntil = 0; p.scaredUntil = 0;
  p.aliveSince = Date.now();
  p.hiddenUntil = 0;
}

function nearestPlayer(p, maxDist) {
  let best = null, bestD = maxDist;
  for (const o of state.players.values()) {
    if (o.id === p.id || o.dead) continue;
    const d = dist(p.x, p.y, o.x, o.y);
    if (d < bestD) { bestD = d; best = o; }
  }
  return best;
}

/* ------------------------------------------------------------------ 主循环 */
let lastTick = Date.now();
setInterval(() => {
  const now = Date.now();
  const dt = Math.min(0.08, (now - lastTick) / 1000);
  lastTick = now;

  // 万圣模式调度
  const m = state.mode;
  if (!m.halloween && now > m.next) {
    m.halloween = true; m.until = now + 45000;
    pushEvent('礼物风暴开始：地板开始喷出装扮和糖果，快去抢！');
    for (let i = 0; i < 26; i++) { const p = randomFloorPos(); spawnItem(randItemType(), p.x, p.y); }
  } else if (m.halloween && now > m.until) {
    m.halloween = false; m.next = now + 90000;
    pushEvent('礼物风暴退去，房间恢复正常。');
  }

  // 玩家
  for (const p of state.players.values()) {
    if (p.dead) {
      if (now > p.ghostUntil) respawn(p);
      continue;
    }
    if (now > p.hiddenUntil) { /* 可见 */ }
    // 回血
    if (now - p.lastDamage > 6000 && p.hp < p.maxHp) p.hp = Math.min(p.maxHp, p.hp + 3 * dt);
    // 骑乘同步
    if (p.riding) {
      const t = state.players.get(p.riding);
      if (!t || t.dead) { p.riding = null; }
      else {
        p.x = t.x + (t.dir > 0 ? -10 : 10);
        p.y = t.y - 26;
        if (Math.random() < dt * 0.6 && t.candy > 0) { t.candy -= 1; p.candy += 1; }
      }
    }
    // 碰撞：树干、礼盒、世界边界
    if (!p.riding) {
      const dx = p.x - TREE.x, dy = p.y - TREE.y;
      const d = Math.hypot(dx, dy);
      if (d < TREE.r * 0.72) { p.x = TREE.x + dx / (d || 1) * TREE.r * 0.72; p.y = TREE.y + dy / (d || 1) * TREE.r * 0.72; }
    }
    p.x = clamp(p.x, 48, WORLD.w - 48);
    p.y = clamp(p.y, 60, WORLD.h - 40);
    // 走到物品上自动捡起
    if (!p.held && !p.dead && now > p.hiddenUntil) {
      const near = state.items.find(it => dist(p.x, p.y, it.x, it.y) < 46);
      if (near) {
        state.items = state.items.filter(i => i.id !== near.id);
        itemsRev++;
        p.held = { type: near.type };
        p.anim = 'grab'; p.animUntil = now + 300;
        if (near.type === 'candy' || near.type === 'pumpkin') bump(p, 'candyPicked', near.type === 'candy' ? 1 : 0);
      }
    }
    p.anim = p.animUntil && now > p.animUntil ? 'idle' : p.anim;
    if (p.emote && now > p.emote.until) p.emote = null;
    bump(p, 'aliveSeconds', dt);
  }

  // 地面物品补充
  if (state.items.length < (m.halloween ? 40 : 26)) {
    const p = randomFloorPos(); spawnItem(randItemType(), p.x, p.y);
  }
  // 过期物品
  const beforeLen = state.items.length;
  state.items = state.items.filter(it => now - it.born < 1000 * 100);
  if (state.items.length > 44) state.items = state.items.slice(state.items.length - 44);
  if (state.items.length !== beforeLen) itemsRev++;

  // 投掷物
  for (const pr of state.projectiles) {
    const steps = 6;
    for (let s = 0; s < steps && pr.life > 0; s++) {
      pr.x += pr.vx * dt / steps;
      pr.y += pr.vy * dt / steps;
      pr.life -= 1;
      for (const t of state.players.values()) {
        if (t.dead || t.id === pr.owner) continue;
        if (Date.now() < t.hiddenUntil) continue;
        if (dist(pr.x, pr.y, t.x, t.y - 18) < 30) {
          const owner = state.players.get(pr.owner);
          applyEffect(t, owner, pr.type, pr);
          pr.life = 0;
          break;
        }
      }
      if (pr.x < 20 || pr.x > WORLD.w - 20 || pr.y < 20 || pr.y > WORLD.h - 20) pr.life = 0;
    }
    pr.ttl -= dt;
    if (pr.ttl <= 0) pr.life = 0;
  }
  state.projectiles = state.projectiles.filter(p => p.life > 0);

  // 状态包（15Hz 左右）
  broadcastState();
}, 60);

let lastBroadcast = 0;
let lastItemsRev = -1;
let bcTick = 0;
function broadcastState() {
  const now = Date.now();
  if (now - lastBroadcast < 65) return;
  lastBroadcast = now;
  const itemsChanged = itemsRev !== lastItemsRev;
  lastItemsRev = itemsRev;
  bcTick = (bcTick + 1) % 8;
  const withTasks = bcTick === 0;
  const players = [];
  for (const p of state.players.values()) players.push(publicPlayer(p, withTasks));
  const msg = {
    t: 'state',
    players,
    projectiles: state.projectiles.map(p => ({ id: p.id, type: p.type, x: Math.round(p.x), y: Math.round(p.y) })),
    mode: { halloween: state.mode.halloween, until: state.mode.until, next: state.mode.next },
    count: state.players.size
  };
  if (itemsChanged) msg.items = state.items.map(i => ({ id: i.id, type: i.type, x: Math.round(i.x), y: Math.round(i.y) }));
  broadcast(msg);
}

/* ------------------------------------------------------------------ HTTP 服务 */
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.png': 'image/png', '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.json': 'application/json; charset=utf-8' };

function json(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(body);
}
function readBody(req, cb) {
  let raw = '';
  req.on('data', c => { raw += c; if (raw.length > 2e5) req.destroy(); });
  req.on('end', () => { try { cb(JSON.parse(raw || '{}')); } catch (e) { cb(null); } });
}
function cleanName(s) { return String(s || '').replace(/[<>"\\/&\u0000-\u001f]/g, '').trim().slice(0, 14); }
function cleanColor(c, fb) { return /^#[0-9a-fA-F]{6}$/.test(String(c || '')) ? String(c).toLowerCase() : fb; }
function sanitizeAvatar(a) {
  a = a || {};
  return {
    skin: cleanColor(a.skin, '#e8b48c'),
    hair: cleanColor(a.hair, '#3b2415'),
    shirt: cleanColor(a.shirt, '#8a2be2'),
    pants: cleanColor(a.pants, '#2b2b3d'),
    shoes: cleanColor(a.shoes, '#141018'),
    style: clamp(Number(a.style) || 0, 0, 3)
  };
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');

  if (url.pathname === '/api/register' && req.method === 'POST') {
    readBody(req, (b) => {
      if (!b) return json(res, 400, { ok: false, error: '请求格式错误' });
      const name = cleanName(b.name);
      const pass = String(b.password || '');
      if (name.length < 2) return json(res, 400, { ok: false, error: '名字至少 2 个字' });
      if (pass.length < 4) return json(res, 400, { ok: false, error: '密码至少 4 位' });
      const key = name.toLowerCase();
      if (accounts[key]) return json(res, 409, { ok: false, error: '这个名字已经被注册了' });
      const salt = crypto.randomBytes(8).toString('hex');
      accounts[key] = { name, salt, hash: hashPass(pass, salt), avatar: sanitizeAvatar(b.avatar), candy: 0, createdAt: Date.now() };
      saveAccounts();
      const token = newSession(key);
      json(res, 200, { ok: true, token, name, avatar: accounts[key].avatar });
    });
    return;
  }

  if (url.pathname === '/api/login' && req.method === 'POST') {
    readBody(req, (b) => {
      if (!b) return json(res, 400, { ok: false, error: '请求格式错误' });
      const name = cleanName(b.name);
      const pass = String(b.password || '');
      const acc = accounts[name.toLowerCase()];
      if (!acc) return json(res, 404, { ok: false, error: '没有这个玩家，先去注册' });
      if (hashPass(pass, acc.salt) !== acc.hash) return json(res, 401, { ok: false, error: '密码不对，再想想' });
      const token = newSession(name.toLowerCase());
      json(res, 200, { ok: true, token, name: acc.name, avatar: acc.avatar, candy: acc.candy || 0 });
    });
    return;
  }

  if (url.pathname === '/api/health') return json(res, 200, { ok: true, players: state.players.size, mode: state.mode.halloween });

  // 静态文件
  let file = path.join(PUBLIC_DIR, decodeURIComponent(url.pathname === '/' ? '/index.html' : url.pathname));
  if (!file.startsWith(PUBLIC_DIR)) { res.writeHead(403); return res.end('forbidden'); }
  fs.readFile(file, (err, buf) => {
    if (err) {
      fs.readFile(path.join(PUBLIC_DIR, 'index.html'), (e2, b2) => {
        if (e2) { res.writeHead(404); return res.end('not found'); }
        res.writeHead(200, { 'Content-Type': MIME['.html'] }); res.end(b2);
      });
      return;
    }
    const type = MIME[path.extname(file)] || 'application/octet-stream';
    const texty = /html|text|json|javascript|svg/.test(type);
    const gzOk = /\bgzip\b/.test(String(req.headers['accept-encoding'] || ''));
    if (texty && gzOk && buf.length > 2048) {
      zlib.gzip(buf, (e, gz) => {
        if (e || !gz) { res.writeHead(200, { 'Content-Type': type, 'Cache-Control': 'no-cache' }); return res.end(buf); }
        res.writeHead(200, { 'Content-Type': type, 'Cache-Control': 'no-cache', 'Content-Encoding': 'gzip', Vary: 'Accept-Encoding' });
        res.end(gz);
      });
      return;
    }
    res.writeHead(200, { 'Content-Type': type, 'Cache-Control': 'no-cache' });
    res.end(buf);
  });
});

/* ------------------------------------------------------------------ WebSocket */
const wss = new WebSocketServer({ server, path: '/ws', perMessageDeflate: { threshold: 0, zlibDeflateOptions: { level: 6 }, clientNoContextTakeover: true, serverNoContextTakeover: true } });

function attachSocket(ws) {
  ws.isAlive = true;
  ws.playerId = null;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw); } catch (e) { return; }
    if (!ws.playerId) {
      // 直连模式下，注册/登录也走这条连接
      if (msg.t === 'rpc') return handleRpcWith(msg, obj => send(ws, obj));
      if (msg.t !== 'auth') return;
      const key = sessions.get(String(msg.token || ''));
      if (!key || !accounts[key]) return send(ws, { t: 'auth_fail', error: '登录状态失效，请重新登录' });
      const acc = accounts[key];
      const id = key + '#' + Math.random().toString(36).slice(2, 7);
      // 同名旧连接踢掉
      for (const [pid, old] of sockets) {
        const op = state.players.get(pid);
        if (op && op.name.toLowerCase() === key) {
          send(old, { t: 'kicked', text: '你已经在另一个窗口登录了' });
          state.players.delete(pid); sockets.delete(pid);
          old.close();
        }
      }
      const a = Math.random() * Math.PI * 2;
      const p = makePlayer(id, acc.name, acc.avatar, SPAWN.x + Math.cos(a) * 130, SPAWN.y + Math.sin(a) * 100);
      p.candy = acc.candy || 0;
      state.players.set(id, p);
      sockets.set(id, ws);
      ws.playerId = id;
      send(ws, {
        t: 'welcome',
        self: id, name: p.name, avatar: p.avatar, tasks: p.tasks,
        world: WORLD, tree: TREE, coffin: COFFIN, cauldron: CAULDRON, spawn: SPAWN,
        decor: DECOR, itemDefs: ITEMS,
        mode: state.mode, online: state.players.size, candy: p.candy
      });
      pushEvent(`${p.name} 走进了圣诞房间`);
      lastItemsRev = -1; // 让下一次广播带上完整物品列表
      return;
    }

    const p = state.players.get(ws.playerId);
    if (!p) return;

    switch (msg.t) {
      case 'input': {
        if (p.dead) break;
        p.dir = msg.dir === -1 ? -1 : 1;
        if (p.riding) break;
        const nx = Number(msg.x), ny = Number(msg.y);
        if (!isFinite(nx) || !isFinite(ny)) break;
        const now = Date.now();
        const dt = Math.max(0.016, Math.min(0.4, (now - (p.lastMoveAt || now)) / 1000));
        p.lastMoveAt = now;
        const maxStep = MAX_SPEED * dt * (Date.now() < p.stickyUntil ? 0.45 : 1) * (msg.sprint ? 1.5 : 1) + 12;
        const d = dist(p.x, p.y, nx, ny);
        if (d <= maxStep) { p.x = nx; p.y = ny; }
        else { p.x += (nx - p.x) / d * maxStep; p.y += (ny - p.y) / d * maxStep; }
        p.anim = d > 4 ? 'walk' : 'idle';
        break;
      }
      case 'chat': {
        const text = String(msg.text || '').replace(/[\u0000-\u001f]/g, '').slice(0, 80).trim();
        if (!text) break;
        const now = Date.now();
        if (now - p.lastChat < 900) { send(ws, { t: 'toast', text: '说慢一点…' }); break; }
        p.lastChat = now;
        broadcast({ t: 'chat', id: p.id, name: p.name, text, ts: now });
        bump(p, 'chats', 1);
        break;
      }
      case 'pickup': {
        if (p.dead || p.held) break;
        if (Date.now() < p.hiddenUntil) break;
        const it = state.items.find(i => i.id === Number(msg.itemId));
        if (!it) break;
        if (dist(p.x, p.y, it.x, it.y) > 150) break;
        state.items = state.items.filter(i => i.id !== it.id);
        itemsRev++;
        p.held = { type: it.type };
        p.anim = 'grab'; p.animUntil = Date.now() + 300;
        if (it.type === 'candy' || it.type === 'pumpkin') bump(p, 'candyPicked', it.type === 'candy' ? 1 : 0);
        break;
      }
      case 'drop': {
        if (!p.held) break;
        const t = removeHeld(p);
        const x = p.x + p.dir * 26, y = p.y + 6;
        spawnItem(t.type, clamp(x, 40, WORLD.w - 40), clamp(y, 50, WORLD.h - 30));
        break;
      }
      case 'wear': {
        if (!p.held) break;
        const def = ITEMS[p.held.type];
        const slot = def.slot;
        if (!slot) { send(ws, { t: 'toast', text: '这个不能穿，试试丢出去砸人' }); break; }
        const prev = p.wear[slot];
        p.wear[slot] = p.held.type;
        p.held = null;
        bump(p, 'worn', 1);
        if (prev) { const pos = randomFloorPos(); spawnItem(prev, pos.x, pos.y); }
        p.anim = 'happy'; p.animUntil = Date.now() + 500;
        break;
      }
      case 'throw': {
        if (!p.held || p.dead) break;
        const type = p.held.type;
        const def = ITEMS[type];
        if (!def || !def.throwable) break;
        p.held = null;
        let tx = Number(msg.x), ty = Number(msg.y);
        if (!isFinite(tx) || !isFinite(ty)) { tx = p.x + p.dir * 200; ty = p.y - 10; }
        const dx = tx - p.x, dy = ty - p.y - 10;
        const len = Math.hypot(dx, dy) || 1;
        const speed = 620;
        state.projectiles.push({
          id: state.projSeq++, type, owner: p.id,
          x: p.x + dx / len * 20, y: p.y - 18 + dy / len * 20,
          vx: dx / len * speed, vy: dy / len * speed,
          life: 1e9, ttl: Math.min(1.2, len / speed + 0.25)
        });
        p.anim = 'throw'; p.animUntil = Date.now() + 350;
        break;
      }
      case 'attack': {
        if (p.dead) break;
        const now = Date.now();
        if (now - (p.lastAttack || 0) < 500) break;
        p.lastAttack = now;
        p.anim = 'attack'; p.animUntil = now + 350;
        let hitAny = false;
        for (const t of state.players.values()) {
          if (t.id === p.id || t.dead) continue;
          if (Date.now() < t.hiddenUntil) continue;
          if (dist(p.x + p.dir * 26, p.y - 16, t.x, t.y - 16) < 62) {
            t.hp -= 12; t.lastDamage = now;
            t.x += p.dir * 12; t.anim = 'hurt'; t.animUntil = now + 300;
            hitAny = true;
            if (t.hp <= 0 && !t.dead) killPlayer(t);
            if (t.riding) { const r = state.players.get(t.riding); if (r) r.riding = null; t.rider = null; t.riding = null; }
          }
        }
        if (hitAny) bump(p, 'punches', 1);
        break;
      }
      case 'ride': {
        if (p.dead || p.riding) break;
        const t = nearestPlayer(p, 110);
        if (!t || t.rider || t.riding) { send(ws, { t: 'toast', text: '附近没有可以骑的人' }); break; }
        p.riding = t.id; t.rider = p.id;
        p.x = t.x - 10; p.y = t.y - 26;
        bump(p, 'rides', 1);
        t.anim = 'hurt'; t.animUntil = Date.now() + 400;
        pushEvent(`${p.name} 骑上了 ${t.name} 的背`);
        break;
      }
      case 'dismount': {
        if (!p.riding) {
          if (p.rider) { const r = state.players.get(p.rider); if (r) { r.riding = null; r.y = p.y + 30; } p.rider = null; }
          break;
        }
        p.riding = null;
        break;
      }
      case 'emote': {
        const kinds = ['dance', 'laugh', 'roar', 'cry'];
        if (!kinds.includes(msg.kind)) break;
        p.emote = { kind: msg.kind, until: Date.now() + 2500 };
        break;
      }
      case 'brew': {
        if (p.dead || p.held) break;
        if (dist(p.x, p.y, CAULDRON.x, CAULDRON.y) > 90) { send(ws, { t: 'toast', text: '走到热可可锅旁边才能舀' }); break; }
        p.held = { type: 'potion' };
        send(ws, { t: 'toast', text: '从锅里舀出一杯热可可' });
        break;
      }
      case 'drink': {
        if (!p.held || ITEMS[p.held.type].effect !== 'potion') break;
        p.held = null;
        p.hp = Math.min(p.maxHp, p.hp + 26);
        p.anim = 'happy'; p.animUntil = Date.now() + 600;
        bump(p, 'potions', 1);
        break;
      }
      case 'hide': {
        if (p.dead) break;
        const now = Date.now();
        if (now < p.hiddenUntil) { p.hiddenUntil = 0; break; }
        if (dist(p.x, p.y, COFFIN.x, COFFIN.y) > 95) { send(ws, { t: 'toast', text: '靠近大礼盒才能钻进去' }); break; }
        p.hiddenUntil = now + 3200;
        p.x = COFFIN.x; p.y = COFFIN.y;
        bump(p, 'hides', 1);
        break;
      }
      case 'ping': send(ws, { t: 'pong', ts: msg.ts });
        break;
    }
  });

  ws.on('close', () => {
    if (ws.playerId) {
      const p = state.players.get(ws.playerId);
      if (p) {
        if (accounts[p.name.toLowerCase()]) { accounts[p.name.toLowerCase()].candy = p.candy; saveAccounts(); }
        pushEvent(`${p.name} 离开了房间`);
        state.players.delete(p.id);
      }
      sockets.delete(ws.playerId);
    }
  });
}

wss.on('connection', attachSocket);

setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  }
}, 30000);


/* ------------------------------------------------------------------ MQTT 传输（用于公网无隧道场景） */
const BROKER_URLS = (process.env.MQTT_URLS ||
  'wss://broker-cn.emqx.io:8084/mqtt,wss://broker.emqx.io:8084/mqtt,wss://broker.hivemq.com:8884/mqtt')
  .split(',').map(x => x.trim()).filter(Boolean);
const ROOM_KEY = process.env.ROOM_KEY || 'main';
const TP = 'hroom2/' + ROOM_KEY;
const bridges = [];

function vPub(bridge, cid, str) {
  if (bridge.mq && bridge.mq.connected) bridge.mq.publish(TP + '/dn/' + cid, str, { qos: 0 });
}

function makeVSocket(bridge, cid) {
  const listeners = { message: [], close: [] };
  const vs = {
    cid, playerId: null, readyState: 1, isAlive: true,
    on(ev, cb) { (listeners[ev] = listeners[ev] || []).push(cb); return vs; },
    _in(raw) { for (const cb of listeners.message || []) { try { cb(raw); } catch (e) {} } },
    send(str) { vPub(bridge, cid, str); },
    ping() {},
    terminate() { vs.close(); },
    close() {
      if (vs.readyState === 0) return;
      vs.readyState = 0;
      for (const cb of listeners.close || []) { try { cb(); } catch (e) {} }
      bridge.vclients.delete(cid);
    }
  };
  bridge.vclients.set(cid, vs);
  return vs;
}

function rpcReply(bridge, cid, m, obj) {
  vPub(bridge, cid, JSON.stringify(Object.assign({ t: 'rpc', rid: m.rid }, obj)));
}

function handleRpcWith(m, reply) {
  const replyObj = (obj) => reply(Object.assign({ t: 'rpc', rid: m.rid }, obj));
  if (m.op === 'register') {
    const name = cleanName(m.name), pass = String(m.password || '');
    if (name.length < 2) return replyObj({ ok: false, error: '名字至少 2 个字' });
    if (pass.length < 4) return replyObj({ ok: false, error: '密码至少 4 位' });
    const key = name.toLowerCase();
    if (accounts[key]) return replyObj({ ok: false, error: '这个名字已经被注册了' });
    const salt = crypto.randomBytes(8).toString('hex');
    accounts[key] = { name, salt, hash: hashPass(pass, salt), avatar: sanitizeAvatar(m.avatar), candy: 0, createdAt: Date.now() };
    saveAccounts();
    const token = newSession(key);
    return replyObj({ ok: true, token, name, avatar: accounts[key].avatar });
  }
  if (m.op === 'login') {
    const name = cleanName(m.name), pass = String(m.password || '');
    const acc = accounts[name.toLowerCase()];
    if (!acc) return replyObj({ ok: false, error: '没有这个玩家，先去注册' });
    if (hashPass(pass, acc.salt) !== acc.hash) return replyObj({ ok: false, error: '密码不对，再想想' });
    const token = newSession(name.toLowerCase());
    return replyObj({ ok: true, token, name: acc.name, avatar: acc.avatar, candy: acc.candy || 0 });
  }
  return replyObj({ ok: false, error: '未知请求' });
}

function handleRpc(bridge, cid, m) {
  handleRpcWith(m, obj => vPub(bridge, cid, JSON.stringify(obj)));
}

function startBridge(url) {
  const bridge = { url, mq: null, vclients: new Map(), ok: false };
  bridges.push(bridge);
  const mq = mqtt.connect(url, {
    clientId: 'hroom' + Math.random().toString(16).slice(2, 10),
    clean: true, reconnectPeriod: 4000, connectTimeout: 20000, keepalive: 30
  });
  bridge.mq = mq;
  mq.on('connect', () => {
    bridge.ok = true;
    console.log('[mqtt] bridge connected:', url);
    mq.subscribe(TP + '/up/+', { qos: 0 });
    mq.subscribe(TP + '/rpc/+', { qos: 0 });
  });
  mq.on('close', () => { bridge.ok = false; });
  mq.on('error', () => {});
  mq.on('message', (topic, payload) => {
    const parts = String(topic).split('/');
    const kind = parts[parts.length - 2];
    const cid = parts[parts.length - 1];
    let msg; try { msg = JSON.parse(payload.toString()); } catch (e) { return; }
    if (!msg || !cid) return;
    if (kind === 'rpc') return handleRpc(bridge, cid, msg);
    let vs = bridge.vclients.get(cid);
    if (!vs || vs.readyState === 0) { vs = makeVSocket(bridge, cid); attachSocket(vs); }
    vs._in(JSON.stringify(msg));
  });
}
BROKER_URLS.forEach(startBridge);

setInterval(() => {
  for (const b of bridges) {
    for (const [cid, vs] of b.vclients) if (vs.playerId && !state.players.has(vs.playerId)) vs.close();
  }
}, 60000);

server.listen(PORT, () => console.log('halloween-room listening on ' + PORT));
//（注：内容由AI生成）
