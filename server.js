'use strict';
// เซิร์ฟเวอร์เกมมนุษย์หมาป่า — ไม่ต้องติดตั้งแพ็กเกจเพิ่ม (ใช้ Node.js ล้วน)
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const PUBLIC = path.join(__dirname, 'public');
const F = +process.env.WW_FAST || 1; // ใช้ทดสอบเท่านั้น: ย่นเวลาให้สั้นลง

/* ───────────── WebSocket เล็กๆ (RFC 6455) ───────────── */
class Sock {
  constructor(socket) {
    this.socket = socket; this.buf = Buffer.alloc(0); this.open = true;
    this.onmessage = null; this.onclose = null; this.frag = [];
    socket.on('data', d => { this.buf = Buffer.concat([this.buf, d]); this.parse(); });
    socket.on('close', () => this.end()); socket.on('error', () => this.end());
  }
  end() { if (!this.open) return; this.open = false; try { this.socket.destroy(); } catch (e) {} this.onclose && this.onclose(); }
  parse() {
    for (;;) {
      const b = this.buf; if (b.length < 2) return;
      const fin = !!(b[0] & 0x80), op = b[0] & 15, masked = !!(b[1] & 0x80);
      let len = b[1] & 127, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (len > 1 << 20) return this.end();
      const need = off + (masked ? 4 : 0) + len; if (b.length < need) return;
      let payload = b.subarray(off + (masked ? 4 : 0), need);
      if (masked) { const m = b.subarray(off, off + 4); payload = Buffer.from(payload); for (let i = 0; i < payload.length; i++) payload[i] ^= m[i & 3]; }
      this.buf = b.subarray(need);
      if (op === 8) return this.end();
      if (op === 9) { this.raw(10, payload); continue; }
      if (op === 10) continue;
      if (op === 0 || op === 1 || op === 2) {
        this.frag.push(payload);
        if (fin) { const msg = Buffer.concat(this.frag).toString('utf8'); this.frag = []; this.onmessage && this.onmessage(msg); }
      }
    }
  }
  raw(op, payload) {
    if (!this.open) return;
    const n = payload.length; let h;
    if (n < 126) h = Buffer.from([0x80 | op, n]);
    else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(n, 2); }
    else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); }
    try { this.socket.write(Buffer.concat([h, payload])); } catch (e) { this.end(); }
  }
  send(obj) { this.raw(1, Buffer.from(JSON.stringify(obj))); }
  ping() { this.raw(9, Buffer.alloc(0)); }
}

/* ───────────── เว็บเซิร์ฟเวอร์ไฟล์นิ่ง ───────────── */
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.png': 'image/png', '.ico': 'image/x-icon', '.json': 'application/json', '.webmanifest': 'application/manifest+json' };
const server = http.createServer((req, res) => {
  let p = decodeURIComponent((req.url || '/').split('?')[0]);
  if (p === '/healthz') { res.writeHead(200); return res.end('ok'); }
  if (p === '/') p = '/index.html';
  const f = path.normalize(path.join(PUBLIC, p));
  if (!f.startsWith(PUBLIC)) { res.writeHead(403); return res.end(); }
  fs.readFile(f, (err, data) => {
    if (err) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('ไม่พบหน้านี้'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
    res.end(data);
  });
});
server.on('upgrade', (req, socket) => {
  if (req.headers.upgrade !== 'websocket' || !req.headers['sec-websocket-key']) return socket.destroy();
  const acc = crypto.createHash('sha1').update(req.headers['sec-websocket-key'] + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  socket.setNoDelay(true);
  onConnect(new Sock(socket));
});

/* ───────────── ข้อมูลบทบาท ───────────── */
const ROLE = {
  villager: { name: 'ชาวบ้าน', team: 'village' },
  wolf: { name: 'หมาป่า', team: 'wolf' },
  seer: { name: 'หมอดู', team: 'village' },
  doctor: { name: 'หมอ', team: 'village' },
  hunter: { name: 'นักล่า', team: 'village' },
  witch: { name: 'แม่มด', team: 'village' },
  cupid: { name: 'คิวปิด', team: 'village' },
  bodyguard: { name: 'บอดี้การ์ด', team: 'village' },
  tanner: { name: 'คนบ้า', team: 'village' },
};
const SPECIALS = ['seer', 'doctor', 'hunter', 'witch', 'cupid', 'bodyguard', 'tanner'];
const MIN_PLAYERS = 4, MAX_PLAYERS = 18;

const rooms = new Map();
const rnd = (a, b) => a + Math.random() * (b - a);
const pick = a => a[Math.floor(Math.random() * a.length)];
const shuffle = a => { a = a.slice(); for (let i = a.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [a[i], a[j]] = [a[j], a[i]]; } return a; };
const uid = () => crypto.randomBytes(8).toString('hex');

function suggest(n) {
  const wolves = Math.max(1, Math.floor(n / 4));
  const sp = { seer: n >= 5, doctor: n >= 6, hunter: n >= 7, witch: n >= 8, cupid: n >= 9, bodyguard: n >= 10, tanner: n >= 11 };
  return { wolves, specials: sp };
}
function defaultSettings() {
  const s = suggest(6);
  return { auto: true, wolves: s.wolves, specials: s.specials, revealOnDeath: true, discussSec: 180, voteMode: 'secret', actionSec: 45, voteSec: 60 };
}

function newRoom() {
  let code; do { code = String(1000 + Math.floor(Math.random() * 9000)); } while (rooms.has(code));
  const r = {
    code, hostToken: uid(), host: null, players: [], settings: defaultSettings(),
    phase: 'lobby', night: 0, say: null, sayId: 0, sayCb: null, sayTimer: null,
    tickFn: null, nt: null, potion: { heal: true, poison: true }, lastD: null, lastB: null,
    lovers: null, votes: {}, tally: null, hunterId: null, hunterQueue: [], winner: null,
    log: [], endsAt: 0, afterHunter: null, lynched: null, touched: Date.now(), msg: '',
  };
  rooms.set(code, r); applyAuto(r); return r;
}
function applyAuto(r) {
  if (!r.settings.auto) return;
  const s = suggest(Math.max(r.players.length, MIN_PLAYERS));
  r.settings.wolves = s.wolves; r.settings.specials = s.specials;
}

/* ───────────── ช่วยเหลือ ───────────── */
const byId = (r, id) => r.players.find(p => p.id === id);
const alive = r => r.players.filter(p => p.alive);
const nm = (r, id) => (byId(r, id) || {}).name || '?';
const roleIn = (r, role) => r.players.some(p => p.role === role);
const logAdd = (r, t) => { r.log.push(t); if (r.log.length > 80) r.log.shift(); };

function say(r, text, opts = {}) {
  r.sayId++; r.say = { id: r.sayId, text, fx: opts.fx || null, silent: !!opts.silent };
  r.sayCb = opts.cb || null;
  clearTimeout(r.sayTimer);
  const est = Math.min(16000, 1800 + text.length * 120);
  const wait = r.host && r.host.open ? est + 9000 : 1200 + text.length * 40;
  if (r.sayCb) r.sayTimer = setTimeout(() => fireSay(r, r.sayId), wait);
  broadcast(r);
}
function fireSay(r, id) {
  if (!r.say || r.say.id !== id || !r.sayCb) return;
  const cb = r.sayCb; r.sayCb = null; clearTimeout(r.sayTimer); safe(r, cb);
}
function safe(r, fn) { try { fn(); } catch (e) { console.error('error', e); } broadcast(r); }
function setTick(r, fn) { r.tickFn = fn; }

/* ───────────── เริ่มเกม / แจกบทบาท ───────────── */
function startGame(r) {
  const n = r.players.length, s = r.settings;
  if (n < MIN_PLAYERS) return r.msg = 'ต้องมีผู้เล่นอย่างน้อย ' + MIN_PLAYERS + ' คน';
  const spec = SPECIALS.filter(k => s.specials[k]);
  if (s.wolves < 1) return r.msg = 'ต้องมีหมาป่าอย่างน้อย 1 ตัว';
  if (s.wolves + spec.length > n) return r.msg = 'บทบาทมากกว่าจำนวนผู้เล่น ลดบทบาทพิเศษหรือหมาป่าลง';
  if (s.wolves * 2 >= n) return r.msg = 'หมาป่ามากเกินไป (ต้องน้อยกว่าครึ่งหนึ่งของผู้เล่น)';
  r.msg = '';
  const roles = shuffle([].concat(Array(s.wolves).fill('wolf'), spec, Array(n - s.wolves - spec.length).fill('villager')));
  r.players.forEach((p, i) => { p.role = roles[i]; p.alive = true; p.ready = false; p.lover = null; });
  Object.assign(r, { night: 0, potion: { heal: true, poison: true }, lastD: null, lastB: null, lovers: null, votes: {}, tally: null,
    winner: null, log: [], hunterQueue: [], nt: null, lynched: null });
  r.phase = 'reveal';
  say(r, 'ยินดีต้อนรับสู่เกมมนุษย์หมาป่า ทุกคนโปรดดูบทบาทของตัวเองบนโทรศัพท์ แล้วกดรับทราบ อย่าให้คนอื่นเห็นนะครับ');
  setTick(r, () => { if (r.players.every(p => p.ready || !p.ws)) { setTick(r, null); startNight(r); } });
}

/* ───────────── กลางคืน ───────────── */
const STEP_TEXT = {
  cupid: ['คิวปิดลืมตา เลือกคู่รักสองคน', 'คิวปิดหลับตา'],
  lovers: ['คู่รักลืมตา มองหน้ากันให้ดี', 'คู่รักหลับตา'],
  bodyguard: ['บอดี้การ์ดลืมตา เลือกคนที่จะปกป้องคืนนี้', 'บอดี้การ์ดหลับตา'],
  doctor: ['หมอลืมตา เลือกคนที่จะช่วยชีวิตคืนนี้', 'หมอหลับตา'],
  seer: ['หมอดูลืมตา เลือกคนที่ต้องการตรวจสอบ', 'หมอดูหลับตา'],
  wolves: ['หมาป่าลืมตา เลือกเหยื่อที่จะฆ่า', 'หมาป่าหลับตา'],
  witch: ['แม่มดลืมตา คืนนี้มีคนถูกทำร้าย แม่มดจะช่วยหรือวางยาใครไหม', 'แม่มดหลับตา'],
};
const STEP_ROLE = { cupid: 'cupid', bodyguard: 'bodyguard', doctor: 'doctor', seer: 'seer', witch: 'witch' };
const STEP_LABEL = { cupid: 'คิวปิด', lovers: 'คู่รัก', bodyguard: 'บอดี้การ์ด', doctor: 'หมอ', seer: 'หมอดู', wolves: 'หมาป่า', witch: 'แม่มด' };

function startNight(r) {
  r.night++; r.phase = 'night'; r.votes = {}; r.tally = null; r.lynched = null;
  const steps = [];
  if (r.night === 1 && roleIn(r, 'cupid')) steps.push('cupid', 'lovers');
  if (roleIn(r, 'bodyguard')) steps.push('bodyguard');
  if (roleIn(r, 'doctor')) steps.push('doctor');
  if (roleIn(r, 'seer')) steps.push('seer');
  steps.push('wolves');
  if (roleIn(r, 'witch')) steps.push('witch');
  r.nt = { steps, i: -1, step: null, protB: null, protD: null, picks: {}, wolfTarget: null, witchSave: false, witchPoison: null, seerRes: null, saw: null };
  logAdd(r, '— คืนที่ ' + r.night + ' —');
  say(r, 'ค่ำคืนมาเยือนหมู่บ้าน ทุกคนหลับตา และเงียบเสียงลง', { fx: 'night', cb: () => nextStep(r) });
}
function nextStep(r) {
  const nt = r.nt; nt.i++; setTick(r, null);
  if (nt.i >= nt.steps.length) return dawn(r);
  const key = nt.steps[nt.i];
  nt.step = { key, stage: 'announce', actors: [], done: {}, minUntil: 0, deadline: 0 };
  say(r, STEP_TEXT[key][0], { fx: key === 'wolves' ? 'howl' : 'wake', cb: () => beginAct(r) });
}
function stepActors(r, key) {
  if (key === 'wolves') return alive(r).filter(p => p.role === 'wolf');
  if (key === 'lovers') return [];
  return alive(r).filter(p => p.role === STEP_ROLE[key]);
}
function beginAct(r) {
  const st = r.nt.step; st.stage = 'act';
  st.actors = stepActors(r, st.key).map(p => p.id);
  const now = Date.now();
  st.minUntil = now + F * (st.key === 'lovers' ? 8000 : rnd(6500, 10000));
  st.deadline = now + F * r.settings.actionSec * 1000;
  r.endsAt = st.deadline;
  if (st.key === 'witch') r.nt.wolfTarget = wolfTarget(r);
  setTick(r, () => {
    const now = Date.now(), s = r.nt.step;
    const finished = s.actors.every(id => s.done[id]);
    if (now >= s.minUntil && (finished || now >= s.deadline)) finishStep(r);
  });
}
function wolfTarget(r) {
  const nt = r.nt, w = alive(r).filter(p => p.role === 'wolf');
  const c = {}; w.forEach(p => { const t = nt.picks[p.id]; if (t) c[t] = (c[t] || 0) + 1; });
  const m = Math.max(0, ...Object.values(c)); if (!m) return null;
  return pick(Object.keys(c).filter(k => c[k] === m));
}
function finishStep(r) {
  setTick(r, null); const nt = r.nt, st = nt.step; st.stage = 'close'; r.endsAt = 0;
  let text = STEP_TEXT[st.key][1];
  if (st.key === 'wolves') {
    nt.wolfTarget = wolfTarget(r);
    if (nt.wolfTarget) { text = 'หมาป่าได้เลือกเหยื่อแล้ว หมาป่าหลับตา'; logAdd(r, 'หมาป่าเลือกฆ่า ' + nm(r, nt.wolfTarget)); }
    else logAdd(r, 'หมาป่าไม่ได้เลือกใคร');
  }
  say(r, text, { fx: 'sleep', cb: () => nextStep(r) });
}

function actNight(r, p, m) {
  const nt = r.nt, st = nt && nt.step;
  if (r.phase !== 'night' || !st || st.stage !== 'act' || !p.alive || !st.actors.includes(p.id)) return;
  const A = alive(r), valid = id => A.some(x => x.id === id);
  const k = st.key;
  if (k === 'cupid') {
    const t = (m.targets || []).filter(valid);
    if (new Set(t).size !== 2 || st.done[p.id]) return;
    r.lovers = t.slice(); byId(r, t[0]).lover = t[1]; byId(r, t[1]).lover = t[0];
    st.done[p.id] = 1; logAdd(r, 'คิวปิดจับคู่ ' + nm(r, t[0]) + ' และ ' + nm(r, t[1]));
  } else if (k === 'bodyguard') {
    if (!valid(m.target) || m.target === p.id || m.target === r.lastB || st.done[p.id]) return;
    nt.protB = m.target; st.done[p.id] = 1; logAdd(r, 'บอดี้การ์ดปกป้อง ' + nm(r, m.target));
  } else if (k === 'doctor') {
    if (!valid(m.target) || m.target === r.lastD || st.done[p.id]) return;
    nt.protD = m.target; st.done[p.id] = 1; logAdd(r, 'หมอช่วย ' + nm(r, m.target));
  } else if (k === 'seer') {
    if (m.ack) { if (nt.seerRes) st.done[p.id] = 1; return; }
    if (!valid(m.target) || m.target === p.id || nt.seerRes) return;
    const t = byId(r, m.target); nt.seerRes = { id: t.id, wolf: t.role === 'wolf' };
    logAdd(r, 'หมอดูตรวจ ' + t.name + (t.role === 'wolf' ? ' (เป็นหมาป่า)' : ' (ไม่ใช่หมาป่า)'));
  } else if (k === 'wolves') {
    if (!valid(m.target) || byId(r, m.target).role === 'wolf') return;
    nt.picks[p.id] = m.target;
    const w = A.filter(x => x.role === 'wolf');
    w.forEach(x => delete st.done[x.id]);
    if (w.every(x => nt.picks[x.id]) && new Set(w.map(x => nt.picks[x.id])).size === 1) w.forEach(x => st.done[x.id] = 1);
  } else if (k === 'witch') {
    if (st.done[p.id]) return;
    if (m.save && r.potion.heal && nt.wolfTarget) { nt.witchSave = true; r.potion.heal = false; logAdd(r, 'แม่มดช่วย ' + nm(r, nt.wolfTarget)); }
    if (m.poison && r.potion.poison && valid(m.poison) && m.poison !== p.id) { nt.witchPoison = m.poison; r.potion.poison = false; logAdd(r, 'แม่มดวางยา ' + nm(r, m.poison)); }
    st.done[p.id] = 1;
  }
}

/* ───────────── รุ่งสาง / การตาย ───────────── */
function killPlayers(r, ids) {
  const dead = []; const q = ids.slice();
  while (q.length) {
    const p = byId(r, q.shift()); if (!p || !p.alive) continue;
    p.alive = false; dead.push(p);
    if (p.role === 'hunter') r.hunterQueue.push(p.id);
    if (p.lover) { const l = byId(r, p.lover); if (l && l.alive) q.push(l.id); }
  }
  return dead;
}
function roleSuffix(r, p) { return r.settings.revealOnDeath ? ' ซึ่งเป็น' + ROLE[p.role].name : ''; }

function dawn(r) {
  const nt = r.nt; let victim = nt.wolfTarget, sacrificed = null;
  r.lastD = nt.protD; r.lastB = nt.protB;
  if (victim) {
    if (nt.witchSave || nt.protD === victim) victim = null;
    else if (nt.protB === victim) { sacrificed = nt.protB; victim = null; }
  }
  const kills = [];
  if (victim) kills.push(victim);
  if (sacrificed) { const bg = alive(r).find(p => p.role === 'bodyguard'); if (bg) kills.push(bg.id); }
  if (nt.witchPoison) kills.push(nt.witchPoison);
  const dead = killPlayers(r, [...new Set(kills)]);
  r.phase = 'dawn'; r.nt = null;
  dead.forEach(p => logAdd(r, p.name + ' ตาย (' + ROLE[p.role].name + ')'));
  let text = 'ฟ้าสางแล้ว ทุกคนลืมตาได้';
  text += dead.length ? ' เมื่อคืนมีผู้เสียชีวิต ' + dead.length + ' คน คือ ' + dead.map(p => p.name + roleSuffix(r, p)).join(' และ ') : ' เมื่อคืนไม่มีใครเสียชีวิต';
  say(r, text, { fx: 'dawn', cb: () => processHunters(r, () => afterDeaths(r, () => startDiscuss(r))) });
}
function afterDeaths(r, next) {
  const w = checkWin(r); if (w) return endGame(r, w);
  next();
}
function checkWin(r) {
  const A = alive(r), wolves = A.filter(p => p.role === 'wolf').length, others = A.length - wolves;
  if (A.length === 2 && A[0].lover === A[1].id && (A[0].role === 'wolf') !== (A[1].role === 'wolf')) return 'lovers';
  if (wolves === 0) return 'village';
  if (wolves >= others) return 'wolves';
  return null;
}
function processHunters(r, next) {
  r.hunterNext = next;
  while (r.hunterQueue.length) {
    const id = r.hunterQueue.shift(), h = byId(r, id);
    if (!h || alive(r).length < 2) continue;
    r.phase = 'hunter'; r.hunterId = id; r.endsAt = Date.now() + F * 30000;
    say(r, 'นักล่า ' + h.name + ' ถูกกำจัดแล้ว แต่ก่อนสิ้นลม นักล่าเลือกยิงใครสักหนึ่งคนได้', { fx: 'shot', cb: () => {
      setTick(r, () => { if (Date.now() > r.endsAt) { setTick(r, null); r.endsAt = 0; say(r, 'นักล่าตัดสินใจไม่ยิงใคร', { cb: () => processHunters(r, next) }); } });
    } });
    return;
  }
  r.hunterId = null; next();
}
function shoot(r, p, target) {
  if (r.phase !== 'hunter' || r.hunterId !== p.id) return;
  const t = byId(r, target); if (!t || !t.alive) return;
  setTick(r, null); r.endsAt = 0;
  const dead = killPlayers(r, [t.id]); logAdd(r, 'นักล่ายิง ' + t.name);
  say(r, 'นักล่ายิง ' + dead.map(d => d.name + roleSuffix(r, d)).join(' และ '), { fx: 'shot', cb: () => processHunters(r, r.hunterNext || (() => {})) });
}

/* ───────────── กลางวัน ───────────── */
function startDiscuss(r) {
  r.phase = 'discuss'; r.endsAt = Date.now() + F * r.settings.discussSec * 1000;
  const mins = r.settings.discussSec / 60;
  say(r, 'ถึงเวลาอภิปราย ทุกคนมีเวลา ' + (mins >= 1 ? Math.round(mins * 10) / 10 + ' นาที' : r.settings.discussSec + ' วินาที') + ' ผลัดกันพูด หาว่าใครคือหมาป่า');
  const warned = {};
  setTick(r, () => {
    const left = (r.endsAt - Date.now()) / 1000;
    if (left <= 0) { setTick(r, null); startVote(r); }
    else if (r.settings.discussSec > 90 && left < 60 && !warned.a) { warned.a = 1; say(r, 'เหลือเวลาอีกหนึ่งนาที', { silent: false }); }
    else if (r.settings.discussSec > 45 && left < 15 && !warned.b) { warned.b = 1; say(r, 'เหลือเวลาอีกสิบห้าวินาที'); }
  });
}
function startVote(r) {
  r.votes = {}; r.tally = null; r.endsAt = 0;
  if (r.settings.voteMode === 'point') {
    r.phase = 'point';
    say(r, 'หมดเวลาอภิปราย ถึงเวลาโหวต เมื่อนับสาม ให้ทุกคนชี้คนที่สงสัยพร้อมกัน หนึ่ง สอง สาม ชี้', { fx: 'drum' });
  } else {
    r.phase = 'vote'; r.endsAt = Date.now() + F * r.settings.voteSec * 1000;
    say(r, 'หมดเวลาอภิปราย ถึงเวลาโหวต ทุกคนเลือกคนที่สงสัยบนโทรศัพท์ของตัวเอง ภายใน ' + r.settings.voteSec + ' วินาที', { fx: 'drum' });
    setTick(r, () => { if (alive(r).every(p => r.votes[p.id] || !p.ws) || Date.now() > r.endsAt) { setTick(r, null); finishVote(r); } });
  }
}
function finishVote(r) {
  const c = {}; const voters = {};
  alive(r).forEach(p => { const t = r.votes[p.id]; if (t && t !== 'skip') { c[t] = (c[t] || 0) + 1; (voters[t] = voters[t] || []).push(p.name); } });
  const m = Math.max(0, ...Object.values(c)); const top = Object.keys(c).filter(k => c[k] === m);
  const out = m && top.length === 1 ? top[0] : null;
  r.tally = { counts: c, voters, skipped: alive(r).filter(p => r.votes[p.id] === 'skip' || !r.votes[p.id]).map(p => p.name), out };
  r.phase = 'voteResult'; r.endsAt = 0;
  const sum = Object.keys(c).sort((a, b) => c[b] - c[a]).map(k => nm(r, k) + ' ' + c[k] + ' คะแนน').join(' ');
  const res = out ? nm(r, out) + ' ถูกโหวตออกจากหมู่บ้าน' : (m ? 'คะแนนเสมอกัน ไม่มีใครถูกโหวตออก' : 'ไม่มีใครถูกโหวต ไม่มีใครถูกโหวตออก');
  say(r, (sum ? 'ผลโหวต ' + sum + ' ' : '') + res, { cb: () => lynch(r, out) });
}
function lynch(r, id) {
  r.tally = r.tally || null;
  if (!id) return processHunters(r, () => afterDeaths(r, () => startNight(r)));
  const p = byId(r, id), dead = killPlayers(r, [id]);
  logAdd(r, p.name + ' ถูกโหวตออก (' + ROLE[p.role].name + ')');
  r.lynched = id; r.phase = 'lynch';
  let text = dead.map(d => d.name + roleSuffix(r, d)).join(' และ ') + ' ออกจากเกม';
  if (p.role === 'tanner') text = p.name + ' เป็นคนบ้า และดีใจที่ถูกโหวตออก';
  say(r, text, { fx: 'dead', cb: () => {
    if (p.role === 'tanner') return endGame(r, 'tanner');
    processHunters(r, () => afterDeaths(r, () => startNight(r)));
  } });
}
function endGame(r, w) {
  r.phase = 'ended'; r.winner = w; r.endsAt = 0; setTick(r, null);
  const t = { village: 'ชาวบ้านชนะ หมาป่าถูกกำจัดหมดแล้ว', wolves: 'หมาป่าชนะ หมู่บ้านถูกยึดครอง', lovers: 'คู่รักชนะ ความรักเอาชนะทุกสิ่ง', tanner: 'คนบ้าชนะ' }[w];
  say(r, 'เกมจบแล้ว ' + t, { fx: w === 'wolves' ? 'howl' : 'dawn' });
}
// ให้ hunter ที่ตายตอนถูกโหวต/ยิง ทำงานต่อได้ถูกต้อง
function resetToLobby(r) {
  r.phase = 'lobby'; setTick(r, null); r.say = null; r.sayCb = null; clearTimeout(r.sayTimer);
  r.players.forEach(p => { p.role = null; p.alive = true; p.ready = false; p.lover = null; });
  r.nt = null; r.winner = null; r.tally = null; r.votes = {}; r.hunterQueue = []; r.endsAt = 0; applyAuto(r);
}

/* ───────────── มุมมองของแต่ละคน ───────────── */
function revealed(r, p) { return r.phase === 'ended' || (!p.alive && r.settings.revealOnDeath); }
function pubPlayers(r, viewer) {
  const spec = viewer && !viewer.alive || r.phase === 'ended';
  return r.players.map(p => ({ id: p.id, name: p.name, alive: p.alive, conn: !!p.ws, role: (revealed(r, p) || spec) ? p.role : undefined,
    ready: r.phase === 'reveal' ? !!p.ready : undefined, wolf: undefined }));
}
function hostView(r) {
  const nt = r.nt, st = nt && nt.step;
  return {
    t: 'state', who: 'host', code: r.code, phase: r.phase, night: r.night, settings: r.settings, msg: r.msg,
    players: pubPlayers(r, null), say: r.say, endsAt: r.endsAt, winner: r.winner, tally: r.tally,
    step: st ? { key: st.key, label: STEP_LABEL[st.key], stage: st.stage } : null,
    voted: r.phase === 'vote' ? Object.keys(r.votes).length : 0, aliveCount: alive(r).length,
    hunter: r.hunterId ? nm(r, r.hunterId) : null, lynched: r.lynched, potion: undefined,
    suggest: suggest(Math.max(r.players.length, MIN_PLAYERS)), min: MIN_PLAYERS, max: MAX_PLAYERS,
    roles: r.phase === 'ended' ? r.players.map(p => ({ id: p.id, role: p.role, lover: p.lover })) : undefined,
  };
}
function playerView(r, p) {
  const nt = r.nt, st = nt && nt.step;
  const A = alive(r);
  const v = {
    t: 'state', who: 'player', code: r.code, me: { id: p.id, name: p.name, role: p.role, alive: p.alive, lover: p.lover ? nm(r, p.lover) : null },
    phase: r.phase, night: r.night, players: pubPlayers(r, p), endsAt: r.endsAt, winner: r.winner, say: r.say && !r.say.silent ? { id: r.say.id, text: r.say.text } : null,
    tally: r.tally, settings: { revealOnDeath: r.settings.revealOnDeath, voteMode: r.settings.voteMode }, hunterId: r.hunterId, lynched: r.lynched,
  };
  if (p.role === 'wolf') v.me.mates = r.players.filter(x => x.role === 'wolf' && x.id !== p.id).map(x => ({ id: x.id, name: x.name }));
  if (r.phase === 'ended') v.roles = r.players.map(x => ({ id: x.id, role: x.role }));
  if (!p.alive && r.phase !== 'lobby') { v.spectate = { log: r.log.slice(-40) }; }
  if (r.phase === 'reveal') v.me.ready = !!p.ready;
  // คำสั่งให้ทำอะไร
  const opts = f => A.filter(f).map(x => ({ id: x.id, name: x.name }));
  if (p.alive && r.phase === 'night' && st && st.stage === 'act' && st.actors.includes(p.id)) {
    const k = st.key, done = !!st.done[p.id];
    if (k === 'cupid') v.act = { kind: 'cupid', done, options: opts(() => true) };
    else if (k === 'bodyguard') v.act = { kind: 'bodyguard', done, options: opts(x => x.id !== p.id && x.id !== r.lastB && !(done)), last: r.lastB ? nm(r, r.lastB) : null };
    else if (k === 'doctor') v.act = { kind: 'doctor', done, options: opts(x => x.id !== r.lastD), last: r.lastD ? nm(r, r.lastD) : null };
    else if (k === 'seer') v.act = { kind: 'seer', done, options: opts(x => x.id !== p.id), result: nt.seerRes ? { name: nm(r, nt.seerRes.id), wolf: nt.seerRes.wolf } : null };
    else if (k === 'wolves') v.act = { kind: 'wolves', done, options: opts(x => x.role !== 'wolf'), picks: Object.fromEntries(Object.entries(nt.picks).filter(([id]) => byId(r, id) && byId(r, id).alive)), mates: r.players.filter(x => x.role === 'wolf' && x.alive).map(x => ({ id: x.id, name: x.name })) };
    else if (k === 'witch') v.act = { kind: 'witch', done, victim: nt.wolfTarget ? { id: nt.wolfTarget, name: nm(r, nt.wolfTarget) } : null, potion: r.potion, options: opts(x => x.id !== p.id) };
  } else if (p.alive && r.phase === 'night' && st && st.key === 'lovers' && st.stage === 'act' && p.lover) {
    v.act = { kind: 'lovers', partner: nm(r, p.lover) };
  }
  if (p.alive && r.phase === 'vote') v.act = { kind: 'vote', options: opts(x => x.id !== p.id), mine: r.votes[p.id] || null, count: Object.keys(r.votes).length, total: A.length };
  if (p.alive && r.phase === 'point') v.act = { kind: 'point' };
  if (p.alive && r.phase === 'hunter' && r.hunterId === p.id) v.act = { kind: 'hunter', options: opts(x => x.id !== p.id) };
  if (!p.alive && r.phase === 'hunter' && r.hunterId === p.id) v.act = { kind: 'hunter', options: opts(() => true) };
  if (r.phase === 'night' && st) v.awake = st.key;
  return v;
}

function push(ws, obj) {
  const k = JSON.stringify(obj); if (ws.last === k) return; ws.last = k;
  obj.now = Date.now(); ws.send(obj);
}
function broadcast(r) {
  r.touched = Date.now();
  if (r.host && r.host.open) push(r.host, hostView(r));
  r.players.forEach(p => { if (p.ws && p.ws.open) push(p.ws, playerView(r, p)); });
}

/* ───────────── การเชื่อมต่อ ───────────── */
function onConnect(ws) {
  let room = null, player = null, isHost = false;
  const err = m => ws.send({ t: 'error', msg: m });
  ws.onclose = () => {
    if (!room) return;
    if (isHost && room.host === ws) room.host = null;
    if (player && player.ws === ws) player.ws = null;
    broadcast(room);
  };
  const alivePing = setInterval(() => ws.open ? ws.ping() : clearInterval(alivePing), 25000);
  ws.onmessage = raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    try { handle(m); } catch (e) { console.error(e); }
  };
  function handle(m) {
    if (m.t === 'create') {
      room = newRoom(); isHost = true; room.host = ws;
      ws.send({ t: 'hello', role: 'host', code: room.code, token: room.hostToken }); return broadcast(room);
    }
    if (m.t === 'hostResume') {
      const r = rooms.get(String(m.code));
      if (!r || r.hostToken !== m.token) return ws.send({ t: 'gone' });
      room = r; isHost = true; r.host = ws; ws.send({ t: 'hello', role: 'host', code: r.code, token: r.hostToken }); return broadcast(r);
    }
    if (m.t === 'join') {
      const r = rooms.get(String(m.code || '').trim());
      if (!r) return err('ไม่พบห้องนี้ ตรวจรหัสอีกครั้ง');
      let p = m.token && r.players.find(x => x.token === m.token);
      if (p) { if (p.ws && p.ws !== ws) p.ws.end(); p.ws = ws; }
      else {
        const name = String(m.name || '').trim().slice(0, 14);
        if (!name) return err('กรุณาใส่ชื่อ');
        if (r.phase !== 'lobby') return err('เกมเริ่มไปแล้ว เข้าห้องไม่ได้');
        if (r.players.length >= MAX_PLAYERS) return err('ห้องเต็มแล้ว');
        if (r.players.some(x => x.name.toLowerCase() === name.toLowerCase())) return err('ชื่อนี้มีคนใช้แล้ว');
        p = { id: uid(), name, token: uid(), ws, role: null, alive: true, ready: false, lover: null }; r.players.push(p); applyAuto(r);
      }
      room = r; player = p; ws.send({ t: 'hello', role: 'player', code: r.code, token: p.token, id: p.id }); return broadcast(r);
    }
    if (!room) return;
    const r = room;
    if (isHost) {
      switch (m.t) {
        case 'settings': {
          if (r.phase !== 'lobby') return; const s = m.settings || {}, c = r.settings;
          if (typeof s.auto === 'boolean') c.auto = s.auto;
          if (s.auto === true) applyAuto(r);
          else {
            if (Number.isInteger(s.wolves)) c.wolves = Math.max(1, Math.min(6, s.wolves));
            if (s.specials) SPECIALS.forEach(k => { if (typeof s.specials[k] === 'boolean') c.specials[k] = s.specials[k]; });
          }
          if (typeof s.revealOnDeath === 'boolean') c.revealOnDeath = s.revealOnDeath;
          if (['secret', 'point'].includes(s.voteMode)) c.voteMode = s.voteMode;
          if (Number.isFinite(s.discussSec)) c.discussSec = Math.max(30, Math.min(900, s.discussSec | 0));
          if (Number.isFinite(s.actionSec)) c.actionSec = Math.max(15, Math.min(120, s.actionSec | 0));
          return broadcast(r);
        }
        case 'start': if (r.phase === 'lobby') { startGame(r); broadcast(r); } return;
        case 'kick': if (r.phase === 'lobby') { const p = byId(r, m.id); if (p) { p.ws && p.ws.send({ t: 'kicked' }); r.players = r.players.filter(x => x !== p); applyAuto(r); broadcast(r); } } return;
        case 'sayDone': return fireSay(r, m.id), broadcast(r);
        case 'skip':
          if (r.phase === 'discuss') { setTick(r, null); startVote(r); }
          else if (r.phase === 'reveal') { setTick(r, null); startNight(r); }
          return broadcast(r);
        case 'finishVote': if (r.phase === 'vote') { setTick(r, null); finishVote(r); } return broadcast(r);
        case 'pointResult':
          if (r.phase === 'point') {
            const out = m.target && byId(r, m.target) && byId(r, m.target).alive ? m.target : null;
            r.tally = { counts: {}, voters: {}, skipped: [], out }; r.phase = 'voteResult';
            say(r, out ? nm(r, out) + ' ถูกชี้มากที่สุด และถูกโหวตออกจากหมู่บ้าน' : 'ไม่มีใครถูกโหวตออก', { cb: () => lynch(r, out) });
          } return broadcast(r);
        case 'again': if (r.phase === 'ended') { resetToLobby(r); broadcast(r); } return;
        case 'closeGame': resetToLobby(r); return broadcast(r);
      }
      return;
    }
    if (!player) return;
    switch (m.t) {
      case 'ready': if (r.phase === 'reveal') { player.ready = true; } return broadcast(r);
      case 'act': actNight(r, player, m); return broadcast(r);
      case 'ack': actNight(r, player, { ack: 1 }); return broadcast(r);
      case 'vote':
        if (r.phase === 'vote' && player.alive) {
          if (m.target === 'skip' || (byId(r, m.target) && byId(r, m.target).alive && m.target !== player.id)) r.votes[player.id] = m.target;
        } return broadcast(r);
      case 'shoot': shoot(r, player, m.target); return broadcast(r);
      case 'leave': if (r.phase === 'lobby') { r.players = r.players.filter(x => x !== player); applyAuto(r); broadcast(r); } return;
    }
  }
}

setInterval(() => {
  const now = Date.now();
  rooms.forEach((r, code) => {
    if (r.tickFn) safe(r, r.tickFn);
    if (now - r.touched > 8 * 3600e3) rooms.delete(code);
  });
}, 400);

server.listen(PORT, () => console.log('Werewolf server on :' + PORT));
