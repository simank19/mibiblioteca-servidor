/* =====================================================================
   MiBiblioteca · Backend de aceleración y puente MEGA  (server.js)
   ---------------------------------------------------------------------
   - Descarga de MEGA en streams directo a disco (nunca carga un RAR entero en RAM).
   - Abre RAR (node-unrar-js desde archivo) y ZIP (yauzl, acceso aleatorio) desde disco.
   - Lee la ficha de cada EPUB (título, autor, serie, idioma…) y genera la portada (sharp).
   - Caché en disco con límite de tamaño (se borra lo menos usado).
   - Endpoints livianos para la app:
       GET /health
       GET /api/session                 → clave para pedir portadas (24 h)
       GET /api/pack?rp=RUTA.rar         → libros de un autor con ficha y portada
       GET /api/book?rp=RUTA&n=ENTRADA   → el libro (EPUB/PDF) en stream, con soporte de rangos
           ?fp=RUTA (archivo suelto) · ?zp=RUTA.zip&n=ENTRADA · ?ml=ENLACE_MEGA (subidos por ti)
       GET /api/cover?rp=&n=&k=CLAVE     → portada JPG (cacheable, para <img>)
       POST /api/prefetch {rps:[…]}      → prepara autores en segundo plano
   - Solo entra tu cuenta (token de Firebase) o un visitante que aprobaste (bp_access).
   ===================================================================== */
import http from 'node:http';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';

/* ----------------------------- Configuración ----------------------------- */
const env = process.env;
const CFG = {
  port: +(env.PORT || 8787),
  origins: (env.ALLOWED_ORIGINS || 'https://bibliopub.netlify.app').split(',').map(s => s.trim()).filter(Boolean),
  allowedEmail: (env.ALLOWED_EMAIL || 'brandonsimanca@gmail.com').toLowerCase(),
  projectId: env.FIREBASE_PROJECT_ID || 'bibliopub-854f4',
  megaFolderUrl: env.MEGA_FOLDER_URL || '',          // si no está, se lee de Firestore (bp_config/mega.link)
  megaEmail: env.MEGA_EMAIL || '', megaPassword: env.MEGA_PASSWORD || '', // opcional: descargar con tu cuenta (más cuota que anónimo)
  localDir: env.LOCAL_LIBRARY_DIR || '',             // alternativa a MEGA: una carpeta en este servidor
  cacheDir: path.resolve(env.CACHE_DIR || './cache'),
  cacheMaxMB: +(env.CACHE_MAX_MB || 2048),
  maxDownloads: +(env.MAX_DOWNLOADS || 3),
  treeRefreshMin: +(env.TREE_REFRESH_MIN || 30),
  secret: env.COVER_SECRET || crypto.randomBytes(32).toString('hex'),
  authDisabled: env.AUTH_DISABLED === '1'             // SOLO para pruebas locales
};
const DIRS = ['dl', 'books', 'covers', 'packs', 'tmp'].reduce((o, d) => (o[d] = path.join(CFG.cacheDir, d), o), {});
for (const d of Object.values(DIRS)) fs.mkdirSync(d, { recursive: true });
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

/* ----------------------------- Utilidades ----------------------------- */
// Mismo id que la app (FNV-1a en dos mitades → 16 hex): así las portadas y fichas coinciden
function hashId(str) {
  let h1 = 0x811c9dc5, h2 = 0x01000193 ^ 0x9e3779b9;
  for (let i = 0; i < str.length; i++) { const c = str.charCodeAt(i); h1 = Math.imul(h1 ^ c, 0x01000193) >>> 0; h2 = Math.imul(h2 ^ c, 0x5bd1e995) >>> 0; }
  return h1.toString(16).padStart(8, '0') + h2.toString(16).padStart(8, '0');
}
const sha = s => crypto.createHash('sha1').update(s).digest('hex');
const BOOK_RX = /\.(epub|pdf)$/i;
const fmtOf = n => /\.pdf$/i.test(n || '') ? 'pdf' : 'epub';
function guessMeta(p) { // igual que la app
  const parts = p.split('/').filter(Boolean);
  const file = (parts.pop() || '').replace(BOOK_RX, '');
  const clean = s => s.replace(/\s*\(\d+\)\s*$/, '').replace(/_+/g, ' ').trim();
  if (parts.length >= 2 && /\(\d+\)$/.test(parts[parts.length - 1])) return { t: clean(parts[parts.length - 1]), a: clean(parts[parts.length - 2]) };
  const m = file.match(/^(.+?)\s+-\s+(.+)$/);
  if (m) return { a: clean(m[1]), t: clean(m[2]) };
  return { t: clean(file), a: '' };
}
// Semáforo: como mucho N descargas de MEGA a la vez
function limiter(n) { let active = 0; const q = []; const next = () => { if (active >= n || !q.length) return; active++; const { fn, res, rej } = q.shift(); fn().then(res, rej).finally(() => { active--; next(); }); }; return fn => new Promise((res, rej) => { q.push({ fn, res, rej }); next(); }); }
const dlLimit = limiter(CFG.maxDownloads);
// Si dos pedidos piden lo mismo a la vez, se hace una sola vez
const inflight = new Map();
const once = (key, fn) => { if (inflight.has(key)) return inflight.get(key); const p = fn().finally(() => inflight.delete(key)); inflight.set(key, p); return p; };
const exists = f => fsp.stat(f).then(s => s, () => null);
const touch = f => fsp.utimes(f, new Date(), new Date()).catch(() => {});

/* ----------------------------- Firebase (auth + config) ----------------------------- */
let admin = null, firestore = null;
async function initFirebase() {
  try {
    const mod = await import('firebase-admin'); admin = mod.default || mod;
    const opts = { projectId: CFG.projectId };
    if (env.FIREBASE_SERVICE_ACCOUNT) opts.credential = admin.credential.cert(JSON.parse(env.FIREBASE_SERVICE_ACCOUNT));
    else if (env.GOOGLE_APPLICATION_CREDENTIALS) opts.credential = admin.credential.applicationDefault();
    admin.initializeApp(opts);
    if (opts.credential) firestore = admin.firestore();
    log('Firebase listo', firestore ? '(con acceso a Firestore)' : '(solo verificación de sesión)');
  } catch (e) { log('Firebase no disponible:', e.message); }
}
const guestCache = new Map();
async function authUser(req) {
  if (CFG.authDisabled) return { uid: 'local', email: CFG.allowedEmail };
  const h = req.headers.authorization || '';
  const token = h.startsWith('Bearer ') ? h.slice(7) : '';
  if (!token || !admin) throw Object.assign(new Error('Falta la sesión'), { status: 401 });
  const dec = await admin.auth().verifyIdToken(token).catch(() => { throw Object.assign(new Error('Sesión no válida'), { status: 401 }); });
  if ((dec.email || '').toLowerCase() === CFG.allowedEmail) return dec;
  // visitante aprobado por ti (bp_access/{uid}); se revisa cada 5 min
  if (firestore) {
    const c = guestCache.get(dec.uid);
    if (c && Date.now() - c.at < 300000) { if (c.ok) return dec; }
    else {
      const ok = (await firestore.collection('bp_access').doc(dec.uid).get()).exists;
      guestCache.set(dec.uid, { ok, at: Date.now() }); if (ok) return dec;
    }
  }
  throw Object.assign(new Error('Sin acceso'), { status: 403 });
}
// Clave para portadas (<img> no puede mandar el token): HMAC del usuario y el día
const dayKey = (uid, d = Math.floor(Date.now() / 864e5)) => crypto.createHmac('sha256', CFG.secret).update(uid + ':' + d).digest('base64url').slice(0, 22) + '.' + Buffer.from(uid).toString('base64url');
function checkCoverKey(k) {
  if (CFG.authDisabled) return true;
  const [, u] = String(k || '').split('.'); if (!u) return false;
  const uid = Buffer.from(u, 'base64url').toString(), d = Math.floor(Date.now() / 864e5);
  return k === dayKey(uid, d) || k === dayKey(uid, d - 1);
}

/* ----------------------------- Origen de los archivos ----------------------------- */
// Árbol: ruta (con la carpeta raíz, igual que la app) → { size, open(): stream }
let tree = new Map(), treeAt = 0, treeLoading = null, MegaFile = null, megaAcct = null;
async function megaAccount() { // con tu cuenta de MEGA la cuota de descarga es la de tu plan (no la anónima por IP)
  if (!CFG.megaEmail || !CFG.megaPassword) return null;
  if (megaAcct) return megaAcct;
  const { Storage } = await import('megajs');
  megaAcct = await new Storage({ email: CFG.megaEmail, password: CFG.megaPassword, keepalive: false, autoload: false }).ready;
  log('Sesión de MEGA iniciada con tu cuenta'); return megaAcct;
}
async function megaLink() {
  if (CFG.megaFolderUrl) return CFG.megaFolderUrl;
  if (firestore) { const d = await firestore.collection('bp_config').doc('mega').get(); if (d.exists && d.data().link) return d.data().link; }
  throw new Error('Configura MEGA_FOLDER_URL (o credenciales de Firebase para leerlo de tu cuenta)');
}
async function loadTree(force) {
  if (!force && tree.size && Date.now() - treeAt < CFG.treeRefreshMin * 60000) return tree;
  return treeLoading ||= (async () => {
    const t = new Map();
    if (CFG.localDir) {
      const root = path.resolve(CFG.localDir), rootName = path.basename(root);
      const walk = async (dir, rel) => { for (const e of await fsp.readdir(dir, { withFileTypes: true })) { const full = path.join(dir, e.name), r = rel + '/' + e.name; if (e.isDirectory()) await walk(full, r); else { const st = await fsp.stat(full); t.set(r, { size: st.size, open: (o) => fs.createReadStream(full, o) }); } } };
      await walk(root, rootName);
    } else {
      MegaFile ||= (await import('megajs')).File;
      const root = MegaFile.fromURL(await megaLink());
      await root.loadAttributes();
      const acct = await megaAccount().catch(e => { log('MEGA con cuenta falló, se usa acceso anónimo:', e.message); return null; });
      (function walk(n, base) { const p = base ? base + '/' + (n.name ?? '(sin nombre)') : (n.name ?? ''); if (n.directory) (n.children || []).forEach(c => walk(c, p)); else { if (acct) n.api = acct.api; t.set(p, { size: n.size, open: () => n.download({}) }); } })(root, '');
    }
    tree = t; treeAt = Date.now(); log(`Árbol cargado: ${t.size} archivos`);
    return t;
  })().finally(() => { treeLoading = null; });
}
async function node(p) {
  let n = (await loadTree()).get(p);
  if (!n) n = (await loadTree(true)).get(p); // pudo haberse agregado después
  if (!n) throw Object.assign(new Error('No existe en la biblioteca: ' + p), { status: 404 });
  return n;
}
// Baja un archivo de la biblioteca a disco (stream → archivo temporal → renombrar). Devuelve la ruta local.
function ensureLocal(p) {
  const ext = path.extname(p).toLowerCase() || '.bin', dest = path.join(DIRS.dl, sha(p) + ext);
  return once('dl:' + p, async () => {
    if (await exists(dest)) { touch(dest); return dest; }
    const n = await node(p);
    if (CFG.localDir) return dest; // (se lee directo abajo)
    await dlLimit(async () => {
      const tmp = path.join(DIRS.tmp, crypto.randomUUID());
      const t0 = Date.now();
      try { await pipeline(n.open(), fs.createWriteStream(tmp)); await fsp.rename(tmp, dest); }
      catch (e) { await fsp.rm(tmp, { force: true }); throw e; }
      log(`MEGA → disco ${p} (${(n.size / 1048576).toFixed(1)} MB en ${((Date.now() - t0) / 1000).toFixed(1)} s)`);
    });
    gcSoon();
    return dest;
  });
}
async function localPath(p) { // ruta en disco del archivo de la biblioteca
  if (CFG.localDir) { const root = path.resolve(CFG.localDir); const full = path.resolve(path.dirname(root), p); if (!full.startsWith(root)) throw Object.assign(new Error('Ruta no válida'), { status: 400 }); await node(p); return full; }
  return ensureLocal(p);
}

/* ----------------------------- RAR y ZIP desde disco ----------------------------- */
let unrar = null;
async function rarList(file) {
  unrar ||= await import('node-unrar-js');
  const ex = await unrar.createExtractorFromFile({ filepath: file });
  return [...ex.getFileList().fileHeaders].filter(h => !h.flags.directory).map(h => ({ name: h.name, size: h.unpSize }));
}
async function rarExtractTo(file, name, outFile) { // extrae UNA entrada a un archivo (sin cargar el RAR en memoria)
  unrar ||= await import('node-unrar-js');
  const dir = path.join(DIRS.tmp, crypto.randomUUID()); await fsp.mkdir(dir, { recursive: true });
  try {
    const ex = await unrar.createExtractorFromFile({ filepath: file, targetPath: dir });
    const r = ex.extract({ files: [name] }); for (const _ of r.files) { /* recorre para que extraiga */ }
    await fsp.rename(path.join(dir, name), outFile);
  } finally { await fsp.rm(dir, { recursive: true, force: true }); }
}
let yauzl = null;
async function zipOpen(file) { yauzl ||= (await import('yauzl')).default; return new Promise((res, rej) => yauzl.open(file, { lazyEntries: true, autoClose: false }, (e, z) => e ? rej(e) : res(z))); }
async function zipEntries(z) {
  return new Promise((res, rej) => { const out = []; z.on('entry', e => { out.push(e); z.readEntry(); }); z.on('end', () => res(out)); z.on('error', rej); z.readEntry(); });
}
const zipStream = (z, e) => new Promise((res, rej) => z.openReadStream(e, (err, s) => err ? rej(err) : res(s)));
async function zipReadText(z, entries, name) { const e = entries.find(x => x.fileName === name) || entries.find(x => x.fileName.toLowerCase() === name.toLowerCase()); if (!e) return null; const ch = []; for await (const c of await zipStream(z, e)) ch.push(c); return Buffer.concat(ch); }

/* ----------------------------- Ficha y portada de un EPUB ----------------------------- */
const decodeXml = s => s.replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;|&apos;/g, "'").replace(/&#(\d+);/g, (_, n) => String.fromCharCode(+n)).replace(/&amp;/g, '&');
const tagAll = (xml, tag) => [...xml.matchAll(new RegExp(`<(?:[\\w-]+:)?${tag}\\b[^>]*>([\\s\\S]*?)</(?:[\\w-]+:)?${tag}>`, 'gi'))].map(m => decodeXml(m[1].replace(/<[^>]+>/g, '')).trim()).filter(Boolean);
const attr = (s, a) => (s.match(new RegExp(`\\b${a}\\s*=\\s*"([^"]*)"`, 'i')) || s.match(new RegExp(`\\b${a}\\s*=\\s*'([^']*)'`, 'i')) || [])[1];
function joinPath(base, rel) { const parts = (base + rel).split('/'), out = []; for (const p of parts) { if (p === '..') out.pop(); else if (p && p !== '.') out.push(p); } return out.join('/'); }
let sharp = null; try { sharp = (await import('sharp')).default; } catch { log('sharp no instalado: las portadas se guardan sin redimensionar'); }
async function makeCovers(buf, id) { // guarda 320 px (para <img>) y devuelve dataURL de 160 px (para la app)
  if (!sharp) { await fsp.writeFile(path.join(DIRS.covers, id + '.jpg'), buf); return 'data:image/jpeg;base64,' + buf.toString('base64'); }
  const big = await sharp(buf).rotate().resize({ width: 320, withoutEnlargement: true }).jpeg({ quality: 72, mozjpeg: true }).toBuffer();
  await fsp.writeFile(path.join(DIRS.covers, id + '.jpg'), big);
  const meta = await sharp(buf).metadata(); const h = Math.round((meta.height || 240) * 160 / (meta.width || 160));
  const small = await sharp(buf).rotate().resize({ width: 160, height: Math.min(h, 260), fit: 'cover', position: 'top' }).jpeg({ quality: 68 }).toBuffer();
  return 'data:image/jpeg;base64,' + small.toString('base64');
}
async function epubMeta(file, id) {
  const z = await zipOpen(file);
  try {
    const entries = await zipEntries(z);
    const container = (await zipReadText(z, entries, 'META-INF/container.xml'))?.toString('utf8') || '';
    const opfPath = attr(container.match(/<rootfile\b[^>]*>/i)?.[0] || '', 'full-path');
    if (!opfPath) return {};
    const opf = (await zipReadText(z, entries, opfPath))?.toString('utf8') || '';
    const metas = [...opf.matchAll(/<meta\b[^>]*?(?:\/>|>[\s\S]*?<\/meta>)/gi)].map(m => m[0]);
    const named = n => { const m = metas.find(x => attr(x, 'name') === n); return m ? attr(m, 'content') : ''; };
    const prop = n => { const m = metas.find(x => attr(x, 'property') === n); return m ? decodeXml(m.replace(/<[^>]+>/g, '')).trim() : ''; };
    const items = [...opf.matchAll(/<item\b[^>]*>/gi)].map(m => m[0]);
    let ci = items.find(i => (attr(i, 'properties') || '').split(/\s+/).includes('cover-image'));
    if (!ci) { const cid = named('cover'); ci = items.find(i => attr(i, 'id') === cid); }
    if (!ci) ci = items.find(i => /cover/i.test((attr(i, 'id') || '') + (attr(i, 'href') || '')) && /^image\//.test(attr(i, 'media-type') || ''));
    let cover = '';
    if (ci) {
      try {
        const base = opfPath.includes('/') ? opfPath.slice(0, opfPath.lastIndexOf('/') + 1) : '';
        const img = await zipReadText(z, entries, joinPath(base, decodeURIComponent(attr(ci, 'href'))));
        if (img) cover = await makeCovers(img, id);
      } catch (e) { log('portada', id, e.message); }
    }
    return {
      t: tagAll(opf, 'title')[0] || '', a: tagAll(opf, 'creator').join(', '),
      y: (tagAll(opf, 'date')[0] || '').slice(0, 4), s: tagAll(opf, 'subject').slice(0, 6),
      se: named('calibre:series') || prop('belongs-to-collection') || '',
      si: named('calibre:series_index') || prop('group-position') || '',
      lang: tagAll(opf, 'language')[0] || '', cover
    };
  } finally { z.close(); }
}

// Título y autor de un PDF (diccionario /Info sin comprimir; lee solo el inicio y el final del archivo)
async function pdfInfo(file) {
  const st = await fsp.stat(file), fh = await fsp.open(file, 'r');
  try {
    const n = Math.min(st.size, 262144), a = Buffer.alloc(n), b = Buffer.alloc(n);
    await fh.read(a, 0, n, 0); await fh.read(b, 0, n, Math.max(0, st.size - n));
    const txt = a.toString('latin1') + b.toString('latin1');
    const str = key => {
      const m = txt.match(new RegExp(`/${key}\\s*(\\((?:\\\\.|[^\\\\)])*\\)|<[0-9A-Fa-f\\s]+>)`)); if (!m) return '';
      let v = m[1];
      if (v[0] === '<') { const hex = v.slice(1, -1).replace(/\s/g, ''); const buf = Buffer.from(hex, 'hex'); return (buf[0] === 0xfe && buf[1] === 0xff ? Buffer.from(buf.subarray(2)).swap16().toString('utf16le') : buf.toString('latin1')).trim(); }
      v = v.slice(1, -1).replace(/\\([nrtbf()\\]|[0-7]{1,3})/g, (_, c) => ({ n: '\n', r: '\r', t: '\t', b: '\b', f: '\f', '(': '(', ')': ')', '\\': '\\' }[c] ?? String.fromCharCode(parseInt(c, 8))));
      if (v.charCodeAt(0) === 0xfe && v.charCodeAt(1) === 0xff) { const buf = Buffer.from(v.slice(2), 'latin1'); return buf.swap16().toString('utf16le').trim(); }
      return v.replace(/\u0000/g, '').trim();
    };
    const t = str('Title'), au = str('Author');
    return { t: /^(untitled|sin título|microsoft word|documento)/i.test(t) ? '' : t, a: au };
  } finally { await fh.close(); }
}

/* ----------------------------- Autores (paquetes RAR/ZIP) ----------------------------- */
// Devuelve los libros de un paquete con su ficha. Queda guardado en disco: la segunda vez es instantáneo.
function scanPack(rp) {
  const metaFile = path.join(DIRS.packs, sha(rp) + '.json');
  return once('pack:' + rp, async () => {
    const hit = await exists(metaFile); if (hit) { touch(metaFile); return JSON.parse(await fsp.readFile(metaFile, 'utf8')); }
    const file = await localPath(rp), isZip = /\.zip$/i.test(rp), out = [];
    const t0 = Date.now();
    if (isZip) {
      const z = await zipOpen(file);
      try {
        const ents = (await zipEntries(z)).filter(e => BOOK_RX.test(e.fileName));
        for (const e of ents) {
          const id = hashId(rp + '|' + e.fileName), bookFile = path.join(DIRS.books, id + '.' + fmtOf(e.fileName));
          if (!(await exists(bookFile))) await pipeline(await zipStream(z, e), fs.createWriteStream(bookFile));
          out.push(await describe(id, e.fileName, e.uncompressedSize, bookFile));
        }
      } finally { z.close(); }
    } else {
      for (const h of (await rarList(file)).filter(x => BOOK_RX.test(x.name))) {
        const id = hashId(rp + '|' + h.name), bookFile = path.join(DIRS.books, id + '.' + fmtOf(h.name));
        if (!(await exists(bookFile))) await rarExtractTo(file, h.name, bookFile);
        out.push(await describe(id, h.name, h.size, bookFile));
      }
    }
    const res = { rp, at: Date.now(), books: out };
    await fsp.writeFile(metaFile, JSON.stringify(res));
    log(`Autor listo ${rp}: ${out.length} libros en ${((Date.now() - t0) / 1000).toFixed(1)} s`);
    gcSoon();
    return res;
  });
}
async function describe(id, name, size, bookFile) {
  const base = { id, n: name, u: size, fmt: fmtOf(name), ...guessMeta(name) };
  if (base.fmt === 'epub') {
    try { const m = await epubMeta(bookFile, id); for (const [k, v] of Object.entries(m)) if (v && (!Array.isArray(v) || v.length)) base[k] = v; }
    catch (e) { log('ficha', name, e.message); }
  }
  else { try { const m = await pdfInfo(bookFile); for (const [k, v] of Object.entries(m)) if (v) base[k] = v; } catch {} }
  base.hasCover = await exists(path.join(DIRS.covers, id + '.jpg')) ? 1 : 0;
  return base;
}
// Un libro listo en disco (lo extrae si hace falta)
async function bookFile(q) {
  if (q.rp && q.n) {
    const id = hashId(q.rp + '|' + q.n), f = path.join(DIRS.books, id + '.' + fmtOf(q.n));
    if (await exists(f)) { touch(f); return { file: f, name: q.n }; }
    return once('book:' + id, async () => {
      const src = await localPath(q.rp);
      if (/\.zip$/i.test(q.rp)) { const z = await zipOpen(src); try { const e = (await zipEntries(z)).find(x => x.fileName === q.n); if (!e) throw Object.assign(new Error('No está en el zip'), { status: 404 }); await pipeline(await zipStream(z, e), fs.createWriteStream(f)); } finally { z.close(); } }
      else await rarExtractTo(src, q.n, f);
      return { file: f, name: q.n };
    });
  }
  if (q.zp && q.n) return bookFile({ rp: q.zp, n: q.n });
  if (q.fp) return { file: await localPath(q.fp), name: q.fp };
  throw Object.assign(new Error('Faltan parámetros (rp+n, zp+n, fp o ml)'), { status: 400 });
}

/* ----------------------------- Caché: se borra lo menos usado ----------------------------- */
let gcT = null;
const gcSoon = () => { clearTimeout(gcT); gcT = setTimeout(gc, 5000); };
async function gc() {
  const files = [];
  for (const d of [DIRS.dl, DIRS.books, DIRS.covers]) for (const f of await fsp.readdir(d)) { const full = path.join(d, f); const st = await exists(full); if (st) files.push({ full, size: st.size, t: st.atimeMs > st.mtimeMs ? st.atimeMs : st.mtimeMs, cover: d === DIRS.covers }); }
  let total = files.reduce((s, f) => s + f.size, 0); const max = CFG.cacheMaxMB * 1048576;
  if (total <= max) return;
  files.sort((a, b) => (a.cover - b.cover) || (a.t - b.t)); // primero RAR y libros viejos; las portadas al final
  for (const f of files) { if (total <= max * .9) break; if (inflight.size && [...inflight.keys()].some(k => f.full.includes(k))) continue; await fsp.rm(f.full, { force: true }); total -= f.size; }
  log(`Caché recortada a ${(total / 1048576).toFixed(0)} MB`);
}

/* ----------------------------- HTTP ----------------------------- */
function cors(req, res) {
  const o = req.headers.origin || '';
  const ok = CFG.origins.includes(o) || /^https?:\/\/(localhost|127\.0\.0\.1)(:\d+)?$/.test(o);
  if (ok) { res.setHeader('Access-Control-Allow-Origin', o); res.setHeader('Vary', 'Origin'); }
  res.setHeader('Access-Control-Allow-Headers', 'Authorization, Content-Type, Range');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Accept-Ranges, X-Cache');
}
const json = (res, code, obj, extra = {}) => { const b = Buffer.from(JSON.stringify(obj)); res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': b.length, 'Cache-Control': 'no-store', ...extra }); res.end(b); };
async function sendFile(req, res, file, type, cache) { // con soporte de rangos (Range) para leer por partes
  const st = await fsp.stat(file), size = st.size;
  const head = { 'Content-Type': type, 'Accept-Ranges': 'bytes', 'Cache-Control': cache };
  const m = /bytes=(\d*)-(\d*)/.exec(req.headers.range || '');
  if (m) {
    const start = m[1] ? +m[1] : size - +m[2], end = m[1] && m[2] ? Math.min(+m[2], size - 1) : size - 1;
    if (start > end || start >= size) { res.writeHead(416, { 'Content-Range': `bytes */${size}` }); return res.end(); }
    res.writeHead(206, { ...head, 'Content-Range': `bytes ${start}-${end}/${size}`, 'Content-Length': end - start + 1 });
    return pipeline(fs.createReadStream(file, { start, end }), res).catch(() => {});
  }
  res.writeHead(200, { ...head, 'Content-Length': size });
  return pipeline(fs.createReadStream(file), res).catch(() => {});
}
const mime = n => /\.pdf$/i.test(n) ? 'application/pdf' : 'application/epub+zip';
async function readBody(req, max = 1e6) { const ch = []; let n = 0; for await (const c of req) { n += c.length; if (n > max) throw Object.assign(new Error('Muy grande'), { status: 413 }); ch.push(c); } return ch.length ? JSON.parse(Buffer.concat(ch).toString()) : {}; }

const server = http.createServer(async (req, res) => {
  cors(req, res);
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  const url = new URL(req.url, 'http://x'), q = Object.fromEntries(url.searchParams);
  try {
    if (url.pathname === '/health') return json(res, 200, { ok: true, origen: CFG.localDir ? 'carpeta local' : 'MEGA', archivos: tree.size, descargasActivas: inflight.size });

    // Portadas: con clave del día (las etiquetas <img> no mandan el token)
    if (url.pathname === '/api/cover') {
      if (!checkCoverKey(q.k)) return json(res, 403, { error: 'Clave de portada vencida' });
      const id = q.id || (q.rp && q.n ? hashId(q.rp + '|' + q.n) : '');
      if (!/^[0-9a-f]{16}$/.test(id)) return json(res, 400, { error: 'Falta el libro' });
      const f = path.join(DIRS.covers, id + '.jpg');
      if (!(await exists(f)) && q.rp) await scanPack(q.rp); // la genera si aún no existe
      if (!(await exists(f))) return json(res, 404, { error: 'Este libro no trae portada' }, { 'Cache-Control': 'public, max-age=86400' });
      touch(f); return sendFile(req, res, f, 'image/jpeg', 'public, max-age=2592000, immutable');
    }

    const user = await authUser(req);
    if (url.pathname === '/api/session') return json(res, 200, { coverKey: dayKey(user.uid), validoHasta: (Math.floor(Date.now() / 864e5) + 1) * 864e5 });

    if (url.pathname === '/api/pack') {
      if (!q.rp) return json(res, 400, { error: 'Falta rp' });
      const r = await scanPack(q.rp);
      // la app recibe la portada pequeña incluida, lista para guardar
      const books = await Promise.all(r.books.map(async b => {
        if (!b.hasCover) return b;
        try { const img = await fsp.readFile(path.join(DIRS.covers, b.id + '.jpg')); const small = sharp ? await sharp(img).resize({ width: 160, height: 260, fit: 'cover', position: 'top', withoutEnlargement: true }).jpeg({ quality: 68 }).toBuffer() : img; return { ...b, cover: 'data:image/jpeg;base64,' + small.toString('base64') }; } catch { return b; }
      }));
      return json(res, 200, { ...r, books });
    }

    if (url.pathname === '/api/book') {
      if (q.ml) { // libro subido por ti: se pasa en stream directo desde MEGA (descifrado al vuelo)
        MegaFile ||= (await import('megajs')).File;
        const f = MegaFile.fromURL(q.ml); await f.loadAttributes();
        res.writeHead(200, { 'Content-Type': mime(f.name || ''), 'Content-Length': f.size, 'Cache-Control': 'private, max-age=3600' });
        return pipeline(f.download({}), res).catch(() => {});
      }
      const { file, name } = await bookFile(q);
      return sendFile(req, res, file, mime(name), 'private, max-age=86400');
    }

    if (url.pathname === '/api/prefetch' && req.method === 'POST') {
      const { rps = [] } = await readBody(req);
      const list = rps.slice(0, 50); list.forEach(rp => scanPack(rp).catch(e => log('prefetch', rp, e.message)));
      return json(res, 202, { enCola: list.length });
    }
    json(res, 404, { error: 'No existe' });
  } catch (e) {
    const code = e.status || 500; if (code >= 500) log('ERROR', req.url, e.stack || e.message);
    if (!res.headersSent) json(res, code, { error: e.message }); else res.destroy();
  }
});

await initFirebase();
server.listen(CFG.port, () => {
  log(`MiBiblioteca · acelerador escuchando en :${CFG.port} · caché ${CFG.cacheDir} (máx ${CFG.cacheMaxMB} MB)`);
  loadTree().catch(e => log('Árbol de la biblioteca: ' + e.message));
  setInterval(() => loadTree(true).catch(() => {}), CFG.treeRefreshMin * 60000).unref();
  gcSoon();
});
for (const s of ['SIGINT', 'SIGTERM']) process.on(s, () => { log('Cerrando…'); server.close(() => process.exit(0)); setTimeout(() => process.exit(0), 3000).unref(); });
