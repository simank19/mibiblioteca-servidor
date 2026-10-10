// Prueba rápida del servidor contra una carpeta local (sin MEGA ni Firebase)
// Uso: LOCAL_LIBRARY_DIR=./ejemplo/Libros AUTH_DISABLED=1 node server.js   y en otra terminal: node test/smoke.mjs
const base = process.env.BASE || 'http://localhost:8787';
const get = async (p, h = {}) => { const r = await fetch(base + p, { headers: h }); return r; };
const rp = process.env.RP || 'Libros/400 Parte 01 de 60/Ana Saga.rar';
let r = await get('/health'); console.log('health', r.status, await r.json());
let t0 = Date.now(); r = await get('/api/pack?rp=' + encodeURIComponent(rp)); const pk = await r.json();
console.log('pack (1ª vez)', r.status, Date.now() - t0, 'ms', pk.books?.map(b => `${b.id} ${b.t} · ${b.a} · ${b.fmt} · portada:${b.hasCover ? 'sí' : 'no'}`));
t0 = Date.now(); r = await get('/api/pack?rp=' + encodeURIComponent(rp)); await r.json(); console.log('pack (caché)', r.status, Date.now() - t0, 'ms');
const b = pk.books[0];
r = await get(`/api/book?rp=${encodeURIComponent(rp)}&n=${encodeURIComponent(b.n)}`); const buf = Buffer.from(await r.arrayBuffer());
console.log('libro', r.status, r.headers.get('content-type'), buf.length, 'bytes', 'zip:', buf.slice(0, 2).toString() === 'PK');
r = await get(`/api/book?rp=${encodeURIComponent(rp)}&n=${encodeURIComponent(b.n)}`, { Range: 'bytes=0-99' }); console.log('rango', r.status, r.headers.get('content-range'), (await r.arrayBuffer()).byteLength);
const s = await (await get('/api/session')).json(); console.log('sesión', s.coverKey ? 'clave ok' : s);
r = await get(`/api/cover?rp=${encodeURIComponent(rp)}&n=${encodeURIComponent(b.n)}&k=${s.coverKey}`); console.log('portada', r.status, r.headers.get('content-type'), (await r.arrayBuffer()).byteLength, 'bytes');
r = await get('/api/book?fp=' + encodeURIComponent('Libros/400 Parte 01 de 60/Carla Ruiz - Manual del Viento.pdf')); console.log('pdf suelto', r.status, r.headers.get('content-type'), (await r.arrayBuffer()).byteLength);
r = await get('/api/book?fp=' + encodeURIComponent('../../etc/passwd')); console.log('ruta peligrosa', r.status);
r = await get('/api/pack?rp=' + encodeURIComponent('Libros/no-existe.rar')); console.log('no existe', r.status);
