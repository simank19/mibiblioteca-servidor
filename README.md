# MiBiblioteca · Servidor acelerador (puente MEGA)

Servidor opcional para MiBiblioteca. Baja los RAR de tu MEGA **a disco por partes** (no los carga en memoria), los abre, saca la ficha y la portada de cada libro, y se los entrega a la app ya listos. El celular deja de descargar y descomprimir RAR: solo recibe el libro que vas a leer.

Si el servidor está apagado o dormido, la app sigue funcionando sola como siempre.

## Qué expone

| Ruta | Para qué |
|---|---|
| `GET /health` | Saber si está despierto |
| `GET /api/session` | Clave del día para pedir portadas |
| `GET /api/pack?rp=RUTA.rar` | Libros de un autor, con ficha y portada |
| `GET /api/book?rp=RUTA&n=ENTRADA` | El libro (EPUB/PDF), con soporte de rangos. También `?fp=`, `?zp=&n=`, `?ml=` |
| `GET /api/cover?id=…&k=CLAVE` | Portada JPG cacheable |
| `POST /api/prefetch` | Prepara autores en segundo plano |

Solo responde a **tu cuenta de Google** (token de Firebase). Los visitantes aprobados también entran si agregas `FIREBASE_SERVICE_ACCOUNT`.

## Publicarlo gratis desde el teléfono (Render)

1. **GitHub** (github.com, desde el navegador del celular): crea una cuenta si no tienes → botón **＋ → New repository** → nombre `mibiblioteca-servidor` → **Public** → *Create*.
2. En el repositorio: **Add file → Upload files** → sube `server.js`, `package.json`, `render.yaml` y `README.md` → **Commit changes**.
3. **Render** (render.com): *Get Started* → **Sign in with GitHub**. No pide tarjeta.
4. **New ＋ → Blueprint** → elige `mibiblioteca-servidor` → te pide **MEGA_FOLDER_URL**: pega el enlace de tu carpeta de libros de MEGA → **Apply**.
5. Espera 3–5 minutos a que diga **Live**. Copia la dirección (algo como `https://mibiblioteca-acelerador.onrender.com`).
6. En MiBiblioteca: **Ajustes → Servidor acelerador** → pega la dirección → **Probar** → **Guardar servidor**. Queda guardado para todos tus dispositivos.

### Opcional (en Render → tu servicio → Environment)
- `MEGA_EMAIL` y `MEGA_PASSWORD`: descarga con tu cuenta de MEGA (más cuota que el acceso anónimo).
- `FIREBASE_SERVICE_ACCOUNT`: para visitantes. En Firebase → ⚙ Configuración del proyecto → Cuentas de servicio → *Generar nueva clave privada*, abre el archivo y pega todo su contenido.

### Lo que hay que saber del plan gratis de Render
- **Se duerme** tras 15 min sin uso y tarda ~1 min en despertar. La app lo despierta al abrirse y mientras tanto trabaja sola.
- **750 horas al mes**: alcanza para un servicio todo el mes.
- **Disco temporal**: la caché se borra al dormirse o reiniciarse (se vuelve a llenar sola).
- **Ancho de banda**: el plan Hobby incluye 5 GB de salida al mes; para leer libros (1–2 MB cada uno) sobra.

## Probarlo en tu PC
```bash
npm install
LOCAL_LIBRARY_DIR=/ruta/a/Libros AUTH_DISABLED=1 node server.js   # solo pruebas: sin login
node test/smoke.mjs
```
