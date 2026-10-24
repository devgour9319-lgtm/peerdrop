const CACHE = 'peerdrop-v19';
const ASSETS = [
  '/',
  '/index.html',
  '/css/style.css',
  '/js/app.js',
  '/js/pc-connect.js',
  '/js/local-chat.js',
  '/js/qrcode.min.js',
  '/manifest.json',
  '/socket.io/socket.io.js'
];

self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE)
      .then(c => c.addAll(ASSETS))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', e => {
  if (e.request.method !== 'GET') return;

  const url = new URL(e.request.url);
  const isSocketClient = url.pathname === '/socket.io/socket.io.js';
  // Keep live socket traffic and API responses online-only; the client library is static.
  if (url.origin !== self.location.origin
    || (url.pathname.startsWith('/socket.io/') && !isSocketClient)
    || url.pathname === '/local-info'
    || url.pathname.startsWith('/api/')) {
    return;
  }
  e.respondWith(
    caches.match(e.request).then(r => r || fetch(e.request).then(resp => {
      if (resp.ok) {
        const clone = resp.clone();
        caches.open(CACHE).then(c => c.put(e.request, clone));
      }
      return resp;
    })).catch(() => e.request.mode === 'navigate'
      ? caches.match('/index.html')
      : new Response('', { status: 503, statusText: 'Offline' }))
  );
});
