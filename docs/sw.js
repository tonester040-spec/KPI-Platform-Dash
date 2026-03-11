/* KPI Service Worker — v1
   Cache strategy:
   - Network First  → HTML files (fresh pipeline data when online)
   - Cache First    → everything else (icons, manifest, fonts)
*/

const CACHE     = 'kpi-v1';
const BASE      = '/KPI-Platform-Dash';
const PRECACHE  = [
  `${BASE}/`,
  `${BASE}/index.html`,
  `${BASE}/jess.html`,
  `${BASE}/jenn.html`,
  `${BASE}/manifest.json`,
  `${BASE}/icons/icon-192.png`,
  `${BASE}/icons/icon-512.png`,
  `${BASE}/offline.html`,
];

// ── Install: pre-cache shell ──────────────────────────────────────────────────
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE).then(cache => cache.addAll(PRECACHE))
  );
  self.skipWaiting();
});

// ── Activate: clear old caches ────────────────────────────────────────────────
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

// ── Fetch ─────────────────────────────────────────────────────────────────────
self.addEventListener('fetch', event => {
  const url = new URL(event.request.url);

  // Only handle same-origin or GitHub Pages requests
  if (!url.hostname.includes('github.io') && url.hostname !== 'localhost') return;

  const isHTML = event.request.mode === 'navigate' ||
                 url.pathname.endsWith('.html') ||
                 url.pathname.endsWith('/');

  if (isHTML) {
    // Network First — always try to get fresh data; fall back to cache then offline
    event.respondWith(
      fetch(event.request)
        .then(res => {
          const copy = res.clone();
          caches.open(CACHE).then(c => c.put(event.request, copy));
          return res;
        })
        .catch(() =>
          caches.match(event.request).then(cached =>
            cached || caches.match(`${BASE}/offline.html`)
          )
        )
    );
  } else {
    // Cache First — serve static assets instantly
    event.respondWith(
      caches.match(event.request).then(cached => {
        if (cached) return cached;
        return fetch(event.request).then(res => {
          const copy = res.clone();
          caches.open(CACHE).then(c => c.put(event.request, copy));
          return res;
        });
      })
    );
  }
});
