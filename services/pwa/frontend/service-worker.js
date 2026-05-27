// A2A2H PWA service worker — minimal: caches the app shell for offline launch +
// handles Web Push events (delivered by the backend via pywebpush when wired).
// Bump SHELL_CACHE when shipping any change to index.html / app.js / style.css.
// The activate handler deletes any cache != current, so the bump is the only
// thing the user needs for an update to take effect on next page load.
const SHELL_CACHE = "a2a2h-shell-v14";
const SHELL_FILES = ["/", "/index.html", "/static/app.js", "/static/style.css", "/manifest.json", "/static/icon-192.png", "/static/icon-512.png"];
const SHELL_PATHS = new Set(SHELL_FILES);

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(SHELL_CACHE).then((c) => c.addAll(SHELL_FILES))
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== SHELL_CACHE).map((k) => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);
  // API/chat-log/export routes are always network-only so private data and
  // history never come from a stale cache.
  if (url.pathname.startsWith("/api/") || url.pathname.startsWith("/chat-log/")) {
    event.respondWith(fetch(event.request));
    return;
  }

  // The visible PWA shell must update quickly on installed PWAs. Use
  // network-first for shell files instead of cache-first, then refresh the cache
  // from the successful response. This prevents old cached HTML/JS from hiding
  // newly shipped feature-request UI until a manual cache purge.
  const isShellRequest = event.request.mode === "navigate" || SHELL_PATHS.has(url.pathname);
  if (isShellRequest) {
    event.respondWith(
      fetch(event.request).then((resp) => {
        const copy = resp.clone();
        caches.open(SHELL_CACHE).then((c) => c.put(event.request, copy)).catch(() => {});
        return resp;
      }).catch(() => caches.match(event.request))
    );
    return;
  }

  event.respondWith(
    caches.match(event.request).then((hit) => hit || fetch(event.request))
  );
});

self.addEventListener("push", (event) => {
  let data = { title: "A2A2H", body: "New activity" };
  try { if (event.data) data = event.data.json(); } catch (e) {}
  const options = {
    body: data.body || "",
    icon: "/static/icon-192.png",
    badge: "/static/icon-192.png",
    tag: data.tag || "a2a2h",
    data: { url: data.url || "/" },
    requireInteraction: !!data.requireInteraction,
  };
  event.waitUntil(self.registration.showNotification(data.title || "A2A2H", options));
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  event.waitUntil(
    clients.matchAll({ type: "window" }).then((wins) => {
      for (const w of wins) {
        if (w.url.includes(event.notification.data.url) && "focus" in w) return w.focus();
      }
      return clients.openWindow(event.notification.data.url || "/");
    })
  );
});
