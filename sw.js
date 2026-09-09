self.addEventListener("push", event => {
  let data = {};
  try { data = event.data ? event.data.json() : {}; } catch (_) {}
  const title = data.title || "MS Chat";
  const options = { body: data.body || "پیام جدید دارید", icon: "/background.png", badge: "/background.png", data: { url: data.url || "/" } };
  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener("notificationclick", event => {
  event.notification.close();
  const url = event.notification.data && event.notification.data.url ? event.notification.data.url : "/";
  event.waitUntil(clients.matchAll({type:"window",includeUncontrolled:true}).then(list => {
    for (const client of list) { if ("focus" in client) { client.focus(); return client.navigate(url); } }
    if (clients.openWindow) return clients.openWindow(url);
  }));
});
