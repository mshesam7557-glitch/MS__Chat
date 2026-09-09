self.addEventListener("push", event => {
  event.waitUntil((async () => {
    let data = {};
    try { data = event.data ? event.data.json() : {}; } catch (_) {}
    const windows = await clients.matchAll({ type: "window", includeUncontrolled: true });
    const hasVisibleClient = windows.some(c => c.visibilityState === "visible");
    if (hasVisibleClient) {
      for (const client of windows) {
        try { client.postMessage({ type: "push-message", data }); } catch (_) {}
      }
      return;
    }
    const title = data.title || "MS Chat";
    const options = {
      body: data.body || "پیام جدید دارید",
      icon: "/background.png",
      badge: "/background.png",
      tag: data.tag || (data.sender ? `mschat-${data.sender}` : "mschat-message"),
      renotify: true,
      data: { url: data.url || "/" }
    };
    await self.registration.showNotification(title, options);
  })());
});

self.addEventListener("notificationclick", event => {
  event.notification.close();
  const url = event.notification.data && event.notification.data.url ? event.notification.data.url : "/";
  event.waitUntil(clients.matchAll({type:"window",includeUncontrolled:true}).then(list => {
    for (const client of list) {
      if ("focus" in client) return client.focus().then(() => client.navigate(url));
    }
    if (clients.openWindow) return clients.openWindow(url);
  }));
});
