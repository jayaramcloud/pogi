// pogi console: the shell's behaviour. Pages are server-rendered with htmx; this file only adds
// what htmx cannot do: the ⌘K palette, the drawer, toasts, shortcuts, theme and sidebar state.
// Everything is delegated on document, so it survives hx-boost body swaps.
(() => {
  const $ = (s, el = document) => el.querySelector(s);
  const setCookie = (k, v) => { document.cookie = `${k}=${encodeURIComponent(v)}; path=/; max-age=31536000; samesite=lax`; };
  const getCookie = k => decodeURIComponent((document.cookie.split('; ').find(c => c.startsWith(k + '=')) || '=').split('=')[1] || '');
  const typing = () => /INPUT|TEXTAREA|SELECT/.test(document.activeElement.tagName) || document.activeElement.isContentEditable;

  // ---- behaviour the templates ask for with data attributes (the CSP forbids inline handlers) ----
  // data-nosubmit: a form that is never sent by Enter. Capture phase, so it runs first, as an inline onsubmit did.
  document.addEventListener('submit', e => { if (e.target.matches('[data-nosubmit]')) e.preventDefault(); }, true);
  // data-autosubmit: send the form when the field changes
  document.addEventListener('change', e => { if (e.target.matches('[data-autosubmit]')) e.target.form.requestSubmit(); });
  // data-filter: hide the service tiles, or the table rows, that do not match the typed text
  document.addEventListener('input', e => {
    const i = e.target, kind = i.dataset && i.dataset.filter; if (!kind) return;
    const q = i.value.toLowerCase().trim();
    if (kind === 'tiles') {
      document.querySelectorAll('.tile').forEach(t => { t.hidden = !!q && t.dataset.q.indexOf(q) < 0; });
      document.querySelectorAll('.cat').forEach(c => { c.hidden = !c.querySelector('.tile:not([hidden])'); });
    } else {
      i.closest('main').querySelectorAll('.dt tbody tr').forEach(r => { r.hidden = !!q && r.textContent.toLowerCase().indexOf(q) < 0; });
    }
  });

  // ---- theme ----
  const isDark = () => { const t = document.documentElement.dataset.theme; return t ? t === 'dark' : matchMedia('(prefers-color-scheme: dark)').matches; };
  function themeIcon() { const u = $('#themeBtn use'); if (u) { u.setAttribute('href', isDark() ? '#i-sun' : '#i-moon'); $('#themeBtn').title = isDark() ? 'Switch to light' : 'Switch to dark'; } }
  function toggleTheme() { const t = isDark() ? 'light' : 'dark'; document.documentElement.dataset.theme = t; setCookie('pogi-theme', t); themeIcon(); }
  matchMedia('(prefers-color-scheme: dark)').addEventListener('change', themeIcon);

  // ---- sidebar: "mini" collapses a wide window, "wide" expands a narrow one ----
  function toggleSide() {
    const app = $('#app'), narrow = innerWidth <= 1100;
    app.classList.remove(narrow ? 'mini' : 'wide');
    const on = app.classList.toggle(narrow ? 'wide' : 'mini');
    setCookie('pogi-side', on ? (narrow ? 'wide' : 'mini') : '');
    sideTitle();
  }
  // the footer button reads "Expand sidebar" while the sidebar is a rail
  function sideTitle() {
    const app = $('#app'), b = $('.collapse'); if (!app || !b) return;
    const rail = innerWidth <= 1100 ? !app.classList.contains('wide') : app.classList.contains('mini');
    b.title = (rail ? 'Expand' : 'Collapse') + ' sidebar (⌘B)';
  }
  addEventListener('resize', () => sideTitle());
  document.addEventListener('htmx:afterSettle', () => sideTitle());
  document.addEventListener('DOMContentLoaded', () => sideTitle());

  // ---- toasts ----
  let tt;
  function toast(msg, kind, href, label) {
    const t = $('#toast'); if (!t) return;
    t.innerHTML = '';
    if (kind) { const d = document.createElement('span'); d.className = 'dot'; d.style.background = `var(--${kind})`; t.append(d); }
    t.append(document.createTextNode(msg));
    if (href) { const a = document.createElement('a'); a.textContent = label || 'View'; a.href = href; if (href.includes('/_pogi/requests/')) a.dataset.drawer = ''; t.append(a); }
    t.classList.add('on'); clearTimeout(tt);
    tt = setTimeout(() => { t.classList.remove('on'); setTimeout(() => { if (!t.classList.contains('on')) t.textContent = ''; }, 300); }, kind === 'bad' ? 5000 : 2500);
  }

  // ---- overlays ----
  // Each overlay is a dialog or menu: opening moves focus into it, Tab stays inside a modal one (the page behind is inert),
  // and closing with Esc, the scrim or a close button hands focus back to whatever opened it.
  const overlays = ['drawer', 'palette', 'pop', 'menu', 'bell', 'keys'];
  const isOn = id => { const e = document.getElementById(id); return !!e && e.classList.contains('on'); };
  const inOverlay = el => !!el && overlays.some(id => { const e = document.getElementById(id); return e && e.contains(el); });
  const FOCUSABLE = 'a[href], button:not([disabled]), input:not([disabled]):not([type=hidden]), select:not([disabled]), textarea:not([disabled]), summary, [tabindex]:not([tabindex="-1"])';
  const focusables = el => [...el.querySelectorAll(FOCUSABLE)].filter(x => x.tabIndex >= 0 && x.getClientRects().length && !x.closest('[hidden]'));
  let ret = null;   // what to refocus when the overlay closes
  function mark() { const a = document.activeElement; ret = inOverlay(a) ? ret : a; }
  function sync() {
    const app = $('#app'); if (app) app.inert = isOn('drawer') || isOn('palette') || isOn('keys');
    document.querySelectorAll('[data-js=scope], [data-js=bell], [data-js=keys]').forEach(b => b.setAttribute('aria-expanded', isOn(b.dataset.js === 'scope' ? 'pop' : b.dataset.js)));
    const m = $('#menu');
    document.querySelectorAll('[data-menu]').forEach(b => b.setAttribute('aria-expanded', !!m && m.classList.contains('on') && m.dataset.for === b.dataset.menu));
    const pq = $('#pq'); if (pq) pq.setAttribute('aria-expanded', isOn('palette'));
  }
  function focusIn(el) { const f = el.id === 'palette' ? $('#pq') : focusables(el)[0]; if (f) f.focus(); else { el.tabIndex = -1; el.focus(); } }
  function show(el) { el.classList.add('on'); sync(); focusIn(el); }
  function closeAll() { overlays.forEach(id => { const e = document.getElementById(id); if (e) e.classList.remove('on'); }); const s = $('#scrim'); if (s) s.classList.remove('on'); sync(); }
  function dismiss() {
    const was = overlays.some(isOn), r = ret; ret = null; closeAll();
    if (was && r && r.isConnected && r !== document.body) r.focus();
  }
  function scrim() { $('#scrim').classList.add('on'); }
  let marked = false;   // openDrawer already recorded the opener
  function openDrawer(url) { mark(); marked = true; closeAll(); htmx.ajax('GET', url, { target: '#drawer', swap: 'innerHTML' }); }
  function anchor(el, pop, width) { const b = el.getBoundingClientRect(); pop.style.top = b.bottom + 6 + 'px'; pop.style.left = Math.max(12, Math.min(innerWidth - width - 12, b.right - width)) + 'px'; }

  // ---- labels and errors ----
  let uid = 0;
  // a .field's <label> names its control; a placeholder-only input takes the placeholder as its name; required controls say so
  function a11y(root = document) {
    root.querySelectorAll('.field > label:not([for])').forEach(l => {
      const c = l.parentElement.querySelector('input:not([type=hidden]), select, textarea'); if (!c) return;
      c.id = c.id || 'f' + ++uid; l.htmlFor = c.id;
    });
    root.querySelectorAll('input[placeholder], textarea[placeholder]').forEach(c => {
      if (!(c.labels && c.labels.length) && !c.hasAttribute('aria-label') && !c.hasAttribute('aria-labelledby')) c.setAttribute('aria-label', c.getAttribute('placeholder'));
    });
    root.querySelectorAll('[required]:not([aria-required])').forEach(c => c.setAttribute('aria-required', 'true'));
  }
  // a form error that opens with a field's label ("Delay: “x” is not a whole number") is tied to that field
  function linkErrors(root) {
    const f = $('.flash', root); if (!f) return;
    f.id = f.id || 'derr';
    const msg = f.textContent.trim().toLowerCase();
    const l = [...root.querySelectorAll('.field > label')].find(x => { const t = ((x.firstChild || {}).textContent || '').trim().toLowerCase(); return t && msg.startsWith(t) && /^[:\s]/.test(msg.slice(t.length)); });
    const c = l && l.htmlFor && document.getElementById(l.htmlFor);
    if (c) { c.setAttribute('aria-describedby', f.id); c.setAttribute('aria-invalid', 'true'); }
  }
  document.addEventListener('DOMContentLoaded', () => a11y());
  document.addEventListener('htmx:afterSettle', () => a11y());

  // ---- palette ----
  let pi = 0, pt;
  function openPalette() { mark(); closeAll(); scrim(); const q = $('#pq'); q.value = ''; show($('#palette')); search(''); }
  function search(q) { fetch('/_pogi/search?q=' + encodeURIComponent(q)).then(r => r.text()).then(h => { $('#pres').innerHTML = h; pi = 0; hl(); }); }
  const items = () => [...document.querySelectorAll('#pres .it')];
  function hl() {
    document.querySelectorAll('#pres .grp').forEach(g => g.setAttribute('role', 'presentation'));
    items().forEach((e, i) => { e.id = 'po' + i; e.tabIndex = -1; e.setAttribute('role', 'option'); e.setAttribute('aria-selected', i === pi); e.classList.toggle('on', i === pi); });
    const e = items()[pi], q = $('#pq'); if (q) q.setAttribute('aria-activedescendant', e ? e.id : '');
    if (e) e.scrollIntoView({ block: 'nearest' });
  }
  function run(it) {
    dismiss();
    if (it.dataset.js) return js(it.dataset.js, it);
    if (it.hasAttribute('data-post')) return fetch(it.getAttribute('href'), { method: 'POST' }).then(r => { toast(r.ok ? 'Done' : 'Failed', r.ok ? '' : 'bad'); if (r.ok) location.reload(); });
    if (it.hasAttribute('data-drawer')) return openDrawer(it.getAttribute('href'));
    location.assign(it.getAttribute('href'));
  }
  function js(name, el) {
    switch (name) {
      case 'palette': return openPalette();
      case 'theme': return toggleTheme();
      case 'side': return toggleSide();
      case 'keys': { const k = $('#keys'), on = k.classList.contains('on'); mark(); on ? dismiss() : (closeAll(), scrim(), show(k)); return; }
      case 'scope': { const p = $('#pop'), on = p.classList.contains('on'); mark(); if (on) return dismiss(); closeAll(); anchor($('#scopeBtn'), p, 340); show(p); return; }
      case 'bell': {
        const p = $('#bell'), on = p.classList.contains('on'); mark(); if (on) return dismiss(); closeAll();
        anchor($('#bellBtn'), p, 390); show(p);
        fetch('/_pogi/notes').then(r => r.ok ? r.text() : '<div class="empty" style="padding:30px">No notifications.</div>').then(h => { p.innerHTML = h; htmx.process(p); if (p.classList.contains('on')) focusIn(p); });
        return;
      }
      case 'copy-endpoint': return copy(location.origin);
    }
  }
  function copy(text) { navigator.clipboard.writeText(text).then(() => toast('Copied'), () => toast('Copy failed', 'bad')); }

  // ---- pins ----
  function pin(name) {
    const pins = getCookie('pogi-pins').split(',').filter(Boolean), on = pins.includes(name);
    setCookie('pogi-pins', (on ? pins.filter(p => p !== name) : [...pins, name]).join(','));
    try { sessionStorage.setItem('pogi-refocus', name); } catch {}
    location.reload();
  }
  // after a pin's reload, give keyboard users their place back and say what changed
  try {
    const n = sessionStorage.getItem('pogi-refocus');
    if (n) {
      sessionStorage.removeItem('pogi-refocus');
      const el = document.querySelector('[data-pin="' + CSS.escape(n) + '"]');
      if (el) { el.focus(); toast((el.getAttribute('aria-pressed') === 'true' ? 'Pinned ' : 'Unpinned ') + (el.getAttribute('aria-label') || n).replace(/^(Unpin|Pin) /, '')); }
    }
  } catch {}

  // ---- events ----
  document.addEventListener('click', e => {
    const t = e.target;
    // a chosen menu item closes the menu (items that open the drawer have already done so)
    if (t.closest('#menu [role=menuitem]')) setTimeout(() => { if (isOn('menu')) dismiss(); });
    const close = t.closest('[data-close]'); if (close) { e.preventDefault(); return dismiss(); }
    if (t.closest('#scrim')) return dismiss();
    const it = t.closest('#pres .it'); if (it) { e.preventDefault(); return run(it); }
    const j = t.closest('[data-js]'); if (j) { e.preventDefault(); e.stopPropagation(); return js(j.dataset.js, j); }
    const c = t.closest('[data-copy]'); if (c) { e.preventDefault(); const v = c.dataset.copy; return copy(v.startsWith('#') ? ($(v) || {}).textContent || '' : v); }
    const p = t.closest('[data-pin]'); if (p) { e.preventDefault(); return pin(p.dataset.pin); }
    const m = t.closest('[data-menu]');
    if (m) {
      e.preventDefault(); e.stopPropagation();
      const menu = $('#menu'), on = menu.classList.contains('on') && menu.dataset.for === m.dataset.menu; mark();
      if (on) return dismiss();
      closeAll(); menu.innerHTML = (m.nextElementSibling || {}).innerHTML || ''; menu.dataset.for = m.dataset.menu;
      menu.querySelectorAll('a, button').forEach(x => { x.setAttribute('role', 'menuitem'); x.tabIndex = -1; });
      menu.querySelectorAll('hr').forEach(x => x.setAttribute('role', 'separator'));
      htmx.process(menu); anchor(m, menu, 210); show(menu); const f = menu.querySelector('[role=menuitem]'); if (f) f.focus(); return;
    }
    // [data-drawer] opens its href (or its own value) in the drawer, unless the click was on a control inside it
    const d = t.closest('[data-drawer]');
    if (d && !(t.closest('input, button, select, label, a') && t.closest('input, button, select, label, a') !== d)) {
      e.preventDefault(); return openDrawer(d.dataset.drawer || d.getAttribute('href'));
    }
    if (!t.closest('#pop, #bell, #menu')) ['pop', 'bell', 'menu'].forEach(id => { const el = document.getElementById(id); if (el) el.classList.remove('on'); });
  });
  document.addEventListener('input', e => { if (e.target.id === 'pq') { clearTimeout(pt); pt = setTimeout(() => search(e.target.value), 120); } });
  document.addEventListener('htmx:afterSwap', e => {
    if (e.detail.target.id !== 'drawer') return;
    const d = $('#drawer'); if (!d.innerHTML.trim()) return;
    if (!marked) mark(); marked = false;
    scrim(); d.classList.add('on'); sync(); a11y(d); linkErrors(d);
    // a form drawer starts in its first field, so typing right after `c` or a click fills it in
    const f = $('[autofocus]', d) || $('.dbody :is(input:not([type=hidden]):not([type=checkbox]), select, textarea)', d); if (f) f.focus(); else focusIn(d);
  });
  document.addEventListener('htmx:afterSettle', themeIcon);
  document.addEventListener('pogi-toast', e => toast(e.detail.msg, e.detail.kind, e.detail.href));
  // a finished form swaps the page in afterwards (HX-Location), which replaces #toast: show it once that settles
  let doneMsg = '';
  document.addEventListener('pogi-done', e => { dismiss(); doneMsg = e.detail.msg || 'Done'; setTimeout(() => { if (doneMsg) { toast(doneMsg); doneMsg = ''; } }, 1500); });
  document.addEventListener('htmx:afterSettle', () => { if (doneMsg) { toast(doneMsg); doneMsg = ''; } });
  document.addEventListener('DOMContentLoaded', themeIcon);

  // keyboard: Tab stays inside an open modal, arrows drive menus and tablists, Enter/Space press role=button and role=link elements
  document.addEventListener('keydown', e => {
    const t = e.target, k = e.key;
    if (k === 'Tab') {
      const m = ['keys', 'drawer', 'palette'].map(id => document.getElementById(id)).find(x => x && x.classList.contains('on'));
      if (!m) { if (isOn('menu')) dismiss(); return; }
      const f = focusables(m), i = f.indexOf(document.activeElement);
      if (f.length && (i < 0 || (e.shiftKey ? i === 0 : i === f.length - 1))) { f[e.shiftKey && i >= 0 ? f.length - 1 : 0].focus(); e.preventDefault(); }
      return;
    }
    const menu = t.closest && t.closest('#menu');
    if (menu && ['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(k)) {
      const it = [...menu.querySelectorAll('[role=menuitem]')], n = it.length, i = it.indexOf(t);
      if (n) it[k === 'Home' ? 0 : k === 'End' ? n - 1 : (i + (k === 'ArrowDown' ? 1 : -1) + n) % n].focus();
      return e.preventDefault();
    }
    if ((k === 'ArrowRight' || k === 'ArrowLeft') && t.getAttribute && t.getAttribute('role') === 'tab') {
      const tabs = [...t.closest('[role=tablist]').querySelectorAll('[role=tab]')], n = tabs[(tabs.indexOf(t) + (k === 'ArrowRight' ? 1 : -1) + tabs.length) % tabs.length];
      n.focus(); if (n.dataset.tab !== undefined) n.click();
      return e.preventDefault();
    }
    if ((k === 'Enter' || k === ' ') && t.matches && t.matches('[role=button]:not(button):not([href]), [role=link]:not([href])')) { e.preventDefault(); t.click(); }
  });

  let gAt = 0;
  const goto = { h: '/_pogi/', a: '/_pogi/requests', f: '/_pogi/flows', s: '/_pogi/services', m: '/_pogi/map', i: '/_pogi/iam', c: '/_pogi/cost', x: '/_pogi/chaos' };
  document.addEventListener('keydown', e => {
    const mod = e.metaKey || e.ctrlKey;
    if (mod && e.key === 'k') { e.preventDefault(); return $('#palette').classList.contains('on') ? dismiss() : openPalette(); }
    if (mod && e.key === 'b') { e.preventDefault(); return toggleSide(); }
    if ($('#palette').classList.contains('on') && e.target.id === 'pq') {
      if (e.key === 'ArrowDown') { pi = Math.min(pi + 1, items().length - 1); hl(); e.preventDefault(); }
      else if (e.key === 'ArrowUp') { pi = Math.max(pi - 1, 0); hl(); e.preventDefault(); }
      else if (e.key === 'Enter') { const it = items()[pi]; if (it) run(it); e.preventDefault(); }
      else if (e.key === 'Escape') { e.preventDefault(); dismiss(); }
      return;
    }
    if (e.key === 'Escape') return dismiss();
    if (mod || e.altKey || typing()) return;
    if (Date.now() - gAt < 1200) { gAt = 0; if (goto[e.key]) { location.assign(goto[e.key]); return; } }
    switch (e.key) {
      case 'g': gAt = Date.now(); return;
      case '/': e.preventDefault(); return openPalette();
      case '?': return js('keys');
      case 't': return toggleTheme();
      case 'n': return js('bell');
      case 'c': { const c = $('main [data-create]'); if (c) { e.preventDefault(); c.click(); } return; }
      case 'f': { const f = $('main .filter input, main input[type=search]'); if (f) { e.preventDefault(); f.focus(); } return; }
    }
  });

  // ---- builder A: tabs and the notification bell ----
  // [data-tab] links switch the [data-pane] blocks of their drawer, [data-tabs] container or page; the .on tab shows first.
  function showTab(t) {
    const root = t.closest('[data-tabs], .drawer, main');
    root.querySelectorAll('[data-tab]').forEach(x => { x.classList.toggle('on', x === t); x.setAttribute('aria-selected', x === t); });
    root.querySelectorAll('[data-pane]').forEach(p => { p.hidden = p.dataset.pane !== t.dataset.tab; });
  }
  const initTabs = () => document.querySelectorAll('[data-tab].on').forEach(showTab);
  document.addEventListener('click', e => { const t = e.target.closest('[data-tab]'); if (t) { e.preventDefault(); showTab(t); } });
  document.addEventListener('DOMContentLoaded', initTabs);
  document.addEventListener('htmx:afterSettle', initTabs);

  // the bell badge and toasts: poll for failed calls, IAM denials and chaos faults (one toast per 8 s)
  let noteSince = Date.now(), noteToast = 0;
  function pollNotes() {
    if (document.hidden) return;
    fetch('/_pogi/notes/poll?since=' + noteSince).then(r => r.ok ? r.json() : null).then(j => {
      if (!j) return;
      noteSince = j.now;
      newMail(j.mail);
      const b = $('#bellN'); if (b) b.textContent = j.unread > 99 ? '99+' : j.unread || '';
      if (j.new.length && Date.now() - noteToast > 8000) { noteToast = Date.now(); toast(j.new[0].msg, j.new[0].kind, j.new[0].href); }
    }).catch(() => {});
  }
  setInterval(pollNotes, 4000); pollNotes();

  // ---- live inbox: the poll carries the newest captured mail; a new one toasts, and refreshes the Inbox page in place ----
  let mailSeen;
  const loadedAt = Date.now();
  function newMail(m) {
    const id = m ? m.id : '';
    // the first poll only seeds, so mail from before this page loaded doesn't toast; mail that came after it
    // does, even if that first poll ran late (polls pause while the tab is hidden)
    if (mailSeen === undefined && !(m && m.at > loadedAt)) { mailSeen = id; return; }
    if (id === mailSeen) return;
    mailSeen = id;
    if (!id) return; // the inbox was cleared
    toast('New email to ' + (m.to || 'someone') + ' · ', '', '/_pogi/inbox', 'Open');
    if (!location.pathname.startsWith('/_pogi/inbox')) return;
    const list = $('.mail .list'), sel = list ? '.mail .list' : '#main';
    htmx.ajax('GET', location.pathname, { target: sel, select: sel, swap: 'outerHTML' }).then(() => { const a = $('.mail .list a'); if (a) a.classList.add('new'); });
  }

  // live tables pause while someone is working in them (focus inside, or a drawer open on top), so a refresh never swaps out the focused link
  const busy = sel => !!document.querySelector(sel + ':focus-within, #drawer.on');
  // data-poll="ms": send a pogi-poll event to the element every so often, for its hx-trigger (htmx's own
  // "every 2s [condition]" filters need eval, which the CSP forbids). It waits while the tab is hidden;
  // data-poll-live also waits for the Live switch and for typing in the element; data-poll-drawer waits for an open drawer.
  const polled = new WeakMap();
  setInterval(() => {
    if (document.hidden) return;
    const now = Date.now(), live = document.getElementById('live'), drawer = document.getElementById('drawer');
    document.querySelectorAll('[data-poll]').forEach(el => {
      if (!polled.has(el)) polled.set(el, now);
      if (now - polled.get(el) < +el.dataset.poll) return;
      if ('pollLive' in el.dataset && !(live && live.checked && !busy('#' + el.id))) return;
      if ('pollDrawer' in el.dataset && !(drawer && drawer.classList.contains('on'))) return;
      polled.set(el, now);
      htmx.trigger(el, 'pogi-poll');
    });
  }, 500);
  window.pogi = { toast, openDrawer, closeAll, copy, pin, busy };
})();

// ---------- builder D: chaos drawer, run shortcut, mail panes ----------
(() => {
  const field = k => document.querySelector('[data-cli] [data-k="' + k + '"]');
  const val = k => { const e = field(k); return e ? e.value.trim() : ''; };
  // the add-rule form's CLI band follows its fields
  function mirror() {
    const out = document.getElementById('cli'); if (!out || !field('p')) return;
    const p = val('p'), pv = document.getElementById('pv'); if (pv) pv.textContent = p + '%';
    out.textContent = 'pogi chaos add' + (val('svc') ? ' --service ' + val('svc') : '') + (val('op') ? " --op '" + val('op') + "'" : '') + ' --rate ' + (p / 100).toFixed(2) +
      (val('st') ? ' --status ' + val('st') : '') + (val('code') ? ' --code ' + val('code') : '') + (+val('lat') ? ' --latency ' + val('lat') + 'ms' : '');
  }
  document.addEventListener('input', e => { if (e.target.closest('[data-cli]')) mirror(); });
  document.addEventListener('change', e => { if (e.target.closest('[data-cli]')) mirror(); });
  document.addEventListener('click', e => {
    const b = e.target.closest('[data-preset]'); if (!b) return;
    b.parentNode.querySelectorAll('button').forEach(x => x.classList.toggle('on', x === b));
    ["st", "code", "p", "lat"].forEach((k, i) => { const f = field(k); if (f) f.value = b.dataset.preset.split(',')[i]; });
    mirror();
  });
  // ⌘↵ runs the query of a tool page
  document.addEventListener('keydown', e => {
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { const f = e.target.closest && e.target.closest('form[data-run]'); if (f) { e.preventDefault(); f.requestSubmit(); } }
  });
})();

// ---- builder B: bulk selection on service pages ----
(() => {
  // hx-boost would follow an <a data-drawer> before the shell's handler runs: open it in the drawer first
  document.addEventListener('click', e => {
    const a = e.target.closest && e.target.closest('a[data-drawer]');
    if (!a || e.defaultPrevented || e.button || e.metaKey || e.ctrlKey || e.shiftKey || a.closest('#drawer')) return;
    e.preventDefault(); e.stopPropagation();
    window.pogi.openDrawer(a.dataset.drawer || a.getAttribute('href'));
  }, true);
  const sync = () => {
    const rows = [...document.querySelectorAll('.dt tbody input[name=row]')], n = rows.filter(c => c.checked).length;
    rows.forEach(c => c.closest('tr').classList.toggle('sel', c.checked));
    const b = document.getElementById('bulk'), t = document.getElementById('bulkN');
    if (b) { t.textContent = n + ' selected'; b.classList.toggle('on', n > 0); }
  };
  document.addEventListener('change', e => {
    const c = e.target;
    if (!c.matches || !c.matches('.dt input[type=checkbox]')) return;
    if (c.closest('thead')) c.closest('table').querySelectorAll('tbody input[name=row]').forEach(x => { x.checked = c.checked; });
    sync();
  });
  document.addEventListener('click', e => {
    if (!e.target.closest('[data-bulk-clear]')) return;
    document.querySelectorAll('.dt input[type=checkbox]').forEach(x => { x.checked = false; });
    sync();
  });
})();

// Uploads: the server's limits come in meta tags (plain uploads, and function code, which is a zip).
// A file over the limit is refused here, before it is sent, with the same caution the server gives.
(() => {
  const meta = n => +(document.querySelector('meta[name=' + n + ']') || {}).content || 0;
  const sel = 'input[type=file][data-upload], input[type=file][data-code]';
  const limitOf = i => ('code' in i.dataset) ? meta('pogi-code-mb') : meta('pogi-upload-mb');
  const hint = i => 'Up to ' + limitOf(i) + ' MB' + (('code' in i.dataset) ? ' zipped' : '') + ': pogi is a local test emulator.';
  const noteFor = i => (i.closest('label') || i).nextElementSibling;
  const notes = () => {
    document.querySelectorAll(sel).forEach(i => {
      if (i.dataset.noted) return;
      const n = document.createElement('small');
      i.dataset.noted = '1';
      n.className = 'upload-note';
      n.setAttribute('role', 'status');
      n.textContent = hint(i);
      (i.closest('label') || i).after(n);
    });
  };
  document.addEventListener('DOMContentLoaded', notes);
  document.addEventListener('htmx:afterSettle', notes);
  // capture phase: this runs before the autosubmit handler sends the form
  document.addEventListener('change', e => {
    const i = e.target;
    if (!i.matches || !i.matches(sel)) return;
    const mb = limitOf(i), code = 'code' in i.dataset;
    if (!mb) return;
    const size = [...i.files].reduce((a, f) => a + f.size, 0), n = noteFor(i);
    if (size <= mb * 1048576) { if (n) { n.classList.remove('bad'); n.textContent = hint(i); } return; }
    e.stopPropagation();
    i.value = '';
    if (n) {
      const z = code ? ' zipped' : '', big = Math.ceil(size / 1048576);
      n.classList.add('bad');
      n.textContent = 'pogi is a local emulator for tests, so ' + (code ? 'Lambda code is' : 'uploads are') + ' limited to ' + mb + ' MB' + z +
        ' (this one is ' + big + ' MB' + z + '). Raise the limit with --max-upload-mb or server.maxUploadMB in pogi.yaml.';
    }
  }, true);
})();
