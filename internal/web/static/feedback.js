// pogi console: feedback while things happen. Busy buttons, pogi's own confirm dialog, rows that refresh
// while a resource is changing, and the top-bar indicator for background work (image downloads, starts).
// Everything is delegated on document, so it survives hx-boost body swaps.
(() => {
  const $ = (s, el = document) => el.querySelector(s);
  const $$ = (s, el = document) => [...el.querySelectorAll(s)];

  // ---- busy buttons: one place for every post (htmx, boosted and plain forms) ----
  // While a form posts, its submit buttons are locked (so a double click cannot send it twice), the pressed one
  // says what is going on, and everything is put back when the post ends, whether it worked or not.
  const SUBMIT = 'button:not([type=button]):not([type=reset]), input[type=submit]';
  const VERBS = [[/^(create|add|new)\b/i, 'Creating…'], [/^(save|apply|update|tag)\b/i, 'Saving…'], [/^(delete|remove|reset|clear)\b/i, 'Deleting…']];
  const held = new Map(); // button -> what to put back
  function lock(b, label) {
    if (held.has(b)) return;
    held.set(b, { disabled: b.disabled, html: label ? b.innerHTML : null, focus: document.activeElement === b });
    b.disabled = true; b.setAttribute('aria-busy', 'true');
    if (label) b.textContent = label;
  }
  function unlock(b) {
    const s = held.get(b); if (!s) return;
    held.delete(b);
    b.disabled = s.disabled; b.removeAttribute('aria-busy');
    if (s.html !== null) b.innerHTML = s.html;
    if (s.focus && b.isConnected) b.focus(); // a disabled button loses focus: give it back
  }
  const unlockAll = els => els.forEach(unlock);
  const busyLabel = b => {
    if (b.dataset && b.dataset.busy) return b.dataset.busy;
    const t = (b.textContent || '').trim(); // an icon-only button keeps its look
    const v = VERBS.find(([re]) => re.test(t));
    return v ? v[1] : '';
  };
  // the buttons a post locks, and the one that was pressed
  function lockForm(form, pressed) {
    // form.elements also holds buttons outside the form that name it with form="…", like a drawer's footer button
    const all = form.elements ? [...form.elements].filter(b => b.matches && b.matches(SUBMIT)) : $$(SUBMIT, form);
    const main = pressed && all.includes(pressed) ? pressed : all.find(b => b.contains && b.contains(document.activeElement)) || all.find(b => b.classList.contains('primary')) || all[0];
    all.forEach(b => lock(b, b === main ? busyLabel(b) : ''));
    return all;
  }
  const targets = new WeakMap(); // the element htmx sent from -> the buttons it locked
  document.addEventListener('htmx:beforeRequest', e => {
    const d = e.detail, elt = d.elt || e.target, cfg = d.requestConfig || {};
    if (!elt || String(cfg.verb || 'post').toLowerCase() === 'get') return;
    const ev = cfg.triggeringEvent || {};
    const btns = elt.tagName === 'FORM' ? lockForm(elt, ev.submitter) : elt.tagName === 'BUTTON' ? (lock(elt, busyLabel(elt)), [elt]) : [];
    if (btns.length) targets.set(elt, btns);
  });
  const done = e => {
    const d = e.detail || {}, elt = d.elt || e.target, btns = targets.get(elt);
    if (!btns) return;
    targets.delete(elt);
    // a finished form swaps the next page in just after: stay locked until then, so a click in between cannot post again
    const x = d.xhr;
    if (d.successful && x && x.getResponseHeader && (x.getResponseHeader('HX-Location') || x.getResponseHeader('HX-Redirect'))) {
      setTimeout(() => unlockAll(btns), 4000);
      return;
    }
    unlockAll(btns);
  };
  ['htmx:afterRequest', 'htmx:sendError', 'htmx:sendAbort', 'htmx:timeout', 'htmx:responseError'].forEach(n => document.addEventListener(n, done));
  // a plain form post (hx-boost="false"): the page is about to be replaced. Locked after this turn, so the button's own value still goes.
  document.addEventListener('submit', e => {
    const f = e.target;
    if (e.defaultPrevented || !f.tagName || f.tagName !== 'FORM' || String(f.method).toLowerCase() !== 'post') return;
    setTimeout(() => {
      const btns = lockForm(f, e.submitter);
      setTimeout(() => unlockAll(btns), 15000); // a download keeps the page: do not stay locked for good
    });
  });
  addEventListener('pageshow', e => { if (e.persisted) [...held.keys()].forEach(unlock); }); // back button: the page comes from the cache

  // ---- pogi's confirm dialog: every hx-confirm uses it ----
  const DANGER = /^(delete|remove|reset|clear|stop|terminate)\b/i;
  function ask(question, elt, go) {
    const dialog = document.getElementById('confirm'), ok = $('#confirm-ok', dialog);
    const verb = (question.match(DANGER) || [''])[0];
    $('#confirm-title', dialog).textContent = question;
    $('#confirm-msg', dialog).textContent = verb && !/undone/i.test(question) ? "This can't be undone." : '';
    ok.textContent = verb ? verb[0].toUpperCase() + verb.slice(1).toLowerCase() : 'Confirm';
    ok.className = 'btn ' + (verb ? 'danger solid' : 'primary');
    // a menu item is gone once the menu closes: focus goes back to the button that opened the menu
    const opener = elt && elt.closest && elt.closest('#menu') ? $('[data-menu][aria-expanded="true"]') : elt;
    dialog.returnValue = '';
    dialog.addEventListener('close', () => {
      const yes = dialog.returnValue === 'ok'; // Esc and Cancel leave it empty: nothing is sent
      dialog.returnValue = '';
      if (opener && opener.isConnected) opener.focus();
      if (yes) go();
    }, { once: true });
    dialog.showModal();
  }
  document.addEventListener('htmx:confirm', e => {
    const d = e.detail, dialog = document.getElementById('confirm');
    if (!d.question || !dialog || typeof dialog.showModal !== 'function') return; // htmx falls back to the browser's own
    e.preventDefault();
    ask(d.question, d.elt, () => d.issueRequest(true));
  });

  // ---- live rows: a resource table refreshes itself every 2 seconds while any row is changing ----
  // The one list of in-between statuses (any capitalisation). A row in one of these is not finished yet.
  const TRANSIENT = ['creating', 'modifying', 'deleting', 'backing-up', 'provisioning', 'pending_create', 'updating'];
  const inFlux = s => TRANSIENT.includes(String(s).trim().toLowerCase());
  // a status cell showing a task keeps the server's status in data-status: judge by that, not by the task text
  const settled = tbody => !$$('td', tbody).some(td => inFlux(td.dataset.status || td.textContent));
  // polling waits while someone is working in the table, a menu is open, or rows are ticked: a swap would undo that
  const paused = el => !!(window.pogi && window.pogi.busy && window.pogi.busy('#' + el.id)) || !!$('#menu.on') || !!$('input[name=row]:checked', el) || el.classList.contains('htmx-request');
  function tickRows() {
    if (document.hidden) return;
    $$('[data-live-rows]').forEach(el => {
      if (settled(el)) return; // never started on a settled table, and it stops by itself when the last row settles
      loadTasks(); // fresh progress for the status cells
      if (!paused(el)) htmx.trigger(el, 'pogi-rows');
    });
  }
  setInterval(tickRows, 2000);
  // a page without the table (the last row went away) would swap the table out: reload instead
  document.addEventListener('htmx:beforeSwap', e => {
    const t = e.detail.target;
    if (t && t.hasAttribute && t.hasAttribute('data-live-rows') && !String(e.detail.serverResponse).includes('id="rowsbody"')) { e.detail.shouldSwap = false; location.reload(); }
  });
  // a swap drops the filter's hidden rows and the tasks' text: put both back
  document.addEventListener('htmx:afterSettle', () => {
    const f = $('[data-filter=rows]'); if (f && f.value) f.dispatchEvent(new Event('input', { bubbles: true }));
    showTasks();
  });

  // ---- background tasks: /_pogi/tasks, polled every 4 seconds while the page is visible ----
  let tasks = [];
  const mb = n => Math.round(n / 1e6);
  const plain = s => String(s || '').split(' — ')[0]; // the plain sentence; raw Docker text may follow
  function taskText(t) {
    if (t.state === 'failed') return 'Failed: ' + plain(t.error);
    if (t.kind === 'start') return 'Starting…';
    if (t.kind === 'download') return t.total > 0 ? `Downloading ${t.what} · ${mb(t.done)} of ${mb(t.total)} MB` : `Downloading ${t.what}…`;
    return 'Working…';
  }
  // the status cell of a resource's row shows what pogi is doing for it
  function showTasks() {
    tasks.forEach(t => {
      if (t.state === 'done' || !t.service || !t.resource) return;
      const tb = $$('[data-live-rows]').find(x => x.dataset.svc === t.service); if (!tb) return;
      const row = $$('tr', tb).find(r => { const n = $('td.name', r); return n && n.textContent.trim() === t.resource; }); if (!row) return;
      const th = $$('thead th', tb.closest('table') || document), cells = $$('td', row);
      const i = th.findIndex(h => /^(status|state)$/i.test(h.textContent.trim()));
      const cell = i >= 0 && cells[i] ? cells[i] : cells.find(c => inFlux(c.textContent) || /^failed$/i.test(c.textContent.trim()));
      if (!cell) return;
      if (!cell.dataset.status) cell.dataset.status = cell.textContent.trim(); // keeps the table polling until the server's row settles
      cell.textContent = taskText(t);
      cell.className = t.state === 'failed' ? 'taskfail' : 'tasktext';
      if (t.state === 'failed') cell.title = t.error;
    });
  }
  function showIndicator() {
    const btn = $('#taskBtn'), pop = $('#taskpop'); if (!btn || !pop) return;
    const run = tasks.filter(t => t.state === 'running');
    btn.hidden = !run.length;
    btn.setAttribute('aria-label', run.length ? `${run.length} background ${run.length > 1 ? 'tasks' : 'task'} running` : 'Background tasks');
    $('.tn', btn).textContent = run.length > 1 ? run.length : '';
    if (!run.length) closePop();
    const ul = $('ul', pop); ul.replaceChildren();
    run.forEach(t => {
      const li = document.createElement('li'), n = document.createElement('span'), w = document.createElement('span');
      n.textContent = [t.service, t.resource].filter(Boolean).join(' · ');
      w.className = 'what'; w.textContent = taskText(t);
      li.append(n, w);
      if (t.total > 0) { const p = document.createElement('progress'); p.max = t.total; p.value = t.done; p.setAttribute('aria-label', 'Progress of ' + t.what); li.append(p); }
      ul.append(li);
    });
  }
  function loadTasks() {
    if (document.hidden) return;
    return fetch('/_pogi/tasks', { headers: { Accept: 'application/json' } }).then(r => r.ok ? r.json() : { tasks: [] }).then(j => {
      tasks = Array.isArray(j.tasks) ? j.tasks : [];
      showIndicator(); showTasks();
    }).catch(() => {});
  }
  setInterval(loadTasks, 4000);

  // ---- a service page notices resources made elsewhere (CLI, SDK, Terraform): its empty state says "It shows up live" ----
  // /_pogi/rev changes when the service gains or loses a resource or answers a call. A changed rev swaps in fresh rows,
  // or reloads a page that has no table yet. Someone typing, a drawer or dialog, or rows being worked on: try next time.
  function checkRev() {
    const ph = $('[data-rev-svc]'); if (!ph || document.hidden) return;
    return fetch('/_pogi/rev?svc=' + encodeURIComponent(ph.dataset.revSvc), { headers: { Accept: 'application/json' } })
      .then(r => r.ok ? r.json() : {}).then(j => {
        if (typeof j.rev !== 'string' || !j.rev || j.rev === ph.dataset.rev) return;
        const busy = !!$('dialog[open]') || !!$('#drawer.on, #palette.on, #pop.on, #menu.on, #keys.on') || /^(INPUT|TEXTAREA|SELECT)$/.test((document.activeElement || {}).tagName || '');
        const tb = $$('[data-live-rows]')[0];
        if (busy || (tb && paused(tb))) return;
        ph.dataset.rev = j.rev;
        if (tb) htmx.trigger(tb, 'pogi-rows'); else location.reload();
      }).catch(() => {});
  }
  setInterval(checkRev, 4000);
  document.addEventListener('visibilitychange', () => { if (!document.hidden) loadTasks(); });
  document.addEventListener('DOMContentLoaded', loadTasks);
  document.addEventListener('htmx:afterSettle', showIndicator); // a boosted page swap brings a fresh, hidden indicator

  // ---- the indicator's popover: a plain, non-modal list (Esc, a click outside, or the button closes it) ----
  function closePop() {
    const pop = $('#taskpop'), btn = $('#taskBtn'); if (!pop || !pop.classList.contains('on')) return;
    pop.classList.remove('on'); if (btn) { btn.setAttribute('aria-expanded', 'false'); if (document.activeElement === document.body || pop.contains(document.activeElement)) btn.focus(); }
  }
  document.addEventListener('click', e => {
    const btn = e.target.closest && e.target.closest('#taskBtn'), pop = $('#taskpop'); if (!pop) return;
    if (btn) {
      e.stopPropagation();
      if (pop.classList.contains('on')) return closePop();
      const b = btn.getBoundingClientRect();
      pop.style.top = b.bottom + 6 + 'px'; pop.style.left = Math.max(12, Math.min(innerWidth - 372, b.right - 360)) + 'px';
      pop.classList.add('on'); btn.setAttribute('aria-expanded', 'true'); pop.tabIndex = -1; pop.focus();
    } else if (!e.target.closest('#taskpop')) closePop();
  });
  document.addEventListener('keydown', e => { if (e.key === 'Escape') closePop(); });

  window.pogiFeedback = { taskText, inFlux, settled, tickRows, showTasks, checkRev, _tasks: t => { tasks = t; } };
})();
