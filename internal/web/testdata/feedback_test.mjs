// Runs static/feedback.js against a tiny fake DOM (no browser needed): node feedback_test.mjs <path to feedback.js>
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const src = fs.readFileSync(process.argv[2], 'utf8');

// ---- a fake DOM: elements answer the selectors the script asks, from a map ----
class El {
  constructor(p = {}) { Object.assign(this, { tagName: 'DIV', textContent: '', q: {}, attrs: {}, dataset: {}, isConnected: true, hidden: false, disabled: false, children: [], classes: new Set(), style: {} }, p); this.classList = { add: c => this.classes.add(c), remove: c => this.classes.delete(c), contains: c => this.classes.has(c) }; }
  querySelectorAll(s) { return this.q[s] || []; }
  querySelector(s) { return (this.q[s] || [])[0] || null; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  removeAttribute(k) { delete this.attrs[k]; }
  getBoundingClientRect() { return { bottom: 0, right: 400 }; }
  contains(x) { return x === this; }
  closest() { return null; }
  focus() { doc.activeElement = this; }
  append(...c) { this.children.push(...c); }
  replaceChildren() { this.children = []; }
  get innerHTML() { return this.textContent; }
  set innerHTML(v) { this.textContent = v; }
}
const button = (text, p = {}) => new El({ tagName: 'BUTTON', textContent: text, ...p });
const listeners = {}, intervals = [], timers = [];
const byId = {}, bySel = {};
const doc = {
  hidden: false, activeElement: null,
  addEventListener: (t, f) => (listeners[t] ||= []).push(f),
  getElementById: id => byId[id] || null,
  querySelector: s => bySel[s] || null,
  querySelectorAll: s => bySel[s] ? [].concat(bySel[s]) : [],
  createElement: () => new El(),
};
const posts = [];
const ctx = {
  document: doc, window: {}, htmx: { trigger: (el, n) => posts.push(['trigger', n]) }, location: { reload: () => posts.push(['reload']) },
  setInterval: (f, ms) => intervals.push({ f, ms }), setTimeout: f => timers.push(f), addEventListener: () => {}, innerWidth: 1000,
  Event: class { constructor(t) { this.type = t; } }, fetch: () => Promise.resolve({ ok: false }),
};
ctx.window = ctx;
vm.createContext(ctx);
vm.runInContext(src, ctx);
const fire = (type, e = {}) => (listeners[type] || []).forEach(f => f(e));
const flush = () => { while (timers.length) timers.shift()(); };
const F = ctx.pogiFeedback;
let n = 0;
const test = (name, f) => { f(); n++; console.log('ok', name); };

test('a busy form: buttons lock, the pressed one names the work, an error puts everything back', () => {
  const save = button('Create bucket', { dataset: {}, classes: new Set(['primary']) }), cancel = button('Cancel');
  const form = new El({ tagName: 'FORM', q: { 'button:not([type=button]):not([type=reset]), input[type=submit]': [cancel, save] } });
  doc.activeElement = save;
  fire('htmx:beforeRequest', { detail: { elt: form, requestConfig: { verb: 'post' } } });
  assert.equal(save.disabled, true); assert.equal(cancel.disabled, true);
  assert.equal(save.attrs['aria-busy'], 'true'); assert.equal(save.textContent, 'Creating…'); assert.equal(cancel.textContent, 'Cancel');
  fire('htmx:afterRequest', { detail: { elt: form, successful: false } }); // the post failed
  assert.equal(save.disabled, false); assert.equal(cancel.disabled, false);
  assert.equal(save.attrs['aria-busy'], undefined); assert.equal(save.textContent, 'Create bucket');
});

test('a submit button outside the form (form="…", a drawer footer) is locked too', () => {
  // found 2026-10-21: the Create bucket button in the drawer footer never showed it was busy
  const outside = button('Create bucket', { dataset: {}, matches: () => true }), field = new El({ tagName: 'INPUT', matches: () => false });
  const form = new El({ tagName: 'FORM', elements: [field, outside] });
  fire('htmx:beforeRequest', { detail: { elt: form, requestConfig: { verb: 'post' } } });
  assert.equal(outside.disabled, true); assert.equal(outside.textContent, 'Creating…');
  fire('htmx:afterRequest', { detail: { elt: form, successful: false } });
  assert.equal(outside.disabled, false); assert.equal(outside.textContent, 'Create bucket');
});

test('data-busy wins; a get locks nothing; a network error restores too', () => {
  const b = button('Go', { dataset: { busy: 'Sending…' } });
  const form = new El({ tagName: 'FORM', q: { 'button:not([type=button]):not([type=reset]), input[type=submit]': [b] } });
  fire('htmx:beforeRequest', { detail: { elt: form, requestConfig: { verb: 'get' } } });
  assert.equal(b.disabled, false);
  fire('htmx:beforeRequest', { detail: { elt: form, requestConfig: { verb: 'post' } } });
  assert.equal(b.textContent, 'Sending…');
  fire('htmx:sendError', { detail: { elt: form } });
  assert.equal(b.disabled, false); assert.equal(b.textContent, 'Go');
});

test('a double click sends one post', () => {
  const b = button('Delete', {});
  let sent = 0;
  const click = () => { if (!b.disabled) { sent++; fire('htmx:beforeRequest', { detail: { elt: b, requestConfig: { verb: 'post' } } }); } }; // a disabled button gets no click
  click(); click();
  assert.equal(sent, 1);
  fire('htmx:afterRequest', { detail: { elt: b, successful: true, xhr: { getResponseHeader: () => null } } });
  assert.equal(b.disabled, false);
});

test('a post that ends in a page swap stays locked until the swap', () => {
  const b = button('Save');
  fire('htmx:beforeRequest', { detail: { elt: b, requestConfig: { verb: 'post' } } });
  fire('htmx:afterRequest', { detail: { elt: b, successful: true, xhr: { getResponseHeader: h => h === 'HX-Location' ? '{}' : null } } });
  assert.equal(b.disabled, true);
  flush(); // the 4 second guard
  assert.equal(b.disabled, false);
});

test('the confirm dialog: Esc sends nothing, OK sends once, no question leaves htmx alone', () => {
  const dialog = new El({ q: {} }), title = new El(), msg = new El(), ok = new El();
  Object.assign(dialog, { returnValue: '', showModal() { this.shown = true; }, addEventListener(t, f) { this.onclose = f; } });
  dialog.q = { '#confirm-title': [title], '#confirm-msg': [msg], '#confirm-ok': [ok] };
  byId.confirm = dialog;
  const opener = button('Delete');
  let issued = 0, prevented = 0;
  const ev = q => ({ preventDefault: () => prevented++, detail: { question: q, elt: opener, issueRequest: yes => { assert.equal(yes, true); issued++; } } });
  fire('htmx:confirm', ev('Delete jobs?'));
  assert.equal(prevented, 1); assert.ok(dialog.shown); assert.equal(title.textContent, 'Delete jobs?'); assert.equal(ok.textContent, 'Delete'); assert.match(msg.textContent, /undone/);
  doc.activeElement = null;
  dialog.onclose(); // Esc: returnValue stays empty
  assert.equal(issued, 0); assert.equal(doc.activeElement, opener); // and focus is back on the button
  fire('htmx:confirm', ev('Delete jobs?'));
  dialog.returnValue = 'ok'; dialog.onclose();
  assert.equal(issued, 1);
  prevented = 0; fire('htmx:confirm', ev(undefined));
  assert.equal(prevented, 0);
});

const cell = text => new El({ tagName: 'TD', textContent: text });
const liveTable = (cells) => {
  const tb = new El({ id: 'rowsbody', dataset: { svc: 'rds' }, q: { td: cells } });
  bySel['[data-live-rows]'] = [tb];
  return tb;
};

test('live rows: poll while a row changes, stop when all settle, never start on a settled table', () => {
  const tick = intervals.find(i => i.ms === 2000).f;
  posts.length = 0;
  const cells = [cell('uxdemo'), cell('creating')];
  liveTable(cells);
  tick(); assert.deepEqual(posts, [['trigger', 'pogi-rows']]);
  cells[1].textContent = 'available'; posts.length = 0;
  tick(); tick(); assert.deepEqual(posts, []); // settled: no more polling
  liveTable([cell('a'), cell('available')]); tick(); assert.deepEqual(posts, []); // never started
  for (const s of ['modifying', 'deleting', 'backing-up', 'PROVISIONING', 'PENDING_CREATE', 'CREATING', 'UPDATING', 'DELETING', 'Updating', 'Creating']) assert.equal(F.inFlux(s), true, s);
  for (const s of ['available', 'ACTIVE', 'failed', '']) assert.equal(F.inFlux(s), false, s);
  doc.hidden = true; liveTable([cell('x'), cell('creating')]); tick(); assert.deepEqual(posts, []); // a hidden tab waits
  doc.hidden = false;
});

test('live rows: a table the user is working in is not swapped', () => {
  const tick = intervals.find(i => i.ms === 2000).f;
  posts.length = 0;
  const tb = liveTable([cell('x'), cell('creating')]);
  tb.q['input[name=row]:checked'] = [new El()]; // a ticked row
  tick(); assert.deepEqual(posts, []);
});

test('the task text, and the status cell of the row', () => {
  assert.equal(F.taskText({ kind: 'download', what: 'mariadb:11.4', done: 140e6, total: 410e6, state: 'running' }), 'Downloading mariadb:11.4 · 140 of 410 MB');
  assert.equal(F.taskText({ kind: 'download', what: 'mariadb:11.4', done: 0, total: 0, state: 'running' }), 'Downloading mariadb:11.4…'); // total unknown
  assert.equal(F.taskText({ kind: 'start', state: 'running' }), 'Starting…');
  assert.equal(F.taskText({ kind: 'download', state: 'failed', error: "Docker isn't running — Cannot connect to the Docker daemon" }), "Failed: Docker isn't running");
  const status = cell('creating'), nameCell = cell('uxdemo');
  const row = new El({ q: { 'td.name': [nameCell], td: [nameCell, status] } });
  const th = [new El({ textContent: 'Identifier' }), new El({ textContent: 'Status' })];
  const tb = new El({ dataset: { svc: 'rds' }, q: { tr: [row] }, closest: () => new El({ q: { 'thead th': th } }) });
  bySel['[data-live-rows]'] = [tb];
  F._tasks([{ service: 'rds', resource: 'uxdemo', kind: 'download', what: 'mariadb:11.4', done: 0, total: 0, state: 'running' }]);
  F.showTasks();
  assert.equal(status.textContent, 'Downloading mariadb:11.4…'); assert.equal(nameCell.textContent, 'uxdemo');
  // the task text hides "creating", but the table keeps polling until the server's row settles (found 2026-10-21:
  // the row froze on the last download text after the database was already available)
  const tick = intervals.find(i => i.ms === 2000).f;
  posts.length = 0; tb.q.td = [nameCell, status];
  tick(); assert.deepEqual(posts, [['trigger', 'pogi-rows']]);
  F._tasks([{ service: 'rds', resource: 'uxdemo', kind: 'download', what: 'mariadb:99', state: 'failed', error: 'image not found: mariadb:99 — raw' }]);
  F.showTasks();
  assert.equal(status.textContent, 'Failed: image not found: mariadb:99'); assert.equal(status.className, 'taskfail');
  F._tasks([]);
});

const tn = new El(), ul = new El(), btn = new El({ hidden: true, q: { '.tn': [tn] } }), pop = new El({ q: { ul: [ul] } });
bySel['#taskBtn'] = btn; bySel['#taskpop'] = pop;
const load = async tasks => { ctx.fetch = () => Promise.resolve({ ok: true, json: async () => ({ tasks }) }); intervals.find(i => i.ms === 4000).f(); await new Promise(r => setImmediate(r)); };
await load([{ service: 'rds', resource: 'a', kind: 'download', what: 'x:1', done: 5e6, total: 10e6, state: 'running' }, { service: 'rds', resource: 'b', kind: 'start', state: 'running' }, { service: 'rds', resource: 'c', state: 'done' }]);
test('the indicator shows while tasks run, lists each one, and hides when none', () => {
  assert.equal(btn.hidden, false); assert.equal(tn.textContent, 2); assert.equal(ul.children.length, 2);
});
await load([{ service: 'rds', resource: 'a', state: 'failed', error: 'x' }]);
test('a failed task alone does not keep the indicator up', () => { assert.equal(btn.hidden, true); assert.equal(ul.children.length, 0); });
await load(undefined); // an endpoint that is missing or garbled
test('garbage from /_pogi/tasks is ignored', () => assert.equal(btn.hidden, true));

console.log(n + ' passed');

// a service page picks up resources made elsewhere (its empty state promises "It shows up live"; found 2026-10-21
// that nothing refreshed): new rows swap in, an empty page reloads, and someone busy on the page is left alone
const revFetch = rev => { ctx.fetch = () => Promise.resolve({ ok: true, json: async () => ({ rev }) }); };
const ph = new El({ dataset: { revSvc: 'rds', rev: '1-3' } });
bySel['[data-rev-svc]'] = ph; bySel['[data-live-rows]'] = []; posts.length = 0;
revFetch('1-3'); await F.checkRev();
test('an unchanged rev does nothing', () => assert.deepEqual(posts, []));
revFetch('2-4'); await F.checkRev();
test('a new resource reloads an empty page', () => { assert.deepEqual(posts, [['reload']]); assert.equal(ph.dataset.rev, '2-4'); });
posts.length = 0; bySel['[data-live-rows]'] = [new El({ id: 'rowsbody' })];
revFetch('3-5'); await F.checkRev();
test('with a table, fresh rows swap in', () => assert.deepEqual(posts, [['trigger', 'pogi-rows']]));
posts.length = 0; doc.activeElement = new El({ tagName: 'INPUT' });
revFetch('4-6'); await F.checkRev();
test('someone typing is left alone, and the old rev stays so it retries', () => { assert.deepEqual(posts, []); assert.equal(ph.dataset.rev, '3-5'); });
const note = new El({ hidden: true }); bySel['[data-rev-paused]'] = note;
posts.length = 0; doc.activeElement = new El({ tagName: 'INPUT' }); revFetch('5-7'); await F.checkRev();
test('a paused refresh shows the hint', () => { assert.deepEqual(posts, []); assert.equal(note.hidden, false); });
doc.activeElement = null; await F.checkRev();
test('the next free tick refreshes and hides the hint', () => { assert.deepEqual(posts, [['trigger', 'pogi-rows']]); assert.equal(note.hidden, true); assert.equal(ph.dataset.rev, '5-7'); });
posts.length = 0; doc.activeElement = null; revFetch(undefined); await F.checkRev();
test('garbage from /_pogi/rev is ignored', () => assert.deepEqual(posts, []));
delete bySel['[data-rev-svc]'];

