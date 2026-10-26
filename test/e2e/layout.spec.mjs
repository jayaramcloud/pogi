// The console supports tablet and desktop widths, 768px and up. At each of these widths no page
// may scroll sideways: everything fits, or a table hides the columns it can spare. The footer sits at the
// bottom edge of a short page.
import { test, expect, makeBucket, makeQueue, origin } from './helpers.mjs';

const widths = [768, 1024, 1280, 1440, 1920];
const pages = ['/_pogi/', '/_pogi/requests', '/_pogi/flows', '/_pogi/services', '/_pogi/map',
  '/_pogi/resources/s3', '/_pogi/resources/sqs', '/_pogi/resources/secretsmanager', '/_pogi/resources/logs',
  '/_pogi/resources/arm', '/_pogi/resources/eks', '/_pogi/sql', '/_pogi/logs', '/_pogi/logs?kind=gcp',
  '/_pogi/secrets', '/_pogi/iam', '/_pogi/cost', '/_pogi/terraform', '/_pogi/inbox', '/_pogi/chaos',
  '/_pogi/snapshots', '/_pogi/connect'];

// One resource of each kind whose long names and counts once pushed tables past the page's edge.
async function seed() {
  const aws = (target, version, body) => fetch(origin + '/', { method: 'POST', body: JSON.stringify(body),
    headers: { 'Content-Type': `application/x-amz-json-${version}`, 'X-Amz-Target': target } });
  await makeBucket('layout-bucket');
  await makeQueue('layout-queue');
  await aws('secretsmanager.CreateSecret', '1.1', { Name: 'layout-db-password', SecretString: 'x' });
  await aws('Logs_20140328.CreateLogGroup', '1.1', { logGroupName: '/layout/web' });
  await fetch(origin + '/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/layout?api-version=2021-04-01', {
    method: 'PUT', body: JSON.stringify({ location: 'eastus' }),
    headers: { Authorization: 'Bearer x', 'Content-Type': 'application/json' } });
}

for (const width of widths) {
  test(`no page scrolls sideways at ${width}px`, async ({ page }) => {
    test.setTimeout(120_000);
    await seed();
    await page.setViewportSize({ width, height: 900 });
    const wide = [];
    for (const p of pages) {
      await page.goto(p);
      const over = await page.evaluate(() => {
        const m = document.querySelector('main');
        return Math.max(document.documentElement.scrollWidth - innerWidth, m ? m.scrollWidth - m.clientWidth : 0);
      });
      if (over > 1) wide.push(`${p} +${over}px`);
    }
    expect(wide).toEqual([]);
  });
}

test('the footer sits at the bottom edge of a short page', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto('/_pogi/snapshots');
  // the credit's text, not its box: padding once kept the text 64px above the edge
  const gap = await page.evaluate(() =>
    document.querySelector('main').getBoundingClientRect().bottom - document.querySelector('footer.credit a').getBoundingClientRect().bottom);
  expect(gap).toBeGreaterThanOrEqual(0);
  expect(gap).toBeLessThanOrEqual(16);
});

// The primary button stays on the first row of its toolbar, at the right end, from 1024px up.
for (const width of [1024, 1280, 1440, 1920]) {
  test(`the Run button is on the first toolbar row at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    for (const p of ['/_pogi/sql', '/_pogi/logs', '/_pogi/logs?kind=kql', '/_pogi/logs?kind=gcp']) {
      await page.goto(p);
      const r = await page.evaluate(() => {
        const run = document.querySelector('.toolbar .btn.primary').getBoundingClientRect();
        const first = document.querySelector('.toolbar > :first-child').getBoundingClientRect();
        const bar = document.querySelector('.toolbar').getBoundingClientRect();
        return { sameRow: Math.abs(run.top - first.top) < 4, gapRight: bar.right - run.right };
      });
      expect(r.sameRow, p).toBe(true);
      expect(r.gapRight, p).toBeLessThanOrEqual(2);
    }
  });
}
