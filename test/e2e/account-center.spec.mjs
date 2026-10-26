// The account center: pick a Google project from the list, see a hidden topic appear, reset to defaults.
import { test, expect, origin } from './helpers.mjs';

async function topic(project, name) {
  const r = await fetch(`${origin}/v1/projects/${project}/topics/${name}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: '{}' });
  if (!r.ok) throw new Error(`topic ${name}: ${r.status} ${await r.text()}`);
}

test.beforeEach(async () => {
  const r = await fetch(`${origin}/_pogi/api/reset?service=pubsub`, { method: 'POST' });
  if (!r.ok) throw new Error(`reset pubsub: ${r.status}`);
});

test('pick a project from the list, see its topic, then reset to defaults', async ({ page }) => {
  await topic('pogi', 'mine');
  await topic('other', 'theirs');
  await page.goto('/_pogi/resources/pubsub');
  await expect(page.getByText('topics/mine')).toBeVisible();
  await expect(page.getByText('topics/theirs')).toHaveCount(0);
  await expect(page.locator('.scopebar')).toContainText('1 topic in other projects or locations');

  await page.locator('#scopeBtn').click();
  await expect(page.locator('#dl-project option[value="other"]')).toHaveCount(1); // the list is filled from what pogi holds
  await page.locator('#sc-project').fill('other');
  await page.getByRole('button', { name: 'Apply' }).click();
  await expect(page.getByText('topics/theirs')).toBeVisible();
  await expect(page.getByText('topics/mine')).toHaveCount(0);

  await page.locator('#scopeBtn').click();
  await page.getByRole('button', { name: 'Reset to defaults' }).click();
  await expect(page.getByText('topics/mine')).toBeVisible();
  await expect(page.getByText('topics/theirs')).toHaveCount(0);
});

test('the pill shows the picked cloud with its project and location', async ({ page }) => {
  await page.goto('/_pogi/');
  await page.locator('#scopeBtn').click();
  await page.getByRole('tab', { name: 'GCP' }).click();
  await expect(page.locator('#scopeBtn')).toContainText('Google Cloud · pogi · all locations');
});
