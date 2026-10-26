import { test, expect, origin, live } from './helpers.mjs';

test.beforeEach(async () => {
  const r = await fetch(`${origin}/_pogi/api/reset?service=container`, { method: 'POST' });
  if (!r.ok) throw new Error(`reset container: ${r.status}`);
});

test('the GKE form starts with the scope\'s location and creates a cluster from a name alone', async ({ page }) => {
  await page.goto('/_pogi/resources/container');
  await expect(page.getByRole('heading', { name: /No clusters/ })).toBeVisible();
  await page.getByRole('button', { name: 'Create…' }).first().click();
  await page.getByRole('menuitem', { name: 'Create cluster' }).click();
  const drawer = page.locator('#drawer.on');
  await expect(drawer.getByRole('heading', { name: 'Create cluster' })).toBeVisible();
  await expect(drawer.locator('[name="location"]')).toHaveValue('us-central1');
  await expect(drawer.locator('[name="nodeCount"]')).toHaveValue('1');
  await drawer.locator('[name="name"]').fill('c1');
  await drawer.getByRole('button', { name: 'Create cluster' }).click();
  await expect(page.getByRole('link', { name: 'c1' })).toBeVisible(live);
});
