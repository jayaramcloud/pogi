// Azure pages list what their Create makes: a Key Vault from its own page shows in the Vaults tab, first.
import { test, expect, origin } from './helpers.mjs';

test.beforeEach(async () => {
  const r = await fetch(`${origin}/_pogi/api/reset?service=keyvault`, { method: 'POST' });
  if (!r.ok) throw new Error(`reset keyvault: ${r.status}`);
});

test('a vault created from the Key Vault page is listed there', async ({ page }) => {
  await page.goto('/_pogi/resources/keyvault');
  await page.locator('[data-create]').first().click();
  await page.locator('#dform [name=name]').fill('vault-e2e');
  await page.getByRole('button', { name: 'Create or update' }).click();
  await expect(page).toHaveURL(/\/_pogi\/resources\/keyvault/);
  await expect(page.getByText('vault-e2e').first()).toBeVisible();
});
