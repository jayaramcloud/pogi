// Azure create: a Service Bus namespace from its own page (Blob is off in this pogi, so no storage account),
// then a resource group on Resource Manager that lists what is in it.
import { test, expect, origin } from './helpers.mjs';

test.beforeEach(async () => {
  for (const svc of ['servicebus', 'arm']) {
    const r = await fetch(`${origin}/_pogi/api/reset?service=${svc}`, { method: 'POST' });
    if (!r.ok) throw new Error(`reset ${svc}: ${r.status}`);
  }
});

test('create a namespace from the Service Bus page and see it there', async ({ page }) => {
  await page.goto('/_pogi/resources/servicebus');
  await page.locator('[data-create]').first().click();
  await page.getByRole('menuitem', { name: 'Create namespace' }).click();
  await page.locator('#dform [name=name]').fill('bus-e2e');
  await page.getByRole('button', { name: 'Create or update' }).click();
  await expect(page).toHaveURL(/\/_pogi\/resources\/servicebus/);
  await expect(page.getByText('bus-e2e').first()).toBeVisible();
});

test('a new resource group lists its resources in its drawer', async ({ page }) => {
  await page.goto('/_pogi/resources/arm');
  await page.locator('[data-create]').first().click();
  await page.getByRole('menuitem', { name: 'Create resource group' }).click();
  await page.locator('#dform [name=name]').fill('web-e2e');
  await page.getByRole('button', { name: 'Create resource group' }).click();
  await expect(page.getByText('Resource group web-e2e created')).toBeVisible();
  const id = '/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/web-e2e/providers/Microsoft.ServiceBus/namespaces/bus2';
  const r = await fetch(`${origin}${id}?api-version=2024-03-01`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: '{"location":"eastus","sku":{"name":"Standard","tier":"Standard"}}' });
  if (!r.ok) throw new Error(`namespace: ${r.status} ${await r.text()}`);
  await page.goto('/_pogi/resources/arm/Resource%20groups/web-e2e');
  await expect(page.getByRole('link', { name: 'bus2' })).toBeVisible();
});
