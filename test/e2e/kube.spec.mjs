// The short Create cluster forms. No test here starts a cluster: they check the defaults and the errors.
import { test, expect } from './helpers.mjs';

async function openCreate(page, svc) {
  await page.goto(`/_pogi/resources/${svc}`);
  const menu = page.getByRole('button', { name: 'Create…' }).first();
  if (await menu.count()) { // a service with more forms keeps them in a menu; one form is the button itself
    await menu.click();
    await page.getByRole('menuitem', { name: 'Create cluster' }).click();
  } else {
    await page.getByRole('link', { name: 'Create cluster' }).or(page.getByRole('button', { name: 'Create cluster' })).first().click();
  }
  const drawer = page.locator('#drawer.on');
  await expect(drawer.getByRole('heading', { name: 'Create cluster' })).toBeVisible();
  return drawer;
}

test('AKS has its own Create cluster, with the group and location of the scope', async ({ page }) => {
  const drawer = await openCreate(page, 'azaks');
  await expect(drawer.locator('[name="resourceGroup"]')).toHaveValue('pogi');
  await expect(drawer.locator('[name="location"]')).toHaveValue('eastus');
  await expect(drawer.locator('[name="nodeCount"]')).toHaveValue('1');
  await drawer.locator('[name="name"]').fill('aks1');
  await drawer.locator('[name="resourceGroup"]').fill('bad/group');
  await drawer.getByRole('button', { name: 'Create cluster' }).click();
  await expect(drawer.locator('#apierr')).toContainText('Resource group "bad/group" is not valid');
});

test('EKS asks for a name, a version and a role, with the subnets optional', async ({ page }) => {
  const drawer = await openCreate(page, 'eks');
  await expect(drawer.locator('[name="version"]')).toHaveValue('1.35');
  await expect(drawer.locator('[name="roleArn"]')).toHaveValue('arn:aws:iam::000000000000:role/eks');
  await drawer.locator('summary', { hasText: /^Optional/ }).click(); // the subnets sit behind Optional
  await expect(drawer.getByText('pogi makes a VPC and two subnets first')).toBeVisible();
  await drawer.locator('[name="name"]').fill('bad name!');
  await drawer.getByRole('button', { name: 'Create cluster' }).click();
  await expect(drawer.locator('#apierr')).toContainText('must start with a letter or digit');
});

test('GKE refuses a bad location', async ({ page }) => {
  const drawer = await openCreate(page, 'container');
  await drawer.locator('[name="name"]').fill('g1');
  await drawer.locator('[name="location"]').fill('../x');
  await drawer.getByRole('button', { name: 'Create cluster' }).click();
  await expect(drawer.locator('#apierr')).toContainText('is not valid');
});
