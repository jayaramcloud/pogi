import { test, expect, origin } from './helpers.mjs';

// One call that fails and counts: an invalid bucket name is a 400.
const fail = (n) => fetch(`${origin}/Bad_Bucket${n}`, { method: 'PUT' });
const toast = (page) => page.locator('#toast');

test('the bell has real buttons that toggle, and a link to all errors', async ({ page }) => {
  await page.goto(`${origin}/_pogi/`);
  await fail(1);
  await page.getByRole('button', { name: 'Notifications' }).click();
  const bell = page.locator('#bell');
  await expect(bell.getByText('S3 CreateBucket failed').first()).toBeVisible();
  const chip = bell.getByRole('button', { name: 'SDK errors' });
  await expect(chip).toHaveAttribute('aria-pressed', 'true');
  await chip.focus();
  await page.keyboard.press('Enter');
  await expect(bell.getByRole('button', { name: 'SDK errors' })).toHaveAttribute('aria-pressed', 'false');
  await expect(bell.getByRole('link', { name: 'See all errors in Activity' })).toHaveAttribute('href', '/_pogi/requests?errors=1');
  await expect(bell.getByRole('button', { name: 'Mark all read' })).toBeVisible();
});

test('several new errors make one summary toast that opens the bell', async ({ page }) => {
  await page.goto(`${origin}/_pogi/`);
  await fail(1); await fail(2); await fail(3);
  await expect(toast(page)).toContainText('3 calls failed', { timeout: 10000 });
  await toast(page).getByRole('link', { name: 'View' }).click();
  await expect(page.locator('#bell')).toHaveClass(/on/);
});

test('toasts queue, and hovering holds one', async ({ page }) => {
  await page.goto(`${origin}/_pogi/`);
  await page.evaluate(() => { window.pogi.toast('First'); window.pogi.toast('Second'); });
  await expect(toast(page)).toContainText('First');
  await toast(page).hover();
  await page.waitForTimeout(3500); // longer than the 2.5 s timer
  await expect(toast(page)).toContainText('First');
  await page.mouse.move(5, 5);
  await expect(toast(page)).toContainText('Second', { timeout: 6000 });
});
