import { test, expect, makeBucket, gotoS3, filterBox } from './helpers.mjs';

test('200 buckets render, and the filter narrows them', async ({ page }) => {
  test.setTimeout(90_000);
  const names = Array.from({ length: 200 }, (_, i) => `scale-${String(i).padStart(3, '0')}`);
  for (let i = 0; i < names.length; i += 20) await Promise.all(names.slice(i, i + 20).map((n) => makeBucket(n)));
  await gotoS3(page);
  const rows = page.locator('#rowsbody tr:visible');
  await expect(rows).toHaveCount(200);
  await filterBox(page).fill('scale-15');
  await expect(rows).toHaveCount(10); // scale-150 to scale-159
  await expect(page.locator('#rowsbody').getByRole('link', { name: 'scale-155' })).toBeVisible();
  await expect(page.locator('#rowsbody').getByRole('link', { name: 'scale-042' })).toBeHidden();
});
