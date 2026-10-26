import { test, expect, makeBucket, putObject, gotoS3, bucketLink, origin } from './helpers.mjs';

async function openBucket(page, name = 'files') {
  await gotoS3(page);
  await bucketLink(page, name).click();
  const drawer = page.locator('#drawer.on');
  await expect(drawer.getByRole('heading', { name })).toBeVisible();
  return drawer;
}

test('key prefixes show as folders and open', async ({ page }) => {
  await makeBucket('files');
  await putObject('files', 'top.txt');
  await putObject('files', 'docs/a.txt');
  await putObject('files', 'docs/deep/b.txt');
  const drawer = await openBucket(page);
  await expect(drawer.getByRole('link', { name: 'top.txt' })).toBeVisible();
  await expect(drawer.getByRole('link', { name: 'a.txt' })).toHaveCount(0);
  await drawer.getByRole('link', { name: 'docs/' }).click();
  await expect(drawer.getByRole('link', { name: 'a.txt' })).toBeVisible();
  await drawer.getByRole('link', { name: 'deep/' }).click();
  await expect(drawer.getByRole('link', { name: 'b.txt' })).toBeVisible();
});

for (const key of ['with space.txt', 'ünï-日本語.txt', 'what?.txt', 'hash#tag.txt', 'q?a#b c.txt']) {
  test(`the key "${key}" lists and opens`, async ({ page }) => {
    await makeBucket('files');
    await putObject('files', key, `body of ${key}`);
    const drawer = await openBucket(page);
    const link = drawer.getByRole('link', { name: key, exact: true });
    await expect(link).toBeVisible();
    // The link's target is what a click opens in a new tab: it must serve this object.
    const res = await page.request.get(new URL(await link.getAttribute('href'), origin).href);
    expect(res.status()).toBe(200);
    expect(await res.text()).toBe(`body of ${key}`);
  });
}

test('a 0-byte object shows 0 B', async ({ page }) => {
  await makeBucket('files');
  await putObject('files', 'empty.txt', '');
  const drawer = await openBucket(page);
  await expect(drawer.locator('tr', { hasText: 'empty.txt' })).toContainText('0 B');
});

test('a key that looks like HTML shows as plain text and does not run', async ({ page }) => {
  const key = '<img src=x onerror=window.__xss=1>.txt';
  await makeBucket('files');
  await putObject('files', key);
  const drawer = await openBucket(page);
  await expect(drawer.getByRole('link', { name: key, exact: true })).toBeVisible();
  await expect(drawer.locator('img')).toHaveCount(0);
  expect(await page.evaluate(() => window.__xss)).toBeUndefined();
});
