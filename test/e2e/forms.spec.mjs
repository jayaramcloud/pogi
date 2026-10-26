import { test, expect, makeBucket, putObject, tryDropBucket, gotoS3, bucketLink, live } from './helpers.mjs';

async function openCreate(page) {
  await gotoS3(page);
  await page.getByRole('link', { name: 'Create bucket' }).first().click();
  const drawer = page.locator('#drawer.on');
  await expect(drawer.getByRole('heading', { name: 'Create bucket' })).toBeVisible();
  return drawer;
}

test('a good name creates the bucket', async ({ page }) => {
  const drawer = await openCreate(page);
  await drawer.getByLabel('Bucket').fill('made-in-console');
  await drawer.getByRole('button', { name: 'Create bucket' }).click();
  await expect(bucketLink(page, 'made-in-console')).toBeVisible(live);
});

test('an invalid name is stopped by the form, and by the server when the form is skipped', async ({ page }) => {
  const drawer = await openCreate(page);
  const name = drawer.getByLabel('Bucket');
  await name.fill('Bad_Name');
  await drawer.getByRole('button', { name: 'Create bucket' }).click();
  expect(await name.evaluate((e) => e.validity.patternMismatch)).toBe(true); // the browser's own check
  await expect(drawer.getByRole('alert')).toHaveCount(0);
  await name.evaluate((e) => e.removeAttribute('pattern')); // as a script or an old browser would
  await drawer.getByRole('button', { name: 'Create bucket' }).click();
  await expect(drawer.getByRole('alert')).toContainText('Bucket name is invalid');
  await expect(drawer.getByRole('alert')).toContainText('Use 3–63 characters');
});

test('a duplicate name gives an error', async ({ page }) => {
  await makeBucket('taken');
  const drawer = await openCreate(page);
  await drawer.getByLabel('Bucket').fill('taken');
  await drawer.getByRole('button', { name: 'Create bucket' }).click();
  await expect(drawer.getByRole('alert')).toContainText('BucketAlreadyExists');
});

// The console's "Delete bucket" says "and everything in it", so it empties the bucket first.
// BucketNotEmpty is what the S3 API says, and the console must keep showing the bucket after it.
test('the API refuses to delete a bucket that has objects, and the bucket stays', async ({ page }) => {
  await makeBucket('full');
  await putObject('full', 'a.txt');
  await gotoS3(page);
  const r = await tryDropBucket('full');
  expect(r.status).toBe(409);
  expect(r.text).toContain('BucketNotEmpty');
  await page.reload();
  await expect(bucketLink(page, 'full')).toBeVisible();
});

test('the console deletes a bucket with everything in it, after a confirm', async ({ page }) => {
  await makeBucket('full');
  await putObject('full', 'a.txt');
  await gotoS3(page);
  await bucketLink(page, 'full').click();
  const drawer = page.locator('#drawer.on');
  await drawer.getByRole('button', { name: /Delete bucket/ }).click();
  await page.locator('#confirm-ok').click();
  await expect(bucketLink(page, 'full')).toHaveCount(0, live);
  await expect(page.locator('#drawer.on')).toHaveCount(0);
});
