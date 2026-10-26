import { test, expect, makeBucket, makeBuckets, dropBucket, gotoS3, bucketLink, filterBox, live } from './helpers.mjs';

test('an empty page shows the first bucket made through the API', async ({ page }) => {
  await gotoS3(page);
  await expect(page.getByRole('heading', { name: /No buckets/ })).toBeVisible();
  await makeBucket('first');
  await expect(bucketLink(page, 'first')).toBeVisible(live);
});

test('a new bucket appears in place, without a reload', async ({ page }) => {
  await makeBucket('one');
  await gotoS3(page);
  await page.evaluate(() => { window.__marker = 'same page'; });
  await makeBucket('two');
  await expect(bucketLink(page, 'two')).toBeVisible(live);
  await expect(bucketLink(page, 'one')).toBeVisible();
  expect(await page.evaluate(() => window.__marker)).toBe('same page');
});

test('a deleted bucket disappears', async ({ page }) => {
  await makeBuckets(['keep', 'gone']);
  await gotoS3(page);
  await expect(bucketLink(page, 'gone')).toBeVisible();
  await dropBucket('gone');
  await expect(bucketLink(page, 'gone')).toHaveCount(0, live);
  await expect(bucketLink(page, 'keep')).toBeVisible();
});

test('deleting the last bucket brings the empty state back', async ({ page }) => {
  await makeBucket('only');
  await gotoS3(page);
  await expect(bucketLink(page, 'only')).toBeVisible();
  await dropBucket('only');
  await expect(page.getByRole('heading', { name: /No buckets/ })).toBeVisible(live);
});

test('text typed in the filter is kept after a refresh', async ({ page }) => {
  await makeBuckets(['apple', 'banana']);
  await gotoS3(page);
  await filterBox(page).fill('app');
  await expect(bucketLink(page, 'banana')).toBeHidden();
  await filterBox(page).blur(); // focus in the box would pause refreshing
  await makeBucket('apricot-app');
  await expect(bucketLink(page, 'apricot-app')).toBeVisible(live);
  await expect(filterBox(page)).toHaveValue('app');
  await expect(bucketLink(page, 'banana')).toBeHidden(); // the filter still applies to the fresh rows
});

test('two open tabs both update', async ({ page, context }) => {
  await makeBucket('base');
  await gotoS3(page);
  const other = await context.newPage();
  await gotoS3(other);
  await makeBucket('shared');
  await expect(bucketLink(page, 'shared')).toBeVisible(live);
  await expect(bucketLink(other, 'shared')).toBeVisible(live);
});
