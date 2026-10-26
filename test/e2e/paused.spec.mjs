import { test, expect, makeBucket, makeQueue, gotoS3, bucketLink, filterBox, hint, sawNewRev, live } from './helpers.mjs';

// While a person is working on the page, a swap would undo it. The page waits, and says so.

test('focus in the filter pauses the refresh until it is left', async ({ page }) => {
  await makeBucket('old');
  await gotoS3(page);
  await filterBox(page).focus();
  await makeBucket('fresh');
  await sawNewRev(page);
  await expect(hint(page)).toBeVisible();
  await expect(bucketLink(page, 'fresh')).toHaveCount(0);
  await filterBox(page).blur();
  await expect(bucketLink(page, 'fresh')).toBeVisible(live);
  await expect(hint(page)).toBeHidden();
});

test('an open drawer pauses the refresh until it is closed', async ({ page }) => {
  await makeBucket('old');
  await gotoS3(page);
  await bucketLink(page, 'old').click();
  await expect(page.locator('#drawer.on')).toBeVisible();
  await makeBucket('fresh');
  await sawNewRev(page);
  await expect(hint(page)).toBeVisible();
  await expect(bucketLink(page, 'fresh')).toHaveCount(0);
  await page.keyboard.press('Escape');
  await expect(page.locator('#drawer.on')).toHaveCount(0);
  // Closing the drawer gives focus back to the row's link, and the page counts a focused row as work in progress.
  await page.evaluate(() => document.activeElement.blur());
  await expect(bucketLink(page, 'fresh')).toBeVisible(live);
  await expect(hint(page)).toBeHidden();
});

// S3's table has no tick boxes, so this one uses SQS, whose queue table has them.
test('a ticked row pauses the refresh until it is unticked', async ({ page }) => {
  await makeQueue('old-queue');
  await page.goto('/_pogi/resources/sqs');
  const row = page.locator('input[name=row]').first();
  await row.check();
  await makeQueue('fresh-queue');
  await sawNewRev(page);
  await expect(hint(page)).toBeVisible();
  await expect(page.getByRole('link', { name: 'fresh-queue' })).toHaveCount(0);
  await row.uncheck();
  await row.blur(); // a focused box also counts as work in progress
  await expect(page.getByRole('link', { name: 'fresh-queue' })).toBeVisible(live);
  await expect(hint(page)).toBeHidden();
});
