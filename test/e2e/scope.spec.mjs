import { execFileSync } from 'node:child_process';
import { test, expect, makeBucket, gotoS3, bucketLink, live, origin } from './helpers.mjs';

const onPath = (cmd) => { try { execFileSync('which', [cmd], { stdio: 'ignore' }); return true; } catch { return false; } };

test('a bucket made in another region shows under the default region', async ({ page }) => {
  // S3 lists every bucket of the account, whatever its region. This fails until the "global tables" change is merged.
  await makeBucket('mumbai', { region: 'ap-south-1' });
  await gotoS3(page);
  await expect(bucketLink(page, 'mumbai')).toBeVisible();
});

test('a bucket of another account is hidden, until that account is picked', async ({ page, context }) => {
  await makeBucket('mine');
  await makeBucket('theirs', { account: '111122223333' });
  await gotoS3(page);
  await expect(bucketLink(page, 'mine')).toBeVisible();
  await expect(bucketLink(page, 'theirs')).toHaveCount(0);
  await context.addCookies([{ name: 'pogi-account', value: '111122223333', url: origin }]);
  await gotoS3(page);
  await expect(bucketLink(page, 'theirs')).toBeVisible();
  await expect(bucketLink(page, 'mine')).toHaveCount(0);
});

test('a bucket made with the AWS CLI appears', async ({ page }) => {
  test.skip(!onPath('aws'), 'the aws CLI is not on PATH');
  await gotoS3(page);
  // Only these variables: the CLI must never read a person's own AWS config.
  const env = {
    PATH: process.env.PATH,
    AWS_ACCESS_KEY_ID: 'test',
    AWS_SECRET_ACCESS_KEY: 'test',
    AWS_REGION: 'us-east-1',
    AWS_CONFIG_FILE: '/dev/null',
    AWS_SHARED_CREDENTIALS_FILE: '/dev/null',
    AWS_EC2_METADATA_DISABLED: 'true',
  };
  execFileSync('aws', ['--endpoint-url', origin, 's3', 'mb', 's3://from-cli'], { env, stdio: 'pipe', timeout: 30_000 });
  await expect(bucketLink(page, 'from-cli')).toBeVisible(live);
});
