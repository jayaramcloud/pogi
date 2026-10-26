// Shared by the specs: a test that starts every time from empty S3 and SQS, and plain S3 and SQS calls.
// The calls are unsigned: pogi accepts them, so no SDK is needed.
import { test as base, expect } from '@playwright/test';

export { expect };
export const port = process.env.POGI_E2E_PORT || '17645';
export const origin = `http://localhost:${port}`;

export const test = base.extend({
  // Every test starts with no buckets and no queues. Each test also gets a fresh browser context, so no cookies carry over.
  clean: [async ({}, use) => {
    for (const svc of ['s3', 'sqs']) {
      const r = await fetch(`${origin}/_pogi/api/reset?service=${svc}`, { method: 'POST' });
      if (!r.ok) throw new Error(`reset ${svc}: ${r.status}`);
    }
    await use();
  }, { auto: true }],
});

async function call(method, path, { body, headers } = {}) {
  const r = await fetch(origin + path, { method, body, headers });
  const text = await r.text();
  return { status: r.status, text };
}

async function ok(method, path, opts) {
  const r = await call(method, path, opts);
  if (r.status >= 300) throw new Error(`${method} ${path}: ${r.status} ${r.text}`);
  return r;
}

// S3. A bucket made in another region needs its LocationConstraint; an account is a 12-digit access key.
export const makeBucket = (name, { region, account } = {}) => ok('PUT', `/${name}`, {
  body: region ? `<CreateBucketConfiguration><LocationConstraint>${region}</LocationConstraint></CreateBucketConfiguration>` : undefined,
  headers: account ? { Authorization: `AWS4-HMAC-SHA256 Credential=${account}/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=x` } : {},
});
export const dropBucket = (name) => ok('DELETE', `/${name}`);
export const tryDropBucket = (name) => call('DELETE', `/${name}`);
export const putObject = (bucket, key, body = 'x') => ok('PUT', `/${bucket}/${encodeURIComponent(key).replace(/%2F/g, '/')}`, { body });
export const makeBuckets = async (names) => { for (const n of names) await makeBucket(n); };

// SQS (JSON protocol), the one service that has ticked rows on its page.
export const makeQueue = (name) => ok('POST', '/', {
  body: JSON.stringify({ QueueName: name }),
  headers: { 'Content-Type': 'application/x-amz-json-1.0', 'X-Amz-Target': 'AmazonSQS.CreateQueue' },
});

// Resolves when the page's own poll has seen a rev other than the one it shows. After this, a change made
// before the call has reached the page's check, so "it did not update" can be asserted without a sleep.
export async function sawNewRev(page) {
  const shown = await page.locator('[data-rev]').getAttribute('data-rev');
  await page.waitForResponse(async (r) => {
    if (!r.url().includes('/_pogi/rev')) return false;
    const j = await r.json().catch(() => ({}));
    return !!j.rev && j.rev !== shown;
  }, { timeout: 15_000 });
}

export const gotoS3 = (page) => page.goto('/_pogi/resources/s3');
export const bucketLink = (page, name) => page.locator('#rowsbody').getByRole('link', { name, exact: true });
export const filterBox = (page) => page.getByRole('textbox', { name: 'Filter' });
export const hint = (page) => page.locator('[data-rev-paused]');
export const live = { timeout: 10_000 };
