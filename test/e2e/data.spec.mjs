// Data actions: DynamoDB items, Pub/Sub messages and Kinesis records.
import { test as base, expect, origin } from './helpers.mjs';

const test = base.extend({
  data: [async ({}, use) => {
    for (const svc of ['dynamodb', 'pubsub', 'kinesis']) {
      const r = await fetch(`${origin}/_pogi/api/reset?service=${svc}`, { method: 'POST' });
      if (!r.ok) throw new Error(`reset ${svc}: ${r.status}`);
    }
    await use();
  }, { auto: true }],
});

async function json(method, path, body, target, type = 'application/json') {
  const headers = { 'Content-Type': type, ...(target ? { 'X-Amz-Target': target } : {}) };
  const r = await fetch(origin + path, { method, headers, body: JSON.stringify(body) });
  if (!r.ok) throw new Error(`${method} ${path}: ${r.status} ${await r.text()}`);
}

test('add, edit and delete a DynamoDB item', async ({ page }) => {
  await json('POST', '/', { TableName: 'Users', BillingMode: 'PAY_PER_REQUEST',
    AttributeDefinitions: [{ AttributeName: 'id', AttributeType: 'S' }],
    KeySchema: [{ AttributeName: 'id', KeyType: 'HASH' }] }, 'DynamoDB_20120810.CreateTable', 'application/x-amz-json-1.0');
  await page.goto('/_pogi/resources/dynamodb?tab=query');
  await page.getByText('Add item', { exact: true }).click();
  const box = page.getByLabel(/^Item \(plain JSON/);

  await box.fill('{"name": "Kim"}');
  await page.getByRole('button', { name: 'Save item' }).click();
  await expect(page.getByRole('alert')).toContainText('missing the key attribute');

  await box.fill('{"id": "u3", "name": "Kim"}');
  await page.getByRole('button', { name: 'Save item' }).click();
  await expect(page.getByRole('cell', { name: 'Kim', exact: true })).toBeVisible();

  await page.getByText('Edit', { exact: true }).click();
  await page.getByLabel('Item as JSON').fill('{"id": "u3", "name": "Kimberly"}');
  await page.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(page.getByRole('cell', { name: 'Kimberly', exact: true })).toBeVisible();

  await page.getByRole('button', { name: 'Delete' }).click();
  await page.locator('#confirm-ok').click();
  await expect(page.getByRole('cell', { name: 'Kimberly' })).toHaveCount(0);
  await expect(page.getByText('0 items')).toBeVisible();
});

test('pull Pub/Sub messages and ack one', async ({ page }) => {
  await json('PUT', '/v1/projects/pogi/topics/jobs', {});
  await json('PUT', '/v1/projects/pogi/subscriptions/worker', { topic: 'projects/pogi/topics/jobs', ackDeadlineSeconds: 60 });
  await json('POST', '/v1/projects/pogi/topics/jobs:publish', { messages: [{ data: Buffer.from('hello').toString('base64'), attributes: { k: 'v' } }] });
  await page.goto('/_pogi/resources/pubsub?tab=messages');
  await page.getByRole('button', { name: 'Pull', exact: true }).click();
  await expect(page.getByRole('cell', { name: 'hello' })).toBeVisible();
  await expect(page.getByText('k=v')).toBeVisible();
  await page.getByLabel('Ack this message').check();
  await page.getByRole('button', { name: 'Ack ticked' }).click();
  await expect(page.getByText('1 message acked')).toBeVisible();
  await page.getByRole('button', { name: 'Pull and ack' }).click();
  await expect(page.getByText('No messages to pull')).toBeVisible();
});

test('put and get a Kinesis record', async ({ page }) => {
  await json('POST', '/', { StreamName: 'events', ShardCount: 1 }, 'Kinesis_20131202.CreateStream', 'application/x-amz-json-1.1');
  await page.goto('/_pogi/resources/kinesis?tab=records');
  await page.getByLabel('Partition key').fill('k1');
  await page.getByLabel('Data', { exact: true }).fill('hello');
  await page.getByRole('button', { name: 'Put record' }).click();
  await expect(page.getByText('Record put on events')).toBeVisible();
  await page.getByRole('button', { name: 'Get records' }).click();
  await expect(page.getByRole('cell', { name: 'hello', exact: true })).toBeVisible();
});
