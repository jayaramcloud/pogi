// Event and function actions: SNS subscriptions, EventBridge rule targets, Step Functions executions, Lambda.
import { execFileSync } from 'node:child_process';
import { mkdtempSync, writeFileSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test, expect, origin, live } from './helpers.mjs';

const account = '000000000000';

async function aws(target, op, body) {
  const r = await fetch(origin + '/', {
    method: 'POST', body: JSON.stringify(body),
    headers: { 'Content-Type': 'application/x-amz-json-1.0', 'X-Amz-Target': `${target}.${op}` },
  });
  const text = await r.text();
  if (!r.ok) throw new Error(`${op}: ${r.status} ${text}`);
  return text ? JSON.parse(text) : {};
}

test.beforeEach(async () => {
  for (const svc of ['sns', 'events', 'stepfunctions', 'lambda']) {
    const r = await fetch(`${origin}/_pogi/api/reset?service=${svc}`, { method: 'POST' });
    if (!r.ok) throw new Error(`reset ${svc}: ${r.status}`);
  }
});

const drawer = (page) => page.locator('#drawer.on');
const confirm = (page) => page.locator('#confirm-ok').click();

test('subscribe a queue to a topic, then unsubscribe it from the row menu', async ({ page }) => {
  await aws('AmazonSimpleNotificationService', 'CreateTopic', { Name: 'alerts' });
  await page.goto('/_pogi/resources/sns');
  await page.getByRole('button', { name: /Create/ }).first().click();
  await page.locator('#menu').getByText('Create subscription').click();
  await drawer(page).locator('input[name="Endpoint"]').fill('arn:aws:sqs:us-east-1:000000000000:inbox');
  await drawer(page).locator('select[name="Protocol"]').selectOption('sqs');
  await drawer(page).locator('select[name="TopicArn"]').selectOption({ index: 1 });
  await drawer(page).getByRole('button', { name: 'Create subscription' }).last().click();
  await page.goto('/_pogi/resources/sns?tab=Subscriptions');
  await expect(page.getByText('arn:aws:sqs:us-east-1:000000000000:inbox')).toBeVisible(live);
  await page.getByRole('button', { name: 'Row actions' }).click();
  await page.getByRole('menuitem', { name: 'Delete' }).click();
  await confirm(page);
  await expect(page.getByText('arn:aws:sqs:us-east-1:000000000000:inbox')).toHaveCount(0, live);
});

test('a rule made in the console has its target, and deleting it removes both', async ({ page }) => {
  await page.goto('/_pogi/new/events/PutRule');
  await page.locator('input[name="Name"]').fill('orders');
  await page.locator('summary', { hasText: 'Optional' }).click();
  await page.locator('textarea[name="EventPattern"]').fill('{"source":["shop"]}');
  await page.getByRole('button', { name: '+ add' }).first().click();
  await page.locator('input[name="Targets.0.Id"]').fill('t1');
  await page.locator('input[name="Targets.0.Arn"]').fill('arn:aws:sqs:us-east-1:000000000000:sink');
  await page.getByRole('button', { name: 'Put rule' }).last().click();
  await expect(page).toHaveURL(/resources\/events/, live);
  const out = await aws('AWSEvents', 'ListTargetsByRule', { Rule: 'orders' });
  expect(out.Targets.map((t) => t.Id)).toEqual(['t1']);
  await page.goto('/_pogi/resources/events?tab=Rules');
  await page.getByRole('button', { name: 'Row actions' }).click();
  await page.getByRole('menuitem', { name: 'Delete' }).click();
  await confirm(page);
  await expect(page.getByText('orders', { exact: true })).toHaveCount(0, live);
});

test('start an execution, see it running, then stop it', async ({ page }) => {
  await aws('AWSStepFunctions', 'CreateStateMachine', {
    name: 'slow', roleArn: `arn:aws:iam::${account}:role/r`,
    definition: '{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":60,"End":true}}}',
  });
  await page.goto('/_pogi/resources/stepfunctions?tab=exec');
  await page.locator('summary', { hasText: 'Start execution' }).click();
  await page.getByLabel(/Name \(optional\)/).fill('run1');
  await page.getByRole('button', { name: 'Start execution' }).click();
  await expect(page.locator('.badge', { hasText: 'RUNNING' })).toBeVisible(live);
  await page.getByRole('button', { name: 'Stop', exact: true }).click();
  await confirm(page);
  await expect(page.locator('.badge', { hasText: 'ABORTED' })).toBeVisible(live);
});

test('change a function\'s environment variables, then delete it', async ({ page }) => {
  const dir = mkdtempSync(join(tmpdir(), 'pogi-fn-'));
  writeFileSync(join(dir, 'app.py'), 'def handler(e, c):\n    return {"ok": True}\n');
  execFileSync('zip', ['-q', 'code.zip', 'app.py'], { cwd: dir });
  const made = await fetch(origin + '/2015-03-31/functions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `AWS4-HMAC-SHA256 Credential=${account}/20260101/us-east-1/lambda/aws4_request, SignedHeaders=host, Signature=x` },
    body: JSON.stringify({ FunctionName: 'hello', Runtime: 'python3.12', Handler: 'app.handler', Role: `arn:aws:iam::${account}:role/lambda`,
      Code: { ZipFile: readFileSync(join(dir, 'code.zip')).toString('base64') } }),
  });
  expect(made.status).toBeLessThan(300);
  await page.goto(`/_pogi/lambda/${account}/us-east-1/hello`);
  await expect(page.locator('textarea[name=payload]')).toHaveValue('{"hello": "pogi"}');
  await page.locator('summary', { hasText: 'Environment variables' }).click();
  await page.locator('input[name=k0]').fill('STAGE');
  await page.locator('input[name=v0]').fill('dev');
  await page.getByRole('button', { name: 'Save variables' }).click();
  await expect(page.locator('input[name=k0]')).toHaveValue('STAGE', live);
  await page.goto('/_pogi/resources/lambda');
  await page.getByRole('button', { name: 'Row actions' }).click();
  await page.getByRole('menuitem', { name: 'Delete' }).click();
  await confirm(page);
  await expect(page.locator('#rowsbody').getByRole('link', { name: 'hello', exact: true })).toHaveCount(0, live);
});
