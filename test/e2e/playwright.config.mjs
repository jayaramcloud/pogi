import { defineConfig } from '@playwright/test';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

// Never 7644: that is the port a person's own pogi uses.
const port = process.env.POGI_E2E_PORT || '17645';
// It goes into the shell command below, so it must be a plain port number.
if (!/^[1-9][0-9]{0,4}$/.test(port) || +port > 65535 || port === '7644') {
  throw new Error(`POGI_E2E_PORT must be a port number other than 7644, got ${JSON.stringify(port)}`);
}
export const dir = join(tmpdir(), `pogi-e2e-${port}`);

export default defineConfig({
  testDir: '.',
  testMatch: '*.spec.mjs',
  workers: 1,
  fullyParallel: false,
  reporter: 'list',
  globalTeardown: './teardown.mjs',
  use: { baseURL: `http://localhost:${port}`, trace: 'retain-on-failure' },
  projects: [{ name: 'chromium', use: { browserName: 'chromium' } }],
  webServer: {
    // Builds pogi from this repo, then replaces the shell with it (exec), so Playwright's kill reaches pogi.
    // Every service is on except Blob, Queue and Table Storage, which listen on fixed ports (10000-10002) that
    // another pogi on this machine may hold. The other side ports are off for the same reason.
    command: `rm -rf ${dir} && mkdir -p ${dir} && go build -o ${dir}/pogi ./cmd/pogi && exec ${dir}/pogi start --services -azblob,-azqueue,-aztable --port ${port} --amqp-port -1 --mqtt-port -1 --smtp-port -1 --transfer-port -1 --data-dir ${dir}/data`,
    cwd: '../..',
    url: `http://localhost:${port}/_pogi/api/info`,
    reuseExistingServer: false, // a taken port fails loudly
    timeout: 300_000,
    gracefulShutdown: { signal: 'SIGTERM', timeout: 5000 },
  },
});
