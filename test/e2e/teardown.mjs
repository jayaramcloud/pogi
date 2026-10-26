import { rmSync } from 'node:fs';
import { dir } from './playwright.config.mjs';

export default () => rmSync(dir, { recursive: true, force: true });
