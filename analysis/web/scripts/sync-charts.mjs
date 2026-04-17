#!/usr/bin/env node
// Copies chart PNGs from analysis/{output,deep_analysis,network_analysis}
// into public/charts/ so Astro serves them at /charts/*.
// Idempotent; skips files whose mtime hasn't changed.

import { mkdir, copyFile, readdir, stat } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const WEB_ROOT = resolve(HERE, '..');
const ANALYSIS = resolve(WEB_ROOT, '..');
const PUBLIC_CHARTS = resolve(WEB_ROOT, 'public', 'charts');

async function ensureDir(p) {
  await mkdir(p, { recursive: true });
}

async function copyIfNewer(src, dst) {
  if (!existsSync(src)) return false;
  let need = true;
  if (existsSync(dst)) {
    const [s, d] = await Promise.all([stat(src), stat(dst)]);
    need = s.mtimeMs > d.mtimeMs;
  }
  if (need) {
    await ensureDir(dirname(dst));
    await copyFile(src, dst);
    return true;
  }
  return false;
}

async function copyTreeFlat(srcDir, dstDir, pattern = /\.png$/) {
  if (!existsSync(srcDir)) return 0;
  const entries = await readdir(srcDir, { withFileTypes: true });
  let n = 0;
  for (const e of entries) {
    if (!e.isFile()) continue;
    if (!pattern.test(e.name)) continue;
    if (await copyIfNewer(join(srcDir, e.name), join(dstDir, e.name))) n++;
  }
  return n;
}

async function copySteps(srcRoot, dstRoot) {
  if (!existsSync(srcRoot)) return 0;
  const dirs = await readdir(srcRoot, { withFileTypes: true });
  let n = 0;
  for (const d of dirs) {
    if (!d.isDirectory() || !d.name.startsWith('step_')) continue;
    const srcChart = join(srcRoot, d.name, 'chart.png');
    const dstChart = join(dstRoot, d.name, 'chart.png');
    if (await copyIfNewer(srcChart, dstChart)) n++;
    const srcFig = join(srcRoot, d.name, 'figure1_overview.png');
    const dstFig = join(dstRoot, d.name, 'figure1_overview.png');
    if (await copyIfNewer(srcFig, dstFig)) n++;
  }
  return n;
}

console.log('[sync-charts] starting...');
await ensureDir(PUBLIC_CHARTS);

const flat = await copyTreeFlat(
  join(ANALYSIS, 'output'),
  join(PUBLIC_CHARTS, 'output'),
);
console.log(`[sync-charts] output/*.png copied: ${flat}`);

const deep = await copySteps(
  join(ANALYSIS, 'deep_analysis'),
  join(PUBLIC_CHARTS, 'deep'),
);
await copyIfNewer(
  join(ANALYSIS, 'deep_analysis', 'summary_overview.png'),
  join(PUBLIC_CHARTS, 'deep', 'summary_overview.png'),
);
console.log(`[sync-charts] deep_analysis step charts copied: ${deep}`);

const net = await copySteps(
  join(ANALYSIS, 'network_analysis'),
  join(PUBLIC_CHARTS, 'network'),
);
console.log(`[sync-charts] network_analysis step charts copied: ${net}`);

const rir = await copySteps(
  join(ANALYSIS, 'rir_enrichment'),
  join(PUBLIC_CHARTS, 'rir'),
);
console.log(`[sync-charts] rir_enrichment step charts copied: ${rir}`);

console.log('[sync-charts] done.');
