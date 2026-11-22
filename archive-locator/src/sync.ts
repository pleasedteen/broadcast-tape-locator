/**
 * 同步入口（采集器守护进程，主备双站）。
 *
 * 环境变量：
 *   SITE_URL_A  主模拟站点地址（默认 http://127.0.0.1:4181）
 *   SITE_URL_B  备模拟站点地址（默认 http://127.0.0.1:4183）
 *   SITE_URL    单站兼容写法：只给一个地址时退化为仅主站
 *   DB_PATH     SQLite 数据库路径（默认 <项目>/data/archive.db）
 *   POLL_MS     轮询间隔毫秒（默认 500）
 */
import { mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { runCollector } from './collector.ts';

const ROOT = join(fileURLToPath(import.meta.url), '..', '..');
const urlA = process.env.SITE_URL_A ?? process.env.SITE_URL ?? 'http://127.0.0.1:4181';
const urlB = process.env.SITE_URL_B ?? 'http://127.0.0.1:4183';
const dbPath = process.env.DB_PATH ?? join(ROOT, 'data', 'archive.db');
const pollMs = Number(process.env.POLL_MS ?? 500);

// 显式传 SITE_URL 且未传 SITE_URL_B 时退化为单站；默认主备双站
const sites = process.env.SITE_URL && !process.env.SITE_URL_B
  ? [{ replica: 'A', url: urlA }]
  : [{ replica: 'A', url: urlA }, { replica: 'B', url: urlB }];

mkdirSync(dirname(dbPath), { recursive: true });
console.log(`[sync] 主站 ${urlA} · 备站 ${sites.length > 1 ? urlB : '（未配置）'} · 数据库 ${dbPath} · 轮询 ${pollMs}ms`);
await runCollector({ sites, dbPath, pollMs });
