/**
 * 同步入口（采集器守护进程）。
 *
 * 环境变量：
 *   SITE_URL  模拟站点地址（默认 http://127.0.0.1:4181）
 *   DB_PATH   SQLite 数据库路径（默认 <项目>/data/archive.db）
 *   POLL_MS   轮询间隔毫秒（默认 500）
 */
import { mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { runCollector } from './collector.ts';

const ROOT = join(fileURLToPath(import.meta.url), '..', '..');
const siteUrl = process.env.SITE_URL ?? 'http://127.0.0.1:4181';
const dbPath = process.env.DB_PATH ?? join(ROOT, 'data', 'archive.db');
const pollMs = Number(process.env.POLL_MS ?? 500);

mkdirSync(dirname(dbPath), { recursive: true });
console.log(`[sync] 站点 ${siteUrl} · 数据库 ${dbPath} · 轮询 ${pollMs}ms`);
await runCollector({ siteUrl, dbPath, pollMs });
