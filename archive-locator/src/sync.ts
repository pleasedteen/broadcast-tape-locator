/**
 * 同步入口（采集器守护进程）。可同时起多个进程共用同一个 SQLite 库：
 * 按来源的租约保证同一来源同一时刻只由一个进程发请求，不同来源可并行。
 *
 * 环境变量：
 *   SITE_URL      模拟站点地址（默认 http://127.0.0.1:4181）
 *   DB_PATH       SQLite 数据库路径（默认 <项目>/data/archive.db）
 *   POLL_MS       轮询间隔毫秒（默认 500）
 *   SYNC_OWNER    本进程标识（默认 pid + 随机串）
 *   LEASE_TTL_MS  采集租约有效期（默认 800）：进程被挂起超过此值，另一进程即可接管
 *   SYNC_TAPES    只处理这些来源（逗号分隔；默认处理全部）；用于多进程分工不同来源并行采集
 */
import { mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { runCollector } from './collector.ts';

const ROOT = join(fileURLToPath(import.meta.url), '..', '..');
const siteUrl = process.env.SITE_URL ?? 'http://127.0.0.1:4181';
const dbPath = process.env.DB_PATH ?? join(ROOT, 'data', 'archive.db');
const pollMs = Number(process.env.POLL_MS ?? 500);
const owner = process.env.SYNC_OWNER;
const leaseTtlMs = process.env.LEASE_TTL_MS ? Number(process.env.LEASE_TTL_MS) : undefined;
const tapes = process.env.SYNC_TAPES
  ? process.env.SYNC_TAPES.split(',').map((t) => t.trim()).filter(Boolean)
  : undefined;

mkdirSync(dirname(dbPath), { recursive: true });
console.log(`[sync] 站点 ${siteUrl} · 数据库 ${dbPath} · 轮询 ${pollMs}ms`);
await runCollector({ siteUrl, dbPath, pollMs, owner, leaseTtlMs, tapes });
