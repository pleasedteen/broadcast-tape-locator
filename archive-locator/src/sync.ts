/**
 * 同步入口（采集器守护进程）。同一数据库可同时跑多个采集进程：
 * 各进程按来源租约分片并行采集，一个进程卡住时其他进程在租约到期后接手。
 *
 * 环境变量：
 *   SITE_URL   模拟站点地址（默认 http://127.0.0.1:4181）
 *   DB_PATH    SQLite 数据库路径（默认 <项目>/data/archive.db）
 *   POLL_MS    轮询间隔毫秒（默认 500）
 *   WORKER_ID  采集进程标识（默认按 pid 生成；同一库上各进程须唯一）
 *   LEASE_MS   来源租约时长毫秒（默认 2000；卡住超过该时长后被其他进程接手）
 *   HTTP_TIMEOUT_MS  单个请求超时毫秒（默认 3000）
 */
import { mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { runCollector } from './collector.ts';

const ROOT = join(fileURLToPath(import.meta.url), '..', '..');
const siteUrl = process.env.SITE_URL ?? 'http://127.0.0.1:4181';
const dbPath = process.env.DB_PATH ?? join(ROOT, 'data', 'archive.db');
const pollMs = Number(process.env.POLL_MS ?? 500);
const workerId = process.env.WORKER_ID;
const leaseMs = process.env.LEASE_MS ? Number(process.env.LEASE_MS) : undefined;
const reqTimeoutMs = process.env.HTTP_TIMEOUT_MS ? Number(process.env.HTTP_TIMEOUT_MS) : undefined;

mkdirSync(dirname(dbPath), { recursive: true });
console.log(`[sync] 站点 ${siteUrl} · 数据库 ${dbPath} · 轮询 ${pollMs}ms`);
await runCollector({ siteUrl, dbPath, pollMs, workerId, leaseMs, reqTimeoutMs });
