/**
 * 同步入口（采集器守护进程）。
 *
 * 环境变量：
 *   SITE_URL  模拟站点地址（默认 http://127.0.0.1:4181）
 *   DB_PATH   SQLite 数据库路径（默认 <项目>/data/archive.db）
 *   POLL_MS   轮询间隔毫秒（默认 500）
 *   LEASE_TTL_MS  采集租约有效期毫秒（默认 2500，须小于 5000）
 *   HOLDER_ID   进程标识（默认自动生成；同一来源只允许一个持有者，见 lease.ts）
 *   STALL_INJECT_MS / STALL_INJECT_TAPE / STALL_INJECT_PAGE  自检用：指定来源的指定页
 *     响应到达、落库前阻塞事件循环指定毫秒，模拟「卡住但仍存活」的进程
 */
import { mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { runCollector } from './collector.ts';

const ROOT = join(fileURLToPath(import.meta.url), '..', '..');
const siteUrl = process.env.SITE_URL ?? 'http://127.0.0.1:4181';
const dbPath = process.env.DB_PATH ?? join(ROOT, 'data', 'archive.db');
const pollMs = Number(process.env.POLL_MS ?? 500);
const leaseTtlMs = Number(process.env.LEASE_TTL_MS ?? 2500);
const holderId = process.env.HOLDER_ID || undefined;
const stallMs = Number(process.env.STALL_INJECT_MS ?? 0);
const stallTape = process.env.STALL_INJECT_TAPE ?? '';
const stallPage = process.env.STALL_INJECT_PAGE;

mkdirSync(dirname(dbPath), { recursive: true });
console.log(`[sync] 站点 ${siteUrl} · 数据库 ${dbPath} · 轮询 ${pollMs}ms`);

let stallDone = false;
const stallBeforeStore =
  Number.isFinite(stallMs) && stallMs > 0
    ? (info: { tape: string; pageNo: number }) => {
        if (stallDone) return;
        if (stallTape && info.tape !== stallTape) return;
        if (stallPage !== undefined && info.pageNo !== Number(stallPage)) return;
        stallDone = true;
        // 阻塞事件循环：进程仍存活（kill 可观察到），但不再续租/取页——模拟「卡住但仍存活」
        console.log(
          `[sync] 注入卡住：${info.tape} 第${info.pageNo + 1}页响应已到达，落库前阻塞 ${stallMs}ms（存活但不作为）`,
        );
        const end = Date.now() + stallMs;
        while (Date.now() < end) { /* busy wait */ }
        console.log('[sync] 卡住注入结束，恢复运行（此时租约多半已被接管，迟到写入将被围栏拒绝）');
      }
    : undefined;

await runCollector({ siteUrl, dbPath, pollMs, leaseTtlMs, holderId, stallBeforeStore });
