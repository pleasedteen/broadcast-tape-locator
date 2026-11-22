/**
 * 采集租约（lease）与写入围栏（fencing）。
 *
 * 同一个 SQLite 库可被多个同步进程打开，但同一来源同一时刻只允许一路采集：
 * - 每个来源在 sync_leases 中有一行租约，含 holder（进程标识）、fence（单调递增）、
 *   expires_at（到期时间）。进程在持有期间周期性续约。
 * - 租约到期后才允许其他进程接管；接管时 fence + 1。因此「卡住但仍存活」的进程
 *   最多在 TTL 内失去所有权，另一个进程按已落库进度继续采集。
 * - 所有对 sources / revisions / pages / programs / segments / damages / activations
 *   的写入都必须经过 withFencedTx：事务内校验 holder + fence 仍匹配，并顺带续约。
 *   被接管后旧进程的迟到响应不会落库——不能改变会话、采集进度或生效指针。
 * - 接管时清空该来源的会话，新持有者必须重建会话；旧会话即便在站点侧仍有效，
 *   也无法再写回数据库。
 */
import type { DatabaseSync } from 'node:sqlite';
import { recomputeActivation } from './db.ts';

export interface LeaseCtx {
  tape: string;
  holder: string;
  fence: number;
}

export interface FencedResult<T> {
  ok: boolean;
  value?: T;
}

/** 进程标识：主机名 + pid + 随机数，确保不同进程不会互相认作同一持有者 */
export function makeHolderId(): string {
  const rand = Math.random().toString(16).slice(2, 8);
  return `${process.pid}-${rand}`;
}

function ensureSourceRow(db: DatabaseSync, tape: string): void {
  db.prepare('INSERT OR IGNORE INTO sources (tape_id) VALUES (?)').run(tape);
}

/** 接管时清空会话与签名令牌：新持有者必须重新建会话，旧进程的会话状态不再被沿用 */
function resetSession(db: DatabaseSync, tape: string): void {
  db.prepare('UPDATE sources SET session_id = NULL, sign_token = NULL WHERE tape_id = ?').run(tape);
}

/**
 * 尝试取得或续租某来源。返回 null 表示其他进程持有未到期的租约，本轮不应向该来源发任何请求。
 * 若租约已到期，则以 fence+1 接管，并清空旧会话。
 */
export function tryAcquire(db: DatabaseSync, tape: string, holder: string, ttlMs: number): LeaseCtx | null {
  db.exec('BEGIN IMMEDIATE');
  try {
    ensureSourceRow(db, tape);
    const now = Date.now();
    const row = db.prepare('SELECT * FROM sync_leases WHERE tape_id = ?').get(tape) as any;
    if (!row) {
      db.prepare(
        'INSERT INTO sync_leases (tape_id, holder, fence, acquired_at, renewed_at, expires_at) VALUES (?,?,1,?,?,?)',
      ).run(tape, holder, now, now, now + ttlMs);
      db.exec('COMMIT');
      return { tape, holder, fence: 1 };
    }
    if (row.holder === holder) {
      db.prepare('UPDATE sync_leases SET renewed_at = ?, expires_at = ? WHERE tape_id = ?').run(now, now + ttlMs, tape);
      db.exec('COMMIT');
      return { tape, holder, fence: row.fence };
    }
    if (now < row.expires_at) {
      db.exec('ROLLBACK');
      return null;
    }
    // 前任租约已到期：接管。fence 单调递增使旧持有者的所有后续写入失效。
    const fence = row.fence + 1;
    const upd = db
      .prepare(
        `UPDATE sync_leases SET holder = ?, fence = ?, acquired_at = ?, renewed_at = ?, expires_at = ?
         WHERE tape_id = ? AND holder = ? AND fence = ?`,
      )
      .run(holder, fence, now, now, now + ttlMs, tape, row.holder, row.fence);
    if (Number(upd.changes) === 0) {
      db.exec('ROLLBACK');
      return null;
    }
    resetSession(db, tape);
    db.exec('COMMIT');
    return { tape, holder, fence };
  } catch (e) {
    try { db.exec('ROLLBACK'); } catch { /* ignore */ }
    throw e;
  }
}

/** 续租；返回 false 表示已被其他进程接管 */
export function renewLease(db: DatabaseSync, ctx: LeaseCtx, ttlMs: number): boolean {
  const now = Date.now();
  const r = db
    .prepare(
      'UPDATE sync_leases SET renewed_at = ?, expires_at = ? WHERE tape_id = ? AND holder = ? AND fence = ?',
    )
    .run(now, now + ttlMs, ctx.tape, ctx.holder, ctx.fence);
  return Number(r.changes) > 0;
}

export function isHeld(db: DatabaseSync, ctx: LeaseCtx): boolean {
  const r = db
    .prepare('SELECT 1 AS ok FROM sync_leases WHERE tape_id = ? AND holder = ? AND fence = ?')
    .get(ctx.tape, ctx.holder, ctx.fence) as any;
  return !!r;
}

export function releaseLease(db: DatabaseSync, ctx: LeaseCtx): void {
  db.prepare('UPDATE sync_leases SET expires_at = ? WHERE tape_id = ? AND holder = ? AND fence = ?').run(
    Date.now(),
    ctx.tape,
    ctx.holder,
    ctx.fence,
  );
}

/**
 * 围栏事务：先校验并续租，再执行写入。ok=false 即表示租约已被接管，调用方必须丢弃本次结果。
 * 事务内不做任何 await，避免长时间持锁。
 */
export function withFencedTx<T>(
  db: DatabaseSync,
  ctx: LeaseCtx,
  ttlMs: number,
  fn: () => T,
): FencedResult<T> {
  db.exec('BEGIN IMMEDIATE');
  try {
    const now = Date.now();
    const upd = db
      .prepare(
        'UPDATE sync_leases SET renewed_at = ?, expires_at = ? WHERE tape_id = ? AND holder = ? AND fence = ?',
      )
      .run(now, now + ttlMs, ctx.tape, ctx.holder, ctx.fence);
    if (Number(upd.changes) === 0) {
      db.exec('ROLLBACK');
      return { ok: false };
    }
    const value = fn();
    db.exec('COMMIT');
    return { ok: true, value };
  } catch (e) {
    try { db.exec('ROLLBACK'); } catch { /* ignore */ }
    throw e;
  }
}

/** 取得租约后（含接管），在围栏事务内按已落库数据重算生效指针 */
export function recomputeOnAcquire(
  db: DatabaseSync,
  ctx: LeaseCtx,
  ttlMs: number,
): ReturnType<typeof recomputeActivation> | null {
  const r = withFencedTx(db, ctx, ttlMs, () => recomputeActivation(db, ctx.tape));
  return r.ok ? r.value! : null;
}
