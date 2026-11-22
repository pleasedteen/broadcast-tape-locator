/**
 * SQLite 持久层。
 *
 * 表设计：
 * - sources：每个来源（母带）的采集会话状态（暂停截止、会话、签名令牌），重启后可恢复。
 * - timelines：母带各时间轴版次的时长。
 * - revisions：站点通告过的每次清单修订；status: pending/collecting/complete/abandoned。
 * - pages：页级断点，已入库的页不重采（采集恢复的关键）。
 * - programs / segments / damages：按 revision_id 归属的清单记录，修订不可变。
 * - activations：每盘母带当前生效的「时间轴版次 + 节目单修订 + 修复清单修订」指针。
 *   生效切换在单个事务内完成，查询按指针一次读出，保证播放位置与缺失区段同生同灭。
 * - sources.lease_*：跨进程的「按来源」采集租约。不同来源可由不同进程并行采集；
 *   同一来源同一时刻只有一个进程发请求。租约带单调递增的 fence，进程被挂起后
 *   租约到期由另一进程接管并抬高 fence，旧进程恢复后的迟到响应写入一律被拒。
 * - sync_events：租约接管 / 迟到写入被拒等跨进程事件，便于验证与排障。
 */
import { DatabaseSync } from 'node:sqlite';

export function openDb(path: string): DatabaseSync {
  const db = new DatabaseSync(path);
  db.exec('PRAGMA journal_mode = WAL');
  db.exec('PRAGMA busy_timeout = 1000');
  migrate(db);
  return db;
}

function migrate(db: DatabaseSync): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS sources (
      tape_id TEXT PRIMARY KEY,
      paused_until INTEGER,
      session_id TEXT,
      sign_token TEXT
    );
    CREATE TABLE IF NOT EXISTS timelines (
      tape_id TEXT NOT NULL,
      timeline_version INTEGER NOT NULL,
      duration_ms INTEGER NOT NULL,
      PRIMARY KEY (tape_id, timeline_version)
    );
    CREATE TABLE IF NOT EXISTS revisions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tape_id TEXT NOT NULL,
      kind TEXT NOT NULL,              -- 'program' | 'restoration'
      timeline_version INTEGER NOT NULL,
      revision INTEGER NOT NULL,
      page_count INTEGER NOT NULL DEFAULT 0,
      pages_done INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'pending',  -- pending | collecting | complete | abandoned
      listed INTEGER NOT NULL DEFAULT 1,       -- 站点当前是否仍在架
      updated_at INTEGER NOT NULL DEFAULT 0,
      UNIQUE (tape_id, kind, timeline_version, revision)
    );
    CREATE TABLE IF NOT EXISTS pages (
      revision_id INTEGER NOT NULL,
      page_no INTEGER NOT NULL,
      received_at INTEGER NOT NULL,
      PRIMARY KEY (revision_id, page_no)
    );
    CREATE TABLE IF NOT EXISTS programs (
      revision_id INTEGER NOT NULL,
      program_id TEXT NOT NULL,
      title TEXT NOT NULL,
      start_ms INTEGER NOT NULL,
      end_ms INTEGER NOT NULL,
      PRIMARY KEY (revision_id, program_id)
    );
    CREATE TABLE IF NOT EXISTS segments (
      revision_id INTEGER NOT NULL,
      copy_id TEXT NOT NULL,
      master_start_ms INTEGER NOT NULL,
      master_end_ms INTEGER NOT NULL,
      copy_start_ms INTEGER NOT NULL,
      rate REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS damages (
      revision_id INTEGER NOT NULL,
      copy_id TEXT NOT NULL,
      master_start_ms INTEGER NOT NULL,
      master_end_ms INTEGER NOT NULL,
      note TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS activations (
      tape_id TEXT PRIMARY KEY,
      timeline_version INTEGER NOT NULL,
      program_revision_id INTEGER NOT NULL,
      restoration_revision_id INTEGER NOT NULL,
      activated_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS sync_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      t INTEGER NOT NULL,
      tape_id TEXT NOT NULL,
      owner TEXT,
      fence INTEGER,
      kind TEXT NOT NULL,              -- takeover | stale-write（租约接管 / 迟到写入被拒）
      detail TEXT NOT NULL
    );
  `);
  // 旧库平滑升级：补充跨进程租约列（sources 是唯一被多进程写入的表）
  const cols = new Set((db.prepare('PRAGMA table_info(sources)').all() as any[]).map((c) => c.name));
  if (!cols.has('lease_owner')) db.exec('ALTER TABLE sources ADD COLUMN lease_owner TEXT');
  if (!cols.has('lease_expires')) db.exec('ALTER TABLE sources ADD COLUMN lease_expires INTEGER NOT NULL DEFAULT 0');
  if (!cols.has('lease_fence')) db.exec('ALTER TABLE sources ADD COLUMN lease_fence INTEGER NOT NULL DEFAULT 0');
}

/** 单事务执行；生效指针切换与清单入库在同一事务，查询侧永远读到一致快照 */
export function withTx<T>(db: DatabaseSync, fn: () => T): T {
  db.exec('BEGIN IMMEDIATE');
  try {
    const r = fn();
    db.exec('COMMIT');
    return r;
  } catch (e) {
    try { db.exec('ROLLBACK'); } catch { /* ignore */ }
    throw e;
  }
}

export interface ActivationChange {
  changed: boolean;
  timelineVersion?: number;
  programRev?: number;
  restorationRev?: number;
}

/**
 * 生效规则：取「两类清单均已采齐」的最新共同时间轴版次，同版次两侧各取最新完整修订。
 * 已采齐的修订即使被站点撤下也继续可用（查询持续提供此前可用结果）；
 * 无共同版次时不改动既有生效指针。
 */
export function recomputeActivation(db: DatabaseSync, tapeId: string): ActivationChange {
  const vrow = db
    .prepare(
      `SELECT timeline_version AS v FROM revisions
       WHERE tape_id = ? AND status = 'complete'
       GROUP BY timeline_version
       HAVING SUM(kind = 'program') > 0 AND SUM(kind = 'restoration') > 0
       ORDER BY v DESC LIMIT 1`,
    )
    .get(tapeId) as any;
  if (!vrow) return { changed: false };
  const pick = (kind: string) =>
    db
      .prepare(
        `SELECT id, revision FROM revisions
         WHERE tape_id = ? AND kind = ? AND timeline_version = ? AND status = 'complete'
         ORDER BY revision DESC, id DESC LIMIT 1`,
      )
      .get(tapeId, kind, vrow.v) as any;
  const p = pick('program');
  const r = pick('restoration');
  const cur = db.prepare('SELECT * FROM activations WHERE tape_id = ?').get(tapeId) as any;
  if (cur && cur.timeline_version === vrow.v && cur.program_revision_id === p.id && cur.restoration_revision_id === r.id) {
    return { changed: false };
  }
  db.prepare(
    `INSERT INTO activations (tape_id, timeline_version, program_revision_id, restoration_revision_id, activated_at)
     VALUES (?, ?, ?, ?, ?)
     ON CONFLICT(tape_id) DO UPDATE SET
       timeline_version = excluded.timeline_version,
       program_revision_id = excluded.program_revision_id,
       restoration_revision_id = excluded.restoration_revision_id,
       activated_at = excluded.activated_at`,
  ).run(tapeId, vrow.v, p.id, r.id, Date.now());
  return { changed: true, timelineVersion: vrow.v, programRev: p.revision, restorationRev: r.revision };
}

export interface SegmentRow {
  copy_id: string;
  master_start_ms: number;
  master_end_ms: number;
  copy_start_ms: number;
  rate: number;
}
export interface DamageRow {
  copy_id: string;
  master_start_ms: number;
  master_end_ms: number;
  note: string;
}
export interface PlayableRange {
  master: [number, number];
  copy: [number, number];
}
export interface MissingRange {
  master: [number, number];
  note: string | null;
}

/**
 * 计算某副本在母带区间 [rangeStart, rangeEnd) 内的：
 * - playable：实际可播放时间段（母带范围 ↔ 副本范围，含分段变速换算）
 * - missing：该副本缺失的母带区段（区间覆盖的补集），并关联损坏记录说明
 */
export function computeCopyView(
  segments: SegmentRow[],
  damages: DamageRow[],
  rangeStart: number,
  rangeEnd: number,
): { playable: PlayableRange[]; missing: MissingRange[] } {
  const playable: PlayableRange[] = [];
  const missing: MissingRange[] = [];
  let cursor = rangeStart;
  const sorted = [...segments].sort((a, b) => a.master_start_ms - b.master_start_ms);
  for (const seg of sorted) {
    const a = Math.max(seg.master_start_ms, rangeStart);
    const b = Math.min(seg.master_end_ms, rangeEnd);
    if (a >= b) continue;
    if (a > cursor) missing.push({ master: [cursor, a], note: damageNote(damages, cursor, a) });
    const copyA = Math.round(seg.copy_start_ms + (a - seg.master_start_ms) / seg.rate);
    const copyB = Math.round(seg.copy_start_ms + (b - seg.master_start_ms) / seg.rate);
    playable.push({ master: [a, b], copy: [copyA, copyB] });
    cursor = Math.max(cursor, b);
  }
  if (cursor < rangeEnd) missing.push({ master: [cursor, rangeEnd], note: damageNote(damages, cursor, rangeEnd) });
  return { playable, missing };
}

function damageNote(damages: DamageRow[], a: number, b: number): string | null {
  const d = damages.find((x) => x.master_start_ms <= a && x.master_end_ms >= b);
  return d ? d.note : null;
}

// ---------------------------------------------------------------------------
// 跨进程采集租约（按来源）
//
// 不变量：
// - 同一来源同一时刻最多一个进程持有租约，也就最多一个进程向该来源发请求。
// - 不同来源的租约互相独立，可由不同进程并行持有。
// - 租约带单调递增的 fence；进程被挂起后租约到期、被另一进程以更高 fence 接管。
//   旧进程恢复后仍以旧 fence 写入，一律被拒（既不能推进也不能回退进度，也不能覆盖会话）。
// ---------------------------------------------------------------------------

export interface Lease {
  tapeId: string;
  owner: string;
  fence: number;
  expires: number;
}

/** 写入时发现租约已失效（被接管 / 已过期） */
export class LeaseLostError extends Error {
  readonly tapeId: string;
  constructor(tapeId: string) {
    super(`来源 ${tapeId} 的采集租约已失效`);
    this.name = 'LeaseLostError';
    this.tapeId = tapeId;
  }
}

export function recordEvent(
  db: DatabaseSync,
  tapeId: string,
  owner: string | null,
  fence: number | null,
  kind: string,
  detail: string,
): void {
  try {
    db.prepare('INSERT INTO sync_events (t, tape_id, owner, fence, kind, detail) VALUES (?,?,?,?,?,?)').run(
      Date.now(), tapeId, owner, fence, kind, detail,
    );
    db.prepare('DELETE FROM sync_events WHERE id <= (SELECT MAX(id) FROM sync_events) - 2000').run();
  } catch {
    /* 事件只用于排障，写失败不影响采集 */
  }
}

/**
 * 取得（或续约）某来源的租约，单事务内判定，避免两进程同时接管。
 * - 自己仍持有效租约：续期，fence 不变。
 * - 无人持有或已到期：接管，fence + 1。
 * - 他人持有且未到期：返回 null（本进程本轮跳过该来源）。
 */
export function acquireLease(db: DatabaseSync, tapeId: string, owner: string, ttlMs: number): Lease | null {
  return withTx(db, () => {
    const now = Date.now();
    db.prepare('INSERT OR IGNORE INTO sources (tape_id) VALUES (?)').run(tapeId);
    const row = db
      .prepare('SELECT lease_owner AS o, lease_expires AS e, lease_fence AS f FROM sources WHERE tape_id = ?')
      .get(tapeId) as any;
    if (row.o === owner && row.e > now) {
      const expires = now + ttlMs;
      db.prepare('UPDATE sources SET lease_expires = ? WHERE tape_id = ?').run(expires, tapeId);
      return { tapeId, owner, fence: row.f, expires };
    }
    if (row.o && row.e > now) return null;
    const fence = row.f + 1;
    const expires = now + ttlMs;
    db.prepare('UPDATE sources SET lease_owner = ?, lease_expires = ?, lease_fence = ? WHERE tape_id = ?')
      .run(owner, expires, fence, tapeId);
    if (row.o) {
      recordEvent(
        db, tapeId, owner, fence, 'takeover',
        `从 ${row.o} 接手（租约已到期 ${Math.max(0, now - row.e)}ms，fence ${row.f} → ${fence}）`,
      );
    }
    return { tapeId, owner, fence, expires };
  });
}

/** 续约；返回 false 表示租约已过期/被接管，调用方应立即停止该来源的后续请求 */
export function renewLease(db: DatabaseSync, lease: Lease, ttlMs: number): boolean {
  const now = Date.now();
  const r = db
    .prepare(
      `UPDATE sources SET lease_expires = ?
       WHERE tape_id = ? AND lease_owner = ? AND lease_fence = ? AND lease_expires > ?`,
    )
    .run(now + ttlMs, lease.tapeId, lease.owner, lease.fence, now);
  return Number(r.changes) > 0;
}

/** 主动让出租约（不抬高 fence）；迟到写入仍会因 owner/fence 不匹配被拒 */
export function releaseLease(db: DatabaseSync, lease: Lease): void {
  try {
    db.prepare(
      'UPDATE sources SET lease_owner = NULL, lease_expires = 0 WHERE tape_id = ? AND lease_owner = ? AND lease_fence = ?',
    ).run(lease.tapeId, lease.owner, lease.fence);
  } catch {
    /* 让出失败无妨，租约到期后自然可被接管 */
  }
}

/**
 * 围栏写入：仅在租约仍归本进程且未过期时执行 fn（fn 内所有写入与检查在同一事务）。
 * 迟到的响应会在这里被整体拒绝，不会留下半截写入。
 */
export function fencedWrite<T>(db: DatabaseSync, lease: Lease, fn: () => T): T {
  let row: any;
  try {
    return withTx(db, () => {
      const now = Date.now();
      row = db
        .prepare('SELECT lease_owner AS o, lease_fence AS f, lease_expires AS e FROM sources WHERE tape_id = ?')
        .get(lease.tapeId) as any;
      if (!row || row.o !== lease.owner || row.f !== lease.fence || row.e <= now) {
        throw new LeaseLostError(lease.tapeId);
      }
      return fn();
    });
  } catch (e) {
    if (e instanceof LeaseLostError) {
      recordEvent(
        db, lease.tapeId, lease.owner, lease.fence, 'stale-write',
        `丢弃失去租约后的迟到写入（现行 owner=${row?.o ?? '无'} fence=${row?.f ?? '-'}）`,
      );
    }
    throw e;
  }
}

