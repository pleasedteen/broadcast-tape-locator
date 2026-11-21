/**
 * SQLite 持久层。
 *
 * 表设计：
 * - sources：每个来源（母带）的采集会话状态（暂停截止、会话、签名令牌、租约），重启后可恢复。
 *   同一数据库可跑多个采集进程：租约（owner/epoch/expires_at）把来源分给活着的进程，
 *   会话与签名令牌按租约代（epoch）隔离，晚到的旧代响应不会覆盖新一代的进度或会话。
 * - workers：采集进程心跳登记表，崩溃的进程租约到期后被其他进程接手。
 * - timelines：母带各时间轴版次的时长。
 * - revisions：站点通告过的每次清单修订；status: pending/collecting/complete/abandoned。
 * - pages：页级断点，已入库的页不重采（采集恢复与多进程接手的关键）。
 * - programs / segments / damages：按 revision_id 归属的清单记录，修订不可变。
 * - activations：每盘母带当前生效的「时间轴版次 + 节目单修订 + 修复清单修订」指针。
 *   生效切换在单个事务内完成，查询按指针一次读出，保证播放位置与缺失区段同生同灭。
 */
import { DatabaseSync } from 'node:sqlite';

export function openDb(path: string): DatabaseSync {
  const db = new DatabaseSync(path);
  // busy_timeout 必须最先设置：多进程同时打开同一库时，随后的 journal_mode/建表会互相等待
  db.exec('PRAGMA busy_timeout = 5000');
  db.exec('PRAGMA journal_mode = WAL');
  migrate(db);
  return db;
}

function migrate(db: DatabaseSync): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS sources (
      tape_id TEXT PRIMARY KEY,
      paused_until INTEGER,
      session_id TEXT,
      sign_token TEXT,
      session_lease_epoch INTEGER NOT NULL DEFAULT 0,
      owner_id TEXT,
      lease_epoch INTEGER NOT NULL DEFAULT 0,
      lease_expires_at INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS workers (
      worker_id TEXT PRIMARY KEY,
      pid INTEGER NOT NULL,
      started_at INTEGER NOT NULL,
      last_seen INTEGER NOT NULL
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
  `);
  // 兼容旧库：为已存在的 sources 表补充租约列
  const cols = (db.prepare('PRAGMA table_info(sources)').all() as any[]).map((c) => c.name as string);
  const addCol = (name: string, ddl: string) => {
    if (!cols.includes(name)) db.exec(`ALTER TABLE sources ADD COLUMN ${ddl}`);
  };
  addCol('session_lease_epoch', 'session_lease_epoch INTEGER NOT NULL DEFAULT 0');
  addCol('owner_id', 'owner_id TEXT');
  addCol('lease_epoch', 'lease_epoch INTEGER NOT NULL DEFAULT 0');
  addCol('lease_expires_at', 'lease_expires_at INTEGER NOT NULL DEFAULT 0');
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
