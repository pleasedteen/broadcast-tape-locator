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
 */
import { DatabaseSync } from 'node:sqlite';
import { statSync } from 'node:fs';

export function openDb(path: string): DatabaseSync {
  const db = new DatabaseSync(path);
  db.exec('PRAGMA journal_mode = WAL');
  db.exec('PRAGMA busy_timeout = 5000');
  migrate(db);
  return db;
}

/**
 * 只读查询会话（查询接口专用）。
 *
 * 本环境是 virtiofs 挂载。两条约束同时成立：
 *  1) 不能每请求开/关连接——查询进程会反复成为「最后连接」，使 -wal/-shm 被删除重建，
 *     并发的写进程会收到 SIGBUS。因此保留一条长期连接。
 *  2) 长期连接会冻结在旧快照——别处（采集器被 SIGKILL/交接）删除并重建 -wal 后，旧连接
 *     仍指向已删除的 inode。因此在每次查询前比对 -wal 的 inode；一旦变化就立刻重开。
 *
 * 重开采用「先开新连接、再关旧连接」，任何瞬间都至少持有一条连接；读取前后再各比对一次
 * inode，读取期间恰好被替换则自动重试，尽量缩小「刚被替换」的窗口。
 */
export class ReadSession {
  private db: DatabaseSync | null = null;
  private walIno: number | null = null;
  readonly path: string;

  constructor(path: string) {
    this.path = path;
  }

  private static walInode(path: string): number | null {
    try { return statSync(`${path}-wal`).ino; } catch { return null; }
  }

  /** 重开若 -wal inode 变化；返回后保证 self.walIno 与磁盘一致 */
  private refresh(): DatabaseSync {
    const now = ReadSession.walInode(this.path);
    if (this.db === null) {
      this.db = openDb(this.path);
      this.walIno = ReadSession.walInode(this.path);
      return this.db;
    }
    if (now !== this.walIno) {
      // 先开新连接、再关旧连接：任何瞬间都至少持有一条连接，避免本进程成为「最后连接」
      // 而触发 -wal/-shm 被删除重建（会使并发写进程 mmap 访问时收到 SIGBUS）。
      const fresh = openDb(this.path);
      const old = this.db;
      this.db = fresh;
      this.walIno = ReadSession.walInode(this.path);
      try { old.close(); } catch { /* ignore */ }
    } else {
      this.walIno = now;
    }
    return this.db;
  }

  /**
   * 单次请求：单个只读事务内完成整次查询，保证一致快照。
   * 若读取期间 -wal 恰好被替换（说明读到的可能是旧文件），自动重试一次。
   */
  read<T>(fn: (db: DatabaseSync) => T): T {
    for (let attempt = 0; attempt < 3; attempt++) {
      const db = this.refresh();
      const before = this.walIno;
      db.exec('BEGIN');
      try {
        const out = fn(db);
        db.exec('COMMIT');
        if (ReadSession.walInode(this.path) === before) return out;
      } catch (e) {
        try { db.exec('ROLLBACK'); } catch { /* ignore */ }
        throw e;
      }
    }
    // 三次都赶上文件替换：退回最后一次读取结果（极少见）
    const db = this.refresh();
    db.exec('BEGIN');
    try {
      const out = fn(db);
      db.exec('COMMIT');
      return out;
    } catch (e) {
      try { db.exec('ROLLBACK'); } catch { /* ignore */ }
      throw e;
    }
  }
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
    -- 采集租约：同一来源同一时刻只允许一路采集。
    -- fence 单调递增，进程重启/被接管后旧持有者的写入会因 fence 不符被拒（迟到响应隔离）。
    CREATE TABLE IF NOT EXISTS sync_leases (
      tape_id TEXT PRIMARY KEY,
      holder TEXT NOT NULL,
      fence INTEGER NOT NULL,
      acquired_at INTEGER NOT NULL,
      renewed_at INTEGER NOT NULL,
      expires_at INTEGER NOT NULL
    );
  `);
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
