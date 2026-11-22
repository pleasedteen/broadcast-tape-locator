/**
 * SQLite 持久层（主备双站版）。
 *
 * 表设计：
 * - sources：每盘母带一行。每个模拟站点（replica，主 A / 备 B）各有独立的
 *   会话、签名令牌、暂停截止；active_replica 记录当前接力用的是哪个站，重启后沿用。
 * - timelines：母带各时间轴版次的时长。duration_ms 为首次采到的基准时长，
 *   备用站给出不同时长时只置 conflict=1，不覆盖基准（冲突不得改变现有定位结果）。
 * - revisions：站点通告过的每次清单修订；status: pending/collecting/complete/abandoned。
 *   listed = 任一站仍在架；conflict = 备用站同号清单内容哈希与已采内容不一致。
 * - replica_meta：修订 × 站点的对账视图（页大小、总页数、在架、内容哈希、冲突）。
 * - replica_pages：修订 × 站点 × 页号的断点。各站分页不同，断点必须按站独立记录，
 *   接力时据「已采记录数 ÷ 备站页大小」只请求含未采记录的页。
 * - rev_records：按内容哈希幂等的记录集合（跨站去重），两站同一修订汇入同一份记录，
 *   冲突记录不写入；records_done 等于 record_count 即整单采齐。
 * - programs / segments / damages：按 revision_id 归属的清单记录，修订不可变。
 * - activations：每盘母带当前生效的「时间轴版次 + 节目单修订 + 修复清单修订」指针。
 *   生效切换在单个事务内完成，查询按指针一次读出，保证播放位置与缺失区段同生同灭。
 */
import { DatabaseSync } from 'node:sqlite';

export function openDb(path: string): DatabaseSync {
  const db = new DatabaseSync(path);
  db.exec('PRAGMA journal_mode = WAL');
  db.exec('PRAGMA busy_timeout = 5000');
  migrate(db);
  return db;
}

function hasColumn(db: DatabaseSync, table: string, col: string): boolean {
  return (db.prepare(`PRAGMA table_info(${table})`).all() as any[]).some((c) => c.name === col);
}

function addColumn(db: DatabaseSync, table: string, col: string, ddl: string): void {
  if (!hasColumn(db, table, col)) db.exec(`ALTER TABLE ${table} ADD COLUMN ${col} ${ddl}`);
}

function migrate(db: DatabaseSync): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS sources (
      tape_id TEXT PRIMARY KEY,
      active_replica TEXT NOT NULL DEFAULT 'A',
      last_replica TEXT NOT NULL DEFAULT 'A',
      paused_until INTEGER,
      session_id TEXT,
      sign_token TEXT
    );
    CREATE TABLE IF NOT EXISTS timelines (
      tape_id TEXT NOT NULL,
      timeline_version INTEGER NOT NULL,
      duration_ms INTEGER NOT NULL,
      conflict INTEGER NOT NULL DEFAULT 0,
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
      records_done INTEGER NOT NULL DEFAULT 0,
      record_count INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'pending',  -- pending | collecting | complete | abandoned
      listed INTEGER NOT NULL DEFAULT 1,       -- 任一站点当前是否仍在架
      conflict INTEGER NOT NULL DEFAULT 0,     -- 备用站同号清单内容冲突
      updated_at INTEGER NOT NULL DEFAULT 0,
      UNIQUE (tape_id, kind, timeline_version, revision)
    );
    CREATE TABLE IF NOT EXISTS replica_meta (
      revision_id INTEGER NOT NULL,
      replica TEXT NOT NULL,
      page_size INTEGER NOT NULL,
      page_count INTEGER NOT NULL,
      record_count INTEGER NOT NULL,
      listed INTEGER NOT NULL DEFAULT 1,
      content_hash TEXT,
      conflict INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (revision_id, replica)
    );
    CREATE TABLE IF NOT EXISTS replica_pages (
      revision_id INTEGER NOT NULL,
      replica TEXT NOT NULL,
      page_no INTEGER NOT NULL,
      received_at INTEGER NOT NULL,
      PRIMARY KEY (revision_id, replica, page_no)
    );
    CREATE TABLE IF NOT EXISTS rev_records (
      revision_id INTEGER NOT NULL,
      record_hash TEXT NOT NULL,
      PRIMARY KEY (revision_id, record_hash)
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
  // 兼容旧库的增量列
  addColumn(db, 'sources', 'active_replica', "TEXT NOT NULL DEFAULT 'A'");
  for (const [t, c, ddl] of [
    ['sources', 'paused_until_b', 'INTEGER'],
    ['sources', 'session_id_b', 'TEXT'],
    ['sources', 'sign_token_b', 'TEXT'],
    // 本轮实际服务的站点：硬故障接力改写 active_replica；暂停只临时绕行（active 不变、last 变化）
    ['sources', 'last_replica', "TEXT NOT NULL DEFAULT 'A'"],
    ['revisions', 'records_done', 'INTEGER NOT NULL DEFAULT 0'],
    ['revisions', 'record_count', 'INTEGER NOT NULL DEFAULT 0'],
    ['revisions', 'conflict', 'INTEGER NOT NULL DEFAULT 0'],
    ['timelines', 'conflict', 'INTEGER NOT NULL DEFAULT 0'],
  ] as const) addColumn(db, t, c, ddl);
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
 * 无共同版次时不改动既有生效指针。冲突只打标记，绝不影响这里的选择。
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
