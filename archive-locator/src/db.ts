/**
 * SQLite 持久层。
 *
 * 表设计：
 * - sources：每个来源（母带）的采集会话状态（暂停截止、会话、签名令牌），重启后可恢复。
 * - timelines：母带各时间轴版次的时长（站点当前值；生效时长以 activations.duration_ms 快照为准）。
 * - revisions：站点通告过的每次清单修订。同一修订号可被站点多次「更正/原样重发」，
 *   每次重发分配新的 edition（版次号），各占一行、互不覆盖——改稿前后的页物理隔离，
 *   不可能拼成一份。status: pending/collecting/complete/abandoned。
 *   duration_ms 为该修订发布时站点声明的时间轴时长，随生效指针一起原子切换。
 * - pages：页级断点 + 该页内容哈希（站点索引给出当前版次每页哈希）；
 *   已入库且哈希匹配的页可被同一修订号的后续 edition 直接复用，不重复抓取。
 * - programs / segments / damages：按 revision_id + page_no 归属的清单记录，修订不可变。
 * - activations：每盘母带当前生效的「时间轴版次 + 节目单修订（含 edition）+
 *   修复清单修订（含 edition）+ 时长」指针。生效切换在单个事务内完成，
 *   查询按指针一次读出，保证时长、播放位置与缺失区段同生同灭。
 */
import { DatabaseSync } from 'node:sqlite';

/** 同步阻塞睡眠（仅用于 open 阶段锁冲突重试） */
const sleepSync = (ms: number): void => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);

export function openDb(path: string): DatabaseSync {
  // 采集器与只读查询接口是独立进程：一方正持写锁时，另一方打开库/切 WAL 可能
  // 短暂拿到 SQLITE_BUSY。busy_timeout 只管语句级等待，open 阶段需自行重试。
  let db: DatabaseSync | null = null;
  for (let attempt = 0; attempt < 50; attempt++) {
    try {
      db = new DatabaseSync(path);
      db.exec('PRAGMA busy_timeout = 5000');
      db.exec('PRAGMA journal_mode = WAL');
      break;
    } catch (e: any) {
      try { db?.close(); } catch { /* ignore */ }
      if (attempt === 49 || (e?.code !== 'ERR_SQLITE_ERROR' && !String(e?.message).includes('locked'))) throw e;
      sleepSync(100 * (attempt + 1));
    }
  }
  const opened = db as DatabaseSync;
  migrate(opened);
  return opened;
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
      kind TEXT NOT NULL,                              -- 'program' | 'restoration'
      timeline_version INTEGER NOT NULL,
      revision INTEGER NOT NULL,
      edition INTEGER NOT NULL DEFAULT 1,              -- 同一修订号每次更正/重发 +1
      page_count INTEGER NOT NULL DEFAULT 0,
      pages_done INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'pending',          -- pending | collecting | complete | abandoned
      listed INTEGER NOT NULL DEFAULT 1,               -- 站点当前是否仍在架
      duration_ms INTEGER,                             -- 发布时声明的时间轴时长
      updated_at INTEGER NOT NULL DEFAULT 0,
      UNIQUE (tape_id, kind, timeline_version, revision, edition)
    );
    CREATE TABLE IF NOT EXISTS pages (
      revision_id INTEGER NOT NULL,
      page_no INTEGER NOT NULL,
      received_at INTEGER NOT NULL,
      content_hash TEXT,                               -- 站点提供的该页内容哈希
      PRIMARY KEY (revision_id, page_no)
    );
    CREATE TABLE IF NOT EXISTS programs (
      revision_id INTEGER NOT NULL,
      program_id TEXT NOT NULL,
      title TEXT NOT NULL,
      start_ms INTEGER NOT NULL,
      end_ms INTEGER NOT NULL,
      page_no INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (revision_id, program_id)
    );
    CREATE TABLE IF NOT EXISTS segments (
      revision_id INTEGER NOT NULL,
      copy_id TEXT NOT NULL,
      master_start_ms INTEGER NOT NULL,
      master_end_ms INTEGER NOT NULL,
      copy_start_ms INTEGER NOT NULL,
      rate REAL NOT NULL,
      page_no INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS damages (
      revision_id INTEGER NOT NULL,
      copy_id TEXT NOT NULL,
      master_start_ms INTEGER NOT NULL,
      master_end_ms INTEGER NOT NULL,
      note TEXT NOT NULL,
      page_no INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS activations (
      tape_id TEXT PRIMARY KEY,
      timeline_version INTEGER NOT NULL,
      program_revision_id INTEGER NOT NULL,
      restoration_revision_id INTEGER NOT NULL,
      duration_ms INTEGER NOT NULL DEFAULT 0,
      activated_at INTEGER NOT NULL
    );
  `);
  // 旧库平滑升级（验证库每次清空，这里主要照顾手动运行留下的 data/archive.db）
  const hasCol = (table: string, col: string) =>
    (db.prepare(`PRAGMA table_info(${table})`).all() as any[]).some((c) => c.name === col);
  const addCol = (table: string, col: string, decl: string) => {
    if (!hasCol(table, col)) db.exec(`ALTER TABLE ${table} ADD COLUMN ${col} ${decl}`);
  };
  addCol('revisions', 'edition', 'INTEGER NOT NULL DEFAULT 1');
  addCol('revisions', 'duration_ms', 'INTEGER');
  addCol('pages', 'content_hash', 'TEXT');
  addCol('programs', 'page_no', 'INTEGER NOT NULL DEFAULT 0');
  addCol('segments', 'page_no', 'INTEGER NOT NULL DEFAULT 0');
  addCol('damages', 'page_no', 'INTEGER NOT NULL DEFAULT 0');
  addCol('activations', 'duration_ms', 'INTEGER NOT NULL DEFAULT 0');
  db.exec(
    `CREATE UNIQUE INDEX IF NOT EXISTS idx_revisions_edition
     ON revisions (tape_id, kind, timeline_version, revision, edition)`,
  );
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
  programEdition?: number;
  restorationRev?: number;
  restorationEdition?: number;
  durationMs?: number;
}

/**
 * 生效规则：
 * - 先取「两类清单均有已采齐修订」的最新共同时间轴版次；
 * - 同版次下，节目单与修复清单必须选择「发布时长一致」的一对修订行——
 *   同版次改时长时，站点会同时重发两类清单（同修订号、新 edition、新时长快照），
 *   只有一类采齐时找不到时长匹配的对子，既有生效指针保持不动（继续提供原有定位）；
 *   两类都采齐后才在单个事务内一起切换时长、播放位置与缺失区段。
 * - 同对子内同侧取 (revision, edition) 最大的完整修订；更正稿采齐前不参与选择。
 * 已采齐的修订即使被站点撤下也继续可用；无共同版次/无时长匹配对子时不改动既有指针。
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

  const timelineDuration =
    (db.prepare('SELECT duration_ms AS d FROM timelines WHERE tape_id = ? AND timeline_version = ?').get(tapeId, vrow.v) as any)?.d ?? 0;
  const load = (kind: string) =>
    (db
      .prepare(
        `SELECT id, revision, edition, COALESCE(duration_ms, ?) AS duration_ms FROM revisions
         WHERE tape_id = ? AND kind = ? AND timeline_version = ? AND status = 'complete'
         ORDER BY revision DESC, edition DESC, id DESC`,
      )
      .all(timelineDuration, tapeId, kind, vrow.v) as any[]);
  const programs = load('program');
  const restorations = load('restoration');

  // 以节目单侧为主序，找两侧时长快照一致的最新对子（同版次改时长的两类更正必然同档）
  let pick: { p: any; r: any; duration: number } | null = null;
  for (const p of programs) {
    const r = restorations.find((x) => x.duration_ms === p.duration_ms);
    if (r) { pick = { p, r, duration: p.duration_ms }; break; }
  }
  if (!pick) return { changed: false };

  const cur = db.prepare('SELECT * FROM activations WHERE tape_id = ?').get(tapeId) as any;
  if (
    cur &&
    cur.timeline_version === vrow.v &&
    cur.program_revision_id === pick.p.id &&
    cur.restoration_revision_id === pick.r.id &&
    cur.duration_ms === pick.duration
  ) {
    return { changed: false };
  }
  db.prepare(
    `INSERT INTO activations (tape_id, timeline_version, program_revision_id, restoration_revision_id, duration_ms, activated_at)
     VALUES (?, ?, ?, ?, ?, ?)
     ON CONFLICT(tape_id) DO UPDATE SET
       timeline_version = excluded.timeline_version,
       program_revision_id = excluded.program_revision_id,
       restoration_revision_id = excluded.restoration_revision_id,
       duration_ms = excluded.duration_ms,
       activated_at = excluded.activated_at`,
  ).run(tapeId, vrow.v, pick.p.id, pick.r.id, pick.duration, Date.now());
  return {
    changed: true,
    timelineVersion: vrow.v,
    programRev: pick.p.revision,
    programEdition: pick.p.edition,
    restorationRev: pick.r.revision,
    restorationEdition: pick.r.edition,
    durationMs: pick.duration,
  };
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
