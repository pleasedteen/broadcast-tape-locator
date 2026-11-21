/**
 * SQLite 持久层。
 *
 * 表设计：
 * - sources：每个来源（母带）的采集会话状态（暂停截止、会话、签名令牌），重启后可恢复。
 * - timelines：母带各时间轴定义，按 (版次号, 定义版次 edition) 留痕——时长变更产生新 edition 行，
 *   旧定义保留，旧清单永远能对应到它发布时的时长。
 * - revisions：站点通告过的每次清单修订；同号更正以 content_hash 区分、各占独立不可变行；
 *   status: pending/collecting/complete/abandoned。
 * - pages：页级断点，已入库的页不重采（采集恢复的关键）。
 * - programs / segments / damages：按 revision_id 归属的清单记录，修订不可变。
 * - activations：每盘母带当前生效的「时间轴版次+edition + 节目单修订 + 修复清单修订」指针。
 *   生效切换在单个事务内完成，查询按指针一次读出，保证时长、播放位置与缺失区段同生同灭。
 */
import { DatabaseSync } from 'node:sqlite';
import { mkdirSync, rmSync, statSync } from 'node:fs';
import { sleep } from './util.ts';

/**
 * 跨进程排他地执行一次「建库 + 迁移」。
 * 多个进程同时首次打开同一数据库时，SQLite 不阻止 DDL 互相穿插（会出现 table already exists /
 * no such table 等逻辑错误），而 Node 22 未暴露 flock。这里利用 mkdir 的原子性做互斥锁：
 * 锁目录创建成功者执行迁移，其余进程退避等待后重开，看到的必然是迁移完成的库。
 * 锁带 TTL：持锁进程被 SIGKILL 时锁目录会残留，超时后由后来者接管。
 */
async function withMigrationLock<T>(dbPath: string, fn: () => T): Promise<T> {
  const lockDir = `${dbPath}.migrate.lock`;
  const TTL_MS = 30_000;
  for (let i = 0; i < 100; i++) {
    try {
      mkdirSync(lockDir);
      try {
        return fn();
      } finally {
        rmSync(lockDir, { recursive: true, force: true });
      }
    } catch (e: any) {
      if (e?.code !== 'EEXIST') throw e;
      // 锁被持有：陈旧锁（持锁进程已死）则接管，否则退避
      try {
        const age = Date.now() - statSync(lockDir).mtimeMs;
        if (age > TTL_MS) {
          rmSync(lockDir, { recursive: true, force: true });
          continue;
        }
      } catch {
        // 锁刚好被释放，下轮重试
      }
      await sleep(100);
    }
  }
  throw new Error(`获取数据库迁移锁超时: ${lockDir}`);
}

export async function openDb(path: string): DatabaseSync {
  const db = new DatabaseSync(path);
  // busy_timeout 先于 WAL 切换：journal_mode 切换需要排他锁，等待其他进程完成
  db.exec('PRAGMA busy_timeout = 5000');
  let lastErr: unknown;
  for (let i = 0; i < 10; i++) {
    try {
      db.exec('PRAGMA journal_mode = WAL');
      lastErr = undefined;
      break;
    } catch (e) {
      lastErr = e;
    }
  }
  if (lastErr) throw lastErr;
  await withMigrationLock(path, () => migrate(db));
  return db;
}

function migrate(db: DatabaseSync): void {
  // 全新库：直接建最终结构
  const fresh = (db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='revisions'").get() as any) === undefined;
  if (fresh) {
    db.exec(`
      CREATE TABLE sources (
        tape_id TEXT PRIMARY KEY,
        paused_until INTEGER,
        session_id TEXT,
        sign_token TEXT
      );
      CREATE TABLE timelines (
        tape_id TEXT NOT NULL,
        timeline_version INTEGER NOT NULL,
        timeline_edition INTEGER NOT NULL DEFAULT 1,
        duration_ms INTEGER NOT NULL,
        PRIMARY KEY (tape_id, timeline_version, timeline_edition)
      );
      CREATE TABLE revisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tape_id TEXT NOT NULL,
        kind TEXT NOT NULL,              -- 'program' | 'restoration'
        timeline_version INTEGER NOT NULL,
        timeline_edition INTEGER NOT NULL DEFAULT 1,
        revision INTEGER NOT NULL,
        content_hash TEXT NOT NULL DEFAULT '',
        page_count INTEGER NOT NULL DEFAULT 0,
        pages_done INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'pending',  -- pending | collecting | complete | abandoned
        listed INTEGER NOT NULL DEFAULT 1,       -- 站点当前是否仍在架
        record_count INTEGER NOT NULL DEFAULT 0,
        updated_at INTEGER NOT NULL DEFAULT 0,
        UNIQUE (tape_id, kind, timeline_version, revision, content_hash)
      );
      CREATE TABLE pages (
        revision_id INTEGER NOT NULL,
        page_no INTEGER NOT NULL,
        received_at INTEGER NOT NULL,
        PRIMARY KEY (revision_id, page_no)
      );
      CREATE TABLE programs (
        revision_id INTEGER NOT NULL,
        program_id TEXT NOT NULL,
        title TEXT NOT NULL,
        start_ms INTEGER NOT NULL,
        end_ms INTEGER NOT NULL,
        PRIMARY KEY (revision_id, program_id)
      );
      CREATE TABLE segments (
        revision_id INTEGER NOT NULL,
        copy_id TEXT NOT NULL,
        master_start_ms INTEGER NOT NULL,
        master_end_ms INTEGER NOT NULL,
        copy_start_ms INTEGER NOT NULL,
        rate REAL NOT NULL
      );
      CREATE TABLE damages (
        revision_id INTEGER NOT NULL,
        copy_id TEXT NOT NULL,
        master_start_ms INTEGER NOT NULL,
        master_end_ms INTEGER NOT NULL,
        note TEXT NOT NULL
      );
      CREATE TABLE activations (
        tape_id TEXT PRIMARY KEY,
        timeline_version INTEGER NOT NULL,
        timeline_edition INTEGER NOT NULL DEFAULT 1,
        program_revision_id INTEGER NOT NULL,
        restoration_revision_id INTEGER NOT NULL,
        activated_at INTEGER NOT NULL
      );
    `);
    return;
  }

  // 旧库升级：补齐列、按新唯一键重建 timelines / revisions
  const cols = (t: string) =>
    new Set((db.prepare(`PRAGMA table_info(${t})`).all() as any[]).map((c) => c.name));
  if (!cols('revisions').has('content_hash')) {
    db.exec('ALTER TABLE revisions RENAME TO revisions_old');
    db.exec(`
      CREATE TABLE revisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tape_id TEXT NOT NULL,
        kind TEXT NOT NULL,
        timeline_version INTEGER NOT NULL,
        timeline_edition INTEGER NOT NULL DEFAULT 1,
        revision INTEGER NOT NULL,
        content_hash TEXT NOT NULL DEFAULT '',
        page_count INTEGER NOT NULL DEFAULT 0,
        pages_done INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'pending',
        listed INTEGER NOT NULL DEFAULT 1,
        record_count INTEGER NOT NULL DEFAULT 0,
        updated_at INTEGER NOT NULL DEFAULT 0,
        UNIQUE (tape_id, kind, timeline_version, revision, content_hash)
      );
    `);
    db.exec(`
      INSERT INTO revisions
        (id, tape_id, kind, timeline_version, timeline_edition, revision, content_hash,
         page_count, pages_done, status, listed, record_count, updated_at)
      SELECT id, tape_id, kind, timeline_version, 1, revision, '',
             page_count, pages_done, status, listed, 0, updated_at
      FROM revisions_old;
    `);
    db.exec('DROP TABLE revisions_old');
  }
  if (!cols('timelines').has('timeline_edition')) {
    db.exec('ALTER TABLE timelines RENAME TO timelines_old');
    db.exec(`
      CREATE TABLE timelines (
        tape_id TEXT NOT NULL,
        timeline_version INTEGER NOT NULL,
        timeline_edition INTEGER NOT NULL DEFAULT 1,
        duration_ms INTEGER NOT NULL,
        PRIMARY KEY (tape_id, timeline_version, timeline_edition)
      );
    `);
    db.exec('INSERT INTO timelines (tape_id, timeline_version, timeline_edition, duration_ms) SELECT tape_id, timeline_version, 1, duration_ms FROM timelines_old');
    db.exec('DROP TABLE timelines_old');
  }
  if (!cols('activations').has('timeline_edition')) {
    db.exec('ALTER TABLE activations ADD COLUMN timeline_edition INTEGER NOT NULL DEFAULT 1');
  }
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
  timelineEdition?: number;
  programRev?: number;
  restorationRev?: number;
}

/**
 * 生效规则：
 * - 候选时间轴定义 = 两类清单都存在「已采齐且所依据时间轴定义相同」的 (版次号, edition) 组。
 *   时长变更产生新 edition 时，须两侧都按新定义采齐才会切换——切换前旧定义结果继续完整可用。
 * - 组内同号修订各取最新一条完整修订（同号更正以内容指纹区分，新指纹采齐前旧指纹仍生效）。
 * - 组间按版次号降序、edition 降序选最新；切换在单事务内完成，
 *   时长、播放位置、缺失区段必然同时更新，查询侧不会读到混合态。
 * - 无候选组时不改动既有生效指针（已采齐修订即使被站点撤下也继续可用）。
 */
export function recomputeActivation(db: DatabaseSync, tapeId: string): ActivationChange {
  const grow = db
    .prepare(
      `SELECT timeline_version AS v, timeline_edition AS e FROM revisions
       WHERE tape_id = ? AND status = 'complete'
       GROUP BY timeline_version, timeline_edition
       HAVING SUM(kind = 'program') > 0 AND SUM(kind = 'restoration') > 0
       ORDER BY v DESC, e DESC LIMIT 1`,
    )
    .get(tapeId) as any;
  if (!grow) return { changed: false };
  const pick = (kind: string) =>
    db
      .prepare(
        `SELECT id, revision FROM revisions
         WHERE tape_id = ? AND kind = ? AND timeline_version = ? AND timeline_edition = ? AND status = 'complete'
         ORDER BY revision DESC, id DESC LIMIT 1`,
      )
      .get(tapeId, kind, grow.v, grow.e) as any;
  const p = pick('program');
  const r = pick('restoration');
  const cur = db.prepare('SELECT * FROM activations WHERE tape_id = ?').get(tapeId) as any;
  if (
    cur &&
    cur.timeline_version === grow.v &&
    cur.timeline_edition === grow.e &&
    cur.program_revision_id === p.id &&
    cur.restoration_revision_id === r.id
  ) {
    return { changed: false };
  }
  db.prepare(
    `INSERT INTO activations (tape_id, timeline_version, timeline_edition, program_revision_id, restoration_revision_id, activated_at)
     VALUES (?, ?, ?, ?, ?, ?)
     ON CONFLICT(tape_id) DO UPDATE SET
       timeline_version = excluded.timeline_version,
       timeline_edition = excluded.timeline_edition,
       program_revision_id = excluded.program_revision_id,
       restoration_revision_id = excluded.restoration_revision_id,
       activated_at = excluded.activated_at`,
  ).run(tapeId, grow.v, grow.e, p.id, r.id, Date.now());
  return { changed: true, timelineVersion: grow.v, timelineEdition: grow.e, programRev: p.revision, restorationRev: r.revision };
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
