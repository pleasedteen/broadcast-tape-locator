/**
 * 同步采集器。
 *
 * 关键能力：
 * - 断点续采：页级进度落库（pages / revisions.pages_done），重启后已采齐的修订直接复用，
 *   未采齐的从下一个未入库页继续；站点已撤下的未完成修订放弃，已采齐的继续可用。
 * - 两种排版归一化解析（table / cards）。
 * - 翻页签名随站点最新签名令牌 + 当前会话计算；409 时用响应附带的新令牌重签。
 * - 翻页令牌一次性；响应丢失（连接被断开）后重新申请令牌续采，不重复消费。
 * - 来源暂停：423 后记录 paused_until，期间不向该来源发任何请求；恢复后重建会话。
 * - 生效切换：某修订采齐的同一事务内重算生效指针，查询侧原子看到新旧结果。
 * - 多进程：多个采集器进程可共用一个 SQLite 库。按来源的租约保证同一来源同一时刻
 *   只有一个进程发请求（不同来源可并行）；租约带 fence，进程被挂起后另一进程在租约
 *   到期时接管并抬高 fence，旧进程恢复后的迟到响应被整体丢弃（不推进/回退进度、
 *   不覆盖会话）。暂停状态落库，换进程后同样生效，到期由接手进程按原协议重建会话。
 */
import type { DatabaseSync } from 'node:sqlite';
import {
  LeaseLostError,
  acquireLease,
  releaseLease,
  renewLease,
  fencedWrite,
  openDb,
  recomputeActivation,
  recordEvent,
  type Lease,
} from './db.ts';
import { httpReq, randomToken, sha256, sleep } from './util.ts';
import type { Rec } from './types.ts';

export interface CollectorOpts {
  siteUrl: string;
  dbPath: string;
  pollMs: number;
  owner?: string;
  leaseTtlMs?: number;
  /** 只处理这些来源（不设则处理站点通告的全部来源）；用于让多个进程分工不同来源 */
  tapes?: string[];
}

type Log = (msg: string) => void;

const DEFAULT_LEASE_TTL_MS = 800;

export async function runCollector(opts: CollectorOpts): Promise<void> {
  const db = openDb(opts.dbPath);
  const owner = opts.owner ?? `${process.pid}.${randomToken().slice(0, 4)}`;
  const ttl = opts.leaseTtlMs ?? DEFAULT_LEASE_TTL_MS;
  const filter = opts.tapes && opts.tapes.length ? new Set(opts.tapes) : null;
  const log: Log = (m) => console.log(`[sync ${owner}] ${m}`);

  log(`启动（租约 ${ttl}ms${filter ? ` · 来源 ${[...filter].join(',')}` : ''}）`);

  // 启动即对本地已有数据重算生效指针（覆盖上次退出前未完成的切换）
  for (const row of db.prepare('SELECT tape_id AS t FROM sources').all() as any[]) {
    const a = recomputeActivation(db, row.t);
    if (a.changed) log(`${row.t}: 生效版本 → 时间轴 v${a.timelineVersion}（节目单 rev${a.programRev} + 修复清单 rev${a.restorationRev}）`);
  }

  let running = true;
  process.on('SIGTERM', () => { running = false; });
  process.on('SIGINT', () => { running = false; });

  const leases = new Map<string, Lease>();

  /** 续约所有持有的租约；失去的立即从表中移除（此后不再向该来源发请求） */
  const refreshAll = () => {
    for (const [tape, lease] of leases) {
      if (!renewLease(db, lease, ttl)) {
        leases.delete(tape);
        log(`${tape}: 租约已过期或被接管（fence ${lease.fence}），停止该来源采集`);
      }
    }
  };

  /**
   * 发请求前的守卫：确认租约仍归自己；一旦失效立即抛出让出该来源。
   * 这样被挂起后恢复的旧进程会在下一个安全点停手，迟到响应则由写入围栏兜底丢弃。
   */
  const ensureLease = (lease: Lease) => {
    if (!renewLease(db, lease, ttl)) {
      if (leases.get(lease.tapeId) === lease) leases.delete(lease.tapeId);
      throw new LeaseLostError(lease.tapeId);
    }
  };

  // 心跳续约：正常运行时即使正在等待慢响应也能保持租约；进程被挂起则心跳停止，
  // 租约到期后由其他进程接手（这正是"挂起 → 2 秒内接手"的机制）。
  const heartbeat = setInterval(() => {
    try { refreshAll(); } catch (e) { log(`续约异常 ${e}`); }
  }, Math.max(50, Math.floor(ttl / 4)));
  heartbeat.unref?.();

  /** 只处理分给本进程的来源（SYNC_TAPES）；同一来源仍靠租约保证同一时刻只有一个进程发请求 */
  const owned = (tape: string) => !filter || filter.has(tape);

  while (running) {
    const lst = await httpReq('GET', `${opts.siteUrl}/api/sources`);
    if (lst.status !== 200) {
      log(`站点不可达（status=${lst.status}），${opts.pollMs}ms 后重试`);
      await sleepInterruptible(opts.pollMs, () => running, refreshAll);
      continue;
    }
    for (const tape of lst.body.tapes as string[]) {
      if (!running) break;
      if (!owned(tape)) continue;
      refreshAll();
      let lease = leases.get(tape) ?? null;
      if (!lease) {
        lease = acquireLease(db, tape, owner, ttl);
        if (!lease) continue; // 其他进程持有该来源的租约：本进程不发任何请求
        leases.set(tape, lease);
      }
      try {
        await syncSource(db, opts.siteUrl, tape, lease, owner, log, ensureLease);
      } catch (e) {
        if (e instanceof LeaseLostError) {
          leases.delete(tape);
          log(`${tape}: 租约丢失，放弃本轮（迟到响应已丢弃）`);
        } else {
          log(`${tape}: 异常 ${e}`);
        }
      }
    }
    await sleepInterruptible(opts.pollMs, () => running, refreshAll);
  }

  clearInterval(heartbeat);
  for (const lease of leases.values()) releaseLease(db, lease);
  log('采集器退出');
}

/** 分片睡眠，期间持续续约租约（进程被挂起时续约自然停止，租约到期后由其他进程接管） */
async function sleepInterruptible(ms: number, running: () => boolean, refresh: () => void): Promise<void> {
  const end = Date.now() + ms;
  while (running() && Date.now() < end) {
    refresh();
    await sleep(Math.min(100, end - Date.now()));
  }
}

async function syncSource(
  db: DatabaseSync,
  site: string,
  tape: string,
  lease: Lease,
  owner: string,
  log: Log,
  guard: (lease: Lease) => void,
): Promise<void> {
  const src = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;

  // 暂停期间：停止向该来源发请求（暂停状态在库中，换进程同样生效）
  if (src.paused_until && Date.now() < src.paused_until) return;

  if (!src.session_id) {
    guard(lease);
    const r = await httpReq('POST', `${site}/api/sources/${tape}/session`, undefined, 3000, { 'x-sync-owner': owner });
    if (handleCommon(db, tape, lease, r, log)) return;
    if (r.status !== 200) {
      log(`${tape}: 建立会话失败（status=${r.status}）`);
      return;
    }
    fencedWrite(db, lease, () => {
      db.prepare('UPDATE sources SET session_id = ?, sign_token = ? WHERE tape_id = ?').run(r.body.sessionId, r.body.signToken, tape);
    });
    log(`${tape}: 建立会话 ${r.body.sessionId.slice(0, 8)}…（签名令牌 ${String(r.body.signToken).slice(0, 8)}…）`);
  }

  const cur = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;
  guard(lease);
  const idx = await httpReq('GET', `${site}/api/sources/${tape}/index?sessionId=${cur.session_id}`, undefined, 3000, { 'x-sync-owner': owner });
  if (handleCommon(db, tape, lease, idx, log)) return;
  if (idx.status !== 200) {
    log(`${tape}: 拉取索引失败（status=${idx.status}）`);
    return;
  }
  if (idx.body?.signToken) {
    fencedWrite(db, lease, () => {
      db.prepare('UPDATE sources SET sign_token = ? WHERE tape_id = ?').run(idx.body.signToken, tape);
    });
  }
  reconcile(db, tape, lease, idx.body, log);

  // 每个轮询周期内尽量多采（上限避免单个来源占满），逐页落库
  for (let i = 0; i < 8; i++) {
    guard(lease);
    const rev = db
      .prepare(
        `SELECT * FROM revisions
         WHERE tape_id = ? AND listed = 1 AND status IN ('pending','collecting')
         ORDER BY timeline_version, kind, revision LIMIT 1`,
      )
      .get(tape) as any;
    if (!rev) return;
    const progressed = await fetchPage(db, site, tape, rev, lease, owner, log, guard);
    if (!progressed) return;
  }
}

/** 处理 401/423 等来源级状态；返回 true 表示本轮应停止该来源 */
function handleCommon(db: DatabaseSync, tape: string, lease: Lease, r: { status: number; body: any }, log: Log): boolean {
  if (r.status === 401) {
    fencedWrite(db, lease, () => {
      db.prepare('UPDATE sources SET session_id = NULL, sign_token = NULL WHERE tape_id = ?').run(tape);
    });
    log(`${tape}: 会话失效，下轮重建`);
    return true;
  }
  if (r.status === 423) {
    const until = Number(r.body?.pausedUntil ?? Date.now() + 5000);
    fencedWrite(db, lease, () => {
      db.prepare('UPDATE sources SET paused_until = ?, session_id = NULL, sign_token = NULL WHERE tape_id = ?').run(until, tape);
    });
    log(`${tape}: 站点要求暂停至 ${new Date(until).toISOString()}，期间停止向该来源发请求`);
    return true;
  }
  return false;
}

/** 索引对账：新修订登记、撤下标记（未采齐→放弃；已采齐→保留可复用） */
function reconcile(db: DatabaseSync, tape: string, lease: Lease, index: any, log: Log): void {
  fencedWrite(db, lease, () => {
    for (const tl of index.timelines ?? []) {
      db.prepare(
        `INSERT INTO timelines (tape_id, timeline_version, duration_ms) VALUES (?,?,?)
         ON CONFLICT(tape_id, timeline_version) DO UPDATE SET duration_ms = excluded.duration_ms`,
      ).run(tape, tl.version, tl.durationMs);
    }
    const seen = new Set<string>();
    for (const m of index.manifests ?? []) {
      seen.add(`${m.kind}:${m.timelineVersion}:${m.revision}`);
      const row = db
        .prepare('SELECT * FROM revisions WHERE tape_id = ? AND kind = ? AND timeline_version = ? AND revision = ?')
        .get(tape, m.kind, m.timelineVersion, m.revision) as any;
      if (!row) {
        db.prepare(
          `INSERT INTO revisions (tape_id, kind, timeline_version, revision, page_count, pages_done, status, listed, updated_at)
           VALUES (?,?,?,?,?,0,'pending',1,?)`,
        ).run(tape, m.kind, m.timelineVersion, m.revision, m.pageCount, Date.now());
        log(`${tape}: 发现 ${kindName(m.kind)} v${m.timelineVersion} rev${m.revision}（${m.pageCount} 页），待采集`);
      } else {
        db.prepare('UPDATE revisions SET page_count = ?, listed = 1, updated_at = ? WHERE id = ?').run(m.pageCount, Date.now(), row.id);
        if (row.status === 'abandoned') {
          db.prepare("UPDATE revisions SET status = 'pending', pages_done = 0 WHERE id = ?").run(row.id);
          for (const t of ['pages', 'programs', 'segments', 'damages']) {
            db.prepare(`DELETE FROM ${t} WHERE revision_id = ?`).run(row.id);
          }
          log(`${tape}: ${kindName(m.kind)} v${m.timelineVersion} rev${m.revision} 重新上架，从头重采`);
        }
      }
    }
    const locals = db.prepare('SELECT * FROM revisions WHERE tape_id = ? AND listed = 1').all(tape) as any[];
    for (const row of locals) {
      if (seen.has(`${row.kind}:${row.timeline_version}:${row.revision}`)) continue;
      if (row.status === 'complete') {
        db.prepare('UPDATE revisions SET listed = 0 WHERE id = ?').run(row.id);
        log(`${tape}: ${kindName(row.kind)} v${row.timeline_version} rev${row.revision} 站点已撤下；本地已采齐，继续可用`);
      } else {
        db.prepare("UPDATE revisions SET listed = 0, status = 'abandoned' WHERE id = ?").run(row.id);
        log(`${tape}: ${kindName(row.kind)} v${row.timeline_version} rev${row.revision} 站点已撤下，放弃未采齐部分（已采 ${row.pages_done}/${row.page_count} 页）`);
      }
    }
  });
}

/** 采一页；返回 true 表示有进展（可继续采下一页） */
async function fetchPage(
  db: DatabaseSync,
  site: string,
  tape: string,
  rev: any,
  lease: Lease,
  owner: string,
  log: Log,
  guard: (lease: Lease) => void,
): Promise<boolean> {
  const label = `${tape} ${kindName(rev.kind)} v${rev.timeline_version} rev${rev.revision}`;
  for (let attempt = 0; attempt < 4; attempt++) {
    guard(lease);
    const src = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;
    const pageNo = rev.pages_done;

    // 1) 申请一次性翻页令牌
    const pt = await httpReq('POST', `${site}/api/sources/${tape}/page-tokens`, {
      sessionId: src.session_id,
      kind: rev.kind,
      timelineVersion: rev.timeline_version,
      revision: rev.revision,
      page: pageNo,
    }, 3000, { 'x-sync-owner': owner });
    if (handleCommon(db, tape, lease, pt, log)) return false;
    if (pt.status === 410) {
      abandon(db, tape, lease, rev, log, label);
      return false;
    }
    if (pt.status !== 200) {
      log(`${label} 第${pageNo + 1}页：申请翻页令牌失败（status=${pt.status}）`);
      return false;
    }
    const pageToken = pt.body.pageToken as string;
    if (pt.body.signToken) {
      fencedWrite(db, lease, () => {
        db.prepare('UPDATE sources SET sign_token = ? WHERE tape_id = ?').run(pt.body.signToken, tape);
      });
    }

    // 2) 用「站点刚发出的签名令牌 + 当前会话」签名取页；过期则按 409 附的新令牌重签
    for (let sigTry = 0; sigTry < 3; sigTry++) {
      guard(lease);
      const cur = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;
      const sig = sha256(`${cur.sign_token}|${cur.session_id}|${pageToken}`);
      const page = await httpReq(
        'GET',
        `${site}/api/sources/${tape}/page?sessionId=${cur.session_id}&pageToken=${pageToken}&sig=${sig}`,
        undefined,
        3000,
        { 'x-sync-owner': owner },
      );
      if (page.status === 0) {
        // 响应丢失而翻页令牌已消费：重新申请令牌续采（外层循环）
        log(`${label} 第${pageNo + 1}页：响应丢失（翻页令牌已消费），重新申请令牌续采`);
        break;
      }
      if (page.status === 409 && page.body?.error === 'STALE_SIGN_TOKEN') {
        fencedWrite(db, lease, () => {
          db.prepare('UPDATE sources SET sign_token = ? WHERE tape_id = ?').run(page.body.signToken, tape);
        });
        log(`${label} 第${pageNo + 1}页：签名令牌已轮换，用新令牌重签`);
        continue;
      }
      if (page.status === 409 && page.body?.error === 'PAGE_TOKEN_CONSUMED') {
        log(`${label} 第${pageNo + 1}页：翻页令牌已被消费，重新申请`);
        break;
      }
      if (page.status === 410) {
        abandon(db, tape, lease, rev, log, label);
        return false;
      }
      if (handleCommon(db, tape, lease, page, log)) return false;
      if (page.status !== 200) {
        log(`${label} 第${pageNo + 1}页：取页失败（status=${page.status}）`);
        return false;
      }
      const recs = normalize(page.body);
      const result = storePage(db, tape, lease, rev.id, pageNo, recs, log, label, page.body.layout);
      if (result === 'stale') {
        log(`${label} 第${pageNo + 1}页：迟到响应已被丢弃（进度/会话未改动）`);
        return false;
      }
      return result === 'stored';
    }
  }
  return false;
}

/**
 * 页入库：在围栏事务内完成。
 * - 租约已失效 → 整体拒绝（迟到响应既不能推进也不能回退进度）。
 * - 页码不是当前第一个未入库页（如被接管进程的旧页）→ 忽略，避免乱序推进。
 * - 页级幂等：已入库的页直接跳过。
 */
function storePage(
  db: DatabaseSync,
  tape: string,
  lease: Lease,
  revId: number,
  pageNo: number,
  recs: Rec[],
  log: Log,
  label: string,
  layout: string,
): 'stored' | 'dup' | 'stale' | 'ignored' {
  try {
    return fencedWrite(db, lease, () => {
      const rev = db.prepare('SELECT * FROM revisions WHERE id = ?').get(revId) as any;
      if (!rev || rev.listed === 0 || (rev.status !== 'pending' && rev.status !== 'collecting')) return 'ignored';
      if (pageNo !== rev.pages_done) return 'ignored';
      const ins = db.prepare('INSERT OR IGNORE INTO pages (revision_id, page_no, received_at) VALUES (?,?,?)').run(revId, pageNo, Date.now());
      if (Number(ins.changes) === 0) return 'dup';
      for (const rec of recs) {
        if (rec.type === 'program') {
          db.prepare('INSERT OR REPLACE INTO programs (revision_id, program_id, title, start_ms, end_ms) VALUES (?,?,?,?,?)')
            .run(revId, rec.programId, rec.title, rec.startMs, rec.endMs);
        } else if (rec.type === 'segment') {
          db.prepare('INSERT INTO segments (revision_id, copy_id, master_start_ms, master_end_ms, copy_start_ms, rate) VALUES (?,?,?,?,?,?)')
            .run(revId, rec.copyId, rec.masterStartMs, rec.masterEndMs, rec.copyStartMs, rec.rate);
        } else {
          db.prepare('INSERT INTO damages (revision_id, copy_id, master_start_ms, master_end_ms, note) VALUES (?,?,?,?,?)')
            .run(revId, rec.copyId, rec.masterStartMs, rec.masterEndMs, rec.note);
        }
      }
      const done = rev.pages_done + 1;
      const complete = done >= rev.page_count;
      db.prepare('UPDATE revisions SET pages_done = ?, status = ?, updated_at = ? WHERE id = ?')
        .run(done, complete ? 'complete' : 'collecting', Date.now(), revId);
      if (complete) {
        // 同一事务内切换生效指针：查询侧原子看到「播放位置 + 缺失区段」的整体更新
        const a = recomputeActivation(db, rev.tape_id);
        log(
          `${label} 采齐（${done}/${rev.page_count} 页）` +
            (a.changed ? `；生效版本切换 → 时间轴 v${a.timelineVersion}（节目单 rev${a.programRev} + 修复清单 rev${a.restorationRev}）` : ''),
        );
      } else {
        log(`${label} 第${pageNo + 1}/${rev.page_count} 页入库（${layout} 排版）`);
      }
      return 'stored';
    });
  } catch (e) {
    // 围栏内已记录 stale-write 事件；此处仅把结果转换为可读状态
    if (e instanceof LeaseLostError) return 'stale';
    throw e;
  }
}

function abandon(db: DatabaseSync, tape: string, lease: Lease, rev: any, log: Log, label: string): void {
  try {
    fencedWrite(db, lease, () => {
      db.prepare("UPDATE revisions SET status = 'abandoned', listed = 0, updated_at = ? WHERE id = ?").run(Date.now(), rev.id);
    });
    log(`${label} 已被站点撤下（410），放弃未采齐部分（进度 ${rev.pages_done}/${rev.page_count} 页）`);
  } catch (e) {
    if (!(e instanceof LeaseLostError)) throw e;
    log(`${label} 迟到撤下通知已丢弃（租约已失效）`);
  }
}

/** 两种排版归一化为统一记录 */
function normalize(body: any): Rec[] {
  if (body?.layout === 'table') {
    return (body.rows as any[]).map((row) => {
      if (row.rec === 'program') return { type: 'program', programId: row.id, title: row.title, startMs: row.from, endMs: row.to };
      if (row.rec === 'segment') return { type: 'segment', copyId: row.copy, masterStartMs: row.mFrom, masterEndMs: row.mTo, copyStartMs: row.cFrom, rate: row.rate };
      return { type: 'damage', copyId: row.copy, masterStartMs: row.mFrom, masterEndMs: row.mTo, note: row.note };
    });
  }
  if (body?.layout === 'cards') {
    return (body.items as any[]).map((it) => {
      if (it.kind === 'program') return { type: 'program', programId: it.program.id, title: it.program.title, startMs: it.program.span.startMs, endMs: it.program.span.endMs };
      if (it.kind === 'segment') return { type: 'segment', copyId: it.segment.copy, masterStartMs: it.segment.master.startMs, masterEndMs: it.segment.master.endMs, copyStartMs: it.segment.copyStartMs, rate: it.segment.rate };
      return { type: 'damage', copyId: it.damage.copy, masterStartMs: it.damage.master.startMs, masterEndMs: it.damage.master.endMs, note: it.damage.note };
    });
  }
  throw new Error(`未知排版: ${JSON.stringify(body).slice(0, 120)}`);
}

function kindName(kind: string): string {
  return kind === 'program' ? '节目单' : '修复清单';
}
