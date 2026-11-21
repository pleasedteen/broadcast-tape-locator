/**
 * 同步采集器（支持同一数据库上多进程并行）。
 *
 * 关键能力：
 * - 断点续采：页级进度落库（pages / revisions.pages_done），重启或换进程后已采齐的修订
 *   直接复用，未采齐的从下一页继续；站点已撤下的未完成修订放弃，已采齐的继续可用。
 * - 多进程并行：每个进程对未暂停来源抢占租约（sources.owner_id / lease_epoch / lease_expires_at），
 *   同一来源同一时刻只有一个持有者；各来源的取页循环并发运行，本地慢页时多个来源的
 *   取页实际重叠。持有者每页续租；卡住（事件循环阻塞）时租约到期，其他进程接手，
 *   从已落库的页之后继续。
 * - 会话按租约代隔离：会话与签名令牌绑定租约代（session_lease_epoch）；页面只有在入库时
 *   本进程仍持有租约才落库——晚到的旧响应（慢页、丢包重试）不会覆盖新进度或新一代会话。
 * - 两种排版归一化解析（table / cards）。
 * - 翻页签名随站点最新签名令牌 + 当前会话计算；409 时用响应附带的新令牌重签。
 * - 翻页令牌一次性；响应丢失（连接被断开）后重新申请令牌续采，不重复消费。
 * - 来源暂停：任一进程收到 423 后把 paused_until 落库；所有进程每轮取页前检查本地库，
 *   期间不向该来源发任何请求；恢复后重建会话。
 * - 生效切换：某修订采齐的同一事务内重算生效指针，查询侧原子看到新旧结果。
 */
import type { DatabaseSync } from 'node:sqlite';
import { openDb, recomputeActivation, withTx } from './db.ts';
import { httpReq, sha256, sleep } from './util.ts';
import type { Rec } from './types.ts';

export interface CollectorOpts {
  siteUrl: string;
  dbPath: string;
  pollMs: number;
  workerId?: string;
  /** 来源租约时长（毫秒）：持有者卡住超过该时长后，其他进程可接手 */
  leaseMs?: number;
  /** 单个 HTTP 请求超时（毫秒），默认 3000 */
  reqTimeoutMs?: number;
}

type Log = (msg: string) => void;

/** 采集器上下文：在各函数间传递共享状态 */
interface Ctx {
  db: DatabaseSync;
  site: string;
  workerId: string;
  leaseMs: number;
  reqTimeoutMs: number;
  log: Log;
}

export async function runCollector(opts: CollectorOpts): Promise<void> {
  const db = openDb(opts.dbPath);
  const ctx: Ctx = {
    db,
    site: opts.siteUrl,
    workerId: opts.workerId ?? `w-${process.pid}-${Math.random().toString(16).slice(2, 8)}`,
    leaseMs: Math.max(600, opts.leaseMs ?? 2000),
    reqTimeoutMs: Math.max(500, opts.reqTimeoutMs ?? 3000),
    log: () => {},
  };
  ctx.log = (m) => console.log(`[sync ${ctx.workerId}] ${m}`);
  const { log } = ctx;

  // 登记进程并定期心跳（/sync/status 可观察集群成员）
  db.prepare('INSERT OR REPLACE INTO workers (worker_id, pid, started_at, last_seen) VALUES (?,?,?,?)').run(
    ctx.workerId,
    process.pid,
    Date.now(),
    Date.now(),
  );
  const heartbeat = setInterval(() => {
    try {
      db.prepare('UPDATE workers SET last_seen = ? WHERE worker_id = ?').run(Date.now(), ctx.workerId);
    } catch {
      /* 退出过程中忽略 */
    }
  }, Math.max(200, Math.floor(ctx.leaseMs / 3)));

  // 启动即对本地已有数据重算生效指针（覆盖上次退出前未完成的切换）
  for (const row of db.prepare('SELECT tape_id AS t FROM sources').all() as any[]) {
    const a = recomputeActivation(db, row.t);
    if (a.changed) log(`${row.t}: 生效版本 → 时间轴 v${a.timelineVersion}（节目单 rev${a.programRev} + 修复清单 rev${a.restorationRev}）`);
  }

  let running = true;
  process.on('SIGTERM', () => {
    running = false;
  });
  process.on('SIGINT', () => {
    running = false;
  });

  log(`进程上线（租约 ${ctx.leaseMs}ms，每轮最多新认领 1 个来源）`);
  const active = new Map<string, Promise<void>>(); // tape → 正在运行的取页循环
  while (running) {
    const lst = await httpReq('GET', `${ctx.site}/api/sources`, undefined, ctx.reqTimeoutMs);
    if (lst.status !== 200) {
      log(`站点不可达（status=${lst.status}），${opts.pollMs}ms 后重试`);
      await sleep(opts.pollMs);
      continue;
    }
    let claimedThisRound = false;
    for (const tape of lst.body.tapes as string[]) {
      db.prepare('INSERT OR IGNORE INTO sources (tape_id) VALUES (?)').run(tape);
      // 每轮最多新认领一个来源：多进程自然分片，不会一家独占
      const claim = tryClaim(ctx, tape, !claimedThisRound);
      if (claim === 'claimed') claimedThisRound = true;
      if ((claim === 'claimed' || claim === 'held') && !active.has(tape)) {
        const p = fetchLoop(ctx, tape)
          .catch((e) => log(`${tape}: 异常 ${e}`))
          .finally(() => active.delete(tape));
        active.set(tape, p);
      }
    }
    await sleep(opts.pollMs);
  }
  clearInterval(heartbeat);
  await Promise.allSettled([...active.values()]);
  try {
    db.prepare('UPDATE sources SET owner_id = NULL, lease_expires_at = 0 WHERE owner_id = ?').run(ctx.workerId);
    db.prepare('DELETE FROM workers WHERE worker_id = ?').run(ctx.workerId);
  } catch {
    /* ignore */
  }
  log('采集器退出');
}

type ClaimResult = 'claimed' | 'held' | 'theirs' | 'paused';

/**
 * 抢占/确认来源租约（单事务，多进程安全）：
 * - 来源暂停中：任何进程都不认领（双方均停止向该来源发请求）；
 * - 租约未到期且属于他人：不可认领；
 * - 租约过期（持有者卡住或退出）或无主：maySteal 时认领，租约代 +1。
 */
function tryClaim(ctx: Ctx, tape: string, maySteal: boolean): ClaimResult {
  const { db, workerId, leaseMs } = ctx;
  const now = Date.now();
  return withTx(db, () => {
    const src = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;
    if (src.paused_until && now < src.paused_until) return 'paused';
    const expired = !src.lease_expires_at || src.lease_expires_at <= now;
    if (src.owner_id === workerId && !expired) return 'held';
    if (!expired || !maySteal) return 'theirs';
    // 租约代 +1；既有会话（若站点仍认可）绑定到新租约代直接复用，避免接手时的会话抖动
    db.prepare(
      `UPDATE sources SET owner_id = ?, lease_epoch = lease_epoch + 1, lease_expires_at = ?,
         session_lease_epoch = lease_epoch + 1
       WHERE tape_id = ?`,
    ).run(workerId, now + leaseMs, tape);
    return 'claimed';
  });
}

/** 续租；返回 false 表示租约已易主（本进程曾卡住），立即停止该来源的一切操作 */
function renewLease(ctx: Ctx, tape: string): boolean {
  const r = ctx.db
    .prepare('UPDATE sources SET lease_expires_at = ? WHERE tape_id = ? AND owner_id = ?')
    .run(Date.now() + ctx.leaseMs, tape, ctx.workerId);
  return Number(r.changes) === 1;
}

/** 持有租约期间的取页循环：会话 → 索引对账 → 逐页采集（每页落库并续租） */
async function fetchLoop(ctx: Ctx, tape: string): Promise<void> {
  const { db } = ctx;
  if (!(await ensureSession(ctx, tape))) return;
  if (!(await refreshIndex(ctx, tape))) return;
  // 每个周期内尽量多采（上限避免单个来源占满），逐页落库
  for (let i = 0; i < 64; i++) {
    // 每轮取页前：确认租约仍在我手上、来源未被任一进程登记暂停
    if (!renewLease(ctx, tape)) return;
    const src = db.prepare('SELECT paused_until FROM sources WHERE tape_id = ?').get(tape) as any;
    if (src.paused_until && Date.now() < src.paused_until) return;
    const rev = db
      .prepare(
        `SELECT * FROM revisions
         WHERE tape_id = ? AND listed = 1 AND status IN ('pending','collecting')
         ORDER BY timeline_version, kind, revision LIMIT 1`,
      )
      .get(tape) as any;
    if (!rev) return;
    const progressed = await fetchPage(ctx, tape, rev);
    if (!progressed) return;
  }
}

/** 确保当前租约代持有会话；返回 false 表示本轮应停止该来源 */
async function ensureSession(ctx: Ctx, tape: string): Promise<boolean> {
  const { db, site, workerId, log } = ctx;
  const src = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;
  if (src.session_id && src.owner_id === workerId && src.session_lease_epoch === src.lease_epoch) return true;
  const r = await httpReq('POST', `${site}/api/sources/${tape}/session`, undefined, ctx.reqTimeoutMs);
  if (await handleCommon(ctx, tape, r)) return false;
  if (r.status !== 200) {
    log(`${tape}: 建立会话失败（status=${r.status}）`);
    return false;
  }
  // 只有仍持有租约时才登记会话；会话绑定当前租约代，旧代晚到的响应不会覆盖新一代会话
  const u = db
    .prepare('UPDATE sources SET session_id = ?, sign_token = ?, session_lease_epoch = lease_epoch WHERE tape_id = ? AND owner_id = ?')
    .run(r.body.sessionId, r.body.signToken, tape, workerId);
  if (Number(u.changes) !== 1) return false;
  log(`${tape}: 建立会话 ${r.body.sessionId.slice(0, 8)}…（签名令牌 ${String(r.body.signToken).slice(0, 8)}…）`);
  return true;
}

/** 拉取索引并对账；返回 false 表示本轮应停止该来源 */
async function refreshIndex(ctx: Ctx, tape: string): Promise<boolean> {
  const { db, site, workerId, log } = ctx;
  const src = db.prepare('SELECT session_id FROM sources WHERE tape_id = ?').get(tape) as any;
  const idx = await httpReq('GET', `${site}/api/sources/${tape}/index?sessionId=${src.session_id}`, undefined, ctx.reqTimeoutMs);
  if (await handleCommon(ctx, tape, idx)) return false;
  if (idx.status !== 200) {
    log(`${tape}: 拉取索引失败（status=${idx.status}）`);
    return false;
  }
  if (idx.body?.signToken) {
    db.prepare('UPDATE sources SET sign_token = ? WHERE tape_id = ? AND owner_id = ?').run(idx.body.signToken, tape, workerId);
  }
  // 索引对账只在仍持有租约时落库：卡住的进程恢复后，其晚到的旧索引不会回滚新持有者已做的对账
  return reconcile(ctx, tape, idx.body);
}

/** 处理 401/423 等来源级状态；返回 true 表示本轮应停止该来源 */
async function handleCommon(ctx: Ctx, tape: string, r: { status: number; body: any }): Promise<boolean> {
  const { db, workerId, log } = ctx;
  if (r.status === 401) {
    // 会话是租约代私有的：只有当前持有者才能清除，旧代晚到的 401 不动新一代会话
    const u = db.prepare('UPDATE sources SET session_id = NULL, sign_token = NULL WHERE tape_id = ? AND owner_id = ?').run(tape, workerId);
    if (Number(u.changes) === 1) log(`${tape}: 会话失效，下轮重建`);
    return true;
  }
  if (r.status === 423) {
    const until = Number(r.body?.pausedUntil ?? Date.now() + 5000);
    // 暂停是来源级事实：任一进程登记后，所有进程下一轮起停发请求
    db.prepare('UPDATE sources SET paused_until = ? WHERE tape_id = ?').run(until, tape);
    db.prepare('UPDATE sources SET session_id = NULL, sign_token = NULL WHERE tape_id = ? AND owner_id = ?').run(tape, workerId);
    log(`${tape}: 站点要求暂停至 ${new Date(until).toISOString()}，登记落库，所有进程停止向该来源发请求`);
    return true;
  }
  return false;
}

/** 索引对账：新修订登记、撤下标记（未采齐→放弃；已采齐→保留可复用）。仅租约持有者可应用 */
function reconcile(ctx: Ctx, tape: string, index: any): boolean {
  const { db, workerId, log } = ctx;
  return withTx(db, () => {
    const src = db.prepare('SELECT owner_id FROM sources WHERE tape_id = ?').get(tape) as any;
    if (!src || src.owner_id !== workerId) return false;
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
    return true;
  });
}

/** 采一页；返回 true 表示有进展（可继续采下一页） */
async function fetchPage(ctx: Ctx, tape: string, rev: any): Promise<boolean> {
  const { db, site, workerId, log } = ctx;
  const label = `${tape} ${kindName(rev.kind)} v${rev.timeline_version} rev${rev.revision}`;
  for (let attempt = 0; attempt < 4; attempt++) {
    const src = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;
    if (!src.session_id || src.owner_id !== workerId) return false;
    if (src.paused_until && Date.now() < src.paused_until) return false;
    const pageNo = rev.pages_done;

    // 1) 申请一次性翻页令牌
    const pt = await httpReq(
      'POST',
      `${site}/api/sources/${tape}/page-tokens`,
      {
        sessionId: src.session_id,
        kind: rev.kind,
        timelineVersion: rev.timeline_version,
        revision: rev.revision,
        page: pageNo,
      },
      ctx.reqTimeoutMs,
    );
    if (await handleCommon(ctx, tape, pt)) return false;
    if (pt.status === 410) {
      abandon(ctx, tape, rev, label);
      return false;
    }
    if (pt.status !== 200) {
      log(`${label} 第${pageNo + 1}页：申请翻页令牌失败（status=${pt.status}）`);
      return false;
    }
    const pageToken = pt.body.pageToken as string;
    if (pt.body.signToken) {
      db.prepare('UPDATE sources SET sign_token = ? WHERE tape_id = ? AND owner_id = ?').run(pt.body.signToken, tape, workerId);
    }

    // 2) 用「站点刚发出的签名令牌 + 当前会话」签名取页；过期则按 409 附的新令牌重签
    for (let sigTry = 0; sigTry < 3; sigTry++) {
      const cur = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;
      if (!cur.session_id || cur.owner_id !== workerId) return false;
      const sig = sha256(`${cur.sign_token}|${cur.session_id}|${pageToken}`);
      const page = await httpReq(
        'GET',
        `${site}/api/sources/${tape}/page?sessionId=${cur.session_id}&pageToken=${pageToken}&sig=${sig}`,
        undefined,
        ctx.reqTimeoutMs,
      );
      if (page.status === 0) {
        // 响应丢失而翻页令牌已消费：重新申请令牌续采（外层循环）
        log(`${label} 第${pageNo + 1}页：响应丢失（翻页令牌已消费），重新申请令牌续采`);
        break;
      }
      if (page.status === 409 && page.body?.error === 'STALE_SIGN_TOKEN') {
        db.prepare('UPDATE sources SET sign_token = ? WHERE tape_id = ? AND owner_id = ?').run(page.body.signToken, tape, workerId);
        log(`${label} 第${pageNo + 1}页：签名令牌已轮换，用新令牌重签`);
        continue;
      }
      if (page.status === 409 && page.body?.error === 'PAGE_TOKEN_CONSUMED') {
        log(`${label} 第${pageNo + 1}页：翻页令牌已被消费，重新申请`);
        break;
      }
      if (page.status === 410) {
        abandon(ctx, tape, rev, label);
        return false;
      }
      if (await handleCommon(ctx, tape, page)) return false;
      if (page.status !== 200) {
        log(`${label} 第${pageNo + 1}页：取页失败（status=${page.status}）`);
        return false;
      }
      const recs = normalize(page.body);
      storePage(ctx, tape, rev.id, pageNo, recs, label, page.body.layout);
      return true;
    }
  }
  return false;
}

function storePage(ctx: Ctx, tape: string, revId: number, pageNo: number, recs: Rec[], label: string, layout: string): void {
  const { db, workerId, log } = ctx;
  withTx(db, () => {
    // 栅栏：入库时租约已易主（本进程曾卡住、租约被接手）的晚到响应直接丢弃，
    // 不覆盖新持有者的进度，也不触碰新一代会话
    const src = db.prepare('SELECT owner_id, lease_epoch, session_lease_epoch FROM sources WHERE tape_id = ?').get(tape) as any;
    if (!src || src.owner_id !== workerId || src.session_lease_epoch !== src.lease_epoch) {
      log(`${label} 第${pageNo + 1}页：响应到达时租约已易主，丢弃（不覆盖新进度）`);
      return;
    }
    // 页级幂等：已入库的页（如重传、进程交接）直接跳过，保证断点续采不产生重复记录
    const ins = db.prepare('INSERT OR IGNORE INTO pages (revision_id, page_no, received_at) VALUES (?,?,?)').run(revId, pageNo, Date.now());
    if (Number(ins.changes) === 0) return;
    const rev = db.prepare('SELECT * FROM revisions WHERE id = ?').get(revId) as any;
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
  });
}

/** 站点已撤下（410）：放弃未采齐部分。仅租约持有者可标记，旧代晚到的 410 不回滚新进度 */
function abandon(ctx: Ctx, tape: string, rev: any, label: string): void {
  const { db, workerId, log } = ctx;
  const u = db
    .prepare(
      `UPDATE revisions SET status = 'abandoned', listed = 0, updated_at = ?
       WHERE id = ? AND EXISTS (SELECT 1 FROM sources WHERE tape_id = ? AND owner_id = ?)`,
    )
    .run(Date.now(), rev.id, tape, workerId);
  if (Number(u.changes) === 1) {
    log(`${label} 已被站点撤下（410），放弃未采齐部分（进度 ${rev.pages_done}/${rev.page_count} 页）`);
  } else {
    log(`${label} 410 到达时租约已易主，忽略`);
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
