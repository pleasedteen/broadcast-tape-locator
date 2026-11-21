/**
 * 同步采集器。
 *
 * 关键能力：
 * - 断点续采：页级进度落库（pages / revisions.pages_done），重启后已采齐的修订直接复用，
 *   未采齐的从下一页继续；站点已撤下的未完成修订放弃，已采齐的继续可用。
 * - 同号更正：索引中的清单带内容指纹。指纹不变 = 原样重发，复用已采页、仅从缺页续采；
 *   指纹变化 = 同号更正（含总页数未变的更正），登记为新修订行从头采，旧修订行保留。
 *   翻页令牌与取页均绑定指纹，翻页途中收到更正时旧令牌取页得 410 SUPERSEDED，
 *   旧修订立即放弃——新清单的页绝不混入旧修订。
 * - 时间轴定义版次（edition）：时长不变 edition 不变，单侧采齐即可复用另一侧结果；
 *   时长变更产生新 edition，须两侧清单都按新定义采齐，生效指针才在单事务内整体切换
 *   （时长、播放位置、缺失区段同时更新），切换前旧结果完整可查。
 * - 两种排版归一化解析（table / cards）。
 * - 翻页签名随站点最新签名令牌 + 当前会话计算；409 时用响应附带的新令牌重签。
 * - 翻页令牌一次性；响应丢失（连接被断开）后重新申请令牌续采，不重复消费。
 * - 来源暂停：423 后记录 paused_until，期间不向该来源发任何请求；恢复后重建会话。
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
}

type Log = (msg: string) => void;

export async function runCollector(opts: CollectorOpts): Promise<void> {
  const db = await openDb(opts.dbPath);
  const log: Log = (m) => console.log(`[sync] ${m}`);

  // 启动即对本地已有数据重算生效指针（覆盖上次退出前未完成的切换）
  for (const row of db.prepare('SELECT tape_id AS t FROM sources').all() as any[]) {
    const a = recomputeActivation(db, row.t);
    if (a.changed) log(`${row.t}: 生效版本 → 时间轴 v${a.timelineVersion} ed${a.timelineEdition}（节目单 rev${a.programRev} + 修复清单 rev${a.restorationRev}）`);
  }

  let running = true;
  process.on('SIGTERM', () => { running = false; });
  process.on('SIGINT', () => { running = false; });

  while (running) {
    const lst = await httpReq('GET', `${opts.siteUrl}/api/sources`);
    if (lst.status !== 200) {
      log(`站点不可达（status=${lst.status}），${opts.pollMs}ms 后重试`);
      await sleep(opts.pollMs);
      continue;
    }
    for (const tape of lst.body.tapes as string[]) {
      try {
        await syncSource(db, opts.siteUrl, tape, log);
      } catch (e) {
        log(`${tape}: 异常 ${e}`);
      }
    }
    await sleep(opts.pollMs);
  }
  log('采集器退出');
}

async function syncSource(db: DatabaseSync, site: string, tape: string, log: Log): Promise<void> {
  db.prepare('INSERT OR IGNORE INTO sources (tape_id) VALUES (?)').run(tape);
  let src = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;

  // 暂停期间：停止向该来源发请求
  if (src.paused_until && Date.now() < src.paused_until) return;

  if (!src.session_id) {
    const r = await httpReq('POST', `${site}/api/sources/${tape}/session`);
    if (await handleCommon(db, tape, r, log)) return;
    if (r.status !== 200) {
      log(`${tape}: 建立会话失败（status=${r.status}）`);
      return;
    }
    db.prepare('UPDATE sources SET session_id = ?, sign_token = ? WHERE tape_id = ?').run(r.body.sessionId, r.body.signToken, tape);
    log(`${tape}: 建立会话 ${r.body.sessionId.slice(0, 8)}…（签名令牌 ${String(r.body.signToken).slice(0, 8)}…）`);
    src = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;
  }

  const idx = await httpReq('GET', `${site}/api/sources/${tape}/index?sessionId=${src.session_id}`);
  if (await handleCommon(db, tape, idx, log)) return;
  if (idx.status !== 200) {
    log(`${tape}: 拉取索引失败（status=${idx.status}）`);
    return;
  }
  if (idx.body?.signToken) db.prepare('UPDATE sources SET sign_token = ? WHERE tape_id = ?').run(idx.body.signToken, tape);
  reconcile(db, tape, idx.body, log);

  // 每个轮询周期内尽量多采（上限避免单个来源占满），逐页落库。
  // 排序优先把已在采的修订推完（pages_done 多者优先），避免更正产生的新行把旧行饿死、
  // 导致翻页中更正的 SUPERSEDED 迟迟无法触发；其次按版次/类别/修订号。
  for (let i = 0; i < 8; i++) {
    const rev = db
      .prepare(
        `SELECT * FROM revisions
         WHERE tape_id = ? AND listed = 1 AND status IN ('pending','collecting')
         ORDER BY pages_done DESC, timeline_version, kind, revision LIMIT 1`,
      )
      .get(tape) as any;
    if (!rev) return;
    const progressed = await fetchPage(db, site, tape, rev, log);
    if (!progressed) return;
  }
}

/** 处理 401/423 等来源级状态；返回 true 表示本轮应停止该来源 */
async function handleCommon(db: DatabaseSync, tape: string, r: { status: number; body: any }, log: Log): Promise<boolean> {
  if (r.status === 401) {
    db.prepare('UPDATE sources SET session_id = NULL, sign_token = NULL WHERE tape_id = ?').run(tape);
    log(`${tape}: 会话失效，下轮重建`);
    return true;
  }
  if (r.status === 423) {
    const until = Number(r.body?.pausedUntil ?? Date.now() + 5000);
    db.prepare('UPDATE sources SET paused_until = ?, session_id = NULL, sign_token = NULL WHERE tape_id = ?').run(until, tape);
    log(`${tape}: 站点要求暂停至 ${new Date(until).toISOString()}，期间停止向该来源发请求`);
    return true;
  }
  return false;
}

/**
 * 索引对账：
 * - 时间轴定义按 (版次号, edition) 留痕；时长不变时 edition 不变（upsert 同一行），时长变更产生新行。
 * - 同号清单以「内容指纹」区分：指纹不变 = 原样重发，复用已采页只续采缺页；
 *   指纹变化 = 同号更正，登记为新修订行从头采，旧行在架期间保留直到索引确认撤下。
 * - 撤下：未采齐→abandoned；已采齐→保留可复用。
 */
function reconcile(db: DatabaseSync, tape: string, index: any, log: Log): void {
  withTx(db, () => {
    for (const tl of index.timelines ?? []) {
      db.prepare(
        `INSERT INTO timelines (tape_id, timeline_version, timeline_edition, duration_ms) VALUES (?,?,?,?)
         ON CONFLICT(tape_id, timeline_version, timeline_edition) DO UPDATE SET duration_ms = excluded.duration_ms`,
      ).run(tape, tl.version, tl.edition ?? 1, tl.durationMs);
    }
    const seen = new Set<string>();
    for (const m of index.manifests ?? []) {
      const hash = String(m.contentHash ?? '');
      seen.add(`${m.kind}:${m.timelineVersion}:${m.revision}:${hash}`);
      const row = db
        .prepare(
          'SELECT * FROM revisions WHERE tape_id = ? AND kind = ? AND timeline_version = ? AND revision = ? AND content_hash = ?',
        )
        .get(tape, m.kind, m.timelineVersion, m.revision, hash) as any;
      if (!row) {
        // 同号但新指纹 = 更正；或首次出现的修订
        const sameNumber = db
          .prepare('SELECT id, content_hash, status FROM revisions WHERE tape_id = ? AND kind = ? AND timeline_version = ? AND revision = ?')
          .all(tape, m.kind, m.timelineVersion, m.revision) as any[];
        db.prepare(
          `INSERT INTO revisions
             (tape_id, kind, timeline_version, timeline_edition, revision, content_hash,
              page_count, pages_done, status, listed, record_count, updated_at)
           VALUES (?,?,?,?,?,?,?,0,'pending',1,?,?)`,
        ).run(
          tape, m.kind, m.timelineVersion, m.timelineEdition ?? 1, m.revision, hash,
          m.pageCount, m.recordCount ?? 0, Date.now(),
        );
        if (sameNumber.length) {
          log(
            `${tape}: ${kindName(m.kind)} v${m.timelineVersion} rev${m.revision} 同号更正（新内容指纹 ${hash.slice(0, 8)}…，${m.pageCount} 页），旧修订保留、新清单从头采`,
          );
        } else {
          log(`${tape}: 发现 ${kindName(m.kind)} v${m.timelineVersion} rev${m.revision}（${m.pageCount} 页），待采集`);
        }
      } else {
        db.prepare('UPDATE revisions SET page_count = ?, listed = 1, record_count = ?, timeline_edition = ?, updated_at = ? WHERE id = ?')
          .run(m.pageCount, m.recordCount ?? 0, m.timelineEdition ?? 1, Date.now(), row.id);
        if (row.status === 'abandoned') {
          // 能走到这里说明站点索引仍在架且内容指纹一致：只可能是撤下后的原样重发。
          // 被同号更正顶掉的旧指纹不会再出现在索引中，因此不会被错误复活。
          if (row.pages_done > 0 && row.pages_done < row.page_count) {
            // 原样重发——保留已采页，仅把状态拨回继续从缺页续采
            db.prepare("UPDATE revisions SET status = 'pending', listed = 1 WHERE id = ?").run(row.id);
            log(`${tape}: ${kindName(m.kind)} v${m.timelineVersion} rev${m.revision} 原样重发，复用已采 ${row.pages_done}/${row.page_count} 页，从缺页续采`);
          } else {
            db.prepare("UPDATE revisions SET status = 'pending', pages_done = 0 WHERE id = ?").run(row.id);
            for (const t of ['pages', 'programs', 'segments', 'damages']) {
              db.prepare(`DELETE FROM ${t} WHERE revision_id = ?`).run(row.id);
            }
            log(`${tape}: ${kindName(m.kind)} v${m.timelineVersion} rev${m.revision} 重新上架，从头重采`);
          }
        }
      }
    }
    const locals = db.prepare('SELECT * FROM revisions WHERE tape_id = ? AND listed = 1').all(tape) as any[];
    for (const row of locals) {
      if (seen.has(`${row.kind}:${row.timeline_version}:${row.revision}:${row.content_hash}`)) continue;
      if (row.status === 'complete') {
        db.prepare('UPDATE revisions SET listed = 0 WHERE id = ?').run(row.id);
        log(`${tape}: ${kindName(row.kind)} v${row.timeline_version} rev${row.revision}（指纹 ${row.content_hash.slice(0, 8)}…）站点已撤下；本地已采齐，继续可用`);
      } else {
        db.prepare("UPDATE revisions SET listed = 0, status = 'abandoned' WHERE id = ?").run(row.id);
        log(`${tape}: ${kindName(row.kind)} v${row.timeline_version} rev${row.revision}（指纹 ${row.content_hash.slice(0, 8)}…）站点已撤下/被更正，放弃未采齐部分（已采 ${row.pages_done}/${row.page_count} 页）`);
      }
    }
  });
}

/** 采一页；返回 true 表示有进展（可继续采下一页） */
async function fetchPage(db: DatabaseSync, site: string, tape: string, rev: any, log: Log): Promise<boolean> {
  const label = `${tape} ${kindName(rev.kind)} v${rev.timeline_version} rev${rev.revision}`;
  for (let attempt = 0; attempt < 4; attempt++) {
    const src = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;
    const pageNo = rev.pages_done;

    // 1) 申请一次性翻页令牌（绑定本修订的内容指纹，翻页途中被同号更正会收到 410 SUPERSEDED）
    const pt = await httpReq('POST', `${site}/api/sources/${tape}/page-tokens`, {
      sessionId: src.session_id,
      kind: rev.kind,
      timelineVersion: rev.timeline_version,
      revision: rev.revision,
      contentHash: rev.content_hash,
      page: pageNo,
    });
    if (await handleCommon(db, tape, pt, log)) return false;
    if (pt.status === 410) {
      abandon(db, rev, log, label, pt.body?.error === 'SUPERSEDED' ? '同号更正，旧修订页作废' : '站点已撤下');
      return false;
    }
    if (pt.status !== 200) {
      log(`${label} 第${pageNo + 1}页：申请翻页令牌失败（status=${pt.status}）`);
      return false;
    }
    const pageToken = pt.body.pageToken as string;
    if (pt.body.signToken) db.prepare('UPDATE sources SET sign_token = ? WHERE tape_id = ?').run(pt.body.signToken, tape);

    // 2) 用「站点刚发出的签名令牌 + 当前会话」签名取页；过期则按 409 附的新令牌重签
    for (let sigTry = 0; sigTry < 3; sigTry++) {
      const cur = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;
      const sig = sha256(`${cur.sign_token}|${cur.session_id}|${pageToken}`);
      const page = await httpReq(
        'GET',
        `${site}/api/sources/${tape}/page?sessionId=${cur.session_id}&pageToken=${pageToken}&sig=${sig}`,
      );
      if (page.status === 0) {
        // 响应丢失而翻页令牌已消费：重新申请令牌续采（外层循环）
        log(`${label} 第${pageNo + 1}页：响应丢失（翻页令牌已消费），重新申请令牌续采`);
        break;
      }
      if (page.status === 409 && page.body?.error === 'STALE_SIGN_TOKEN') {
        db.prepare('UPDATE sources SET sign_token = ? WHERE tape_id = ?').run(page.body.signToken, tape);
        log(`${label} 第${pageNo + 1}页：签名令牌已轮换，用新令牌重签`);
        continue;
      }
      if (page.status === 409 && page.body?.error === 'PAGE_TOKEN_CONSUMED') {
        log(`${label} 第${pageNo + 1}页：翻页令牌已被消费，重新申请`);
        break;
      }
      if (page.status === 410) {
        abandon(db, rev, log, label, page.body?.error === 'SUPERSEDED' ? '同号更正，旧修订页作废' : '站点已撤下');
        return false;
      }
      if (await handleCommon(db, tape, page, log)) return false;
      if (page.status !== 200) {
        log(`${label} 第${pageNo + 1}页：取页失败（status=${page.status}）`);
        return false;
      }
      // 双保险：页体指纹必须与本地修订一致，任何不一致（协议异常/竞态更正）都不入库
      if (page.body.contentHash && page.body.contentHash !== rev.content_hash) {
        log(`${label} 第${pageNo + 1}页：页体内容指纹与本地修订不一致，拒绝入库并放弃旧修订`);
        abandon(db, rev, log, label, '页体指纹不一致（同号更正竞态）');
        return false;
      }
      const recs = normalize(page.body);
      storePage(db, rev.id, pageNo, recs, log, label, page.body.layout);
      return true;
    }
  }
  return false;
}

function storePage(db: DatabaseSync, revId: number, pageNo: number, recs: Rec[], log: Log, label: string, layout: string): void {
  withTx(db, () => {
    // 页级幂等：已入库的页（如重传）直接跳过，保证断点续采不产生重复记录
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
      // 同一事务内切换生效指针：查询侧原子看到「时长 + 播放位置 + 缺失区段」的整体更新
      const a = recomputeActivation(db, rev.tape_id);
      log(
        `${label}（指纹 ${rev.content_hash.slice(0, 8)}…）采齐（${done}/${rev.page_count} 页）` +
          (a.changed ? `；生效版本切换 → 时间轴 v${a.timelineVersion} ed${a.timelineEdition}（节目单 rev${a.programRev} + 修复清单 rev${a.restorationRev}）` : ''),
      );
    } else {
      log(`${label} 第${pageNo + 1}/${rev.page_count} 页入库（${layout} 排版）`);
    }
  });
}

function abandon(db: DatabaseSync, rev: any, log: Log, label: string, reason: string): void {
  db.prepare("UPDATE revisions SET status = 'abandoned', listed = 0, updated_at = ? WHERE id = ?").run(Date.now(), rev.id);
  log(`${label}（指纹 ${rev.content_hash.slice(0, 8)}…）${reason}（410），放弃未采齐部分（进度 ${rev.pages_done}/${rev.page_count} 页）`);
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
