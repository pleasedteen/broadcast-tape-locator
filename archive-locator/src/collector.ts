/**
 * 同步采集器。
 *
 * 关键能力：
 * - 修订版次（edition）隔离：站点对同一修订号的每次更正/原样重发都分配新 edition，
 *   每个 edition 在本地各占一行不可变修订，改稿前后的页物理隔离，绝不会拼成一份。
 * - 页级哈希复用：索引给出当前 edition 每页内容哈希；新 edition 登记时，哈希与本地
 *   历史版行（含被放弃的未完成版）相同的页直接拷贝复用、不发任何网络请求，
 *   只采集真正变化的页。重启后同样先复用再续采，已采未改页不重抓。
 * - 更正期间继续提供原有定位：新 edition 采齐前不是 complete，不参与生效选择；
 *   采齐的同一事务内切换生效指针（同版次改时长时，时长也随指针原子切换）。
 * - 断点续采：页级进度落库（pages / revisions.pages_done），页入库幂等；
 *   站点已撤下的未完成修订放弃，已采齐的继续可用。
 * - 两种排版归一化解析（table / cards）。
 * - 翻页签名随站点最新签名令牌 + 当前会话计算；409 时按响应附带的新令牌重签。
 * - 翻页令牌一次性；响应丢失（连接被断开）后重新申请令牌续采，不重复消费。
 * - 旧 edition 令牌申请收到 409 EDITION_SUPERSEDED：本轮退出，下轮按索引对账新 edition。
 * - 来源暂停：423 后记录 paused_until，期间不向该来源发任何请求；恢复后重建会话。
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
  const db = openDb(opts.dbPath);
  const log: Log = (m) => console.log(`[sync] ${m}`);

  // 启动即对本地已有数据重算生效指针（覆盖上次退出前未完成的切换）
  for (const row of db.prepare('SELECT tape_id AS t FROM sources').all() as any[]) {
    const a = recomputeActivation(db, row.t);
    if (a.changed) {
      log(
        `${row.t}: 生效版本 → 时间轴 v${a.timelineVersion}（节目单 rev${a.programRev}/ed${a.programEdition}` +
          ` + 修复清单 rev${a.restorationRev}/ed${a.restorationEdition}，时长 ${a.durationMs}ms）`,
      );
    }
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

  // 每个轮询周期内尽量多采（上限避免单个来源占满），逐页落库
  for (let i = 0; i < 8; i++) {
    const rev = db
      .prepare(
        `SELECT * FROM revisions
         WHERE tape_id = ? AND listed = 1 AND status IN ('pending','collecting')
         ORDER BY timeline_version, kind, revision, edition LIMIT 1`,
      )
      .get(tape) as any;
    if (!rev) return;
    // 哈希复用可能留下不连续的已采页（如只改中间一页）：始终从第一个缺页续采，
    // 不能只看 pages_done——否则缺页在中间时会对已存在的尾页空转。
    const missing = firstMissingPage(db, rev);
    if (missing < 0) return;
    const progressed = await fetchPage(db, site, tape, rev, missing, log);
    if (!progressed) return;
  }
}

/** 第一个尚未入库的页号；全部页已在库返回 -1 */
function firstMissingPage(db: DatabaseSync, rev: any): number {
  const have = new Set((db.prepare('SELECT page_no FROM pages WHERE revision_id = ?').all(rev.id) as any[]).map((p) => p.page_no));
  for (let p = 0; p < rev.page_count; p++) if (!have.has(p)) return p;
  return -1;
}

/** 从 0 起连续已采页数（pages_done 仅作进度展示，续采以实际缺页为准） */
function contiguousDone(db: DatabaseSync, revId: number, pageCount: number): number {
  const have = new Set((db.prepare('SELECT page_no FROM pages WHERE revision_id = ?').all(revId) as any[]).map((p) => p.page_no));
  let done = 0;
  while (done < pageCount && have.has(done)) done++;
  return done;
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

interface IndexManifest {
  kind: string;
  timelineVersion: number;
  revision: number;
  edition: number;
  pageCount: number;
  durationMs: number | null;
  pageHashes: string[];
}

/**
 * 索引对账：
 * - 新修订/新 edition 登记并按页哈希复用本地未改页；
 * - 已登记 edition 按索引哈希校验，站点侧被改动的页整页重采（新旧页不混入同一行）；
 * - 索引中消失的本地行：已采齐保留可用，未采齐放弃（其页仍可作为后续 edition 的复用来源）。
 */
function reconcile(db: DatabaseSync, tape: string, index: any, log: Log): void {
  withTx(db, () => {
    for (const tl of index.timelines ?? []) {
      db.prepare(
        `INSERT INTO timelines (tape_id, timeline_version, duration_ms) VALUES (?,?,?)
         ON CONFLICT(tape_id, timeline_version) DO UPDATE SET duration_ms = excluded.duration_ms`,
      ).run(tape, tl.version, tl.durationMs);
    }
    const seen = new Set<string>();
    for (const m of index.manifests ?? []) {
      const mf = m as IndexManifest;
      const edition = mf.edition ?? 1;
      const hashes: string[] = mf.pageHashes ?? [];
      seen.add(`${mf.kind}:${mf.timelineVersion}:${mf.revision}:${edition}`);
      const row = db
        .prepare('SELECT * FROM revisions WHERE tape_id = ? AND kind = ? AND timeline_version = ? AND revision = ? AND edition = ?')
        .get(tape, mf.kind, mf.timelineVersion, mf.revision, edition) as any;
      if (!row) {
        const info = db
          .prepare(
            `INSERT INTO revisions (tape_id, kind, timeline_version, revision, edition, page_count, pages_done, status, listed, duration_ms, updated_at)
             VALUES (?,?,?,?,?,?,0,'pending',1,?,?)`,
          )
          .run(tape, mf.kind, mf.timelineVersion, mf.revision, edition, mf.pageCount, mf.durationMs, Date.now());
        const revId = Number(info.lastInsertRowid);
        const reused = seedPagesFromHistory(db, tape, mf, revId, hashes);
        const have = countPages(db, revId);
        const done = contiguousDone(db, revId, mf.pageCount);
        const complete = done >= mf.pageCount;
        db.prepare('UPDATE revisions SET pages_done = ?, status = ? WHERE id = ?')
          .run(done, complete ? 'complete' : 'collecting', revId);
        if (complete) {
          const a = recomputeActivation(db, tape);
          log(
            `${tape}: ${kindName(mf.kind)} v${mf.timelineVersion} rev${mf.revision} edition ${edition} 登记，` +
              `${reused}/${mf.pageCount} 页按哈希复用本地未改页，直接采齐` +
              (a.changed ? `；生效切换 → rev${a.programRev}/ed${a.programEdition} + rev${a.restorationRev}/ed${a.restorationEdition}` : '（更正稿内容与当前生效一致，定位不变）'),
          );
        } else {
          log(
            `${tape}: ${kindName(mf.kind)} v${mf.timelineVersion} rev${mf.revision} edition ${edition} 待采集，` +
              `${reused}/${mf.pageCount} 页按哈希复用本地未改页，余 ${mf.pageCount - have} 页需抓取`,
          );
        }
        continue;
      }

      db.prepare('UPDATE revisions SET page_count = ?, listed = 1, duration_ms = ?, updated_at = ? WHERE id = ?')
        .run(mf.pageCount, mf.durationMs, Date.now(), row.id);
      if (row.status === 'abandoned') {
        // 重新上架：清空旧内容后按新哈希重新复用/重采
        purgeRevision(db, row.id);
        const reused = seedPagesFromHistory(db, tape, mf, row.id, hashes);
        const done = contiguousDone(db, row.id, mf.pageCount);
        db.prepare('UPDATE revisions SET pages_done = ?, status = ? WHERE id = ?')
          .run(done, done >= mf.pageCount ? 'complete' : 'collecting', row.id);
        log(`${tape}: ${kindName(mf.kind)} v${mf.timelineVersion} rev${mf.revision} edition ${edition} 重新上架，复用 ${reused}/${mf.pageCount} 页后重采`);
      } else {
        // 同 edition 哈希校验：站点若在同一 edition 内又改动（异常更正），被改页整页重采，杜绝拼接
        const purged = purgeMismatchedPages(db, row.id, hashes);
        if (purged > 0) {
          const done = contiguousDone(db, row.id, mf.pageCount);
          db.prepare('UPDATE revisions SET pages_done = ?, status = ? WHERE id = ?')
            .run(done, done >= mf.pageCount ? 'complete' : 'collecting', row.id);
          log(`${tape}: ${kindName(mf.kind)} rev${mf.revision} edition ${edition} 有 ${purged} 页内容哈希变化，整页作废重采（不与旧页拼接）`);
        }
      }
    }
    const locals = db
      .prepare('SELECT * FROM revisions WHERE tape_id = ? AND listed = 1')
      .all(tape) as any[];
    for (const row of locals) {
      if (seen.has(`${row.kind}:${row.timeline_version}:${row.revision}:${row.edition}`)) continue;
      if (row.status === 'complete') {
        db.prepare('UPDATE revisions SET listed = 0 WHERE id = ?').run(row.id);
        log(`${tape}: ${kindName(row.kind)} v${row.timeline_version} rev${row.revision} edition ${row.edition} 站点已撤下；本地已采齐，继续可用`);
      } else {
        db.prepare("UPDATE revisions SET listed = 0, status = 'abandoned' WHERE id = ?").run(row.id);
        log(`${tape}: ${kindName(row.kind)} v${row.timeline_version} rev${row.revision} edition ${row.edition} 已被更正/撤下，放弃未采齐部分（已采 ${row.pages_done}/${row.page_count} 页，仍可供后续版次复用）`);
      }
    }
  });
}

/** 删除某修订行的全部页与记录（页级断点一并清空） */
function purgeRevision(db: DatabaseSync, revId: number): void {
  for (const t of ['pages', 'programs', 'segments', 'damages']) {
    db.prepare(`DELETE FROM ${t} WHERE revision_id = ?`).run(revId);
  }
}

/** 哈希与索引不一致的页整页删除（含该页记录），返回删除页数 */
function purgeMismatchedPages(db: DatabaseSync, revId: number, hashes: string[]): number {
  const pages = db.prepare('SELECT page_no, content_hash FROM pages WHERE revision_id = ?').all(revId) as any[];
  let purged = 0;
  for (const p of pages) {
    if (p.content_hash && hashes[p.page_no] && p.content_hash !== hashes[p.page_no]) {
      deletePage(db, revId, p.page_no);
      purged++;
    }
  }
  return purged;
}

function deletePage(db: DatabaseSync, revId: number, pageNo: number): void {
  db.prepare('DELETE FROM pages WHERE revision_id = ? AND page_no = ?').run(revId, pageNo);
  db.prepare('DELETE FROM programs WHERE revision_id = ? AND page_no = ?').run(revId, pageNo);
  db.prepare('DELETE FROM segments WHERE revision_id = ? AND page_no = ?').run(revId, pageNo);
  db.prepare('DELETE FROM damages WHERE revision_id = ? AND page_no = ?').run(revId, pageNo);
}

function countPages(db: DatabaseSync, revId: number): number {
  return Number((db.prepare('SELECT COUNT(*) AS n FROM pages WHERE revision_id = ?').get(revId) as any).n);
}

/**
 * 页级复用：同一 (tape, kind, 版次, 修订号) 的历史 edition 行中，
 * 页号与内容哈希都匹配的页直接拷贝（页断点 + 该页全部记录），不发网络请求。
 * 未完成而被放弃的 edition 也可作为捐献者——重启续采时已采未改页同样复用。
 * 返回复用页数。
 */
function seedPagesFromHistory(db: DatabaseSync, tape: string, mf: IndexManifest, newRevId: number, hashes: string[]): number {
  const donors = db
    .prepare(
      `SELECT id FROM revisions
       WHERE tape_id = ? AND kind = ? AND timeline_version = ? AND revision = ? AND id <> ?
       ORDER BY edition DESC`,
    )
    .all(tape, mf.kind, mf.timelineVersion, mf.revision, newRevId) as any[];
  if (!donors.length) return 0;
  let reused = 0;
  for (let pageNo = 0; pageNo < mf.pageCount; pageNo++) {
    const hash = hashes[pageNo];
    if (!hash) continue;
    if ((db.prepare('SELECT 1 FROM pages WHERE revision_id = ? AND page_no = ?').get(newRevId, pageNo) as any)) continue;
    const donorPage = db
      .prepare(
        `SELECT p.revision_id FROM pages p
         JOIN revisions r ON r.id = p.revision_id
         WHERE p.revision_id IN (${donors.map(() => '?').join(',')})
           AND p.page_no = ? AND p.content_hash = ?
         ORDER BY r.edition DESC LIMIT 1`,
      )
      .get(...donors.map((d) => d.id), pageNo, hash) as any;
    if (!donorPage) continue;
    const donorId = donorPage.revision_id;
    db.prepare('INSERT OR IGNORE INTO pages (revision_id, page_no, received_at, content_hash) VALUES (?,?,?,?)')
      .run(newRevId, pageNo, Date.now(), hash);
    db.prepare(
      `INSERT OR IGNORE INTO programs (revision_id, program_id, title, start_ms, end_ms, page_no)
       SELECT ?, program_id, title, start_ms, end_ms, page_no FROM programs WHERE revision_id = ? AND page_no = ?`,
    ).run(newRevId, donorId, pageNo);
    db.prepare(
      `INSERT OR IGNORE INTO segments (revision_id, copy_id, master_start_ms, master_end_ms, copy_start_ms, rate, page_no)
       SELECT ?, copy_id, master_start_ms, master_end_ms, copy_start_ms, rate, page_no FROM segments WHERE revision_id = ? AND page_no = ?`,
    ).run(newRevId, donorId, pageNo);
    db.prepare(
      `INSERT OR IGNORE INTO damages (revision_id, copy_id, master_start_ms, master_end_ms, note, page_no)
       SELECT ?, copy_id, master_start_ms, master_end_ms, note, page_no FROM damages WHERE revision_id = ? AND page_no = ?`,
    ).run(newRevId, donorId, pageNo);
    reused++;
  }
  return reused;
}

/** 采指定的一页；返回 true 表示有进展（可继续采下一页） */
async function fetchPage(db: DatabaseSync, site: string, tape: string, rev: any, pageNo: number, log: Log): Promise<boolean> {
  const label = `${tape} ${kindName(rev.kind)} v${rev.timeline_version} rev${rev.revision} ed${rev.edition}`;
  for (let attempt = 0; attempt < 4; attempt++) {
    const src = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;

    // 1) 申请一次性翻页令牌（显式带上 edition：站点已更正时返回 409 引导对账）
    const pt = await httpReq('POST', `${site}/api/sources/${tape}/page-tokens`, {
      sessionId: src.session_id,
      kind: rev.kind,
      timelineVersion: rev.timeline_version,
      revision: rev.revision,
      edition: rev.edition,
      page: pageNo,
    });
    if (await handleCommon(db, tape, pt, log)) return false;
    if (pt.status === 409 && pt.body?.error === 'EDITION_SUPERSEDED') {
      log(`${label} 第${pageNo + 1}页：该 edition 已被 ${pt.body.currentEdition} 取代，下轮按索引对账新 edition`);
      return false;
    }
    if (pt.status === 410) {
      abandon(db, rev, log, label);
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
      if (page.status === 409 && page.body?.error === 'EDITION_SUPERSEDED') {
        log(`${label} 第${pageNo + 1}页：取页时该 edition 已被 ${page.body.currentEdition} 取代，下轮对账`);
        return false;
      }
      if (page.status === 410) {
        abandon(db, rev, log, label);
        return false;
      }
      if (await handleCommon(db, tape, page, log)) return false;
      if (page.status !== 200) {
        log(`${label} 第${pageNo + 1}页：取页失败（status=${page.status}）`);
        return false;
      }
      if (page.body.edition !== rev.edition) {
        // 双保险：站点页响应与本地修订行 edition 不一致时绝不入库，避免拼接
        log(`${label} 第${pageNo + 1}页：响应 edition ${page.body.edition} 与本地不符，丢弃并下轮对账`);
        return false;
      }
      const recs = normalize(page.body);
      storePage(db, rev.id, pageNo, recs, page.body.pageHash ?? null, log, label, page.body.layout);
      return true;
    }
  }
  return false;
}

function storePage(db: DatabaseSync, revId: number, pageNo: number, recs: Rec[], contentHash: string | null, log: Log, label: string, layout: string): void {
  withTx(db, () => {
    // 页级幂等：已入库的页（如重传）直接跳过，保证断点续采不产生重复记录
    const ins = db.prepare('INSERT OR IGNORE INTO pages (revision_id, page_no, received_at, content_hash) VALUES (?,?,?,?)')
      .run(revId, pageNo, Date.now(), contentHash);
    if (Number(ins.changes) === 0) return;
    const rev = db.prepare('SELECT * FROM revisions WHERE id = ?').get(revId) as any;
    for (const rec of recs) {
      if (rec.type === 'program') {
        db.prepare('INSERT OR REPLACE INTO programs (revision_id, program_id, title, start_ms, end_ms, page_no) VALUES (?,?,?,?,?,?)')
          .run(revId, rec.programId, rec.title, rec.startMs, rec.endMs, pageNo);
      } else if (rec.type === 'segment') {
        db.prepare('INSERT INTO segments (revision_id, copy_id, master_start_ms, master_end_ms, copy_start_ms, rate, page_no) VALUES (?,?,?,?,?,?,?)')
          .run(revId, rec.copyId, rec.masterStartMs, rec.masterEndMs, rec.copyStartMs, rec.rate, pageNo);
      } else {
        db.prepare('INSERT INTO damages (revision_id, copy_id, master_start_ms, master_end_ms, note, page_no) VALUES (?,?,?,?,?,?)')
          .run(revId, rec.copyId, rec.masterStartMs, rec.masterEndMs, rec.note, pageNo);
      }
    }
    const done = contiguousDone(db, revId, rev.page_count);
    const complete = done >= rev.page_count;
    db.prepare('UPDATE revisions SET pages_done = ?, status = ?, updated_at = ? WHERE id = ?')
      .run(done, complete ? 'complete' : 'collecting', Date.now(), revId);
    if (complete) {
      // 同一事务内切换生效指针：时长、播放位置、缺失区段整体更新，查询侧原子看到
      const a = recomputeActivation(db, rev.tape_id);
      log(
        `${label} 采齐（${done}/${rev.page_count} 页）` +
          (a.changed
            ? `；生效版本切换 → 时间轴 v${a.timelineVersion}（节目单 rev${a.programRev}/ed${a.programEdition}` +
              ` + 修复清单 rev${a.restorationRev}/ed${a.restorationEdition}，时长 ${a.durationMs}ms）`
            : ''),
      );
    } else {
      log(`${label} 第${pageNo + 1}/${rev.page_count} 页入库（${layout} 排版）`);
    }
  });
}

function abandon(db: DatabaseSync, rev: any, log: Log, label: string): void {
  db.prepare("UPDATE revisions SET status = 'abandoned', listed = 0, updated_at = ? WHERE id = ?").run(Date.now(), rev.id);
  log(`${label} 已被站点撤下（410），放弃未采齐部分（进度 ${rev.pages_done}/${rev.page_count} 页）`);
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
