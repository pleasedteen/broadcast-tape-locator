/**
 * 同步采集器（主备双站接力版）。
 *
 * 同一盘母带接入两个模拟站点：主站 A（SITE_URL_A）与备站 B（SITE_URL_B），
 * 两站会话、签名令牌、暂停状态各自独立，采集器按站分别维护并落库。
 *
 * 接力语义：
 * - 每轮优先使用当前 active_replica（主站优先；一旦主站暂停/撤下/不可达即切备站，
 *   选择结果落库，即使采集器重启也沿用备站，不回摆）。
 * - 各站分页可以不同：页级断点按「修订 × 站点」独立记录（replica_pages）。
 *   接力到备站时，依据已采记录数 records_done 与备站页大小计算
 *   「首个含未采记录的页」 floor(records_done / pageSize)，已在主站采过的页
 *   （含两站页边界的重叠部分）整页跳过，只请求含未采记录的页。
 * - 记录按内容哈希幂等汇入（rev_records）：重叠页里已采记录自动去重，不产生重复。
 * - 同号清单内容冲突（备站索引 contentHash 与已采内容不同）或母带时长冲突：
 *   只置 conflict 标记，拒收冲突站的数据，绝不覆盖已有记录、不改变生效指针。
 * - 采齐判定 records_done == record_count，与从哪一站采到无关；采齐后仍按
 *   「最新共同时间轴版次」规则在单事务内切生效指针。
 *
 * 继承的单站能力：两种排版归一化、签名 409 重签、翻页令牌一次性/丢响应续领、
 * 423 暂停避让、撤下对账（未采齐 abandoned、已采齐保留可用）。
 */
import type { DatabaseSync } from 'node:sqlite';
import { openDb, recomputeActivation, withTx } from './db.ts';
import { canonicalHash, httpReq, sha256, sleep } from './util.ts';
import type { Rec } from './types.ts';

export interface CollectorOpts {
  sites: Array<{ replica: string; url: string }>;
  dbPath: string;
  pollMs: number;
}

type Log = (msg: string) => void;

/** 每站在 sources 表里的独立状态列（A 站沿用无后缀的四列） */
const COLS: Record<string, { paused: string; session: string; token: string }> = {
  A: { paused: 'paused_until', session: 'session_id', token: 'sign_token' },
  B: { paused: 'paused_until_b', session: 'session_id_b', token: 'sign_token_b' },
};

export async function runCollector(opts: CollectorOpts): Promise<void> {
  const db = openDb(opts.dbPath);
  const log: Log = (m) => console.log(`[sync] ${m}`);

  // 启动即对本地已有数据重算生效指针（覆盖上次退出前未完成的切换）
  for (const row of db.prepare('SELECT tape_id AS t FROM sources').all() as any[]) {
    const a = recomputeActivation(db, row.t);
    if (a.changed) log(`${row.t}: 生效版本 → 时间轴 v${a.timelineVersion}（节目单 rev${a.programRev} + 修复清单 rev${a.restorationRev}）`);
  }

  let running = true;
  process.on('SIGTERM', () => { running = false; });
  process.on('SIGINT', () => { running = false; });

  while (running) {
    // 任一站能给出母带列表即可继续；两站都不可达才等待重试
    let tapes: string[] | null = null;
    for (const site of opts.sites) {
      const lst = await httpReq('GET', `${site.url}/api/sources`);
      if (lst.status === 200) {
        tapes = lst.body.tapes as string[];
        break;
      }
      log(`站点 ${site.replica} 不可达（status=${lst.status}）`);
    }
    if (!tapes) {
      log(`主备站点均不可达，${opts.pollMs}ms 后重试`);
      await sleep(opts.pollMs);
      continue;
    }
    for (const tape of tapes) {
      try {
        await syncSource(db, opts.sites, tape, log);
      } catch (e) {
        log(`${tape}: 异常 ${e}`);
      }
    }
    await sleep(opts.pollMs);
  }
  log('采集器退出');
}

async function syncSource(db: DatabaseSync, sites: CollectorOpts['sites'], tape: string, log: Log): Promise<void> {
  db.prepare('INSERT OR IGNORE INTO sources (tape_id) VALUES (?)').run(tape);

  // 站点选择：active 优先。
  // - active 处于暂停窗口时「临时绕行」其他站，不改写 active（恢复后自动回到 active，避免目标修订被孤立）；
  // - 硬故障（410 撤下 / 401 失效 / 网络）才在 switchAwayFrom / tryNextReplica / handleCommonAndRelay 中持久切 active。
  const srcRow0 = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;
  const ordered = [...sites].sort((a, b) =>
    a.replica === srcRow0.active_replica ? -1 : b.replica === srcRow0.active_replica ? 1 : 0,
  );

  let chosen: { replica: string; url: string } | null = null;
  for (const site of ordered) {
    const c = COLS[site.replica];
    const row = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;
    if (row[c.paused] && Date.now() < row[c.paused]) {
      log(`${tape}: 站点 ${site.replica} 暂停避让中（至 ${new Date(row[c.paused]).toISOString()}）`);
      continue;
    }
    chosen = site;
    break;
  }
  if (!chosen) {
    log(`${tape}: 主备两站均在暂停窗口，本轮跳过`);
    return;
  }
  const site = chosen;
  const cols = COLS[site.replica];

  if (site.replica !== srcRow0.active_replica) {
    // 仅因 active 暂停而临时绕行：active_replica 保持不变，恢复后自动回到原站
    log(`${tape}: active 站点 ${srcRow0.active_replica} 暂停，临时绕行站点 ${site.replica}（active 保持 ${srcRow0.active_replica}）`);
  }
  // 记录本轮实际服务站点（无论持久接力还是临时绕行），供 /sync/status 观察
  if (site.replica !== srcRow0.last_replica) {
    db.prepare('UPDATE sources SET last_replica = ? WHERE tape_id = ?').run(site.replica, tape);
  }
  let srcRow = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;

  // 建立本站独立会话（两站会话互不影响）
  if (!srcRow[cols.session]) {
    const r = await httpReq('POST', `${site.url}/api/sources/${tape}/session`);
    if (await handleCommon(db, tape, site.replica, r, log)) return;
    if (r.status !== 200) {
      log(`${tape}: 站点 ${site.replica} 建立会话失败（status=${r.status}），尝试下一站点`);
      return tryNextReplica(db, sites, tape, site.replica, log);
    }
    db.prepare(`UPDATE sources SET ${cols.session} = ?, ${cols.token} = ? WHERE tape_id = ?`).run(
      r.body.sessionId, r.body.signToken, tape,
    );
    log(`${tape}: 与站点 ${site.replica} 建立会话 ${String(r.body.sessionId).slice(0, 8)}…`);
    srcRow = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;
  }

  const idx = await httpReq('GET', `${site.url}/api/sources/${tape}/index?sessionId=${srcRow[cols.session]}`);
  if (await handleCommon(db, tape, site.replica, idx, log)) return;
  if (idx.status !== 200) {
    log(`${tape}: 站点 ${site.replica} 拉取索引失败（status=${idx.status}），尝试下一站点`);
    return tryNextReplica(db, sites, tape, site.replica, log);
  }
  if (idx.body?.signToken) db.prepare(`UPDATE sources SET ${cols.token} = ? WHERE tape_id = ?`).run(idx.body.signToken, tape);
  reconcile(db, tape, site.replica, idx.body, log);

  // 本盘在当前站点没有任何可采修订时结束本轮，避免对在架但无可采内容的来源空转刷索引
  const anyWork = db
    .prepare(
      `SELECT 1 FROM revisions r
       WHERE r.tape_id = ? AND r.listed = 1 AND r.status IN ('pending','collecting')
       AND EXISTS (SELECT 1 FROM replica_meta m WHERE m.revision_id = r.id AND m.replica = ? AND m.listed = 1 AND m.conflict = 0)
       LIMIT 1`,
    )
    .get(tape, site.replica);
  if (!anyWork) return;

  // 每个轮询周期内尽量多采（上限避免单个来源占满），逐页落库
  for (let i = 0; i < 16; i++) {
    const rev = db
      .prepare(
        `SELECT r.* FROM revisions r
         WHERE r.tape_id = ? AND r.listed = 1 AND r.status IN ('pending','collecting')
         AND EXISTS (SELECT 1 FROM replica_meta m WHERE m.revision_id = r.id AND m.replica = ? AND m.listed = 1 AND m.conflict = 0)
         ORDER BY r.timeline_version, r.kind, r.revision LIMIT 1`,
      )
      .get(tape, site.replica) as any;
    if (!rev) return;
    const progressed = await fetchPage(db, sites, site, tape, rev, log);
    if (!progressed) return;
  }
}

/** 当前站点不可用时，把 active 切给另一站并立即再试一轮（重启后同样走此路径） */
async function tryNextReplica(
  db: DatabaseSync,
  sites: CollectorOpts['sites'],
  tape: string,
  failedReplica: string,
  log: Log,
): Promise<void> {
  const other = sites.find((s) => s.replica !== failedReplica);
  if (!other) return;
  const row = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;
  const c = COLS[other.replica];
  if (row[c.paused] && Date.now() < row[c.paused]) {
    log(`${tape}: 另一站点 ${other.replica} 也在暂停窗口，等待恢复`);
    return;
  }
  db.prepare('UPDATE sources SET active_replica = ? WHERE tape_id = ?').run(other.replica, tape);
  log(`${tape}: 站点 ${failedReplica} 不可用，接力到${other.replica === 'B' ? '备' : '主'}站 ${other.replica}`);
  await syncSource(db, sites, tape, log);
}

/** 处理 401/423 等来源级状态（按站独立登记）；返回 true 表示本轮应停止该来源 */
async function handleCommon(
  db: DatabaseSync,
  tape: string,
  replica: string,
  r: { status: number; body: any },
  log: Log,
): Promise<boolean> {
  const cols = COLS[replica];
  if (r.status === 401) {
    db.prepare(`UPDATE sources SET ${cols.session} = NULL, ${cols.token} = NULL WHERE tape_id = ?`).run(tape);
    log(`${tape}: 站点 ${replica} 会话失效，下轮重建`);
    return true;
  }
  if (r.status === 423) {
    const until = Number(r.body?.pausedUntil ?? Date.now() + 5000);
    db.prepare(`UPDATE sources SET ${cols.paused} = ?, ${cols.session} = NULL, ${cols.token} = NULL WHERE tape_id = ?`).run(
      until, tape,
    );
    log(`${tape}: 站点 ${replica} 要求暂停至 ${new Date(until).toISOString()}，期间停止向该站发请求（另一站不受影响）`);
    return true;
  }
  return false;
}

/**
 * 取页过程中收到 401/423：登记本站状态后，若另一站可用则立刻在另一站续采同一修订（接力）。
 * 返回 true 表示本轮无需再采（已接力或两站都不可用）。
 */
async function handleCommonAndRelay(
  db: DatabaseSync,
  sites: CollectorOpts['sites'],
  tape: string,
  rev: any,
  currentReplica: string,
  r: { status: number; body: any },
  log: Log,
): Promise<boolean> {
  const stop = await handleCommon(db, tape, currentReplica, r, log);
  if (!stop) return false;
  if (r.status !== 423 && r.status !== 401) return true;
  const other = sites.find((s) => s.replica !== currentReplica);
  if (!other) return true;
  const cols = COLS[other.replica];
  const row = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;
  if (row[cols.paused] && Date.now() < row[cols.paused]) return true; // 两站都暂停，等待
  if (r.status === 401) {
    // 会话失效属于硬故障：持久接力到另一站
    db.prepare('UPDATE sources SET active_replica = ? WHERE tape_id = ?').run(other.replica, tape);
    log(`${tape}: 站点 ${currentReplica} 会话失效，持久接力到站点 ${other.replica}`);
  } else {
    // 423 暂停：只临时绕行，不改写 active，恢复后自动回到原站（避免目标修订被孤立）
    log(`${tape}: 站点 ${currentReplica} 暂停，临时由站点 ${other.replica} 续采（active 不变）`);
  }
  await syncSource(db, sites, tape, log);
  return true;
}

/** 索引对账（按站）：新修订登记、撤下标记、内容/时长冲突标记 */
function reconcile(db: DatabaseSync, tape: string, replica: string, index: any, log: Log): void {
  withTx(db, () => {
    // 时间轴时长：首次值为基准；同版次不同值只标记冲突，不覆盖基准
    for (const tl of index.timelines ?? []) {
      const exist = db
        .prepare('SELECT * FROM timelines WHERE tape_id = ? AND timeline_version = ?')
        .get(tape, tl.version) as any;
      if (!exist) {
        db.prepare('INSERT INTO timelines (tape_id, timeline_version, duration_ms, conflict) VALUES (?,?,?,0)').run(
          tape, tl.version, tl.durationMs,
        );
      } else if (exist.duration_ms !== tl.durationMs && !exist.conflict) {
        db.prepare('UPDATE timelines SET conflict = 1 WHERE tape_id = ? AND timeline_version = ?').run(tape, tl.version);
        log(`${tape}: 时间轴 v${tl.version} 时长冲突（基准 ${exist.duration_ms}ms vs 站点 ${replica} ${tl.durationMs}ms）— 保留基准，定位结果不变`);
      }
    }

    const seen = new Set<string>();
    for (const m of index.manifests ?? []) {
      seen.add(`${m.kind}:${m.timelineVersion}:${m.revision}`);
      let row = db
        .prepare('SELECT * FROM revisions WHERE tape_id = ? AND kind = ? AND timeline_version = ? AND revision = ?')
        .get(tape, m.kind, m.timelineVersion, m.revision) as any;
      if (!row) {
        const info = db
          .prepare(
            `INSERT INTO revisions (tape_id, kind, timeline_version, revision, page_count, pages_done, records_done,
               record_count, status, listed, conflict, updated_at)
             VALUES (?,?,?,?,?,0,0,?,'pending',1,0,?)`,
          )
          .run(tape, m.kind, m.timelineVersion, m.revision, m.pageCount, m.recordCount, Date.now());
        row = db.prepare('SELECT * FROM revisions WHERE id = ?').get(Number(info.lastInsertRowid)) as any;
        log(`${tape}: 站点 ${replica} 通告 ${kindName(m.kind)} v${m.timelineVersion} rev${m.revision}（${m.recordCount} 条 / ${m.pageCount} 页），待采集`);
      } else {
        db.prepare('UPDATE revisions SET record_count = ?, updated_at = ? WHERE id = ?').run(m.recordCount, Date.now(), row.id);
        // 两站撤下后被本站重新上架：从头重采
        if (row.status === 'abandoned') {
          db.prepare("UPDATE revisions SET status = 'pending', pages_done = 0, records_done = 0, listed = 1 WHERE id = ?").run(row.id);
          for (const t of ['replica_pages', 'rev_records', 'programs', 'segments', 'damages']) {
            db.prepare(`DELETE FROM ${t} WHERE revision_id = ?`).run(row.id);
          }
          db.prepare('DELETE FROM replica_meta WHERE revision_id = ?').run(row.id);
          log(`${tape}: ${kindName(m.kind)} v${m.timelineVersion} rev${m.revision} 站点 ${replica} 重新上架，从头重采`);
        }
      }

      // 内容冲突判定：与「另一站已登记的同号清单哈希」或本站此前哈希比较，不同即冲突。
      // 冲突只打标记并拒收该站此单，不覆盖任何已采记录、不动生效指针。
      const otherHash = db
        .prepare('SELECT content_hash AS h FROM replica_meta WHERE revision_id = ? AND replica <> ? AND content_hash IS NOT NULL LIMIT 1')
        .get(row.id, replica) as any;
      const ownMeta = db.prepare('SELECT * FROM replica_meta WHERE revision_id = ? AND replica = ?').get(row.id, replica) as any;
      const clash =
        (otherHash?.h && m.contentHash && otherHash.h !== m.contentHash) ||
        (ownMeta?.content_hash && m.contentHash && ownMeta.content_hash !== m.contentHash);
      upsertReplicaMeta(db, row.id, replica, m, clash ? 1 : 0);
      if (clash && !ownMeta?.conflict) {
        db.prepare('UPDATE revisions SET conflict = 1 WHERE id = ?').run(row.id);
        log(`${tape}: ${kindName(m.kind)} v${m.timelineVersion} rev${m.revision} 站点 ${replica} 内容哈希与已采清单冲突 — 拒收该站数据，已有定位结果不变`);
      }
    }

    // 本站撤下对账：该站 meta 标 listed=0；两站都撤下才改动修订主状态
    const metas = db
      .prepare(
        `SELECT rm.*, r.kind, r.timeline_version AS tv, r.revision AS rev, r.status, r.pages_done, r.page_count
         FROM replica_meta rm JOIN revisions r ON r.id = rm.revision_id
         WHERE r.tape_id = ? AND rm.replica = ?`,
      )
      .all(tape, replica) as any[];
    for (const meta of metas) {
      const key = `${meta.kind}:${meta.tv}:${meta.rev}`;
      if (seen.has(key)) continue; // 本站仍在架，meta 已在上面更新
      if (!meta.listed) continue; // 本站早已标记撤下，避免每轮重复刷日志
      db.prepare('UPDATE replica_meta SET listed = 0 WHERE revision_id = ? AND replica = ?').run(meta.revision_id, replica);
      const anyListed = (db
        .prepare('SELECT COUNT(*) AS n FROM replica_meta WHERE revision_id = ? AND listed = 1')
        .get(meta.revision_id) as any).n > 0;
      if (!anyListed) {
        if (meta.status === 'complete') {
          db.prepare('UPDATE revisions SET listed = 0 WHERE id = ?').run(meta.revision_id);
          log(`${tape}: ${kindName(meta.kind)} v${meta.tv} rev${meta.rev} 两站均已撤下；本地已采齐，继续可用`);
        } else if (meta.status !== 'abandoned') {
          db.prepare("UPDATE revisions SET listed = 0, status = 'abandoned' WHERE id = ?").run(meta.revision_id);
          log(`${tape}: ${kindName(meta.kind)} v${meta.tv} rev${meta.rev} 两站均已撤下，放弃未采齐部分（已采 ${meta.pages_done}/${meta.page_count} 页）`);
        }
      } else {
        log(`${tape}: ${kindName(meta.kind)} v${meta.tv} rev${meta.rev} 站点 ${replica} 已撤下，另一站仍在架，可接力`);
      }
    }
  });
}

function upsertReplicaMeta(db: DatabaseSync, revId: number, replica: string, m: any, conflict: number): void {
  const exist = db.prepare('SELECT * FROM replica_meta WHERE revision_id = ? AND replica = ?').get(revId, replica) as any;
  if (!exist) {
    db.prepare(
      `INSERT INTO replica_meta (revision_id, replica, page_size, page_count, record_count, listed, content_hash, conflict)
       VALUES (?,?,?,?,?,1,?,?)`,
    ).run(revId, replica, m.pageSize, m.pageCount, m.recordCount, m.contentHash ?? null, conflict);
  } else {
    db.prepare(
      `UPDATE replica_meta SET page_size = ?, page_count = ?, record_count = ?, listed = 1,
        content_hash = COALESCE(content_hash, ?), conflict = MAX(conflict, ?) WHERE revision_id = ? AND replica = ?`,
    ).run(m.pageSize, m.pageCount, m.recordCount, m.contentHash ?? null, conflict, revId, replica);
  }
}

/** 采一页；返回 true 表示有进展（可继续采下一页） */
async function fetchPage(
  db: DatabaseSync,
  sites: CollectorOpts['sites'],
  site: { replica: string; url: string },
  tape: string,
  rev: any,
  log: Log,
): Promise<boolean> {
  const label = `${tape} ${kindName(rev.kind)} v${rev.timeline_version} rev${rev.revision}`;
  const meta = db
    .prepare('SELECT * FROM replica_meta WHERE revision_id = ? AND replica = ?')
    .get(rev.id, site.replica) as any;
  if (!meta || !meta.listed || meta.conflict) return false;

  // 接力关键：首个含未采记录的页 = floor(已采记录数 / 本站页大小)；
  // 已在其他站采过、本站也已取过的页直接跳过，保证只请求含未采记录的页。
  const fresh = db.prepare('SELECT * FROM revisions WHERE id = ?').get(rev.id) as any;
  let pageNo = Math.floor(fresh.records_done / meta.page_size);
  while (pageNo < meta.page_count) {
    const done = db
      .prepare('SELECT 1 FROM replica_pages WHERE revision_id = ? AND replica = ? AND page_no = ?')
      .get(rev.id, site.replica, pageNo);
    if (!done) break;
    pageNo += 1;
  }
  if (pageNo >= meta.page_count) return false; // 本站没有含未采记录的页了

  for (let attempt = 0; attempt < 4; attempt++) {
    const src = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;
    const cols = COLS[site.replica];

    // 1) 申请一次性翻页令牌（本站会话独立签发）
    const pt = await httpReq('POST', `${site.url}/api/sources/${tape}/page-tokens`, {
      sessionId: src[cols.session],
      kind: rev.kind,
      timelineVersion: rev.timeline_version,
      revision: rev.revision,
      page: pageNo,
    });
    if (await handleCommonAndRelay(db, sites, tape, rev, site.replica, pt, log)) return false;
    if (pt.status === 410) {
      return switchAwayFrom(db, sites, tape, rev.id, site.replica, log, label);
    }
    if (pt.status !== 200) {
      log(`${label} 第${pageNo + 1}页（站 ${site.replica}）：申请翻页令牌失败（status=${pt.status}）`);
      return false;
    }
    const pageToken = pt.body.pageToken as string;
    if (pt.body.signToken) db.prepare(`UPDATE sources SET ${cols.token} = ? WHERE tape_id = ?`).run(pt.body.signToken, tape);

    // 2) 用「站点刚发出的签名令牌 + 当前会话」签名取页；过期则按 409 附的新令牌重签
    for (let sigTry = 0; sigTry < 3; sigTry++) {
      const cur = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;
      const sig = sha256(`${cur[cols.token]}|${cur[cols.session]}|${pageToken}`);
      const page = await httpReq(
        'GET',
        `${site.url}/api/sources/${tape}/page?sessionId=${cur[cols.session]}&pageToken=${pageToken}&sig=${sig}`,
      );
      if (page.status === 0) {
        // 响应丢失而翻页令牌已消费：重新申请令牌续采（外层循环）
        log(`${label} 第${pageNo + 1}页（站 ${site.replica}）：响应丢失（翻页令牌已消费），重新申请令牌续采`);
        break;
      }
      if (page.status === 409 && page.body?.error === 'STALE_SIGN_TOKEN') {
        db.prepare(`UPDATE sources SET ${cols.token} = ? WHERE tape_id = ?`).run(page.body.signToken, tape);
        log(`${label} 第${pageNo + 1}页（站 ${site.replica}）：签名令牌已轮换，用新令牌重签`);
        continue;
      }
      if (page.status === 409 && page.body?.error === 'PAGE_TOKEN_CONSUMED') {
        log(`${label} 第${pageNo + 1}页（站 ${site.replica}）：翻页令牌已被消费，重新申请`);
        break;
      }
      if (page.status === 410) {
        return switchAwayFrom(db, sites, tape, rev.id, site.replica, log, label);
      }
      if (await handleCommonAndRelay(db, sites, tape, rev, site.replica, page, log)) return false;
      if (page.status !== 200) {
        log(`${label} 第${pageNo + 1}页（站 ${site.replica}）：取页失败（status=${page.status}）`);
        return false;
      }
      const recs = normalize(page.body);
      storePage(db, rev.id, tape, site.replica, pageNo, recs, log, label, page.body.layout);
      return true;
    }
  }
  return false;
}

/**
 * 当前站点对某修订返回 410（撤下）：
 * 1. 只把本站 meta 标为撤下；若另一站仍在架，修订保持可接力；
 * 2. 立即对另一站拉索引对账（补上此前未同步过的备站视图），并把 active 切过去继续采。
 * 返回 false 结束本轮（接力结果下一轮自然继续；另一站也撤下时修订转 abandoned）。
 */
async function switchAwayFrom(
  db: DatabaseSync,
  sites: CollectorOpts['sites'],
  tape: string,
  revId: number,
  failedReplica: string,
  log: Log,
  label: string,
): Promise<boolean> {
  withTx(db, () => {
    db.prepare('UPDATE replica_meta SET listed = 0 WHERE revision_id = ? AND replica = ?').run(revId, failedReplica);
  });
  const other = sites.find((s) => s.replica !== failedReplica);
  if (!other) {
    // 没有备站：放弃
    withTx(db, () => {
      db.prepare("UPDATE revisions SET status = 'abandoned', listed = 0, updated_at = ? WHERE id = ?").run(Date.now(), revId);
    });
    log(`${label} 无其他站点可接力（410），放弃未采齐部分`);
    return false;
  }
  const cols = COLS[other.replica];
  const row = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;
  if (row[cols.paused] && Date.now() < row[cols.paused]) {
    log(`${label} 站点 ${failedReplica} 已撤下；另一站 ${other.replica} 暂停中，恢复后接力`);
    return false;
  }
  db.prepare('UPDATE sources SET active_replica = ? WHERE tape_id = ?').run(other.replica, tape);
  log(`${label} 站点 ${failedReplica} 已撤下（410），接力到${other.replica === 'B' ? '备' : '主'}站 ${other.replica}`);

  // 立即建立备站会话并对账，使备站分页/哈希视图落库，随后直接续采
  if (!row[cols.session]) {
    const r = await httpReq('POST', `${other.url}/api/sources/${tape}/session`);
    if (r.status === 200) {
      db.prepare(`UPDATE sources SET ${cols.session} = ?, ${cols.token} = ? WHERE tape_id = ?`).run(
        r.body.sessionId, r.body.signToken, tape,
      );
      log(`${tape}: 与站点 ${other.replica} 建立会话 ${String(r.body.sessionId).slice(0, 8)}…`);
    } else if (await handleCommon(db, tape, other.replica, r, log)) {
      return false;
    }
  }
  const cur = db.prepare('SELECT * FROM sources WHERE tape_id = ?').get(tape) as any;
  const idx = await httpReq('GET', `${other.url}/api/sources/${tape}/index?sessionId=${cur[cols.session]}`);
  if (await handleCommon(db, tape, other.replica, idx, log)) return false;
  if (idx.status !== 200) {
    log(`${tape}: 接力站点 ${other.replica} 索引失败（status=${idx.status}），下轮重试`);
    return false;
  }
  if (idx.body?.signToken) db.prepare(`UPDATE sources SET ${cols.token} = ? WHERE tape_id = ?`).run(idx.body.signToken, tape);
  reconcile(db, tape, other.replica, idx.body, log);

  // 对账后备站若也没有该修订，则正式放弃；否则在备站上续采一页
  const stillListed = (db
    .prepare('SELECT COUNT(*) AS n FROM replica_meta WHERE revision_id = ? AND listed = 1 AND conflict = 0')
    .get(revId) as any).n > 0;
  if (!stillListed) {
    withTx(db, () => {
      db.prepare("UPDATE revisions SET status = 'abandoned', listed = 0, updated_at = ? WHERE id = ?").run(Date.now(), revId);
    });
    log(`${label} 接力站点也无此修订，放弃未采齐部分`);
    return false;
  }
  const rev = db.prepare('SELECT * FROM revisions WHERE id = ?').get(revId) as any;
  return fetchPage(db, sites, other, tape, rev, log);
}

function storePage(
  db: DatabaseSync,
  revId: number,
  tape: string,
  replica: string,
  pageNo: number,
  recs: Rec[],
  log: Log,
  label: string,
  layout: string,
): void {
  withTx(db, () => {
    // 页断点按站独立幂等
    const ins = db
      .prepare('INSERT OR IGNORE INTO replica_pages (revision_id, replica, page_no, received_at) VALUES (?,?,?,?)')
      .run(revId, replica, pageNo, Date.now());
    if (Number(ins.changes) === 0) return;

    const rev = db.prepare('SELECT * FROM revisions WHERE id = ?').get(revId) as any;
    let newRecords = 0;
    for (const rec of recs) {
      const hash = canonicalHash(rec);
      const dup = db
        .prepare('INSERT OR IGNORE INTO rev_records (revision_id, record_hash) VALUES (?,?)')
        .run(revId, hash);
      if (Number(dup.changes) === 0) continue; // 另一站已采过同一记录（重叠页），幂等跳过
      newRecords += 1;
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
    const recordsDone = rev.records_done + newRecords;
    const pageCountNow = Math.max(
      rev.pages_done + 1,
      (db.prepare('SELECT COUNT(*) AS n FROM replica_pages WHERE revision_id = ?').get(revId) as any).n,
    );
    const complete = recordsDone >= rev.record_count && rev.record_count > 0;
    db.prepare('UPDATE revisions SET pages_done = ?, records_done = ?, status = ?, updated_at = ? WHERE id = ?')
      .run(pageCountNow, recordsDone, complete ? 'complete' : 'collecting', Date.now(), revId);
    if (complete) {
      // 同一事务内切换生效指针：查询侧原子看到「播放位置 + 缺失区段」的整体更新
      const a = recomputeActivation(db, tape);
      log(
        `${label} 经站点 ${replica} 第${pageNo + 1}页采齐（${recordsDone}/${rev.record_count} 条，本页新增 ${newRecords} 条）` +
          (a.changed ? `；生效版本切换 → 时间轴 v${a.timelineVersion}（节目单 rev${a.programRev} + 修复清单 rev${a.restorationRev}）` : ''),
      );
    } else {
      log(`${label} 站 ${replica} 第${pageNo + 1} 页入库（${layout} 排版，新增 ${newRecords} 条，累计 ${recordsDone}/${rev.record_count}）`);
    }
  });
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
