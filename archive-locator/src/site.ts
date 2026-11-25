/**
 * 本地模拟站点（不依赖真实外站）。
 *
 * 模拟的站点行为：
 * - 页面有两种排版：table（扁平行）与 cards（嵌套卡片），按页交替。
 * - 翻页签名 = sha256(签名令牌 | 会话 | 翻页令牌)，随站点刚发出的签名令牌和当前会话变化；
 *   签名令牌在每次新会话签发时轮换，过期签名返回 409 并附新令牌。
 * - 翻页令牌一次性：每次取页先申请；响应可能丢失而令牌已消费（站点直接断开连接），
 *   客户端需重新申请令牌续采。
 * - 站点可要求暂停某一来源：暂停期间该来源一切请求返回 423；恢复后旧会话作废，发放新会话。
 * - 修订可被撤下：撤下后从索引消失，取页返回 410。
 * - 同一修订号可多次发布：更正或原样重发都分配递增 edition（版次号）。
 *   旧 edition 立刻从索引消失，针对旧 edition 的取页令牌申请返回 409 EDITION_SUPERSEDED，
 *   索引与每页响应附带内容哈希 pageHash，供采集器判断哪些页未改、可直接复用。
 * - /control/* 为演示控制接口（发布修订、暂停、撤下、丢响应、查看站点日志）。
 */
import http from 'node:http';
import { randomToken, sha256 } from './util.ts';
import type { ManifestSeed, Rec } from './types.ts';

const PORT = Number(process.env.PORT ?? 4181);

interface PageTokenRec {
  id: string;
  key: string;
  edition: number;
  page: number;
  consumed: boolean;
}
interface Session {
  id: string;
  epoch: number;
  tokens: Map<string, PageTokenRec>;
}
interface ManifestVersion extends ManifestSeed {
  edition: number;
  listed: boolean;
  withdrawAfterPage: number | null;
  losePages: Map<number, number>; // page -> 剩余丢响应次数
}
interface ManifestSeries {
  key: string;
  kind: string;
  timelineVersion: number;
  revision: number;
  versions: Map<number, ManifestVersion>;
  currentEdition: number;
}
interface LogEntry {
  t: number;
  route: string;
  detail: string;
}
interface Source {
  tapeId: string;
  pausedUntil: number;
  epoch: number; // 每次暂停 +1，使旧会话失效
  sessions: Map<string, Session>;
  timelines: Map<number, number>; // 时间轴版次 -> 当前时长 ms
  manifests: Map<string, ManifestSeries>;
  log: LogEntry[];
}

const state = {
  signToken: randomToken(),
  sources: new Map<string, Source>(),
};

const keyOf = (kind: string, v: number, r: number) => `${kind}:${v}:${r}`;
const pageCountOf = (m: ManifestSeed) => Math.max(1, Math.ceil(m.records.length / m.pageSize));
/** 页内容哈希：同一修订号重发时，未改页哈希不变，采集器据此复用本地已采页 */
const pageHashOf = (m: ManifestSeed, page: number) =>
  sha256(JSON.stringify(m.records.slice(page * m.pageSize, (page + 1) * m.pageSize)));

function slog(src: Source | null, route: string, detail: string): void {
  if (src) {
    src.log.push({ t: Date.now(), route, detail });
    if (src.log.length > 1000) src.log.shift();
  }
  console.log(`[site] ${src?.tapeId ?? '-'} ${route} ${detail}`);
}

function addSource(tapeId: string, timelines: Array<[number, number]>): Source {
  const src: Source = {
    tapeId,
    pausedUntil: 0,
    epoch: 1,
    sessions: new Map(),
    timelines: new Map(timelines),
    manifests: new Map(),
    log: [],
  };
  state.sources.set(tapeId, src);
  return src;
}

/** 发布修订：同 key 再次发布即「更正/原样重发」，分配新 edition，旧 edition 留档但下架 */
function addManifest(tapeId: string, seed: ManifestSeed): { series: ManifestSeries; version: ManifestVersion; isCorrection: boolean } {
  const src = state.sources.get(tapeId);
  if (!src) throw new Error(`未知母带 ${tapeId}`);
  const key = keyOf(seed.kind, seed.timelineVersion, seed.revision);
  let series = src.manifests.get(key);
  let isCorrection = false;
  if (!series) {
    series = { key, kind: seed.kind, timelineVersion: seed.timelineVersion, revision: seed.revision, versions: new Map(), currentEdition: 0 };
    src.manifests.set(key, series);
  } else {
    isCorrection = true;
    // 旧 edition 立即从索引下架（针对它的令牌申请由 409 路径处理）
    for (const old of series.versions.values()) old.listed = false;
  }
  const edition = series.currentEdition + 1;
  const version: ManifestVersion = {
    ...seed,
    edition,
    listed: true,
    withdrawAfterPage: null,
    losePages: new Map(),
  };
  series.versions.set(edition, version);
  series.currentEdition = edition;
  return { series, version, isCorrection };
}

// ---------- 初始目录数据（演示场景） ----------
function seed(): void {
  // T1：60 分钟母带，时间轴 v1
  addSource('T1', [[1, 3_600_000]]);
  addManifest('T1', {
    kind: 'program', timelineVersion: 1, revision: 1, pageSize: 2,
    records: [
      { type: 'program', programId: 'P1', title: '晨间新闻', startMs: 0, endMs: 900_000 },
      { type: 'program', programId: 'P2', title: '戏曲选段', startMs: 900_000, endMs: 2_700_000 },
      { type: 'program', programId: 'P3', title: '旧广告联播', startMs: 2_700_000, endMs: 3_600_000 },
    ],
  });
  addManifest('T1', {
    kind: 'restoration', timelineVersion: 1, revision: 1, pageSize: 2,
    records: [
      { type: 'segment', copyId: 'C1', masterStartMs: 0, masterEndMs: 1_200_000, copyStartMs: 0, rate: 1 },
      { type: 'damage', copyId: 'C1', masterStartMs: 1_200_000, masterEndMs: 1_500_000, note: '磁带霉变，信号丢失' },
      { type: 'segment', copyId: 'C1', masterStartMs: 1_500_000, masterEndMs: 3_600_000, copyStartMs: 1_200_000, rate: 1 },
      { type: 'segment', copyId: 'C2', masterStartMs: 0, masterEndMs: 1_800_000, copyStartMs: 0, rate: 1 },
      { type: 'segment', copyId: 'C2', masterStartMs: 1_800_000, masterEndMs: 2_400_000, copyStartMs: 1_800_000, rate: 1.5 },
      { type: 'segment', copyId: 'C2', masterStartMs: 2_400_000, masterEndMs: 3_600_000, copyStartMs: 2_200_000, rate: 1 },
    ],
  });
  // T2：30 分钟母带
  addSource('T2', [[1, 1_800_000]]);
  addManifest('T2', {
    kind: 'program', timelineVersion: 1, revision: 1, pageSize: 2,
    records: [
      { type: 'program', programId: 'Q1', title: '午间路况', startMs: 0, endMs: 600_000 },
      { type: 'program', programId: 'Q2', title: '音乐点播', startMs: 600_000, endMs: 1_800_000 },
    ],
  });
  addManifest('T2', {
    kind: 'restoration', timelineVersion: 1, revision: 1, pageSize: 2,
    records: [
      { type: 'segment', copyId: 'C1', masterStartMs: 0, masterEndMs: 1_800_000, copyStartMs: 0, rate: 1 },
    ],
  });
  // T3：15 分钟母带（其修复清单 rev1 将在演示中被中途撤下，制造"采不齐"）
  addSource('T3', [[1, 900_000]]);
  addManifest('T3', {
    kind: 'program', timelineVersion: 1, revision: 1, pageSize: 2,
    records: [
      { type: 'program', programId: 'R1', title: '台标音乐', startMs: 0, endMs: 300_000 },
      { type: 'program', programId: 'R2', title: '晚间剧场', startMs: 300_000, endMs: 900_000 },
    ],
  });
  addManifest('T3', {
    kind: 'restoration', timelineVersion: 1, revision: 1, pageSize: 2,
    records: [
      { type: 'segment', copyId: 'C1', masterStartMs: 0, masterEndMs: 100_000, copyStartMs: 0, rate: 1 },
      { type: 'damage', copyId: 'C1', masterStartMs: 100_000, masterEndMs: 120_000, note: '片头划伤' },
      { type: 'segment', copyId: 'C1', masterStartMs: 120_000, masterEndMs: 450_000, copyStartMs: 100_000, rate: 1 },
      { type: 'segment', copyId: 'C1', masterStartMs: 450_000, masterEndMs: 900_000, copyStartMs: 430_000, rate: 1 },
    ],
  });
  // T4：60 分钟母带，专门演示「同一修订号更正/原样重发」与「同版次时长调整」。
  // 初始清单页数较多（节目单 5 页），更正只改其中一页，便于观察未改页复用。
  addSource('T4', [[1, 3_600_000]]);
  addManifest('T4', {
    kind: 'program', timelineVersion: 1, revision: 1, pageSize: 2,
    records: [
      { type: 'program', programId: 'S1', title: '开场曲', startMs: 0, endMs: 600_000 },
      { type: 'program', programId: 'S2', title: '新闻速递', startMs: 600_000, endMs: 1_200_000 },
      { type: 'program', programId: 'S3', title: '人物访谈', startMs: 1_200_000, endMs: 1_800_000 },
      { type: 'program', programId: 'S4', title: '广播剧场', startMs: 1_800_000, endMs: 2_400_000 },
      { type: 'program', programId: 'S5', title: '音乐之旅', startMs: 2_400_000, endMs: 3_000_000 },
      { type: 'program', programId: 'S6', title: '财经观察', startMs: 3_000_000, endMs: 3_300_000 },
      { type: 'program', programId: 'S7', title: '午夜心语', startMs: 3_300_000, endMs: 3_600_000 },
    ],
  });
  addManifest('T4', {
    kind: 'restoration', timelineVersion: 1, revision: 1, pageSize: 2,
    records: [
      { type: 'segment', copyId: 'C1', masterStartMs: 0, masterEndMs: 2_000_000, copyStartMs: 0, rate: 1 },
      { type: 'damage', copyId: 'C1', masterStartMs: 2_000_000, masterEndMs: 2_200_000, note: '带基粘连' },
      { type: 'segment', copyId: 'C1', masterStartMs: 2_200_000, masterEndMs: 3_600_000, copyStartMs: 2_000_000, rate: 1 },
    ],
  });
}

// ---------- 两种排版 ----------
function recToRow(rec: Rec): any {
  if (rec.type === 'program') return { rec: 'program', id: rec.programId, title: rec.title, from: rec.startMs, to: rec.endMs };
  if (rec.type === 'segment') return { rec: 'segment', copy: rec.copyId, mFrom: rec.masterStartMs, mTo: rec.masterEndMs, cFrom: rec.copyStartMs, rate: rec.rate };
  return { rec: 'damage', copy: rec.copyId, mFrom: rec.masterStartMs, mTo: rec.masterEndMs, note: rec.note };
}
function recToCard(rec: Rec): any {
  if (rec.type === 'program') return { kind: 'program', program: { id: rec.programId, title: rec.title, span: { startMs: rec.startMs, endMs: rec.endMs } } };
  if (rec.type === 'segment') return { kind: 'segment', segment: { copy: rec.copyId, master: { startMs: rec.masterStartMs, endMs: rec.masterEndMs }, copyStartMs: rec.copyStartMs, rate: rec.rate } };
  return { kind: 'damage', damage: { copy: rec.copyId, master: { startMs: rec.masterStartMs, endMs: rec.masterEndMs }, note: rec.note } };
}
function renderPage(m: ManifestVersion, page: number, layout: 'table' | 'cards'): any {
  const recs = m.records.slice(page * m.pageSize, (page + 1) * m.pageSize);
  const base: any = { edition: m.edition, pageHash: pageHashOf(m, page) };
  if (layout === 'table') return { ...base, layout, pageNo: page, pageCount: pageCountOf(m), rows: recs.map(recToRow) };
  return { ...base, layout, page: { index: page, count: pageCountOf(m) }, items: recs.map(recToCard) };
}

// ---------- HTTP 基础设施 ----------
function send(res: http.ServerResponse, status: number, obj: any): void {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  res.end(body);
}
function readJson(req: http.IncomingMessage): Promise<any> {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (c) => {
      data += c;
      if (data.length > 5_000_000) req.destroy();
    });
    req.on('end', () => {
      try { resolve(data ? JSON.parse(data) : {}); } catch (e) { reject(e); }
    });
    req.on('error', reject);
  });
}

// ---------- 业务路由 ----------
function guardSource(res: http.ServerResponse, tape: string): Source | null {
  const src = state.sources.get(tape) ?? null;
  if (!src) {
    send(res, 404, { error: 'UNKNOWN_TAPE' });
    return null;
  }
  if (Date.now() < src.pausedUntil) {
    slog(src, 'blocked', `来源暂停中 → 423（至 ${new Date(src.pausedUntil).toISOString()}）`);
    send(res, 423, { error: 'PAUSED', pausedUntil: src.pausedUntil });
    return null;
  }
  return src;
}
function guardSession(res: http.ServerResponse, src: Source, sessionId: string): Session | null {
  const sess = src.sessions.get(sessionId);
  if (!sess || sess.epoch !== src.epoch) {
    slog(src, 'auth', `会话无效 → 401（${sessionId ?? '无'}）`);
    send(res, 401, { error: 'SESSION_INVALID' });
    return null;
  }
  return sess;
}

async function handleApi(req: http.IncomingMessage, res: http.ServerResponse, url: URL): Promise<void> {
  if (url.pathname === '/api/sources' && req.method === 'GET') {
    send(res, 200, { tapes: [...state.sources.keys()] });
    return;
  }
  const m = url.pathname.match(/^\/api\/sources\/([^/]+)\/(session|index|page-tokens|page)$/);
  if (!m) {
    send(res, 404, { error: 'NOT_FOUND' });
    return;
  }
  const [, tape, route] = m;
  const src = guardSource(res, tape);
  if (!src) return;

  if (route === 'session' && req.method === 'POST') {
    // 站点发出新令牌：签名令牌随每次新会话轮换
    state.signToken = randomToken();
    const sess: Session = { id: randomToken(), epoch: src.epoch, tokens: new Map() };
    src.sessions.set(sess.id, sess);
    slog(src, 'session', `发放新会话 ${sess.id}（签名令牌轮换为 ${state.signToken.slice(0, 8)}…）`);
    send(res, 200, { sessionId: sess.id, signToken: state.signToken });
    return;
  }

  const body = req.method === 'POST' ? await readJson(req) : null;
  const sessionId = url.searchParams.get('sessionId') ?? body?.sessionId ?? '';
  const sess = guardSession(res, src, sessionId);
  if (!sess) return;

  if (route === 'index' && req.method === 'GET') {
    slog(src, 'index', '拉取索引');
    send(res, 200, {
      signToken: state.signToken,
      timelines: [...src.timelines.entries()].map(([version, durationMs]) => ({ version, durationMs })),
      manifests: [...src.manifests.values()]
        .map((series) => series.versions.get(series.currentEdition))
        .filter((x): x is ManifestVersion => !!x && x.listed)
        .map((x) => ({
          kind: x.kind,
          timelineVersion: x.timelineVersion,
          revision: x.revision,
          edition: x.edition,
          pageCount: pageCountOf(x),
          recordCount: x.records.length,
          durationMs: src.timelines.get(x.timelineVersion) ?? null,
          pageHashes: Array.from({ length: pageCountOf(x) }, (_, p) => pageHashOf(x, p)),
        })),
    });
    return;
  }

  if (route === 'page-tokens' && req.method === 'POST') {
    const key = keyOf(body.kind, body.timelineVersion, body.revision);
    const series = src.manifests.get(key);
    if (!series || !series.versions.get(series.currentEdition)?.listed) {
      slog(src, 'page-token', `${key} 已撤下 → 410`);
      send(res, 410, { error: 'WITHDRAWN' });
      return;
    }
    const curEdition = series.currentEdition;
    if (typeof body.edition === 'number' && body.edition !== curEdition) {
      // 采集器持有的是更正前的旧 edition：旧版页不再提供，引导其对账新 edition
      slog(src, 'page-token', `${key} edition ${body.edition} 已被 ${curEdition} 取代 → 409`);
      send(res, 409, { error: 'EDITION_SUPERSEDED', currentEdition: curEdition });
      return;
    }
    const mf = series.versions.get(curEdition)!;
    if (typeof body.page !== 'number' || body.page < 0 || body.page >= pageCountOf(mf)) {
      send(res, 416, { error: 'NO_SUCH_PAGE' });
      return;
    }
    const tk: PageTokenRec = { id: randomToken(), key, edition: curEdition, page: body.page, consumed: false };
    sess.tokens.set(tk.id, tk);
    slog(src, 'page-token', `${key} ed${curEdition} 第${body.page + 1}页 签发翻页令牌 ${tk.id.slice(0, 8)}…`);
    send(res, 200, { pageToken: tk.id, signToken: state.signToken, edition: curEdition });
    return;
  }

  if (route === 'page' && req.method === 'GET') {
    const pageToken = url.searchParams.get('pageToken') ?? '';
    const sig = url.searchParams.get('sig') ?? '';
    // 翻页签名随站点刚发出的签名令牌和当前会话变化
    const expect = sha256(`${state.signToken}|${sessionId}|${pageToken}`);
    if (sig !== expect) {
      slog(src, 'page', '签名令牌不是最新 → 409（附新令牌）');
      send(res, 409, { error: 'STALE_SIGN_TOKEN', signToken: state.signToken });
      return;
    }
    const tk = sess.tokens.get(pageToken);
    if (!tk) {
      send(res, 404, { error: 'UNKNOWN_PAGE_TOKEN' });
      return;
    }
    if (tk.consumed) {
      slog(src, 'page', `${tk.key} ed${tk.edition} 第${tk.page + 1}页 翻页令牌已消费 → 409`);
      send(res, 409, { error: 'PAGE_TOKEN_CONSUMED' });
      return;
    }
    const series = src.manifests.get(tk.key);
    if (!series || !series.versions.get(series.currentEdition)?.listed) {
      slog(src, 'page', `${tk.key} 已撤下 → 410`);
      send(res, 410, { error: 'WITHDRAWN' });
      return;
    }
    if (tk.edition !== series.currentEdition) {
      slog(src, 'page', `${tk.key} ed${tk.edition} 已被 ed${series.currentEdition} 取代 → 409`);
      send(res, 409, { error: 'EDITION_SUPERSEDED', currentEdition: series.currentEdition });
      return;
    }
    const mf = series.versions.get(tk.edition)!;
    tk.consumed = true; // 令牌先消费——此后即使响应丢失也不补发同一令牌
    const loses = mf.losePages.get(tk.page) ?? 0;
    if (loses > 0) {
      mf.losePages.set(tk.page, loses - 1);
      slog(src, 'page', `${tk.key} ed${tk.edition} 第${tk.page + 1}页 响应丢失（翻页令牌已消费）`);
      req.socket.destroy();
      return;
    }
    const layout: 'table' | 'cards' = tk.page % 2 === 0 ? 'table' : 'cards';
    slog(src, 'page', `${tk.key} ed${tk.edition} 第${tk.page + 1}页 发出（${layout} 排版，hash ${mf ? pageHashOf(mf, tk.page).slice(0, 8) : ''}…）`);
    if (mf.withdrawAfterPage === tk.page) {
      mf.listed = false;
      slog(src, 'withdraw', `${tk.key} ed${tk.edition} 发出第${tk.page + 1}页后撤下`);
    }
    send(res, 200, renderPage(mf, tk.page, layout));
    return;
  }

  send(res, 405, { error: 'METHOD_NOT_ALLOWED' });
}

// ---------- 演示控制接口 ----------
function findVersion(src: Source | undefined, kind: string, v: number, r: number): { series: ManifestSeries; version: ManifestVersion } | null {
  const series = src?.manifests.get(keyOf(kind, v, r));
  const version = series?.versions.get(series.currentEdition);
  return series && version ? { series, version } : null;
}

async function handleControl(req: http.IncomingMessage, res: http.ServerResponse, url: URL): Promise<void> {
  const body = req.method === 'POST' ? await readJson(req) : {};
  const path = url.pathname;

  if (path === '/control/state' && req.method === 'GET') {
    const sources: any = {};
    for (const [id, s] of state.sources) {
      sources[id] = {
        pausedUntil: s.pausedUntil,
        epoch: s.epoch,
        sessions: [...s.sessions.keys()],
        manifests: [...s.manifests.values()].map((series) => ({
          key: series.key,
          currentEdition: series.currentEdition,
          versions: [...series.versions.values()].map((x) => ({
            edition: x.edition,
            listed: x.listed,
            pageCount: pageCountOf(x),
            recordCount: x.records.length,
          })),
        })),
        log: s.log,
      };
    }
    send(res, 200, { signToken: state.signToken, sources });
    return;
  }
  if (path === '/control/pause' && req.method === 'POST') {
    const src = state.sources.get(body.tape);
    if (!src) return send(res, 404, { error: 'UNKNOWN_TAPE' });
    src.pausedUntil = Date.now() + Number(body.durationMs ?? 5000);
    src.epoch += 1; // 旧会话全部作废，恢复时发放新会话
    slog(src, 'pause', `站点要求暂停该来源 ${body.durationMs}ms（至 ${new Date(src.pausedUntil).toISOString()}）`);
    send(res, 200, { pausedUntil: src.pausedUntil });
    return;
  }
  if (path === '/control/withdraw' && req.method === 'POST') {
    const src = state.sources.get(body.tape);
    const found = findVersion(src, body.kind, body.timelineVersion, body.revision);
    if (!found) return send(res, 404, { error: 'UNKNOWN_MANIFEST' });
    for (const v of found.series.versions.values()) v.listed = false;
    slog(src!, 'withdraw', `${body.kind}:${body.timelineVersion}:${body.revision} 全部 edition 已撤下`);
    send(res, 200, { ok: true });
    return;
  }
  if (path === '/control/withdraw-after' && req.method === 'POST') {
    const src = state.sources.get(body.tape);
    const found = findVersion(src, body.kind, body.timelineVersion, body.revision);
    if (!found) return send(res, 404, { error: 'UNKNOWN_MANIFEST' });
    found.version.withdrawAfterPage = Number(body.page);
    send(res, 200, { ok: true });
    return;
  }
  if (path === '/control/lose-response' && req.method === 'POST') {
    const src = state.sources.get(body.tape);
    const found = findVersion(src, body.kind, body.timelineVersion, body.revision);
    if (!found) return send(res, 404, { error: 'UNKNOWN_MANIFEST' });
    found.version.losePages.set(Number(body.page), Number(body.times ?? 1));
    send(res, 200, { ok: true, edition: found.version.edition });
    return;
  }
  if (path === '/control/publish' && req.method === 'POST') {
    const src = state.sources.get(body.tape);
    if (!src) return send(res, 404, { error: 'UNKNOWN_TAPE' });
    if (body.timeline) src.timelines.set(Number(body.timeline.version), Number(body.timeline.durationMs));
    const mf = body.manifest as ManifestSeed;
    const { series, version, isCorrection } = addManifest(body.tape, mf);
    slog(
      src,
      'publish',
      isCorrection
        ? `${mf.kind} v${mf.timelineVersion} rev${mf.revision} 更正发布 edition ${version.edition}（${mf.records.length} 条记录，旧 edition 下架）`
        : `${mf.kind} v${mf.timelineVersion} rev${mf.revision} edition ${version.edition} 已发布（${mf.records.length} 条记录）`,
    );
    send(res, 200, { ok: true, edition: version.edition, isCorrection, key: series.key });
    return;
  }
  if (path === '/control/rotate-token' && req.method === 'POST') {
    state.signToken = randomToken();
    send(res, 200, { signToken: state.signToken });
    return;
  }
  send(res, 404, { error: 'NOT_FOUND' });
}

// ---------- 启动 ----------
seed();
const server = http.createServer((req, res) => {
  const url = new URL(req.url ?? '/', 'http://localhost');
  const run = url.pathname.startsWith('/control/') ? handleControl(req, res, url) : handleApi(req, res, url);
  run.catch((e) => {
    try { send(res, 500, { error: String(e) }); } catch { /* ignore */ }
  });
});
server.listen(PORT, '127.0.0.1', () => {
  console.log(`[site] 模拟站点已启动: http://127.0.0.1:${PORT}（来源: ${[...state.sources.keys()].join(', ')}）`);
});
