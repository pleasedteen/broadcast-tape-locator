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
 * - /control/* 为演示控制接口（发布修订、暂停、撤下、丢响应、查看站点日志）。
 */
import http from 'node:http';
import { randomToken, sha256 } from './util.ts';
import type { ManifestSeed, Rec } from './types.ts';

const PORT = Number(process.env.PORT ?? 4181);

interface PageTokenRec {
  id: string;
  key: string;
  page: number;
  consumed: boolean;
  contentHash: string; // 签发令牌时清单的内容指纹；清单被同号更正后旧令牌取页会被拒
}
interface Session {
  id: string;
  epoch: number;
  tokens: Map<string, PageTokenRec>;
}
interface TimelineDef {
  durationMs: number;
  edition: number; // 同一版次号下时长每次变更 +1；时长不变则 edition 不变
}
interface Manifest extends ManifestSeed {
  listed: boolean;
  withdrawAfterPage: number | null;
  losePages: Map<number, number>; // page -> 剩余丢响应次数
  contentHash: string; // 清单内容指纹（含 pageSize 与全部记录），同号更正时变化
  timelineEdition: number; // 发布时所依据的时间轴定义 edition
  replaceAfterPage: number | null; // 演示用：发出该页后自动以 replacement 同号更正
  replacement: ManifestSeed | null;
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
  timelines: Map<number, TimelineDef>; // 时间轴版次号 -> 当前定义（时长 + edition）
  manifests: Map<string, Manifest>;
  log: LogEntry[];
}

const state = {
  signToken: randomToken(),
  sources: new Map<string, Source>(),
};

const keyOf = (kind: string, v: number, r: number) => `${kind}:${v}:${r}`;
const pageCountOf = (m: ManifestSeed) => Math.max(1, Math.ceil(m.records.length / m.pageSize));

/**
 * 清单内容指纹：记录内容或 pageSize 任意变化都会改变（含「总页数未变」的同号更正）。
 * 原样重发得到相同指纹——采集器据此复用已采页、只续采缺页。
 */
function contentHashOf(seed: ManifestSeed): string {
  return sha256(`${seed.pageSize}|${JSON.stringify(seed.records)}`);
}

function slog(src: Source | null, route: string, detail: string): void {
  if (src) {
    src.log.push({ t: Date.now(), route, detail });
    if (src.log.length > 500) src.log.shift();
  }
  console.log(`[site] ${src?.tapeId ?? '-'} ${route} ${detail}`);
}

function addSource(tapeId: string, timelines: Array<[number, number]>): Source {
  const src: Source = {
    tapeId,
    pausedUntil: 0,
    epoch: 1,
    sessions: new Map(),
    timelines: new Map(timelines.map(([version, durationMs]) => [version, { durationMs, edition: 1 }])),
    manifests: new Map(),
    log: [],
  };
  state.sources.set(tapeId, src);
  return src;
}

/** 发布（或同号更正）一份清单；同号更正时旧清单被新内容指纹的清单替换 */
function addManifest(tapeId: string, seed: ManifestSeed): Manifest {
  const src = state.sources.get(tapeId);
  if (!src) throw new Error(`未知母带 ${tapeId}`);
  const tl = src.timelines.get(seed.timelineVersion);
  if (!tl) throw new Error(`未知时间轴版次 v${seed.timelineVersion}`);
  const existed = src.manifests.get(keyOf(seed.kind, seed.timelineVersion, seed.revision));
  const mf: Manifest = {
    ...seed,
    listed: true,
    withdrawAfterPage: existed?.withdrawAfterPage ?? null,
    losePages: new Map(),
    replaceAfterPage: null,
    replacement: null,
    contentHash: contentHashOf(seed),
    timelineEdition: tl.edition,
  };
  src.manifests.set(keyOf(seed.kind, seed.timelineVersion, seed.revision), mf);
  return mf;
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
function renderPage(m: Manifest, page: number, layout: 'table' | 'cards'): any {
  const recs = m.records.slice(page * m.pageSize, (page + 1) * m.pageSize);
  if (layout === 'table') return { layout, pageNo: page, pageCount: pageCountOf(m), rows: recs.map(recToRow) };
  return { layout, page: { index: page, count: pageCountOf(m) }, items: recs.map(recToCard) };
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
      timelines: [...src.timelines.entries()].map(([version, def]) => ({
        version,
        durationMs: def.durationMs,
        edition: def.edition,
      })),
      manifests: [...src.manifests.values()]
        .filter((x) => x.listed)
        .map((x) => ({
          kind: x.kind,
          timelineVersion: x.timelineVersion,
          timelineEdition: x.timelineEdition,
          revision: x.revision,
          pageCount: pageCountOf(x),
          recordCount: x.records.length,
          contentHash: x.contentHash,
        })),
    });
    return;
  }

  if (route === 'page-tokens' && req.method === 'POST') {
    const key = keyOf(body.kind, body.timelineVersion, body.revision);
    const mf = src.manifests.get(key);
    if (!mf || !mf.listed) {
      slog(src, 'page-token', `${key} 已撤下 → 410`);
      send(res, 410, { error: 'WITHDRAWN' });
      return;
    }
    if (typeof body.page !== 'number' || body.page < 0 || body.page >= pageCountOf(mf)) {
      send(res, 416, { error: 'NO_SUCH_PAGE' });
      return;
    }
    // 令牌绑定申请时的内容指纹：清单若在翻页途中被同号更正，旧令牌取页会被识别并拒绝
    const wantHash = typeof body.contentHash === 'string' ? body.contentHash : '';
    if (wantHash !== mf.contentHash) {
      slog(src, 'page-token', `${key} 内容指纹已变化（同号更正）→ 410 SUPERSEDED`);
      send(res, 410, { error: 'SUPERSEDED', contentHash: mf.contentHash });
      return;
    }
    const tk: PageTokenRec = { id: randomToken(), key, page: body.page, consumed: false, contentHash: mf.contentHash };
    sess.tokens.set(tk.id, tk);
    slog(src, 'page-token', `${key} 第${body.page + 1}页 签发翻页令牌 ${tk.id.slice(0, 8)}…`);
    send(res, 200, { pageToken: tk.id, signToken: state.signToken, contentHash: mf.contentHash });
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
      slog(src, 'page', `${tk.key} 第${tk.page + 1}页 翻页令牌已消费 → 409`);
      send(res, 409, { error: 'PAGE_TOKEN_CONSUMED' });
      return;
    }
    const mf = src.manifests.get(tk.key);
    if (!mf || !mf.listed) {
      slog(src, 'page', `${tk.key} 已撤下 → 410`);
      send(res, 410, { error: 'WITHDRAWN' });
      return;
    }
    if (tk.contentHash !== mf.contentHash) {
      // 翻页途中清单已被同号更正：拒绝旧指纹的令牌，新清单的页绝不混入旧修订
      slog(src, 'page', `${tk.key} 第${tk.page + 1}页 清单已同号更正，旧页令牌作废 → 410 SUPERSEDED`);
      send(res, 410, { error: 'SUPERSEDED', contentHash: mf.contentHash });
      return;
    }
    tk.consumed = true; // 令牌先消费——此后即使响应丢失也不补发同一令牌
    const loses = mf.losePages.get(tk.page) ?? 0;
    if (loses > 0) {
      mf.losePages.set(tk.page, loses - 1);
      slog(src, 'page', `${tk.key} 第${tk.page + 1}页 响应丢失（翻页令牌已消费）`);
      req.socket.destroy();
      return;
    }
    const layout: 'table' | 'cards' = tk.page % 2 === 0 ? 'table' : 'cards';
    slog(src, 'page', `${tk.key} 第${tk.page + 1}页 发出（${layout} 排版）`);
    if (mf.withdrawAfterPage === tk.page) {
      mf.listed = false;
      slog(src, 'withdraw', `${tk.key} 发出第${tk.page + 1}页后撤下`);
    }
    const pageBody = renderPage(mf, tk.page, layout);
    // 演示用：该页发出后立即以同号更正替换清单（模拟「翻页中收到更正」）。
    // 注意：本页内容取自替换前的 mf，页体回带的必须是本页内容的指纹（即 mf.contentHash）。
    if (mf.replaceAfterPage === tk.page && mf.replacement) {
      const seed = mf.replacement;
      mf.replaceAfterPage = null;
      mf.replacement = null;
      const next = addManifest(src.tapeId, seed);
      slog(src, 'correct', `${tk.key} 发出第${tk.page + 1}页后同号更正（内容指纹 ${mf.contentHash.slice(0, 8)}… → ${next.contentHash.slice(0, 8)}…）`);
    }
    send(res, 200, { ...pageBody, contentHash: mf.contentHash, timelineEdition: mf.timelineEdition });
    return;
  }

  send(res, 405, { error: 'METHOD_NOT_ALLOWED' });
}

// ---------- 演示控制接口 ----------
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
        manifests: [...s.manifests.entries()].map(([key, x]) => ({
          key, listed: x.listed, pageCount: pageCountOf(x), recordCount: x.records.length,
          contentHash: x.contentHash, timelineEdition: x.timelineEdition,
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
    const mf = src?.manifests.get(keyOf(body.kind, body.timelineVersion, body.revision));
    if (!src || !mf) return send(res, 404, { error: 'UNKNOWN_MANIFEST' });
    mf.listed = false;
    slog(src, 'withdraw', `${body.kind}:${body.timelineVersion}:${body.revision} 已撤下`);
    send(res, 200, { ok: true });
    return;
  }
  if (path === '/control/withdraw-after' && req.method === 'POST') {
    const src = state.sources.get(body.tape);
    const mf = src?.manifests.get(keyOf(body.kind, body.timelineVersion, body.revision));
    if (!src || !mf) return send(res, 404, { error: 'UNKNOWN_MANIFEST' });
    mf.withdrawAfterPage = Number(body.page);
    send(res, 200, { ok: true });
    return;
  }
  if (path === '/control/lose-response' && req.method === 'POST') {
    const src = state.sources.get(body.tape);
    const mf = src?.manifests.get(keyOf(body.kind, body.timelineVersion, body.revision));
    if (!src || !mf) return send(res, 404, { error: 'UNKNOWN_MANIFEST' });
    mf.losePages.set(Number(body.page), Number(body.times ?? 1));
    send(res, 200, { ok: true });
    return;
  }
  if (path === '/control/publish' && req.method === 'POST') {
    const src = state.sources.get(body.tape);
    if (!src) return send(res, 404, { error: 'UNKNOWN_TAPE' });
    if (body.timeline) {
      const v = Number(body.timeline.version);
      const dur = Number(body.timeline.durationMs);
      const cur = src.timelines.get(v);
      if (!cur) {
        src.timelines.set(v, { durationMs: dur, edition: 1 });
      } else if (cur.durationMs !== dur) {
        // 时长变更：同版次号下定义 edition +1，旧定义在库中仍可被旧清单引用
        src.timelines.set(v, { durationMs: dur, edition: cur.edition + 1 });
        slog(src, 'timeline', `时间轴 v${v} 时长变更 → edition ${cur.edition + 1}（${cur.durationMs}ms → ${dur}ms）`);
      }
    }
    const mf = body.manifest as ManifestSeed;
    const key = keyOf(mf.kind, mf.timelineVersion, mf.revision);
    const old = src.manifests.get(key);
    const stored = addManifest(body.tape, mf);
    if (old && old.contentHash !== stored.contentHash) {
      slog(src, 'correct', `${key} 同号更正（内容指纹 ${old.contentHash.slice(0, 8)}… → ${stored.contentHash.slice(0, 8)}…，页数 ${pageCountOf(old)} → ${pageCountOf(stored)}）`);
    } else {
      slog(src, 'publish', `${key} 已发布（${mf.records.length} 条记录，内容指纹 ${stored.contentHash.slice(0, 8)}…）`);
    }
    send(res, 200, { ok: true, contentHash: stored.contentHash, timelineEdition: stored.timelineEdition, pageCount: pageCountOf(stored) });
    return;
  }
  if (path === '/control/correct-after' && req.method === 'POST') {
    // 演示控制：目标清单发出指定页后，立即以 replacement 做同号更正（复现「翻页中收到更正」）
    const src = state.sources.get(body.tape);
    const mf = src?.manifests.get(keyOf(body.kind, body.timelineVersion, body.revision));
    if (!src || !mf) return send(res, 404, { error: 'UNKNOWN_MANIFEST' });
    mf.replaceAfterPage = Number(body.page);
    mf.replacement = body.manifest as ManifestSeed;
    send(res, 200, { ok: true });
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
