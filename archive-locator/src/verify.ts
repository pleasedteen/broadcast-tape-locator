/**
 * 一条命令的端到端验证（目标 < 80 秒）：
 *   npm run verify
 *
 * 同一盘母带接入主备两个模拟站点（A=4181 主站 / B=4183 备站），两站会话与暂停各自独立。
 *
 * 场景覆盖：
 *   阶段 1  首次采集：翻页响应丢失（令牌已消费）后的恢复；T3 清单两站均采不齐不影响他盘
 *   阶段 2  主站暂停 → 备站接力：暂停期间对主站零请求，备站独立会话采齐新修订；主站暂停不影响备站
 *   阶段 3  恢复后新会话；同版次节目单新修订原子生效（要么全旧要么全新）
 *   阶段 4  新时间轴版次 v2 生效；被新版清单移除的记录随修订撤下
 *   阶段 5  采集器重启：已采齐修订复用不重采；重启窗口查询结果不变
 *   阶段 6  主备接力核心：主站采到一半撤下 + 采集器重启 + 两站分页不同，
 *           备站只请求含未采记录的页；备站独立暂停下完成接力
 *   阶段 7  冲突与查询切换：同号清单内容冲突、母带时长冲突均不改变定位结果；
 *           冲突站数据被拒收；查询接口全程可切换验证
 *   阶段 8  节目定位结果展示
 */
import { spawn, type ChildProcess } from 'node:child_process';
import { mkdirSync, rmSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { fmtMs, httpReq, sleep } from './util.ts';
import type { Rec } from './types.ts';

const ROOT = join(fileURLToPath(import.meta.url), '..', '..');
const SITE_A_PORT = 4181;
const SITE_B_PORT = 4183;
const API_PORT = 4182;
const SITE_A = `http://127.0.0.1:${SITE_A_PORT}`;
const SITE_B = `http://127.0.0.1:${SITE_B_PORT}`;
const SITES: Record<string, string> = { A: SITE_A, B: SITE_B };
const API = `http://127.0.0.1:${API_PORT}`;
const WORK = join(ROOT, '.verify');
const DB = join(WORK, 'archive.db');
const T0 = Date.now();
const HARD_DEADLINE = T0 + 75_000;

// ---------- 输出工具 ----------
let passed = 0;
let failed = 0;
function banner(title: string): void {
  console.log(`\n══ ${title} ${'═'.repeat(Math.max(2, 60 - title.length * 2))}`);
}
function note(text: string): void {
  console.log(`  ${text}`);
}
function check(name: string, cond: boolean, extra = ''): void {
  if (cond) {
    passed++;
    console.log(`  ✓ ${name}${extra ? ` — ${extra}` : ''}`);
  } else {
    failed++;
    console.log(`  ✗ ${name}${extra ? ` — ${extra}` : ''}`);
  }
}
const elapsed = () => `${((Date.now() - T0) / 1000).toFixed(1)}s`;

// ---------- 子进程管理 ----------
const children: ChildProcess[] = [];
function startProc(name: string, script: string, env: Record<string, string>): ChildProcess {
  const c = spawn(process.execPath, ['--disable-warning=ExperimentalWarning', join(ROOT, 'src', script)], {
    env: { ...process.env, ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  c.stdout!.on('data', (d) => String(d).split('\n').filter(Boolean).forEach((l) => console.log(`  │ ${name} │ ${l}`)));
  c.stderr!.on('data', (d) => String(d).split('\n').filter(Boolean).forEach((l) => console.log(`  │ ${name}⚠ │ ${l}`)));
  children.push(c);
  return c;
}
function killAll(): void {
  for (const c of children) {
    try { c.kill('SIGKILL'); } catch { /* ignore */ }
  }
}
process.on('exit', killAll);
const watchdog = setTimeout(() => {
  console.error(`\n✗ 验证超时（75 秒）`);
  process.exit(1);
}, 75_000);

// ---------- HTTP 辅助 ----------
const ctl = (replica: 'A' | 'B', path: string, body: any) => httpReq('POST', `${SITES[replica]}${path}`, body);
const ctlGet = (replica: 'A' | 'B', path: string) => httpReq('GET', `${SITES[replica]}${path}`);
const ctlBoth = (path: string, body: any) => Promise.all([ctl('A', path, body), ctl('B', path, body)]);
const getTape = (t: string) => httpReq('GET', `${API}/tapes/${t}`);
const getProg = (t: string, p: string) => httpReq('GET', `${API}/tapes/${t}/programs/${p}`);
const getStatus = () => httpReq('GET', `${API}/sync/status`);

/** 带重试地读取同步状态中的某盘某修订（验证脚本健壮性，避免偶发请求失败空引用） */
async function findRev(tape: string, kind: string, revision?: number): Promise<any> {
  for (let i = 0; i < 10; i++) {
    const s = await getStatus();
    const r = s.body?.revisions?.find((x: any) => x.tapeId === tape && x.kind === kind && (revision === undefined || x.revision === revision));
    if (r) return r;
    await sleep(150);
  }
  return undefined;
}
async function findSource(tape: string): Promise<any> {
  for (let i = 0; i < 10; i++) {
    const s = await getStatus();
    const r = s.body?.sources?.find((x: any) => x.tapeId === tape);
    if (r) return r;
    await sleep(150);
  }
  return undefined;
}

/** 某站某带某类清单 rev 的实际取页页号（去重；同一页因令牌重领可能多次发出） */
async function programPageNos(replica: 'A' | 'B', tape: string, key: string): Promise<number[]> {
  const st = await ctlGet(replica, '/control/state');
  const nos = (st.body.sources[tape]?.log ?? [])
    .filter((l: any) => l.route === 'page' && l.detail.includes(`${key} 第`) && l.detail.includes('发出'))
    .map((l: any) => Number((l.detail.match(/第(\d+)页/) ?? [])[1]));
  return [...new Set(nos)].sort((a, b) => a - b);
}

async function waitFor(label: string, fn: () => Promise<boolean>, timeoutMs: number): Promise<boolean> {
  const deadline = Math.min(Date.now() + timeoutMs, HARD_DEADLINE - 1000);
  while (Date.now() < deadline) {
    try {
      if (await fn()) return true;
    } catch { /* retry */ }
    await sleep(200);
  }
  note(`（等待超时：${label}）`);
  return false;
}

// ---------- 可读打印 ----------
function printCopies(copies: any[], indent = '    '): void {
  for (const c of copies) {
    console.log(`${indent}副本 ${c.copyId}：`);
    if (!c.playable.length) console.log(`${indent}  （无可播放段）`);
    for (const p of c.playable) {
      const md = p.master[1] - p.master[0];
      const cd = p.copy[1] - p.copy[0];
      const rate = cd > 0 ? md / cd : 0;
      const tag = Math.abs(rate - 1) > 1e-9 ? `  ← ${rate.toFixed(2)}x 变速段` : '';
      console.log(`${indent}  可播放 母带 ${fmtMs(p.master[0])}–${fmtMs(p.master[1])} → 副本 ${fmtMs(p.copy[0])}–${fmtMs(p.copy[1])}${tag}`);
    }
    if (!c.missing.length) {
      console.log(`${indent}  缺失母带段：无`);
    } else {
      for (const m of c.missing) {
        console.log(`${indent}  缺失母带段 ${fmtMs(m.master[0])}–${fmtMs(m.master[1])}${m.note ? `（${m.note}）` : ''}`);
      }
    }
  }
}
function printProgramView(v: any): void {
  console.log(
    `    节目 ${v.program.id}「${v.program.title}」 @ ${v.tapeId}` +
      `（时间轴 v${v.timelineVersion} · 节目单 rev${v.programRevision} + 修复清单 rev${v.restorationRevision}）`,
  );
  console.log(`    母带范围 ${fmtMs(v.program.masterRange[0])} – ${fmtMs(v.program.masterRange[1])}`);
  printCopies(v.copies);
}
function printTapeView(v: any): void {
  console.log(
    `    母带 ${v.tapeId}（时间轴 v${v.timelineVersion} · 时长 ${fmtMs(v.durationMs)} · 节目单 rev${v.programRevision} + 修复清单 rev${v.restorationRevision}）`,
  );
  for (const p of v.programs) {
    console.log(`    节目 ${p.id}「${p.title}」 ${fmtMs(p.masterRange[0])}–${fmtMs(p.masterRange[1])}`);
  }
  printCopies(v.copies);
}

// ---------- 演示数据（通过控制接口发布的后续修订） ----------
const T1_PROG_REV2: Rec[] = [
  { type: 'program', programId: 'P1', title: '晨间新闻', startMs: 0, endMs: 900_000 },
  { type: 'program', programId: 'P2', title: '戏曲选段·修订版', startMs: 900_000, endMs: 2_400_000 },
  { type: 'program', programId: 'P3', title: '旧广告联播', startMs: 2_400_000, endMs: 3_600_000 },
];
const T1V2_PROG: Rec[] = [
  { type: 'program', programId: 'P1', title: '晨间新闻', startMs: 0, endMs: 900_000 },
  { type: 'program', programId: 'P2', title: '戏曲选段·修订版', startMs: 900_000, endMs: 2_400_000 },
  { type: 'program', programId: 'P4', title: '新发现片段', startMs: 2_400_000, endMs: 3_900_000 },
];
const T1V2_REST: Rec[] = [
  { type: 'segment', copyId: 'C1', masterStartMs: 0, masterEndMs: 3_000_000, copyStartMs: 0, rate: 1 },
  { type: 'damage', copyId: 'C1', masterStartMs: 3_000_000, masterEndMs: 3_300_000, note: '片尾灼伤' },
  { type: 'segment', copyId: 'C1', masterStartMs: 3_300_000, masterEndMs: 3_900_000, copyStartMs: 3_000_000, rate: 1 },
  { type: 'segment', copyId: 'C2', masterStartMs: 0, masterEndMs: 3_900_000, copyStartMs: 0, rate: 1 },
];
const T2_PROG_REV2: Rec[] = [
  { type: 'program', programId: 'Q1', title: '午间路况', startMs: 0, endMs: 600_000 },
  { type: 'program', programId: 'Q2', title: '音乐点播', startMs: 600_000, endMs: 1_500_000 },
  { type: 'program', programId: 'Q3', title: '深夜重播', startMs: 1_500_000, endMs: 1_800_000 },
];
const T3_REST_REV2: Rec[] = [
  { type: 'segment', copyId: 'C1', masterStartMs: 0, masterEndMs: 900_000, copyStartMs: 0, rate: 1 },
];
// 主站暂停期间发布的 T1 修复清单 rev2（同 v1，内容与 rev1 一致，走备站接力采齐）
const T1_REST_REV2: Rec[] = [
  { type: 'segment', copyId: 'C1', masterStartMs: 0, masterEndMs: 1_200_000, copyStartMs: 0, rate: 1 },
  { type: 'damage', copyId: 'C1', masterStartMs: 1_200_000, masterEndMs: 1_500_000, note: '磁带霉变，信号丢失' },
  { type: 'segment', copyId: 'C1', masterStartMs: 1_500_000, masterEndMs: 3_600_000, copyStartMs: 1_200_000, rate: 1 },
  { type: 'segment', copyId: 'C2', masterStartMs: 0, masterEndMs: 1_800_000, copyStartMs: 0, rate: 1 },
  { type: 'segment', copyId: 'C2', masterStartMs: 1_800_000, masterEndMs: 2_400_000, copyStartMs: 1_800_000, rate: 1.5 },
  { type: 'segment', copyId: 'C2', masterStartMs: 2_400_000, masterEndMs: 3_600_000, copyStartMs: 2_200_000, rate: 1 },
];
// T2 节目单 rev2：主站为干净版；备站注入内容冲突（Q2 范围被改写）
const T2_PROG_REV2_CLEAN: Rec[] = [
  { type: 'program', programId: 'Q1', title: '午间路况', startMs: 0, endMs: 600_000 },
  { type: 'program', programId: 'Q2', title: '音乐点播', startMs: 600_000, endMs: 1_500_000 },
  { type: 'program', programId: 'Q3', title: '深夜重播', startMs: 1_500_000, endMs: 1_800_000 },
];
const T2_PROG_REV2_CONFLICT: Rec[] = [
  { type: 'program', programId: 'Q1', title: '午间路况', startMs: 0, endMs: 600_000 },
  { type: 'program', programId: 'Q2', title: '音乐点播（被备站篡改）', startMs: 600_000, endMs: 1_200_000 },
  { type: 'program', programId: 'Q3', title: '深夜重播', startMs: 1_500_000, endMs: 1_800_000 },
];

// ---------- 主流程 ----------
async function main(): Promise<void> {
  rmSync(WORK, { recursive: true, force: true });
  mkdirSync(WORK, { recursive: true });

  banner('阶段 0/8 · 启动主备两个模拟站点与查询接口');
  startProc('siteA', 'site.ts', { PORT: String(SITE_A_PORT), REPLICA_ID: 'A' });
  startProc('siteB', 'site.ts', { PORT: String(SITE_B_PORT), REPLICA_ID: 'B' });
  const siteUpA = await waitFor('主站 A 启动', async () => (await ctlGet('A', '/control/state')).status === 200, 10_000);
  const siteUpB = await waitFor('备站 B 启动', async () => (await ctlGet('B', '/control/state')).status === 200, 10_000);
  check('主站 A（4181）、备站 B（4183）均启动', siteUpA && siteUpB);
  check('两站站点标识独立', (await ctlGet('A', '/control/state')).body.replicaId === 'A' && (await ctlGet('B', '/control/state')).body.replicaId === 'B');
  // 混沌配置：T1 修复清单 rev1 第 2 页丢一次响应；
  // T3 修复清单 rev1：主站发出第 1 页后撤下，备站直接撤下（主备都采不齐，验证不阻塞他盘且可查进度）
  await ctlBoth('/control/lose-response', { tape: 'T1', kind: 'restoration', timelineVersion: 1, revision: 1, page: 1, times: 1 });
  await ctl('A', '/control/withdraw-after', { tape: 'T3', kind: 'restoration', timelineVersion: 1, revision: 1, page: 0 });
  await ctl('B', '/control/withdraw', { tape: 'T3', kind: 'restoration', timelineVersion: 1, revision: 1 });
  // 接力专用：T4 备站页大小改为 1（主站为 3），制造两站分页不同
  await ctl('B', '/control/page-size', { tape: 'T4', kind: 'program', timelineVersion: 1, revision: 1, pageSize: 1 });
  await ctl('B', '/control/page-size', { tape: 'T4', kind: 'restoration', timelineVersion: 1, revision: 1, pageSize: 1 });
  note('已注入：T1 两站第 2 页丢一次响应；T3 两站发第 1 页后撤下；T4 备站页大小=1（主站=3，分页不同）');
  startProc('api', 'api.ts', { PORT: String(API_PORT), DB_PATH: DB });
  const apiUp = await waitFor('查询接口启动', async () => (await httpReq('GET', `${API}/health`)).status === 200, 10_000);
  check('查询接口启动', apiUp);

  banner('阶段 1/8 · 首次采集：响应丢失恢复 · 采不齐不阻塞他盘');
  let syncProc = startProc('sync', 'sync.ts', {
    SITE_URL_A: SITE_A, SITE_URL_B: SITE_B, DB_PATH: DB, POLL_MS: '120',
  });
  const firstSync = await waitFor(
    'T1/T2 首版采齐生效',
    async () => (await getTape('T1')).status === 200 && (await getTape('T2')).status === 200,
    20_000,
  );
  check('T1、T2 首版采齐并生效', firstSync, elapsed());
  check('T4 首版亦采齐（主站 A 完成）', await waitFor('T4 生效', async () => (await getTape('T4')).status === 200, 10_000), elapsed());

  const st1 = await ctlGet('A', '/control/state');
  const t1log = st1.body.sources.T1.log as any[];
  const lost = t1log.some((l) => l.detail.includes('响应丢失'));
  const retokens = t1log.filter((l) => l.route === 'page-token' && l.detail.includes('restoration:1:1 第2页')).length;
  check('翻页响应丢失（令牌已消费）后凭新令牌续采恢复', lost && retokens >= 2, `第 2 页令牌签发 ${retokens} 次`);
  const layouts = new Set(t1log.filter((l) => l.route === 'page' && l.detail.includes('排版')).map((l) => (l.detail.includes('table') ? 'table' : 'cards')));
  check('两种排版均被解析', layouts.has('table') && layouts.has('cards'), [...layouts].join(' / '));

  const t3 = await getTape('T3');
  check('T3 修复清单两站均采不齐 → 无生效版次，但不影响 T1/T2/T4', t3.status === 404 && t3.body.error === 'NO_ACTIVE_VERSION');
  const ss1 = await getStatus();
  const t3rev = ss1.body.revisions.find((r: any) => r.tapeId === 'T3' && r.kind === 'restoration' && r.revision === 1);
  check(
    '待补齐清单可查进度',
    !!t3rev && t3rev.status === 'abandoned' && t3rev.recordsDone === 2 && t3rev.recordCount === 4,
    t3rev ? `${t3rev.status} ${t3rev.recordsDone}/${t3rev.recordCount} 条` : '无记录',
  );
  check(
    '待补齐清单可查损坏记录位置',
    !!t3rev && t3rev.damages.some((d: any) => d.masterRange[0] === 100_000 && d.masterRange[1] === 120_000),
    t3rev?.damages?.map((d: any) => `${fmtMs(d.masterRange[0])}–${fmtMs(d.masterRange[1])} ${d.note}`).join('；') ?? '',
  );
  const t1src = ss1.body.sources.find((s: any) => s.tapeId === 'T1');
  check('主站会话已落库且默认使用主站 A', !!t1src.replicas.A.sessionId && t1src.activeReplica === 'A');

  note('首次生效的节目定位（T1 / P2）：');
  const p2a = await getProg('T1', 'P2');
  printProgramView(p2a.body);
  const c1a = p2a.body.copies.find((c: any) => c.copyId === 'C1');
  const c2a = p2a.body.copies.find((c: any) => c.copyId === 'C2');
  check(
    'C1 缺失母带段与损坏记录一致',
    JSON.stringify(c1a.missing.map((m: any) => m.master)) === JSON.stringify([[1_200_000, 1_500_000]]),
  );
  check(
    'C2 全程可播且含 1.5x 变速段',
    c2a.missing.length === 0 && c2a.playable.some((p: any) => p.master[0] === 1_800_000 && p.copy[1] === 2_200_000),
  );

  banner('阶段 2/8 · 主站暂停 → 备站接力：两站暂停/会话独立，备站采齐新修订');
  const sessA2 = (await getStatus()).body.sources.find((s: any) => s.tapeId === 'T1')?.replicas?.A?.sessionId;
  const sessB2 = (await getStatus()).body.sources.find((s: any) => s.tapeId === 'T1')?.replicas?.B?.sessionId;
  const pauseStart = Date.now();
  await ctl('A', '/control/pause', { tape: 'T1', durationMs: 6000 });
  note(`已只暂停主站 A 的 T1 来源 6 秒（备站 B 不暂停，应照常服务）`);
  await sleep(300);
  // 暂停期间在两站发布 T1 修复清单 rev2 与 T2 节目单 rev2
  await ctlBoth('/control/publish', { tape: 'T1', manifest: { kind: 'restoration', timelineVersion: 1, revision: 2, pageSize: 2, records: T1_REST_REV2 } });
  await ctlBoth('/control/publish', { tape: 'T2', manifest: { kind: 'program', timelineVersion: 1, revision: 2, pageSize: 2, records: T2_PROG_REV2_CLEAN } });
  note('暂停期间两站发布 T1 修复清单 rev2、T2 节目单 rev2（主站暂停，只能由备站接力采集）');

  const relayDone = await waitFor(
    'T1 修复清单 rev2 经备站采齐',
    async () => {
      const s = await getStatus();
      const r = s.body.revisions.find((x: any) => x.tapeId === 'T1' && x.kind === 'restoration' && x.revision === 2);
      return r?.status === 'complete';
    },
    12_000,
  );
  check('主站暂停期间备站接力采齐 T1 修复清单 rev2', relayDone, elapsed());
  const t2Switched = await waitFor(
    'T2 rev2 生效',
    async () => {
      const t = await getTape('T2');
      return t.status === 200 && t.body.programRevision === 2;
    },
    10_000,
  );
  check('T1 暂停不影响其他母带：T2 rev2 同步生效', t2Switched, elapsed());

  const stA2 = await ctlGet('A', '/control/state');
  const API_ROUTES = new Set(['session', 'index', 'page-token', 'page', 'auth', 'blocked']);
  const t1logA = (stA2.body.sources.T1.log as any[]).filter((l) => API_ROUTES.has(l.route));
  const blocked = t1logA.find((l) => l.route === 'blocked' && l.t >= pauseStart - 100);
  check('采集器对主站 A 收到 423 后登记暂停', !!blocked);
  const afterLearn = t1logA.filter((l) => blocked && l.t > blocked.t + 50 && l.t < pauseStart + 5900);
  check('主站暂停登记后未再向 A 发任何请求（零请求）', afterLearn.length === 0, `窗口内请求 ${afterLearn.length} 个`);

  const stB2 = await ctlGet('B', '/control/state');
  const t1logB = stB2.body.sources.T1.log as any[];
  const bServed = t1logB.some((l) => l.route === 'page' && l.t >= pauseStart);
  check('备站 B 独立会话在暂停窗口内照常出页（两站暂停互不影响）', bServed);
  const ss2 = await getStatus();
  const t1src2 = ss2.body.sources.find((s: any) => s.tapeId === 'T1');
  check('暂停只临时绕行：本轮服务站切到 B（lastReplica=B），active 持久指针保持 A', t1src2.lastReplica === 'B' && t1src2.activeReplica === 'A');
  check('两站会话独立：B 会话与 A 会话不同且各自保留', t1src2.replicas.B.sessionId && t1src2.replicas.B.sessionId !== sessA2 && sessB2 !== sessA2);

  banner('阶段 3/8 · 主站恢复后新会话 · 同版次修订原子切换');
  // 恢复后再发布 T1 节目单 rev2（同版次），轮询观察原子性
  await ctlBoth('/control/publish', { tape: 'T1', manifest: { kind: 'program', timelineVersion: 1, revision: 2, pageSize: 2, records: T1_PROG_REV2 } });
  note('主站恢复后两站发布 T1 节目单 rev2（暂停只是临时绕行：恢复后 active 指针仍指 A，由 A 完成同版次切换）');
  let sawOld = false;
  let sawNew = false;
  let sawMixed = false;
  const pollEnd = Date.now() + 20_000;
  while (Date.now() < pollEnd && !sawNew) {
    const r = await getProg('T1', 'P2');
    if (r.status === 200) {
      const isNew = r.body.programRevision === 2;
      const rangeIsNew = r.body.program.masterRange[1] === 2_400_000;
      if (isNew !== rangeIsNew) sawMixed = true;
      if (isNew) sawNew = true;
      else sawOld = true;
    }
    if (!sawNew) await sleep(100);
  }
  check('T1 节目单 rev2 采齐生效', sawNew, elapsed());
  check('修订切换原子性：查询结果要么全旧要么全新', sawOld && !sawMixed, `旧 ${sawOld ? '见过' : '未见过'} / 混合 ${sawMixed ? '出现过!' : '未出现'}`);
  const ss3 = await getStatus();
  const t1src3 = ss3.body.sources.find((s: any) => s.tapeId === 'T1');
  const sessA3 = t1src3.replicas.A.sessionId;
  check('主站恢复后 A 站会话已轮换（暂停使旧会话作废）', !sessA3 || sessA3 !== sessA2, `${String(sessA2).slice(0, 8)}… → ${String(sessA3 ?? '无').slice(0, 8)}…`);
  check('临时绕行不改变持久 active 指针（恢复后 active 仍为主站 A）', t1src3.activeReplica === 'A');

  note('切换后的节目定位（T1 / P2，播放位置与缺失区段一起更新）：');
  const p2b = await getProg('T1', 'P2');
  printProgramView(p2b.body);
  const c1b = p2b.body.copies.find((c: any) => c.copyId === 'C1');
  check(
    '节目范围与播放位置随新修订更新',
    p2b.body.program.title.includes('修订版') &&
      p2b.body.program.masterRange[1] === 2_400_000 &&
      c1b.playable.some((p: any) => p.master[0] === 1_500_000 && p.master[1] === 2_400_000),
  );

  banner('阶段 4/8 · 新时间轴版次 v2 生效 · 旧记录随修订撤下');
  await ctlBoth('/control/publish', { tape: 'T1', timeline: { version: 2, durationMs: 3_900_000 }, manifest: { kind: 'program', timelineVersion: 2, revision: 1, pageSize: 2, records: T1V2_PROG } });
  await ctlBoth('/control/publish', { tape: 'T1', manifest: { kind: 'restoration', timelineVersion: 2, revision: 1, pageSize: 2, records: T1V2_REST } });
  note('两站发布 T1 时间轴 v2：节目单 rev1（P3 移除、新增 P4）+ 修复清单 rev1（损坏位置变化）');
  const v2Active = await waitFor(
    'T1 v2 生效',
    async () => {
      const t = await getTape('T1');
      return t.status === 200 && t.body.timelineVersion === 2;
    },
    15_000,
  );
  check('最新共同版次 v2 生效（两侧各取最新完整修订）', v2Active, elapsed());
  const p3 = await getProg('T1', 'P3');
  check('被新版清单移除的节目 P3 随修订撤下', p3.status === 404 && p3.body.error === 'PROGRAM_NOT_FOUND');
  note('新版次下的节目定位（T1 / P4）：');
  const p4 = await getProg('T1', 'P4');
  printProgramView(p4.body);
  const c1p4 = p4.body.copies.find((c: any) => c.copyId === 'C1');
  check(
    'v2 缺失区段正确（C1 缺 00:50:00–00:55:00）',
    JSON.stringify(c1p4.missing.map((m: any) => m.master)) === JSON.stringify([[3_000_000, 3_300_000]]),
  );

  banner('阶段 5/8 · 采集器重启：原有修订已撤下 · 已采齐复用');
  await ctlBoth('/control/withdraw', { tape: 'T1', kind: 'program', timelineVersion: 1, revision: 1 });
  await ctlBoth('/control/withdraw', { tape: 'T1', kind: 'program', timelineVersion: 1, revision: 2 });
  await ctlBoth('/control/withdraw', { tape: 'T1', kind: 'restoration', timelineVersion: 1, revision: 1 });
  await ctlBoth('/control/publish', { tape: 'T3', manifest: { kind: 'restoration', timelineVersion: 1, revision: 2, pageSize: 2, records: T3_REST_REV2 } });
  note('两站撤下 T1 全部 v1 修订；两站发布 T3 修复清单 rev2（补齐 T3）');
  const t1Keys = ['program:1:1', 'program:1:2', 'restoration:1:1', 'program:2:1', 'restoration:2:1'];
  const countT1 = async (r: 'A' | 'B') =>
    (await Promise.all(t1Keys.map((k) => programPageNos(r, 'T1', k)))).reduce((a, b) => a + b.length, 0);
  const pageReqsBeforeA = await countT1('A');
  const pageReqsBeforeB = await countT1('B');
  const beforeKill = await getProg('T1', 'P4');

  syncProc.kill('SIGKILL');
  note('已强制终止采集器（SIGKILL），1 秒后重启…');
  await sleep(1_000);
  const during = await getProg('T1', 'P4');
  check(
    '采集器重启期间查询持续提供此前可用结果',
    during.status === 200 && JSON.stringify(during.body) === JSON.stringify(beforeKill.body),
  );

  syncProc = startProc('sync', 'sync.ts', { SITE_URL_A: SITE_A, SITE_URL_B: SITE_B, DB_PATH: DB, POLL_MS: '120' });
  const t3Active = await waitFor('T3 生效', async () => (await getTape('T3')).status === 200, 15_000);
  check('重启后 T3 修复清单 rev2 采齐，T3 生效', t3Active, elapsed());
  const t1After = await getTape('T1');
  check('T1 已采齐修订不受站点撤下影响（仍为 v2）', t1After.status === 200 && t1After.body.timelineVersion === 2);
  const pageReqsAfterA = await countT1('A');
  const pageReqsAfterB = await countT1('B');
  check(
    '已采齐的修订可复用，主备两站均未重复抓取',
    pageReqsAfterA === pageReqsBeforeA && pageReqsAfterB === pageReqsBeforeB,
    `A ${pageReqsBeforeA}→${pageReqsAfterA} 次 / B ${pageReqsBeforeB}→${pageReqsAfterB} 次`,
  );

  banner('阶段 6/8 · 主备接力核心：主站采到一半撤下 · 重启 · 分页不同只取未采页');
  // 用一盘尚未在本轮被采过的新母带 T5：两站内容一致，主站页大小 3，备站页大小 1
  const T5_PROG: Rec[] = [
    { type: 'program', programId: 'U1', title: '新闻一', startMs: 0, endMs: 300_000 },
    { type: 'program', programId: 'U2', title: '新闻二', startMs: 300_000, endMs: 600_000 },
    { type: 'program', programId: 'U3', title: '新闻三', startMs: 600_000, endMs: 900_000 },
    { type: 'program', programId: 'U4', title: '新闻四', startMs: 900_000, endMs: 1_200_000 },
    { type: 'program', programId: 'U5', title: '新闻五', startMs: 1_200_000, endMs: 1_500_000 },
    { type: 'program', programId: 'U6', title: '新闻六', startMs: 1_500_000, endMs: 1_800_000 },
    { type: 'program', programId: 'U7', title: '新闻七', startMs: 1_800_000, endMs: 2_100_000 },
  ];
  const T5_REST: Rec[] = [
    { type: 'segment', copyId: 'C1', masterStartMs: 0, masterEndMs: 900_000, copyStartMs: 0, rate: 1 },
    { type: 'damage', copyId: 'C1', masterStartMs: 900_000, masterEndMs: 1_020_000, note: '中间掉磁' },
    { type: 'segment', copyId: 'C1', masterStartMs: 1_020_000, masterEndMs: 2_100_000, copyStartMs: 900_000, rate: 1 },
  ];
  await ctlBoth('/control/publish', {
    tape: 'T5', timeline: { version: 1, durationMs: 2_100_000 },
    manifest: { kind: 'program', timelineVersion: 1, revision: 1, pageSize: 3, records: T5_PROG },
  });
  await ctlBoth('/control/publish', {
    tape: 'T5', manifest: { kind: 'restoration', timelineVersion: 1, revision: 1, pageSize: 3, records: T5_REST },
  });
  // 备站页大小 1：7 条节目 → 7 页；3 条修复 → 3 页（主站分别为 3 页 / 1 页）
  await ctl('B', '/control/page-size', { tape: 'T5', kind: 'program', timelineVersion: 1, revision: 1, pageSize: 1 });
  await ctl('B', '/control/page-size', { tape: 'T5', kind: 'restoration', timelineVersion: 1, revision: 1, pageSize: 1 });
  // 主站节目单发出第 1 页（含 3 条记录）后撤下（只能由备站接力节目单）；
  // 修复清单两站都保持在架（暂停备站时采集器绕行主站也不会因全撤而放弃，恢复后仍回 B 接力节目单）
  await ctl('A', '/control/withdraw-after', { tape: 'T5', kind: 'program', timelineVersion: 1, revision: 1, page: 0 });
  note('T5 已在两站发布（内容一致）：主站页大小 3、备站页大小 1；主站节目单发第 1 页后撤下，修复清单两站在架');

  // 等 410 接力完成：备站 meta 入库、active 切到 B、修订仍未 abandoned
  const relayReady = await waitFor(
    '410 后切到备站并完成对账',
    async () => {
      const s = await getStatus();
      const src = s.body?.sources?.find((x: any) => x.tapeId === 'T5');
      const r = s.body?.revisions?.find((x: any) => x.tapeId === 'T5' && x.kind === 'program');
      return src?.activeReplica === 'B' && !!r?.replicas?.some((m: any) => m.replica === 'B');
    },
    15_000,
  );
  check('410 立即触发备站接力（active=B 且备站视图已对账）', relayReady, elapsed());
  const t5Prog = await findRev('T5', 'program');
  check('主站撤下后修订未被判 abandoned（备站仍在架，可接力）', relayReady && t5Prog.status !== 'abandoned' && t5Prog.listed === true);
  const t5MetaA = t5Prog.replicas.find((m: any) => m.replica === 'A');
  const t5MetaB = t5Prog.replicas.find((m: any) => m.replica === 'B');
  check('两站分页不同已登记', t5MetaA.pageSize === 3 && t5MetaB.pageSize === 1, `A ${t5MetaA.pageCount} 页 / B ${t5MetaB.pageCount} 页`);
  // 主站只发出并被采了第 1 页（3 条/页）即撤下 —— 用各站实际取页数佐证「采到一半」
  check('主站 A 仅采到第 1 页（3 条）就撤下，未越页', t5MetaA.pagesFetched === 1 && t5Prog.recordsDone >= 3, `A 取 ${t5MetaA.pagesFetched} 页`);

  // 杀掉采集器；在它停机期间暂停备站 T5（主站 A 不受影响），再重启。
  // 重启后采集器从落库的 active=B 接力，但 B 暂停中：验证独立暂停、暂停期间零取页、恢复后续采。
  syncProc.kill('SIGKILL');
  await ctl('B', '/control/pause', { tape: 'T5', durationMs: 6000 });
  note('采集器已 SIGKILL；停机期间备站 B 的 T5 单独暂停 6 秒（主站 A 不暂停），1 秒后重启…');
  await sleep(1_000);
  syncProc = startProc('sync', 'sync.ts', { SITE_URL_A: SITE_A, SITE_URL_B: SITE_B, DB_PATH: DB, POLL_MS: '120' });

  // 重启后的采集器撞上备站 423 才把暂停状态落库（A 站无暂停）
  const pauseLearned = await waitFor(
    '采集器登记备站暂停',
    async () => {
      const src = await findSource('T5');
      return !!src?.replicas?.B?.pausedUntil && !src?.replicas?.A?.pausedUntil;
    },
    8_000,
  );
  check('备站暂停独立落库（B 有暂停、A 无暂停，两站暂停各自独立）', pauseLearned);

  // 以「暂停登记完成」为基准点（此前的取页属于重启竞态，不计入暂停窗口），
  // 再连续两个轮询周期采样，要求备站取页数稳定不变，证明暂停期间对 B 零取页。
  const baseProg = (await programPageNos('B', 'T5', 'program:1:1')).length;
  const baseRest = (await programPageNos('B', 'T5', 'restoration:1:1')).length;
  await sleep(350);
  const bProgMid = (await programPageNos('B', 'T5', 'program:1:1')).length;
  const bRestMid = (await programPageNos('B', 'T5', 'restoration:1:1')).length;
  await sleep(350);
  const bProgMid2 = (await programPageNos('B', 'T5', 'program:1:1')).length;
  const bRestMid2 = (await programPageNos('B', 'T5', 'restoration:1:1')).length;
  check(
    '备站暂停登记后对 B 零取页（连续两轮采样，节目单/修复清单取页数都稳定不变）',
    bProgMid === baseProg && bProgMid2 === baseProg && bRestMid === baseRest && bRestMid2 === baseRest,
    `节目单 ${baseProg}/${bProgMid}/${bProgMid2}，修复 ${baseRest}/${bRestMid}/${bRestMid2}`,
  );

  const t5Done = await waitFor(
    'T5 节目单经备站接力采齐 7 条',
    async () => {
      const s = await getStatus();
      const r = s.body?.revisions?.find((x: any) => x.tapeId === 'T5' && x.kind === 'program');
      return r?.status === 'complete' && r.recordsDone === 7;
    },
    20_000,
  );
  check('备站恢复后从内容一致的备站接力，T5 节目单采齐（7/7 条）', t5Done, elapsed());
  const bProgPagesAfter = await programPageNos('B', 'T5', 'program:1:1');
  // 已采 3 条，备站页大小 1：首个含未采记录的页是第 4 页（pageNo=3），只请求第 4–7 页，绝不回取前 3 页
  check(
    '备站只请求含未采记录的页（7 条中已采 3 条 → 仅取第 4–7 页，共 4 页）',
    bProgPagesAfter.length === 4 && JSON.stringify(bProgPagesAfter) === JSON.stringify([4, 5, 6, 7]),
    `备站节目单取页 ${JSON.stringify(bProgPagesAfter)}`,
  );
  // 节目单是主站 410 硬故障接力：active 持久切到 B；暂停恢复后仍由 B 采（不回摆主站，因主站该单已撤）
  const t5srcEnd = await findSource('T5');
  check('410 硬故障接力持久落库 active=B（重启后仍由备站接力）', !!t5srcEnd && t5srcEnd.activeReplica === 'B');
  const t5Prog2 = await findRev('T5', 'program');
  check('记录无重复、无遗漏（7 个节目齐全）', !!t5Prog2 && t5Prog2.recordsDone === 7 && t5Prog2.status === 'complete');

  // 节目单在主站已撤下，全部由备站采齐；T5 整体按共同版次规则生效
  const t5Active = await waitFor('T5 整体生效', async () => (await getTape('T5')).status === 200, 20_000);
  check('备站暂停恢复后接力采齐，T5 按共同版次规则生效', t5Active, elapsed());
  const t5TapeResp = await getTape('T5');
  const t5View = t5TapeResp.body ?? {};
  const t5C1 = t5View.copies?.find((c: any) => c.copyId === 'C1');
  check(
    '接力采齐后的定位结果与清单一致（7 个节目 + C1 缺失段 00:15:00–00:17:00）',
    t5TapeResp.status === 200 &&
      t5View.programs?.length === 7 &&
      !!t5C1 &&
      JSON.stringify(t5C1.missing.map((m: any) => m.master)) === JSON.stringify([[900_000, 1_020_000]]),
  );

  banner('阶段 7/8 · 内容冲突与时长冲突：接力不改变现有定位结果 · 查询切换');
  // 1) 同号清单内容冲突。先在两站发布干净的 T2 节目单 rev3 并采齐
  await ctlBoth('/control/publish', { tape: 'T2', manifest: { kind: 'program', timelineVersion: 1, revision: 3, pageSize: 2, records: T2_PROG_REV2_CLEAN } });
  const t2rev3 = await waitFor(
    'T2 节目单 rev3 先采齐干净版',
    async () => {
      const s = await getStatus();
      const r = s.body?.revisions?.find((x: any) => x.tapeId === 'T2' && x.kind === 'program' && x.revision === 3);
      return r?.status === 'complete';
    },
    12_000,
  );
  check('T2 节目单 rev3 干净版采齐', t2rev3, elapsed());
  const q2Before = (await getProg('T2', 'Q2')).body;

  // 暂停主站 A 的 T2：采集器临时绕行备站 B（active 仍为 A），开始对 B 的索引对账（冲突才有机会被发现）
  await ctl('A', '/control/pause', { tape: 'T2', durationMs: 15_000 });
  const onB = await waitFor(
    'T2 临时绕行到备站 B 并完成对账',
    async () => {
      const src = await findSource('T2');
      const rev = await findRev('T2', 'program', 3);
      return src?.lastReplica === 'B' && src?.activeReplica === 'A' && !!rev?.replicas?.some((m: any) => m.replica === 'B');
    },
    10_000,
  );
  check('主站暂停后 T2 临时绕行备站 B（lastReplica=B、active 保持 A），备站视图已对账', onB);

  // 备站撤下后重新发布同号 rev3 但内容被篡改（哈希不同）
  await ctl('B', '/control/withdraw', { tape: 'T2', kind: 'program', timelineVersion: 1, revision: 3 });
  await ctl('B', '/control/publish', { tape: 'T2', manifest: { kind: 'program', timelineVersion: 1, revision: 3, pageSize: 2, records: T2_PROG_REV2_CONFLICT } });
  note('备站 B 重新发布同号 T2 节目单 rev3，但 Q2 内容被篡改（内容哈希冲突）');
  const conflictMarked = await waitFor(
    'rev3 冲突被标记',
    async () => {
      const r = await findRev('T2', 'program', 3);
      return r?.conflict === true;
    },
    10_000,
  );
  check('同号清单内容冲突被检出并标记（修订 conflict=1、备站 meta.conflict=1）', conflictMarked);
  await sleep(800); // 给采集器几轮，确认它不会去拉冲突站的页
  const rConf = await findRev('T2', 'program', 3);
  const bMeta = rConf?.replicas?.find((m: any) => m.replica === 'B');
  check('冲突站数据被拒收（备站该单零取页）', !!bMeta && bMeta.conflict === true && bMeta.pagesFetched === 0, `备站取页 ${bMeta?.pagesFetched}`);
  const q2After = (await getProg('T2', 'Q2')).body;
  check(
    '冲突不改变现有定位结果（Q2 范围仍是干净版 600000–1500000）',
    JSON.stringify(q2After.program.masterRange) === JSON.stringify(q2Before.program.masterRange) &&
      q2After.program.masterRange[1] === 1_500_000,
  );

  // 2) 母带时长冲突：备站把 T2 v1 时长改成不同值（T2 当前 active=B，下一轮对账即发现）
  await ctl('B', '/control/timeline-duration', { tape: 'T2', timelineVersion: 1, durationMs: 1_999_999 });
  note('备站 B 把 T2 时间轴 v1 时长改为 1999999ms（主站基准 1800000ms）');
  const durConflict = await waitFor(
    '时长冲突被标记',
    async () => (await getStatus()).body?.timelines?.some((t: any) => t.tapeId === 'T2' && t.conflict === true),
    10_000,
  );
  check('母带时长冲突被检出并标记', durConflict);
  const t2ViewAfter = (await getTape('T2')).body;
  check('时长冲突不覆盖基准时长（查询仍为 1800000ms）', t2ViewAfter.durationMs === 1_800_000, `实际 ${t2ViewAfter.durationMs}ms`);

  // 3) 查询切换：采集器持续运行中，查询接口各路由随时可切、结果稳定
  const routes = ['/tapes', '/tapes/T1', '/tapes/T5/programs', '/tapes/T2/programs/Q2', '/programs/U4', '/sync/status', '/health'];
  const okCount = (await Promise.all(routes.map((r) => httpReq('GET', `${API}${r}`)))).filter((r) => r.status === 200).length;
  check('查询接口在接力/冲突全程可切换，7 条路由全部 200', okCount === routes.length, `${okCount}/${routes.length}`);
  const cross = await httpReq('GET', `${API}/programs/U4`);
  check('跨母带节目查询可切到接力采齐的 T5（U4）', cross.status === 200 && cross.body.results[0].tapeId === 'T5');

  banner('阶段 8/8 · 节目定位结果展示');
  note('按母带查询（GET /tapes/T1）：');
  printTapeView((await getTape('T1')).body);
  note('接力母带查询（GET /tapes/T5）：');
  printTapeView((await getTape('T5')).body);
  note('按节目跨库查询（GET /programs/R2 → T3）：');
  printProgramView((await httpReq('GET', `${API}/programs/R2`)).body.results[0]);
  note('同步状态（GET /sync/status，含主备视图与冲突标记）：');
  const ss = (await getStatus()).body;
  for (const s of ss.sources) {
    console.log(
      `    来源 ${s.tapeId}：持久站点 ${s.activeReplica} · 本轮服务 ${s.lastReplica} · A 会话 ${s.replicas.A.sessionId ? String(s.replicas.A.sessionId).slice(0, 8) + '…' : '无'}` +
        `${s.replicas.A.pausedUntil && s.replicas.A.pausedUntil > Date.now() ? `（暂停至 ${new Date(s.replicas.A.pausedUntil).toISOString().slice(11, 19)}）` : ''}` +
        ` · B 会话 ${s.replicas.B.sessionId ? String(s.replicas.B.sessionId).slice(0, 8) + '…' : '无'}` +
        `${s.replicas.B.pausedUntil && s.replicas.B.pausedUntil > Date.now() ? `（暂停至 ${new Date(s.replicas.B.pausedUntil).toISOString().slice(11, 19)}）` : ''}`,
    );
  }
  for (const r of ss.revisions) {
    const dmg = r.damages.length ? ` · 损坏记录 ${r.damages.map((d: any) => `${fmtMs(d.masterRange[0])}–${fmtMs(d.masterRange[1])}`).join('、')}` : '';
    const conflict = r.conflict ? ' · ⚠ 内容冲突（拒收冲突站）' : '';
    const perSite = r.replicas.map((m: any) => `${m.replica}:${m.pageSize}条/页·取${m.pagesFetched}页${m.conflict ? '·冲突' : ''}${m.listed ? '' : '·撤下'}`).join(' / ');
    console.log(
      `    ${r.tapeId} ${r.kind === 'program' ? '节目单' : '修复清单'} v${r.timelineVersion} rev${r.revision}` +
        `：${r.status} ${r.recordsDone}/${r.recordCount} 条${r.listed ? '' : '（站点已撤下）'}${conflict}${dmg}`,
    );
    console.log(`      主备视图 ${perSite}`);
  }
  for (const t of ss.timelines) {
    if (t.conflict) console.log(`    ⚠ ${t.tapeId} 时间轴 v${t.timelineVersion} 时长冲突（基准保留 ${t.durationMs}ms）`);
  }

  banner('验证结果');
  console.log(`  ${passed} 项通过，${failed} 项失败 · 总耗时 ${elapsed()}`);
  clearTimeout(watchdog);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((e) => {
  console.error(`\n✗ 验证异常: ${e?.stack ?? e}`);
  process.exit(1);
});
