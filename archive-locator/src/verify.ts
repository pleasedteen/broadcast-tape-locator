/**
 * 一条命令的端到端验证（目标 < 80 秒）：
 *   npm run verify
 *
 * 场景覆盖：
 *   阶段 1  首次采集：翻页响应丢失（令牌已消费）后的恢复；T3 清单采不齐不影响他盘
 *   阶段 2  来源暂停：暂停期间该来源零请求，其他来源继续同步
 *   阶段 3  恢复后发放新会话；同版次节目单新修订生效，播放位置与缺失区段一起更新
 *   阶段 4  同修订号更正/原样重发：取页途中更正、重启后复用已采未改页、
 *           采齐前保持原有定位，采齐后替换更正侧；原样重发零抓取
 *   阶段 5  同版次改时长：两类清单按新时长都采齐前门控，采齐后时长/播放位置/缺口一起切换
 *   阶段 6  新时间轴版次 v2 生效；被新版清单移除的记录随修订撤下
 *   阶段 7  采集器重启：原有修订已撤下；已采齐修订复用不重采；查询持续可用
 *   阶段 8  节目定位结果展示
 */
import { spawn, type ChildProcess } from 'node:child_process';
import { mkdirSync, rmSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { fmtMs, httpReq, sleep } from './util.ts';
import type { Rec } from './types.ts';

const ROOT = join(fileURLToPath(import.meta.url), '..', '..');
const SITE_PORT = 4181;
const API_PORT = 4182;
const SITE = `http://127.0.0.1:${SITE_PORT}`;
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
const ctl = (path: string, body: any) => httpReq('POST', `${SITE}${path}`, body);
const ctlGet = (path: string) => httpReq('GET', `${SITE}${path}`);
const getTape = (t: string) => httpReq('GET', `${API}/tapes/${t}`);
const getProg = (t: string, p: string) => httpReq('GET', `${API}/tapes/${t}/programs/${p}`);
const getStatus = () => httpReq('GET', `${API}/sync/status`);

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
      `（时间轴 v${v.timelineVersion} · 节目单 rev${v.programRevision}/ed${v.programEdition ?? 1}` +
      ` + 修复清单 rev${v.restorationRevision}/ed${v.restorationEdition ?? 1}）`,
  );
  console.log(`    母带范围 ${fmtMs(v.program.masterRange[0])} – ${fmtMs(v.program.masterRange[1])}`);
  printCopies(v.copies);
}
function printTapeView(v: any): void {
  console.log(
    `    母带 ${v.tapeId}（时间轴 v${v.timelineVersion} · 时长 ${fmtMs(v.durationMs)}` +
      ` · 节目单 rev${v.programRevision}/ed${v.programEdition ?? 1}` +
      ` + 修复清单 rev${v.restorationRevision}/ed${v.restorationEdition ?? 1}）`,
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

// ---------- T4：同一修订号更正/原样重发 + 同版次改时长 ----------
// 初始节目单 7 条 4 页；更正只改第 2 页（S3），其余 3 页哈希不变、应被直接复用
const T4_PROG_REV1: Rec[] = [
  { type: 'program', programId: 'S1', title: '开场曲', startMs: 0, endMs: 600_000 },
  { type: 'program', programId: 'S2', title: '新闻速递', startMs: 600_000, endMs: 1_200_000 },
  { type: 'program', programId: 'S3', title: '人物访谈', startMs: 1_200_000, endMs: 1_800_000 },
  { type: 'program', programId: 'S4', title: '广播剧场', startMs: 1_800_000, endMs: 2_400_000 },
  { type: 'program', programId: 'S5', title: '音乐之旅', startMs: 2_400_000, endMs: 3_000_000 },
  { type: 'program', programId: 'S6', title: '财经观察', startMs: 3_000_000, endMs: 3_300_000 },
  { type: 'program', programId: 'S7', title: '午夜心语', startMs: 3_300_000, endMs: 3_600_000 },
];
// 原样重发：记录与 ed1 完全一致，所有页哈希相同，新 edition 应零抓取直接采齐
const T4_PROG_ED3: Rec[] = T4_PROG_REV1.map((r) => ({ ...r }));
// 更正稿 ed2：仅第 2 页 S3 变化
const T4_PROG_ED2: Rec[] = T4_PROG_REV1.map((r) =>
  r.type === 'program' && r.programId === 'S3' ? { ...r, title: '人物访谈·更正版' } : r,
);
// 同版次改时长（3600000 → 4000000）后的两类更正：节目单侧新增 S8，修复侧延长末段
const T4_PROG_V1_ED4: Rec[] = [
  ...T4_PROG_ED2.slice(0, 6),
  { type: 'program', programId: 'S8', title: '加长时段新节目', startMs: 3_600_000, endMs: 4_000_000 },
];
const T4_REST_ED2: Rec[] = [
  { type: 'segment', copyId: 'C1', masterStartMs: 0, masterEndMs: 2_000_000, copyStartMs: 0, rate: 1 },
  { type: 'damage', copyId: 'C1', masterStartMs: 2_000_000, masterEndMs: 2_200_000, note: '带基粘连' },
  { type: 'segment', copyId: 'C1', masterStartMs: 2_200_000, masterEndMs: 4_000_000, copyStartMs: 2_000_000, rate: 1 },
];

// ---------- 主流程 ----------
async function main(): Promise<void> {
  rmSync(WORK, { recursive: true, force: true });
  mkdirSync(WORK, { recursive: true });

  banner('阶段 0/8 · 启动本地模拟站点与查询接口');
  startProc('site', 'site.ts', { PORT: String(SITE_PORT) });
  const siteUp = await waitFor('站点启动', async () => (await ctlGet('/control/state')).status === 200, 10_000);
  check('模拟站点启动', siteUp);
  // 混沌配置：T1 修复清单 rev1 第 2 页丢一次响应；T3 修复清单 rev1 发出第 1 页后撤下
  await ctl('/control/lose-response', { tape: 'T1', kind: 'restoration', timelineVersion: 1, revision: 1, page: 1, times: 1 });
  await ctl('/control/withdraw-after', { tape: 'T3', kind: 'restoration', timelineVersion: 1, revision: 1, page: 0 });
  note('已注入故障：T1 修复清单第 2 页丢一次响应；T3 修复清单 rev1 发出第 1 页后撤下');
  startProc('api', 'api.ts', { PORT: String(API_PORT), DB_PATH: DB });
  const apiUp = await waitFor('查询接口启动', async () => (await httpReq('GET', `${API}/health`)).status === 200, 10_000);
  check('查询接口启动', apiUp);

  banner('阶段 1/8 · 首次采集：响应丢失恢复 · 采不齐不阻塞他盘');
  let syncProc = startProc('sync', 'sync.ts', { SITE_URL: SITE, DB_PATH: DB, POLL_MS: '250' });
  const firstSync = await waitFor(
    'T1/T2 首版采齐生效',
    async () => (await getTape('T1')).status === 200 && (await getTape('T2')).status === 200,
    20_000,
  );
  check('T1、T2 首版采齐并生效', firstSync, elapsed());

  const st1 = await ctlGet('/control/state');
  const t1log = st1.body.sources.T1.log as any[];
  const lost = t1log.some((l) => l.detail.includes('响应丢失'));
  const retokens = t1log.filter((l) => l.route === 'page-token' && l.detail.includes('restoration:1:1') && l.detail.includes('第2页')).length;
  check('翻页响应丢失（令牌已消费）后凭新令牌续采恢复', lost && retokens >= 2, `第 2 页令牌签发 ${retokens} 次`);
  const layouts = new Set(t1log.filter((l) => l.route === 'page' && l.detail.includes('排版')).map((l) => (l.detail.includes('table') ? 'table' : 'cards')));
  check('两种排版均被解析', layouts.has('table') && layouts.has('cards'), [...layouts].join(' / '));

  const t3 = await getTape('T3');
  check('T3 修复清单采不齐 → 无生效版次，但不影响 T1/T2', t3.status === 404 && t3.body.error === 'NO_ACTIVE_VERSION');
  const ss1 = await getStatus();
  const t3rev = ss1.body.revisions.find((r: any) => r.tapeId === 'T3' && r.kind === 'restoration' && r.revision === 1);
  check(
    '待补齐清单可查进度',
    !!t3rev && t3rev.status === 'abandoned' && t3rev.pagesDone === 1 && t3rev.pageCount === 2,
    t3rev ? `${t3rev.status} ${t3rev.pagesDone}/${t3rev.pageCount} 页` : '无记录',
  );
  check(
    '待补齐清单可查损坏记录位置',
    !!t3rev && t3rev.damages.some((d: any) => d.masterRange[0] === 100_000 && d.masterRange[1] === 120_000),
    t3rev?.damages?.map((d: any) => `${fmtMs(d.masterRange[0])}–${fmtMs(d.masterRange[1])} ${d.note}`).join('；') ?? '',
  );

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

  banner('阶段 2/8 · 来源暂停：该来源零请求，其他来源继续同步');
  const sessBefore = (await getStatus()).body.sources.find((s: any) => s.tapeId === 'T1')?.sessionId;
  const pauseStart = Date.now();
  await ctl('/control/pause', { tape: 'T1', durationMs: 4000 });
  note(`已要求暂停 T1 来源 4 秒（${new Date(pauseStart).toISOString().slice(11, 19)} 起）`);
  await sleep(300);
  await ctl('/control/publish', { tape: 'T2', manifest: { kind: 'program', timelineVersion: 1, revision: 2, pageSize: 2, records: T2_PROG_REV2 } });
  note('暂停期间发布 T2 节目单 rev2（新增节目 Q3）');
  const t2Switched = await waitFor(
    'T2 rev2 生效',
    async () => {
      const t = await getTape('T2');
      return t.status === 200 && t.body.programRevision === 2;
    },
    10_000,
  );
  check('T1 暂停期间 T2 继续同步并生效', t2Switched, elapsed());
  await sleep(1_000);
  await ctl('/control/publish', { tape: 'T1', manifest: { kind: 'program', timelineVersion: 1, revision: 2, pageSize: 2, records: T1_PROG_REV2 } });
  note('暂停期间发布 T1 节目单 rev2（恢复后才会被采集）');
  // 边等暂停结束边轮询 P2：观察修订切换原子性（任何一次查询结果应整体为旧或整体为新）
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
  const st2 = await ctlGet('/control/state');
  // 只统计采集器向来源发的 API 请求（排除 publish/pause 等站点控制记录）
  const API_ROUTES = new Set(['session', 'index', 'page-token', 'page', 'auth', 'blocked']);
  const t1log2 = (st2.body.sources.T1.log as any[]).filter((l) => API_ROUTES.has(l.route));
  const blocked = t1log2.find((l) => l.route === 'blocked' && l.t >= pauseStart - 100);
  check('采集器收到 423 后登记暂停并停发请求', !!blocked);
  const afterLearn = t1log2.filter((l) => blocked && l.t > blocked.t + 50 && l.t < pauseStart + 3900);
  check('得知暂停后未再向 T1 发任何请求', afterLearn.length === 0, `登记暂停后窗口内请求 ${afterLearn.length} 个`);

  banner('阶段 3/8 · 恢复后新会话 · 同版次修订原子切换');
  check('恢复后 T1 节目单 rev2 采齐生效', sawNew, elapsed());
  check('修订切换原子性：查询结果要么全旧要么全新', sawOld && !sawMixed, `旧 ${sawOld ? '见过' : '未见过'} / 混合 ${sawMixed ? '出现过!' : '未出现'}`);
  const sessAfter = (await getStatus()).body.sources.find((s: any) => s.tapeId === 'T1')?.sessionId;
  check('恢复时站点发放新会话', !!sessBefore && !!sessAfter && sessBefore !== sessAfter, `${String(sessBefore).slice(0, 8)}… → ${String(sessAfter).slice(0, 8)}…`);

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

  banner('阶段 4/8 · 同修订号更正：取页途中更正、重启复用未改页、采齐后原子替换');
  // T4 首版随阶段 1 一起采集；确认已生效
  const t4Init = await waitFor('T4 首版生效', async () => (await getTape('T4')).status === 200, 15_000);
  check('T4 首版（节目单 ed1 + 修复清单 ed1）采齐生效', t4Init, elapsed());
  const t4v1 = await getTape('T4');
  check('首版时长与版次正确', t4v1.body.timelineVersion === 1 && t4v1.body.durationMs === 3_600_000);
  const s3before = (await getProg('T4', 'S3')).body;
  check('更正前 S3 为原标题', s3before.program.title === '人物访谈' && s3before.programEdition === 1);

  // 在更正稿「唯一变化页」上注入持续丢响应，使 ed2 登记+复用后稳定停在未采齐状态
  await ctl('/control/publish', { tape: 'T4', manifest: { kind: 'program', timelineVersion: 1, revision: 1, pageSize: 2, records: T4_PROG_ED2 } });
  await ctl('/control/lose-response', { tape: 'T4', kind: 'program', timelineVersion: 1, revision: 1, page: 1, times: 50 });
  note('已发布 T4 节目单 rev1 更正 edition 2（仅第 2 页 S3 变化），并让该页持续丢响应');

  const ed2Registered = await waitFor(
    'ed2 登记并复用未改页',
    async () => {
      const s = await getStatus();
      const ed2 = s.body.revisions.find((r: any) => r.tapeId === 'T4' && r.kind === 'program' && r.revision === 1 && r.edition === 2);
      return !!ed2 && ed2.status === 'collecting';
    },
    10_000,
  );
  const ssMid = await getStatus();
  const ed2row = ssMid.body.revisions.find((r: any) => r.tapeId === 'T4' && r.kind === 'program' && r.edition === 2);
  check('更正 edition 2 独立登记（与 ed1 分行，物理隔离不拼接）', ed2Registered && ed2row.revision === 1 && ed2row.edition === 2);
  check('未改页按哈希复用（4 页中复用 3 页，连续进度 1/4）', ed2row.pagesDone === 1 && ed2row.pageCount === 4, `进度 ${ed2row.pagesDone}/${ed2row.pageCount}`);

  // 更正稿采齐前：查询持续提供原有定位（ed1）
  const t4During = await getTape('T4');
  const s3during = await getProg('T4', 'S3');
  check('更正稿采齐前继续提供原有定位（仍为 ed1、原标题）', t4During.body.programEdition === 1 && s3during.body.program.title === '人物访谈');

  // 站点在取页途中完成更正后，强制重启采集器
  const t4PageBefore = (await ctlGet('/control/state')).body.sources.T4.log
    .filter((l: any) => l.route === 'page' && l.detail.includes('program:1:1') && l.detail.includes('ed2') && l.detail.includes('发出')).length;
  syncProc.kill('SIGKILL');
  note('更正采集中强制终止采集器（SIGKILL），1 秒后重启…');
  await sleep(1_000);
  const duringRestart = await getProg('T4', 'S3');
  check('重启窗口内查询结果不变（仍为 ed1 原定位）', duringRestart.body.programEdition === 1 && duringRestart.body.program.title === '人物访谈');

  syncProc = startProc('sync', 'sync.ts', { SITE_URL: SITE, DB_PATH: DB, POLL_MS: '250' });
  // 重启后先确认复用仍在，再解除故障让变化页续采
  await sleep(800);
  const ssAfterBoot = await getStatus();
  const ed2afterBoot = ssAfterBoot.body.revisions.find((r: any) => r.tapeId === 'T4' && r.kind === 'program' && r.edition === 2);
  check('重启后已采未改页继续复用、不重抓', !!ed2afterBoot && ed2afterBoot.pagesDone === 1 && ed2afterBoot.status === 'collecting');
  await ctl('/control/lose-response', { tape: 'T4', kind: 'program', timelineVersion: 1, revision: 1, page: 1, times: 0 });

  const ed2Active = await waitFor(
    'T4 节目单 ed2 生效',
    async () => {
      const t = await getTape('T4');
      return t.status === 200 && t.body.programEdition === 2;
    },
    12_000,
  );
  check('重启续采后更正稿采齐，替换更正侧（节目单 → ed2）', ed2Active, elapsed());
  const s3after = await getProg('T4', 'S3');
  check('更正内容生效：S3 标题更新且修复侧仍为 ed1（未改侧不重发）',
    s3after.body.program.title.includes('更正版') && s3after.body.restorationEdition === 1);
  const t4PageAfter = (await ctlGet('/control/state')).body.sources.T4.log
    .filter((l: any) => l.route === 'page' && l.detail.includes('program:1:1') && l.detail.includes('ed2') && l.detail.includes('发出')).length;
  check('整个更正只抓取变化的第 2 页（未改页零网络请求）', t4PageAfter - t4PageBefore <= 1 && t4PageAfter === 1, `ed2 成功取页 ${t4PageAfter} 次`);

  // 原样重发：所有页哈希相同，新 edition 应零抓取直接采齐，定位内容不变
  const t4PageBeforeEd3 = (await ctlGet('/control/state')).body.sources.T4.log
    .filter((l: any) => l.route === 'page' && l.detail.includes('program:1:1')).length;
  await ctl('/control/publish', { tape: 'T4', manifest: { kind: 'program', timelineVersion: 1, revision: 1, pageSize: 2, records: T4_PROG_ED3 } });
  note('原样重发 T4 节目单 rev1 edition 3（内容与 ed1 完全一致）');
  const ed3Idle = await waitFor(
    'ed3 零抓取采齐',
    async () => {
      const s = await getStatus();
      const ed3 = s.body.revisions.find((r: any) => r.tapeId === 'T4' && r.kind === 'program' && r.edition === 3);
      return !!ed3 && ed3.status === 'complete';
    },
    8_000,
  );
  const t4PageAfterEd3 = (await ctlGet('/control/state')).body.sources.T4.log
    .filter((l: any) => l.route === 'page' && l.detail.includes('program:1:1')).length;
  check('原样重发按全部页哈希复用、零网络抓取', ed3Idle && t4PageAfterEd3 === t4PageBeforeEd3, `页面请求增量 ${t4PageAfterEd3 - t4PageBeforeEd3}`);
  const s3ed3 = await getProg('T4', 'S3');
  check('原样重发零抓取采齐并按其内容生效（edition 推进到 3，内容回到原版）',
    ed3Idle && t4PageAfterEd3 === t4PageBeforeEd3 && s3ed3.body.programEdition === 3 && s3ed3.body.program.title === '人物访谈',
    `页面请求增量 ${t4PageAfterEd3 - t4PageBeforeEd3}，S3 标题「${s3ed3.body.program.title}」`);

  banner('阶段 5/8 · 同版次改时长：两类清单按新时长采齐前门控，采齐后一起切换');
  // 修复侧变化页先卡住：只让节目单侧新 edition 采齐，验证时长/播放位置/缺口不得先切
  await ctl('/control/publish', { tape: 'T4', timeline: { version: 1, durationMs: 4_000_000 },
    manifest: { kind: 'program', timelineVersion: 1, revision: 1, pageSize: 2, records: T4_PROG_V1_ED4 } });
  await ctl('/control/publish', { tape: 'T4', manifest: { kind: 'restoration', timelineVersion: 1, revision: 1, pageSize: 2, records: T4_REST_ED2 } });
  await ctl('/control/lose-response', { tape: 'T4', kind: 'restoration', timelineVersion: 1, revision: 1, page: 1, times: 50 });
  note('站点声明 v1 新时长 4000000ms 并重发两类清单；修复侧变化页持续丢响应制造门控窗口');

  const progEd4Done = await waitFor(
    '节目单侧新 edition 采齐',
    async () => {
      const s = await getStatus();
      const p = s.body.revisions.find((r: any) => r.tapeId === 'T4' && r.kind === 'program' && r.edition === 4);
      return !!p && p.status === 'complete';
    },
    10_000,
  );
  await sleep(700); // 留几个轮询周期确认修复侧仍未齐、激活一直不切
  const ssGate = await getStatus();
  const restEd2 = ssGate.body.revisions.find((r: any) => r.tapeId === 'T4' && r.kind === 'restoration' && r.edition === 2);
  check('仅节目单侧采齐、修复侧仍在采集（门控前提成立）', progEd4Done && !!restEd2 && restEd2.status === 'collecting',
    restEd2 ? `修复侧 ${restEd2.pagesDone}/${restEd2.pageCount} 页` : '无修复侧行');
  const gated = await getTape('T4');
  check('门控期间时长/播放位置/缺口整体保持旧档（3600000ms、ed3 + ed1）',
    gated.body.durationMs === 3_600_000 && gated.body.programEdition === 3 && gated.body.restorationEdition === 1,
    `实际 ${gated.body.durationMs}ms / prog ed${gated.body.programEdition} / rest ed${gated.body.restorationEdition}`);

  // 解除故障，两类齐活后应在同一事务整体切换
  await ctl('/control/lose-response', { tape: 'T4', kind: 'restoration', timelineVersion: 1, revision: 1, page: 1, times: 0 });
  const switched = await waitFor(
    '两类清单按新时长采齐后整体切换',
    async () => {
      const t = await getTape('T4');
      return t.status === 200 && t.body.durationMs === 4_000_000 &&
        t.body.programEdition === 4 && t.body.restorationEdition === 2;
    },
    12_000,
  );
  check('时长、播放位置、缺口在同一事务一起切换（4000000ms、ed4 + ed2）', switched, elapsed());
  const fin = await getTape('T4');
  const c1fin = fin.body.copies.find((c: any) => c.copyId === 'C1');
  check('切换后缺口与延长段正确（C1 缺 00:33:20–00:36:40，末段延至新时长）',
    JSON.stringify(c1fin.missing.map((m: any) => m.master)) === JSON.stringify([[2_000_000, 2_200_000]]) &&
      c1fin.playable.at(-1).master[1] === 4_000_000);
  const s8 = await getProg('T4', 'S8');
  check('新时长下新增节目 S8 可定位', s8.status === 200 && s8.body.program.masterRange[1] === 4_000_000);

  banner('阶段 6/8 · 新时间轴版次 v2 生效 · 旧记录随修订撤下');
  await ctl('/control/publish', { tape: 'T1', timeline: { version: 2, durationMs: 3_900_000 }, manifest: { kind: 'program', timelineVersion: 2, revision: 1, pageSize: 2, records: T1V2_PROG } });
  await ctl('/control/publish', { tape: 'T1', manifest: { kind: 'restoration', timelineVersion: 2, revision: 1, pageSize: 2, records: T1V2_REST } });
  note('已发布 T1 时间轴 v2：节目单 rev1（P3 移除、新增 P4）+ 修复清单 rev1（损坏位置变化）');
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

  banner('阶段 7/8 · 采集器重启：原有修订已撤下 · 已采齐复用');
  await ctl('/control/withdraw', { tape: 'T1', kind: 'program', timelineVersion: 1, revision: 1 });
  await ctl('/control/withdraw', { tape: 'T1', kind: 'program', timelineVersion: 1, revision: 2 });
  await ctl('/control/withdraw', { tape: 'T1', kind: 'restoration', timelineVersion: 1, revision: 1 });
  await ctl('/control/publish', { tape: 'T3', manifest: { kind: 'restoration', timelineVersion: 1, revision: 2, pageSize: 2, records: T3_REST_REV2 } });
  note('已撤下 T1 全部 v1 修订；发布 T3 修复清单 rev2（补齐 T3）');
  const pageReqsBefore = (await ctlGet('/control/state')).body.sources.T1.log.filter((l: any) => l.route === 'page').length;
  const beforeKill = await getProg('T1', 'P4');

  syncProc.kill('SIGKILL');
  note('已强制终止采集器（SIGKILL），1 秒后重启…');
  await sleep(1_000);
  const during = await getProg('T1', 'P4');
  check(
    '采集器重启期间查询持续提供此前可用结果',
    during.status === 200 && JSON.stringify(during.body) === JSON.stringify(beforeKill.body),
  );

  syncProc = startProc('sync', 'sync.ts', { SITE_URL: SITE, DB_PATH: DB, POLL_MS: '250' });
  const t3Active = await waitFor('T3 生效', async () => (await getTape('T3')).status === 200, 15_000);
  check('重启后 T3 修复清单 rev2 采齐，T3 生效', t3Active, elapsed());
  const t1After = await getTape('T1');
  check('T1 已采齐修订不受站点撤下影响（仍为 v2）', t1After.status === 200 && t1After.body.timelineVersion === 2);
  const pageReqsAfter = (await ctlGet('/control/state')).body.sources.T1.log.filter((l: any) => l.route === 'page').length;
  check('已采齐的修订可复用，未重复抓取', pageReqsAfter === pageReqsBefore, `T1 页面请求 ${pageReqsBefore} → ${pageReqsAfter} 次`);

  banner('阶段 8/8 · 节目定位结果展示');
  note('按母带查询（GET /tapes/T1）：');
  printTapeView((await getTape('T1')).body);
  note('按节目跨库查询（GET /programs/R2 → T3）：');
  printProgramView((await httpReq('GET', `${API}/programs/R2`)).body.results[0]);
  note('同步状态（GET /sync/status）：');
  const ss = (await getStatus()).body;
  for (const s of ss.sources) {
    console.log(`    来源 ${s.tapeId}：会话 ${s.sessionId ? `${String(s.sessionId).slice(0, 8)}…` : '无'}${s.pausedUntil && s.pausedUntil > Date.now() ? ` · 暂停至 ${new Date(s.pausedUntil).toISOString()}` : ''}`);
  }
  for (const r of ss.revisions) {
    const dmg = r.damages.length ? ` · 损坏记录 ${r.damages.map((d: any) => `${fmtMs(d.masterRange[0])}–${fmtMs(d.masterRange[1])}`).join('、')}` : '';
    console.log(
      `    ${r.tapeId} ${r.kind === 'program' ? '节目单' : '修复清单'} v${r.timelineVersion} rev${r.revision} ed${r.edition}` +
        `（声明时长 ${r.durationMs ?? '—'}ms）：${r.status} ${r.pagesDone}/${r.pageCount} 页${r.listed ? '' : '（站点已撤下）'}${dmg}`,
    );
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
