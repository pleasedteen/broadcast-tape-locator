/**
 * 一条命令的端到端验证（目标 < 80 秒）：
 *   npm run verify
 *
 * 场景覆盖：
 *   阶段 1  首次采集：翻页响应丢失（令牌已消费）后的恢复；T3 清单采不齐不影响他盘
 *   阶段 2  来源暂停：暂停期间该来源零请求，其他来源继续同步
 *   阶段 3  恢复后发放新会话；同版次节目单新修订生效，播放位置与缺失区段一起更新
 *   阶段 4  新时间轴版次 v2 生效；被新版清单移除的记录随修订撤下
 *   阶段 5  采集器重启：原有修订已撤下；已采齐修订复用不重采；查询持续可用
 *   阶段 6  节目定位结果展示
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

// ---------- 主流程 ----------
async function main(): Promise<void> {
  rmSync(WORK, { recursive: true, force: true });
  mkdirSync(WORK, { recursive: true });

  banner('阶段 0/6 · 启动本地模拟站点与查询接口');
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

  banner('阶段 1/6 · 首次采集：响应丢失恢复 · 采不齐不阻塞他盘');
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
  const retokens = t1log.filter((l) => l.route === 'page-token' && l.detail.includes('restoration:1:1 第2页')).length;
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

  banner('阶段 2/6 · 来源暂停：该来源零请求，其他来源继续同步');
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

  banner('阶段 3/6 · 恢复后新会话 · 同版次修订原子切换');
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

  banner('阶段 4/6 · 新时间轴版次 v2 生效 · 旧记录随修订撤下');
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

  banner('阶段 5/6 · 采集器重启：原有修订已撤下 · 已采齐复用');
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

  banner('阶段 6/6 · 节目定位结果展示');
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
      `    ${r.tapeId} ${r.kind === 'program' ? '节目单' : '修复清单'} v${r.timelineVersion} rev${r.revision}` +
        `：${r.status} ${r.pagesDone}/${r.pageCount} 页${r.listed ? '' : '（站点已撤下）'}${dmg}`,
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
