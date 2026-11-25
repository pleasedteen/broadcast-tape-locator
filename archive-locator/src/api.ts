/**
 * 查询接口（只读 SQLite，独立于采集器进程；同步/重启期间持续提供此前可用结果）。
 *
 * 环境变量：
 *   PORT     监听端口（默认 4182）
 *   DB_PATH  SQLite 数据库路径（默认 <项目>/data/archive.db）
 *
 * 路由：
 *   GET /                              接口说明
 *   GET /health                        存活检查
 *   GET /tapes                         母带列表（含当前生效版次）
 *   GET /tapes/:tape                   母带视图：各副本可播放时间段 + 缺失母带区段
 *   GET /tapes/:tape/programs          当前生效节目单
 *   GET /tapes/:tape/programs/:pid     节目视图：母带范围 + 各副本可播放时间段 + 缺失区段
 *   GET /programs/:pid                 按节目跨母带查询
 *   GET /sync/status                   同步进度（含待补齐清单的进度与损坏记录位置）
 */
import http from 'node:http';
import { mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { computeCopyView, openDb } from './db.ts';

const PORT = Number(process.env.PORT ?? 4182);
const ROOT = join(fileURLToPath(import.meta.url), '..', '..');
const DB_PATH = process.env.DB_PATH ?? join(ROOT, 'data', 'archive.db');

mkdirSync(dirname(DB_PATH), { recursive: true });
const db = openDb(DB_PATH);

function send(res: http.ServerResponse, status: number, obj: any): void {
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(obj, null, 2));
}

function activationInfo(tape: string): any {
  return db
    .prepare(
      `SELECT a.tape_id, a.timeline_version, a.activated_at, a.duration_ms,
              pr.revision AS program_rev, pr.edition AS program_edition, pr.id AS program_rev_id,
              rr.revision AS restoration_rev, rr.edition AS restoration_edition, rr.id AS restoration_rev_id
       FROM activations a
       JOIN revisions pr ON pr.id = a.program_revision_id
       JOIN revisions rr ON rr.id = a.restoration_revision_id
       WHERE a.tape_id = ?`,
    )
    .get(tape) as any;
}

/** 各副本在 [rangeStart, rangeEnd) 内的可播放时间段与缺失母带区段 */
function copyViews(restorationRevId: number, rangeStart: number, rangeEnd: number): any[] {
  const segs = db.prepare('SELECT * FROM segments WHERE revision_id = ? ORDER BY copy_id, master_start_ms').all(restorationRevId) as any[];
  const dmgs = db.prepare('SELECT * FROM damages WHERE revision_id = ?').all(restorationRevId) as any[];
  const copyIds = new Set<string>();
  for (const s of segs) copyIds.add(s.copy_id);
  for (const d of dmgs) copyIds.add(d.copy_id);
  const out: any[] = [];
  for (const copyId of [...copyIds].sort()) {
    const view = computeCopyView(
      segs.filter((s) => s.copy_id === copyId),
      dmgs.filter((d) => d.copy_id === copyId),
      rangeStart,
      rangeEnd,
    );
    out.push({ copyId, playable: view.playable, missing: view.missing });
  }
  return out;
}

/** 生效时长以激活指针上的快照为准：同版次改时长时与指针在同一事务切换 */
function tapeView(tape: string): { status: number; body: any } {
  const act = activationInfo(tape);
  if (!act) return { status: 404, body: { error: 'NO_ACTIVE_VERSION', message: '两类清单尚无均已采齐的共同时间轴版次' } };
  const durationMs = act.duration_ms;
  const programs = db
    .prepare('SELECT program_id, title, start_ms, end_ms FROM programs WHERE revision_id = ? ORDER BY start_ms')
    .all(act.program_rev_id) as any[];
  return {
    status: 200,
    body: {
      tapeId: tape,
      timelineVersion: act.timeline_version,
      durationMs,
      programRevision: act.program_rev,
      programEdition: act.program_edition,
      restorationRevision: act.restoration_rev,
      restorationEdition: act.restoration_edition,
      activatedAt: act.activated_at,
      programs: programs.map((p) => ({ id: p.program_id, title: p.title, masterRange: [p.start_ms, p.end_ms] })),
      copies: copyViews(act.restoration_rev_id, 0, durationMs),
    },
  };
}

function programView(tape: string, pid: string): { status: number; body: any } {
  const act = activationInfo(tape);
  if (!act) return { status: 404, body: { error: 'NO_ACTIVE_VERSION', message: '两类清单尚无均已采齐的共同时间轴版次' } };
  const prog = db.prepare('SELECT * FROM programs WHERE revision_id = ? AND program_id = ?').get(act.program_rev_id, pid) as any;
  if (!prog) {
    return { status: 404, body: { error: 'PROGRAM_NOT_FOUND', message: '节目不在当前生效的节目单修订中（可能已随旧修订撤下）' } };
  }
  return {
    status: 200,
    body: {
      tapeId: tape,
      timelineVersion: act.timeline_version,
      durationMs: act.duration_ms,
      programRevision: act.program_rev,
      programEdition: act.program_edition,
      restorationRevision: act.restoration_rev,
      restorationEdition: act.restoration_edition,
      program: { id: prog.program_id, title: prog.title, masterRange: [prog.start_ms, prog.end_ms] },
      copies: copyViews(act.restoration_rev_id, prog.start_ms, prog.end_ms),
    },
  };
}

function syncStatus(): any {
  const sources = (db.prepare('SELECT * FROM sources ORDER BY tape_id').all() as any[]).map((s) => ({
    tapeId: s.tape_id,
    pausedUntil: s.paused_until,
    sessionId: s.session_id,
  }));
  const revisions = (db.prepare('SELECT * FROM revisions ORDER BY tape_id, timeline_version, kind, revision, edition').all() as any[]).map((r) => ({
    tapeId: r.tape_id,
    kind: r.kind,
    timelineVersion: r.timeline_version,
    revision: r.revision,
    edition: r.edition,
    status: r.status,
    pagesDone: r.pages_done,
    pageCount: r.page_count,
    listed: !!r.listed,
    durationMs: r.duration_ms,
    // 待补齐清单已采到的损坏记录位置
    damages: (db.prepare('SELECT * FROM damages WHERE revision_id = ?').all(r.id) as any[]).map((d) => ({
      copyId: d.copy_id,
      masterRange: [d.master_start_ms, d.master_end_ms],
      note: d.note,
    })),
  }));
  const activations = (db.prepare('SELECT * FROM activations ORDER BY tape_id').all() as any[]).map((a) => ({
    tapeId: a.tape_id,
    timelineVersion: a.timeline_version,
    durationMs: a.duration_ms,
    programRevisionId: a.program_revision_id,
    restorationRevisionId: a.restoration_revision_id,
  }));
  return { sources, revisions, activations };
}

const HELP = {
  endpoints: [
    'GET /tapes',
    'GET /tapes/:tape',
    'GET /tapes/:tape/programs',
    'GET /tapes/:tape/programs/:pid',
    'GET /programs/:pid',
    'GET /sync/status',
  ],
};

const server = http.createServer((req, res) => {
  try {
    const url = new URL(req.url ?? '/', 'http://localhost');
    const parts = url.pathname.split('/').filter(Boolean);

    if (url.pathname === '/' && req.method === 'GET') return send(res, 200, HELP);
    if (url.pathname === '/health' && req.method === 'GET') return send(res, 200, { ok: true });
    if (url.pathname === '/sync/status' && req.method === 'GET') return send(res, 200, syncStatus());

    if (parts[0] === 'tapes' && req.method === 'GET') {
      if (parts.length === 1) {
        const acts = (db.prepare('SELECT * FROM activations ORDER BY tape_id').all() as any[]).map((a) => ({
          tapeId: a.tape_id,
          timelineVersion: a.timeline_version,
          durationMs: a.duration_ms,
        }));
        return send(res, 200, { tapes: acts });
      }
      const tape = decodeURIComponent(parts[1]);
      if (parts.length === 2) {
        const v = tapeView(tape);
        return send(res, v.status, v.body);
      }
      if (parts.length === 3 && parts[2] === 'programs') {
        const act = activationInfo(tape);
        if (!act) return send(res, 404, { error: 'NO_ACTIVE_VERSION' });
        const programs = db
          .prepare('SELECT program_id, title, start_ms, end_ms FROM programs WHERE revision_id = ? ORDER BY start_ms')
          .all(act.program_rev_id) as any[];
        return send(res, 200, {
          tapeId: tape,
          timelineVersion: act.timeline_version,
          durationMs: act.duration_ms,
          programRevision: act.program_rev,
          programEdition: act.program_edition,
          restorationRevision: act.restoration_rev,
          restorationEdition: act.restoration_edition,
          programs: programs.map((p) => ({ id: p.program_id, title: p.title, masterRange: [p.start_ms, p.end_ms] })),
        });
      }
      if (parts.length === 4 && parts[2] === 'programs') {
        const v = programView(tape, decodeURIComponent(parts[3]));
        return send(res, v.status, v.body);
      }
    }

    if (parts[0] === 'programs' && parts.length === 2 && req.method === 'GET') {
      const pid = decodeURIComponent(parts[1]);
      const tapes = (db.prepare('SELECT tape_id FROM activations ORDER BY tape_id').all() as any[]).map((r) => r.tape_id);
      const results: any[] = [];
      for (const t of tapes) {
        const v = programView(t, pid);
        if (v.status === 200) results.push(v.body);
      }
      if (!results.length) return send(res, 404, { error: 'PROGRAM_NOT_FOUND' });
      return send(res, 200, { results });
    }

    send(res, 404, { error: 'NOT_FOUND' });
  } catch (e) {
    send(res, 500, { error: String(e) });
  }
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`[api] 查询接口已启动: http://127.0.0.1:${PORT}（数据库 ${DB_PATH}）`);
});
