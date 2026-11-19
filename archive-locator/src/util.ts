import crypto from 'node:crypto';

export function sha256(s: string): string {
  return crypto.createHash('sha256').update(s).digest('hex');
}

export function randomToken(): string {
  return crypto.randomBytes(8).toString('hex');
}

export function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

export interface HttpResult {
  status: number; // 0 表示网络层失败（如响应丢失 / 连接被重置）
  body: any;
}

/** 简单 JSON HTTP 客户端；网络异常归一为 status=0，便于采集器统一走"响应丢失"恢复路径 */
export async function httpReq(method: string, url: string, body?: unknown, timeoutMs = 3000): Promise<HttpResult> {
  try {
    const res = await fetch(url, {
      method,
      headers: body !== undefined ? { 'content-type': 'application/json' } : undefined,
      body: body !== undefined ? JSON.stringify(body) : undefined,
      signal: AbortSignal.timeout(timeoutMs),
    });
    const text = await res.text();
    let parsed: any = null;
    try {
      parsed = text ? JSON.parse(text) : null;
    } catch {
      parsed = { raw: text };
    }
    return { status: res.status, body: parsed };
  } catch (e: any) {
    return { status: 0, body: { error: String(e?.message ?? e) } };
  }
}

/** 毫秒 → hh:mm:ss（用于可读输出） */
export function fmtMs(ms: number): string {
  const neg = ms < 0;
  if (neg) ms = -ms;
  const s = Math.floor(ms / 1000);
  const p = (n: number) => String(n).padStart(2, '0');
  return `${neg ? '-' : ''}${p(Math.floor(s / 3600))}:${p(Math.floor((s % 3600) / 60))}:${p(s % 60)}`;
}
