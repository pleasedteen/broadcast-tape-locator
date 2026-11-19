/**
 * 共享类型定义。
 *
 * 领域模型：
 * - 母带（tape）有若干时间轴版次（timelineVersion），每个版次有时长。
 * - 节目单（program 清单）：某版次下，节目在母带时间轴上的时间段。每次修订给出整盘母带的完整节目单。
 * - 修复清单（restoration 清单）：某版次下，母带区段在各数字副本中的位置。
 *   修复时剪掉损坏录音（damage 记录），也会分段变速（segment.rate 段内恒定）。
 * - 两类清单独立修订，每条修订都标明使用的母带时间轴版次。
 */

export type Kind = 'program' | 'restoration';

/** 节目记录：节目在母带时间轴上的范围 */
export interface ProgramRec {
  type: 'program';
  programId: string;
  title: string;
  startMs: number;
  endMs: number;
}

/**
 * 区段记录：母带 [masterStartMs, masterEndMs) 在副本 copyId 中从 copyStartMs 开始。
 * rate 为该段播放速度（段内恒定）：副本时长 = 母带时长 / rate。
 * rate=1 原速；rate=1.5 表示 1.5 倍速（副本中更短）。
 */
export interface SegmentRec {
  type: 'segment';
  copyId: string;
  masterStartMs: number;
  masterEndMs: number;
  copyStartMs: number;
  rate: number;
}

/** 损坏记录：修复时被剪掉的母带区段（解释副本缺失的原因） */
export interface DamageRec {
  type: 'damage';
  copyId: string;
  masterStartMs: number;
  masterEndMs: number;
  note: string;
}

export type Rec = ProgramRec | SegmentRec | DamageRec;

/** 站点发布的一整盘母带、某一类清单的一次完整修订 */
export interface ManifestSeed {
  kind: Kind;
  timelineVersion: number;
  revision: number;
  pageSize: number;
  records: Rec[];
}
