import { pool } from "../configs/database";
import { logSecurityEvent } from "./security";
import type { Request } from "express";

interface UserActivity {
  uniqueLeadIds: Set<number>;
  searchCount: number;
  listRequestCount: number;
  windowStart: number;
  leadViewTimestamps: number[];
}

const unlockAttempts = new Map<number, { count: number; lastAttempt: number }>();
const MAX_UNLOCK_ATTEMPTS = 5;
const UNLOCK_COOLDOWN_MS = 5 * 60 * 1000;

const activityMap = new Map<number, UserActivity>();
const lockedUsers = new Set<number>();

const WINDOW_MS = 5 * 60 * 1000;
const UNIQUE_LEADS_WARN = 40;
const UNIQUE_LEADS_LOCK = 60;
const SEARCH_BURST_LOCK = 30;
const RAPID_VIEW_COUNT = 20;
const RAPID_VIEW_WINDOW_MS = 60 * 1000;

function getOrCreateActivity(userId: number): UserActivity {
  const now = Date.now();
  let activity = activityMap.get(userId);

  if (!activity || now - activity.windowStart > WINDOW_MS) {
    activity = {
      uniqueLeadIds: new Set(),
      searchCount: 0,
      listRequestCount: 0,
      windowStart: now,
      leadViewTimestamps: [],
    };
    activityMap.set(userId, activity);
  }

  return activity;
}

export function isUserLocked(userId: number): boolean {
  return lockedUsers.has(userId);
}

export function unlockUser(userId: number): void {
  lockedUsers.delete(userId);
  activityMap.delete(userId);
  unlockAttempts.delete(userId);
}

export function checkUnlockAttempts(userId: number): { allowed: boolean; remainingAttempts: number; cooldownUntil?: number } {
  const entry = unlockAttempts.get(userId);
  if (!entry) return { allowed: true, remainingAttempts: MAX_UNLOCK_ATTEMPTS };

  const now = Date.now();
  if (entry.count >= MAX_UNLOCK_ATTEMPTS) {
    const cooldownEnd = entry.lastAttempt + UNLOCK_COOLDOWN_MS;
    if (now < cooldownEnd) {
      return { allowed: false, remainingAttempts: 0, cooldownUntil: cooldownEnd };
    }
    unlockAttempts.delete(userId);
    return { allowed: true, remainingAttempts: MAX_UNLOCK_ATTEMPTS };
  }

  return { allowed: true, remainingAttempts: MAX_UNLOCK_ATTEMPTS - entry.count };
}

export function recordUnlockAttempt(userId: number): void {
  const entry = unlockAttempts.get(userId) || { count: 0, lastAttempt: 0 };
  entry.count++;
  entry.lastAttempt = Date.now();
  unlockAttempts.set(userId, entry);
}

export async function trackLeadView(userId: number, leadId: number, req: Request): Promise<"ok" | "locked"> {
  if (lockedUsers.has(userId)) return "locked";

  const activity = getOrCreateActivity(userId);
  const now = Date.now();

  activity.uniqueLeadIds.add(leadId);
  activity.leadViewTimestamps.push(now);

  activity.leadViewTimestamps = activity.leadViewTimestamps.filter(
    (t) => now - t < RAPID_VIEW_WINDOW_MS
  );

  const uniqueCount = activity.uniqueLeadIds.size;
  const rapidViewCount = activity.leadViewTimestamps.length;

  const isRapidFire = rapidViewCount >= RAPID_VIEW_COUNT;

  if (uniqueCount >= UNIQUE_LEADS_LOCK || (uniqueCount >= UNIQUE_LEADS_WARN && isRapidFire)) {
    lockedUsers.add(userId);
    await logSecurityEvent("data_export" as any, "critical",
      `Suspicious lead access pattern detected: ${uniqueCount} unique leads in ${Math.round((now - activity.windowStart) / 1000)}s, ${rapidViewCount} views in last 60s`, {
      userId, req,
      metadata: {
        uniqueLeadsViewed: uniqueCount,
        rapidViewCount,
        windowSeconds: Math.round((now - activity.windowStart) / 1000),
        trigger: uniqueCount >= UNIQUE_LEADS_LOCK ? "unique_lead_threshold" : "rapid_access_pattern",
      },
    });
    return "locked";
  }

  return "ok";
}

const BULK_LIST_LOCK = 15;

export async function trackBulkList(userId: number, resultCount: number, req: Request): Promise<"ok" | "locked"> {
  if (lockedUsers.has(userId)) return "locked";

  const activity = getOrCreateActivity(userId);
  activity.listRequestCount++;

  if (activity.listRequestCount >= BULK_LIST_LOCK) {
    lockedUsers.add(userId);
    await logSecurityEvent("data_export" as any, "critical",
      `Suspicious bulk listing pattern detected: ${activity.listRequestCount} list requests in ${Math.round((Date.now() - activity.windowStart) / 1000)}s`, {
      userId, req,
      metadata: {
        listRequestCount: activity.listRequestCount,
        windowSeconds: Math.round((Date.now() - activity.windowStart) / 1000),
        trigger: "bulk_list_pagination",
      },
    });
    return "locked";
  }

  return "ok";
}

export async function trackSearchQuery(userId: number, req: Request): Promise<"ok" | "locked"> {
  if (lockedUsers.has(userId)) return "locked";

  const activity = getOrCreateActivity(userId);
  activity.searchCount++;

  if (activity.searchCount >= SEARCH_BURST_LOCK) {
    lockedUsers.add(userId);
    await logSecurityEvent("data_export" as any, "critical",
      `Suspicious search pattern detected: ${activity.searchCount} searches in ${Math.round((Date.now() - activity.windowStart) / 1000)}s`, {
      userId, req,
      metadata: {
        searchCount: activity.searchCount,
        windowSeconds: Math.round((Date.now() - activity.windowStart) / 1000),
        trigger: "search_burst",
      },
    });
    return "locked";
  }

  return "ok";
}

setInterval(() => {
  const now = Date.now();
  for (const [userId, activity] of activityMap.entries()) {
    if (now - activity.windowStart > WINDOW_MS * 2 && !lockedUsers.has(userId)) {
      activityMap.delete(userId);
    }
  }
}, 60 * 1000);
