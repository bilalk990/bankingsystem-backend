import { db, securityEventsTable, securityScansTable, usersTable } from "../configs/database";
import { eq, sql, desc, and, gte, count } from "drizzle-orm";
import type { Request } from "express";

export type EventType =
  | "login_success" | "login_failed" | "login_blocked"
  | "logout" | "session_expired"
  | "password_change" | "password_change_failed"
  | "account_locked" | "account_unlocked"
  | "role_change" | "permission_change" | "user_created" | "user_deactivated"
  | "data_export" | "pii_data_accessed" | "bulk_action" | "lead_delete"
  | "brute_force_detected" | "suspicious_ip" | "rapid_login_attempts" | "session_hijack_attempt";

export type Severity = "info" | "warning" | "critical" | "fatal";

export function extractClientInfo(req: Request) {
  const ipAddress = req.headers["x-forwarded-for"]?.toString().split(",")[0]?.trim()
    || req.ip || "unknown";
  const userAgent = req.headers["user-agent"] || "unknown";

  const parsed = parseUserAgent(userAgent);
  return { ipAddress, userAgent, ...parsed };
}

function parseUserAgent(ua: string) {
  let browser = "Unknown";
  let os = "Unknown";
  let device = "Desktop";

  if (ua.includes("Firefox")) browser = "Firefox";
  else if (ua.includes("Edg/")) browser = "Edge";
  else if (ua.includes("Chrome")) browser = "Chrome";
  else if (ua.includes("Safari")) browser = "Safari";

  if (ua.includes("Windows")) os = "Windows";
  else if (ua.includes("Mac OS")) os = "macOS";
  else if (ua.includes("Linux")) os = "Linux";
  else if (ua.includes("Android")) os = "Android";
  else if (ua.includes("iPhone") || ua.includes("iPad")) os = "iOS";

  if (ua.includes("Mobile") || ua.includes("Android")) device = "Mobile";
  else if (ua.includes("iPad") || ua.includes("Tablet")) device = "Tablet";

  return { browser, os, device };
}

export async function logSecurityEvent(
  eventType: EventType,
  severity: Severity,
  description: string,
  opts: {
    userId?: number;
    req?: Request;
    ipAddress?: string;
    userAgent?: string;
    metadata?: Record<string, any>;
  } = {}
) {
  try {
    const clientInfo = opts.req ? extractClientInfo(opts.req) : null;
    const ip = opts.ipAddress || clientInfo?.ipAddress || "unknown";
    const ua = opts.userAgent || clientInfo?.userAgent || "unknown";

    const meta = {
      ...opts.metadata,
      ...(clientInfo ? { browser: clientInfo.browser, os: clientInfo.os, device: clientInfo.device } : {}),
    };

    await db.insert(securityEventsTable).values({
      userId: opts.userId || null,
      eventType,
      severity,
      ipAddress: ip,
      userAgent: ua,
      description,
      metadata: meta,
    });
  } catch (e) {
    console.error("Failed to log security event:", e);
  }
}

export async function runSecurityScans() {
  const now = new Date();
  const results: Array<{ scanType: string; status: string; findings: any }> = [];

  const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);
  const fifteenMinAgo = new Date(now.getTime() - 15 * 60 * 1000);
  const oneDayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);

  const failedLogins = await db.select({
    ipAddress: securityEventsTable.ipAddress,
    cnt: count(),
  })
    .from(securityEventsTable)
    .where(and(
      eq(securityEventsTable.eventType, "login_failed"),
      gte(securityEventsTable.createdAt, fifteenMinAgo)
    ))
    .groupBy(securityEventsTable.ipAddress);

  const bruteForceIPs = failedLogins.filter(r => Number(r.cnt) >= 5);
  if (bruteForceIPs.length > 0) {
    results.push({
      scanType: "brute_force_detection",
      status: "critical",
      findings: {
        message: `${bruteForceIPs.length} IP(s) with 5+ failed logins in 15 minutes`,
        ips: bruteForceIPs.map(r => ({ ip: r.ipAddress, attempts: Number(r.cnt) })),
      },
    });
  } else {
    results.push({ scanType: "brute_force_detection", status: "clean", findings: { message: "No brute force attempts detected" } });
  }

  const lockedAccounts = await db.select({
    id: usersTable.id,
    email: usersTable.email,
    fullName: usersTable.fullName,
    lockedUntil: usersTable.lockedUntil,
  })
    .from(usersTable)
    .where(gte(usersTable.lockedUntil, now));

  if (lockedAccounts.length > 0) {
    results.push({
      scanType: "account_lockout_review",
      status: "warning",
      findings: {
        message: `${lockedAccounts.length} account(s) currently locked`,
        accounts: lockedAccounts.map(a => ({ id: a.id, email: a.email, name: a.fullName, lockedUntil: a.lockedUntil })),
      },
    });
  } else {
    results.push({ scanType: "account_lockout_review", status: "clean", findings: { message: "No locked accounts" } });
  }

  const expiredSessions = await db.select({ cnt: count() })
    .from(usersTable)
    .where(and(
      sql`${usersTable.sessionToken} IS NOT NULL`,
      sql`${usersTable.sessionExpiresAt} < NOW()`
    ));

  const expiredCount = Number(expiredSessions[0]?.cnt || 0);
  if (expiredCount > 0) {
    results.push({
      scanType: "stale_sessions",
      status: "warning",
      findings: { message: `${expiredCount} expired session(s) not cleaned up`, count: expiredCount },
    });
  } else {
    results.push({ scanType: "stale_sessions", status: "clean", findings: { message: "No stale sessions" } });
  }

  const criticalEvents = await db.select({ cnt: count() })
    .from(securityEventsTable)
    .where(and(
      sql`${securityEventsTable.severity} IN ('critical', 'fatal')`,
      gte(securityEventsTable.createdAt, oneDayAgo)
    ));

  const criticalCount = Number(criticalEvents[0]?.cnt || 0);
  if (criticalCount > 0) {
    results.push({
      scanType: "critical_event_review",
      status: "critical",
      findings: { message: `${criticalCount} critical/fatal event(s) in last 24 hours`, count: criticalCount },
    });
  } else {
    results.push({ scanType: "critical_event_review", status: "clean", findings: { message: "No critical events in last 24 hours" } });
  }

  const userLoginIps = await db.select({
    userId: securityEventsTable.userId,
    ipAddress: securityEventsTable.ipAddress,
  })
    .from(securityEventsTable)
    .where(and(
      eq(securityEventsTable.eventType, "login_success"),
      gte(securityEventsTable.createdAt, oneDayAgo)
    ));

  const userIpMap = new Map<number, Set<string>>();
  for (const row of userLoginIps) {
    if (!row.userId || !row.ipAddress) continue;
    if (!userIpMap.has(row.userId)) userIpMap.set(row.userId, new Set());
    userIpMap.get(row.userId)!.add(row.ipAddress);
  }

  const multiIpUsers = Array.from(userIpMap.entries())
    .filter(([_, ips]) => ips.size >= 3)
    .map(([userId, ips]) => ({ userId, ipCount: ips.size, ips: Array.from(ips) }));

  if (multiIpUsers.length > 0) {
    results.push({
      scanType: "multi_ip_login",
      status: "warning",
      findings: {
        message: `${multiIpUsers.length} user(s) logged in from 3+ different IPs today`,
        users: multiIpUsers,
      },
    });
  } else {
    results.push({ scanType: "multi_ip_login", status: "clean", findings: { message: "No suspicious multi-IP logins" } });
  }

  const inactiveWithSessions = await db.select({
    id: usersTable.id,
    email: usersTable.email,
  })
    .from(usersTable)
    .where(and(
      eq(usersTable.active, false),
      sql`${usersTable.sessionToken} IS NOT NULL`
    ));

  if (inactiveWithSessions.length > 0) {
    results.push({
      scanType: "inactive_active_sessions",
      status: "critical",
      findings: {
        message: `${inactiveWithSessions.length} deactivated user(s) still have active sessions`,
        users: inactiveWithSessions.map(u => ({ id: u.id, email: u.email })),
      },
    });
  } else {
    results.push({ scanType: "inactive_active_sessions", status: "clean", findings: { message: "No deactivated users with active sessions" } });
  }

  for (const scan of results) {
    await db.insert(securityScansTable).values({
      scanType: scan.scanType,
      status: scan.status,
      findings: scan.findings,
    }).catch(() => {});
  }

  return results;
}
