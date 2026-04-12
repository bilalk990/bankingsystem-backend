import { Router, type IRouter } from "express";
import { db, securityEventsTable, securityScansTable, usersTable } from "../configs/database";
import { eq, sql, desc, and, gte, or, count } from "drizzle-orm";
import { requireAuth, requireSuperAdmin } from "../middlewares/authMiddleware";
import { runSecurityScans } from "../utils/security";

const router: IRouter = Router();

router.get("/security/dashboard", requireAuth, requireSuperAdmin, async (req, res): Promise<void> => {
  const now = new Date();
  const oneDayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);
  const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);

  const [
    todayEventCounts,
    criticalAlerts,
    recentLogins,
    activeUsers,
    lockedAccounts,
    weeklyTrend,
  ] = await Promise.all([
    db.select({
      eventType: securityEventsTable.eventType,
      cnt: count(),
    })
      .from(securityEventsTable)
      .where(gte(securityEventsTable.createdAt, oneDayAgo))
      .groupBy(securityEventsTable.eventType),

    db.select()
      .from(securityEventsTable)
      .where(and(
        or(eq(securityEventsTable.severity, "critical"), eq(securityEventsTable.severity, "fatal")),
        gte(securityEventsTable.createdAt, sevenDaysAgo)
      ))
      .orderBy(desc(securityEventsTable.createdAt))
      .limit(20),

    db.select({
      id: securityEventsTable.id,
      userId: securityEventsTable.userId,
      ipAddress: securityEventsTable.ipAddress,
      userAgent: securityEventsTable.userAgent,
      metadata: securityEventsTable.metadata,
      createdAt: securityEventsTable.createdAt,
      eventType: securityEventsTable.eventType,
      userName: usersTable.fullName,
      userEmail: usersTable.email,
    })
      .from(securityEventsTable)
      .leftJoin(usersTable, eq(securityEventsTable.userId, usersTable.id))
      .where(and(
        or(eq(securityEventsTable.eventType, "login_success"), eq(securityEventsTable.eventType, "login_failed")),
        gte(securityEventsTable.createdAt, sevenDaysAgo)
      ))
      .orderBy(desc(securityEventsTable.createdAt))
      .limit(50),

    db.select({ cnt: count() })
      .from(usersTable)
      .where(and(
        eq(usersTable.active, true),
        sql`${usersTable.sessionToken} IS NOT NULL`,
        gte(usersTable.sessionExpiresAt, now)
      )),

    db.select({ cnt: count() })
      .from(usersTable)
      .where(gte(usersTable.lockedUntil, now)),

    db.select({
      day: sql<string>`DATE(${securityEventsTable.createdAt})`,
      eventType: securityEventsTable.eventType,
      cnt: count(),
    })
      .from(securityEventsTable)
      .where(gte(securityEventsTable.createdAt, sevenDaysAgo))
      .groupBy(sql`DATE(${securityEventsTable.createdAt})`, securityEventsTable.eventType)
      .orderBy(sql`DATE(${securityEventsTable.createdAt})`),
  ]);

  const totalToday = todayEventCounts.reduce((sum, r) => sum + Number(r.cnt), 0);
  const failedToday = Number(todayEventCounts.find(r => r.eventType === "login_failed")?.cnt || 0);
  const successToday = Number(todayEventCounts.find(r => r.eventType === "login_success")?.cnt || 0);

  let overallStatus: "secure" | "warning" | "critical" = "secure";
  if (criticalAlerts.length > 0) overallStatus = "critical";
  else if (failedToday > 10 || Number(lockedAccounts[0]?.cnt || 0) > 0) overallStatus = "warning";

  res.json({
    overallStatus,
    stats: {
      totalEventsToday: totalToday,
      successfulLoginsToday: successToday,
      failedLoginsToday: failedToday,
      activeSessionCount: Number(activeUsers[0]?.cnt || 0),
      lockedAccountCount: Number(lockedAccounts[0]?.cnt || 0),
      criticalAlertCount: criticalAlerts.length,
    },
    criticalAlerts: criticalAlerts.map(a => ({
      id: a.id,
      eventType: a.eventType,
      severity: a.severity,
      description: a.description,
      ipAddress: a.ipAddress,
      metadata: a.metadata,
      createdAt: a.createdAt,
    })),
    recentLogins,
    weeklyTrend,
  });
});

router.get("/security/events", requireAuth, requireSuperAdmin, async (req, res): Promise<void> => {
  const page = Math.max(1, parseInt(req.query.page as string) || 1);
  const limit = Math.min(100, parseInt(req.query.limit as string) || 50);
  const offset = (page - 1) * limit;
  const severity = req.query.severity as string;
  const eventType = req.query.eventType as string;
  const userId = req.query.userId ? parseInt(req.query.userId as string) : null;

  const conditions: any[] = [];
  if (severity) conditions.push(eq(securityEventsTable.severity, severity));
  if (eventType) conditions.push(eq(securityEventsTable.eventType, eventType));
  if (userId) conditions.push(eq(securityEventsTable.userId, userId));

  const where = conditions.length > 0 ? and(...conditions) : undefined;

  const [events, totalResult] = await Promise.all([
    db.select({
      id: securityEventsTable.id,
      userId: securityEventsTable.userId,
      eventType: securityEventsTable.eventType,
      severity: securityEventsTable.severity,
      ipAddress: securityEventsTable.ipAddress,
      userAgent: securityEventsTable.userAgent,
      description: securityEventsTable.description,
      metadata: securityEventsTable.metadata,
      createdAt: securityEventsTable.createdAt,
      userName: usersTable.fullName,
      userEmail: usersTable.email,
    })
      .from(securityEventsTable)
      .leftJoin(usersTable, eq(securityEventsTable.userId, usersTable.id))
      .where(where)
      .orderBy(desc(securityEventsTable.createdAt))
      .limit(limit)
      .offset(offset),

    db.select({ cnt: count() })
      .from(securityEventsTable)
      .where(where),
  ]);

  res.json({
    events,
    pagination: {
      page,
      limit,
      total: Number(totalResult[0]?.cnt || 0),
      totalPages: Math.ceil(Number(totalResult[0]?.cnt || 0) / limit),
    },
  });
});

router.post("/security/scan", requireAuth, requireSuperAdmin, async (req, res): Promise<void> => {
  const results = await runSecurityScans();
  res.json({ results, timestamp: new Date().toISOString() });
});

router.get("/security/scans", requireAuth, requireSuperAdmin, async (req, res): Promise<void> => {
  const limit = Math.min(50, parseInt(req.query.limit as string) || 20);

  const scans = await db.select()
    .from(securityScansTable)
    .orderBy(desc(securityScansTable.createdAt))
    .limit(limit);

  res.json({ scans });
});

router.get("/security/user-sessions", requireAuth, requireSuperAdmin, async (req, res): Promise<void> => {
  const users = await db.select({
    id: usersTable.id,
    email: usersTable.email,
    fullName: usersTable.fullName,
    role: usersTable.role,
    active: usersTable.active,
    lastLoginIp: usersTable.lastLoginIp,
    lastLoginAt: usersTable.lastLoginAt,
    lastSeenAt: usersTable.lastSeenAt,
    sessionExpiresAt: usersTable.sessionExpiresAt,
    failedLoginAttempts: usersTable.failedLoginAttempts,
    lockedUntil: usersTable.lockedUntil,
    hasActiveSession: sql<boolean>`${usersTable.sessionToken} IS NOT NULL AND ${usersTable.sessionExpiresAt} > NOW()`,
    twoFactorEnabled: usersTable.twoFactorEnabled,
    passwordChangedAt: usersTable.passwordChangedAt,
    createdAt: usersTable.createdAt,
  })
    .from(usersTable)
    .orderBy(desc(usersTable.lastLoginAt));

  res.json({ users });
});

router.get("/security/user/:userId/history", requireAuth, requireSuperAdmin, async (req, res): Promise<void> => {
  const userId = parseInt(req.params.userId);
  if (isNaN(userId)) { res.status(400).json({ error: "Invalid user ID" }); return; }

  const events = await db.select()
    .from(securityEventsTable)
    .where(eq(securityEventsTable.userId, userId))
    .orderBy(desc(securityEventsTable.createdAt))
    .limit(100);

  const uniqueIps = [...new Set(events.filter(e => e.ipAddress).map(e => e.ipAddress))];

  res.json({ events, uniqueIps, totalEvents: events.length });
});

router.post("/security/force-logout/:userId", requireAuth, requireSuperAdmin, async (req, res): Promise<void> => {
  const userId = parseInt(req.params.userId);
  if (isNaN(userId)) { res.status(400).json({ error: "Invalid user ID" }); return; }

  await db.update(usersTable).set({
    sessionToken: null,
    sessionExpiresAt: null,
  }).where(eq(usersTable.id, userId));

  const { logSecurityEvent, extractClientInfo } = await import("../utils/security");
  await logSecurityEvent("logout", "warning", `Force logout by admin for user #${userId}`, {
    userId: (req as any).user.id,
    req,
    metadata: { targetUserId: userId, action: "force_logout" },
  });

  res.json({ message: "User session terminated" });
});

export default router;
