import { type Request, type Response, type NextFunction } from "express";
import { db, pool, usersTable, rolePermissionsTable, leadsTable, dealsTable } from "../configs/database";
import { eq, and } from "drizzle-orm";
import crypto from "crypto";

const PASSWORD_EXPIRY_DAYS = 90;
const INACTIVITY_TIMEOUT_MS = 60 * 60 * 1000;

export function generateFingerprint(req: Request): string {
  const ua = req.headers["user-agent"] || "";
  const accept = req.headers["accept-language"] || "";
  const raw = `${ua}|${accept}`;
  return crypto.createHash("sha256").update(raw).digest("hex").slice(0, 32);
}

function mapUserRow(row: any) {
  return {
    id: row.id,
    email: row.email,
    password: row.password,
    fullName: row.full_name,
    role: row.role,
    phone: row.phone,
    active: row.active,
    commissionPct: row.commission_pct,
    lastSeenAt: row.last_seen_at,
    canUnderwrite: row.can_underwrite,
    canDistributeLeads: row.can_distribute_leads,
    canManageDeals: row.can_manage_deals,
    canViewAllLeads: row.can_view_all_leads,
    canImport: row.can_import,
    canManageFunders: row.can_manage_funders,
    canSendMessages: row.can_send_messages,
    canAccessAnalytics: row.can_access_analytics,
    twoFactorEnabled: row.two_factor_enabled,
    twoFactorPhone: row.two_factor_phone,
    lastLoginIp: row.last_login_ip,
    lastLoginAt: row.last_login_at,
    failedLoginAttempts: row.failed_login_attempts,
    lockedUntil: row.locked_until,
    sessionToken: row.session_token,
    sessionExpiresAt: row.session_expires_at,
    sessionFingerprint: row.session_fingerprint,
    passwordChangedAt: row.password_changed_at,
    mustChangePassword: row.must_change_password,
    createdAt: row.created_at,
  };
}

export async function requireAuth(req: Request, res: Response, next: NextFunction): Promise<void> {
  let userId: string | undefined;
  let sessionToken: string | undefined;

  // Try to get from cookies first
  userId = req.cookies?.userId;
  sessionToken = req.cookies?.sessionToken;

  // Fallback: Check Authorization header
  if (!sessionToken) {
    const authHeader = req.headers.authorization;
    if (authHeader && authHeader.startsWith('Bearer ')) {
      sessionToken = authHeader.substring(7);
      // Extract userId from query or header
      userId = req.headers['x-user-id'] as string;
    }
  }

  if (!userId || !sessionToken) {
    const cookieKeys = req.cookies ? Object.keys(req.cookies) : [];
    console.log(`[Auth Middleware] 401 Unauthorized for ${req.method} ${req.path}`);
    console.log(`[Auth Middleware] Missing credentials: userId=${!!userId}, sessionToken=${!!sessionToken}`);
    console.log(`[Auth Middleware] Available cookies: ${cookieKeys.join(", ") || "none"}`);
    console.log(`[Auth Middleware] Authorization header: ${req.headers.authorization ? 'present' : 'missing'}`);
    res.status(401).json({ error: "Not authenticated" });
    return;
  }

  const parsedId = parseInt(userId, 10);
  if (isNaN(parsedId)) {
    res.status(401).json({ error: "Not authenticated" });
    return;
  }

  const result = await pool.query(`SELECT * FROM users WHERE id = $1 LIMIT 1`, [parsedId]);
  const user = result.rows[0] ? mapUserRow(result.rows[0]) : null;
  if (!user) {
    console.log(`[Auth] 401 User not found in DB for ID: ${parsedId}`);
    res.status(401).json({ error: "Not authenticated" });
    return;
  }
  if (!user.active) {
    console.log(`[Auth] 401 User deactivated: ${user.email}`);
    res.status(401).json({ error: "Not authenticated" });
    return;
  }

  if (!user.sessionToken || user.sessionToken !== sessionToken) {
    console.log(`[Auth] 401 Session token mismatch for ${user.email}. Provided: ${sessionToken?.slice(0, 8)}... Expected: ${user.sessionToken?.slice(0, 8)}...`);
    res.status(401).json({ error: "Session expired. Please log in again." });
    return;
  }

  if (user.sessionExpiresAt && new Date(user.sessionExpiresAt) < new Date()) {
    res.status(401).json({ error: "Session expired. Please log in again." });
    return;
  }

  if (user.sessionFingerprint) {
    const currentFingerprint = generateFingerprint(req);
    if (user.sessionFingerprint !== currentFingerprint) {
      const { logSecurityEvent } = await import("../utils/security");
      await logSecurityEvent("session_hijack_attempt", "fatal",
        `Session fingerprint mismatch for ${user.email} — possible session hijacking`, {
          userId: user.id, req,
          metadata: { expectedFingerprint: user.sessionFingerprint.slice(0, 8) + "...", currentFingerprint: currentFingerprint.slice(0, 8) + "..." },
        });
      await pool.query(`UPDATE users SET session_token = NULL, session_expires_at = NULL, session_fingerprint = NULL WHERE id = $1`, [user.id]);
      res.status(401).json({ error: "Session invalidated — security anomaly detected. Please log in again." });
      return;
    }
  }

  if (user.lastSeenAt) {
    const idleTime = Date.now() - new Date(user.lastSeenAt).getTime();
    if (idleTime > INACTIVITY_TIMEOUT_MS) {
      await pool.query(`UPDATE users SET session_token = NULL, session_expires_at = NULL, session_fingerprint = NULL WHERE id = $1`, [user.id]);
      res.status(401).json({ error: "Session timed out due to inactivity. Please log in again.", code: "INACTIVITY_TIMEOUT" });
      return;
    }
  }

  const isExemptRoute = req.path === "/auth/change-password" || req.path === "/auth/me" || req.path === "/auth/logout" || req.path === "/auth/security-questions" || (req.method === "GET" && req.path === `/role-permissions/${user.role}`);
  if (!isExemptRoute && user.mustChangePassword) {
    res.status(403).json({ error: "Password change required before continuing.", code: "MUST_CHANGE_PASSWORD" });
    return;
  }

  if (!isExemptRoute) {
    if (!user.passwordChangedAt) {
      res.status(403).json({ error: "Your password has expired. Please change it to continue.", code: "PASSWORD_EXPIRED" });
      return;
    }
    const daysSinceChange = (Date.now() - new Date(user.passwordChangedAt).getTime()) / (1000 * 60 * 60 * 24);
    if (daysSinceChange > PASSWORD_EXPIRY_DAYS) {
      res.status(403).json({ error: "Your password has expired. Please change it to continue.", code: "PASSWORD_EXPIRED" });
      return;
    }
  }

  pool.query(`UPDATE users SET last_seen_at = $1 WHERE id = $2`, [new Date(), user.id]).catch(() => {});

  (req as any).user = user;
  next();
}

export async function requireAdmin(req: Request, res: Response, next: NextFunction): Promise<void> {
  const user = (req as any).user;
  if (!user || (user.role !== "admin" && user.role !== "super_admin" && user.role !== "manager")) {
    res.status(403).json({ error: "Admin access required" });
    return;
  }
  next();
}

export async function requireSuperAdmin(req: Request, res: Response, next: NextFunction): Promise<void> {
  const user = (req as any).user;
  if (!user || user.role !== "super_admin") {
    res.status(403).json({ error: "Super admin access required" });
    return;
  }
  next();
}

export function requirePermission(permission: string) {
  return async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    const user = (req as any).user;
    if (!user) {
      res.status(401).json({ error: "Not authenticated" });
      return;
    }
    if (user.role === "super_admin") {
      next();
      return;
    }
    const permResult = await pool.query(
      `SELECT * FROM role_permissions WHERE role = $1 AND permission = $2 LIMIT 1`,
      [user.role, permission]
    );
    const perm = permResult.rows[0];
    if (!perm || !perm.enabled) {
      res.status(403).json({ error: "Permission denied" });
      return;
    }
    next();
  };
}

export async function checkLeadOwnership(leadId: number, user: any, res: Response): Promise<boolean> {
  const leadResult = await pool.query(`SELECT id, assigned_to_id FROM leads WHERE id = $1 LIMIT 1`, [leadId]);
  const lead = leadResult.rows[0];
  if (!lead) {
    res.status(404).json({ error: "Lead not found" });
    return false;
  }
  if (user.role === "rep" && lead.assigned_to_id !== user.id) {
    res.status(403).json({ error: "Access denied" });
    return false;
  }
  return true;
}

export async function checkDealOwnership(dealId: number, user: any, res: Response): Promise<boolean> {
  const dealResult = await pool.query(`SELECT id, rep_id FROM deals WHERE id = $1 LIMIT 1`, [dealId]);
  const deal = dealResult.rows[0];
  if (!deal) {
    res.status(404).json({ error: "Deal not found" });
    return false;
  }
  if (user.role === "rep" && deal.rep_id !== user.id) {
    res.status(403).json({ error: "Access denied" });
    return false;
  }
  return true;
}
