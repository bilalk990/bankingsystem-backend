import { Router, type IRouter } from "express";
import { eq, and, desc, sql } from "drizzle-orm";
import { db, pool, usersTable, passwordResetRequestsTable, securityQuestionsTable } from "../configs/database";
import { LoginBody, LoginResponse, GetMeResponse } from "../validationSchemas";
import { requireAuth, requireAdmin, requireSuperAdmin, generateFingerprint } from "../middlewares/authMiddleware";
import { logSecurityEvent } from "../utils/security";
import { isUserLocked, unlockUser, checkUnlockAttempts, recordUnlockAttempt } from "../utils/activityGuard";
import bcrypt from "bcryptjs";
import crypto from "crypto";

const PASSWORD_MIN_LENGTH = 12;
const PASSWORD_RULES = [
  { regex: /[A-Z]/, message: "at least one uppercase letter" },
  { regex: /[a-z]/, message: "at least one lowercase letter" },
  { regex: /[0-9]/, message: "at least one number" },
  { regex: /[!@#$%^&*()_+\-=\[\]{};':"\\|,.<>\/?`~]/, message: "at least one special character" },
];

function validatePasswordStrength(password: string): string | null {
  if (password.length < PASSWORD_MIN_LENGTH) {
    return `Password must be at least ${PASSWORD_MIN_LENGTH} characters`;
  }
  const missing = PASSWORD_RULES.filter(r => !r.regex.test(password)).map(r => r.message);
  if (missing.length > 0) {
    return `Password must contain ${missing.join(", ")}`;
  }
  return null;
}

const router: IRouter = Router();

const SESSION_DURATION_MS = 24 * 60 * 60 * 1000;

const LOCKOUT_TIERS = [
  { threshold: 5, durationMs: 30 * 1000, label: "30 seconds" },
  { threshold: 7, durationMs: 5 * 60 * 1000, label: "5 minutes" },
  { threshold: 9, durationMs: 30 * 60 * 1000, label: "30 minutes" },
  { threshold: 11, durationMs: 60 * 60 * 1000, label: "1 hour" },
];

const CHALLENGE_THRESHOLD = 3;

const challengeStore = new Map<string, { a: number; b: number; answer: number; expiresAt: number; email: string }>();

function generateSessionToken(): string {
  return crypto.randomBytes(48).toString("hex");
}

function getClientIp(req: any): string {
  return req.headers["x-forwarded-for"]?.split(",")[0]?.trim() || req.ip || "unknown";
}

function generateChallenge(email: string): { question: string; challengeId: string } {
  const a = Math.floor(Math.random() * 20) + 5;
  const b = Math.floor(Math.random() * 15) + 3;
  const challengeId = crypto.randomBytes(16).toString("hex");
  challengeStore.set(challengeId, { a, b, answer: a + b, expiresAt: Date.now() + 5 * 60 * 1000, email: email.toLowerCase() });
  for (const [key, val] of challengeStore) {
    if (val.expiresAt < Date.now()) challengeStore.delete(key);
  }
  return { question: `What is ${a} + ${b}?`, challengeId };
}

function verifyChallenge(challengeId: string, answer: number, email: string): boolean {
  const challenge = challengeStore.get(challengeId);
  if (!challenge) return false;
  if (challenge.expiresAt < Date.now()) {
    challengeStore.delete(challengeId);
    return false;
  }
  if (challenge.email !== email.toLowerCase()) {
    return false;
  }
  const valid = challenge.answer === answer;
  challengeStore.delete(challengeId);
  return valid;
}

function getLockoutDuration(attempts: number): { durationMs: number; label: string } | null {
  let result: { durationMs: number; label: string } | null = null;
  for (const tier of LOCKOUT_TIERS) {
    if (attempts >= tier.threshold) {
      result = { durationMs: tier.durationMs, label: tier.label };
    }
  }
  return result;
}

router.post("/auth/login", async (req, res): Promise<void> => {
  console.log("[LOGIN] Attempt received for:", req.body?.email);
  const parsed = LoginBody.safeParse(req.body);
  if (!parsed.success) {
    console.log("[LOGIN] Body parse failed:", parsed.error.message);
    res.status(400).json({ error: parsed.error.message });
    return;
  }

  const userResult = await pool.query(
    `SELECT id, email, password, full_name, role, phone, active, commission_pct,
            last_seen_at, can_underwrite, can_distribute_leads, can_manage_deals,
            can_view_all_leads, can_import, can_manage_funders, can_send_messages,
            can_access_analytics, two_factor_enabled, two_factor_phone,
            last_login_ip, last_login_at, failed_login_attempts, locked_until,
            session_token, session_expires_at, session_fingerprint,
            password_changed_at, must_change_password, invite_expires_at, created_at
     FROM users WHERE email = $1 LIMIT 1`,
    [parsed.data.email]
  );
  const userRow = userResult.rows[0];
  const user = userRow ? {
    id: userRow.id,
    email: userRow.email,
    password: userRow.password,
    fullName: userRow.full_name,
    role: userRow.role,
    phone: userRow.phone,
    active: userRow.active,
    commissionPct: userRow.commission_pct,
    lastSeenAt: userRow.last_seen_at,
    canUnderwrite: userRow.can_underwrite,
    canDistributeLeads: userRow.can_distribute_leads,
    canManageDeals: userRow.can_manage_deals,
    canViewAllLeads: userRow.can_view_all_leads,
    canImport: userRow.can_import,
    canManageFunders: userRow.can_manage_funders,
    canSendMessages: userRow.can_send_messages,
    canAccessAnalytics: userRow.can_access_analytics,
    twoFactorEnabled: userRow.two_factor_enabled,
    twoFactorPhone: userRow.two_factor_phone,
    lastLoginIp: userRow.last_login_ip,
    lastLoginAt: userRow.last_login_at,
    failedLoginAttempts: userRow.failed_login_attempts,
    lockedUntil: userRow.locked_until,
    sessionToken: userRow.session_token,
    sessionExpiresAt: userRow.session_expires_at,
    sessionFingerprint: userRow.session_fingerprint,
    passwordChangedAt: userRow.password_changed_at,
    mustChangePassword: userRow.must_change_password,
    inviteExpiresAt: userRow.invite_expires_at,
    createdAt: userRow.created_at,
  } : null;
  console.log("[LOGIN] User query result:", user ? `found id=${user.id}` : "not found");
  if (!user) {
    await logSecurityEvent("login_failed", "warning", `Failed login attempt for unknown email: ${parsed.data.email}`, {
      req, metadata: { email: parsed.data.email, reason: "unknown_email" },
    });
    res.status(401).json({ error: "Invalid email or password" });
    return;
  }

  if (user.lockedUntil && new Date(user.lockedUntil) > new Date()) {
    const secondsLeft = Math.ceil((new Date(user.lockedUntil).getTime() - Date.now()) / 1000);
    const timeLabel = secondsLeft > 60
      ? `${Math.ceil(secondsLeft / 60)} minute${Math.ceil(secondsLeft / 60) > 1 ? "s" : ""}`
      : `${secondsLeft} second${secondsLeft > 1 ? "s" : ""}`;
    await logSecurityEvent("login_blocked", "critical", `Login blocked for locked account: ${user.email}`, {
      userId: user.id, req, metadata: { email: user.email, lockedUntil: user.lockedUntil },
    });
    res.status(423).json({ error: `Account temporarily locked. Try again in ${timeLabel}.`, lockedUntil: user.lockedUntil });
    return;
  }

  const currentAttempts = user.failedLoginAttempts || 0;
  if (currentAttempts >= CHALLENGE_THRESHOLD) {
    const { challengeAnswer, challengeId } = req.body as any;
    if (!challengeId || challengeAnswer === undefined || challengeAnswer === null) {
      const challenge = generateChallenge(parsed.data.email);
      res.status(428).json({
        error: "Security verification required",
        requiresChallenge: true,
        challengeQuestion: challenge.question,
        challengeId: challenge.challengeId,
        failedAttempts: currentAttempts,
      });
      return;
    }
    if (!verifyChallenge(challengeId, Number(challengeAnswer), parsed.data.email)) {
      res.status(401).json({ error: "Incorrect security answer. Please try again.", requiresChallenge: true });
      return;
    }
  }

  let passwordValid = false;
  console.log("[LOGIN] User found:", user.email, "| password hash starts with:", user.password.substring(0, 7), "| attempts:", user.failedLoginAttempts);
  if (user.password.startsWith("$2")) {
    passwordValid = await bcrypt.compare(parsed.data.password, user.password);
    console.log("[LOGIN] bcrypt compare result:", passwordValid);
  } else {
    passwordValid = user.password === parsed.data.password;
    if (passwordValid) {
      const hash = await bcrypt.hash(parsed.data.password, 12);
      await pool.query(`UPDATE users SET password = $1 WHERE id = $2`, [hash, user.id]);
    }
  }

  if (!passwordValid) {
    const newAttempts = currentAttempts + 1;

    const lockout = getLockoutDuration(newAttempts);
    const lockedUntil = lockout ? new Date(Date.now() + lockout.durationMs) : null;

    await pool.query(
      `UPDATE users SET failed_login_attempts = $1, locked_until = $2 WHERE id = $3`,
      [newAttempts, lockedUntil, user.id]
    );

    const severity = newAttempts >= 10 ? "fatal" : (newAttempts >= 5 ? "critical" : "warning");
    await logSecurityEvent(
      lockout ? "account_locked" : "login_failed",
      severity,
      lockout
        ? `Account locked for ${lockout.label} after ${newAttempts} failed attempts: ${user.email}`
        : `Failed login attempt #${newAttempts} for ${user.email}`,
      { userId: user.id, req, metadata: { email: user.email, attempts: newAttempts, reason: "wrong_password" } }
    );

    if (lockout) {
      res.status(423).json({
        error: `Too many failed attempts. Account locked for ${lockout.label}.`,
        lockedUntil: lockedUntil,
      });
    } else {
      const remaining = CHALLENGE_THRESHOLD - newAttempts;
      if (remaining > 0) {
        res.status(401).json({ error: `Invalid email or password. ${remaining} attempt${remaining > 1 ? "s" : ""} remaining before verification required.` });
      } else {
        res.status(401).json({ error: "Invalid email or password" });
      }
    }
    return;
  }

  if (!user.active) {
    await logSecurityEvent("login_blocked", "warning", `Login attempt on deactivated account: ${user.email}`, {
      userId: user.id, req, metadata: { email: user.email, reason: "account_deactivated" },
    });
    res.status(401).json({ error: "Account is deactivated" });
    return;
  }

  if (user.mustChangePassword && user.inviteExpiresAt && new Date(user.inviteExpiresAt) < new Date()) {
    res.status(401).json({ error: "Your invitation has expired. Please ask your admin to resend the invite.", inviteExpired: true });
    return;
  }

  const sessionToken = generateSessionToken();
  const clientIp = getClientIp(req);
  const fingerprint = generateFingerprint(req);

  await pool.query(
    `UPDATE users SET failed_login_attempts = 0, locked_until = NULL,
     last_login_ip = $1, last_login_at = $2, session_token = $3,
     session_expires_at = $4, session_fingerprint = $5, last_seen_at = $2
     WHERE id = $6`,
    [clientIp, new Date(), sessionToken, new Date(Date.now() + SESSION_DURATION_MS), fingerprint, user.id]
  );

  await logSecurityEvent("login_success", "info", `Successful login: ${user.email}`, {
    userId: user.id, req,
    metadata: { email: user.email, role: user.role },
  });

  const isProduction = process.env.NODE_ENV === "production";
  const isReplit = !!process.env.REPL_ID;

  const cookieOptions = {
    httpOnly: true,
    secure: isProduction || isReplit,
    sameSite: (isReplit && !isProduction ? "none" : isProduction ? "strict" : "lax") as "none" | "strict" | "lax",
    maxAge: SESSION_DURATION_MS,
    path: "/",
  };

  res.cookie("sessionToken", sessionToken, cookieOptions);
  res.cookie("userId", String(user.id), cookieOptions);

  const PASSWORD_EXPIRY_DAYS = 90;
  let passwordExpired = false;
  if (!user.passwordChangedAt) {
    passwordExpired = true;
  } else {
    const daysSinceChange = (Date.now() - new Date(user.passwordChangedAt).getTime()) / (1000 * 60 * 60 * 24);
    passwordExpired = daysSinceChange > PASSWORD_EXPIRY_DAYS;
  }

  res.json({
    ...LoginResponse.parse({
      id: user.id,
      email: user.email,
      fullName: user.fullName,
      role: user.role,
    }),
    mustChangePassword: user.mustChangePassword || false,
    passwordExpired,
  });
});

router.get("/auth/me", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;

  const questions = await db.select({ id: securityQuestionsTable.id })
    .from(securityQuestionsTable)
    .where(eq(securityQuestionsTable.userId, user.id));

  const PASSWORD_EXPIRY_DAYS = 90;
  let passwordExpired = false;
  if (!user.passwordChangedAt) {
    passwordExpired = true;
  } else {
    const daysSinceChange = (Date.now() - new Date(user.passwordChangedAt).getTime()) / (1000 * 60 * 60 * 24);
    passwordExpired = daysSinceChange > PASSWORD_EXPIRY_DAYS;
  }

  res.json({
    ...GetMeResponse.parse({
      id: user.id,
      email: user.email,
      fullName: user.fullName,
      role: user.role,
    }),
    hasSecurityQuestions: questions.length >= 2,
    mustChangePassword: user.mustChangePassword || false,
    passwordExpired,
  });
});

router.post("/auth/logout", async (req, res): Promise<void> => {
  const sessionToken = req.cookies?.sessionToken;
  const userId = req.cookies?.userId;

  if (sessionToken && userId) {
    const uid = parseInt(userId, 10);
    if (!isNaN(uid)) {
      const [user] = await db.select({ id: usersTable.id, sessionToken: usersTable.sessionToken })
        .from(usersTable).where(eq(usersTable.id, uid));
      if (user && user.sessionToken === sessionToken) {
        await db.update(usersTable).set({ sessionToken: null, sessionExpiresAt: null, sessionFingerprint: null }).where(eq(usersTable.id, uid));
        logSecurityEvent("logout", "info", `User logged out`, { userId: uid, req }).catch(() => {});
      }
    }
  }

  const isProduction = process.env.NODE_ENV === "production";
  const isReplit = !!process.env.REPL_ID;
  const clearOpts = { path: "/", secure: isProduction || isReplit, sameSite: (isReplit && !isProduction ? "none" : isProduction ? "strict" : "lax") as "none" | "strict" | "lax" };
  res.clearCookie("sessionToken", clearOpts);
  res.clearCookie("userId", clearOpts);
  res.json({ message: "Logged out" });
});

router.post("/auth/change-password", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  const { currentPassword, newPassword } = req.body;
  if (!currentPassword || !newPassword) {
    res.status(400).json({ error: "Current and new password required" });
    return;
  }
  const strengthError = validatePasswordStrength(newPassword);
  if (strengthError) {
    res.status(400).json({ error: strengthError });
    return;
  }

  let valid = false;
  if (user.password.startsWith("$2")) {
    valid = await bcrypt.compare(currentPassword, user.password);
  } else {
    valid = user.password === currentPassword;
  }

  if (!valid) {
    await logSecurityEvent("password_change_failed", "warning", `Failed password change attempt for ${user.email}`, {
      userId: user.id, req,
    });
    res.status(401).json({ error: "Current password is incorrect" });
    return;
  }

  const hash = await bcrypt.hash(newPassword, 12);
  await db.update(usersTable).set({ password: hash, passwordChangedAt: new Date(), mustChangePassword: false, inviteExpiresAt: null }).where(eq(usersTable.id, user.id));
  await logSecurityEvent("password_change", "info", `Password changed for ${user.email}`, {
    userId: user.id, req,
  });
  res.json({ message: "Password changed successfully" });
});

router.get("/auth/security-info", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  res.json({
    twoFactorEnabled: user.twoFactorEnabled || false,
    twoFactorPhone: user.twoFactorPhone ? `***-***-${user.twoFactorPhone.slice(-4)}` : null,
    lastLoginIp: user.lastLoginIp,
    lastLoginAt: user.lastLoginAt,
    passwordChangedAt: user.passwordChangedAt,
  });
});

router.get("/auth/security-questions", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  const questions = await db.select({
    id: securityQuestionsTable.id,
    question: securityQuestionsTable.question,
    createdAt: securityQuestionsTable.createdAt,
  })
    .from(securityQuestionsTable)
    .where(eq(securityQuestionsTable.userId, user.id));

  res.json({ questions, hasRequired: questions.length >= 2 });
});

router.post("/auth/security-questions", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  const { questions } = req.body;

  if (!Array.isArray(questions) || questions.length < 2) {
    res.status(400).json({ error: "At least 2 security questions are required" });
    return;
  }

  for (const q of questions) {
    if (!q.question || typeof q.question !== "string" || q.question.trim().length < 5) {
      res.status(400).json({ error: "Each question must be at least 5 characters" });
      return;
    }
    if (!q.answer || typeof q.answer !== "string" || q.answer.trim().length < 2) {
      res.status(400).json({ error: "Each answer must be at least 2 characters" });
      return;
    }
  }

  await db.delete(securityQuestionsTable).where(eq(securityQuestionsTable.userId, user.id));

  for (const q of questions) {
    const answerHash = await bcrypt.hash(q.answer.trim().toLowerCase(), 10);
    await db.insert(securityQuestionsTable).values({
      userId: user.id,
      question: q.question.trim(),
      answerHash,
    });
  }

  await logSecurityEvent("security_questions_updated", "info", `Security questions updated for ${user.email}`, {
    userId: user.id, req,
  });

  res.json({ message: "Security questions saved successfully" });
});

router.get("/auth/activity-lock-status", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  const locked = isUserLocked(user.id);
  if (!locked) {
    res.json({ locked: false });
    return;
  }
  const questions = await db.select({
    id: securityQuestionsTable.id,
    question: securityQuestionsTable.question,
  }).from(securityQuestionsTable).where(eq(securityQuestionsTable.userId, user.id));

  res.json({ locked: true, questions });
});

router.post("/auth/verify-activity-unlock", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;

  if (!isUserLocked(user.id)) {
    res.json({ success: true, message: "Account is not locked" });
    return;
  }

  const attemptCheck = checkUnlockAttempts(user.id);
  if (!attemptCheck.allowed) {
    const waitMinutes = Math.ceil((attemptCheck.cooldownUntil! - Date.now()) / 60000);
    res.status(429).json({ error: `Too many failed attempts. Please wait ${waitMinutes} minute(s) and try again.`, cooldownUntil: attemptCheck.cooldownUntil });
    return;
  }

  const { questionId, answer } = req.body;
  if (!questionId || !answer || typeof answer !== "string") {
    res.status(400).json({ error: "Question ID and answer are required" });
    return;
  }

  const [question] = await db.select().from(securityQuestionsTable)
    .where(and(
      eq(securityQuestionsTable.id, questionId),
      eq(securityQuestionsTable.userId, user.id)
    ));

  if (!question) {
    res.status(400).json({ error: "Invalid security question" });
    return;
  }

  const isValid = await bcrypt.compare(answer.trim().toLowerCase(), question.answerHash);
  if (!isValid) {
    recordUnlockAttempt(user.id);
    const remaining = checkUnlockAttempts(user.id);
    await logSecurityEvent("login_failed" as any, "warning",
      `Failed activity unlock attempt for ${user.email} — wrong security answer (${remaining.remainingAttempts} attempts remaining)`, {
      userId: user.id, req,
    });
    res.status(403).json({ error: `Incorrect answer. ${remaining.remainingAttempts} attempt(s) remaining.` });
    return;
  }

  unlockUser(user.id);
  await logSecurityEvent("account_unlocked" as any, "info",
    `Activity lock cleared for ${user.email} after successful security verification`, {
    userId: user.id, req,
  });

  res.json({ success: true, message: "Account unlocked successfully" });
});

router.post("/auth/request-password-reset", requireAuth, async (req, res): Promise<void> => {
  const requestor = (req as any).user;
  const { userId, reason } = req.body;

  const targetId = userId || requestor.id;

  if (targetId !== requestor.id && requestor.role !== "admin" && requestor.role !== "super_admin" && requestor.role !== "manager") {
    res.status(403).json({ error: "Only admins can request password resets for other users" });
    return;
  }

  const pendingRequests = await db.select()
    .from(passwordResetRequestsTable)
    .where(and(
      eq(passwordResetRequestsTable.userId, targetId),
      eq(passwordResetRequestsTable.status, "pending")
    ));

  if (pendingRequests.length > 0) {
    res.status(409).json({ error: "A password reset request is already pending for this user" });
    return;
  }

  if (!reason || typeof reason !== "string" || reason.trim().length < 5) {
    res.status(400).json({ error: "Please provide a reason for the password reset (at least 5 characters)" });
    return;
  }

  const [request] = await db.insert(passwordResetRequestsTable).values({
    userId: targetId,
    requestedBy: requestor.id,
    reason: reason.trim(),
    status: "pending",
  }).returning();

  const [targetUser] = await db.select({ email: usersTable.email, fullName: usersTable.fullName })
    .from(usersTable).where(eq(usersTable.id, targetId));

  await logSecurityEvent("password_reset_requested", "warning",
    `Password reset requested for ${targetUser?.email || "unknown"} by ${requestor.email}`, {
      userId: targetId, req,
      metadata: { requestId: request.id, requestedBy: requestor.id, reason: reason.trim() },
    });

  res.json({ message: "Password reset request submitted. A Super Admin must approve it.", requestId: request.id });
});

router.get("/auth/password-reset-requests", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  const isSuperAdmin = user.role === "super_admin";
  const isAdmin = user.role === "admin" || isSuperAdmin;

  let requests;
  if (isSuperAdmin) {
    requests = await db.select({
      id: passwordResetRequestsTable.id,
      userId: passwordResetRequestsTable.userId,
      requestedBy: passwordResetRequestsTable.requestedBy,
      status: passwordResetRequestsTable.status,
      reason: passwordResetRequestsTable.reason,
      reviewedBy: passwordResetRequestsTable.reviewedBy,
      reviewedAt: passwordResetRequestsTable.reviewedAt,
      reviewNote: passwordResetRequestsTable.reviewNote,
      createdAt: passwordResetRequestsTable.createdAt,
      userName: usersTable.fullName,
      userEmail: usersTable.email,
    })
      .from(passwordResetRequestsTable)
      .innerJoin(usersTable, eq(passwordResetRequestsTable.userId, usersTable.id))
      .orderBy(desc(passwordResetRequestsTable.createdAt))
      .limit(50);
  } else {
    requests = await db.select({
      id: passwordResetRequestsTable.id,
      userId: passwordResetRequestsTable.userId,
      status: passwordResetRequestsTable.status,
      reason: passwordResetRequestsTable.reason,
      reviewNote: passwordResetRequestsTable.reviewNote,
      createdAt: passwordResetRequestsTable.createdAt,
    })
      .from(passwordResetRequestsTable)
      .where(eq(passwordResetRequestsTable.userId, user.id))
      .orderBy(desc(passwordResetRequestsTable.createdAt))
      .limit(10);
  }

  res.json({ requests });
});

router.post("/auth/password-reset-requests/:id/approve", requireAuth, requireSuperAdmin, async (req, res): Promise<void> => {
  const requestId = parseInt(req.params.id, 10);
  const reviewer = (req as any).user;
  const { tempPassword, reviewNote } = req.body;

  if (!tempPassword || typeof tempPassword !== "string") {
    res.status(400).json({ error: "Temporary password is required" });
    return;
  }

  const [request] = await db.select()
    .from(passwordResetRequestsTable)
    .where(and(eq(passwordResetRequestsTable.id, requestId), eq(passwordResetRequestsTable.status, "pending")));

  if (!request) {
    res.status(404).json({ error: "Reset request not found or already processed" });
    return;
  }

  const pwError = validatePasswordStrength(tempPassword);
  if (pwError) {
    res.status(400).json({ error: `Temporary password: ${pwError}` });
    return;
  }

  const hash = await bcrypt.hash(tempPassword, 12);

  await db.update(usersTable).set({
    password: hash,
    passwordChangedAt: new Date(),
    failedLoginAttempts: 0,
    lockedUntil: null,
    mustChangePassword: true,
  }).where(eq(usersTable.id, request.userId));

  await db.update(passwordResetRequestsTable).set({
    status: "approved",
    reviewedBy: reviewer.id,
    reviewedAt: new Date(),
    reviewNote: reviewNote || null,
    tempPassword: "***SET***",
  }).where(eq(passwordResetRequestsTable.id, requestId));

  const [targetUser] = await db.select({ email: usersTable.email })
    .from(usersTable).where(eq(usersTable.id, request.userId));

  await logSecurityEvent("password_reset_approved", "warning",
    `Password reset approved for ${targetUser?.email} by ${reviewer.email}`, {
      userId: request.userId, req,
      metadata: { requestId, approvedBy: reviewer.id },
    });

  res.json({ message: "Password reset approved. User can now log in with the temporary password." });
});

router.post("/auth/password-reset-requests/:id/deny", requireAuth, requireSuperAdmin, async (req, res): Promise<void> => {
  const requestId = parseInt(req.params.id, 10);
  const reviewer = (req as any).user;
  const { reviewNote } = req.body;

  const [request] = await db.select()
    .from(passwordResetRequestsTable)
    .where(and(eq(passwordResetRequestsTable.id, requestId), eq(passwordResetRequestsTable.status, "pending")));

  if (!request) {
    res.status(404).json({ error: "Reset request not found or already processed" });
    return;
  }

  await db.update(passwordResetRequestsTable).set({
    status: "denied",
    reviewedBy: reviewer.id,
    reviewedAt: new Date(),
    reviewNote: reviewNote || null,
  }).where(eq(passwordResetRequestsTable.id, requestId));

  const [targetUser] = await db.select({ email: usersTable.email })
    .from(usersTable).where(eq(usersTable.id, request.userId));

  await logSecurityEvent("password_reset_denied", "info",
    `Password reset denied for ${targetUser?.email} by ${reviewer.email}`, {
      userId: request.userId, req,
      metadata: { requestId, deniedBy: reviewer.id, reason: reviewNote },
    });

  res.json({ message: "Password reset request denied." });
});

export default router;
