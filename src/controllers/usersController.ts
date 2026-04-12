import { Router, type IRouter } from "express";
import { eq, isNull, and, gte, sql } from "drizzle-orm";
import { db, usersTable, leadsTable, dealsTable, callsTable, commissionsTable, leadMessagesTable, activitiesTable, smartRemindersTable, renewalSuggestionsTable, webhooksTable } from "../configs/database";
import {
  CreateUserBody,
  UpdateUserBody,
  UpdateUserParams,
  DeleteUserParams,
  GetUsersResponse,
  UpdateUserResponse,
} from "../validationSchemas";
import { requireAuth, requireAdmin, requireSuperAdmin, requirePermission } from "../middlewares/authMiddleware";
import bcrypt from "bcryptjs";
import crypto from "crypto";
import { getUncachableGmailClient } from "../services/gmailService";

const INVITE_EXPIRY_MINUTES = 15;

function generateTempPassword(): string {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789!@#$%";
  let pw = "";
  for (let i = 0; i < 16; i++) pw += chars[crypto.randomInt(chars.length)];
  return pw;
}

async function sendInviteEmail(email: string, fullName: string, tempPassword: string, role: string): Promise<void> {
  const appDomain = process.env.REPLIT_DEV_DOMAIN || process.env.REPLIT_DOMAINS?.split(",")[0] || "";
  const loginUrl = appDomain ? `https://${appDomain}` : "the Bridge Capital platform";

  const subject = "Welcome to Bridge Capital - Your Account is Ready";
  const body = [
    `Hi ${fullName},`,
    ``,
    `You've been invited to join Bridge Capital as a ${role === "admin" ? "Manager" : "Sales Rep"}.`,
    ``,
    `Here are your login credentials:`,
    ``,
    `Email: ${email}`,
    `Temporary Password: ${tempPassword}`,
    ``,
    `Login here: ${loginUrl}`,
    ``,
    `IMPORTANT: This temporary password expires in ${INVITE_EXPIRY_MINUTES} minutes.`,
    `You will be prompted to set your own password when you first log in.`,
    ``,
    `If the password has expired, ask your admin to resend the invitation.`,
    ``,
    `Welcome aboard!`,
    `- Bridge Capital Team`,
  ].join("\r\n");

  const rawEmail = [
    `From: me`,
    `To: ${email}`,
    `Subject: ${subject}`,
    `Content-Type: text/html; charset="UTF-8"`,
    ``,
    buildInviteHtml(fullName, email, tempPassword, role, loginUrl),
  ].join("\r\n");

  const encodedEmail = Buffer.from(rawEmail).toString("base64url");
  const gmail = await getUncachableGmailClient();
  await gmail.users.messages.send({ userId: "me", requestBody: { raw: encodedEmail } });
}

function buildInviteHtml(name: string, email: string, tempPw: string, role: string, loginUrl: string): string {
  const roleLabel = role === "admin" ? "Manager" : "Sales Rep";
  return `<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#111;font-family:Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#111;padding:40px 0;">
<tr><td align="center">
<table width="520" cellpadding="0" cellspacing="0" style="background:#1a1a1a;border-radius:16px;border:1px solid #333;overflow:hidden;">
<tr><td style="background:linear-gradient(135deg,#b8860b,#daa520);padding:32px;text-align:center;">
<h1 style="margin:0;color:#fff;font-size:24px;letter-spacing:1px;">Bridge Capital</h1>
<p style="margin:8px 0 0;color:rgba(255,255,255,0.85);font-size:14px;">FUNDING PLATFORM</p>
</td></tr>
<tr><td style="padding:32px;">
<h2 style="color:#fff;margin:0 0 8px;font-size:20px;">Welcome aboard, ${name}!</h2>
<p style="color:#999;margin:0 0 24px;font-size:14px;line-height:1.5;">You've been invited to join Bridge Capital as a <strong style="color:#daa520;">${roleLabel}</strong>.</p>
<div style="background:#222;border:1px solid #333;border-radius:12px;padding:20px;margin-bottom:24px;">
<p style="color:#999;margin:0 0 12px;font-size:12px;text-transform:uppercase;letter-spacing:1px;">Your Login Credentials</p>
<table width="100%" cellpadding="0" cellspacing="0">
<tr><td style="color:#999;font-size:13px;padding:6px 0;">Email</td><td style="color:#fff;font-size:13px;padding:6px 0;text-align:right;font-weight:bold;">${email}</td></tr>
<tr><td style="color:#999;font-size:13px;padding:6px 0;">Temporary Password</td><td style="color:#daa520;font-size:15px;padding:6px 0;text-align:right;font-weight:bold;font-family:monospace;letter-spacing:1px;">${tempPw}</td></tr>
</table>
</div>
<a href="${loginUrl}" style="display:block;background:linear-gradient(135deg,#b8860b,#daa520);color:#fff;text-decoration:none;text-align:center;padding:14px;border-radius:10px;font-weight:bold;font-size:15px;letter-spacing:0.5px;">ENTER PLATFORM &rarr;</a>
<div style="background:#2a1a0a;border:1px solid #b8860b33;border-radius:8px;padding:14px;margin-top:20px;">
<p style="color:#daa520;margin:0;font-size:12px;line-height:1.5;">&#9888; This temporary password expires in <strong>${INVITE_EXPIRY_MINUTES} minutes</strong>. You'll be asked to set your own password on first login.</p>
</div>
</td></tr>
<tr><td style="padding:20px 32px;border-top:1px solid #333;text-align:center;">
<p style="color:#555;margin:0;font-size:11px;">Bridge Capital &bull; Intelligent Funding</p>
</td></tr>
</table>
</td></tr>
</table>
</body></html>`;
}

const router: IRouter = Router();

const ONLINE_THRESHOLD_MS = 3 * 60 * 1000;

router.post("/users/heartbeat", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  await db.update(usersTable).set({ lastSeenAt: new Date() }).where(eq(usersTable.id, user.id));
  res.json({ ok: true });
});

router.get("/users/team-status", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  const users = await db.select({
    id: usersTable.id,
    fullName: usersTable.fullName,
    role: usersTable.role,
    active: usersTable.active,
    lastSeenAt: usersTable.lastSeenAt,
    canUnderwrite: usersTable.canUnderwrite,
    canDistributeLeads: usersTable.canDistributeLeads,
    canManageDeals: usersTable.canManageDeals,
    canViewAllLeads: usersTable.canViewAllLeads,
    canImport: usersTable.canImport,
    canManageFunders: usersTable.canManageFunders,
    canSendMessages: usersTable.canSendMessages,
    canAccessAnalytics: usersTable.canAccessAnalytics,
  }).from(usersTable).where(eq(usersTable.active, true)).orderBy(usersTable.role, usersTable.fullName);

  const now = Date.now();
  const enriched = users.map(u => ({
    ...u,
    isOnline: u.lastSeenAt ? (now - new Date(u.lastSeenAt).getTime()) < ONLINE_THRESHOLD_MS : false,
  }));

  res.json(enriched);
});

router.patch("/users/:id/capabilities", requireAuth, requireSuperAdmin, async (req, res): Promise<void> => {
  const userId = parseInt(String(req.params.id), 10);
  if (isNaN(userId)) { res.status(400).json({ error: "Invalid user ID" }); return; }

  const allowedFields = [
    "canUnderwrite", "canDistributeLeads", "canManageDeals", "canViewAllLeads",
    "canImport", "canManageFunders", "canSendMessages", "canAccessAnalytics",
  ];
  const updateData: Record<string, boolean> = {};
  for (const field of allowedFields) {
    if (typeof req.body[field] === "boolean") {
      updateData[field] = req.body[field];
    }
  }

  if (Object.keys(updateData).length === 0) {
    res.status(400).json({ error: "No valid capability fields provided" });
    return;
  }

  const [updated] = await db.update(usersTable).set(updateData).where(eq(usersTable.id, userId)).returning();
  if (!updated) { res.status(404).json({ error: "User not found" }); return; }

  res.json({
    id: updated.id,
    fullName: updated.fullName,
    canUnderwrite: updated.canUnderwrite,
    canDistributeLeads: updated.canDistributeLeads,
    canManageDeals: updated.canManageDeals,
    canViewAllLeads: updated.canViewAllLeads,
    canImport: updated.canImport,
    canManageFunders: updated.canManageFunders,
    canSendMessages: updated.canSendMessages,
    canAccessAnalytics: updated.canAccessAnalytics,
  });
});

router.get("/users/online-underwriters", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  const threshold = new Date(Date.now() - ONLINE_THRESHOLD_MS);
  const underwriters = await db.select({
    id: usersTable.id,
    fullName: usersTable.fullName,
    role: usersTable.role,
    lastSeenAt: usersTable.lastSeenAt,
  }).from(usersTable)
    .where(and(
      eq(usersTable.active, true),
      eq(usersTable.canUnderwrite, true),
      gte(usersTable.lastSeenAt, threshold),
    ));

  res.json(underwriters);
});

router.get("/users", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  const users = await db.select({
    id: usersTable.id,
    email: usersTable.email,
    fullName: usersTable.fullName,
    role: usersTable.role,
    phone: usersTable.phone,
    commissionPct: usersTable.commissionPct,
    active: usersTable.active,
    lastSeenAt: usersTable.lastSeenAt,
    canUnderwrite: usersTable.canUnderwrite,
    canDistributeLeads: usersTable.canDistributeLeads,
    canManageDeals: usersTable.canManageDeals,
    canViewAllLeads: usersTable.canViewAllLeads,
    canImport: usersTable.canImport,
    canManageFunders: usersTable.canManageFunders,
    canSendMessages: usersTable.canSendMessages,
    canAccessAnalytics: usersTable.canAccessAnalytics,
    createdAt: usersTable.createdAt,
  }).from(usersTable).orderBy(usersTable.createdAt);
  res.json(users);
});

router.post("/users", requireAuth, requireAdmin, requirePermission("users"), async (req, res): Promise<void> => {
  const parsed = CreateUserBody.safeParse(req.body);
  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.message });
    return;
  }

  const currentUser = (req as any).user;
  if (parsed.data.role === "super_admin" && currentUser.role !== "super_admin") {
    res.status(403).json({ error: "Only super admins can create super admin users" });
    return;
  }

  const existing = await db.select().from(usersTable).where(eq(usersTable.email, parsed.data.email));
  if (existing.length > 0) {
    res.status(409).json({ error: "Email already exists" });
    return;
  }

  const tempPassword = generateTempPassword();
  const hashedPassword = await bcrypt.hash(tempPassword, 12);
  const inviteExpiresAt = new Date(Date.now() + INVITE_EXPIRY_MINUTES * 60 * 1000);

  const [user] = await db.insert(usersTable).values({
    email: parsed.data.email,
    password: hashedPassword,
    fullName: parsed.data.fullName,
    role: parsed.data.role,
    phone: parsed.data.phone || null,
    commissionPct: String(parsed.data.commissionPct ?? 10),
    mustChangePassword: true,
    inviteExpiresAt,
  } as any).returning();

  let emailSent = false;
  try {
    await sendInviteEmail(parsed.data.email, parsed.data.fullName, tempPassword, parsed.data.role);
    emailSent = true;
    console.log(`[Invite] Sent invite email to ${parsed.data.email}`);
  } catch (e: any) {
    console.error(`[Invite] Failed to send invite email to ${parsed.data.email}:`, e.message);
  }

  res.status(201).json({
    ...UpdateUserResponse.parse({
      id: user.id,
      email: user.email,
      fullName: user.fullName,
      role: user.role,
      phone: user.phone,
      commissionPct: user.commissionPct,
      active: user.active,
      createdAt: user.createdAt,
    }),
    emailSent,
    inviteExpiresAt,
  });
});

router.post("/users/:id/resend-invite", requireAuth, requireAdmin, requirePermission("users"), async (req, res): Promise<void> => {
  try {
    const currentUser = (req as any).user;
    const userId = parseInt(String(req.params.id), 10);
    const [user] = await db.select().from(usersTable).where(eq(usersTable.id, userId));
    if (!user) { res.status(404).json({ error: "User not found" }); return; }

    if (user.role === "super_admin" && currentUser.role !== "super_admin") {
      res.status(403).json({ error: "Only super admins can resend invites to super admin accounts" });
      return;
    }

    const tempPassword = generateTempPassword();
    const hashedPassword = await bcrypt.hash(tempPassword, 12);
    const inviteExpiresAt = new Date(Date.now() + INVITE_EXPIRY_MINUTES * 60 * 1000);

    await sendInviteEmail(user.email, user.fullName, tempPassword, user.role);

    await db.update(usersTable).set({
      password: hashedPassword,
      mustChangePassword: true,
      inviteExpiresAt,
    }).where(eq(usersTable.id, userId));

    console.log(`[Invite] Resent invite email to ${user.email}`);
    res.json({ success: true, inviteExpiresAt, message: `Invitation resent to ${user.email}` });
  } catch (e: any) {
    console.error("[Invite] Resend error:", e.message);
    res.status(500).json({ error: `Failed to resend invite: ${e.message}` });
  }
});

router.patch("/users/:id", requireAuth, requireAdmin, requirePermission("users"), async (req, res): Promise<void> => {
  const params = UpdateUserParams.safeParse(req.params);
  if (!params.success) {
    res.status(400).json({ error: params.error.message });
    return;
  }

  const parsed = UpdateUserBody.safeParse(req.body);
  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.message });
    return;
  }

  const currentUser = (req as any).user;
  const [targetUser] = await db.select().from(usersTable).where(eq(usersTable.id, params.data.id));
  if (targetUser?.role === "super_admin" && currentUser.role !== "super_admin") {
    res.status(403).json({ error: "Cannot modify super admin accounts" });
    return;
  }
  if (parsed.data.role === "super_admin" && currentUser.role !== "super_admin") {
    res.status(403).json({ error: "Only super admins can assign super admin role" });
    return;
  }

  const updateData: any = {};
  if (parsed.data.email !== undefined) updateData.email = parsed.data.email;
  if (parsed.data.fullName !== undefined) updateData.fullName = parsed.data.fullName;
  if (parsed.data.role !== undefined) updateData.role = parsed.data.role;
  if (parsed.data.phone !== undefined) updateData.phone = parsed.data.phone;
  if (parsed.data.active !== undefined) updateData.active = parsed.data.active;
  if (parsed.data.commissionPct !== undefined) updateData.commissionPct = String(parsed.data.commissionPct);
  if (parsed.data.password !== undefined) {
    res.status(403).json({ error: "Password resets must go through the approval system. Use Security Guard > Password Resets." });
    return;
  }

  const [user] = await db.update(usersTable).set(updateData).where(eq(usersTable.id, params.data.id)).returning();
  if (!user) {
    res.status(404).json({ error: "User not found" });
    return;
  }

  const { logSecurityEvent } = await import("../utils/security");
  if (parsed.data.role !== undefined && parsed.data.role !== targetUser?.role) {
    await logSecurityEvent("role_change", "critical", `Role changed for ${user.email}: ${targetUser?.role} → ${parsed.data.role}`, {
      userId: currentUser.id, req, metadata: { targetUserId: user.id, oldRole: targetUser?.role, newRole: parsed.data.role },
    });
  }
  if (parsed.data.active !== undefined && parsed.data.active !== targetUser?.active) {
    await logSecurityEvent(parsed.data.active ? "user_created" : "user_deactivated",
      parsed.data.active ? "info" : "warning",
      `User ${parsed.data.active ? "activated" : "deactivated"}: ${user.email}`, {
      userId: currentUser.id, req, metadata: { targetUserId: user.id },
    });
  }

  res.json(UpdateUserResponse.parse({
    id: user.id,
    email: user.email,
    fullName: user.fullName,
    role: user.role,
    phone: user.phone,
    commissionPct: user.commissionPct,
    active: user.active,
    createdAt: user.createdAt,
  }));
});

router.delete("/users/:id", requireAuth, requireAdmin, requirePermission("users"), async (req, res): Promise<void> => {
  const params = DeleteUserParams.safeParse(req.params);
  if (!params.success) {
    res.status(400).json({ error: params.error.message });
    return;
  }

  const currentUser = (req as any).user;
  const [targetUser] = await db.select().from(usersTable).where(eq(usersTable.id, params.data.id));
  if (targetUser?.role === "super_admin" && currentUser.role !== "super_admin") {
    res.status(403).json({ error: "Cannot delete super admin accounts" });
    return;
  }

  try {
    await db.update(leadsTable).set({ assignedToId: null }).where(eq(leadsTable.assignedToId, params.data.id));
    await db.delete(callsTable).where(eq(callsTable.userId, params.data.id));
    await db.delete(commissionsTable).where(eq(commissionsTable.repId, params.data.id));
    await db.delete(leadMessagesTable).where(eq(leadMessagesTable.userId, params.data.id));
    await db.delete(smartRemindersTable).where(eq(smartRemindersTable.userId, params.data.id));
    await db.delete(renewalSuggestionsTable).where(eq(renewalSuggestionsTable.repId, params.data.id));
    await db.update(webhooksTable).set({ createdById: null }).where(eq(webhooksTable.createdById, params.data.id));
    await db.delete(dealsTable).where(eq(dealsTable.repId, params.data.id));
    await db.delete(activitiesTable).where(eq(activitiesTable.userId, params.data.id));

    const [user] = await db.delete(usersTable).where(eq(usersTable.id, params.data.id)).returning();
    if (!user) {
      res.status(404).json({ error: "User not found" });
      return;
    }

    res.json({ message: "User deleted" });
  } catch (err: any) {
    console.error("Delete user error:", err);
    res.status(500).json({ error: "Failed to delete user. The user may have associated records that cannot be removed." });
  }
});

export default router;
