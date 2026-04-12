import { pgTable, text, serial, timestamp, boolean, doublePrecision, numeric, integer } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod/v4";

export const usersTable = pgTable("users", {
  id: serial("id").primaryKey(),
  email: text("email").notNull().unique(),
  password: text("password").notNull(),
  fullName: text("full_name").notNull(),
  role: text("role").notNull().default("rep"),
  phone: text("phone"),
  commissionPct: numeric("commission_pct", { precision: 20, scale: 2 }).default("10"),
  active: boolean("active").notNull().default(true),
  lastSeenAt: timestamp("last_seen_at", { withTimezone: true }),
  canUnderwrite: boolean("can_underwrite").notNull().default(false),
  canDistributeLeads: boolean("can_distribute_leads").notNull().default(false),
  canManageDeals: boolean("can_manage_deals").notNull().default(false),
  canViewAllLeads: boolean("can_view_all_leads").notNull().default(false),
  canImport: boolean("can_import").notNull().default(false),
  canManageFunders: boolean("can_manage_funders").notNull().default(false),
  canSendMessages: boolean("can_send_messages").notNull().default(false),
  canAccessAnalytics: boolean("can_access_analytics").notNull().default(false),
  twoFactorEnabled: boolean("two_factor_enabled").notNull().default(false),
  twoFactorPhone: text("two_factor_phone"),
  lastLoginIp: text("last_login_ip"),
  lastLoginAt: timestamp("last_login_at", { withTimezone: true }),
  failedLoginAttempts: integer("failed_login_attempts").notNull().default(0),
  lockedUntil: timestamp("locked_until", { withTimezone: true }),
  sessionToken: text("session_token"),
  sessionExpiresAt: timestamp("session_expires_at", { withTimezone: true }),
  sessionFingerprint: text("session_fingerprint"),
  passwordChangedAt: timestamp("password_changed_at", { withTimezone: true }),
  mustChangePassword: boolean("must_change_password").notNull().default(false),
  inviteExpiresAt: timestamp("invite_expires_at", { withTimezone: true }),
  repTier: text("rep_tier").default("standard"),
  riskPreference: text("risk_preference").default("any"),
  minDealAmount: numeric("min_deal_amount", { precision: 20, scale: 2 }),
  maxDealAmount: numeric("max_deal_amount", { precision: 20, scale: 2 }),
  googleSheetUrl: text("google_sheet_url"),
  googleSheetTab: text("google_sheet_tab"),
  autoAssignEnabled: boolean("auto_assign_enabled").notNull().default(true),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});

export const USER_CAPABILITIES = [
  { key: "canUnderwrite", label: "Underwriting", description: "Review and underwrite leads" },
  { key: "canDistributeLeads", label: "Distribute Leads", description: "Assign leads to reps" },
  { key: "canManageDeals", label: "Manage Deals", description: "Create and modify deals" },
  { key: "canViewAllLeads", label: "View All Leads", description: "See all leads regardless of assignment" },
  { key: "canImport", label: "Import Data", description: "Import leads and bank statements" },
  { key: "canManageFunders", label: "Manage Funders", description: "Add and edit funder info" },
  { key: "canSendMessages", label: "Send Messages", description: "Send SMS/email to leads" },
  { key: "canAccessAnalytics", label: "Analytics", description: "Access reports and analytics" },
] as const;

export const insertUserSchema = createInsertSchema(usersTable).omit({ id: true, createdAt: true });
export type InsertUser = z.infer<typeof insertUserSchema>;
export type User = typeof usersTable.$inferSelect;
