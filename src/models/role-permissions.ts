import { pgTable, serial, text, boolean, timestamp } from "drizzle-orm/pg-core";

export const rolePermissionsTable = pgTable("role_permissions", {
  id: serial("id").primaryKey(),
  role: text("role").notNull(),
  permission: text("permission").notNull(),
  enabled: boolean("enabled").notNull().default(true),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
});

export const PERMISSIONS = [
  "dashboard",
  "leads",
  "leads.create",
  "leads.delete",
  "leads.bulk_actions",
  "pipeline",
  "pipeline.move_deals",
  "analytics",
  "commissions",
  "reports",
  "templates",
  "lead_scoring",
  "forecast",
  "calculator",
  "goals",
  "activity",
  "portfolio",
  "ai_learning",
  "funders",
  "isos",
  "import",
  "users",
  "role_management",
  "webhooks",
  "ai_chat",
  "ai_underwriting",
  "ai_scripts",
] as const;

export type Permission = typeof PERMISSIONS[number];
