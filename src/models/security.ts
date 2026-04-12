import { pgTable, text, serial, timestamp, integer, jsonb, index } from "drizzle-orm/pg-core";
import { usersTable } from "./users";

export const securityEventsTable = pgTable("security_events", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").references(() => usersTable.id, { onDelete: "set null" }),
  eventType: text("event_type").notNull(),
  severity: text("severity").notNull().default("info"),
  ipAddress: text("ip_address"),
  userAgent: text("user_agent"),
  metadata: jsonb("metadata"),
  description: text("description").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  index("security_events_user_id_idx").on(table.userId),
  index("security_events_type_idx").on(table.eventType),
  index("security_events_severity_idx").on(table.severity),
  index("security_events_created_at_idx").on(table.createdAt),
  index("security_events_ip_idx").on(table.ipAddress),
]);

export const securityScansTable = pgTable("security_scans", {
  id: serial("id").primaryKey(),
  scanType: text("scan_type").notNull(),
  status: text("status").notNull().default("clean"),
  findings: jsonb("findings"),
  resolvedAt: timestamp("resolved_at", { withTimezone: true }),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  index("security_scans_type_idx").on(table.scanType),
  index("security_scans_status_idx").on(table.status),
  index("security_scans_created_at_idx").on(table.createdAt),
]);

export const passwordResetRequestsTable = pgTable("password_reset_requests", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").notNull().references(() => usersTable.id, { onDelete: "cascade" }),
  requestedBy: integer("requested_by").references(() => usersTable.id, { onDelete: "set null" }),
  status: text("status").notNull().default("pending"),
  reason: text("reason"),
  reviewedBy: integer("reviewed_by").references(() => usersTable.id, { onDelete: "set null" }),
  reviewedAt: timestamp("reviewed_at", { withTimezone: true }),
  reviewNote: text("review_note"),
  tempPassword: text("temp_password"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  index("password_reset_requests_user_id_idx").on(table.userId),
  index("password_reset_requests_status_idx").on(table.status),
  index("password_reset_requests_created_at_idx").on(table.createdAt),
]);

export const securityQuestionsTable = pgTable("security_questions", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").notNull().references(() => usersTable.id, { onDelete: "cascade" }),
  question: text("question").notNull(),
  answerHash: text("answer_hash").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  index("security_questions_user_id_idx").on(table.userId),
]);

export type SecurityEvent = typeof securityEventsTable.$inferSelect;
export type SecurityScan = typeof securityScansTable.$inferSelect;
export type PasswordResetRequest = typeof passwordResetRequestsTable.$inferSelect;
export type SecurityQuestion = typeof securityQuestionsTable.$inferSelect;
