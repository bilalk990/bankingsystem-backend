import { pgTable, serial, text, timestamp, integer, index } from "drizzle-orm/pg-core";

export const lenderRulesTable = pgTable("lender_rules", {
  id: serial("id").primaryKey(),
  lenderName: text("lender_name").notNull(),
  verdict: text("verdict").notNull(),
  adminNotes: text("admin_notes"),
  confirmedById: integer("confirmed_by_id"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  index("lender_rules_name_idx").on(table.lenderName),
  index("lender_rules_verdict_idx").on(table.verdict),
]);

export type LenderRule = typeof lenderRulesTable.$inferSelect;
