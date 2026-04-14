import { pgTable, text, serial, timestamp, integer, boolean, jsonb, index } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod/v4";
import { leadsTable } from "./leads";
import { bankStatementAnalysesTable } from "./bank-statement-analyses";

export const underwritingConfirmationsTable = pgTable("underwriting_confirmations", {
  id: serial("id").primaryKey(),
  analysisId: integer("analysis_id").references(() => bankStatementAnalysesTable.id, { onDelete: "set null" }),
  leadId: integer("lead_id").references(() => leadsTable.id, { onDelete: "cascade" }),
  findingType: text("finding_type").notNull(),
  findingIndex: integer("finding_index").notNull(),
  originalValue: jsonb("original_value"),
  status: text("status").notNull().default("pending"),
  adminLabel: text("admin_label"),
  adminNotes: text("admin_notes"),
  confirmedById: integer("confirmed_by_id"),
  confirmedAt: timestamp("confirmed_at", { withTimezone: true }),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  leadBusinessName: text("lead_business_name"),
}, (table) => [
  index("uw_conf_analysis_id_idx").on(table.analysisId),
  index("uw_conf_lead_id_idx").on(table.leadId),
  index("uw_conf_status_idx").on(table.status),
]);

export const insertUnderwritingConfirmationSchema = createInsertSchema(underwritingConfirmationsTable).omit({ id: true, createdAt: true });
export type InsertUnderwritingConfirmation = z.infer<typeof insertUnderwritingConfirmationSchema>;
export type UnderwritingConfirmation = typeof underwritingConfirmationsTable.$inferSelect;
