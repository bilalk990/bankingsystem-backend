import { pgTable, text, serial, timestamp, doublePrecision, integer, index } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod/v4";
import { dealsTable } from "./deals";
import { fundersTable } from "./funders";

export const submissionsTable = pgTable("submissions", {
  id: serial("id").primaryKey(),
  dealId: integer("deal_id").notNull().references(() => dealsTable.id, { onDelete: "cascade" }),
  funderId: integer("funder_id").notNull().references(() => fundersTable.id),
  status: text("status").notNull().default("pending"),
  submittedAt: timestamp("submitted_at", { withTimezone: true }).notNull().defaultNow(),
  approvedAmount: doublePrecision("approved_amount"),
  approvedFactorRate: doublePrecision("approved_factor_rate"),
  approvedTerm: integer("approved_term"),
  declineReason: text("decline_reason"),
  notes: text("notes"),
  respondedAt: timestamp("responded_at", { withTimezone: true }),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  index("submissions_deal_id_idx").on(table.dealId),
  index("submissions_funder_id_idx").on(table.funderId),
  index("submissions_status_idx").on(table.status),
]);

export const insertSubmissionSchema = createInsertSchema(submissionsTable).omit({ id: true, createdAt: true });
export type InsertSubmission = z.infer<typeof insertSubmissionSchema>;
export type Submission = typeof submissionsTable.$inferSelect;
