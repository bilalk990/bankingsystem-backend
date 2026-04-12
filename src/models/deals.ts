import { pgTable, text, serial, timestamp, integer, doublePrecision, numeric, index } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod/v4";
import { leadsTable } from "./leads";
import { usersTable } from "./users";

export const dealsTable = pgTable("deals", {
  id: serial("id").primaryKey(),
  leadId: integer("lead_id").notNull().references(() => leadsTable.id, { onDelete: "cascade" }),
  repId: integer("rep_id").notNull().references(() => usersTable.id),
  stage: text("stage").notNull().default("prospect"),
  amount: numeric("amount", { precision: 20, scale: 2 }).notNull(),
  factorRate: numeric("factor_rate", { precision: 20, scale: 2 }),
  paybackAmount: numeric("payback_amount", { precision: 20, scale: 2 }),
  term: integer("term"),
  commission: numeric("commission", { precision: 20, scale: 2 }),
  funderId: integer("funder_id"),
  fundedDate: timestamp("funded_date", { withTimezone: true }),
  paymentFrequency: text("payment_frequency").default("daily"),
  paymentAmount: numeric("payment_amount", { precision: 20, scale: 2 }),
  totalPayments: integer("total_payments"),
  paymentsCompleted: integer("payments_completed").default(0),
  renewalEligibleDate: timestamp("renewal_eligible_date", { withTimezone: true }),
  fundingSource: text("funding_source").default("in_house"),
  funderName: text("funder_name"),
  contractUrl: text("contract_url"),
  notes: text("notes"),
  defaultStatus: text("default_status"),
  defaultedAt: timestamp("defaulted_at", { withTimezone: true }),
  defaultNotes: text("default_notes"),
  defaultAmount: numeric("default_amount", { precision: 20, scale: 2 }),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow().$onUpdate(() => new Date()),
}, (table) => [
  index("deals_lead_id_idx").on(table.leadId),
  index("deals_rep_id_idx").on(table.repId),
  index("deals_stage_idx").on(table.stage),
  index("deals_funder_id_idx").on(table.funderId),
  index("deals_created_at_idx").on(table.createdAt),
  index("deals_funded_date_idx").on(table.fundedDate),
  index("deals_default_status_idx").on(table.defaultStatus),
]);

export const insertDealSchema = createInsertSchema(dealsTable).omit({ id: true, createdAt: true, updatedAt: true });
export type InsertDeal = z.infer<typeof insertDealSchema>;
export type Deal = typeof dealsTable.$inferSelect;
