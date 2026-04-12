import { pgTable, text, serial, timestamp, doublePrecision, boolean, integer, jsonb } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod/v4";

export const fundersTable = pgTable("funders", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
  type: text("type").default("direct"),
  description: text("description"),
  contactName: text("contact_name"),
  contactEmail: text("contact_email"),
  contactPhone: text("contact_phone"),
  website: text("website"),
  minAmount: doublePrecision("min_amount"),
  maxAmount: doublePrecision("max_amount"),
  minCreditScore: integer("min_credit_score"),
  minTimeInBusiness: integer("min_time_in_business"),
  industries: jsonb("industries"),
  states: jsonb("states"),
  maxPositions: integer("max_positions").default(4),
  defaultFactorRate: doublePrecision("default_factor_rate"),
  commissionPct: doublePrecision("commission_pct"),
  paymentFrequency: text("payment_frequency").default("daily"),
  notes: text("notes"),
  active: boolean("active").default(true),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});

export const insertFunderSchema = createInsertSchema(fundersTable).omit({ id: true, createdAt: true });
export type InsertFunder = z.infer<typeof insertFunderSchema>;
export type Funder = typeof fundersTable.$inferSelect;
