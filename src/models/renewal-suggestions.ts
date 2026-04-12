import { pgTable, text, serial, timestamp, integer, doublePrecision, boolean, jsonb } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod/v4";
import { leadsTable } from "./leads";
import { dealsTable } from "./deals";
import { usersTable } from "./users";

export const renewalSuggestionsTable = pgTable("renewal_suggestions", {
  id: serial("id").primaryKey(),
  leadId: integer("lead_id").notNull().references(() => leadsTable.id, { onDelete: "cascade" }),
  dealId: integer("deal_id").notNull().references(() => dealsTable.id, { onDelete: "cascade" }),
  repId: integer("rep_id").notNull().references(() => usersTable.id),
  status: text("status").notNull().default("pending"),
  suggestedReachOutDate: timestamp("suggested_reach_out_date", { withTimezone: true }),
  reason: text("reason"),
  suggestedMessage: text("suggested_message"),
  suggestedChannel: text("suggested_channel").default("sms"),
  confidence: integer("confidence"),
  patternSource: text("pattern_source"),
  estimatedAmount: doublePrecision("estimated_amount"),
  metadata: jsonb("metadata"),
  actedAt: timestamp("acted_at", { withTimezone: true }),
  dismissedAt: timestamp("dismissed_at", { withTimezone: true }),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});

export const insertRenewalSuggestionSchema = createInsertSchema(renewalSuggestionsTable).omit({ id: true, createdAt: true });
export type InsertRenewalSuggestion = z.infer<typeof insertRenewalSuggestionSchema>;
export type RenewalSuggestion = typeof renewalSuggestionsTable.$inferSelect;
