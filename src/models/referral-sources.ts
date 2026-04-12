import { pgTable, text, serial, timestamp, doublePrecision, boolean } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod/v4";

export const referralSourcesTable = pgTable("referral_sources", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
  type: text("type").notNull().default("iso"),
  contactName: text("contact_name"),
  contactEmail: text("contact_email"),
  contactPhone: text("contact_phone"),
  commissionPct: doublePrecision("commission_pct"),
  active: boolean("active").default(true),
  notes: text("notes"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});

export const insertReferralSourceSchema = createInsertSchema(referralSourcesTable).omit({ id: true, createdAt: true });
export type InsertReferralSource = z.infer<typeof insertReferralSourceSchema>;
export type ReferralSource = typeof referralSourcesTable.$inferSelect;
