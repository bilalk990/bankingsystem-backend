import { pgTable, text, serial, timestamp, doublePrecision, numeric, integer, index } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod/v4";
import { dealsTable } from "./deals";
import { usersTable } from "./users";

export const commissionsTable = pgTable("commissions", {
  id: serial("id").primaryKey(),
  dealId: integer("deal_id").notNull().references(() => dealsTable.id, { onDelete: "cascade" }),
  repId: integer("rep_id").notNull().references(() => usersTable.id),
  type: text("type").notNull().default("funding"),
  amount: numeric("amount", { precision: 20, scale: 2 }).notNull(),
  percentage: numeric("percentage", { precision: 20, scale: 2 }),
  status: text("status").notNull().default("pending"),
  paidAt: timestamp("paid_at", { withTimezone: true }),
  notes: text("notes"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  index("commissions_deal_id_idx").on(table.dealId),
  index("commissions_rep_id_idx").on(table.repId),
  index("commissions_status_idx").on(table.status),
]);

export const insertCommissionSchema = createInsertSchema(commissionsTable).omit({ id: true, createdAt: true });
export type InsertCommission = z.infer<typeof insertCommissionSchema>;
export type Commission = typeof commissionsTable.$inferSelect;
