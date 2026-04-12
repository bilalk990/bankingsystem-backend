import { pgTable, text, serial, timestamp, integer, index } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod/v4";
import { leadsTable } from "./leads";
import { usersTable } from "./users";

export const callsTable = pgTable("calls", {
  id: serial("id").primaryKey(),
  leadId: integer("lead_id").notNull().references(() => leadsTable.id, { onDelete: "cascade" }),
  userId: integer("user_id").notNull().references(() => usersTable.id),
  outcome: text("outcome").notNull(),
  notes: text("notes"),
  duration: integer("duration"),
  callbackAt: timestamp("callback_at", { withTimezone: true }),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  index("calls_lead_id_idx").on(table.leadId),
  index("calls_user_id_idx").on(table.userId),
  index("calls_callback_at_idx").on(table.callbackAt),
]);

export const insertCallSchema = createInsertSchema(callsTable).omit({ id: true, createdAt: true });
export type InsertCall = z.infer<typeof insertCallSchema>;
export type Call = typeof callsTable.$inferSelect;
