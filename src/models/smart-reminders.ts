import { pgTable, serial, text, integer, timestamp, boolean, jsonb, index } from "drizzle-orm/pg-core";
import { usersTable } from "./users";
import { leadsTable } from "./leads";

export const smartRemindersTable = pgTable("smart_reminders", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").notNull().references(() => usersTable.id),
  leadId: integer("lead_id").references(() => leadsTable.id, { onDelete: "cascade" }),
  type: text("type").notNull(),
  title: text("title").notNull(),
  message: text("message").notNull(),
  triggerAt: timestamp("trigger_at", { withTimezone: true }).notNull(),
  triggered: boolean("triggered").notNull().default(false),
  dismissed: boolean("dismissed").notNull().default(false),
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  index("smart_reminders_user_id_idx").on(table.userId),
  index("smart_reminders_trigger_at_idx").on(table.triggerAt),
  index("smart_reminders_triggered_idx").on(table.triggered),
]);
