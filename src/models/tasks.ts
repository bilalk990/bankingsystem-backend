import { pgTable, text, serial, timestamp, integer, index } from "drizzle-orm/pg-core";
import { usersTable } from "./users";
import { leadsTable } from "./leads";

export const tasksTable = pgTable("tasks", {
  id: serial("id").primaryKey(),
  title: text("title").notNull(),
  description: text("description"),
  priority: text("priority").notNull().default("medium"),
  status: text("status").notNull().default("pending"),
  dueDate: timestamp("due_date", { withTimezone: true }),
  assignedToId: integer("assigned_to_id").references(() => usersTable.id),
  assignedById: integer("assigned_by_id").references(() => usersTable.id),
  leadId: integer("lead_id").references(() => leadsTable.id),
  completedAt: timestamp("completed_at", { withTimezone: true }),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  index("tasks_assigned_to_idx").on(table.assignedToId),
  index("tasks_status_idx").on(table.status),
  index("tasks_due_date_idx").on(table.dueDate),
  index("tasks_lead_id_idx").on(table.leadId),
]);

export type Task = typeof tasksTable.$inferSelect;
export type InsertTask = typeof tasksTable.$inferInsert;
