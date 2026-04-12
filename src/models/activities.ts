import { pgTable, text, serial, timestamp, integer, jsonb, index } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod/v4";
import { leadsTable } from "./leads";

export const activitiesTable = pgTable("activities", {
  id: serial("id").primaryKey(),
  leadId: integer("lead_id").notNull().references(() => leadsTable.id, { onDelete: "cascade" }),
  userId: integer("user_id"),
  type: text("type").notNull(),
  description: text("description").notNull(),
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  index("activities_lead_id_idx").on(table.leadId),
  index("activities_user_id_idx").on(table.userId),
  index("activities_created_at_idx").on(table.createdAt),
  index("activities_type_idx").on(table.type),
]);

export const insertActivitySchema = createInsertSchema(activitiesTable).omit({ id: true, createdAt: true });
export type InsertActivity = z.infer<typeof insertActivitySchema>;
export type Activity = typeof activitiesTable.$inferSelect;
