import { pgTable, text, serial, timestamp, integer, boolean, jsonb } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod/v4";
import { usersTable } from "./users";

export const webhooksTable = pgTable("webhooks", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
  type: text("type").notNull(),
  webhookKey: text("webhook_key").notNull().unique(),
  fieldMapping: jsonb("field_mapping"),
  isActive: boolean("is_active").notNull().default(true),
  lastTriggered: timestamp("last_triggered", { withTimezone: true }),
  triggerCount: integer("trigger_count").notNull().default(0),
  createdById: integer("created_by_id").references(() => usersTable.id),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});

export const insertWebhookSchema = createInsertSchema(webhooksTable).omit({ id: true, createdAt: true, lastTriggered: true, triggerCount: true });
export type InsertWebhook = z.infer<typeof insertWebhookSchema>;
export type Webhook = typeof webhooksTable.$inferSelect;
