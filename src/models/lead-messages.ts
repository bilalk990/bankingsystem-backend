import { pgTable, text, serial, timestamp, integer, boolean, jsonb, index } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod/v4";
import { leadsTable } from "./leads";
import { usersTable } from "./users";

export const leadMessagesTable = pgTable("lead_messages", {
  id: serial("id").primaryKey(),
  leadId: integer("lead_id").notNull().references(() => leadsTable.id),
  source: text("source").notNull(),
  direction: text("direction").notNull().default("inbound"),
  content: text("content"),
  senderName: text("sender_name"),
  metadata: jsonb("metadata"),
  isHotTrigger: boolean("is_hot_trigger").notNull().default(false),
  webhookId: integer("webhook_id"),
  userId: integer("user_id").references(() => usersTable.id),
  isRead: boolean("is_read").notNull().default(false),
  messageType: text("message_type").default("sms"),
  aiGenerated: boolean("ai_generated").notNull().default(false),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  index("lead_messages_lead_id_idx").on(table.leadId),
  index("lead_messages_user_id_idx").on(table.userId),
  index("lead_messages_is_read_idx").on(table.isRead),
  index("lead_messages_created_at_idx").on(table.createdAt),
]);

export const insertLeadMessageSchema = createInsertSchema(leadMessagesTable).omit({ id: true, createdAt: true });
export type InsertLeadMessage = z.infer<typeof insertLeadMessageSchema>;
export type LeadMessage = typeof leadMessagesTable.$inferSelect;
