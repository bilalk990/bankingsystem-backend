import { pgTable, text, serial, timestamp, integer, boolean } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod/v4";
import { usersTable } from "./users";

export const phoneNumbersTable = pgTable("phone_numbers", {
  id: serial("id").primaryKey(),
  number: text("number").notNull().unique(),
  friendlyName: text("friendly_name"),
  areaCode: text("area_code").notNull(),
  state: text("state"),
  region: text("region"),
  provider: text("provider").default("twilio"),
  status: text("status").notNull().default("available"),
  assignedToId: integer("assigned_to_id").references(() => usersTable.id),
  isPrimary: boolean("is_primary").default(false),
  capabilities: text("capabilities").default("voice,sms"),
  monthlyFee: text("monthly_fee"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  lastUsedAt: timestamp("last_used_at", { withTimezone: true }),
});

export const insertPhoneNumberSchema = createInsertSchema(phoneNumbersTable).omit({ id: true, createdAt: true });
export type InsertPhoneNumber = z.infer<typeof insertPhoneNumberSchema>;
export type PhoneNumber = typeof phoneNumbersTable.$inferSelect;
