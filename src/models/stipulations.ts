import { pgTable, text, serial, timestamp, boolean, integer } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod/v4";
import { dealsTable } from "./deals";

export const stipulationsTable = pgTable("stipulations", {
  id: serial("id").primaryKey(),
  dealId: integer("deal_id").notNull().references(() => dealsTable.id, { onDelete: "cascade" }),
  name: text("name").notNull(),
  type: text("type").notNull().default("document"),
  required: boolean("required").default(true),
  completed: boolean("completed").default(false),
  completedAt: timestamp("completed_at", { withTimezone: true }),
  completedById: integer("completed_by_id"),
  notes: text("notes"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});

export const insertStipulationSchema = createInsertSchema(stipulationsTable).omit({ id: true, createdAt: true });
export type InsertStipulation = z.infer<typeof insertStipulationSchema>;
export type Stipulation = typeof stipulationsTable.$inferSelect;
