import { pgTable, text, serial, timestamp, integer, jsonb } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod/v4";

export const importBatchesTable = pgTable("import_batches", {
  id: serial("id").primaryKey(),
  fileName: text("file_name").notNull(),
  importDate: timestamp("import_date", { withTimezone: true }).notNull().defaultNow(),
  totalRecords: integer("total_records").default(0),
  processedRecords: integer("processed_records").default(0),
  duplicatesFound: integer("duplicates_found").default(0),
  status: text("status").notNull().default("pending"),
  errors: jsonb("errors"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});

export const insertImportBatchSchema = createInsertSchema(importBatchesTable).omit({ id: true, createdAt: true });
export type InsertImportBatch = z.infer<typeof insertImportBatchSchema>;
export type ImportBatch = typeof importBatchesTable.$inferSelect;
