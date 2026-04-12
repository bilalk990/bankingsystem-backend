import { pgTable, text, serial, timestamp, integer, boolean, index } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod/v4";
import { leadsTable } from "./leads";

export const documentsTable = pgTable("documents", {
  id: serial("id").primaryKey(),
  leadId: integer("lead_id").notNull().references(() => leadsTable.id, { onDelete: "cascade" }),
  type: text("type").notNull(),
  name: text("name").notNull(),
  url: text("url").notNull(),
  storageKey: text("storage_key"),
  classifiedType: text("classified_type"),
  classificationConfidence: text("classification_confidence"),
  classifiedAt: timestamp("classified_at", { withTimezone: true }),
  mismatch: boolean("mismatch").default(false),
  cachedRawText: text("cached_raw_text"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  index("documents_lead_id_idx").on(table.leadId),
  index("documents_type_idx").on(table.type),
  index("documents_lead_type_idx").on(table.leadId, table.type),
]);

export const insertDocumentSchema = createInsertSchema(documentsTable).omit({ id: true, createdAt: true });
export type InsertDocument = z.infer<typeof insertDocumentSchema>;
export type Document = typeof documentsTable.$inferSelect;
