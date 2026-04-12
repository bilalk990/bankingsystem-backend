import { pgTable, serial, text, integer, timestamp, jsonb } from "drizzle-orm/pg-core";

export const uploadBatchesTable = pgTable("upload_batches", {
  id: serial("id").primaryKey(),
  fileName: text("file_name").notNull(),
  status: text("status").notNull().default("processing"),
  totalFolders: integer("total_folders").default(0),
  matchedFolders: integer("matched_folders").default(0),
  totalFiles: integer("total_files").default(0),
  matchedFiles: integer("matched_files").default(0),
  unmatchedFolders: jsonb("unmatched_folders"),
  matchDetails: jsonb("match_details"),
  sourceTier: text("source_tier"),
  error: text("error"),
  extractDir: text("extract_dir"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  completedAt: timestamp("completed_at", { withTimezone: true }),
});
