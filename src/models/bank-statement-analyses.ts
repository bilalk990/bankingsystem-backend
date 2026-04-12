import { pgTable, text, serial, timestamp, integer, doublePrecision, boolean, jsonb, index, numeric } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod/v4";
import { leadsTable } from "./leads";
import { documentsTable } from "./documents";

export const bankStatementAnalysesTable = pgTable("bank_statement_analyses", {
  id: serial("id").primaryKey(),
  leadId: integer("lead_id").notNull().references(() => leadsTable.id, { onDelete: "cascade" }),
  documentId: integer("document_id").references(() => documentsTable.id, { onDelete: "set null" }),
  hasLoans: boolean("has_loans").default(false),
  hasExistingLoans: boolean("has_existing_loans").default(false),
  bankName: text("bank_name"),
  loanDetails: jsonb("loan_details"),
  monthlyRevenues: jsonb("monthly_revenues"),
  avgDailyBalance: numeric("avg_daily_balance", { precision: 20, scale: 2 }),
  revenueTrend: text("revenue_trend"),
  riskFactors: jsonb("risk_factors"),
  riskScore: text("risk_score"),
  hasOnDeck: boolean("has_on_deck").default(false),
  grossRevenue: numeric("gross_revenue", { precision: 20, scale: 2 }),
  negativeDays: jsonb("negative_days"),
  nsfCount: integer("nsf_count").default(0),
  aiRawAnalysis: text("ai_raw_analysis"),
  extractedStatementText: text("extracted_statement_text"),
  statementMonth: text("statement_month"),
  accountNumber: text("account_number"),
  businessNameOnStatement: text("business_name_on_statement"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  index("bsa_lead_id_idx").on(table.leadId),
  index("bsa_document_id_idx").on(table.documentId),
  index("bsa_risk_score_idx").on(table.riskScore),
  index("bsa_created_at_idx").on(table.createdAt),
]);

export const insertBankStatementAnalysisSchema = createInsertSchema(bankStatementAnalysesTable).omit({ id: true, createdAt: true });
export type InsertBankStatementAnalysis = z.infer<typeof insertBankStatementAnalysisSchema>;
export type BankStatementAnalysis = typeof bankStatementAnalysesTable.$inferSelect;
