import { pgTable, text, serial, timestamp, integer, boolean, jsonb, index } from "drizzle-orm/pg-core";
import { leadsTable } from "./leads";
import { documentsTable } from "./documents";

export interface StageA {
  documentId: number;
  leadId: number;
  detectedBankName: string | null;
  pdfType: "text" | "scanned" | "mixed";
  pageCount: number;
  statementPeriodCandidates: Array<{ value: string; source: string; selected: boolean; reasoning: string }>;
  accountNumberCandidates: Array<{ value: string; source: string; selected: boolean; reasoning: string }>;
}

export interface StageB {
  pages: Array<{
    pageNumber: number;
    rawTextLength: number;
    ocrUsed: boolean;
    ocrFallback: boolean;
    snippets: {
      accountNumber?: string;
      statementPeriod?: string;
      depositSummary?: string;
      transactionTable?: string;
    };
  }>;
}

export interface StageCCandidate {
  field: string;
  value: number;
  pageNumber?: number;
  nearbyText: string;
  regexRuleName: string;
  confidence: "high" | "medium" | "low";
  selected: boolean;
}

export interface StageC {
  candidates: StageCCandidate[];
  splitDepositDetected: boolean;
  splitDepositComponents?: Array<{ label: string; value: number }>;
  finalDepositTotal: number;
  finalDepositSource: "single" | "sum" | "fallback";
}

export interface StageDRow {
  pageNumber?: number;
  rawRowText?: string;
  parsedDate?: string;
  parsedDescription?: string;
  parsedAmount?: number;
  debitOrCredit?: "debit" | "credit";
  runningBalance?: number;
  parserTemplateUsed?: string;
  confidence?: "high" | "medium" | "low";
  rowIndex?: number;
  rejected: boolean;
  rejectionReason?: string;
  columnSelected?: string;
  columnReasoning?: string;
  date?: string;
  description?: string;
  amount?: number;
  category?: string;
}

export interface StageD {
  totalRows: number;
  acceptedRows: number;
  rejectedRows: number;
  rows: StageDRow[];
}

export interface StageECandidate {
  normalizedName?: string;
  matchedLenderDbName?: string;
  transactions?: Array<{ date: string; amount: number; description: string }>;
  guessedFrequency?: string;
  selectedLoanAmount?: number;
  selectionReasoning?: string;
  amountSource?: "parsed_rows" | "ai";
  rejected: boolean;
  rejectionReason?: string;
  accountsFoundIn?: string[];
  deduplicatedFrom?: string[];
  lender?: string;
  amount?: number;
  frequency?: string;
  source?: string;
}

export interface StageE {
  candidates: StageECandidate[];
  deduplicationApplied: boolean;
  crossAccountMerges: Array<string | { lender: string; accounts: string[]; mergedAmount: number }>;
}

export interface StageFEntry {
  field: string;
  originalValue: any;
  aiOutput: any;
  finalValue: any;
  aiChanged: boolean;
  mergePoint?: string;
  reason?: string;
  promptSent?: string;
  aiResponse?: string;
}

export interface StageF {
  entries: StageFEntry[];
  aiModifiedAnyNumeric: boolean;
  aiOverrideBlocked: boolean;
}

export interface NumericSource {
  fieldName: string;
  finalValue: number;
  sourceStage: "A" | "B" | "C" | "D" | "E" | "F";
  sourcePage?: number;
  sourceRawText: string;
  sourceRule: string;
  modifiedByAi: boolean;
}

export interface TraceData {
  stageA: StageA;
  stageB: StageB;
  stageC: StageC;
  stageD: StageD;
  stageE: StageE;
  stageF: StageF;
}

export interface TraceSummary {
  bankName: string | null;
  totalDeposit: number;
  depositSource: string;
  loansFound: number;
  aiOverridesBlocked: number;
  aiChangesApplied: number;
  splitDeposit: boolean;
  crossAccountDedups: number;
}

export const scrubTracesTable = pgTable("scrub_traces", {
  id: serial("id").primaryKey(),
  leadId: integer("lead_id").notNull().references(() => leadsTable.id, { onDelete: "cascade" }),
  documentId: integer("document_id").references(() => documentsTable.id, { onDelete: "set null" }),
  bankName: text("bank_name"),
  traceData: jsonb("trace_data").$type<TraceData>(),
  numericSources: jsonb("numeric_sources").$type<NumericSource[]>(),
  summary: jsonb("summary").$type<TraceSummary>(),
  aiModifiedAnyNumeric: boolean("ai_modified_any_numeric").default(false),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  index("scrub_traces_lead_id_idx").on(table.leadId),
  index("scrub_traces_document_id_idx").on(table.documentId),
  index("scrub_traces_created_at_idx").on(table.createdAt),
]);

export type ScrubTrace = typeof scrubTracesTable.$inferSelect;
export type InsertScrubTrace = typeof scrubTracesTable.$inferInsert;
