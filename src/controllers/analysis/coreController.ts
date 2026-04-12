import fs from "fs";
import path from "path";
import { execSync } from "child_process";
import os from "os";
import { eq, sql, and } from "drizzle-orm";
import { db, leadsTable, documentsTable, bankStatementAnalysesTable, underwritingConfirmationsTable, lenderRulesTable } from "../../configs/database";
import { anthropic } from "../../integrations/anthropic";
import { parseStatementData, formatParsedDataForPrompt } from "../../services/documentAiService";
import { extractAllTransactions, extractFromParsedTransactions, TransactionEngineResult, normalizeLenderName, areSameLender, deduplicateLoansAcrossAccounts, consolidateSameLenderEntries } from "../../services/transactionEngine";
import { identifyBank, findBankByName } from "../../services/bankTemplates";
import { scrubTracesTable, type TraceData, type StageA, type StageB, type StageC, type StageD, type StageE, type StageF, type NumericSource, type TraceSummary, type StageCCandidate, type StageDRow, type StageECandidate, type StageFEntry } from "../../models/scrub-traces";
import { extractAccountLast4, extractAccountFromFilename, extractAllAccountLast4s } from "../../services/accountExtractor";
import { roundToTwo } from "../../utils/math";

const AI_MODEL = "claude-sonnet-4-5";
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;
const CONCURRENCY_LIMIT = 3;

function scrubLog(tag: string, msg: string) {
  const e = new Error();
  const frame = e.stack?.split("\n")[2] || "";
  const match = frame.match(/(?:coreController\.ts|transactionEngine\.ts|underwritingController\.ts):(\d+)/);
  const line = match ? `:${match[1]}` : "";
  console.log(`[${tag}${line}] ${msg}`);
}

class ScrubTrace {
  private entries: Array<{ ts: number; step: string; detail: string; level: "INFO" | "FIX" | "WARN" | "ERROR" }> = [];
  private leadId: number;
  private docName: string;

  constructor(leadId: number, docName: string = "") {
    this.leadId = leadId;
    this.docName = docName;
  }

  private getCallerLine(): string {
    const e = new Error();
    const frame = e.stack?.split("\n")[3] || "";
    const match = frame.match(/(?:coreController\.ts|transactionEngine\.ts|underwritingController\.ts):(\d+)/);
    return match ? `:${match[1]}` : "";
  }

  info(step: string, detail: string) {
    this.entries.push({ ts: Date.now(), step, detail, level: "INFO" });
    console.log(`[TRACE L${this.leadId}${this.getCallerLine()}] ${step}: ${detail}`);
  }

  fix(step: string, detail: string) {
    this.entries.push({ ts: Date.now(), step, detail, level: "FIX" });
    console.log(`[TRACE L${this.leadId}${this.getCallerLine()}] FIX ${step}: ${detail}`);
  }

  warn(step: string, detail: string) {
    this.entries.push({ ts: Date.now(), step, detail, level: "WARN" });
    console.log(`[TRACE L${this.leadId}${this.getCallerLine()}] ⚠ ${step}: ${detail}`);
  }

  error(step: string, detail: string) {
    this.entries.push({ ts: Date.now(), step, detail, level: "ERROR" });
    console.log(`[TRACE L${this.leadId}${this.getCallerLine()}] ✗ ${step}: ${detail}`);
  }

  getSummary(): string {
    const fixes = this.entries.filter(e => e.level === "FIX").length;
    const warns = this.entries.filter(e => e.level === "WARN").length;
    const errors = this.entries.filter(e => e.level === "ERROR").length;
    return `Lead ${this.leadId} (${this.docName}): ${this.entries.length} steps, ${fixes} fixes, ${warns} warnings, ${errors} errors`;
  }

  getDetailedLog(): string {
    return this.entries.map(e => {
      const time = new Date(e.ts).toISOString().slice(11, 23);
      return `[${time}] [${e.level}] ${e.step}: ${e.detail}`;
    }).join("\n");
  }

  getEntries() { return this.entries; }
}

const activeScrubTraces = new Map<number, ScrubTrace>();
let currentActiveLeadId: number | null = null;

export function getScrubTrace(leadId: number): ScrubTrace | undefined {
  return activeScrubTraces.get(leadId);
}

function getActiveTrace(): ScrubTrace | null {
  if (currentActiveLeadId !== null) return activeScrubTraces.get(currentActiveLeadId) || null;
  return null;
}

export function getScrubTraceLog(leadId: number): string | null {
  const trace = activeScrubTraces.get(leadId);
  return trace ? trace.getDetailedLog() : null;
}

class StructuredTraceCollector {
  stageA: StageA = {
    documentId: 0, leadId: 0, detectedBankName: null, pdfType: "text",
    pageCount: 0, statementPeriodCandidates: [], accountNumberCandidates: [],
  };
  stageB: StageB = { pages: [] };
  stageC: StageC = {
    candidates: [], splitDepositDetected: false,
    finalDepositTotal: 0, finalDepositSource: "single",
  };
  stageD: StageD = { totalRows: 0, acceptedRows: 0, rejectedRows: 0, rows: [] };
  stageE: StageE = { candidates: [], deduplicationApplied: false, crossAccountMerges: [] };
  stageF: StageF = { entries: [], aiModifiedAnyNumeric: false, aiOverrideBlocked: false };
  numericSources: NumericSource[] = [];
  leadId: number;
  documentId: number;
  bankName: string | null = null;

  constructor(leadId: number, documentId: number) {
    this.leadId = leadId;
    this.documentId = documentId;
    this.stageA.leadId = leadId;
    this.stageA.documentId = documentId;
  }

  addDepositCandidate(c: StageCCandidate) {
    this.stageC.candidates.push(c);
  }

  addTransactionRow(r: StageDRow) {
    this.stageD.rows.push(r);
    this.stageD.totalRows++;
    if (r.rejected) this.stageD.rejectedRows++;
    else this.stageD.acceptedRows++;
  }

  addLoanCandidate(c: StageECandidate) {
    this.stageE.candidates.push(c);
  }

  addAIChange(entry: StageFEntry) {
    this.stageF.entries.push(entry);
    if (entry.aiChanged && typeof entry.originalValue === "number") {
      this.stageF.aiModifiedAnyNumeric = true;
    }
  }

  addNumericSource(ns: NumericSource) {
    this.numericSources.push(ns);
  }

  getSummary(): TraceSummary {
    return {
      bankName: this.bankName,
      totalDeposit: this.stageC.finalDepositTotal,
      depositSource: this.stageC.finalDepositSource,
      loansFound: this.stageE.candidates.filter(c => !c.rejected).length,
      aiOverridesBlocked: this.stageF.entries.filter(e => !e.aiChanged && e.originalValue !== e.aiOutput).length,
      aiChangesApplied: this.stageF.entries.filter(e => e.aiChanged).length,
      splitDeposit: this.stageC.splitDepositDetected,
      crossAccountDedups: this.stageE.crossAccountMerges.length,
    };
  }

  getTraceData(): TraceData {
    return {
      stageA: this.stageA,
      stageB: this.stageB,
      stageC: this.stageC,
      stageD: this.stageD,
      stageE: this.stageE,
      stageF: this.stageF,
    };
  }
}

const activeStructuredTraces = new Map<number, StructuredTraceCollector>();

export function getStructuredTrace(leadId: number): StructuredTraceCollector | undefined {
  return activeStructuredTraces.get(leadId);
}

async function saveTraceToDb(collector: StructuredTraceCollector): Promise<void> {
  try {
    const existing = await db.select({ id: scrubTracesTable.id })
      .from(scrubTracesTable)
      .where(eq(scrubTracesTable.leadId, collector.leadId))
      .orderBy(sql`created_at DESC`);

    if (existing.length >= 3) {
      const toDelete = existing.slice(2).map(e => e.id);
      for (const id of toDelete) {
        await db.delete(scrubTracesTable).where(eq(scrubTracesTable.id, id));
      }
    }

    await db.insert(scrubTracesTable).values({
      leadId: collector.leadId,
      documentId: collector.documentId || null,
      bankName: collector.bankName,
      traceData: collector.getTraceData(),
      numericSources: collector.numericSources,
      summary: collector.getSummary(),
      aiModifiedAnyNumeric: collector.stageF.aiModifiedAnyNumeric,
    });
  } catch (err) {
    console.error(`[TraceCollector] Failed to save trace for lead ${collector.leadId}:`, err);
  }
}

const aiCallQueue: (() => void)[] = [];
let aiCallsInFlight = 0;
const AI_MAX_CONCURRENT = 3;

const SONNET_INPUT_COST_PER_MTOK = 3.00;
const SONNET_OUTPUT_COST_PER_MTOK = 15.00;
const CHARS_PER_TOKEN_ESTIMATE = 4;

interface CostEntry {
  timestamp: number;
  leadId: number | null;
  callType: string;
  inputTokens: number;
  outputTokens: number;
  costUsd: number;
}

const costTracker = {
  entries: [] as CostEntry[],
  totalCostUsd: 0,
  totalInputTokens: 0,
  totalOutputTokens: 0,
  totalCalls: 0,

  record(leadId: number | null, callType: string, inputTokens: number, outputTokens: number) {
    const inputCost = (inputTokens / 1_000_000) * SONNET_INPUT_COST_PER_MTOK;
    const outputCost = (outputTokens / 1_000_000) * SONNET_OUTPUT_COST_PER_MTOK;
    const costUsd = inputCost + outputCost;
    const entry: CostEntry = { timestamp: Date.now(), leadId, callType, inputTokens, outputTokens, costUsd };
    this.entries.push(entry);
    if (this.entries.length > 5000) this.entries = this.entries.slice(-2500);
    this.totalCostUsd += costUsd;
    this.totalInputTokens += inputTokens;
    this.totalOutputTokens += outputTokens;
    this.totalCalls++;
    // console.log(`[AI-Cost] ${callType}: ${inputTokens} in / ${outputTokens} out = $${costUsd.toFixed(4)} (running total: $${this.totalCostUsd.toFixed(4)})`);
  },

  getLeadCost(leadId: number): { calls: number; inputTokens: number; outputTokens: number; costUsd: number } {
    const leadEntries = this.entries.filter(e => e.leadId === leadId);
    return {
      calls: leadEntries.length,
      inputTokens: leadEntries.reduce((s, e) => s + e.inputTokens, 0),
      outputTokens: leadEntries.reduce((s, e) => s + e.outputTokens, 0),
      costUsd: leadEntries.reduce((s, e) => s + e.costUsd, 0),
    };
  },

  getSummary() {
    const last24h = this.entries.filter(e => e.timestamp > Date.now() - 86400000);
    const last24hCost = last24h.reduce((s, e) => s + e.costUsd, 0);
    const uniqueLeads = new Set(last24h.filter(e => e.leadId).map(e => e.leadId)).size;
    const avgPerLead = uniqueLeads > 0 ? last24hCost / uniqueLeads : 0;
    return {
      totalCalls: this.totalCalls,
      totalCostUsd: Math.round(this.totalCostUsd * 10000) / 10000,
      totalInputTokens: this.totalInputTokens,
      totalOutputTokens: this.totalOutputTokens,
      last24h: {
        calls: last24h.length,
        costUsd: Math.round(last24hCost * 10000) / 10000,
        leadsProcessed: uniqueLeads,
        avgCostPerLead: Math.round(avgPerLead * 10000) / 10000,
      },
      recentEntries: this.entries.slice(-20).map(e => ({
        ...e, costUsd: Math.round(e.costUsd * 10000) / 10000,
      })),
    };
  },
};

export { costTracker };

export function acquireAiSlot(): Promise<void> {
  return new Promise((resolve) => {
    if (aiCallsInFlight < AI_MAX_CONCURRENT) {
      aiCallsInFlight++;
      resolve();
    } else {
      aiCallQueue.push(() => {
        aiCallsInFlight++;
        resolve();
      });
    }
  });
}

export function releaseAiSlot(): void {
  aiCallsInFlight--;
  if (aiCallQueue.length > 0 && aiCallsInFlight < AI_MAX_CONCURRENT) {
    const next = aiCallQueue.shift();
    if (next) next();
  }
}

export interface AnalysisResult {
  hasLoans: boolean;
  hasOnDeck: boolean;
  loanDetails: any[];
  monthlyRevenues: any[];
  avgDailyBalance: number;
  revenueTrend: string;
  negativeDays: any[];
  nsfCount: number;
  riskFactors: string[];
  riskScore: string;
  grossRevenue: number;
  hasExistingLoans: boolean;
  bankName: string | null;
  accountNumber?: string | null;
  businessNameOnStatement?: string | null;
  nameMatchConfidence?: string | null;
  notableTransactions?: any[];
  summary?: string;
  verificationNotes?: string;
  recurringPulls?: any[];
  estimatedApprovalAmount?: number;
  hasNegativeBalance?: boolean;
  lowestBalance?: number;
  depositReviewFlags?: Array<{
    month: string;
    account: string;
    aiAmount: number;
    parserAmount: number;
    reason: string;
    usedValue: number;
  }>;
}

export interface DepositReviewFlag {
  month: string;
  account: string;
  aiAmount: number;
  parserAmount: number;
  reason: string;
  usedValue: number;
}

export interface BackgroundJob {
  id: string;
  status: "running" | "completed" | "error";
  total: number;
  processed: number;
  currentLead: string;
  results: { leadId: number; businessName: string; status: string; riskScore?: string; error?: string; note?: string }[];
  startedAt: number;
  completedAt?: number;
  error?: string;
}

export const backgroundJobs = new Map<string, BackgroundJob>();
export let scrubCancelled = false;
export function cancelScrubbing() { scrubCancelled = true; }
export function resetScrubCancel() { scrubCancelled = false; }

let _cachedLearningContext: string = "";
let _cachedLearningContextAt: number = 0;

async function ocrScannedPdf(buffer: Buffer, fileName: string, maxPages = 15): Promise<string> {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ocr-"));
  const pdfPath = path.join(tmpDir, "input.pdf");
  try {
    fs.writeFileSync(pdfPath, buffer);
    let pageFiles: string[] = [];
    const attempts = [
      { dpi: 200, timeout: 60000, label: "200dpi/60s" },
      { dpi: 150, timeout: 90000, label: "150dpi/90s" },
      { dpi: 100, timeout: 120000, label: "100dpi/120s" },
    ];
    for (const attempt of attempts) {
      try {
        const existingPages = fs.readdirSync(tmpDir).filter(f => f.startsWith("page") && f.endsWith(".png"));
        for (const p of existingPages) fs.unlinkSync(path.join(tmpDir, p));
        execSync(`pdftoppm -png -r ${attempt.dpi} -l ${maxPages} "${pdfPath}" "${tmpDir}/page" 2>/dev/null`, { timeout: attempt.timeout });
        pageFiles = fs.readdirSync(tmpDir)
          .filter(f => f.startsWith("page") && f.endsWith(".png"))
          .sort();
        if (pageFiles.length > 0) {
//           scrubLog("OCR", `${fileName}: pdftoppm succeeded at ${attempt.label}, ${pageFiles.length} pages`);
          break;
        }
      } catch (retryErr: any) {
//         scrubLog("OCR", `${fileName}: pdftoppm failed at ${attempt.label}: ${retryErr.message}`);
      }
    }
    if (pageFiles.length === 0) {
//       scrubLog("OCR", `${fileName}: pdftoppm failed all retries, trying direct PDF-to-Claude vision`);
      const pdfBase64 = buffer.toString("base64");
      if (pdfBase64.length < 20 * 1024 * 1024) {
        try {
          await acquireAiSlot();
          let directResponse;
          try {
            directResponse = await anthropic.messages.create({
              model: "claude-sonnet-4-5",
              max_tokens: 8000,
              messages: [{ role: "user", content: [
                { type: "document" as const, source: { type: "base64" as const, media_type: "application/pdf" as const, data: pdfBase64 } },
                { type: "text" as const, text: "Extract ALL text from this bank statement PDF exactly as shown. Include the bank name, account number (preserve masked digits like xxxxxxx1234), statement period/dates, every transaction date, description, amount, and running balance. Include any summary sections showing total deposits, total withdrawals, beginning/ending balance. Preserve the table structure. Output ONLY the extracted text, no commentary." }
              ] }],
            });
          } finally {
            releaseAiSlot();
          }
          const dInputTok = (directResponse.usage as any)?.input_tokens || Math.ceil(pdfBase64.length / 10);
          const dOutputTok = (directResponse.usage as any)?.output_tokens || 0;
          costTracker.record(null, "ocr-vision-pdf", dInputTok, dOutputTok);
          const directText = (directResponse.content[0] as any)?.text || "";
          if (directText.length > 100) {
//             scrubLog("OCR", `${fileName}: direct PDF vision extracted ${directText.length} chars`);
            return `[PDF OCR: direct vision]\n${directText}`;
          }
        } catch (directErr: any) {
//           scrubLog("OCR", `${fileName}: direct PDF vision failed: ${directErr.message}`);
        }
      }
      return "[OCR: pdftoppm produced no images after all retries]";
    }
    const imageContents: any[] = [];
    for (const pf of pageFiles.slice(0, maxPages)) {
      const imgBuf = fs.readFileSync(path.join(tmpDir, pf));
      const base64 = imgBuf.toString("base64");
      imageContents.push({
        type: "image" as const,
        source: { type: "base64" as const, media_type: "image/png" as const, data: base64 }
      });
    }
    imageContents.push({
      type: "text" as const,
      text: "Extract ALL text from these bank statement pages exactly as shown. Include the bank name, account number (preserve masked digits like xxxxxxx1234), statement period/dates, every transaction date, description, amount, and running balance. Include any summary sections showing total deposits, total withdrawals, beginning/ending balance. Preserve the table structure. Output ONLY the extracted text, no commentary."
    });
    let ocrText = "";
    try {
      await acquireAiSlot();
      let response;
      try {
        response = await anthropic.messages.create({
          model: "claude-sonnet-4-5",
          max_tokens: 8000,
          messages: [{ role: "user", content: imageContents }],
        });
      } finally {
        releaseAiSlot();
      }
      const ocrInputTok = (response.usage as any)?.input_tokens || Math.ceil(pageFiles.length * 2000);
      const ocrOutputTok = (response.usage as any)?.output_tokens || 0;
      costTracker.record(null, "ocr-vision-pages", ocrInputTok, ocrOutputTok);
      ocrText = (response.content[0] as any)?.text || "";
//       scrubLog("OCR", `${fileName}: Claude vision extracted ${ocrText.length} chars from ${pageFiles.length} pages`);
    } catch (claudeErr: any) {
//       scrubLog("OCR", `${fileName}: Claude vision failed (${claudeErr.message}), falling back to Tesseract`);
    }
    if (!ocrText || ocrText.length < 100) {
      try {
        const tesseractPages: string[] = [];
        for (const pf of pageFiles.slice(0, maxPages)) {
          const imgPath = path.join(tmpDir, pf);
          try {
            const pageText = execSync(`tesseract "${imgPath}" stdout -l eng --psm 6 2>/dev/null`, {
              timeout: 30000,
              maxBuffer: 10 * 1024 * 1024,
            }).toString("utf-8").trim();
            if (pageText.length > 20) tesseractPages.push(pageText);
          } catch {}
        }
        if (tesseractPages.length > 0) {
          ocrText = tesseractPages.join("\n\n--- Page Break ---\n\n");
//           scrubLog("OCR", `${fileName}: Tesseract extracted ${ocrText.length} chars from ${tesseractPages.length}/${pageFiles.length} pages`);
        }
      } catch (tessErr: any) {
//         scrubLog("OCR", `${fileName}: Tesseract fallback failed: ${tessErr.message}`);
      }
    }
    if (ocrText && ocrText.length >= 100) {
      return `[PDF OCR: ${pageFiles.length} pages]\n${ocrText}`;
    }
    return "[OCR produced no text]";
  } catch (e: any) {
    // console.error(`[OCR] Error for ${fileName}:`, e.message);
    return `[OCR error: ${e.message}]`;
  } finally {
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}
  }
}

interface AccountSection {
  accountNumber: string;
  accountType: string;
  month: string;
  year: string;
  pages: number[];
}

async function extractTextPerPage(buffer: Buffer): Promise<Map<number, string>> {
  const pageTexts = new Map<number, string>();
  try {
    const pdfParse = (await import("pdf-parse") as any).default;
    let pageNum = 0;
    await pdfParse(buffer, {
      pagerender: async (pageData: any) => {
        pageNum++;
        const textContent = await pageData.getTextContent();
        const text = textContent.items.map((item: any) => item.str).join(" ");
        pageTexts.set(pageNum, text);
        return text;
      }
    });
  } catch {}
  return pageTexts;
}

function detectAccountSections(pageTexts: Map<number, string>): AccountSection[] {
  const months: Record<string, string> = {
    "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr", "05": "May", "06": "Jun",
    "07": "Jul", "08": "Aug", "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec",
    january: "Jan", february: "Feb", march: "Mar", april: "Apr", may: "May", june: "Jun",
    july: "Jul", august: "Aug", september: "Sep", october: "Oct", november: "Nov", december: "Dec",
  };

  const sections: AccountSection[] = [];
  const seen = new Map<string, number>();

  for (const [pageNum, text] of [...pageTexts.entries()].sort((a, b) => a[0] - b[0])) {
    const numericPeriod = text.match(/(?:Statement\s*Period|Period)[:\s]*(\d{1,2})\/(\d{1,2})\/(\d{2,4})\s*[-–to]+\s*(\d{1,2})\/(\d{1,2})\/(\d{2,4})/i);
    const wordPeriod = text.match(/(?:for|period|through|ending)[:\s]*(?:.*?(?:to|through|-|–)\s*)?(\w+)\s+\d{1,2}[\s,]*(\d{4})/i);

    let mo = "", yr = "";
    if (numericPeriod) {
      const endMo = numericPeriod[4].padStart(2, "0");
      mo = months[endMo] || "";
      yr = numericPeriod[6].length === 4 ? numericPeriod[6].slice(2) : numericPeriod[6];
    } else if (wordPeriod) {
      const mw = wordPeriod[1].toLowerCase();
      for (const [key, abbr] of Object.entries(months)) {
        if (mw.startsWith(key.slice(0, 3))) { mo = abbr; break; }
      }
      yr = wordPeriod[2].slice(2);
    }

    const acctPatterns: { pattern: RegExp; type: string }[] = [
      { pattern: /(?:Account\s*(?:Number|No\.?|#))[:\s]*[-\s]*([\d][\d\s-]{4,}[\d])/i, type: "Account" },
      { pattern: /(?:Account\s*(?:Number|No\.?|#))[:\s]*[-\s]*(?:[xX.*●•✱◆■□○]+[\s-]*)(\d{4})/i, type: "Account" },
      { pattern: /(?:Acct\.?\s*(?:No\.?|#|Number))[:\s]*[-\s]*(?:[xX.*●•✱◆■□○]+[\s-]*)(\d{4})/i, type: "Account" },
      { pattern: /(?:Statement\s*(?:Number|No\.?))[:\s]*[-\s]*(\d{4,})/i, type: "Account" },
      { pattern: /(?:Checking|Business\s*Checking|Bus\s*Plus\s*Checking|Primary\s*Checking)[\s\S]{0,40}?([\d][\d\s-]{4,}[\d])/i, type: "Checking" },
      { pattern: /(?:Checking|Business\s*Checking|Bus\s*Plus\s*Checking|Primary\s*Checking)\s*[-–—]?\s*(?:[xX.*●•✱◆■□○]+[\s-]*)(\d{4})/i, type: "Checking" },
      { pattern: /(?:Savings|Money\s*Market|Share\s*Draft|Share\s*Account)[\s\S]{0,40}?([\d][\d\s-]{4,}[\d])/i, type: "Savings" },
      { pattern: /(?:Savings|Money\s*Market|Share\s*Draft|Share\s*Account)\s*[-–—]?\s*(?:[xX.*●•✱◆■□○]+[\s-]*)(\d{4})/i, type: "Savings" },
      { pattern: /(?:member\s*(?:number|no\.?|#))[:\s]*[-\s]*(\d{4,})/i, type: "Account" },
    ];
    let acctNum = "";
    let acctType = "Account";
    for (const { pattern, type } of acctPatterns) {
      const m = text.match(pattern);
      if (m) {
        acctNum = m[1].replace(/[\s-]/g, "");
        if (acctNum.length >= 4) {
          acctType = type;
          break;
        }
        acctNum = "";
      }
    }

    if (mo && yr && acctNum) {
      const key = `${mo}${yr}-${acctNum}`;
      if (seen.has(key)) {
        const idx = seen.get(key)!;
        if (!sections[idx].pages.includes(pageNum)) sections[idx].pages.push(pageNum);
      } else {
        seen.set(key, sections.length);
        sections.push({ accountNumber: acctNum, accountType: acctType, month: mo, year: yr, pages: [pageNum] });
      }
    } else if (acctNum && (!mo || !yr)) {
      const last4 = acctNum.slice(-4);
      let matchIdx = -1;
      for (let i = sections.length - 1; i >= 0; i--) {
        if (sections[i].accountNumber.slice(-4) === last4) { matchIdx = i; break; }
      }
      if (matchIdx >= 0) {
        if (!sections[matchIdx].pages.includes(pageNum)) sections[matchIdx].pages.push(pageNum);
      } else if (sections.length > 0) {
        const lastIdx = sections.length - 1;
        if (!sections[lastIdx].pages.includes(pageNum)) sections[lastIdx].pages.push(pageNum);
      }
    } else if (sections.length > 0) {
      const lastIdx = sections.length - 1;
      if (!sections[lastIdx].pages.includes(pageNum)) sections[lastIdx].pages.push(pageNum);
    }
  }

  return sections;
}

export async function splitMultiAccountPdf(
  buffer: Buffer,
  originalDoc: { id: number; name: string; leadId: number; url: string; storageKey?: string | null },
): Promise<{ split: boolean; newDocs: { name: string; buffer: Buffer; month: string; acct: string }[] }> {
  const pageTexts = await extractTextPerPage(buffer);
  if (pageTexts.size < 2) return { split: false, newDocs: [] };

  const sections = detectAccountSections(pageTexts);
  const uniqueAccounts = new Set(sections.map(s => s.accountNumber));

  if (uniqueAccounts.size <= 1 || sections.length <= 1) return { split: false, newDocs: [] };

  // console.log(`[PDF Split] ${originalDoc.name}: detected ${sections.length} account sections across ${uniqueAccounts.size} accounts`);

  try {
    const { PDFDocument } = await import("pdf-lib");
    const srcDoc = await PDFDocument.load(buffer, { ignoreEncryption: true });
    const totalPages = srcDoc.getPageCount();
    const newDocs: { name: string; buffer: Buffer; month: string; acct: string }[] = [];

    const assignedPages = new Set<number>();
    for (const section of sections) {
      for (const p of section.pages) assignedPages.add(p);
    }
    for (let p = 1; p <= totalPages; p++) {
      if (!assignedPages.has(p) && sections.length > 0) {
        let nearest = sections[0];
        for (const s of sections) {
          const maxPage = Math.max(...s.pages);
          if (maxPage < p) nearest = s;
          else break;
        }
        nearest.pages.push(p);
      }
    }

    for (const section of sections) {
      const last4 = section.accountNumber.slice(-4);
      const label = `${section.month}${section.year}_${last4}`;
      const baseName = originalDoc.name.replace(/\.pdf$/i, "");

      const newPdf = await PDFDocument.create();
      const sortedPages = [...new Set(section.pages)].sort((a, b) => a - b);
      const pageIndices = sortedPages.map(p => p - 1).filter(i => i >= 0 && i < totalPages);

      if (pageIndices.length === 0) continue;

      const copiedPages = await newPdf.copyPages(srcDoc, pageIndices);
      for (const page of copiedPages) newPdf.addPage(page);

      const pdfBytes = await newPdf.save();
      newDocs.push({
        name: `${baseName}_${label}.pdf`,
        buffer: Buffer.from(pdfBytes),
        month: `${section.month}${section.year}`,
        acct: last4,
      });

      // console.log(`[PDF Split] Created: ${label} (${pageIndices.length} pages)`);
    }

    return { split: true, newDocs };
  } catch (e: any) {
    // console.error(`[PDF Split] Error splitting ${originalDoc.name}:`, e.message);
    return { split: false, newDocs: [] };
  }
}

function isTextGarbled(text: string): boolean {
  const sample = text.slice(0, 5000).toLowerCase();
  const bankingTerms = [
    "deposit", "balance", "checking", "savings", "account", "withdrawal",
    "credit", "debit", "statement", "transaction", "transfer", "payment",
    "beginning", "ending", "total", "service", "fee", "interest", "january",
    "february", "march", "april", "may", "june", "july", "august",
    "september", "october", "november", "december", "bank",
  ];
  let matchCount = 0;
  for (const term of bankingTerms) {
    if (sample.includes(term)) matchCount++;
  }
  const words = sample.split(/\s+/).filter(w => w.length >= 3);
  const asciiWords = words.filter(w => /^[a-z0-9.,/$#@&()\-]+$/i.test(w));
  const asciiRatio = words.length > 0 ? asciiWords.length / words.length : 0;

  const bodySample = text.slice(500, 5000).toLowerCase();
  const bodyBankingTerms = [
    "deposit", "balance", "checking", "account", "withdrawal",
    "credit", "debit", "statement", "transaction", "transfer", "payment",
    "beginning", "ending", "total",
  ];
  let bodyMatchCount = 0;
  for (const term of bodyBankingTerms) {
    if (bodySample.includes(term)) bodyMatchCount++;
  }

  const hasAmounts = /\$?\d{1,3}(,\d{3})*\.\d{2}/.test(bodySample);
  const hasDates = /\d{1,2}\/\d{1,2}\/\d{2,4}/.test(bodySample);

  if (bodySample.length > 500 && bodyMatchCount === 0 && !hasAmounts && !hasDates) {
//     scrubLog("GarbledCheck", `Header has ${matchCount} terms but body (500+) has 0 terms, no amounts, no dates — font substitution detected`);
    return true;
  }

  if (matchCount >= 3) return false;
  if (matchCount === 0 && asciiRatio < 0.5) return true;
  if (matchCount <= 1 && asciiRatio < 0.3) return true;
  return false;
}

function normalizePdfAmounts(text: string): string {
  return text
    .replace(/(\d),(\d{3})\s{2,}\.(\d{2})\b/g, "$1,$2.$3")
    .replace(/(\d{1,3})\s{2,}\.(\d{2})\b/g, "$1.$2");
}

async function extractBatchWithPdfplumber(files: { key: string; filePath: string; fileName: string }[]): Promise<Map<string, string>> {
  const results = new Map<string, string>();
  const pdfFiles = files.filter(f => path.extname(f.fileName).toLowerCase() === ".pdf");
  const nonPdfFiles = files.filter(f => path.extname(f.fileName).toLowerCase() !== ".pdf");

  for (const f of nonPdfFiles) {
    const ext = path.extname(f.fileName).toLowerCase();
    if (ext === ".csv" || ext === ".txt") {
      try {
        results.set(f.key, fs.readFileSync(f.filePath, "utf-8").slice(0, 50000));
      } catch (e: any) {
        results.set(f.key, `[File read error: ${e.message}]`);
      }
    } else {
      results.set(f.key, `[Image file: ${f.fileName}]`);
    }
  }

  if (pdfFiles.length === 0) return results;

  const batchInput: Record<string, string> = {};
  for (const f of pdfFiles) batchInput[f.key] = f.filePath;

  try {
    let dirnamePath: string | null = null;
    try { dirnamePath = path.resolve(__dirname, "../../../scripts/extract_pdf.py"); } catch {}
    const possiblePaths = [
      ...(dirnamePath ? [dirnamePath] : []),
      path.resolve(process.cwd(), "artifacts/api-server/scripts/extract_pdf.py"),
      path.resolve(process.cwd(), "scripts/extract_pdf.py"),
    ];
    const scriptPath = possiblePaths.find(p => fs.existsSync(p));
    if (!scriptPath) throw new Error(`extract_pdf.py not found, searched: ${possiblePaths.join(", ")}`);
    const { execSync } = await import("child_process");
    const result = execSync(`python3 "${scriptPath}" --batch`, {
      timeout: 180000,
      maxBuffer: 100 * 1024 * 1024,
      input: JSON.stringify(batchInput),
      env: { ...process.env, PYTHONIOENCODING: "utf-8" },
    });
    const parsed = JSON.parse(result.toString("utf-8"));
    for (const f of pdfFiles) {
      const r = parsed[f.key];
      const minViableChars = 200;
      if (r && r.success && r.text && r.charCount >= minViableChars && !isTextGarbled(r.text)) {
//         scrubLog("pdfplumber", `${f.fileName}: ${r.charCount} chars, ${r.numPages} pages (${r.method})`);
        const truncated = r.text.length > 60000 ? r.text.slice(0, 60000) + "\n[...truncated]" : r.text;
        results.set(f.key, `[PDF: ${r.numPages} pages via ${r.method}]\n${truncated}`);
      } else {
        const garbled = r && r.success && r.text && r.charCount >= minViableChars && isTextGarbled(r.text);
        const reason = garbled ? `garbled/encoded text (${r.charCount} chars but unreadable)` : (r ? `${r.charCount || 0} chars < ${minViableChars} min` : "empty/failed");
        const buf = fs.readFileSync(f.filePath);
//         scrubLog("pdfplumber", `${f.fileName}: ${reason}, trying pdf-parse first`);
        const pdfParseText = await extractTextWithPdfParseFallback(buf, f.fileName);
        if (pdfParseText && pdfParseText.length > minViableChars && !isTextGarbled(pdfParseText) && !pdfParseText.startsWith("[OCR")) {
//           scrubLog("pdfplumber", `${f.fileName}: pdf-parse succeeded (${pdfParseText.length} chars)`);
          results.set(f.key, pdfParseText);
        } else {
//           scrubLog("pdfplumber", `${f.fileName}: pdf-parse insufficient, falling back to Claude vision OCR`);
          const ocrResult = await ocrScannedPdf(buf, f.fileName);
          if (ocrResult && !ocrResult.startsWith("[OCR error") && !ocrResult.startsWith("[OCR: pdftoppm") && !ocrResult.startsWith("[OCR produced no text") && ocrResult.length > 100) {
            results.set(f.key, ocrResult);
          } else {
//             scrubLog("pdfplumber", `${f.fileName}: all extraction methods failed — document unreadable`);
            results.set(f.key, `[GARBLED: ${f.fileName} - encrypted text layer, OCR blocked by content filter]`);
          }
        }
      }
    }
  } catch (e: any) {
    // console.error(`[pdfplumber] Batch extraction failed (${e.message}), falling back to pdf-parse`);
    for (const f of pdfFiles) {
      try {
        results.set(f.key, await extractTextWithPdfParseFallback(fs.readFileSync(f.filePath), f.fileName));
      } catch (fe: any) {
        results.set(f.key, `[Extraction error: ${fe.message}]`);
      }
    }
  }
  return results;
}

async function extractTextWithPdfplumber(buffer: Buffer, fileName: string): Promise<string> {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "pdfplumber-"));
  const pdfPath = path.join(tmpDir, fileName.replace(/[^a-zA-Z0-9._-]/g, "_"));
  try {
    fs.writeFileSync(pdfPath, buffer);
    const files = [{ key: "0", filePath: pdfPath, fileName }];
    const results = await extractBatchWithPdfplumber(files);
    return results.get("0") || await extractTextWithPdfParseFallback(buffer, fileName);
  } finally {
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}
  }
}

async function extractTextWithPdfParseFallback(buffer: Buffer, fileName: string): Promise<string> {
  try {
    const pdfParse = (await import("pdf-parse") as any).default;
    const data = await pdfParse(buffer);
    const text = data.text || "";
    if (!text.trim()) {
//       scrubLog("PDF", `${fileName}: no extractable text, attempting OCR...`);
      return await ocrScannedPdf(buffer, fileName);
    }
    if (isTextGarbled(text)) {
//       scrubLog("PDF", `${fileName}: pdf-parse returned ${text.length} chars but text is GARBLED (font-substitution), attempting OCR...`);
      return await ocrScannedPdf(buffer, fileName);
    }
    const truncated = text.length > 40000 ? text.slice(0, 40000) + "\n[...truncated]" : text;
    return `[PDF: ${data.numpages || "?"} pages]\n${truncated}`;
  } catch (e: any) {
//     scrubLog("PDF", `${fileName}: pdf-parse failed (${e.message}), attempting OCR...`);
    return await ocrScannedPdf(buffer, fileName);
  }
}

async function extractTextFromBufferLegacy(buffer: Buffer, fileName: string): Promise<string> {
  const ext = path.extname(fileName).toLowerCase();
  if (ext === ".pdf") {
    return await extractTextWithPdfplumber(buffer, fileName);
  }
  if (ext === ".csv" || ext === ".txt") {
    try {
      return buffer.toString("utf-8").slice(0, 50000);
    } catch (e: any) {
      return `[File read error: ${e.message}]`;
    }
  }
  return `[Image file: ${fileName} - would need OCR for text extraction]`;
}

function getBufferFromDoc(docUrl: string, storageKey?: string | null): { buffer: Buffer; fileName: string } | null {
  const normalized = docUrl.startsWith("/") ? docUrl.slice(1) : docUrl;
  const filePath = path.resolve(process.cwd(), normalized);
  const uploadsRoot = path.resolve(process.cwd(), "uploads");

  if (filePath.startsWith(uploadsRoot) && fs.existsSync(filePath)) {
    return { buffer: fs.readFileSync(filePath), fileName: path.basename(filePath) };
  }

  if (filePath.startsWith(uploadsRoot)) {
    const dir = path.dirname(filePath);
    const base = path.basename(filePath);
    const withToplevel = path.join(dir, "__toplevel", base);
    if (fs.existsSync(withToplevel)) {
      return { buffer: fs.readFileSync(withToplevel), fileName: base };
    }
    const parent = path.dirname(dir);
    const grandparent = path.basename(dir);
    if (grandparent === "__toplevel") {
      const withoutToplevel = path.join(parent, base);
      if (fs.existsSync(withoutToplevel)) {
        return { buffer: fs.readFileSync(withoutToplevel), fileName: base };
      }
    }
  }

  return null;
}

async function getBufferFromDocAsync(docUrl: string, storageKey?: string | null): Promise<{ buffer: Buffer; fileName: string } | null> {
  const local = getBufferFromDoc(docUrl, storageKey);
  if (local) return local;

  if (storageKey) {
    try {
      const { getFileFromStorage } = await import("../../utils/fileStorage");
      const { buffer } = await getFileFromStorage(storageKey);
      return { buffer, fileName: path.basename(docUrl) };
    } catch (e: any) {
      // console.error(`[Storage] Failed to get ${storageKey}: ${e.message}`);
    }
  }

  return null;
}

export async function extractTextFromDocument(docUrl: string, storageKey?: string | null, options?: { forceLegacy?: boolean }): Promise<string> {
  const result = await getBufferFromDocAsync(docUrl, storageKey);
  if (!result) return `[Document at ${docUrl} - file not accessible locally or in cloud storage]`;

  const { buffer, fileName } = result;
  const rawText = await extractTextFromBufferLegacy(buffer, fileName);

  if (rawText.startsWith("[OCR error:") || rawText === "[OCR produced no text]" || rawText === "[OCR: pdftoppm produced no images]") {
    return rawText;
  }

  const ext = path.extname(fileName).toLowerCase();
  if (ext === ".pdf" && rawText.length > 100) {
    try {
      const parsed = parseStatementData(rawText);
      const formatted = formatParsedDataForPrompt(parsed);
      if (formatted) {
        // console.log(`[Parser→Enhanced] ${fileName}: Appending structured parsed data to raw text`);
        return rawText + "\n" + formatted;
      }
    } catch (e: any) {
      // console.warn(`[Parser] ${fileName}: Structured parsing failed (${e.message}), using raw text only`);
    }
  }

  return rawText;
}

export async function getLearningContext(): Promise<string> {
  const persistentRules = await db.select().from(lenderRulesTable);

  const allReviewed = await db.select().from(underwritingConfirmationsTable)
    .where(
      sql`${underwritingConfirmationsTable.status} IN ('confirmed', 'rejected', 'relabeled')`
    )
    .orderBy(sql`${underwritingConfirmationsTable.confirmedAt} DESC`);

  if (allReviewed.length === 0 && persistentRules.length === 0) return "";

  const lenderStats = new Map<string, { confirmed: number; rejected: number; relabeled: number; notes: string[] }>();

  for (const rule of persistentRules) {
    const lender = rule.lenderName.toLowerCase().trim();
    if (!lender) continue;
    if (!lenderStats.has(lender)) lenderStats.set(lender, { confirmed: 0, rejected: 0, relabeled: 0, notes: [] });
    const stats = lenderStats.get(lender)!;
    if (rule.verdict === "confirmed") stats.confirmed++;
    else if (rule.verdict === "rejected") { stats.rejected++; if (rule.adminNotes) stats.notes.push(rule.adminNotes); }
    else if (rule.verdict === "relabeled") { stats.relabeled++; if (rule.adminNotes) stats.notes.push(rule.adminNotes); }
  }

  for (const c of allReviewed) {
    const val = c.originalValue as any;
    const lender = (val?.lender || "").toLowerCase().trim();
    if (!lender || lender === "unknown") continue;

    if (!lenderStats.has(lender)) lenderStats.set(lender, { confirmed: 0, rejected: 0, relabeled: 0, notes: [] });
    const stats = lenderStats.get(lender)!;

    if (c.status === "confirmed") stats.confirmed++;
    else if (c.status === "rejected") { stats.rejected++; if (c.adminNotes) stats.notes.push(c.adminNotes); }
    else if (c.status === "relabeled") { stats.relabeled++; if (c.adminNotes) stats.notes.push(c.adminNotes); }
  }

  const alwaysLoan: string[] = [];
  const neverLoan: { name: string; reason: string }[] = [];
  const relabeledItems: { name: string; note: string }[] = [];

  for (const [lender, stats] of lenderStats) {
    if (stats.confirmed >= 1 && stats.rejected === 0) {
      alwaysLoan.push(lender);
    } else if (stats.rejected >= 1 && stats.confirmed === 0) {
      neverLoan.push({ name: lender, reason: stats.notes[0] || "Not a loan" });
    }
    if (stats.relabeled > 0) {
      relabeledItems.push({ name: lender, note: stats.notes[0] || "" });
    }
  }

  let context = `\n\n--- LEARNED RULES FROM ADMIN REVIEWS (${allReviewed.length + persistentRules.length} total rules) ---\n`;
  context += `These are ABSOLUTE rules — do NOT ask about these lenders again.\n\n`;

  if (alwaysLoan.length > 0) {
    context += `CONFIRMED LOANS — These are ALWAYS loans/MCAs/advances. Flag them with HIGH confidence:\n`;
    for (const l of alwaysLoan) context += `  - "${l}"\n`;
    context += `\n`;
  }

  context += `GOVERNMENT LOANS — These are ALWAYS loans (government-backed). Flag with HIGH confidence:\n`;
  context += `  - Any "SBA" loan (SBA EIDL, SBA Loan, SBA Payment, etc.) — government small business loan\n\n`;

  if (neverLoan.length > 0) {
    context += `NOT LOANS — NEVER flag these as loans:\n`;
    for (const { name, reason } of neverLoan) context += `  - "${name}" — ${reason}\n`;
    context += `\n`;
  }

  if (relabeledItems.length > 0) {
    context += `RELABELED — Admin corrected these:\n`;
    for (const { name, note } of relabeledItems) context += `  - "${name}" — ${note}\n`;
    context += `\n`;
  }

  return context;
}

export function normalizeLenderKey(name: string): string {
  let key = name.toLowerCase()
    .replace(/\(.*?\)/g, "")
    .replace(/\b(account|acct|acct\.?|a\/c)\s*#?\s*\d+/gi, "")
    .replace(/\s+\d+$/g, "")
    .replace(/[^a-z0-9]/g, "")
    .replace(/\d+$/, "")
    .trim();
  return key;
}

export const KNOWN_LENDER_SHORTNAMES = new Set([
  "ondeck", "forward", "fundbox", "bluevine", "kabbage", "kapitus", "gmfunding",
  "square", "wfunding", "cancapital", "rapid", "credibly", "libertas", "yellowstone",
  "pearl", "fora", "kalamata", "national", "fox", "mantis", "everest", "cfg",
  "mulligan", "clearview", "itria", "cloudfund", "navitas", "vox", "wynwood",
  "platinum", "qfs", "jmb", "unique", "samson", "kings", "stage", "7even",
  "cashable", "vitalcap", "vcg", "zen", "ace", "aspire", "breeze", "canfield",
  "clara", "compass", "daytona", "diamond", "elevate", "epic", "expansion",
  "family", "fenix", "figure", "fresh", "metrics", "giggle", "gotorro",
  "highland", "hightower", "honor", "idea", "ifund", "immediate", "iou", "lcf",
  "legend", "lendbuzz", "lendistry", "lg", "liberty", "litefund", "millstone",
  "mradvance", "newport", "nitro", "oak", "ocean", "olympus", "oneriver",
  "orange", "overton", "parkside", "path2", "power", "premium", "prosperum", "prosperity",
  "readycap", "reboost", "redwood", "reliance", "retro", "revenued", "rocket",
  "specialty", "stellar", "suncoast", "swift", "tbf", "fundworks", "triton",
  "trupath", "ufce", "ufs", "upfunding", "vader", "wave", "webfunder",
  "westwood", "wide", "pipe", "ssmb", "coast", "fintegra", "alt", "futures",
  "mako", "mainstreet", "integra", "reliant", "headway", "behalf", "breakout",
  "greenbox", "world", "tvt", "united", "bretton", "fleetcor",
  "fdm", "dlp", "gotfunded", "advsyn", "fratello", "ascentra", "luminar",
  "kif", "greenbridge", "arbitrage", "jrg", "aurum", "pdm", "pfg",
  "stashcap", "merchadv", "lily", "mckenzie", "purpletree", "lexio", "global",
  "monetaria", "trustify", "bluetie", "seamless", "liquidbee", "belltower",
  "palisade", "marlin", "xuper", "ghkapital", "fundfi", "newco", "slim", "steady",
  "secure", "dib", "dibcapital",
]);

export async function getLenderVerdicts(): Promise<Map<string, { verdict: "confirmed" | "rejected"; notes?: string }>> {
  const verdicts = new Map<string, { verdict: "confirmed" | "rejected"; notes?: string }>();

  const persistentRules = await db.select().from(lenderRulesTable);
  for (const rule of persistentRules) {
    const key = normalizeLenderKey(rule.lenderName);
    if (!key) continue;
    if (rule.verdict === "confirmed" || rule.verdict === "rejected") {
      verdicts.set(key, { verdict: rule.verdict as "confirmed" | "rejected", notes: rule.adminNotes || undefined });
    }
  }

  const allReviewed = await db.select().from(underwritingConfirmationsTable)
    .where(sql`${underwritingConfirmationsTable.status} IN ('confirmed', 'rejected')`);

  const lenderStats = new Map<string, { confirmed: number; rejected: number; lastNotes?: string }>();
  for (const c of allReviewed) {
    const val = c.originalValue as any;
    const key = normalizeLenderKey(val?.lender || "");
    if (!key || key === "unknown") continue;
    if (!lenderStats.has(key)) lenderStats.set(key, { confirmed: 0, rejected: 0 });
    const stats = lenderStats.get(key)!;
    if (c.status === "confirmed") stats.confirmed++;
    else if (c.status === "rejected") { stats.rejected++; stats.lastNotes = c.adminNotes || undefined; }
  }

  for (const [key, stats] of lenderStats) {
    if (verdicts.has(key)) continue;
    if (stats.confirmed >= 1 && stats.rejected === 0) verdicts.set(key, { verdict: "confirmed" });
    else if (stats.rejected >= 1 && stats.confirmed === 0) verdicts.set(key, { verdict: "rejected", notes: stats.lastNotes });
  }

  return verdicts;
}

let _cachedVerdicts: Map<string, { verdict: "confirmed" | "rejected"; notes?: string }> | null = null;
let _cachedVerdictsAt = 0;
async function getCachedVerdicts() {
  if (!_cachedVerdicts || Date.now() - _cachedVerdictsAt > 30000) {
    _cachedVerdicts = await getLenderVerdicts();
    _cachedVerdictsAt = Date.now();
  }
  return _cachedVerdicts;
}
export function invalidateVerdictCache() { _cachedVerdicts = null; }

const GENERIC_BANK_TERMS_SET = new Set([
  "withdrawal","withdrawals","deposit","deposits","transfer","transfers",
  "check","checks","debit","debits","credit","credits","payment","payments",
  "purchase","purchases","atm","pos","wire","ach","online","mobile",
  "counter","teller","overdraft","fee","fees","charge","charges",
  "misc","other","adjustment","correction","reversal","refund",
  "authorized","pending","posted"
]);
const GENERIC_BANK_TERMS = /^(withdrawal|withdrawals|deposit|deposits|transfer|transfers|check|checks|debit|debits|credit|credits|payment|payments|purchase|purchases|atm|pos|wire|ach|online|mobile|counter|teller|overdraft|fee|fees|charge|charges|misc|other|adjustment|correction|reversal|refund|authorized|pending|posted)$/i;

function isGenericBankTerm(name: string): boolean {
  const lower = name.toLowerCase().trim();
  if (GENERIC_BANK_TERMS.test(lower)) return true;
  const normalized = lower.replace(/[^a-z0-9]/g, "");
  if (GENERIC_BANK_TERMS_SET.has(normalized)) return true;
  const words = lower.split(/\s+/).filter(Boolean);
  if (words.length > 0 && words.every(w => GENERIC_BANK_TERMS_SET.has(w.replace(/[^a-z]/g, "")))) return true;
  return false;
}

export async function saveLenderRule(lenderName: string, verdict: string, adminNotes?: string, confirmedById?: number, options?: { skipIfRejected?: boolean }): Promise<void> {
  const normalized = lenderName.toLowerCase().trim();
  if (!normalized) return;
  if (isGenericBankTerm(normalized)) {
    // console.log(`[LenderRule] Blocked saving generic bank term "${normalized}" as lender rule`);
    return;
  }
  const normalizedKey = normalizeLenderKey(lenderName);

  const allRules = await db.select().from(lenderRulesTable);
  const existing = allRules.find(r => normalizeLenderKey(r.lenderName) === normalizedKey);

  if (existing) {
    if (options?.skipIfRejected && existing.verdict === "rejected" && verdict === "confirmed") {
      // console.log(`[LenderRule] Skipping "${normalized}": already rejected by admin, won't override with auto-confirm`);
      return;
    }
    await db.update(lenderRulesTable).set({
      verdict, adminNotes: adminNotes || existing.adminNotes, confirmedById, updatedAt: new Date(),
    }).where(eq(lenderRulesTable.id, existing.id));
  } else {
    await db.insert(lenderRulesTable).values({
      lenderName: normalized, verdict, adminNotes, confirmedById,
    });
  }
  invalidateVerdictCache();
}

export async function autoConfirmKnownLenders(): Promise<number> {
  const lenderVerdicts = await getLenderVerdicts();
  const sbaPattern = /\bsba\b/i;

  let autoCount = 0;
  const pending = await db.select().from(underwritingConfirmationsTable)
    .where(eq(underwritingConfirmationsTable.status, "pending"));

  for (const p of pending) {
    const val = p.originalValue as any;
    const lenderRaw = (val?.lender || "").trim();
    if (!lenderRaw) continue;

    const key = normalizeLenderKey(lenderRaw);
    if (!key) continue;
    if (isGenericBankTerm(lenderRaw)) continue;

    let result = lenderVerdicts.get(key);
    if (!result && sbaPattern.test(lenderRaw)) result = { verdict: "confirmed" };

    if (result) {
      const updated = await db.update(underwritingConfirmationsTable).set({
        status: result.verdict,
        adminNotes: result.verdict === "confirmed"
          ? "Auto-confirmed: lender previously verified as loan"
          : `Auto-rejected: ${result.notes || "previously rejected by admin"}`,
        confirmedAt: new Date(),
      }).where(and(
        eq(underwritingConfirmationsTable.id, p.id),
        eq(underwritingConfirmationsTable.status, "pending")
      )).returning();
      if (updated.length > 0) autoCount++;
    }
  }

  if (autoCount > 0) {
    // console.log(`[Auto-Confirm] Auto-resolved ${autoCount} pending findings based on learned lender rules`);
  }
  invalidateVerdictCache();
  return autoCount;
}

export const ANALYSIS_PROMPT = `You are an elite cash advance underwriting analyst. You analyze bank statements with extreme precision. Your findings directly impact lending decisions worth tens of thousands of dollars — errors are unacceptable.

## CRITICAL: ONLY ANALYZE THE LEAD'S BUSINESS
The business name is provided above. Some ZIP files contain statements from MULTIPLE different companies.
ONLY analyze bank statements belonging to the lead's business (or closely related entities with similar names).
IGNORE statements from completely different companies — their data is NOT relevant.
If a statement header shows a different company name than the lead, SKIP that statement entirely.

## YOUR ANALYSIS PROCESS (follow this exactly)

STEP 0 — IDENTIFY WHICH STATEMENTS BELONG TO THIS BUSINESS. Check each statement header for the account holder name. Only use statements where the name matches or is clearly related to the lead's business name. Skip all others. MANDATORY: You must return the EXACT name you find in the statement header in the "businessNameOnStatement" field. If it is different from the lead, still mention it.

STEP 0.1 — VERIFY DEPOSIT SUMMATION. Look for Account Summaries. If a summary has multiple lines for deposits (e.g., "Customer Deposits" AND "Other Deposits"), you MUST ADD THEM TOGETHER to get the monthly total.
Example: Customer Deposits $400 + Other Deposits $400 = $800 Monthly Revenue.
NEVER report only the first line if multiple categories exist. SEARCH the entire page for all summary lines.

STEP 1 — READ EVERY LINE. Go through each RELEVANT statement page by page. Note every single transaction. Do not skim or skip.

STEP 2 — BUILD A COMPLETE TRANSACTION LIST. For each transaction, note: date, description, amount (debit vs credit), running balance.

STEP 3 — IDENTIFY LOANS/MCA. Scan the complete transaction list for payments to lending companies. Flag loans with DAILY, WEEKLY, or MONTHLY frequency.
  CRITICAL: Transaction descriptions on bank statements are often cryptic. A payment to "VOX FUNDING" might appear as "ACH CORP DEBIT EXPC T VOX FUNDING EXPERIENCED CARE INC CUSTOMER ID 3789320". You MUST read the FULL description and look for lender names EMBEDDED within the text. Do NOT just look at the first few words — scan the ENTIRE description for ANY known lender name or funding keyword.
  For each candidate:
  a) Check: Is this company a KNOWN LENDER? (see list below). Scan the FULL transaction description — lender names are often buried in the middle of ACH descriptions (e.g. "ACH CORP DEBIT EXPC T **VOX FUNDING** BUSINESS NAME CUSTOMER ID 12345").
  b) Check: Does ANY part of the description contain "capital", "advance", "funding", "finance", "lending"? Search the ENTIRE description string, not just the payee field.
  c) Check: Is this a RECURRING fixed-amount debit where EVERY SINGLE payment is the EXACT same dollar amount? This is the most important test. Go through ALL payments to this lender and verify they are ALL identical. If even ONE payment is a different amount, it is NOT a loan — do NOT flag it. Example: if you see payments of $3,700, $3,700, $4,400, $3,700 — this FAILS because $4,400 differs. ALL must match. No exceptions.
  d) Check: Is there a corresponding large lump-sum deposit from this company earlier? (= the funding)
  e) VERIFY: Could this actually be a regular business expense instead? If yes → DO NOT flag as loan.
  NEVER flag these as loans — they are payment transfer services, NOT lenders: Zelle, Venmo, CashApp, Apple Pay, Google Pay, Samsung Pay, Wire Transfer, "Payment To" (generic bank transfers), "Payment Sent" (P2P transfers), "Money Transfer" (inter-account), "Online Payment" (generic). These are person-to-person or business payment methods.
  NEVER flag vendor/supplier payments as loans — fuel companies (Booster Fuels, Shell, ExxonMobil), freight/logistics, suppliers, landlords, and recurring business expenses are NOT loans even if they recur at fixed amounts.
  CRITICAL SQUARE DISTINCTION: "SQ *" transactions (e.g., "SQ *STORENAME", "SQUARE INC", "Square Processing", "Square Deposit", "Square Payroll") are MERCHANT PROCESSING payments from Square's payment terminal — they are deposits or processing fees, NOT loans. The ONLY Square loan product is "SQ Advance" or "Square Capital" — these appear as "SQ ADVANCE" with a number (e.g., "SQ ADVANCE 7678 WEEKLY"). Do NOT confuse general Square merchant activity with Square Capital loans. If you see "Square" or "SQ" without "Advance" or "Capital" in the description, it is NOT a loan.
  CRITICAL: The "lender" field MUST contain the actual company name. Never use generic fragments like "on", "to", "authorized on", "Payment Sent", or partial transaction descriptions. If you cannot identify a clear lender company name, do NOT include the entry in loanDetails.
  f) Assign confidence: "high" only if criteria a/b AND c/d are met. "medium" if strong indicators but not certain. "low" if possible but uncertain.
  g) COUNT EVERY SINGLE PAYMENT: For each loan, count the TOTAL number of individual payment transactions found across ALL pages of ALL statements provided. If Fox Funding pulls $3,300 weekly and you see statements from Oct-Jan, there should be ~16 payments. Report the ACTUAL count you find. Do NOT undercount — go page by page and tally every occurrence. The "occurrences" field must reflect the real count of individual debit transactions to this lender.
  h) FREQUENCY RESTRICTION: Valid loan frequencies are "daily", "weekly", and "monthly". Daily = 15-30 payments/month. Weekly = every 6-8 business days (~4/month). Monthly = every 28-31 days (~1/month). Biweekly payments should be classified as "weekly".

STEP 4 — DETECT RECURRING PULLS. Separately from Step 3, scan ALL debits for same-amount patterns:
  - Group all debits by exact dollar amount
  - For each amount appearing 3+ times, check if pulls happen on a daily or weekly pattern
  - Daily MCA pulls = same amount every business day (Mon-Fri)
  - Weekly pulls = same amount on the same day each week
  - This catches MCA debt even when the lender name is unclear
  - COUNT EVERY OCCURRENCE across all pages/months — do not just sample a few. The "occurrences" must be the total count found in these statements.

STEP 5 — CALCULATE MONTHLY DEPOSITS PER ACCOUNT. If the business has MULTIPLE bank accounts (different account numbers), track deposits SEPARATELY for each account.
  CRITICAL: Use the bank's OWN summary/total line for deposits — do NOT manually add up individual transactions. Look for lines like:
    - "61 Deposits and Other Credits $182,516.40" → revenue = 182516.40
    - "29 Credit(s) This Period $23,289.74" → revenue = 23289.74
    - "Total Deposits $X,XXX.XX" → use that exact number
    - "Total Credits $X,XXX.XX" → use that exact number
    - "Deposits/Credits: $X,XXX.XX" → use that exact number
    - "DEPOSIT AMOUNT = X,XXX.XX" or "DEPOSIT AMOUNT + X,XXX.XX" → revenue = that exact number (common in Monson Savings and similar small community banks)
    - "TOTAL DEPOSIT AMOUNT $X,XXX.XX" or "DEPOSITS $X,XXX.XX" → use that exact number
    - "Total Amount of Deposits $X,XXX.XX" → use that exact number
  Some small/community banks (e.g., Monson Savings) use an activity summary block with lines like:
    "SUMMARY OF YOUR ACTIVITY"
    "ACTIVITY THROUGH [DATE]"
    "BEGINNING BALANCE    [amount]"
    "DEPOSIT AMOUNT  +  [amount]" or "DEPOSIT AMOUNT  =  [amount]"
    "WITHDRAWAL AMOUNT  -  [amount]"
    "ENDING BALANCE  =  [amount]"
  In this format, the DEPOSIT AMOUNT line IS the total deposits for that month. Read the EXACT number from the actual statement — do NOT use any example numbers from these instructions. The "ACTIVITY THROUGH" line gives you the statement date — parse the month and year from it (e.g., "DEC 31 25" = December 2025 = "2025-12"). The account number may appear as "STATEMENT NUMBER" or elsewhere on the page.
  WARNING: The numbers in these instructions are EXAMPLES ONLY. You MUST read the actual numbers from the bank statement text provided. NEVER copy example numbers from the prompt into your output.
  The bank's summary total is ALWAYS more accurate than manually summing transactions. USE IT.
  If the summary breaks deposits into multiple categories, ADD ALL categories together. Common patterns:
    - "X Other Credits for: $[amount1]" + "X ATM/DEBIT Deposits: $[amount2]" → total deposits = amount1 + amount2 (credit unions often split these into separate sections)
    - "Customer Deposits $X" + "Other Deposits $X" + "Electronic Deposits $X" + "Other Credits $X" → SUM ALL (U.S. Bank uses this format — Account Summary shows "Customer Deposits [count] [amount]" and "Other Deposits [count] [amount]" as separate lines. You MUST add both together for total deposits.)
    - "Direct Deposits $X" + "Wire Credits $X" + "ACH Credits $X" + "Mobile Deposits $X" + "ATM Deposits $X" → SUM ALL
  CRITICAL: Many credit unions (FirstLight, PenFed, etc.) show "Other Credits" and "ATM/DEBIT Deposits" as SEPARATE summary sections on DIFFERENT parts of the page. You MUST find ALL deposit summary lines throughout the ENTIRE statement and add them together. Do NOT stop after finding the first deposit total — scroll through the entire document.
  If there is NO summary line, then and ONLY then sum individual deposit transactions.

  CRITICAL EXCEPTION — NAVY FEDERAL CREDIT UNION (and similar credit unions with multi-account summary tables):
  Navy Federal statements have a "Summary of your deposit accounts" table with columns: Previous Balance | Deposits/Credits | Withdrawals/Debits.
  When the summary table has MULTIPLE accounts (e.g., Business Checking + Membership Savings), there will be a "Totals" row at the bottom.
  USE the "Totals" row's Deposits/Credits value as the monthly deposit total — this is the CORRECT combined deposit amount across all accounts.
  Do NOT use just a single sub-account's Deposits/Credits value — you must include ALL accounts' deposits by using the Totals row.
  If there is NO Totals row (only one account), use that account's Deposits/Credits value.

  DO NOT subtract or exclude loan funding deposits, inter-account transfers, or any other credits.
  The only items to exclude: reversed/returned deposits where the exact same amount was deposited and then reversed (i.e., the deposit bounced back).

STEP 6 — FIND NEGATIVE DAYS AND NSF. Check EVERY day's ending balance. Flag every day below $0. Count every NSF/returned item fee. This is critical — missing negative days is a serious error.

STEP 7 — CLASSIFY LOAN FREQUENCY CAREFULLY:
  Frequency MUST be determined by how often payments ACTUALLY appear in the statements, NOT just by the lender name.
  ONLY TWO valid loan frequencies exist:
  - "daily" = payments appear nearly EVERY business day (15-22+ times per month). If a lender has fewer than 10 payments in a single month, it is NOT daily.
  - "weekly" = payments appear roughly 3-5 times per month (once per week). ALL weekly payments should be the same dollar amount. If amounts vary AND the lender is a KNOWN MCA company from the list below (e.g., Kapitus, OnDeck, etc.), still flag it — the amount changes likely indicate renewals or increases. Use the EXACT dollar amount from the SINGLE MOST RECENT payment — do NOT average multiple payments. If amounts vary AND the lender is NOT a known company, do NOT flag it.
  
  CRITICAL — DAILY LOAN AMOUNT CHANGES (increases/renewals):
  A daily loan can change its payment amount mid-month when the merchant gets an INCREASE or RENEWAL. For example: $175/day for 18 consecutive business days, then $225/day for the remaining business days. This is STILL ONE daily loan — NOT two separate loans, NOT a weekly loan. The lender increased the position.
  When this happens:
  - Report the EXACT dollar amount from the SINGLE MOST RECENT payment as the loan amount — do NOT average or round
  - Add the TOTAL number of payments at ALL amounts as occurrences
  - Frequency is "daily" — look at whether payments are on consecutive business days, NOT the count per month
  - In reasoning, note the amount change: "Increased from $175 to $225 on [date]"
  DO NOT split an increased daily loan into two loans. DO NOT label it "weekly" because the old-amount payments only appeared 18 times. Look at the DATES — if payments are on consecutive business days (01/02, 01/03, 01/05, 01/06...), it is DAILY regardless of amount changes.
  
  CRITICAL — MULTIPLE CONCURRENT POSITIONS FROM SAME LENDER:
  If the SAME lender has TWO different payment amounts on the SAME DAY (e.g., $250 AND $300 both debited on 01/02, both on 01/05, etc.), these are TWO SEPARATE loan positions — NOT one loan. Report each as its own entry with its own EXACT amount and occurrences. Do NOT add them together. Do NOT average them. Do NOT report a combined total. Each concurrent daily payment at a distinct amount = a separate position. The amount for each position MUST be the EXACT dollar amount as it appears in the transaction, down to the cent.
  How to tell the difference:
  - Amount CHANGE (one position): old amount STOPS, new amount STARTS on a specific date → ONE loan, use latest amount
  - TWO amounts on same day (two positions): BOTH amounts appear together on the same business day → TWO separate loans
  
  FREQUENCY GUIDELINES:
  - Daily loans: 15-30 payments per month (every business day). Must have 3+ payments in a single month.
  - Weekly loans: ~4 payments per month (every 6-8 business days). Must have 3+ payments in a single month.
  - Monthly loans: exactly 1 payment per month, same amount, same lender, appearing across 2+ months. Only flag monthly if the lender is a KNOWN MCA/lending company.
  - Biweekly payments should be classified as "weekly".
  MINIMUM PER-MONTH OCCURRENCE RULE for daily/weekly: A lender must have AT LEAST 3 payment occurrences IN A SINGLE MONTH to be flagged as a daily or weekly loan.
  For monthly loans: the lender must appear in at least 2 different months with the same fixed amount.
  Examples: OnDeck appears once in Oct, once in Nov, once in Dec with the SAME amount = monthly loan. OnDeck appears 4 times in Dec = weekly loan.
  Focus on the MOST RECENT month when determining if a position is active. A lender that had 4 payments/month in October but zero in November and December is likely PAID OFF — do NOT flag it as active.
  COUNT the actual payment occurrences per month. If you see 6 total payments across 3 months, that is approximately 2 per month = NOT a loan (too infrequent).
  IMPORTANT: Do NOT trust the transaction description (e.g. "Daily ACH") to determine frequency. Look at the ACTUAL DATES. If payments appear on 10/03, 10/10, 10/17, 10/24 — that is WEEKLY (7 days apart), NOT daily, regardless of what the description says. Count the actual number of payments per month from the dates.
  Getting frequency wrong drastically affects the monthly cost calculation. Be conservative — if unsure between daily and weekly, choose weekly.

STEP 8 — CROSS-CHECK EVERYTHING:
  - Re-read each loan finding. Could it be a vendor payment, credit card payment, or subscription? If yes, remove it.
  - Re-check monthly deposit totals against the bank's OWN summary line (e.g., "61 Deposits and Other Credits $182,516.40" or "29 Credit(s) This Period $23,289.74"). Your reported revenue number MUST match the bank's total. If it doesn't match, fix it now. If the summary has multiple categories, add them all. Do NOT exclude any credits.
  - Re-verify negative day dates against the statement. Are the dates correct?
  - Re-verify the risk score makes sense given all findings.
  - Re-verify each loan's frequency: count the actual occurrences per month and confirm the frequency label matches.
  - Re-verify each loan's payment amounts. For WEEKLY loans, ALL amounts must be identical — remove if any differ. For DAILY loans, amounts CAN change if the position was increased/renewed — use the CURRENT (latest) amount and note the change in reasoning. Look at the DATES to confirm daily consecutive pattern.
  - For concurrent positions (same lender, two amounts on same day): report the EXACT dollar amount for each position as it appears in the statement. Do NOT average amounts across different positions. Each position's amount must match a specific transaction amount on the statement.
  - CRITICAL AMOUNT CHECK: For EVERY loan you report, go back to the statement text and find the EXACT line containing that lender's name. The amount you report MUST appear on THAT SAME LINE or the immediately adjacent line — not on a different transaction line above or below. Bank statements list transactions vertically — each line has its OWN amount. Do NOT accidentally copy the amount from a DIFFERENT transaction on a nearby line. If a lender line says "The Fundworks Daily ACH 34227" and the amount on that line is "$1,877.14", then report $1,877.14 — NOT the amount from the previous line which might be "$63.96" for a Square payment. Read the EXACT amount aligned with the lender's transaction description.
  - REFERENCE NUMBERS vs AMOUNTS: Many bank statements embed numeric references, confirmation numbers, or trace IDs in the transaction description (e.g., "ACH OnDeck 794.25" where 794.25 is a trace/ref number, NOT the payment amount). The actual payment amount is usually the LAST column before the running balance. When you see multiple numbers on a transaction line, the DEBIT or CREDIT AMOUNT is the number in the Amount column — NOT embedded numbers within the description text. If a transaction line looks like "OnDeck 794.25 Debit 1,849.00 45,123.67", the payment is $1,849.00 (the amount column), NOT $794.25 (a reference number in the description).
  - RECURRING AMOUNT CONSISTENCY: When you find multiple payments to the same lender, they should almost always be the SAME amount (within a few cents). If you're seeing different amounts for the same lender across different dates, double-check that you're reading the right column. The most FREQUENTLY occurring amount is usually the correct payment amount.

## KNOWN MCA/LENDING COMPANIES (flag payments to these as loans):
OnDeck, On Deck Capital, Kabbage, PayPal Working Capital, Square Capital, BlueVine, Bluevine Capital, Fundbox, CAN Capital, Rapid Finance, Credibly, Libertas, Yellowstone Capital, Pearl Capital, Forward Financing, Fora Financial, Kalamata Capital, National Funding, Capytal, Capitalize, Fox Capital, Mantis Funding, Everest Business Funding, EBF, CFG Merchant Solutions, Byline, Mulligan Funding, Reliant Funding, Clearview, Itria Ventures, Cloudfund, Navitas, Ascentium, Ascentra, TVT Capital, United Capital Source, Greenbox Capital, World Business Lenders, Biz2Credit, Lendio, Fundation, Breakout Capital, RAM Payment, RAM Capital, Headway Capital, Behalf, Payability, Newtek, SmartBiz, 1st Bank Richmond, BFS Capital, Business Backer, Complete Business Solutions, Corporation Service Company, Direct Capital, Expansion Capital, First Data, Merchant Cash Group, National Business Capital, Reward Capital, Swift Capital, Thinking Capital, Uplyft Capital, Vox Funding, Wynwood Capital, Platinum Rapid Funding, Green Capital, QFS Capital, JMB Capital, Unique Funding, Samson MCA, Kings Capital, Fleetcor, Stage Advance, Stage Adv, Stage Funding, 7Even Capital, Cashable, VitalCap, VitalCap Fund, Vital Capital, VCG, VCG Capital, VCG Funding, FundFi, Fund Fi, ByzFlex, Byz Flex, ByzFund, Byz Fund, ByzWash, Byz Wash, Marlin Capital, Marlin MCA, Reliance Capital, Reliance Funding, Xuper, Xuper Funding, Idea Financial, Idea Fund, BizFund, Biz Fund, GH Capital, GHKAP, GHK Capital, Family Fund, Family Business Fund, Stellar Capital, Stellar Funding, FDM Capital, FDM Funding, LendingServ, Lending Serv, Lending Services, SBFS, SBF Solutions, Fintap, Fin Tap, Fenix Capital, Fenix Funding, Global Funding, Global Capital, LG Funding, LG Capital, FundWorks, Fund Works, Revenued, Revnued, WFunding, W Funding, Fratello, Fratello Funding, Link Capital, Link Funding, Kapitus, KAP, Zen Capital, Zen Funding, Epic Funding, Epic Capital, KIF Capital, KIF Funding, IOU Financial, IOU Central, Essential Capital, Essential Funding, Arbitrage Funding, Arbitrage Capital, LCF Capital, LCF Funding, Rocket Capital, Rocket Advance, UFS Capital, UFS Funding, Wave Capital, Wave Advance, Fresh Capital, Fresh Funding, Aurum Capital, Aurum Funding, Orange Funding, Orange Capital, Milstone Capital, Milestone Capital, Legend Capital, Legend Funding, Alt Capital, Alt Funding, Nreeze, N Reeze, DLP Capital, DLP Funding, Lily Capital, Lily Funding, Snap Advance, Snap Capital, TopChoice, Top Choice Capital, McKenzie Capital, McKenzie Funding, PurpleTree, Purple Tree Capital, American Capital, American Funding, Monetaria, Monetaria Capital, Specialty Capital, Specialty Funding, Sepcialty, Lexio Capital, Lexio Funding, Olympus Capital, Olympus Funding, Reliable Capital, Reliable Funding, Trustify, Trustify Capital, Blade Capital, Blade Funding, Overton Capital, Overton Funding, Prosperity Capital, Prosperity Funding, Fintegra, Fintegra Capital, Merit Capital, Merit Funding, Garden Capital, Garden State Capital, Micro Capital, Micro Funding, NewCo Capital, NewCo Funding, LiteFund, Lite Fund, Merk Capital, Merk Funding, River Capital, River Funding, DIB Funding, DIB Capital, Thoro Capital, Thoro Funding, BlueTie, Blue Tie Capital, Seamless Capital, Seamless Funding, Vader Capital, Vader Funding, LiquidBee, Liquid Bee, Slim Capital, Slim Funding, Steady Capital, Steady Funding, BellTower, Bell Tower Capital, eFinancial, E Financial, Palisad, Palisade Capital, Palisade Funding, GM Financial MCA, Lendr, Lendr Capital, Spartan Capital, Spartan Funding, Aspire Capital, Aspire Funding, Aspira, UFCE, UFCE Capital, Elevate Capital, Elevate Funding, Pinnacle Capital, Pinnacle Funding, Nexi Capital, Nexi Funding, Pinewood Capital, Pinewood Funding, Shor Capital, Shor Funding, Zlur, Zlur Capital, Wide Capital, Wide Merchant, Simply Funding, Simply Capital, FundKite, Fund Kite, Enod Capital, Enod Funding, Pathway Capital, Pathway Funding, Fundr, Fundr Capital, FBF Capital, FBF Funding, Coolidge Capital, Cooldige, SmallBiz, Small Business Funding, Logic Capital, Logic Funding, OneRiver, One River Capital, Mercury Capital, Merucry, Viking Capital, Viking Funding, BizPoint, Biz Point, Express Capital, Express Funding, Gotorro, Go Torro, Funding Futures, FundingFutures, Carlton Capital, Carlton Funding, Iron Capital, Iron Funding, BlackRock MCA, Blackroc, BizCap, Biz Cap, Power Capital, Power Funding, Mynt Capital, Mynt Funding, SunAdvance, Sun Advance, Likety, Likety Capital, ASAP Funding, ASAPFunding, Calabria Capital, Calabria Funding, Arena Capital, Arena Funding, TruPath, Tru Path, Smart Funding, SmartFunding, Freedom Capital, Freedom Funding, Apollo Capital, Apollo Funding, Triton Recovery, TritonRecovery, BHB Funding, BHBFudning, Balboa Capital, Balboa Funding, Star Capital, Star Funding, Coconut Capital, Coconut Funding, Money Capital, Money Advance, Forever Capital, Forever Funding, Emmy Capital, Emmy Funding, Ideal Funding, IdealFunding, Milvado, Milvado Capital, AmeriFi, Ameri Fi, AdvanceSyn, Advance Syn, Lendzi, Lendzi Capital, Retro Capital, Retro Funding, Rettro, Verve Capital, Verve Funding, Prime Funding, PrimeFundign, App Funding, AppFunding, Finova, Finova Capital, LendoCity, Lendo City, Ultra Capital, Ultra Funding, Alpha Capital, Alpha Funding, Beckham Capital, Beckham Funding, CapAssist, Cap Assist, YouLend, You Lend, Eminet, Eminet Capital, Speedy Capital, Speedy Funding, PIRS Capital, PIRS Funding, Westmount Capital, Westmount Funding, Formentra Capital, FormentraCap, Highland Capital, Highland Funding, ParkView Capital, Parkview Funding, Liquidity Capital, Liquidity Funding, CRC Capital, CRC Funding, Caybara, Caybara Capital, Cobalt Capital, Cobalt Funding, Oakmont Capital, Oakmont Funding, Cash Advance, Cash Capital, Fleet Capital, IdeaK, Widie, Wide Funding, eFinancialTree, eFinancialTTree, Capitawize, FundingFutres, Legned, SundAdvance, Nwetek, Kapiuts, LedningServices, LendingServices, Bzipoitn, Reveneued, Reveneud, True Capital, True Funding

## ORIGINATING BANKS — SPECIAL HANDLING:
Celtic Bank, WebBank, and Cross River Bank are ORIGINATING BANKS — they issue loans on behalf of many different MCA lenders (OnDeck, Kabbage, Forward Financing, etc.). 
- For DEBIT payments: If you see debits to "Celtic Bank", "WebBank", or "Cross River", these ARE loan payments — flag them. But you may not know WHICH lender they belong to, so use "Celtic Bank", "WebBank", or "Cross River" as the lender name.
- For CREDIT deposits: A deposit FROM "Celtic Bank", "WebBank", or "Cross River" does NOT tell you which lender funded it. Do NOT assume it's from any specific lender. Do NOT set fundedAmount on any loan based on an originating bank deposit unless the deposit description ALSO explicitly names the specific lender (e.g., "OnDeck via Celtic Bank").
- FUNDED AMOUNT RULE: Only set fundedAmount/fundedDate when you see a DEPOSIT that explicitly names the SAME lender as the loan (e.g., "Deposit ACH ONDECK" → matches OnDeck loan). If the deposit says "Celtic Bank", "WebBank", or "Cross River" without mentioning the specific lender, set fundedAmount to null. Never guess or assume which lender a funding deposit came from.

## NOT LOANS — DO NOT FLAG:
Credit cards (Amex, Visa, Mastercard, Discover, Chase, Capital One card, Citi, Barclays), insurance, rent/lease, utilities (electric, gas, water, phone, internet), payroll (ADP, Gusto, Paychex), taxes (IRS, state), merchant processing (Stripe, Square processing, Clover, PayPal merchant), subscriptions, vendors, suppliers, advertising (Google, Facebook, Meta), accounting, equipment purchases, refunds.
BANK INTERNAL PAYMENTS — do NOT flag: "CAPITAL PMT", "CAPITAL PAYMENT", "CAPITAL ONE PMT", "CHASE PMT", "WELLS FARGO PMT", "BANK PMT", or any payment description that matches the bank's own name. These are the bank's own fees, credit card payments, or internal transfers — NOT MCA loans.
EQUIPMENT/TRUCK FINANCING — these are NOT MCA loans, do NOT flag them: M&T Equipment Finance, Sumitomo Mitsui, VFS US, Paccar Financial, Priority First, Volvo Financial, Daimler Truck Financial, Peterbilt, Kenworth, Freightliner, Navistar, Caterpillar Financial, John Deere Financial, Kubota Credit, Komatsu, Case Credit, Bobcat, Toyota Motor Credit, De Lage Landen (DLL), PNC Equipment, Wells Fargo Equipment, any company with "Equipment Finance", "Equipment Leasing", "Truck Finance", "Truck Leasing", "Commercial Vehicle", or "Fleet Finance" in the name.

## RISK SCORING:
- A1 (Very Low Risk): Strong revenue, no/few loans, healthy balances, no negative days, no NSFs
- A2 (Medium Risk): Decent revenue, 1-2 loans manageable, occasional low balance, minimal issues
- B1 (Higher Risk): Revenue concerns OR 3+ loans OR some negative days OR declining trend
- B2 (High Risk): Multiple red flags — many loans + negative days + declining revenue + NSFs
- C (Decline): Severe issues — consistent negative balance, heavy stacking, revenue collapse, many NSFs

OnDeck presence is a POSITIVE signal — businesses with OnDeck loans passed their underwriting. If OnDeck is present, set hasOnDeck=true and lean toward A1/A2.

## OUTPUT FORMAT — Return ONLY valid JSON, no markdown, no code blocks:
{
  "hasLoans": true/false,
  "hasOnDeck": true/false,
  "loanDetails": [{"lender": "name", "amount": number, "frequency": "daily/weekly", "occurrences": number_of_individual_payments_found_in_these_statements, "fundedAmount": number_or_null, "fundedDate": "YYYY-MM-DD_or_null", "account": "last 4 digits of the bank account these loan payments are debited from", "confidence": "high/medium/low", "reasoning": "why this is a loan — cite specific transaction descriptions and amounts from the statement. You MUST confirm ALL payments are the EXACT same amount. List every payment amount found. If any differ, do NOT include this loan."}],
  "recurringPulls": [{"amount": number, "frequency": "daily/weekly", "occurrences": number, "dateRange": "YYYY-MM-DD to YYYY-MM-DD", "likelyLender": "name or Unknown", "monthlyTotal": number, "confidence": "high/medium/low"}],
  "businessNameOnStatement": "EXACT name from statement header",
  "nameMatchConfidence": "high/medium/low",
  "bankName": "name of the bank — read it ONLY from the statement header/logo/footer. If no bank name is printed on the statement, use 'Unknown'",
  "accountNumber": "last 4 digits of the PRIMARY account as shown on the statement (read from 'Account Number' field on the statement, NOT from the filename)",
  "monthlyRevenues": [{"month": "YYYY-MM", "revenue": number_total_deposits_for_this_month_SUMMING_all_categories, "account": "last 4 digits of this account", "bankName": "name of the bank"}],
  // CRITICAL — REVENUE SUMMATION: If the statement shows "Customer Deposits $X" and "Other Deposits $Y", revenue MUST be X+Y.
  "avgDailyBalance": number,
  "revenueTrend": "stable/growing/declining",
  "negativeDays": [{"date": "YYYY-MM-DD", "endingBalance": number, "cause": "what caused it"}],
  "nsfCount": number,
  "riskFactors": ["factor1", "factor2"],
  "riskScore": "A1/A2/B1/B2/C",
  "grossRevenue": number,
  "estimatedApprovalAmount": number,
  "hasNegativeBalance": true/false,
  "lowestBalance": number,
  "notableTransactions": [{"date": "YYYY-MM-DD", "description": "desc", "amount": number, "type": "deposit/withdrawal/loan_payment/nsf/fee", "isLoan": true/false}],
  "summary": "2-3 sentence summary",
  "verificationNotes": "What you double-checked"
}`;

function buildSourceTierContext(lead: any): string {
  if (!lead.sourceTier) return "";
  const descriptions: Record<string, string> = {
    A1: "Pre-classified as VERY LOW RISK by the submission source",
    A2: "Pre-classified as MEDIUM-HIGH RISK by the submission source",
  };
  return `Source Classification: ${lead.sourceTier} (${descriptions[lead.sourceTier] || "Unknown tier"})\n`;
}

export function buildCombinedTextHeader(lead: any): string {
  let text = `Business: ${lead.businessName}\nOwner: ${lead.ownerName}\n`;
  if (lead.monthlyRevenue) text += `Reported Monthly Revenue: $${lead.monthlyRevenue}\n`;
  if (lead.creditScore) text += `Credit Score: ${lead.creditScore}\n`;
  text += buildSourceTierContext(lead);
  text += `\n--- Bank Statement Data ---\n`;
  return text;
}

async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export function parseAIResponse(aiResponse: string): AnalysisResult {
  const fallback: AnalysisResult = {
    hasLoans: false,
    hasOnDeck: false,
    loanDetails: [],
    monthlyRevenues: [],
    avgDailyBalance: 0,
    revenueTrend: "stable",
    negativeDays: [],
    nsfCount: 0,
    riskFactors: ["Unable to parse bank statement"],
    riskScore: "B1",
    grossRevenue: 0,
    hasExistingLoans: false,
    bankName: null,
    accountNumber: null,
    businessNameOnStatement: null,
    nameMatchConfidence: "low",
    summary: aiResponse,
  };

  const strategies = [
    () => {
      const cleaned = aiResponse
        .replace(/```json\s*/g, "")
        .replace(/```\s*/g, "")
        .replace(/[\x00-\x1F\x7F]/g, (c) => c === '\n' || c === '\t' ? c : '')
        .trim();
      return JSON.parse(cleaned);
    },
    () => {
      const fenced = aiResponse.match(/```json\s*([\s\S]*?)```/);
      if (fenced) return JSON.parse(fenced[1].trim());
      throw new Error("no fenced json");
    },
    () => {
      const firstBrace = aiResponse.indexOf('{');
      const lastBrace = aiResponse.lastIndexOf('}');
      if (firstBrace === -1 || lastBrace === -1 || lastBrace <= firstBrace) throw new Error("no braces");
      let depth = 0;
      let jsonStart = -1;
      for (let i = firstBrace; i <= lastBrace; i++) {
        if (aiResponse[i] === '{') { if (depth === 0) jsonStart = i; depth++; }
        else if (aiResponse[i] === '}') {
          depth--;
          if (depth === 0) {
            const candidate = aiResponse.substring(jsonStart, i + 1);
            const parsed = JSON.parse(candidate);
            if (parsed.monthlyRevenues || parsed.hasLoans !== undefined || parsed.riskScore) return parsed;
          }
        }
      }
      throw new Error("no valid json object found");
    },
    () => {
      const firstBrace = aiResponse.indexOf('{');
      if (firstBrace === -1) throw new Error("no opening brace");
      let truncated = aiResponse.substring(firstBrace);
      truncated = truncated.replace(/,\s*$/, "");
      truncated = truncated.replace(/,\s*"[^"]*":\s*$/, "");
      truncated = truncated.replace(/,\s*"[^"]*":\s*"[^"]*$/, "");
      truncated = truncated.replace(/,\s*"[^"]*":\s*\[[^\]]*$/, (m) => "");
      let openBraces = 0, openBrackets = 0;
      let inString = false, escape = false;
      for (const ch of truncated) {
        if (escape) { escape = false; continue; }
        if (ch === '\\') { escape = true; continue; }
        if (ch === '"') { inString = !inString; continue; }
        if (inString) continue;
        if (ch === '{') openBraces++;
        else if (ch === '}') openBraces--;
        else if (ch === '[') openBrackets++;
        else if (ch === ']') openBrackets--;
      }
      for (let i = 0; i < openBrackets; i++) truncated += ']';
      for (let i = 0; i < openBraces; i++) truncated += '}';
      const parsed = JSON.parse(truncated);
      if (parsed.monthlyRevenues || parsed.hasLoans !== undefined || parsed.riskScore) {
        // console.log("[parseAIResponse] Recovered truncated JSON successfully");
        return parsed;
      }
      throw new Error("repaired json missing expected fields");
    },
  ];

  for (const strategy of strategies) {
    try {
      const analysis = strategy();
      if (typeof analysis === "object" && analysis !== null) {
        if (!Array.isArray(analysis.negativeDays)) analysis.negativeDays = [];
        if (typeof analysis.nsfCount !== "number" || isNaN(analysis.nsfCount)) analysis.nsfCount = 0;
        return analysis;
      }
    } catch {}
  }

  // console.error("[parseAIResponse] All parse strategies failed. Response starts with:", aiResponse.substring(0, 200));
  return fallback;
}

export async function callAIWithRetry(prompt: string, content: string, meta?: { leadId?: number; callType?: string }): Promise<string> {
//   scrubLog("SCRUB-AI-CALL", `callType="${meta?.callType || "analysis"}" leadId=${meta?.leadId || "?"} model=${AI_MODEL} contentLen=${content.length} promptLen=${prompt.length}`);
  await acquireAiSlot();
  try {
    let lastError: Error | null = null;
    const maxAttempts = 5;
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      try {
//         scrubLog("SCRUB-AI-CALL", `Attempt ${attempt + 1}/${maxAttempts}...`);
        const completion = await anthropic.messages.create({
          model: AI_MODEL,
          max_tokens: 8000,
          system: prompt,
          messages: [{ role: "user", content: content + "\n\nOutput ONLY the JSON object — no explanation, no markdown, no commentary." }],
        });
        const inputTokens = (completion.usage as any)?.input_tokens || Math.ceil(content.length / CHARS_PER_TOKEN_ESTIMATE);
        const outputTokens = (completion.usage as any)?.output_tokens || 0;
        costTracker.record(meta?.leadId ?? null, meta?.callType || "analysis", inputTokens, outputTokens);
//         scrubLog("SCRUB-AI-CALL", `Success: inputTokens=${inputTokens}, outputTokens=${outputTokens}, stopReason=${completion.stop_reason}`);
        for (const block of completion.content) {
          if (block.type === "text" && block.text.trim().length > 2) return block.text;
        }
        return "{}";
      } catch (e: any) {
        lastError = e;
        const isRateLimit = e.status === 429 || e.message?.includes("429") || e.message?.includes("RATELIMIT");
        // console.error(`AI call attempt ${attempt + 1}/${maxAttempts} failed:`, e.message);
        if (attempt < maxAttempts - 1) {
          let delay: number;
          if (isRateLimit) {
            const retryAfter = e.headers?.["retry-after"];
            if (retryAfter) {
              delay = (parseFloat(retryAfter) + 1) * 1000;
            } else {
              delay = 5000 * Math.pow(2, attempt) + Math.random() * 2000;
            }
            // console.log(`[AI] Rate limited — waiting ${Math.round(delay / 1000)}s before retry`);
          } else {
            delay = RETRY_DELAY_MS * Math.pow(2, attempt);
          }
          await sleep(delay);
        }
      }
    }
    throw lastError || new Error("AI call failed after retries");
  } finally {
    releaseAiSlot();
  }
}

const LOAN_ONLY_PROMPT = `You are analyzing bank statements for a cash advance underwriting company.
Deposits have already been extracted by our parser. Your ONLY job is to detect MCA/loan activity.

A LOAN is defined as: SAME amount + SAME vendor/payee + recurring pattern (daily, weekly, or monthly).
- Daily: payments every business day (15-30 per month). You'll see the exact same dollar amount debited almost every day.
- Weekly: payments every 6-8 days (3-5 per month). Same amount, same vendor, roughly once a week.
- Monthly: payments every 28-31 days (once per month). Same amount, same vendor, once per month across multiple months.

NOT a loan:
- Credit cards: same vendor but DIFFERENT amounts each time (Visa, Amex, Chase Card, etc.)
- Investment platforms: Robinhood, Coinbase, Webull, E*Trade, Schwab, Fidelity, Acorns
- Insurance: Allstate, Geico, State Farm, Progressive
- Equipment leases: ClickLease, truck financing, equipment financing
- Utilities, rent, payroll, Zelle/Venmo/CashApp transfers
- Any personal transfers or purchases

Look for:
1. Recurring debits to known MCA lenders (OnDeck, Kabbage, BlueVine, Credibly, Forward Financing, etc.)
2. Large lump-sum deposits followed by daily/weekly ACH debits (funding + repayment pattern)
3. Any other cash advance or loan repayment patterns with SAME exact amount recurring

For each loan found, count the actual number of payments you can see in the statement (occurrences).
Determine frequency from ACTUAL transaction dates, not guesses.

Return ONLY valid JSON (no markdown, no backticks):
{
  "hasExistingLoans": true/false,
  "recurringPulls": [
    {"likelyLender": "name", "amount": 123.45, "frequency": "daily|weekly|monthly", "occurrences": 0, "fundedAmount": null, "fundedDate": null}
  ],
  "riskFactors": ["factor1"],
  "nsfCount": 0,
  "negativeDays": [],
  "avgDailyBalance": 0,
  "hasNegativeBalance": false,
  "lowestBalance": 0
}

If no loans found, return: {"hasExistingLoans": false, "recurringPulls": [], "riskFactors": [], "nsfCount": 0, "negativeDays": [], "avgDailyBalance": 0, "hasNegativeBalance": false, "lowestBalance": 0}`;

function tryParserFirstExtraction(docs: any[], docTexts: Map<number, string>, traceCollector?: StructuredTraceCollector): {
  confident: boolean;
  monthlyRevenues: Array<{ month: string; revenue: number; account: string }>;
  reason: string;
} {
  const monthlyRevenues: Array<{ month: string; revenue: number; account: string }> = [];
  let allConfident = true;
//   scrubLog("PARSER-FIRST", `Attempting parser-first extraction on ${docs.length} doc(s)`);

  for (const doc of docs) {
    const text = docTexts.get(doc.id);
    if (!text || text.length < 200) {
//       scrubLog("PARSER-FIRST", `doc.id=${doc.id} "${doc.name}": text too short (${text?.length || 0} chars) — marking not confident`);
      allConfident = false;
      continue;
    }

    const isOcrText = /^\[(?:OCR|PDF OCR)/i.test(text.trim());
    const balThresholdLow = isOcrText ? 0.35 : 0.5;
    const balThresholdHigh = isOcrText ? 1.5 : 1.3;
    const creditBalTolerance = isOcrText ? 0.25 : 0.15;

    const monthKey = extractMonthFromSection(text);
    if (!monthKey) {
//       scrubLog("PARSER-FIRST", `doc.id=${doc.id} "${doc.name}": could NOT extract month — marking not confident`);
      allConfident = false;
      continue;
    }
//     scrubLog("PARSER-FIRST", `doc.id=${doc.id} "${doc.name}": monthKey="${monthKey}", isOCR=${isOcrText}`);

    const depositTotal = extractDepositSummaryFromSection(text);
    if (traceCollector) {
      traceCollector.addDepositCandidate({
        field: "total_deposits", value: depositTotal, regexRuleName: "extractDepositSummaryFromSection",
        nearbyText: "", confidence: depositTotal > 0 ? "high" : "low", selected: depositTotal > 0,
      });
    }
    if (depositTotal <= 0) {
      const creditSum = sumCreditTransactionsFromSection(text);
      const balCalc = balanceEquationFallback(text);
      if (traceCollector) {
        if (creditSum > 0) traceCollector.addDepositCandidate({ field: "credit_sum", value: creditSum, regexRuleName: "sumCreditTransactions", nearbyText: "", confidence: "medium", selected: false });
        if (balCalc > 0) traceCollector.addDepositCandidate({ field: "balance_equation", value: balCalc, regexRuleName: "balanceEquationFallback", nearbyText: "", confidence: "medium", selected: false });
      }
      if (creditSum > 0) {
        if (balCalc > 0 && Math.abs(creditSum - balCalc) / Math.max(creditSum, balCalc) < creditBalTolerance) {
          const acct = extractAccountNumberFromText(text) || "";
          const parts = monthKey.split("-");
          const mo = parseInt(parts[0]);
          const yr = parts[1]?.length === 2 ? `20${parts[1]}` : parts[1];
          const fullMonth = `${yr}-${String(mo).padStart(2, "0")}`;
          if (traceCollector) {
            traceCollector.stageC.finalDepositTotal = creditSum;
            traceCollector.stageC.finalDepositSource = "fallback";
            traceCollector.stageC.candidates[traceCollector.stageC.candidates.length - 2].selected = true;
          }
          monthlyRevenues.push({ month: fullMonth, revenue: creditSum, account: acct });
          continue;
        }
      }
      allConfident = false;
      continue;
    }

    const acct = extractAccountNumberFromText(text) || "";
    const parts = monthKey.split("-");
    const mo = parseInt(parts[0]);
    const yr = parts[1]?.length === 2 ? `20${parts[1]}` : parts[1];
    const fullMonth = `${yr}-${String(mo).padStart(2, "0")}`;

    const balCalc = balanceEquationFallback(text);
    if (balCalc > 0 && depositTotal > 0) {
      const ratio = depositTotal / balCalc;
//       scrubLog("PARSER-FIRST", `doc.id=${doc.id} "${doc.name}": deposit/balCalc ratio=${ratio.toFixed(2)} (thresholds: ${balThresholdLow}-${balThresholdHigh})`);
      if (ratio < balThresholdLow || ratio > balThresholdHigh) {
//         scrubLog("PARSER-FIRST", `doc.id=${doc.id} "${doc.name}": ratio OUT OF RANGE — marking not confident`);
        allConfident = false;
        continue;
      }
    }

    if (traceCollector) {
      traceCollector.stageC.finalDepositTotal = depositTotal;
      traceCollector.stageC.finalDepositSource = "single";
    }
    monthlyRevenues.push({ month: fullMonth, revenue: depositTotal, account: acct });
  }

  if (!allConfident || monthlyRevenues.length === 0) {
//     scrubLog("PARSER-FIRST", `Result: NOT CONFIDENT (allConfident=${allConfident}, months=${monthlyRevenues.length})`);
    return { confident: false, monthlyRevenues: [], reason: "parser_incomplete" };
  }

//   scrubLog("PARSER-FIRST", `Result: CONFIDENT — ${monthlyRevenues.length} month(s) extracted`);
  return { confident: true, monthlyRevenues, reason: "parser_confident" };
}

async function analyzeDocumentBatch(
  lead: any,
  docs: typeof documentsTable.$inferSelect[],
  docTexts: Map<number, string>,
  batchLabel: string,
  fullPrompt: string,
  structuredTrace?: StructuredTraceCollector,
): Promise<{ analysis: AnalysisResult; aiResponse: string; rawText: string }> {
  const header = buildCombinedTextHeader(lead);
  let combinedText = header + `\nStatements in this batch: ${docs.length}\n`;
  let rawStatementText = "";

  for (let i = 0; i < docs.length; i++) {
    const doc = docs[i];
    const text = docTexts.get(doc.id);
    if (!text) continue;
    const normalizedText = normalizePdfAmounts(text);
    const section = `\n========== STATEMENT ${i + 1}: "${doc.name}" ==========\n${normalizedText}\n========== END STATEMENT ${i + 1} ==========\n`;
    combinedText += section;
    rawStatementText += `\n========== STATEMENT ${i + 1}: "${doc.name}" ==========\n${normalizedText}\n========== END STATEMENT ${i + 1} ==========\n`;
  }

  // console.log(`${batchLabel} [SCRUB-ANALYZE] Combined text: ${combinedText.length} chars, rawStatementText: ${rawStatementText.length} chars`);

  const parserResult = tryParserFirstExtraction(docs, docTexts, structuredTrace);
  // console.log(`${batchLabel} [SCRUB-PARSER-FIRST] confident=${parserResult.confident}, reason="${parserResult.reason}", months=${parserResult.monthlyRevenues.length}`);
  if (parserResult.confident) {
    for (const mr of parserResult.monthlyRevenues) {
      // console.log(`${batchLabel} [SCRUB-PARSER-FIRST]   ${mr.month} acct=${mr.account}: $${mr.revenue.toLocaleString()}`);
    }
    // console.log(`${batchLabel} [ParserFirst] Parser confidently extracted ${parserResult.monthlyRevenues.length} month(s)`);

    let engineRecurring: ReturnType<typeof extractAllTransactions>["recurringPayments"] = [];
    let engineFunding: ReturnType<typeof extractAllTransactions>["fundingDeposits"] = [];
    let engineNsfCount = 0;
    let crossCheckFlags: Array<{ month: string; account: string; aiAmount: number; parserAmount: number; reason: string; usedValue: number }> = [];
    for (const doc of docs) {
      const text = docTexts.get(doc.id);
      if (!text) continue;
      const parsed = parseStatementData(text);
      let txnResult: TransactionEngineResult;
      if (parsed.transactions.length > 0) {
        txnResult = extractFromParsedTransactions(parsed.transactions, parsed.bankName || null, text);
        const creditSum = txnResult.totalCredits;
        if (parsed.totalDeposits && parsed.totalDeposits > 0) {
          const ratio = creditSum / parsed.totalDeposits;
          if (ratio < 0.8 || ratio > 1.2) {
            const monthKey = parsed.statementPeriod?.end || parsed.statementPeriod?.start || "";
            crossCheckFlags.push({
              month: monthKey,
              account: parsed.accountNumber || "",
              aiAmount: 0,
              parserAmount: creditSum,
              reason: `Cross-check: parsed credits $${creditSum.toFixed(2)} vs statement total $${parsed.totalDeposits.toFixed(2)} (ratio=${ratio.toFixed(2)})`,
              usedValue: parsed.totalDeposits,
            });
          }
        }
      } else {
        txnResult = extractAllTransactions(text);
      }
      const stmtPeriodEnd = parsed.statementPeriod?.end || "";
      for (const rp of txnResult.recurringPayments) {
        (rp as any).statementPeriodEnd = stmtPeriodEnd;
      }
      if (structuredTrace) {
        const totalTxnRows = txnResult.debits.length + txnResult.credits.length;
        const acceptedRows = txnResult.recurringPayments.length + txnResult.fundingDeposits.length;
        const rejectedRows = Math.max(0, totalTxnRows - acceptedRows);
        structuredTrace.stageD.totalRows += rejectedRows;
        structuredTrace.stageD.rejectedRows += rejectedRows;
        for (const rp of txnResult.recurringPayments) {
          structuredTrace.addTransactionRow({
            date: (rp as any).dates?.[0] || "", description: rp.lender, amount: rp.amount,
            category: "recurring_debit", rejected: false,
          });
        }
        for (const fd of txnResult.fundingDeposits) {
          structuredTrace.addTransactionRow({
            date: fd.date || "", description: fd.lender, amount: fd.amount,
            category: "funding_deposit", rejected: false,
          });
        }
      }
      engineRecurring = [...engineRecurring, ...txnResult.recurringPayments];
      engineFunding = [...engineFunding, ...txnResult.fundingDeposits];
      const nsfLines = (text.match(/\bnsf\b|\bnon.sufficient\b|\binsufficient\s+funds\b|\breturned\s+item\b/gi) || []).length;
      engineNsfCount += nsfLines;
    }

    const preDedup1Count = engineRecurring.length;
    const { recurring: dedupedRecurring, funding: dedupedFunding } = deduplicateLoansAcrossAccounts(engineRecurring, engineFunding);
    engineRecurring = consolidateSameLenderEntries(dedupedRecurring);
    engineFunding = dedupedFunding;
    if (structuredTrace) {
      const mergeCount = preDedup1Count - engineRecurring.length;
      if (mergeCount > 0) {
        structuredTrace.stageE.deduplicationApplied = true;
        structuredTrace.stageE.crossAccountMerges.push(`Parser-first path: merged ${mergeCount} duplicate(s) across accounts`);
      }
      for (const r of engineRecurring) {
        structuredTrace.addLoanCandidate({
          lender: r.lender, amount: r.amount, frequency: r.frequency,
          source: "parser", rejected: false,
          accountsFoundIn: (r as any).accountsFoundIn,
        });
      }
    }

    const engineFoundLoans = engineRecurring.length > 0 || engineFunding.length > 0;

    let loanData: any;
    let aiResponseStr: string;
    if (engineFoundLoans) {
      // console.log(`${batchLabel} [ParserFirst] Transaction engine found ${engineRecurring.length} recurring payments, ${engineFunding.length} funding deposits — confirming with loan-only AI`);
      const loanOnlyResponse = await callAIWithRetry(LOAN_ONLY_PROMPT, combinedText, { leadId: lead.id, callType: "loan-only" });
      aiResponseStr = `[ParserFirst:deposits+engineLoans] ${loanOnlyResponse}`;
      try {
        loanData = parseAIResponse(loanOnlyResponse);
      } catch {
        loanData = { hasExistingLoans: true, recurringPulls: [], riskFactors: [], nsfCount: 0, negativeDays: [], avgDailyBalance: 0 };
      }
      if (!loanData.recurringPulls || loanData.recurringPulls.length === 0) {
        const combinedPulls = [
          ...engineRecurring.map(r => ({
            likelyLender: r.lender,
            amount: r.amount,
            frequency: r.frequency,
            occurrences: r.occurrences,
          })),
          ...engineFunding.map(f => ({
            likelyLender: f.lender,
            amount: f.amount,
            frequency: "funded" as const,
            fundedDate: f.date,
          })),
        ];
        loanData.recurringPulls = combinedPulls;
        loanData.hasExistingLoans = true;
      } else {
        const aiLenderAmtKeys = new Set((loanData.recurringPulls || []).map((p: any) => {
          const lk = (p.likelyLender || "").toLowerCase().replace(/[^a-z0-9]/g, "");
          return `${lk}__${Math.round(p.amount || 0)}`;
        }));
        const aiLenderKeys = new Set((loanData.recurringPulls || []).map((p: any) =>
          (p.likelyLender || "").toLowerCase().replace(/[^a-z0-9]/g, "")
        ));
        for (const r of engineRecurring) {
          const engineKey = r.shortName.toLowerCase().replace(/[^a-z0-9]/g, "");
          const engineFullKey = r.lender.toLowerCase().replace(/[^a-z0-9]/g, "");
          const amtKey1 = `${engineKey}__${Math.round(r.amount)}`;
          const amtKey2 = `${engineFullKey}__${Math.round(r.amount)}`;
          const exactAmtMatch = ([...aiLenderAmtKeys] as string[]).some((ak: string) => ak === amtKey1 || ak === amtKey2);
          if (exactAmtMatch) continue;

          const lenderInAI = ([...aiLenderKeys] as any[]).some((ak: string) =>
            ak.includes(engineKey) || engineKey.includes(ak) ||
            ak.includes(engineFullKey) || engineFullKey.includes(ak)
          );
          if (lenderInAI) {
            const aiAmtForLender = (loanData.recurringPulls || []).find((p: any) => {
              const lk = (p.likelyLender || "").toLowerCase().replace(/[^a-z0-9]/g, "");
              return lk.includes(engineKey) || engineKey.includes(lk);
            });
            const aiAmt = aiAmtForLender?.amount || 0;
            const ratio = Math.max(aiAmt, r.amount) / Math.min(aiAmt || 1, r.amount || 1);
            if (ratio > 1.20) {
              loanData.recurringPulls.push({
                likelyLender: r.lender,
                amount: r.amount,
                frequency: r.frequency,
                occurrences: r.occurrences,
              });
              loanData.hasExistingLoans = true;
            }
            continue;
          }

          loanData.recurringPulls.push({
            likelyLender: r.lender,
            amount: r.amount,
            frequency: r.frequency,
            occurrences: r.occurrences,
          });
          loanData.hasExistingLoans = true;
        }
        for (const f of engineFunding) {
          const fKey = f.lender.toLowerCase().replace(/[^a-z0-9]/g, "");
          const alreadyInAI = ([...aiLenderKeys] as any[]).some((ak: string) =>
            ak.includes(fKey) || fKey.includes(ak)
          );
          if (!alreadyInAI) {
            loanData.recurringPulls.push({
              likelyLender: f.lender,
              amount: f.amount,
              frequency: "funded" as const,
              fundedDate: f.date,
            });
            loanData.hasExistingLoans = true;
          }
        }
      }

      const postMergeRawScan = /\b(ondeck|on\s*deck|odk\s*capital|kabbage|bluevine|fundbox|can\s*capital|rapid\s*financ|credibly|libertas|yellowstone|pearl\s*capital|forward\s*fin|forwardfinusa|fora\s*financial|kalamata|national\s*funding|fox\s*fund|mantis|everest\s*business|cfg\s*merchant|cfgms|mulligan|clearview|itria|cloudfund|navitas|vox\s*fund|wynwood|platinum\s*rapid|qfs|jmb\s*capital|unique\s*fund|samson|kings\s*capital|stage\s*adv|stage\s*fund|7even|cashable|vitalcap|vital\s*capital|vcg|zen\s*fund|ace\s*fund|aspire\s*fund|breeze\s*advance|canfield|clara\s*capital|compass\s*fund|daytona|diamond\s*advance|elevate\s*fund|epic\s*advance|expansion\s*capital|family\s*fund|fenix\s*capital|figure\s*lending|fresh\s*fund|funding\s*metrics|giggle\s*financ|gotorro|highland|hightower|honor\s*capital|idea\s*247|idea\s*financial|ifund|immediate\s*advance|immediate\s*capital|iou\s*central|lcf|legend\s*advance|lendbuzz|lendistry|lg\s*funding|liberty\s*fund|litefund|millstone|mr\s*advance|newport\s*business|nitro\s*advance|oak\s*capital|ocean\s*advance|olympus|one\s*river|orange\s*advance|overton|parkside|path\s*2\s*capital|power\s*fund|premium\s*merchant|prosperum|prosperity\s*fund|readycap|reboost|redwood\s*business|reliance|retro\s*advance|revenued|rocket\s*capital|specialty|stellar|suncoast|swift\s*fund|tbf\s*group|thefundworks|the\s*fundworks|triton|trupath|ufce|ufs|upfunding|vader|wave\s*advance|webfunder|westwood|wide\s*merchant|pipe\s*capital|ssmb|coast\s*fund|fintegra|altfunding|alt\s*funding|funding\s*futures|mako\s*fund|main\s*street\s*group|integra\s*fund|reliant|headway|behalf|breakout|greenbox|world\s*business|tvt\s*capital|united\s*capital|bretton|fleetcor|kapitus|kap\s*servic|servicing\s*by\s*kap|gmfunding|sq\s*advance|square\s*capital|sq\s*capital|sq\s*loan|w\s+funding|elixir|sbfs|byzfund|capybara|fratello|ascentra|luminar|kif\s*fund|greenbridge|arbitrage|jrg\s*capital|aurum|pdm|pfg|stashcap|stash\s*cap|merchadv|merchant\s*adv|lily\s*fund|mckenzie|purpletree|purple\s*tree|lexio|global\s*fund|monetaria|trustify|bluetie|seamless\s*fund|liquidbee|belltower|palisade|marlin|xuper|fundfi|slim\s*fund|steady\s*fund|newco\s*capital)/ig;
      const allDocsText = docs.map(d => docTexts.get(d.id) || "").join("\n");
      const rawMatches = [...allDocsText.matchAll(postMergeRawScan)];
      if (rawMatches.length > 0) {
        const mergedKeys = new Set((loanData.recurringPulls || []).map((p: any) =>
          normalizeLenderKey(p.likelyLender || "")
        ));
        const rawLenderNames = new Set(rawMatches.map(m => m[0].trim()));
        for (const rawName of rawLenderNames) {
          const norm = normalizeLenderKey(rawName);
          const alreadyMerged = [...mergedKeys].some((mk: any) => {
            const mks = String(mk);
            return mks.includes(norm) || norm.includes(mks);
          });
          if (!alreadyMerged) {
            const displayName = rawName.charAt(0).toUpperCase() + rawName.slice(1);
            // console.log(`${batchLabel} [ParserFirst] Raw text scan found lender "${displayName}" missed by engine+AI — adding as needs-confirmation`);
            loanData.recurringPulls.push({
              likelyLender: displayName,
              amount: 0,
              frequency: "unknown" as const,
              needsConfirmation: true,
            });
            loanData.hasExistingLoans = true;
          }
        }
      }
    } else {
      const rawTextLenderScan = /\b(ondeck|on\s*deck|odk\s*capital|kabbage|bluevine|fundbox|can\s*capital|rapid\s*financ|credibly|libertas|yellowstone|pearl\s*capital|forward\s*fin|forwardfinusa|fora\s*financial|kalamata|national\s*funding|fox\s*fund|mantis|everest\s*business|cfg\s*merchant|cfgms|mulligan|clearview|itria|cloudfund|navitas|vox\s*fund|wynwood|platinum\s*rapid|qfs|jmb\s*capital|unique\s*fund|samson|kings\s*capital|stage\s*adv|stage\s*fund|7even|cashable|vitalcap|vital\s*capital|vcg|zen\s*fund|ace\s*fund|aspire\s*fund|breeze\s*advance|canfield|clara\s*capital|compass\s*fund|daytona|diamond\s*advance|elevate\s*fund|epic\s*advance|expansion\s*capital|family\s*fund|fenix\s*capital|figure\s*lending|fresh\s*fund|funding\s*metrics|giggle\s*financ|gotorro|highland|hightower|honor\s*capital|idea\s*247|idea\s*financial|ifund|immediate\s*advance|immediate\s*capital|iou\s*central|lcf|legend\s*advance|lendbuzz|lendistry|lg\s*funding|liberty\s*fund|litefund|millstone|mr\s*advance|newport\s*business|nitro\s*advance|oak\s*capital|ocean\s*advance|olympus|one\s*river|orange\s*advance|overton|parkside|path\s*2\s*capital|power\s*fund|premium\s*merchant|prosperum|prosperity\s*fund|readycap|reboost|redwood\s*business|reliance|retro\s*advance|revenued|rocket\s*capital|specialty|stellar|suncoast|swift\s*fund|tbf\s*group|thefundworks|the\s*fundworks|triton|trupath|ufce|ufs|upfunding|vader|wave\s*advance|webfunder|westwood|wide\s*merchant|pipe\s*capital|ssmb|coast\s*fund|fintegra|altfunding|alt\s*funding|funding\s*futures|mako\s*fund|main\s*street\s*group|integra\s*fund|reliant|headway|behalf|breakout|greenbox|world\s*business|tvt\s*capital|united\s*capital|bretton|fleetcor|kapitus|kap\s*servic|servicing\s*by\s*kap|gmfunding|sq\s*advance|square\s*capital|sq\s*capital|sq\s*loan|w\s+funding|elixir|sbfs|byzfund|capybara|fratello|ascentra|luminar|kif\s*fund|greenbridge|arbitrage|jrg\s*capital|aurum|pdm|pfg|stashcap|stash\s*cap|merchadv|merchant\s*adv|lily\s*fund|mckenzie|purpletree|purple\s*tree|lexio|global\s*fund|monetaria|trustify|bluetie|seamless\s*fund|liquidbee|belltower|palisade|marlin|xuper|fundfi|slim\s*fund|steady\s*fund|dib\s*capital)/i;
      const allDocsText = docs.map(d => docTexts.get(d.id) || "").join("\n");
      const rawScanMatch = allDocsText.match(rawTextLenderScan);
      if (rawScanMatch) {
        // console.log(`${batchLabel} [ParserFirst] Transaction engine missed lenders but raw text scan found "${rawScanMatch[0]}" — calling loan-only AI`);
        const loanOnlyResponse = await callAIWithRetry(LOAN_ONLY_PROMPT, combinedText, { leadId: lead.id, callType: "loan-only-rawscan" });
        aiResponseStr = `[ParserFirst:deposits+rawScanLoans] ${loanOnlyResponse}`;
        try {
          loanData = parseAIResponse(loanOnlyResponse);
        } catch {
          loanData = { hasExistingLoans: true, recurringPulls: [], riskFactors: [], nsfCount: 0, negativeDays: [], avgDailyBalance: 0 };
        }
      } else {
        // console.log(`${batchLabel} [ParserFirst] Transaction engine found NO lenders — skipping AI loan call entirely`);
        aiResponseStr = "[ParserFirst:deposits+noLoans] AI call skipped — transaction engine found no lenders";
      }

      let computedAvgDailyBal = 0;
      let computedLowestBal = Infinity;
      let computedNegDays: string[] = [];
      let hasNegBal = false;
      const riskFactors: string[] = [];
      for (const doc of docs) {
        const text = docTexts.get(doc.id);
        if (!text) continue;
        const balMatches = text.matchAll(/(?:balance|bal)\s*[:\s]\s*\$?([\d,]+\.\d{2})/gi);
        const balances: number[] = [];
        for (const m of balMatches) {
          const b = parseFloat(m[1].replace(/,/g, ""));
          if (!isNaN(b)) balances.push(b);
        }
        const negBalMatch = text.match(/(?:ending|closing)\s+balance\s*[:\s]*-\s*\$?([\d,]+\.\d{2})/i);
        if (negBalMatch) {
          hasNegBal = true;
          const negAmt = -parseFloat(negBalMatch[1].replace(/,/g, ""));
          if (negAmt < computedLowestBal) computedLowestBal = negAmt;
        }
        if (balances.length > 0) {
          const avg = balances.reduce((a, b) => a + b, 0) / balances.length;
          if (computedAvgDailyBal === 0) computedAvgDailyBal = avg;
          else computedAvgDailyBal = (computedAvgDailyBal + avg) / 2;
          const minBal = Math.min(...balances);
          if (minBal < computedLowestBal) computedLowestBal = minBal;
        }
      }
      if (computedLowestBal === Infinity) computedLowestBal = 0;
      if (engineNsfCount > 0) riskFactors.push(`${engineNsfCount} NSF/returned items detected`);
      if (hasNegBal) riskFactors.push("Negative balance detected");

      if (!loanData) {
        loanData = {
          hasExistingLoans: false, recurringPulls: [], riskFactors,
          nsfCount: engineNsfCount, negativeDays: computedNegDays,
          avgDailyBalance: Math.round(computedAvgDailyBal * 100) / 100,
          hasNegativeBalance: hasNegBal, lowestBalance: Math.round(computedLowestBal * 100) / 100,
        };
      } else {
        loanData.riskFactors = [...(loanData.riskFactors || []), ...riskFactors];
        loanData.nsfCount = loanData.nsfCount || engineNsfCount;
        loanData.avgDailyBalance = loanData.avgDailyBalance || Math.round(computedAvgDailyBal * 100) / 100;
        if (computedNegDays.length > 0) loanData.negativeDays = [...(loanData.negativeDays || []), ...computedNegDays];
      }
    }

    const revenues = parserResult.monthlyRevenues;
    const sorted = [...revenues].sort((a, b) => a.month.localeCompare(b.month));
    const grossRevenue = sorted.length >= 2
      ? (sorted[sorted.length - 1].revenue + sorted[sorted.length - 2].revenue) / 2
      : sorted.length === 1 ? sorted[0].revenue : 0;

    const analysis: AnalysisResult = {
      monthlyRevenues: revenues.map(r => ({ month: r.month, revenue: r.revenue, account: r.account })),
      grossRevenue,
      hasLoans: loanData.hasExistingLoans || loanData.hasLoans || false,
      hasOnDeck: (loanData.recurringPulls || []).some((p: any) => /ondeck/i.test(p.likelyLender || "")),
      hasExistingLoans: loanData.hasExistingLoans || false,
      bankName: null,
      loanDetails: (loanData.recurringPulls || []).filter((p: any) => {
        if ((p.occurrences || 0) < 2) return false;
        if (!p.amount || p.amount <= 0) return false;
        const name = (p.likelyLender || p.likely_lender || "").toLowerCase();
        if (/\b(online\s*(?:banking|transfer)|payment\s+to\b|transfer\s+to\s+(?:sav|chk|checking|savings)|apple\s+cash|self\s+financial|intuit\s+financ|isuzu\s+financ|styku|next\s+insur|pmnt\s+sent|bkofamerica|ascentium|leasechg|lease\s+pymt|loan\s+pymt|td\s+auto|orig\s+co\s+name.*(?:visa|bk\s+of\s+amer|ins\b))\b/i.test(name)) return false;
        return true;
      }).map((p: any) => ({
        lender: (p.likelyLender || p.likely_lender || "Unknown").replace(/^business\s+to\s+business\s*/i, "").replace(/^(?:ach|corp)\s+(?:debit|credit|payment|pmt)\s*[-–—]?\s*/i, "").trim() || "Unknown",
        amount: p.amount || 0,
        frequency: p.frequency || "daily",
        fundedAmount: p.fundedAmount || null,
        fundedDate: p.fundedDate || null,
        occurrences: p.occurrences || 0,
        account: revenues[0]?.account || "",
      })),
      revenueTrend: sorted.length >= 2
        ? sorted[sorted.length - 1].revenue > sorted[0].revenue * 1.05 ? "increasing"
          : sorted[sorted.length - 1].revenue < sorted[0].revenue * 0.95 ? "decreasing" : "stable"
        : "stable",
      avgDailyBalance: loanData.avgDailyBalance || 0,
      riskScore: "B1",
      riskFactors: loanData.riskFactors || [],
      nsfCount: loanData.nsfCount || engineNsfCount,
      negativeDays: loanData.negativeDays || [],
      hasNegativeBalance: loanData.hasNegativeBalance || false,
      lowestBalance: loanData.lowestBalance || 0,
      depositReviewFlags: crossCheckFlags,
    };

    if (crossCheckFlags.length > 0) {
      console.log(`[CrossCheck] ${crossCheckFlags.length} flag(s): ${crossCheckFlags.map(f => f.reason).join("; ")}`);
    }

    const validated = await postAIValidation(analysis, rawStatementText, parserResult.monthlyRevenues);
    applyParserDrivenPaidOff(validated, engineRecurring);
    return { analysis: validated, aiResponse: aiResponseStr, rawText: rawStatementText };
  }

  let parserLoanRecurring: ReturnType<typeof extractAllTransactions>["recurringPayments"] = [];
  let parserLoanFunding: ReturnType<typeof extractAllTransactions>["fundingDeposits"] = [];
  for (const doc of docs) {
    const text = docTexts.get(doc.id);
    if (!text) continue;
    const parsed = parseStatementData(text);
    let txnResult: TransactionEngineResult;
    if (parsed.transactions.length > 0) {
      txnResult = extractFromParsedTransactions(parsed.transactions, parsed.bankName || null, text);
    } else {
      txnResult = extractAllTransactions(text);
    }
    const stmtPeriodEnd2 = parsed.statementPeriod?.end || "";
    for (const rp of txnResult.recurringPayments) {
      (rp as any).statementPeriodEnd = stmtPeriodEnd2;
    }
    parserLoanRecurring = [...parserLoanRecurring, ...txnResult.recurringPayments];
    parserLoanFunding = [...parserLoanFunding, ...txnResult.fundingDeposits];
  }

  const preDedup2Count = parserLoanRecurring.length;
  const { recurring: dedupedParserRecurring, funding: dedupedParserFunding } = deduplicateLoansAcrossAccounts(parserLoanRecurring, parserLoanFunding);
  parserLoanRecurring = consolidateSameLenderEntries(dedupedParserRecurring);
  parserLoanFunding = dedupedParserFunding;
  if (structuredTrace) {
    const mergeCount2 = preDedup2Count - parserLoanRecurring.length;
    if (mergeCount2 > 0) {
      structuredTrace.stageE.deduplicationApplied = true;
      structuredTrace.stageE.crossAccountMerges.push(`Full-AI path: merged ${mergeCount2} duplicate(s) across accounts`);
    }
  }

  console.log(`${batchLabel} [SCRUB-AI] Parser NOT confident — calling full AI (parser found ${parserLoanRecurring.length} recurring, ${parserLoanFunding.length} funding from parsed txns)`);
  const aiResponse = await callAIWithRetry(fullPrompt, combinedText, { leadId: lead.id, callType: "full-analysis" });
  const analysis = parseAIResponse(aiResponse);

  if (parserLoanRecurring.length > 0 || parserLoanFunding.length > 0) {
    const aiLenderEntries = (analysis.loanDetails || []) as any[];
    for (const r of parserLoanRecurring) {
      let matchedAIEntry: any = null;
      const alreadyInAI = aiLenderEntries.some(aiEntry => {
        if (!areSameLender(aiEntry.lender || "", r.lender) && !areSameLender(aiEntry.lender || "", r.shortName)) return false;
        const ratio = Math.max(aiEntry.amount || 0, r.amount) / Math.min(aiEntry.amount || 1, r.amount || 1);
        if (ratio <= 1.20) {
          matchedAIEntry = aiEntry;
          return true;
        }
        return false;
      });
      if (alreadyInAI && matchedAIEntry && matchedAIEntry.amount !== r.amount) {
        console.log(`${batchLabel} [Parser→AI-Merge] Correcting AI amount for "${r.lender}": AI $${matchedAIEntry.amount} → parser $${r.amount} (parser is ground truth, AI override blocked)`);
        if (structuredTrace) {
          structuredTrace.addAIChange({
            field: `loan_amount:${r.lender}`,
            originalValue: r.amount,
            aiOutput: matchedAIEntry.amount,
            finalValue: r.amount,
            aiChanged: false,
            reason: "Parser amount is ground truth — AI numeric override blocked",
          });
          structuredTrace.stageF.aiOverrideBlocked = true;
        }
        matchedAIEntry.amount = r.amount;
        if (r.occurrences) matchedAIEntry.occurrences = r.occurrences;
        if (r.frequency) matchedAIEntry.frequency = r.frequency;
      }
      if (!alreadyInAI) {
        const verifiedAmount = crossCheckLenderAmountInText(r.lender, r.shortName, r.amount, rawStatementText);
        if (verifiedAmount !== null) {
          const useAmount = verifiedAmount || r.amount;
          if (verifiedAmount && verifiedAmount !== r.amount) {
            console.log(`${batchLabel} [Parser→AI-Merge] Cross-check corrected "${r.lender}": parser $${r.amount} → text $${useAmount}`);
          }
          console.log(`${batchLabel} [Parser→AI-Merge] Adding parser-detected lender "${r.lender}" ($${useAmount} x${r.occurrences}) not found in AI`);
          if (!analysis.loanDetails) analysis.loanDetails = [];
          analysis.loanDetails.push({
            lender: r.lender, amount: useAmount, frequency: r.frequency, occurrences: r.occurrences,
          });
          analysis.hasLoans = true;
        } else {
          console.log(`${batchLabel} [Parser→AI-Merge] SKIPPING "${r.lender}" ($${r.amount}) — cross-check found no matching amount near lender name in text`);
        }
      }
    }
    for (const f of parserLoanFunding) {
      const alreadyInAI = (analysis.loanDetails || []).some((l: any) =>
        areSameLender(l.lender || "", f.lender)
      );
      if (!alreadyInAI) {
        console.log(`${batchLabel} [Parser→AI-Merge] Adding parser-detected funding "${f.lender}" ($${f.amount}) not found in AI`);
        if (!analysis.loanDetails) analysis.loanDetails = [];
        analysis.loanDetails.push({
          lender: f.lender, amount: f.amount, frequency: "funded" as any, fundedDate: f.date,
        });
        analysis.hasLoans = true;
      }
    }
  }

  if (analysis.loanDetails && analysis.loanDetails.length > 1) {
    const stripB2B = (s: string) => s.replace(/^business\s+to\s+business\s*/i, "").replace(/^(?:ach|corp)\s+(?:debit|credit|payment|pmt)\s*[-–—]?\s*/i, "").trim();
    for (const loan of analysis.loanDetails) {
      const stripped = stripB2B(loan.lender || "");
      if (stripped !== loan.lender && stripped.length >= 3) {
        loan.lender = stripped;
      }
    }
    const amountMap = new Map<number, any[]>();
    for (const loan of analysis.loanDetails) {
      const rounded = Math.round(loan.amount || 0);
      if (!amountMap.has(rounded)) amountMap.set(rounded, []);
      amountMap.get(rounded)!.push(loan);
    }
    for (const [, loans] of amountMap) {
      if (loans.length < 2) continue;
      const parserBacked = loans.filter((l: any) => parserLoanRecurring.some(r => areSameLender(r.lender, l.lender)));
      const aiOnly = loans.filter((l: any) => !parserLoanRecurring.some(r => areSameLender(r.lender, l.lender)));
      if (parserBacked.length >= 1 && aiOnly.length >= 1) {
        for (const remove of aiOnly) {
          if (!areSameLender(remove.lender, parserBacked[0].lender)) {
            console.log(`${batchLabel} [CrossLenderDedup] Removing AI-only "${remove.lender}" $${remove.amount} — parser found "${parserBacked[0].lender}" at same amount`);
            remove._remove = true;
          }
        }
      }
    }
    analysis.loanDetails = analysis.loanDetails.filter((l: any) => !l._remove);
  }

  if (structuredTrace) {
    for (const loan of (analysis.loanDetails || [])) {
      const fromParser = parserLoanRecurring.some(r => areSameLender(r.lender, loan.lender));
      if (!fromParser) {
        structuredTrace.addLoanCandidate({
          lender: loan.lender, amount: loan.amount || 0, frequency: loan.frequency || "unknown",
          source: "ai", rejected: false,
        });
        structuredTrace.addAIChange({
          field: `loan_lender:${loan.lender}`,
          originalValue: null,
          aiOutput: loan.amount || 0,
          finalValue: loan.amount || 0,
          aiChanged: true,
          reason: "AI-only lender detection (no parser match)",
        });
      }
    }
  }

  const validated = await postAIValidation(analysis, rawStatementText);
  applyParserDrivenPaidOff(validated, parserLoanRecurring);
  return { analysis: validated, aiResponse, rawText: rawStatementText };
}

function crossCheckLenderAmountInText(
  lenderName: string,
  shortName: string,
  parserAmount: number,
  rawText: string
): number | null {
  const lines = rawText.split("\n");
  const lenderPattern = new RegExp(shortName.replace(/[^a-zA-Z0-9]/g, "\\s*"), "i");

  const sameLineAmounts: number[] = [];

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    if (!lenderPattern.test(line)) continue;

    const amtsOnLine = [...line.matchAll(/(?<![0-9])(\d[\d,]*\.\d{2})(?!\d)/g)].map(m =>
      parseFloat(m[1].replace(/,/g, ""))
    ).filter(a => a >= 50 && a <= 100000);

    if (amtsOnLine.length > 0) {
      sameLineAmounts.push(...amtsOnLine);
    }
  }

  if (sameLineAmounts.length === 0) {
    console.log(`[CrossCheck] "${shortName}": no amounts found on same line as lender name — unreliable detection`);
    return null;
  }

  const parserAmtInText = sameLineAmounts.some(a => Math.abs(a - parserAmount) < 0.05);
  if (parserAmtInText) return 0;

  const amountCounts = new Map<number, number>();
  for (const a of sameLineAmounts) {
    const rounded = Math.round(a * 100) / 100;
    amountCounts.set(rounded, (amountCounts.get(rounded) || 0) + 1);
  }
  const sorted = [...amountCounts.entries()].sort((a, b) => b[1] - a[1]);
  console.log(`[CrossCheck] "${shortName}": parser=$${parserAmount}, text amounts=${sorted.map(([a,c]) => "$"+a+"x"+c).join(", ")}`);
  return sorted[0][0];
}

function parseDollar(s: string): number {
  let cleaned = s.replace(/[$,\s]/g, "");
  const dots = (cleaned.match(/\./g) || []).length;
  if (dots > 1) {
    const lastDot = cleaned.lastIndexOf('.');
    cleaned = cleaned.slice(0, lastDot).replace(/\./g, '') + cleaned.slice(lastDot);
  }
  return parseFloat(cleaned);
}

interface BankSummaryDeposit {
  total: number;
  monthKey: string;
  label: string;
}

const DEPOSIT_SUMMARY_PATTERNS = [
  /(\d+)\s+deposits?\s*\/\s*credits?\s+\$?([\d,.]+)/gi,
  /(\d+)\s+deposits?\s+and\s+other\s+credits?\s+\$?([\d,.]+)/gi,
  /(\d+)\s+credit\(?s?\)?\s+this\s+period\s+\$?([\d,.]+)/gi,
  /deposits?\s+and\s+(?:other\s+)?(?:additions?|credits?)\s+(\d+)\s+\$?([\d,.]+)/gi,
  /deposits?\s+and\s+(?:other\s+)?(?:additions?|credits?)\s+\$?([\d,.]+)/gi,
  /total\s+deposits?\s+(?:and\s+(?:other\s+)?(?:credits?|additions?)?\s*)?\$?([\d,.]+)/gi,
  /total\s+credits?\s+\$?([\d,.]+)/gi,
  /deposits?\s*\/\s*credits?\s*[:=]?\s*\$?([\d,.]+)/gi,
  /total\s+deposits?\s+and\s+additions\s+\$?([\d,.]+)/gi,
  /total\s+additions?\s+\$?([\d,.]+)/gi,
  /deposits,?\s+credits?\s+and\s+interest\s+\$?([\d,.]+)/gi,
  /total\s+(?:other\s+)?deposits?\s+(?:&|and)\s+(?:other\s+)?credits?\s+\$?([\d,.]+)/gi,
];

const MONTH_ABBRS_MAP: Record<string, number> = {
  jan:1,feb:2,mar:3,apr:4,may:5,jun:6,jul:7,aug:8,sep:9,oct:10,nov:11,dec:12,
  january:1,february:2,march:3,april:4,june:6,july:7,august:8,september:9,october:10,november:11,december:12,
};

export function extractMonthFromSection(text: string): string {
  const header = text.slice(0, 3000);

  const stmtPeriodMatch = header.match(/Statement\s+Period:\s*(.+?)\s+to\s+(.+?)(?:\n|$)/i);
  if (stmtPeriodMatch) {
    const startDate = stmtPeriodMatch[1].trim();
    const endDate = stmtPeriodMatch[2].trim();
    const numericEnd = endDate.match(/(\d{1,2})\/(\d{1,2})\/(\d{2,4})/);
    const numericStart = startDate.match(/(\d{1,2})\/(\d{1,2})\/(\d{2,4})/);
    if (numericEnd) {
      const endMo = parseInt(numericEnd[1]);
      const endDay = parseInt(numericEnd[2]);
      const endYr = numericEnd[3].length === 4 ? numericEnd[3].slice(2) : numericEnd[3];
      if (endDay <= 5 && numericStart) {
        const startMo = parseInt(numericStart[1]);
        const startYr = numericStart[3].length === 4 ? numericStart[3].slice(2) : numericStart[3];
        if (startMo !== endMo && startMo >= 1 && startMo <= 12) return `${startMo}-${startYr}`;
      }
      if (endMo >= 1 && endMo <= 12) return `${endMo}-${endYr}`;
    }
    const namedDate = endDate.match(/(\w{3,9})\s+(\d{1,2})\s*,?\s*(\d{4})/);
    if (namedDate) {
      const endDay = parseInt(namedDate[2]);
      const endMo = MONTH_ABBRS_MAP[namedDate[1].toLowerCase().slice(0, 3)];
      const yr = namedDate[3].slice(2);
      if (endDay <= 5) {
        const namedStart = startDate.match(/(\w{3,9})\s+(\d{1,2})\s*,?\s*(\d{4})/);
        if (namedStart) {
          const startMo = MONTH_ABBRS_MAP[namedStart[1].toLowerCase().slice(0, 3)];
          const startYr = namedStart[3].slice(2);
          if (startMo && startMo !== endMo) return `${startMo}-${startYr}`;
        }
      }
      if (endMo) return `${endMo}-${yr}`;
    }
  }

  const rangePats = [
    /(\d{1,2})\/(\d{1,2})\/(\d{2,4})\s*(?:[-–—]|thru|through|to)\s*(\d{1,2})\/(\d{1,2})\/(\d{2,4})/i,
    /(\w{3,9})\s+(\d{1,2})\s*,?\s*(\d{4})\s*(?:[-–—]|thru|through|to)\s*(\w{3,9})\s+(\d{1,2})\s*,?\s*(\d{4})/i,
  ];
  for (const pat of rangePats) {
    const m = header.match(pat);
    if (!m) continue;
    if (/^\d+$/.test(m[4])) {
      const startMo = parseInt(m[1]);
      const startDay = parseInt(m[2]);
      const startYr = m[3].length === 4 ? m[3].slice(2) : m[3];
      const endMo = parseInt(m[4]);
      const endDay = parseInt(m[5]);
      const yr = m[6].length === 4 ? m[6].slice(2) : m[6];
      if (endDay <= 5 && endMo !== startMo) {
        if (startMo >= 1 && startMo <= 12) return `${startMo}-${startYr}`;
      }
      if (endMo >= 1 && endMo <= 12) return `${endMo}-${yr}`;
    } else {
      const endMo = MONTH_ABBRS_MAP[m[4].toLowerCase().slice(0, 3)];
      const endDay = parseInt(m[5]);
      const startMo = MONTH_ABBRS_MAP[m[1].toLowerCase().slice(0, 3)];
      const startYr = m[3].length === 4 ? m[3].slice(2) : m[3];
      const yr = m[6].length === 4 ? m[6].slice(2) : m[6];
      if (endDay <= 5 && endMo !== startMo && startMo) {
        return `${startMo}-${startYr}`;
      }
      if (endMo) return `${endMo}-${yr}`;
    }
  }

  const activityThroughMatch = header.match(/activity\s+through\s+(\w{3,9})\s+\d{1,2}\s*,?\s*(\d{2,4})/i);
  if (activityThroughMatch) {
    const mo = MONTH_ABBRS_MAP[activityThroughMatch[1].toLowerCase().slice(0, 3)];
    const yr = activityThroughMatch[2].length === 4 ? activityThroughMatch[2].slice(2) : activityThroughMatch[2];
    if (mo) return `${mo}-${yr}`;
  }

  const endingPats = [
    /(?:statement\s+ending|period\s*ending|closing\s*date|ending\s+balance\s+on|statement\s*date)[:\s]*(\d{1,2})\/(\d{1,2})\/(\d{2,4})/i,
    /(?:statement\s+ending|period\s*ending|closing\s*date|ending\s+balance\s+on|statement\s*date)[:\s]*(\w{3,9})\s+\d{1,2}\s*,?\s*(\d{4})/i,
  ];
  for (const pat of endingPats) {
    const m = header.match(pat);
    if (!m) continue;
    if (m.length >= 4 && /^\d+$/.test(m[1])) {
      const mo = parseInt(m[1]);
      const yr = m[3].length === 4 ? m[3].slice(2) : m[3];
      if (mo >= 1 && mo <= 12) return `${mo}-${yr}`;
    } else if (m.length >= 3) {
      const mo = MONTH_ABBRS_MAP[m[1].toLowerCase().slice(0, 3)];
      const yr = m[2].length === 4 ? m[2].slice(2) : m[2];
      if (mo) return `${mo}-${yr}`;
    }
  }

  return "";
}

function parseMoneyAmount(raw: string): number {
  const cleaned = raw.replace(/\s/g, "");
  const isNeg = /^\(.*\)$/.test(cleaned) || /^-/.test(cleaned) || /-$/.test(cleaned) || /\bDR\b/i.test(cleaned);
  const digits = cleaned.replace(/[$(),\-]|CR|DR/gi, "");
  const val = parseFloat(digits);
  if (isNaN(val)) return NaN;
  return isNeg ? -val : val;
}

export function parseBalanceFromSection(sectionText: string, which: "begin" | "end"): number {
  let patterns: RegExp[];
  if (which === "begin") {
    patterns = [
      /Starting\/Beginning\s+Balance:\s*(-?\$?[\d,]+\.\d{2}|\(\$?[\d,]+\.\d{2}\))/i,
      /Beginning\s+Balance[:\s]*(-?\$?[\d,]+\.\d{2}|\(\$?[\d,]+\.\d{2}\))/i,
      /Starting\s+Balance[:\s]*(-?\$?[\d,]+\.\d{2}|\(\$?[\d,]+\.\d{2}\))/i,
      /Opening\s+Balance[:\s]*(-?\$?[\d,]+\.\d{2}|\(\$?[\d,]+\.\d{2}\))/i,
      /Previous\s+Balance[:\s]*(-?\$?[\d,]+\.\d{2}|\(\$?[\d,]+\.\d{2}\))/i,
      /Balance\s+(?:Forward|Brought\s+Forward)[:\s]*(-?\$?[\d,]+\.\d{2}|\(\$?[\d,]+\.\d{2}\))/i,
      /Balance\s+Last\s+Statement[:\s]*\$?\s*(-?[\d,]+\.\d{2}|\(\$?\s*[\d,]+\.\d{2}\))/i,
      /Balance\s+Previous\s+Statement[:\s]*\$?\s*(-?[\d,]+\.\d{2}|\(\$?\s*[\d,]+\.\d{2}\))/i,
      /Balance\s+Prior\s+Statement[:\s]*\$?\s*(-?[\d,]+\.\d{2}|\(\$?\s*[\d,]+\.\d{2}\))/i,
    ];
  } else {
    patterns = [
      /Ending\s+Balance[:\s]*(-?\$?[\d,]+\.\d{2}|\(\$?[\d,]+\.\d{2}\))/i,
      /Closing\s+Balance[:\s]*(-?\$?[\d,]+\.\d{2}|\(\$?[\d,]+\.\d{2}\))/i,
      /New\s+Balance[:\s]*(-?\$?[\d,]+\.\d{2}|\(\$?[\d,]+\.\d{2}\))/i,
      /Balance\s+This\s+Statement[:\s]*\$?\s*(-?[\d,]+\.\d{2}|\(\$?\s*[\d,]+\.\d{2}\))/i,
      /Balance\s+Current\s+Statement[:\s]*\$?\s*(-?[\d,]+\.\d{2}|\(\$?\s*[\d,]+\.\d{2}\))/i,
    ];
  }
  for (const pat of patterns) {
    const m = sectionText.match(pat);
    if (m) return parseMoneyAmount(m[1]);
  }
  return NaN;
}

function extractDebitTotalFromSection(sectionText: string): number {
  const patterns = [
    /(\d+)\s+debit\(?s?\)?\s+this\s+period\s+\$?([\d,]+\.?\d*)/gi,
    /total\s+(?:withdrawals?|debits?)\s+(?:and\s+(?:other\s+)?(?:fees?|charges?)?\s*)?\$?([\d,]+\.?\d*)/gi,
    /(?:withdrawals?|debits?)\s+(?:and\s+other\s+)?(?:subtractions?|charges?)?\s+\$?([\d,]+\.?\d*)/gi,
    /(?:checks?\s+paid\s+and\s+other\s+)?(?:withdrawals?\s+and\s+(?:other\s+)?(?:debits?|subtractions?))\s+\$?([\d,]+\.?\d*)/gi,
    /withdrawals?\s*\/\s*debits?\s+\$?([\d,]+\.\d{2})/gi,
    /(?:^|\n)\s*(?:total\s+)?(?:withdrawals?|debits?)\s+\$?([\d,]+\.\d{2})/gim,
    /(?:checks?\s+and\s+other\s+)?debits?\s*\(-\)\s*\$?\s*([\d,]+\.\d{2})/gi,
  ];
  for (const pat of patterns) {
    const regex = new RegExp(pat.source, pat.flags);
    let m;
    while ((m = regex.exec(sectionText)) !== null) {
      const amt = parseDollar(m[m.length - 1]);
      if (amt > 100 && amt < 100_000_000) return amt;
    }
  }
  const debitIdx = sectionText.search(/debit\(?s?\)?\s+this\s+period/i);
  if (debitIdx >= 0) {
    const nearby = sectionText.slice(debitIdx, debitIdx + 500);
    const amtMatch = nearby.match(/\$?([\d,]+\.\d{2})/);
    if (amtMatch) {
      const amt = parseFloat(amtMatch[1].replace(/,/g, ""));
      if (amt > 100 && amt < 100_000_000) return amt;
    }
    const behind = sectionText.slice(Math.max(0, debitIdx - 300), debitIdx);
    const behindAmounts: number[] = [];
    const behindRegex = /\$?([\d,]+\.\d{2})/g;
    let bm;
    while ((bm = behindRegex.exec(behind)) !== null) {
      const amt = parseFloat(bm[1].replace(/,/g, ""));
      if (amt > 100 && amt < 100_000_000) behindAmounts.push(amt);
    }
    if (behindAmounts.length > 0) return behindAmounts[behindAmounts.length - 1];
  }
  return 0;
}

export function balanceEquationFallback(sectionText: string): number {
  const beginBal = parseBalanceFromSection(sectionText, "begin");
  const endBal = parseBalanceFromSection(sectionText, "end");
  if (isNaN(beginBal) || isNaN(endBal)) return 0;

  const debitTotal = extractDebitTotalFromSection(sectionText);
  if (debitTotal > 0) {
    const deposits = endBal - beginBal + debitTotal;
    if (deposits > 0 && deposits < 100_000_000) {
      // console.log(`[BalanceEq] end($${endBal.toFixed(2)}) - begin($${beginBal.toFixed(2)}) + debits($${debitTotal.toFixed(2)}) = deposits($${deposits.toFixed(2)})`);
      return deposits;
    }
  }
  if (endBal > beginBal) {
    const minDeposits = endBal - beginBal;
    // console.log(`[BalanceEq] floor: end > begin, min deposits = $${minDeposits.toFixed(2)}`);
    return minDeposits;
  }
  return 0;
}

const UNIVERSAL_SUMMARY_LABELS_CORE = [
  { key: "beginningBalance", pattern: /(?:(?:beginning|opening|starting|previous)\s+balance|balance\s+(?:last|previous|prior|beginning)\s+(?:statement|period))/i },
  { key: "deposits",         pattern: /(?:deposits?\s+and\s+(?:other\s+)?(?:additions?|credits?)|total\s+deposits?\s+and\s+(?:other\s+)?(?:credits?|additions?)|total\s+additions?|deposits,?\s+credits?\s+and\s+interest|credits?\s+this\s+(?:statement\s+)?period|total\s+credits?(?:\s+(?:this|for)\s+(?:statement|period))?|\bDeposits?\b)/i },
  { key: "checksPaid",       pattern: /checks?\s+paid/i },
  { key: "electronicWith",   pattern: /electronic\s+(?:withdrawals?|payments?)/i },
  { key: "fees",             pattern: /(?:(?:service|monthly|maintenance|account)\s+)?(?:fees?|charges?)(?:\s+(?:and\s+)?(?:charges?|assessments?))?/i },
  { key: "endingBalance",    pattern: /(?:(?:ending|closing|new)\s+balance|balance\s+(?:this|current|ending|closing)\s+(?:statement|period))/i },
  { key: "otherWith",        pattern: /(?:other|atm|misc(?:ellaneous)?|wire)\s+(?:withdrawals?|charges?|debits?)/i },
  { key: "debits",           pattern: /(?:debits?\s+this\s+(?:statement\s+)?period|(?:withdrawals?|debits?)\s+and\s+(?:other\s+)?(?:subtractions?|charges?|debits?)|total\s+(?:withdrawals?|debits?)|total\s+subtractions?|total\s+debits?(?:\s+(?:this|for)\s+(?:statement|period))?|\bWithdrawals?\b)/i },
];

const CHASE_SUMMARY_LABELS_CORE = UNIVERSAL_SUMMARY_LABELS_CORE;

const SUMMARY_SECTION_HEADERS_CORE = [
  /CHECKING\s+SUMMARY/i,
  /SAVINGS\s+SUMMARY/i,
  /ACCOUNT\s+SUMMARY/i,
  /STATEMENT\s+SUMMARY/i,
  /ACCOUNT\s+ACTIVITY\s+SUMMARY/i,
  /ACCOUNT\s+OVERVIEW/i,
  /SUMMARY\s+OF\s+ACCOUNT\s+ACTIVITY/i,
  /ACCOUNT\s+BALANCE\s+SUMMARY/i,
];

interface CoreSummaryLabelPosition {
  key: string;
  lineIdx: number;
  charIdx: number;
  inlineAmount?: number;
}

function parseCheckingSummaryTableCore(blockText: string): { deposits: number; beginningBalance: number } | null {
  const lines = blockText.split("\n");

  const seenKeys = new Set<string>();
  const labelPositions: CoreSummaryLabelPosition[] = [];
  for (let i = 0; i < lines.length; i++) {
    for (const lbl of CHASE_SUMMARY_LABELS_CORE) {
      if (seenKeys.has(lbl.key)) continue;
      const m = lines[i].match(lbl.pattern);
      if (m) {
        seenKeys.add(lbl.key);
        labelPositions.push({ key: lbl.key, lineIdx: i, charIdx: lines.slice(0, i).join("\n").length + (m.index || 0) });
      }
    }
  }

  if (!labelPositions.find(l => l.key === "deposits")) return null;
  if (labelPositions.length < 2) return null;

  const labelOrder = [...labelPositions].sort((a, b) => a.charIdx - b.charIdx);

  for (const lp of labelOrder) {
    const lineText = lines[lp.lineIdx];
    const amts = lineText.match(/\$?\-?\(?\d[\d,]*\.\d{2}\)?/g);
    if (amts && amts.length > 0) {
      const lastAmt = amts[amts.length - 1];
      lp.inlineAmount = Math.abs(parseFloat(lastAmt.replace(/[$,()]/g, "")));
    }
  }

  let extraDepositTotal = 0;
  const EXTRA_DEPOSIT_PATTERNS = [
    /(?:^|\n)\s*Automatic\s+Deposits?\s+(?:\d{1,3}\s+)?([\d,]+\.\d{2})\s*\+?/im,
    /(?:^|\n)\s*Electronic\s+Deposits?\s+(?:\d{1,3}\s+)?([\d,]+\.\d{2})\s*\+?/im,
    /(?:^|\n)\s*Direct\s+Deposits?\s+(?:\d{1,3}\s+)?([\d,]+\.\d{2})\s*\+?/im,
    /(?:^|\n)\s*Mobile\s+Deposits?\s+(?:\d{1,3}\s+)?([\d,]+\.\d{2})\s*\+?/im,
    /(?:^|\n)\s*(?:Wire|Incoming)\s+Deposits?\s+(?:\d{1,3}\s+)?([\d,]+\.\d{2})\s*\+?/im,
    /(?:^|\n)\s*(?:Misc(?:ellaneous)?|Other)\s+Deposits?\s+(?:\d{1,3}\s+)?([\d,]+\.\d{2})\s*\+?/im,
    /(?:^|\n)\s*ACH\s+(?:Deposits?|Credits?)\s+(?:\d{1,3}\s+)?([\d,]+\.\d{2})\s*\+?/im,
  ];
  for (const pat of EXTRA_DEPOSIT_PATTERNS) {
    const m = blockText.match(pat);
    if (m) {
      const amt = parseFloat(m[1].replace(/,/g, ""));
      if (amt > 0) {
        // console.log(`[DepositExtract-Core] Extra deposit category found: "${m[0].trim()}" → $${amt.toFixed(2)}`);
        extraDepositTotal += amt;
      }
    }
  }

  const allHaveInline = labelOrder.every(lp => lp.inlineAmount !== undefined && lp.inlineAmount > 0);
  if (allHaveInline) {
    const depositsEntry = labelOrder.find(l => l.key === "deposits");
    const begBalEntry = labelOrder.find(l => l.key === "beginningBalance");
    const endBalEntry = labelOrder.find(l => l.key === "endingBalance");
    const begBal = begBalEntry?.inlineAmount || 0;
    const deposits = depositsEntry!.inlineAmount! + extraDepositTotal;
    if (endBalEntry?.inlineAmount && begBal > 0) {
      const endBal = endBalEntry.inlineAmount;
      let totalDebits = 0;
      for (const lp of labelOrder) {
        if (lp.key === "checksPaid" || lp.key === "electronicWith" || lp.key === "otherWith" || lp.key === "fees" || lp.key === "debits") {
          totalDebits += lp.inlineAmount || 0;
        }
      }
      const expected = begBal + deposits - totalDebits;
      const diff = Math.abs(expected - endBal);
      const tolerance = Math.max(endBal, begBal) * 0.05;
      if (diff > tolerance && diff > 100) {
        // console.log(`[DepositExtract-Core] Account Summary inline balance check WARN: beg=$${begBal.toFixed(2)} + dep=$${deposits.toFixed(2)} - debits=$${totalDebits.toFixed(2)} = $${expected.toFixed(2)} vs ending=$${endBal.toFixed(2)} (diff=$${diff.toFixed(2)})`);
      }
    }
    // console.log(`[DepositExtract-Core] Account Summary (inline amounts): ${labelOrder.map(l => `${l.key}=$${l.inlineAmount?.toFixed(2)}`).join(", ")}`);
    return { deposits, beginningBalance: begBal };
  }

  const endingBalEntry = labelOrder.find(l => l.key === "endingBalance");
  const tableBoundaryLine = endingBalEntry
    ? endingBalEntry.lineIdx
    : Math.max(...labelOrder.map(l => l.lineIdx));
  const firstLabelLine = Math.min(...labelOrder.map(l => l.lineIdx));
  const tableStartChar = lines.slice(0, firstLabelLine).join("\n").length + (firstLabelLine > 0 ? 1 : 0);
  const tableEndChar = lines.slice(0, tableBoundaryLine + 1).join("\n").length + 200;
  const tableRegion = blockText.slice(tableStartChar, Math.min(blockText.length, tableEndChar));

  const labelsInRegion = labelOrder.filter(l => l.lineIdx <= tableBoundaryLine);

  const amountRegex = /\$?\-?\(?\d[\d,]*\.\d{2}\)?/g;
  const allAmounts: number[] = [];
  let am;
  while ((am = amountRegex.exec(tableRegion)) !== null) {
    allAmounts.push(Math.abs(parseFloat(am[0].replace(/[$,()]/g, ""))));
  }

  const depositsIdx = labelsInRegion.findIndex(l => l.key === "deposits");
  if (depositsIdx >= 0 && depositsIdx < allAmounts.length) {
    const begBalIdx = labelsInRegion.findIndex(l => l.key === "beginningBalance");
    const begBal = begBalIdx >= 0 && begBalIdx < allAmounts.length ? allAmounts[begBalIdx] : 0;
    const deposits = allAmounts[depositsIdx] + extraDepositTotal;

    const endBalIdx = labelsInRegion.findIndex(l => l.key === "endingBalance");
    if (endBalIdx >= 0 && endBalIdx < allAmounts.length && begBal > 0) {
      const endBal = allAmounts[endBalIdx];
      let totalDebits = 0;
      for (let i = 0; i < labelsInRegion.length && i < allAmounts.length; i++) {
        const k = labelsInRegion[i].key;
        if (k === "checksPaid" || k === "electronicWith" || k === "otherWith" || k === "fees" || k === "debits") {
          totalDebits += allAmounts[i];
        }
      }
      const expected = begBal + deposits - totalDebits;
      const diff = Math.abs(expected - endBal);
      const tolerance = Math.max(endBal, begBal) * 0.05;
      if (diff > tolerance && diff > 100) {
        // console.log(`[DepositExtract-Core] Account Summary balance check WARN: beg=$${begBal.toFixed(2)} + dep=$${deposits.toFixed(2)} - debits=$${totalDebits.toFixed(2)} = $${expected.toFixed(2)} vs ending=$${endBal.toFixed(2)} (diff=$${diff.toFixed(2)})`);
      }
    }

    // console.log(`[DepositExtract-Core] Account Summary (positional): labels=[${labelsInRegion.map(l => l.key).join(",")}], amounts=[${allAmounts.map(a => `$${a.toFixed(2)}`).join(",")}], depositsIdx=${depositsIdx} → $${deposits.toFixed(2)}`);
    return {
      deposits,
      beginningBalance: begBal,
    };
  }

  return null;
}

function extractHorizontalSummaryTableDeposit(text: string): number {
  const lines = text.split("\n");
  for (let i = 0; i < lines.length - 1; i++) {
    const line = lines[i];
    const depositHeaderMatch = line.match(/total\s+deposits?\s+and\s+(?:other\s+)?(?:credits?|additions?)/i);
    if (!depositHeaderMatch) continue;
    const hasAmountOnLine = /\$?\s*[\d,]+\.\d{2}/.test(line.slice(depositHeaderMatch.index! + depositHeaderMatch[0].length));
    if (hasAmountOnLine) continue;

    const depositColStart = depositHeaderMatch.index!;
    const depositColEnd = depositColStart + depositHeaderMatch[0].length;
    const depositColMid = (depositColStart + depositColEnd) / 2;

    for (let j = i + 1; j <= Math.min(i + 4, lines.length - 1); j++) {
      const valueLine = lines[j];
      const amtRegex = /\$?\s*([\d,]+\.\d{2})/g;
      let am;
      const amounts: { value: number; pos: number }[] = [];
      while ((am = amtRegex.exec(valueLine)) !== null) {
        amounts.push({ value: parseFloat(am[1].replace(/,/g, "")), pos: am.index + am[0].length / 2 });
      }
      if (amounts.length < 2) continue;

      let bestMatch: { value: number; dist: number } | null = null;
      for (const a of amounts) {
        const dist = Math.abs(a.pos - depositColMid);
        if (!bestMatch || dist < bestMatch.dist) {
          bestMatch = { value: a.value, dist };
        }
      }
      if (bestMatch && bestMatch.value > 100 && bestMatch.value < 100_000_000) {
        const checksHeaderMatch = line.match(/total\s+checks?\s+and\s+(?:other\s+)?debits?/i);
        if (checksHeaderMatch) {
          const checksColMid = checksHeaderMatch.index! + checksHeaderMatch[0].length / 2;
          let checksAmt: number | null = null;
          let checksMinDist = Infinity;
          for (const a of amounts) {
            const dist = Math.abs(a.pos - checksColMid);
            if (dist < checksMinDist) {
              checksMinDist = dist;
              checksAmt = a.value;
            }
          }
          if (checksAmt && Math.abs(checksAmt - bestMatch.value) < 0.01) {
            continue;
          }
        }

        const balanceMatch = line.match(/\bbalance\b/i);
        if (balanceMatch) {
          const balColMid = balanceMatch.index! + balanceMatch[0].length / 2;
          const balAmt = amounts.reduce((best, a) => {
            const dist = Math.abs(a.pos - balColMid);
            return dist < best.dist ? { value: a.value, dist } : best;
          }, { value: 0, dist: Infinity });

          const begBal = balAmt.value;
          if (begBal > 0) {
            const expected = begBal + bestMatch.value;
            const totalDebits = amounts.find(a => {
              if (Math.abs(a.value - bestMatch!.value) < 0.01) return false;
              if (Math.abs(a.value - begBal) < 0.01) return false;
              return a.value > 1000;
            });
          }
        }

        return bestMatch.value;
      }
    }
  }

  const depositLabelRegex = /total\s+deposits?\s+and\s+(?:other\s+)?(?:credits?|additions?)\s*\n\s*\$?\s*([\d,]+\.\d{2})/gi;
  let dlm;
  let best = 0;
  while ((dlm = depositLabelRegex.exec(text)) !== null) {
    const amt = parseFloat(dlm[1].replace(/,/g, ""));
    if (amt > 100 && amt < 100_000_000 && amt > best) best = amt;
  }
  if (best > 0) return best;

  const multiLineRegex = /total\s+deposits?\s*\n\s*and\s*\n?\s*(?:other\s+)?(?:credits?|additions?)\s*\n\s*\$?\s*([\d,]+\.\d{2})/gi;
  let mlm;
  while ((mlm = multiLineRegex.exec(text)) !== null) {
    const amt = parseFloat(mlm[1].replace(/,/g, ""));
    if (amt > 100 && amt < 100_000_000 && amt > best) best = amt;
  }
  return best;
}

function extractColumnLayoutDeposit(text: string): number {
  for (const headerPattern of SUMMARY_SECTION_HEADERS_CORE) {
    const headerMatch = text.match(headerPattern);
    if (headerMatch) {
      const summaryStart = headerMatch.index!;
      const summaryBlock = text.slice(summaryStart, summaryStart + 1500);
      const parsed = parseCheckingSummaryTableCore(summaryBlock);
      if (parsed && parsed.deposits > 100 && parsed.deposits < 100_000_000) {
        return parsed.deposits;
      }
    }
  }

  const depositRegex = /deposits?\s+and\s+(?:other\s+)?(?:additions?|credits?)/gi;
  let depositMatch;
  let best = 0;
  while ((depositMatch = depositRegex.exec(text)) !== null) {
    const nearby = text.slice(depositMatch.index, depositMatch.index + 400);
    const hasColumnLayout = /(?:Deposits\s+and\s+\w+)\n(?:(?:Checks|Electronic|ATM|Fees|Ending|Other|Service|Misc|Wire|Beginning|CHECKING|Chase|INSTANCES|AMOUNT)\b[^\n]*\n)+/i.test(nearby)
      || /INSTANCES\s*\n\s*AMOUNT/i.test(nearby);
    if (hasColumnLayout) {
      const amountRegex = /\$?([\d,]+\.\d{2})/g;
      const allAmounts: number[] = [];
      let cm;
      while ((cm = amountRegex.exec(nearby)) !== null) {
        const amt = parseFloat(cm[1].replace(/,/g, ""));
        if (amt > 0) allAmounts.push(amt);
      }
      if (allAmounts.length >= 2 && allAmounts[1] > 100 && allAmounts[1] < 100_000_000 && allAmounts[1] > best) {
        // console.log(`[DepositExtract-Core] Column layout fallback: skip $${allAmounts[0].toFixed(2)} (beg bal), use $${allAmounts[1].toFixed(2)} (deposits)`);
        best = allAmounts[1];
      }
    }
  }
  if (best > 0) return best;

  return 0;
}

function extractCreditThisPeriodFromSection(text: string): number {
  const labelPatterns = [
    /(\d+)\s+credit\(?s?\)?\s+this\s+period/gi,
    /credit\(?s?\)?\s+this\s+period/gi,
    /credit\s*\(\s*s\s*\)\s*this\s*period/gi,
    /credits?\s+this\s+(?:statement\s+)?period/gi,
  ];
  let best = 0;
  for (const labelRegex of labelPatterns) {
    let labelMatch;
    while ((labelMatch = labelRegex.exec(text)) !== null) {
      const forwardRange = 500;
      const backwardRange = 200;
      const startBack = Math.max(0, labelMatch.index - backwardRange);
      const endFwd = Math.min(text.length, labelMatch.index + labelMatch[0].length + forwardRange);
      const nearby = text.slice(labelMatch.index, endFwd);
      const behind = text.slice(startBack, labelMatch.index);
      const amountRegex = /\$?([\d,]+\.\d{2})/g;
      let am;
      while ((am = amountRegex.exec(nearby)) !== null) {
        const amt = parseFloat(am[1].replace(/,/g, ""));
        if (amt > 100 && amt < 100_000_000 && amt > best) {
          // console.log(`[DepositExtract-Core] Credit(s) This Period proximity (fwd): $${amt.toFixed(2)}`);
          best = amt;
          break;
        }
      }
      if (best === 0) {
        const behindAmounts: number[] = [];
        const behindRegex = /\$?([\d,]+\.\d{2})/g;
        let bm;
        while ((bm = behindRegex.exec(behind)) !== null) {
          const amt = parseFloat(bm[1].replace(/,/g, ""));
          if (amt > 100 && amt < 100_000_000) behindAmounts.push(amt);
        }
        if (behindAmounts.length > 0) {
          const lastBehind = behindAmounts[behindAmounts.length - 1];
          if (lastBehind > best) {
            // console.log(`[DepositExtract-Core] Credit(s) This Period proximity (behind): $${lastBehind.toFixed(2)}`);
            best = lastBehind;
          }
        }
      }
      if (best > 0) break;
    }
    if (best > 0) break;
  }
  return best;
}

function extractBalanceCreditsDebitsFormat(text: string): number {
  const headerPatterns = [
    /balance\s+(?:last|previous|prior)\s+(?:statement|period)\s+credits?\s+debits?\s+balance\s+(?:this|current|new)\s+(?:statement|period)/i,
    /balance\s+(?:last|previous|prior)\s+(?:statement|period)\s*\n\s*credits?\s*\n?\s*debits?\s*\n?\s*balance\s+(?:this|current|new)\s+(?:statement|period)/i,
    /balance\s+(?:last|previous|prior)\s+(?:statement|period)[\s\S]{0,80}credits?[\s\S]{0,40}debits?[\s\S]{0,80}balance\s+(?:this|current|new)\s+(?:statement|period)/i,
  ];

  for (const headerPattern of headerPatterns) {
    const headerMatch = text.match(headerPattern);
    if (!headerMatch) continue;

    const afterHeader = text.slice(headerMatch.index! + headerMatch[0].length, headerMatch.index! + headerMatch[0].length + 500);
    const amountRegex = /\$?\s*([\d,]+\.\d{2})/g;
    const amounts: number[] = [];
    let am;
    while ((am = amountRegex.exec(afterHeader)) !== null) {
      amounts.push(parseFloat(am[1].replace(/,/g, "")));
      if (amounts.length >= 4) break;
    }

    if (amounts.length >= 4) {
      const [begBal, credits, debits, endBal] = amounts;
      const expected = begBal + credits - debits;
      const diff = Math.abs(expected - endBal);
      // console.log(`[DepositExtract-Core] Balance/Credits/Debits/Balance format: beg=$${begBal.toFixed(2)} credits=$${credits.toFixed(2)} debits=$${debits.toFixed(2)} end=$${endBal.toFixed(2)} (diff=$${diff.toFixed(2)})`);
      return credits;
    }

    if (amounts.length >= 2) {
      // console.log(`[DepositExtract-Core] Balance/Credits/Debits/Balance partial: credits=$${amounts[1].toFixed(2)}`);
      return amounts[1];
    }
  }

  const balLastMatch = text.match(/balance\s+(?:last|previous|prior)\s+(?:statement|period)/i);
  if (balLastMatch) {
    const region = text.slice(balLastMatch.index!, Math.min(text.length, balLastMatch.index! + 800));
    const creditsMatch = region.match(/\bcredits?\b/i);
    const debitsMatch = region.match(/\bdebits?\b/i);
    const balThisMatch = region.match(/balance\s+(?:this|current|new)\s+(?:statement|period)/i);
    if (creditsMatch && debitsMatch) {
      const amountRegex = /\$?\s*([\d,]+\.\d{2})/g;
      const amounts: number[] = [];
      let am;
      while ((am = amountRegex.exec(region)) !== null) {
        amounts.push(parseFloat(am[1].replace(/,/g, "")));
      }
      if (amounts.length >= 3) {
        const expectedCreditsIdx = 1;
        if (amounts[expectedCreditsIdx] > 100) {
          if (amounts.length >= 4) {
            const [begBal, credits, debits, endBal] = amounts;
            const expected = begBal + credits - debits;
            const diff = Math.abs(expected - endBal);
            if (diff < Math.max(endBal, credits) * 0.05) {
              // console.log(`[DepositExtract-Core] Balance Last/Credits/Debits/Balance This (loose): beg=$${begBal.toFixed(2)} credits=$${credits.toFixed(2)} debits=$${debits.toFixed(2)} end=$${endBal.toFixed(2)} ✓`);
              return credits;
            }
          }
          // console.log(`[DepositExtract-Core] Balance Last/Credits (loose): credits=$${amounts[expectedCreditsIdx].toFixed(2)} from amounts=[${amounts.slice(0,5).map(a=>`$${a.toFixed(2)}`).join(",")}]`);
          return amounts[expectedCreditsIdx];
        }
      }
    }
  }

  return 0;
}

function extractAccountSummaryCreditFromSection(text: string): number {
  const creditMatch = text.match(/credit\(?s?\)?\s+this\s+(?:statement\s+)?period/i);
  const beginMatch = text.match(/(?:beginning|opening|starting|previous)\s+balance/i);
  if (!creditMatch || !beginMatch) return 0;

  const creditPos = creditMatch.index!;
  const beginPos = beginMatch.index!;

  const debitMatch = text.match(/debit\(?s?\)?\s+this\s+(?:statement\s+)?period/i);
  const endMatch = text.match(/(?:ending|closing)\s+balance/i);

  const descLabels = [
    { label: "beginning", pos: beginPos },
    { label: "credit", pos: creditPos },
  ];
  if (debitMatch) descLabels.push({ label: "debit", pos: debitMatch.index! });
  if (endMatch) descLabels.push({ label: "ending", pos: endMatch.index! });
  descLabels.sort((a, b) => a.pos - b.pos);

  const creditOrder = descLabels.findIndex(d => d.label === "credit");
  if (creditOrder < 0) return 0;

  const endingEntry = descLabels.find(d => d.label === "ending");
  const lastDescPos = descLabels[descLabels.length - 1].pos;
  const boundaryEnd = endingEntry ? endingEntry.pos + 500 : lastDescPos + 1000;
  const searchArea = text.slice(lastDescPos, Math.min(text.length, boundaryEnd));
  const amountRegex = /\$?([\d,]+\.\d{2})/g;
  const amounts: number[] = [];
  let am;
  while ((am = amountRegex.exec(searchArea)) !== null) {
    const val = parseFloat(am[1].replace(/,/g, ""));
    if (val > 0) amounts.push(val);
  }

  if (amounts.length > creditOrder && amounts[creditOrder] > 100) {
    const begIdx = descLabels.findIndex(d => d.label === "beginning");
    const endIdx = descLabels.findIndex(d => d.label === "ending");
    const debitIdx = descLabels.findIndex(d => d.label === "debit");
    if (begIdx >= 0 && endIdx >= 0 && debitIdx >= 0 && begIdx < amounts.length && endIdx < amounts.length && debitIdx < amounts.length) {
      const bv = amounts[begIdx];
      const ev = amounts[endIdx];
      const dv = amounts[debitIdx];
      const cv = amounts[creditOrder];
      const expected = bv + cv - dv;
      const diff = Math.abs(expected - ev);
      if (diff > 1.0 && diff / Math.max(ev, 1) > 0.01) {
        // console.log(`[DepositExtract-Core] Account Summary balance check WARN: beg=$${bv.toFixed(2)} + credit=$${cv.toFixed(2)} - debit=$${dv.toFixed(2)} = $${expected.toFixed(2)} vs ending=$${ev.toFixed(2)} (diff=$${diff.toFixed(2)})`);
      }
    }
    // console.log(`[DepositExtract-Core] Account Summary column-format: credit is description #${creditOrder}, amount=$${amounts[creditOrder].toFixed(2)}`);
    return amounts[creditOrder];
  }
  return 0;
}

function parseMoneyFromLine(line: string): number | null {
  const m = line.match(/^[\s]*[-]?\$?\(?([\d,]+\.\d{2})\)?(?:\s*(?:CR|DR))?[\s]*$/i);
  if (!m) return null;
  const v = parseFloat(m[1].replace(/,/g, ""));
  return v > 0 ? v : null;
}

function extractChaseAccountSummaryFromSection(sectionText: string): number {
  const labelMatch = sectionText.match(/deposits?\s+and\s+other\s+credits?[\s:]*(?:\n|(?=\$))/i);
  if (!labelMatch) return 0;

  const inlineAmt = labelMatch[0].match(/\$?([\d,]+\.\d{2})/);
  if (inlineAmt) {
    const v = parseFloat(inlineAmt[1].replace(/,/g, ""));
    if (v > 100 && v < 100_000_000) {
      // console.log(`[DepositExtract-Core] Chase Account Summary (inline): $${v.toFixed(2)}`);
      return v;
    }
  }

  const pos = labelMatch.index!;
  const nearby = sectionText.slice(pos, pos + 800);
  const lines = nearby.split("\n").map(l => l.trim()).filter(Boolean);
  const descLabels: string[] = [];
  const amountValues: number[] = [];

  for (const line of lines) {
    const amt = parseMoneyFromLine(line);
    if (amt !== null) {
      amountValues.push(amt);
    } else if (/^#\s*of\s/i.test(line) || /^\d+$/.test(line)) {
      continue;
    } else if (/^[A-Za-z]/.test(line)) {
      descLabels.push(line.toLowerCase());
    }
  }

  const depositIdx = descLabels.findIndex(l => /deposits?\s+and\s+other\s+credits?/i.test(l));
  if (depositIdx >= 0 && depositIdx < amountValues.length) {
    const amt = amountValues[depositIdx];
    if (amt > 100 && amt < 100_000_000) {
      // console.log(`[DepositExtract-Core] Chase Account Summary: label="${descLabels[depositIdx]}" → $${amt.toFixed(2)}`);
      return amt;
    }
  }

  const creditsCount = /# of deposits\/credits:\s*(\d+)/i.exec(nearby);
  if (creditsCount && amountValues.length >= 2) {
    const begBal = sectionText.match(/(?:beginning|opening|previous)\s+balance[:\s]*\$?([\d,]+\.\d{2})/i);
    const bv = begBal ? parseFloat(begBal[1].replace(/,/g, "")) : -1;
    const endBal = sectionText.match(/(?:ending|closing)\s+balance[:\s]*\$?([\d,]+\.\d{2})/i);
    const ev = endBal ? parseFloat(endBal[1].replace(/,/g, "")) : -1;
    for (const amt of amountValues) {
      if (bv >= 0 && Math.abs(amt - bv) < 0.01) continue;
      if (ev >= 0 && Math.abs(amt - ev) < 0.01) continue;
      if (amt > 100 && amt < 100_000_000) {
        // console.log(`[DepositExtract-Core] Chase Account Summary (credits count): $${amt.toFixed(2)}`);
        return amt;
      }
    }
  }
  return 0;
}

function extractMultiCategoryDeposits(sectionText: string): number {
  const cleanText = sectionText.replace(/\|/g, ' ').replace(/[-]{3,}/g, ' ');
  const DEPOSIT_CATEGORIES = [
    { label: "deposits", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?(?:Total\s+)?Deposits?\s+(?:(\d{1,3})\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im, exclusive: true },
    { label: "automatic", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?Automatic\s+(?:Deposits?|Additions?)\s+(?:(\d{1,3})\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "electronic", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?Electronic\s+(?:Deposits?|Additions?)\s+(?:(\d{1,3})\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "direct", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?Direct\s+(?:Deposits?|Additions?)\s+(?:(\d{1,3})\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "mobile", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?Mobile\s+(?:Deposits?|Additions?)\s+(?:(\d{1,3})\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "wire", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?(?:Wire|Incoming)\s+(?:Deposits?|Additions?)\s+(?:(\d{1,3})\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "misc", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?(?:Misc(?:ellaneous)?|Other)\s+(?:Deposits?|Additions?)\s+(?:(\d{1,3})\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "otherCredits", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?Other\s+Credits?\s+(?:(\d{1,3})\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "customer", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?Customer\s+(?:Deposits?|Additions?)\s+(?:(\d{1,3})\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "ach", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?ACH\s+(?:Deposits?|Credits?|Additions?)\s+(?:(\d{1,3})\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "teller", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?Teller\s+(?:Deposits?|Additions?)\s+(?:(\d{1,3})\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "atmDeposits", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?ATM\s+(?:Deposits?(?:\s+and\s+Additions?)?|Additions?)\s+(?:(\d{1,3})\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
  ];

  const hasAutoDeposit = /automatic\s+deposit/i.test(cleanText);
  const hasAuto = /automatic/i.test(sectionText);
  const summarySnippetMatch = sectionText.match(/(?:ACCOUNT\s*SUMMARY|CHECKING\s+SUMMARY|BUSINESS\s+(?:CHECKING\s+)?SUMMARY)/i);
  if (summarySnippetMatch) {
    const snippet = sectionText.slice(summarySnippetMatch.index!, summarySnippetMatch.index! + 600).replace(/\n/g, '\\n');
    // console.log(`[MultiCatDeposit] Summary section text: "${snippet.substring(0, 400)}"`);
  } else {
    // console.log(`[MultiCatDeposit] No summary header found. hasAutoDeposit=${hasAutoDeposit}, hasAuto=${hasAuto}, textLen=${sectionText.length}`);
    const depositLines = sectionText.match(/.*deposit.*/gi) || [];
    // console.log(`[MultiCatDeposit] Deposit lines (${depositLines.length}): ${depositLines.slice(0, 5).map(l => `"${l.trim().substring(0, 100)}"`).join(", ")}`);
  }
  if (hasAutoDeposit) {
    const autoLine = sectionText.match(/.*automatic\s+deposit.*/i);
    // console.log(`[MultiCatDeposit] Found "automatic deposit" in text, line: "${autoLine?.[0]?.trim().substring(0, 120)}"`);
  }

  let total = 0;
  let count = 0;
  const parts: string[] = [];
  const matched: { label: string; amt: number }[] = [];
  const DEPOSIT_PREFIX_WORDS = /\b(?:customer|other|electronic|direct|mobile|wire|incoming|automatic|misc|miscellaneous|ach|teller|counter|merchant|lockbox|lock\s*box|branch|atm|cash|check|return|returned|credit|remit|pos|point)\b/i;
  for (const cat of DEPOSIT_CATEGORIES) {
    const m = cleanText.match(cat.pattern);
    if (m) {
      const amt = parseFloat((m[2] || m[1]).replace(/,/g, ""));
      if (amt > 0 && amt < 100_000_000) {
        if ((cat as any).exclusive) {
          const matchedLine = m[0].trim();
          const beforeDeposit = matchedLine.replace(/deposits?\s.*$/i, "").trim();
          const lastWords = beforeDeposit.replace(/[\d\s+$.,]+/g, " ").trim();
          if (DEPOSIT_PREFIX_WORDS.test(lastWords)) continue;
        }
        matched.push({ label: cat.label, amt });
      }
    }
  }
  const depositsEntry = matched.find(m => m.label === "deposits");
  const subCategories = matched.filter(m => m.label !== "deposits");
  console.log(`[MultiCatDeposit-Core] matched=${matched.length}: ${matched.map(m => `${m.label}=$${m.amt.toFixed(2)}`).join(", ")}`);
  if (depositsEntry && subCategories.length >= 1) {
    const subSum = subCategories.reduce((s, m) => s + m.amt, 0);
    if (depositsEntry.amt >= subSum) {
      for (const m of [depositsEntry]) { total += m.amt; count++; parts.push(`${m.label}=$${m.amt.toFixed(2)} (parent total)`); }
    } else {
      for (const m of matched) { total += m.amt; count++; parts.push(`${m.label}=$${m.amt.toFixed(2)}`); }
    }
  } else {
    for (const m of matched) { total += m.amt; count++; parts.push(`${m.label}=$${m.amt.toFixed(2)}`); }
  }
  if (count >= 2) {
    console.log(`[MultiCatDeposit-Core] Returning sum: $${total.toFixed(2)} from ${count} categories: ${parts.join(", ")}`);
    return total;
  }
  if (count === 1 && subCategories.length === 1 && !depositsEntry) {
    console.log(`[MultiCatDeposit-Core] Only 1 sub-category matched (${parts.join(", ")}), not enough for sum — returning 0`);
  }
  if (hasAutoDeposit && count < 2) {
    // console.log(`[MultiCatDeposit] WARN: "automatic deposit" present but only ${count} categories matched. Trying fallback...`);
    const fallbackPat = /(?:automatic|electronic|direct|mobile|wire|ach|teller|atm)\s+(?:deposits?|additions?)(?:\s+and\s+additions?)?\s+(?:\d{1,3}\s+)?(?:\$?\s*)?([\d,]+\.\d{2})\s*\+?/gi;
    let fm;
    let fallbackTotal = 0;
    while ((fm = fallbackPat.exec(cleanText)) !== null) {
      const amt = parseFloat(fm[1].replace(/,/g, ""));
      if (amt > 0 && amt < 100_000_000) {
        // console.log(`[MultiCatDeposit] Fallback match: "${fm[0].trim()}" → $${amt.toFixed(2)}`);
        fallbackTotal += amt;
      }
    }
    const mainDepPat = /(?:^|\n)\s*Deposits?\s+(?:\d{1,3}\s+)?(?:\$?\s*)?([\d,]+\.\d{2})\s*\+?/im;
    const mainMatch = cleanText.match(mainDepPat);
    const mainAmt = mainMatch ? parseFloat(mainMatch[1].replace(/,/g, "")) : 0;
    if (mainAmt > 0 && fallbackTotal > 0) {
      const combined = mainAmt + fallbackTotal;
      // console.log(`[MultiCatDeposit] Fallback combined: deposits=$${mainAmt.toFixed(2)} + extras=$${fallbackTotal.toFixed(2)} = $${combined.toFixed(2)}`);
      return combined;
    }
  }
  return 0;
}

export function extractDepositSummaryFromSection(sectionText: string): number {
  const multiCat = extractMultiCategoryDeposits(sectionText);
  if (multiCat > 0) {
    console.log(`[DepositExtract-Core] Multi-category deposit sum wins: $${multiCat.toFixed(2)}`);
    return multiCat;
  }

  const horizontalTableAmt = extractHorizontalSummaryTableDeposit(sectionText);
  if (horizontalTableAmt > 0) {
    console.log(`[DepositExtract-Core] Horizontal summary table wins: $${horizontalTableAmt.toFixed(2)}`);
    return horizontalTableAmt;
  }

  const balCredDebAmt = extractBalanceCreditsDebitsFormat(sectionText);
  const columnLayoutAmt = extractColumnLayoutDeposit(sectionText);
  const creditThisPeriodAmt = extractCreditThisPeriodFromSection(sectionText);
  const accountSummaryAmt = extractAccountSummaryCreditFromSection(sectionText);
  const chaseSummaryAmt = extractChaseAccountSummaryFromSection(sectionText);

  if (balCredDebAmt > 0) {
    // console.log(`[DepositExtract-Core] Balance/Credits/Debits format wins: $${balCredDebAmt.toFixed(2)}`);
    return balCredDebAmt;
  }

  const PRIORITY_PATTERNS = [
    /deposit\s+amount\s*[=:+\-–►→*]?\s*\$?\s*([\d,.]+)/gi,
    /(\d+)\s+deposits?\s*\/\s*credits?\s+\$?([\d,.]+)/gi,
    /(\d+)\s+deposits?\s+and\s+other\s+credits?\s+\$?([\d,.]+)/gi,
    /(\d+)\s+credit\(?s?\)?\s+this\s+period\s+\$?([\d,.]+)/gi,
    /total\s+deposits?\s+and\s+(?:other\s+)?(?:credits?|additions?)\s+\$?([\d,.]+)/gi,
    /total\s+(?:other\s+)?deposits?\s+(?:&|and)\s+(?:other\s+)?credits?\s+\$?([\d,.]+)/gi,
    /deposits,?\s+credits?\s+and\s+interest\s+\$?([\d,.]+)/gi,
    /total\s+deposits?\s+and\s+additions\s+\$?([\d,.]+)/gi,
    /total\s+additions?\s+\$?([\d,.]+)/gi,
    /deposits?\s+and\s+(?:other\s+)?(?:additions?|credits?)\s+(\d+)\s+\$?([\d,.]+)/gi,
    /deposits?\s+and\s+(?:other\s+)?(?:additions?|credits?)\s+\$?([\d,.]+)/gi,
    /deposits?\s*\/\s*credits?\s+\$?([\d,.]+)/gi,
    /(?:^|\n)\s*Deposits\s+\$?([\d,.]+)\s*[+]?/gi,
    /(?:^|\n)\s*Deposits?\s+(\d{1,3})\s+([\d,.]+)\s*[+]?/gi,
    /credits?\s*\(\s*\+\s*\)\s*\$?\s*([\d,.]+)/gi,
    /total\s+credits?\s+\$?\s*([\d,.]+)/gi,
    /total\s+deposits?\s+(?:\d+\s+)?\$?\s*([\d,.]+)/gi,
  ];

  let priorityAmt = 0;
  for (const pat of PRIORITY_PATTERNS) {
    const regex = new RegExp(pat.source, pat.flags);
    let m;
    let best = 0;
    while ((m = regex.exec(sectionText)) !== null) {
      const amt = parseDollar(m[m.length - 1]);
      if (amt > 100 && amt < 100_000_000 && amt > best) best = amt;
    }
    if (best > 0) { priorityAmt = best; break; }
  }

  console.log(`[DepositExtract-Core] Candidates: balCredDeb=$${balCredDebAmt.toFixed(2)}, columnLayout=$${columnLayoutAmt.toFixed(2)}, creditPeriod=$${creditThisPeriodAmt.toFixed(2)}, acctSummary=$${accountSummaryAmt.toFixed(2)}, chaseSummary=$${chaseSummaryAmt.toFixed(2)}, priority=$${priorityAmt.toFixed(2)}`);

  const bestSpecial = Math.max(columnLayoutAmt, creditThisPeriodAmt, accountSummaryAmt, chaseSummaryAmt);
  if (bestSpecial > 0 && bestSpecial > priorityAmt) {
    console.log(`[DepositExtract-Core] Special extraction $${bestSpecial.toFixed(2)} wins over inline $${priorityAmt.toFixed(2)}`);
    return bestSpecial;
  }
  if (priorityAmt > 0) {
    console.log(`[DepositExtract-Core] Priority pattern wins: $${priorityAmt.toFixed(2)}`);
    return priorityAmt;
  }
  if (bestSpecial > 0) return bestSpecial;

  const ADDITIVE_PATTERNS = [
    /total\s+(?:customer|other|direct|regular|atm|mobile|ach|wire|check|electronic|misc(?:ellaneous)?|teller)?\s*deposits?\s+\$?\s*([\d,]+\.\d{2})/gi,
    /total\s+(?:customer|other|direct|regular|atm|mobile|ach|wire|check|electronic|misc(?:ellaneous)?|teller)?\s*(?:credits?|additions?)\s+\$?\s*([\d,]+\.\d{2})/gi,
    /(?:customer|other|direct|regular|atm|mobile|ach|wire|check|electronic|misc(?:ellaneous)?|teller|automatic)\s+(?:deposits?|credits?)\s+(?:\d+\s+)?\$?\s*([\d,]+\.\d{2})\s*[+]?/gi,
    /(?:deposits?|credits?)\s+(?:and\s+)?(?:interest|dividends?)\s+\$?\s*([\d,]+\.\d{2})/gi,
  ];

  const foundEntries: { amt: number; pos: number; label: string }[] = [];

  for (const pat of ADDITIVE_PATTERNS) {
    const regex = new RegExp(pat.source, pat.flags);
    let m;
    while ((m = regex.exec(sectionText)) !== null) {
      const amt = parseDollar(m[m.length - 1]);
      const pos = m.index;
      const label = m[0].replace(/\$?\s*[\d,]+\.\d{2}/g, "").replace(/\d+/g, "").replace(/\s+/g, " ").trim().toLowerCase();
      if (amt > 100 && amt < 100_000_000) {
        let tooClose = false;
        for (const existing of foundEntries) {
          if (Math.abs(existing.pos - pos) < 10) { tooClose = true; break; }
        }
        if (!tooClose) {
          const duplicateLabel = foundEntries.some(e => e.label === label && Math.abs(e.amt - amt) < 0.01);
          const sameAmountRelatedLabel = foundEntries.some(e => {
            if (Math.abs(e.amt - amt) > 0.01) return false;
            const a = e.label.replace(/^total\s+/, "");
            const b = label.replace(/^total\s+/, "");
            return a === b || e.label.includes(b) || label.includes(a);
          });
          if (!duplicateLabel && !sameAmountRelatedLabel) {
            foundEntries.push({ amt, pos, label });
          } else if (sameAmountRelatedLabel) {
            // console.log(`[PostAI-DepositSum] Dedup: skipping "$${amt.toFixed(2)}" (${label}) — same amount as related label already found`);
          }
        }
      }
    }
  }

  if (foundEntries.length > 1) {
    const labelCounts: Record<string, number> = {};
    for (const e of foundEntries) {
      labelCounts[e.label] = (labelCounts[e.label] || 0) + 1;
    }
    const maxLabelCount = Math.max(...Object.values(labelCounts));
    if (maxLabelCount > 4) {
      // console.log(`[PostAI-DepositSum] Skipping — found ${foundEntries.length} entries but label "${Object.entries(labelCounts).find(([,v]) => v === maxLabelCount)?.[0]}" appears ${maxLabelCount} times (likely individual transactions, not summary categories)`);
      return 0;
    }

    const sorted = [...foundEntries].sort((a, b) => b.amt - a.amt);
    const largest = sorted[0];
    const restSum = sorted.slice(1).reduce((s, e) => s + e.amt, 0);
    const combined = largest.amt + restSum;
    if (restSum > 0 && Math.abs(largest.amt - restSum) / Math.max(largest.amt, 1) < 0.15) {
      // console.log(`[PostAI-DepositSum] Double-count detected: largest=$${largest.amt.toFixed(2)} (${largest.label}) ≈ rest sum=$${restSum.toFixed(2)} — using largest only`);
      return largest.amt;
    }
    // console.log(`[PostAI-DepositSum] Combined ${foundEntries.length} deposit categories: ${foundEntries.map(e => `$${e.amt.toFixed(2)} (${e.label})`).join(" + ")} = $${combined.toFixed(2)}`);
    return combined;
  }
  if (foundEntries.length === 1) return foundEntries[0].amt;

  const FALLBACK_PATTERNS = [
    /deposits?\s*\/\s*credits?\s*[:=]?\s*\$?([\d,]+\.?\d*)/gi,
  ];

  for (const pat of FALLBACK_PATTERNS) {
    const regex = new RegExp(pat.source, pat.flags);
    let m;
    let best = 0;
    while ((m = regex.exec(sectionText)) !== null) {
      const amt = parseDollar(m[m.length - 1]);
      if (amt > 100 && amt < 100_000_000 && amt > best) best = amt;
    }
    if (best > 0) return best;
  }

  const depositIdx = sectionText.search(/deposits?\s+and\s+(?:other\s+)?(?:additions?|credits?)/i);
  if (depositIdx >= 0) {
    const nearby = sectionText.slice(depositIdx, depositIdx + 400);
    const isTransactionList = /\bDate\b[\s\n]+\bDescription\b/i.test(nearby)
      || /\n\s*\d{1,2}\/\d{1,2}\s+[A-Z]/m.test(nearby);
    if (!isTransactionList) {
      let begBalToExclude = 0;
      for (const headerPattern of SUMMARY_SECTION_HEADERS_CORE) {
        const csMatch = sectionText.match(headerPattern);
        if (csMatch) {
          const parsed = parseCheckingSummaryTableCore(sectionText.slice(csMatch.index!, csMatch.index! + 1500));
          if (parsed) { begBalToExclude = parsed.beginningBalance; break; }
        }
      }
      if (begBalToExclude === 0) {
        const begBalMatch = sectionText.match(/(?:beginning|opening|starting|previous)\s+balance[^\n]*?\$?([\d,]+\.\d{2})/i);
        if (begBalMatch) {
          begBalToExclude = parseFloat(begBalMatch[1].replace(/,/g, ""));
        }
      }

      const amountMatches = nearby.match(/\$?([\d,]+\.\d{2})/g);
      if (amountMatches) {
        for (const amtStr of amountMatches) {
          const amt = parseFloat(amtStr.replace(/[$,]/g, ""));
          if (amt > 100 && amt < 100_000_000) {
            if (begBalToExclude > 0 && Math.abs(amt - begBalToExclude) < 0.01) {
              // console.log(`[DepositExtract-Core] Proximity: skipping $${amt.toFixed(2)} (matches Beginning Balance)`);
              continue;
            }
            // console.log(`[DepositExtract-Core] Proximity match: $${amt.toFixed(2)}`);
            return amt;
          }
        }
      }
    } else {
      // console.log(`[DepositExtract-Core] Skipping proximity match — detected transaction listing, not summary`);
    }
  }

  return 0;
}

function splitRawTextIntoSections(rawText: string): { name: string; text: string }[] {
  const sections: { name: string; text: string }[] = [];
  const stmtRegex = /={5,}\s*STATEMENT\s+\d+:\s*"([^"]+)"\s*={5,}\s*\n([\s\S]*?)(?=={5,}\s*END\s+STATEMENT|$)/gi;
  let stmtMatch;
  while ((stmtMatch = stmtRegex.exec(rawText)) !== null) {
    sections.push({ name: stmtMatch[1].trim(), text: stmtMatch[2].trim() });
  }
  if (sections.length === 0) {
    const parts = rawText.split(/^---\s+(.+?)\s+---$/m).filter(Boolean);
    for (let i = 0; i < parts.length - 1; i += 2) {
      sections.push({ name: parts[i], text: parts[i + 1] || "" });
    }
  }
  if (sections.length === 0 && rawText.trim()) {
    sections.push({ name: "statement", text: rawText });
  }
  return sections;
}

function normalizeAIMonth(m: string): string {
  const ymd = m.match(/^(\d{4})[\-\/](\d{1,2})/);
  if (ymd) {
    const mo = parseInt(ymd[2]);
    const yr = ymd[1].slice(2);
    return `${mo}-${yr}`;
  }
  const parts = m.toLowerCase().split(/[\s\/\-]+/);
  const moNum = MONTH_ABBRS_MAP[parts[0]?.slice(0, 3)] || 0;
  if (moNum && parts[1]) {
    const yr = parts[1].length === 4 ? parts[1].slice(2) : parts[1];
    return `${moNum}-${yr}`;
  }
  return m;
}

export function extractAccountNumberFromText(text: string): string | null {
  return extractAccountLast4(text);
}

const NON_REVENUE_CREDIT_FILTER = /\b(transfer\s+from|xfer\s+from|online\s+transfer|internal\s+transfer|transfer\s+between|tfr\s+from|loan\s+proceed|loan\s+disburse|reversal|refund|chargeback|credit\s+adjustment|returned\s+item|nsf\s+reversal|overdraft\s+protection|od\s+protection|courtesy\s+credit|fee\s+reversal|interest\s+paid|interest\s+credit|ondeck|fundbox|bluevine|kabbage|kapitus|can\s*capital|credibly|rapid\s*finance|forward\s*fin|clear\s*balance|libertas|yellowstone|everest\s*bus|mantis\s*fund|cloudfund)\b/i;

export function sumCreditTransactionsFromSection(sectionText: string): number {
  const creditBlock = sectionText.match(/All Credit Transactions:\n([\s\S]*?)(?=\n(?:All Debit|Total Deposits|---\s+END|={5,})|$)/i);

  if (creditBlock) {
    const lines = creditBlock[1].split("\n");
    let total = 0;
    let count = 0;
    let skipped = 0;
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      if (NON_REVENUE_CREDIT_FILTER.test(trimmed)) {
        skipped++;
        continue;
      }
      const amtMatch = trimmed.match(/\|\s*\$?([\d,]+\.\d{2})(?:\s*\||\s*$)/);
      if (amtMatch) {
        const amt = parseFloat(amtMatch[1].replace(/,/g, ""));
        if (amt > 0 && amt < 100_000_000) {
          total += amt;
          count++;
        }
      }
    }
    if (count >= 1) {
      // console.log(`[PostAI-CreditSum] Summed ${count} credit transactions = $${total.toFixed(2)}${skipped > 0 ? ` (filtered ${skipped} non-revenue credits)` : ""}`);
      return total;
    }
  }

  const parsedCreditSum = sectionText.match(/(?:Parsed|Document\s+AI)\s+Credit\s+Transaction\s+Sum:\s*\$?([\d,]+\.\d{2})/i);
  if (parsedCreditSum) {
    const amt = parseFloat(parsedCreditSum[1].replace(/,/g, ""));
    if (amt > 0) {
      // console.log(`[PostAI-CreditSum] Using parsed credit sum (no line-level data for filtering): $${amt.toFixed(2)}`);
      return amt;
    }
  }

  return 0;
}

function correctLoanAmountsFromText(loanDetails: any[], rawText: string): void {
  if (!loanDetails || loanDetails.length === 0) return;
  const debits = parseDebitsFromText(rawText);
  // console.log(`[PostAI-AmountCorrect] Parsed ${debits.length} debits from raw text (${rawText.length} chars)`);
  if (debits.length === 0) return;

  for (const loan of loanDetails) {
    const lenderName = (loan.lender || "").toLowerCase();
    if (!lenderName || lenderName.length < 3) continue;

    const lenderKey = normalizeLenderKey(lenderName);
    const otherSameLender = loanDetails.filter(l => l !== loan && normalizeLenderKey((l.lender || "").toLowerCase()) === lenderKey);
    if (otherSameLender.length > 0) continue;

    const matchingDebits = debits.filter(d => {
      const descKey = normalizeLenderKey(d.description);
      return descKey.includes(lenderKey) || lenderKey.includes(descKey) ||
        (lenderKey.length >= 4 && d.description.toLowerCase().includes(lenderKey.substring(0, Math.min(lenderKey.length, 8))));
    });

    if (matchingDebits.length < 2) continue;

    const amountCounts = new Map<number, { count: number; latestDate: string; debits: typeof matchingDebits }>();
    for (const d of matchingDebits) {
      const key = Math.round(d.amount * 100) / 100;
      const existing = amountCounts.get(key);
      if (!existing) {
        amountCounts.set(key, { count: 1, latestDate: d.date, debits: [d] });
      } else {
        existing.count++;
        existing.debits.push(d);
        const parseDate = (ds: string) => {
          const [mm, dd, yy] = ds.split("/");
          return new Date(2000 + parseInt(yy), parseInt(mm) - 1, parseInt(dd)).getTime();
        };
        if (parseDate(d.date) > parseDate(existing.latestDate)) {
          existing.latestDate = d.date;
        }
      }
    }

    const aiAmount = loan.amount;

    const sortedByDate = [...matchingDebits].sort((a, b) => {
      const parseDate = (ds: string) => {
        const [mm, dd, yy] = ds.split("/");
        return new Date(2000 + parseInt(yy), parseInt(mm) - 1, parseInt(dd)).getTime();
      };
      return parseDate(b.date) - parseDate(a.date);
    });
    const mostRecentAmount = sortedByDate[0].amount;

    const uniqueAmounts = [...amountCounts.keys()];
    const allSameAmount = uniqueAmounts.length === 1;

    let bestAmount: number;
    let bestInfo: string;

    if (allSameAmount) {
      bestAmount = uniqueAmounts[0];
      bestInfo = `all-same=${amountCounts.get(uniqueAmounts[0])!.count}x`;
    } else {
      const sortedByFrequency = [...amountCounts.entries()].sort((a, b) => b[1].count - a[1].count);
      const topCount = sortedByFrequency[0][1].count;
      const secondCount = sortedByFrequency.length > 1 ? sortedByFrequency[1][1].count : 0;
      if (topCount >= 2 && topCount > secondCount) {
        bestAmount = sortedByFrequency[0][0];
        bestInfo = `most-frequent=${topCount}x (vs ${secondCount}x)`;
      } else {
        bestAmount = mostRecentAmount;
        bestInfo = `most-recent=${sortedByDate[0].date}`;
      }
    }

    const aiAmountKey = Math.round(aiAmount * 100) / 100;
    const aiAmountCount = amountCounts.get(aiAmountKey)?.count || 0;

    if (Math.abs(aiAmount - bestAmount) > 0.01 && bestAmount > 0) {
      if (aiAmountCount > 0) {
        // console.log(`[PostAI-AmountCorrect] ${loan.lender}: AI=$${aiAmount.toFixed(2)} appears ${aiAmountCount}x in text — keeping AI amount (not overriding to $${bestAmount.toFixed(2)} ${bestInfo}). All amounts: ${[...amountCounts.entries()].map(([a, i]) => `$${a}x${i.count}`).join(", ")}`);
      } else {
        // console.log(`[PostAI-AmountCorrect] ${loan.lender}: AI=$${aiAmount.toFixed(2)} (0x in text) → corrected=$${bestAmount.toFixed(2)} (${bestInfo}). All amounts: ${[...amountCounts.entries()].map(([a, i]) => `$${a}x${i.count}`).join(", ")}`);
        loan.amount = bestAmount;
        loan.payment = bestAmount;
      }
    }
  }
}

function removeDuplicateLenderEntries(loanDetails: any[], logTag: string): any[] {
  if (!loanDetails || loanDetails.length < 2) return loanDetails;
  const lenderGroups = new Map<string, any[]>();
  for (const loan of loanDetails) {
    const key = normalizeLenderKey(loan.lender || "");
    // console.log(`[${logTag}-KeyDebug] "${loan.lender}" → key="${key}" amt=$${loan.amount} occ=${loan.occurrences}`);
    if (!lenderGroups.has(key)) lenderGroups.set(key, []);
    lenderGroups.get(key)!.push(loan);
  }
  let result = [...loanDetails];
  for (const [key, group] of lenderGroups) {
    if (group.length < 2) continue;
    // console.log(`[${logTag}-Group] key="${key}" has ${group.length} entries: ${group.map((l: any) => `"${l.lender}" $${l.amount}(${l.occurrences}occ)`).join(", ")}`);
    group.sort((a: any, b: any) => (b.occurrences || 0) - (a.occurrences || 0));
    const primary = group[0];
    const primaryOcc = primary.occurrences || 0;
    for (let i = 1; i < group.length; i++) {
      const other = group[i];
      const otherOcc = other.occurrences || 0;
      const hasFunding = (other.fundedAmount && other.fundedAmount > 0) || (other.funded_amount && other.funded_amount > 0);
      if (hasFunding) { continue; }
      const amtRatio = Math.max(primary.amount || 0, other.amount || 0) / Math.min(primary.amount || 1, other.amount || 1);
      if (otherOcc <= 5 && primaryOcc >= 10 && amtRatio > 2) {
        // console.log(`[${logTag}] Removing duplicate "${other.lender}" $${other.amount} (${otherOcc} occ) — same lender as "$${primary.amount}" (${primaryOcc} occ), likely one-time payment not recurring position`);
        result = result.filter((l: any) => l !== other);
      } else {
        // console.log(`[${logTag}] Keeping "${other.lender}" $${other.amount} (${otherOcc} occ) — criteria not met: otherOcc<=5?${otherOcc <= 5} primaryOcc>=10?${primaryOcc >= 10} amtRatio>2?${amtRatio > 2} (ratio=${amtRatio.toFixed(2)})`);
      }
    }
  }
  return result;
}

function splitMultiPositionLenders(loanDetails: any[], rawText: string): any[] {
  if (!loanDetails || loanDetails.length === 0) return loanDetails;
  const debits = parseDebitsFromText(rawText);
  if (debits.length === 0) return loanDetails;

  const newLoans: any[] = [];

  for (const loan of loanDetails) {
    const lenderName = (loan.lender || "").toLowerCase();
    if (!lenderName || lenderName.length < 3) { newLoans.push(loan); continue; }

    const lenderKey = normalizeLenderKey(lenderName);
    const otherSameLender = loanDetails.filter(l => l !== loan && normalizeLenderKey((l.lender || "").toLowerCase()) === lenderKey);
    if (otherSameLender.length > 0) { newLoans.push(loan); continue; }

    const matchingDebits = debits.filter(d => {
      const descKey = normalizeLenderKey(d.description);
      return descKey.includes(lenderKey) || lenderKey.includes(descKey) ||
        (lenderKey.length >= 4 && d.description.toLowerCase().includes(lenderKey.substring(0, Math.min(lenderKey.length, 8))));
    });

    if (matchingDebits.length < 5) { newLoans.push(loan); continue; }

    const amountCounts = new Map<number, { count: number; descriptions: string[] }>();
    for (const d of matchingDebits) {
      const key = Math.round(d.amount * 100) / 100;
      const existing = amountCounts.get(key);
      if (!existing) {
        amountCounts.set(key, { count: 1, descriptions: [d.description] });
      } else {
        existing.count++;
        if (!existing.descriptions.includes(d.description)) existing.descriptions.push(d.description);
      }
    }

    const clusters: { amount: number; count: number; descriptions: string[] }[] = [];
    const sortedAmounts = [...amountCounts.entries()].sort((a, b) => b[1].count - a[1].count);

    for (const [amt, info] of sortedAmounts) {
      let merged = false;
      for (const cluster of clusters) {
        const ratio = Math.max(amt, cluster.amount) / Math.min(amt, cluster.amount);
        if (ratio <= 1.10) {
          if (info.count > cluster.count) cluster.amount = amt;
          cluster.count += info.count;
          for (const d of info.descriptions) { if (!cluster.descriptions.includes(d)) cluster.descriptions.push(d); }
          merged = true;
          break;
        }
      }
      if (!merged && info.count >= 2) {
        clusters.push({ amount: amt, count: info.count, descriptions: [...info.descriptions] });
      }
    }

    if (clusters.length < 2) { newLoans.push(loan); continue; }

    clusters.sort((a, b) => b.count - a.count);
    const primaryCluster = clusters[0];
    const secondaryCluster = clusters[1];

    const amtRatio = Math.max(primaryCluster.amount, secondaryCluster.amount) / Math.min(primaryCluster.amount, secondaryCluster.amount);
    if (amtRatio < 1.20) { newLoans.push(loan); continue; }

    const descHasWeekly = secondaryCluster.descriptions.some(d => /weekly/i.test(d));
    const descHasDaily = primaryCluster.descriptions.some(d => /daily/i.test(d));

    let minSecondaryOcc = descHasWeekly || descHasDaily ? 4 : 8;
    if (amtRatio > 3) minSecondaryOcc = Math.max(minSecondaryOcc, 12);
    if (secondaryCluster.count < minSecondaryOcc) {
      // console.log(`[SplitLender] "${loan.lender}": secondary cluster $${secondaryCluster.amount} has only ${secondaryCluster.count} occurrences (need ${minSecondaryOcc}+, ratio=${amtRatio.toFixed(1)}x) — likely one-time payment, skipping split`);
      newLoans.push(loan);
      continue;
    }
    if (secondaryCluster.count < primaryCluster.count * 0.15 && amtRatio > 2) {
      // console.log(`[SplitLender] "${loan.lender}": secondary cluster $${secondaryCluster.amount} (${secondaryCluster.count}x) is <15% of primary (${primaryCluster.count}x) with ${amtRatio.toFixed(1)}x amount diff — skipping split`);
      newLoans.push(loan);
      continue;
    }

    let primaryFreq = "daily";
    let secondaryFreq = "weekly";
    if (primaryCluster.count >= 15) primaryFreq = "daily";
    else if (primaryCluster.count >= 3) primaryFreq = "weekly";
    if (secondaryCluster.count >= 15) secondaryFreq = "daily";
    else if (secondaryCluster.count >= 3) secondaryFreq = "weekly";

    if (descHasWeekly) secondaryFreq = "weekly";
    if (descHasDaily) primaryFreq = "daily";

    if (primaryFreq === secondaryFreq && primaryCluster.count > secondaryCluster.count * 3) {
      secondaryFreq = primaryFreq === "daily" ? "weekly" : "daily";
    }

    // console.log(`[SplitLender] "${loan.lender}": detected 2 positions — $${primaryCluster.amount} (${primaryCluster.count}x, ${primaryFreq}) and $${secondaryCluster.amount} (${secondaryCluster.count}x, ${secondaryFreq})`);

    loan.amount = primaryCluster.amount;
    loan.payment = primaryCluster.amount;
    loan.frequency = primaryFreq;
    loan.occurrences = primaryCluster.count;
    newLoans.push(loan);

    const secondLoan = {
      lender: loan.lender,
      amount: secondaryCluster.amount,
      payment: secondaryCluster.amount,
      frequency: secondaryFreq,
      occurrences: secondaryCluster.count,
      account: loan.account || "",
      fundedAmount: null,
      fundedDate: null,
      _splitFromOriginal: true,
    };
    newLoans.push(secondLoan);
  }

  return newLoans;
}

async function visionReadSummaryTable(pdfBuffer: Buffer, fileName: string): Promise<{ totals: number; subAccounts: number[] } | null> {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "nfv-"));
  try {
    const pdfPath = path.join(tmpDir, "input.pdf");
    fs.writeFileSync(pdfPath, pdfBuffer);
    execSync(`pdftoppm -png -r 200 -f 1 -l 1 "${pdfPath}" "${tmpDir}/page"`, { timeout: 30000 });
    const pageFiles = fs.readdirSync(tmpDir).filter(f => f.startsWith("page") && f.endsWith(".png")).sort();
    if (pageFiles.length === 0) return null;
    const imgBuf = fs.readFileSync(path.join(tmpDir, pageFiles[0]));
    const base64 = imgBuf.toString("base64");
    await acquireAiSlot();
    let response;
    try {
      response = await anthropic.messages.create({
        model: "claude-sonnet-4-5",
        max_tokens: 500,
        messages: [{ role: "user", content: [
          { type: "image" as const, source: { type: "base64" as const, media_type: "image/png" as const, data: base64 } },
          { type: "text" as const, text: `Look at the "Summary of your deposit accounts" table in this bank statement image. Return ONLY a JSON object with these fields:
- "totals_deposits_credits": the dollar amount from the "Totals" row under the "Deposits/Credits" column (number, no $ sign)
- "sub_accounts": array of objects with "name" (account name) and "deposits_credits" (dollar amount under Deposits/Credits for that row)
Example: {"totals_deposits_credits": 32603.62, "sub_accounts": [{"name": "Business Checking", "deposits_credits": 29600.00}, {"name": "Membership Savings", "deposits_credits": 3003.62}]}
Return ONLY the JSON, nothing else.` }
        ] }],
      });
    } finally {
      releaseAiSlot();
    }
    const inputTok = (response.usage as any)?.input_tokens || 2000;
    const outputTok = (response.usage as any)?.output_tokens || 0;
    costTracker.record(null, "navy-fed-summary-vision", inputTok, outputTok);
    const text = ((response.content[0] as any)?.text || "").trim();
    // console.log(`[PostAI-NavyFed] Vision summary table raw: ${text}`);
    const jsonMatch = text.match(/\{[\s\S]*\}/);
    if (!jsonMatch) return null;
    const parsed = JSON.parse(jsonMatch[0]);
    const totals = parseFloat(parsed.totals_deposits_credits);
    if (!totals || isNaN(totals)) return null;
    const subAccounts = (parsed.sub_accounts || []).map((sa: any) => parseFloat(sa.deposits_credits)).filter((n: number) => !isNaN(n) && n > 0);
    // console.log(`[PostAI-NavyFed] Vision: Totals=$${totals}, SubAccounts=[${subAccounts.map((s: number) => `$${s}`).join(", ")}]`);
    return { totals, subAccounts };
  } catch (e: any) {
    // console.log(`[PostAI-NavyFed] Vision summary table failed: ${e.message}`);
    return null;
  } finally {
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}
  }
}

async function postAIValidation(analysis: AnalysisResult, rawText: string, parserDeposits?: Array<{ month: string; revenue: number; account: string }>): Promise<AnalysisResult> {
//   scrubLog("POSTAI", `===== postAIValidation START =====`);
//   scrubLog("POSTAI", `Input: ${(analysis.monthlyRevenues || []).length} revenues, ${(analysis.loanDetails || []).length} loans, rawText=${rawText.length} chars, parserDeposits=${parserDeposits?.length || 0}`);
  const validated = { ...analysis };
  const reviewFlags: AnalysisResult["depositReviewFlags"] = [];

  const sections = splitRawTextIntoSections(rawText);
//   scrubLog("POSTAI", `Split raw text into ${sections.length} section(s): ${sections.map(s => `"${s.name}" (${s.text.length} chars)`).join(", ")}`);
  const sectionSummaries: BankSummaryDeposit[] = [];

  const extractedAccounts: string[] = [];
  for (const section of sections) {
    const acct = extractAccountNumberFromText(section.text);
    if (acct) extractedAccounts.push(acct);
  }
  if (extractedAccounts.length === 0) {
    const acct = extractAccountNumberFromText(rawText);
    if (acct) extractedAccounts.push(acct);
  }

  const uniqueAccounts = [...new Set(extractedAccounts)];
  if (uniqueAccounts.length > 0) {
    const primaryAcct = uniqueAccounts[0];
    const aiAcct = (validated as any).accountNumber || "";
    const aiLast4 = aiAcct.replace(/[\s-]/g, "").slice(-4);
    const isMultiAccount = uniqueAccounts.length > 1;
    if (isMultiAccount && uniqueAccounts.includes(aiLast4)) {
//       scrubLog("PostAI-Validation", `Multi-account PDF: AI account "${aiAcct}" (${aiLast4}) found in extracted accounts [${uniqueAccounts.join(", ")}], keeping AI value`);
    } else if (aiLast4 !== primaryAcct) {
//       scrubLog("PostAI-Validation", `Account correction: AI="${aiAcct}" → Bank="${primaryAcct}"`);
      (validated as any).accountNumber = primaryAcct;
    }

    if (validated.monthlyRevenues) {
      for (const rev of validated.monthlyRevenues) {
        const revAcct = (rev.account || "").replace(/[\s-]/g, "").slice(-4);
        if (revAcct && revAcct !== primaryAcct && !isMultiAccount) {
//           scrubLog("PostAI-Validation", `Revenue account correction for ${rev.month}: "${rev.account}" → "${primaryAcct}"`);
          rev.account = primaryAcct;
        } else if (revAcct && isMultiAccount && !uniqueAccounts.includes(revAcct)) {
//           scrubLog("PostAI-Validation", `Revenue account "${rev.account}" not in extracted accounts [${uniqueAccounts.join(", ")}], correcting to "${primaryAcct}"`);
          rev.account = primaryAcct;
        }
      }
    }

    if (validated.loanDetails) {
      for (const loan of validated.loanDetails) {
        const loanAcct = (loan.account || "").replace(/[\s-]/g, "").slice(-4);
        if (loanAcct && loanAcct !== primaryAcct && !isMultiAccount) {
          loan.account = primaryAcct;
        } else if (loanAcct && isMultiAccount && !uniqueAccounts.includes(loanAcct)) {
          loan.account = primaryAcct;
        }
      }
    }
  }

  interface BankDeposit { total: number; monthKey: string; fullMonth: string; label: string; acct: string; fromLabel: boolean }
  const bankDeposits: BankDeposit[] = [];

  if (parserDeposits && parserDeposits.length > 0) {
    for (const pd of parserDeposits) {
      const parts = pd.month.split("-");
      const yr = parts[0];
      const mo = parseInt(parts[1] || "0");
      const shortYr = yr.length === 4 ? yr.slice(2) : yr;
      const monthKey = mo > 0 ? `${mo}-${shortYr}` : "";
      const acct = (pd.account || "").replace(/[\s-]/g, "").slice(-4);
//       scrubLog("BankDeposit", `Using parser deposit: $${pd.revenue.toFixed(2)}, month=${pd.month}, acct=${acct}`);
      bankDeposits.push({ total: pd.revenue, monthKey, fullMonth: pd.month, label: "parser", acct, fromLabel: true });
    }
  }

  if (bankDeposits.length === 0) for (const section of sections) {
    const monthKey = extractMonthFromSection(section.text);
    const labelTotal = extractDepositSummaryFromSection(section.text);
    const creditSum = sumCreditTransactionsFromSection(section.text);
    const acct = extractAccountNumberFromText(section.text) || extractedAccounts[0] || "";

    let depositTotal = labelTotal > 0 ? labelTotal : creditSum;
    const fromLabel = labelTotal > 0;

    if (depositTotal <= 0) {
      const balCalc = balanceEquationFallback(section.text);
      if (balCalc > 0) {
//         scrubLog("BankDeposit", `"${section.name}": balance equation fallback = $${balCalc.toFixed(2)}`);
        depositTotal = balCalc;
      }
    }
    if (depositTotal > 0) {
      const balCalc = balanceEquationFallback(section.text);
      if (balCalc > 0) {
        const beginBal = parseBalanceFromSection(section.text, "begin");
        const endBal = parseBalanceFromSection(section.text, "end");
        if (!isNaN(beginBal) && !isNaN(endBal)) {
          const balChange = endBal - beginBal;
          if (depositTotal < balChange * 0.5 && balCalc > depositTotal) {
//             scrubLog("BankDeposit", `"${section.name}": balance validation override regex=$${depositTotal.toFixed(2)} → calc=$${balCalc.toFixed(2)}`);
            depositTotal = balCalc;
          }
        }
      }
    }

    let fullMonth = "";
    if (monthKey) {
      const parts = monthKey.split("-");
      const mo = parseInt(parts[0]);
      const yr = parts[1];
      const fullYr = yr.length === 2 ? `20${yr}` : yr;
      fullMonth = `${fullYr}-${String(mo).padStart(2, "0")}`;
    }

    if (depositTotal > 0) {
      const source = labelTotal > 0 ? "bank summary" : "credit transaction sum";
//       scrubLog("BankDeposit", `"${section.name}": $${depositTotal.toFixed(2)} from ${source}, month=${fullMonth || "?"}, acct=${acct || "?"}`);
      bankDeposits.push({ total: depositTotal, monthKey, fullMonth, label: section.name, acct, fromLabel });
    } else {
//       scrubLog("BankDeposit", `"${section.name}": NO deposit found (labelTotal=$${labelTotal.toFixed(2)}, creditSum=$${creditSum.toFixed(2)}, month=${monthKey || "?"}, acct=${acct || "?"}, textLen=${section.text.length})`);
    }
  }

  if (validated.monthlyRevenues && validated.monthlyRevenues.length > 0 && bankDeposits.length > 0) {
    const usedBank = new Set<number>();

    for (const rev of validated.monthlyRevenues) {
      const aiMonthKey = normalizeAIMonth(rev.month || "");
      const revAcct = (rev.account || "").replace(/[\s-]/g, "").slice(-4);
      let matchIdx = -1;

      if (aiMonthKey) {
        if (revAcct) {
          for (let i = 0; i < bankDeposits.length; i++) {
            if (usedBank.has(i)) continue;
            const bankAcct = (bankDeposits[i].acct || "").replace(/[\s-]/g, "").slice(-4);
            if (bankDeposits[i].monthKey === aiMonthKey && bankAcct && bankAcct === revAcct) { matchIdx = i; break; }
          }
        }
        if (matchIdx === -1) {
          for (let i = 0; i < bankDeposits.length; i++) {
            if (usedBank.has(i)) continue;
            if (bankDeposits[i].monthKey === aiMonthKey) {
              const bankAcct = (bankDeposits[i].acct || "").replace(/[\s-]/g, "").slice(-4);
              if (bankAcct && revAcct && bankAcct !== revAcct) {
//                 scrubLog("BankDeposit", `Skipping month match ${aiMonthKey}: rev acct=${revAcct} ≠ bank acct=${bankAcct}`);
                continue;
              }
              matchIdx = i;
              break;
            }
          }
        }
      }

      if (matchIdx === -1 && bankDeposits.length === 1 && validated.monthlyRevenues.length === 1 && !usedBank.has(0)) {
        matchIdx = 0;
      }

      if (matchIdx >= 0) {
        const bank = bankDeposits[matchIdx];
        const aiAmt = rev.revenue ?? rev.rev ?? 0;
        if (aiAmt > 0 && bank.total < aiAmt * 0.5) {
//           scrubLog("BankDeposit", `PARSER LOW ${rev.month}: AI=$${aiAmt.toFixed(2)} >> Bank=$${bank.total.toFixed(2)} (parser likely from secondary account in multi-account PDF, using AI)`);
          usedBank.add(matchIdx);
        } else if (aiAmt > 0 && bank.total > aiAmt * 1.3) {
          if (bank.fromLabel) {
//             scrubLog("BankDeposit", `OVERRIDE ${rev.month}: AI=$${aiAmt.toFixed(2)} → Bank labeled deposit=$${bank.total.toFixed(2)} (bank summary label takes precedence)`);
            if (rev.revenue !== undefined) rev.revenue = bank.total;
            else rev.rev = bank.total;
          } else {
//             scrubLog("BankDeposit", `PARSER HIGH ${rev.month}: AI=$${aiAmt.toFixed(2)} << Bank credit sum=$${bank.total.toFixed(2)} (summed credits may include non-deposit items, flagging for review)`);
            rev.needsReview = true;
            rev.reviewReason = `AI deposit ($${aiAmt.toLocaleString()}) vs bank credits ($${bank.total.toLocaleString()})`;
            rev.aiAmount = aiAmt;
            rev.parserAmount = bank.total;
          }
          usedBank.add(matchIdx);
        } else {
          if (Math.abs(bank.total - aiAmt) / Math.max(bank.total, 1) > 0.02) {
//             scrubLog("BankDeposit", `OVERRIDE ${rev.month}: AI=$${aiAmt.toFixed(2)} → Bank=$${bank.total.toFixed(2)}`);
          }
          if (rev.revenue !== undefined) rev.revenue = bank.total;
          else rev.rev = bank.total;
          if (bank.fullMonth && rev.month !== bank.fullMonth) {
//             scrubLog("BankDeposit", `MONTH FIX ${rev.month} → ${bank.fullMonth}`);
            rev.month = bank.fullMonth;
          }
          usedBank.add(matchIdx);
        }
      }
    }

    const unmatchedBank = bankDeposits.filter((_, i) => !usedBank.has(i));
    const unmatchedRevs = validated.monthlyRevenues.filter((r: any) => {
      const mk = normalizeAIMonth(r.month || "");
      return !bankDeposits.some((b, i) => usedBank.has(i) && b.monthKey === mk);
    });

    if (unmatchedBank.length > 0 && unmatchedRevs.length > 0) {
      const usedSafetyBank = new Set<number>();
      const usedSafetyRev = new Set<number>();

      for (let ri = 0; ri < unmatchedRevs.length; ri++) {
        const rev = unmatchedRevs[ri];
        const revAcct = (rev.account || "").replace(/[\s-]/g, "").slice(-4);
        const revMk = normalizeAIMonth(rev.month || "");
        const aiAmt = rev.revenue ?? rev.rev ?? 0;
        let bestIdx = -1;
        let bestScore = -1;

        for (let bi = 0; bi < unmatchedBank.length; bi++) {
          if (usedSafetyBank.has(bi)) continue;
          const bank = unmatchedBank[bi];
          const bankAcct = (bank.acct || "").replace(/[\s-]/g, "").slice(-4);

          if (bankAcct && revAcct && bankAcct !== revAcct) continue;

          let score = 0;
          if (bank.monthKey && revMk && bank.monthKey === revMk) score += 10;
          if (bankAcct && revAcct && bankAcct === revAcct) score += 5;
          if (aiAmt > 0 && bank.total > 0) {
            const ratio = Math.min(aiAmt, bank.total) / Math.max(aiAmt, bank.total);
            score += ratio * 3;
          }
          if (score > bestScore) { bestScore = score; bestIdx = bi; }
        }

        if (bestIdx >= 0) {
          const bank = unmatchedBank[bestIdx];
          if (aiAmt > 0 && bank.total < aiAmt * 0.5) {
//             scrubLog("BankDeposit", `SAFETY PARSER LOW ${rev.month}: AI=$${aiAmt.toFixed(2)} >> Bank=$${bank.total.toFixed(2)} (parser likely from secondary account, using AI)`);
          } else if (aiAmt > 0 && bank.total > aiAmt * 1.3) {
            if (bank.fromLabel) {
//               scrubLog("BankDeposit", `SAFETY OVERRIDE ${rev.month}: AI=$${aiAmt.toFixed(2)} → Bank labeled deposit=$${bank.total.toFixed(2)} (bank summary label takes precedence)`);
              if (rev.revenue !== undefined) rev.revenue = bank.total;
              else rev.rev = bank.total;
            } else {
//               scrubLog("BankDeposit", `SAFETY PARSER HIGH ${rev.month}: AI=$${aiAmt.toFixed(2)} << Bank credit sum=$${bank.total.toFixed(2)} (flagging for review)`);
              rev.needsReview = true;
              rev.reviewReason = `AI deposit ($${aiAmt.toLocaleString()}) vs bank credits ($${bank.total.toLocaleString()})`;
              rev.aiAmount = aiAmt;
              rev.parserAmount = bank.total;
            }
          } else {
//             scrubLog("BankDeposit", `SAFETY OVERRIDE ${rev.month}: AI=$${aiAmt.toFixed(2)} → Bank=$${bank.total.toFixed(2)} (acct=${bank.acct || "?"})`);
            if (rev.revenue !== undefined) rev.revenue = bank.total;
            else rev.rev = bank.total;
            if (bank.fullMonth) {
//               scrubLog("BankDeposit", `SAFETY MONTH FIX ${rev.month} → ${bank.fullMonth}`);
              rev.month = bank.fullMonth;
            }
          }
          usedSafetyBank.add(bestIdx);
          usedSafetyRev.add(ri);
        }
      }
    }

    for (const bank of unmatchedBank) {
      if (!bank.fullMonth) continue;
      const alreadyHas = validated.monthlyRevenues.some((r: any) => {
        const mk = normalizeAIMonth(r.month || "");
        return mk === bank.monthKey;
      });
      if (!alreadyHas) {
//         scrubLog("BankDeposit", `ADDING MISSING MONTH: ${bank.fullMonth} = $${bank.total.toFixed(2)}`);
        validated.monthlyRevenues.push({
          month: bank.fullMonth,
          revenue: bank.total,
          account: bank.acct || extractedAccounts[0] || "",
        });
      }
    }
  } else if (bankDeposits.length > 0 && (!validated.monthlyRevenues || validated.monthlyRevenues.length === 0)) {
//     scrubLog("BankDeposit", `AI returned NO monthly revenues — building from bank data`);
    validated.monthlyRevenues = bankDeposits
      .filter(b => b.fullMonth)
      .map(b => ({
        month: b.fullMonth,
        revenue: b.total,
        account: b.acct || extractedAccounts[0] || "",
      }));
  }

  if (validated.loanDetails && validated.loanDetails.length > 0) {
    validated.loanDetails = validated.loanDetails.filter((loan: any) => {
      const lenderName = (loan.lender || "").toLowerCase().trim();
      let freq = (loan.frequency || "").toLowerCase();
      const occ = loan.occurrences || 0;
      const reasoning = (loan.reasoning || "");
      const reasoningLc = reasoning.toLowerCase();

      const hasFundedAmountEarly = (loan.fundedAmount && loan.fundedAmount > 0) || (loan.funded_amount && loan.funded_amount > 0);
      if ((!loan.amount || loan.amount <= 0) && !hasFundedAmountEarly) {
//         scrubLog("PostAI-Validation", `Removing "${loan.lender}": zero or negative amount, no funded amount`);
        return false;
      }

      if (lenderName.length < 3 || /^(on|to|at|in|of|for|the|a|an|and|or|authorized\s+on|payment\s+sent|payment|debit|credit|transfer|check|misc|other|fee):?\s*$/i.test(lenderName)) {
//         scrubLog("PostAI-Validation", `Removing "${loan.lender}": lender name too generic/short`);
        return false;
      }

      const paymentServicePatterns = /\b(zelle|venmo|cash\s*app|cashapp|apple\s*pay|google\s*pay|samsung\s*pay|paypal(?!\s*working)|wire\s*transfer|money\s*transfer|payment\s*sent|recurring\s*payment\s*authorized|authorized\s*on)\b/i;
      if (paymentServicePatterns.test(lenderName)) {
//         scrubLog("PostAI-Validation", `Removing ${loan.lender}: payment service, NOT a lender`);
        return false;
      }

      const lenderKeywords = /\b(capital|advance|funding|finance|lending|financial|finserv|loan|mca)\b/i;
      const vendorPatterns = /\b(booster\s*fuels?|gasoline|diesel|sams?\s*club|costco)\b/i;
      if (vendorPatterns.test(lenderName) && !lenderKeywords.test(lenderName)) {
//         scrubLog("PostAI-Validation", `Removing ${loan.lender}: vendor/fuel company, NOT a lender`);
        return false;
      }

      const bankInternalPattern = /\b(capital\s+one\s+online\s+pmt|capital\s+one\s+card|cap\s+one\s+card|capital\s+pmt|capital\s+payment|capital\s+one\s+pmt|chase\s+pmt|wells\s+fargo\s+pmt|bank\s+pmt)\b/i;
      if (bankInternalPattern.test(lenderName)) {
//         scrubLog("PostAI-Validation", `Removing ${loan.lender}: bank internal payment/credit card, NOT a loan`);
        return false;
      }

      if (occ > 0 && occ < 3) {
//         scrubLog("PostAI-Validation", `Removing "${loan.lender}": only ${occ} occurrence(s) — need 3+ in a single month to qualify as recurring loan (could be one-time payoff/settlement)`);
        return false;
      }

      if (/\b(withdrawal|withdrawl)\b/i.test(lenderName) && (() => {
        const wKey = normalizeLenderKey(lenderName);
        if (KNOWN_LENDER_SHORTNAMES.has(wKey)) return false;
        for (const known of KNOWN_LENDER_SHORTNAMES) { if (wKey.includes(known)) return false; }
        return true;
      })()) {
//         scrubLog("PostAI-Validation", `Removing ${loan.lender}: generic withdrawal label, NOT a lender`);
        return false;
      }

      const confidence = (loan.confidence || "").toLowerCase();
      const isKnownLender = { test: (name: string) => {
        const key = normalizeLenderKey(name);
        if (KNOWN_LENDER_SHORTNAMES.has(key)) return true;
        for (const known of KNOWN_LENDER_SHORTNAMES) {
          if (key.startsWith(known) || key.includes(known)) return true;
        }
        return false;
      }};
      if (confidence === "low" && !isKnownLender.test(lenderName) && occ < 5) {
//         scrubLog("PostAI-Validation", `Removing ${loan.lender}: low confidence with few occurrences — not reliable enough`);
        return false;
      }

      const equipmentPatterns = /\b(equipment\s*financ|truck\s*financ|truck\s*leas|equipment\s*leas|commercial\s*vehicle|fleet\s*financ|m&t\s*equipment|sumitomo|paccar|volvo\s*financial|daimler|peterbilt|kenworth|freightliner|navistar|caterpillar\s*financial|john\s*deere|kubota|komatsu|bobcat)\b/i;
      if (equipmentPatterns.test(lenderName) || equipmentPatterns.test(reasoning)) {
//         scrubLog("PostAI-Validation", `Removing ${loan.lender}: equipment/truck financing is NOT a loan`);
        return false;
      }

      const squareMerchantPattern = /\b(square\s+inc|square\s+processing|sq\s*\*|square\s+payroll|square\s+payment|square\s+deposit)\b/i;
      const squareLoanPattern = /\b(sq\s*advance|square\s*capital|sq\s*capital|sq\s*loan)\b/i;
      if ((squareMerchantPattern.test(lenderName) || squareMerchantPattern.test(reasoning)) && !squareLoanPattern.test(lenderName) && !squareLoanPattern.test(reasoning)) {
//         scrubLog("PostAI-Validation", `Removing ${loan.lender}: Square merchant processing/payments, NOT a Square Capital loan`);
        return false;
      }
      if (/\bsquare\b/i.test(lenderName) && !squareLoanPattern.test(lenderName) && !squareLoanPattern.test(reasoning)) {
//         scrubLog("PostAI-Validation", `Removing ${loan.lender}: Generic "Square" reference without "Advance/Capital/Loan" — likely merchant processing, NOT a loan`);
        return false;
      }

      if (freq === "bi-weekly" || freq === "biweekly" || freq === "bi weekly" || freq === "every two weeks" || freq === "every 2 weeks" || freq === "semi-weekly") {
//         scrubLog("PostAI-Validation", `Normalizing "${loan.lender}" frequency "${freq}" → "weekly" (bi-weekly treated as weekly for MCA)`);
        loan.frequency = "weekly";
        freq = "weekly";
      }
      if (freq !== "daily" && freq !== "weekly") {
        if (isKnownLender.test(lenderName) && (freq === "monthly" || freq === "bi-monthly")) {
//           scrubLog("PostAI-Validation", `WARNING: Known lender "${loan.lender}" has "${freq}" frequency — unusual for MCA. Keeping with low confidence.`);
          loan.confidence = "low";
        } else {
//           scrubLog("PostAI-Validation", `Removing ${loan.lender}: frequency="${freq}" is not daily/weekly`);
          return false;
        }
      }

      if (freq === "weekly" && !isKnownLender.test(lenderName)) {
        const amountMatches = reasoning.match(/\$[\d,]+(?:\.\d{2})?/g);
        if (amountMatches && amountMatches.length >= 3) {
          const amounts = amountMatches.map((a: string) => parseFloat(a.replace(/[$,]/g, "")));
          const nonZero = amounts.filter((a: number) => a > 0 && a < 1000000);
          if (nonZero.length >= 3) {
            const mode = (() => {
              const freq: Record<number, number> = {};
              for (const a of nonZero) {
                const key = Math.round(a * 100) / 100;
                freq[key] = (freq[key] || 0) + 1;
              }
              let best = 0, bestCount = 0;
              for (const [k, v] of Object.entries(freq)) {
                if (v > bestCount) { best = parseFloat(k); bestCount = v; }
              }
              return { amount: best, count: bestCount };
            })();
            const consistencyRatio = mode.count / nonZero.length;
            if (consistencyRatio < 0.6) {
//               scrubLog("PostAI-Validation", `Removing ${loan.lender}: weekly amounts are NOT consistent — mode $${mode.amount} appears ${mode.count}/${nonZero.length} times (${(consistencyRatio * 100).toFixed(0)}%). Amounts: ${nonZero.slice(0, 10).map(a => `$${a}`).join(", ")}`);
              return false;
            }
          }
        }
      }

      const uniqueMonths = new Set((validated.monthlyRevenues || []).map((r: any) => normalizeAIMonth(r.month || "")));
      const numMonths = Math.max(uniqueMonths.size, 1);
      const perMonth = occ / numMonths;

      const hasAmountChange = reasoningLc.includes("increas") || reasoningLc.includes("renew") || reasoningLc.includes("changed") || reasoningLc.includes("adjusted");

      const recentlyFunded = hasFundedAmountEarly && loan.fundedDate;

      if (freq === "daily" && occ > 0 && perMonth < 10 && !hasAmountChange) {
        if (recentlyFunded && perMonth >= 5) {
//           scrubLog("PostAI-Validation", `Keeping ${loan.lender}: recently funded ($${loan.fundedAmount || loan.funded_amount} on ${loan.fundedDate}) with ${perMonth.toFixed(1)}/month — new position ramping up`);
        } else {
//           scrubLog("PostAI-Validation", `Removing ${loan.lender}: labeled "daily" but only ${perMonth.toFixed(1)}/month (${occ} over ${numMonths} months) — need 10+/month for daily`);
          return false;
        }
      }

      if (freq === "weekly" && occ > 0 && perMonth < 2.5) {
        if (recentlyFunded && perMonth >= 2) {
//           scrubLog("PostAI-Validation", `Keeping ${loan.lender}: recently funded ($${loan.fundedAmount || loan.funded_amount} on ${loan.fundedDate}) with ${perMonth.toFixed(1)}/month — new position`);
        } else {
//           scrubLog("PostAI-Validation", `Removing ${loan.lender}: labeled "weekly" but only ${perMonth.toFixed(1)}/month (${occ} over ${numMonths} months) — need 3+/month for weekly`);
          return false;
        }
      }

      let foundInText = false;
      let programmaticOccCount = 0;
      if (rawText && lenderName.length >= 3) {
        const rawLower = rawText.toLowerCase().replace(/[^a-z0-9\s]/g, " ");
        const lenderWords = lenderName.split(/\s+/).filter((w: string) => w.length >= 3 && !/^(the|and|inc|llc|corp|co|ltd|daily|weekly|ach|debit|credit)$/i.test(w));
        const distinctiveWords = lenderWords.filter((w: string) => !/^(capital|advance|funding|finance|lending|financial|payment|group|services?)$/i.test(w));
        const searchWords = distinctiveWords.length > 0 ? distinctiveWords : lenderWords.slice(0, 1);
        const foundExact = searchWords.some((w: string) => rawLower.includes(w));
        const foundPartial = !foundExact && searchWords.some((w: string) => {
          if (w.length < 4) return false;
          const prefix = w.substring(0, Math.min(w.length, 6));
          return rawLower.includes(prefix);
        });
        const foundNormalized = !foundExact && !foundPartial && (() => {
          const normalizedLender = normalizeLenderKey(lenderName);
          if (normalizedLender.length < 4) return false;
          const normalizedRaw = rawLower.replace(/\s+/g, "");
          if (normalizedRaw.includes(normalizedLender) || normalizedRaw.includes(normalizedLender.substring(0, Math.min(normalizedLender.length, 8)))) return true;
          const ocrSubs: Record<string, string[]> = { "o": ["0"], "0": ["o"], "l": ["1","i"], "1": ["l","i"], "i": ["l","1"], "s": ["5"], "5": ["s"], "b": ["6","8"], "g": ["9"], "9": ["g"] };
          for (const w of (searchWords as string[])) {
            if (w.length < 4) continue;
            for (let ci = 0; ci < w.length; ci++) {
              const subs = ocrSubs[w[ci]];
              if (!subs) continue;
              for (const sub of subs) {
                const variant = w.substring(0, ci) + sub + w.substring(ci + 1);
                if (rawLower.includes(variant)) return true;
              }
            }
          }
          return false;
        })();
        foundInText = foundExact || foundPartial || foundNormalized;

        if (foundInText) {
          const bestSearchWord = (searchWords as string[]).find((w: string) => rawLower.includes(w)) || searchWords[0];
          if (bestSearchWord) {
            let idx = -1;
            while ((idx = rawLower.indexOf(bestSearchWord, idx + 1)) !== -1) {
              programmaticOccCount++;
            }
          }
//           scrubLog("PostAI-Validation", `Text scan: "${loan.lender}" found "${bestSearchWord}" ${programmaticOccCount}x in raw text (AI reported ${occ} occ). Text mentions are NOT payment counts — using AI occ for filtering.`);
        }
      }

      if (occ > 0 && occ < 3) {
//         scrubLog("PostAI-Validation", `Removing "${loan.lender}": AI reported only ${occ} occurrence(s) — need 3+ payments in a single month to confirm recurring position`);
        return false;
      }

      if (rawText && lenderName.length >= 3) {
        if (!foundInText && !isKnownLender.test(lenderName) && occ <= 5) {
//           scrubLog("PostAI-Validation", `Removing ${loan.lender}: lender name NOT found in raw statement text — possible AI hallucination`);
          return false;
        } else if (!foundInText && !isKnownLender.test(lenderName) && occ > 5) {
//           scrubLog("PostAI-Validation", `WARNING: ${loan.lender} NOT found in raw text but has ${occ} occurrences. Keeping with low confidence.`);
          loan.confidence = "low";
        } else if (!foundInText && isKnownLender.test(lenderName)) {
//           scrubLog("PostAI-Validation", `WARNING: Known lender ${loan.lender} NOT found in raw text. Keeping but flagging.`);
          loan.confidence = "low";
        }
      }

      return true;
    });

    validated.loanDetails = removeDuplicateLenderEntries(validated.loanDetails, "PostAI-Validation");

    validated.hasLoans = validated.loanDetails.length > 0;
    validated.hasOnDeck = validated.loanDetails.some((l: any) =>
      /ondeck|on\s*deck/i.test(l.lender || "")
    );

  }

  if (validated.monthlyRevenues) {
    const MONTH_NAME_TO_NUM: Record<string, number> = { jan:1,feb:2,mar:3,apr:4,may:5,jun:6,jul:7,aug:8,sep:9,oct:10,nov:11,dec:12,january:1,february:2,march:3,april:4,june:6,july:7,august:8,september:9,october:10,november:11,december:12 };
    for (const rev of validated.monthlyRevenues) {
      const m = (rev.month || "").trim();
      const strictYMD = m.match(/^(\d{4})-(\d{1,2})$/);
      if (strictYMD) {
        const paddedMonth = `${strictYMD[1]}-${String(parseInt(strictYMD[2])).padStart(2, "0")}`;
        if (paddedMonth !== m) {
//           scrubLog("PostAI-Validation", `Month padding fix: "${m}" → "${paddedMonth}"`);
          rev.month = paddedMonth;
        }
        continue;
      }
      const parts = m.toLowerCase().split(/[\s\/\-]+/);
      const moNum = MONTH_NAME_TO_NUM[parts[0]?.replace(/[^a-z]/g, "")] || parseInt(parts[0]) || 0;
      let yr = 0;
      for (let pi = 1; pi < parts.length; pi++) {
        const yc = parseInt(parts[pi]);
        if (yc >= 2020 && yc <= 2030) { yr = yc; break; }
        if (yc >= 20 && yc <= 30) { yr = 2000 + yc; break; }
      }
      if (moNum > 0 && yr > 0) {
        const fixed = `${yr}-${String(moNum).padStart(2, "0")}`;
//         scrubLog("PostAI-Validation", `Month format fix: "${m}" → "${fixed}"`);
        rev.month = fixed;
      } else if (moNum > 0 && yr === 0) {
        // console.warn(`[PostAI-Validation] WARNING: Month "${m}" missing year — AI should always return YYYY-MM format`);
      }
    }

    const MONTH_ABBR_MAP: Record<string, number> = { jan:1,feb:2,mar:3,apr:4,may:5,jun:6,jul:7,aug:8,sep:9,oct:10,nov:11,dec:12 };
    const parserMonths = new Map<number, number>();
    const actThroughAll = [...rawText.matchAll(/activity\s+through\s+(\w{3,9})\s+\d{1,2}\s*,?\s*(\d{2,4})/gi)];
    for (const m of actThroughAll) {
      const mo = MONTH_ABBR_MAP[m[1].toLowerCase().slice(0, 3)] || 0;
      const yr = m[2].length === 4 ? parseInt(m[2]) : 2000 + parseInt(m[2]);
      if (mo > 0 && yr >= 2020 && yr <= 2030) parserMonths.set(mo, yr);
    }
    const stmtPeriodAll = [...rawText.matchAll(/(?:statement\s+period|period\s+ending|closing\s+date)[:\s]+[^\n]*?(\d{1,2})\/(\d{1,2})\/(\d{2,4})/gi)];
    for (const m of stmtPeriodAll) {
      const mo = parseInt(m[1]);
      const yr = m[3].length === 4 ? parseInt(m[3]) : 2000 + parseInt(m[3]);
      if (mo >= 1 && mo <= 12 && yr >= 2020 && yr <= 2030) parserMonths.set(mo, yr);
    }
    const namedDateAll = [...rawText.matchAll(/(?:through|thru|ending|period)[:\s]+(\w{3,9})\s+\d{1,2}\s*,?\s*(\d{2,4})/gi)];
    for (const m of namedDateAll) {
      const mo = MONTH_ABBR_MAP[m[1].toLowerCase().slice(0, 3)] || 0;
      const yr = m[2].length === 4 ? parseInt(m[2]) : 2000 + parseInt(m[2]);
      if (mo > 0 && yr >= 2020 && yr <= 2030) parserMonths.set(mo, yr);
    }
//     scrubLog("POSTAI-YEAR", `Year scanner found ${parserMonths.size} month-year pair(s) from rawText: ${[...parserMonths.entries()].map(([m, y]) => `month${m}→${y}`).join(", ")}`);
    if (parserMonths.size > 0) {
      for (const rev of validated.monthlyRevenues) {
        const ymd = (rev.month || "").match(/^(\d{4})-(\d{1,2})$/);
        if (!ymd) continue;
        const aiMo = parseInt(ymd[2]);
        const aiYr = parseInt(ymd[1]);
        const parserYr = parserMonths.get(aiMo);
        if (parserYr && parserYr !== aiYr && Math.abs(parserYr - aiYr) === 1) {
          const fixed = `${parserYr}-${String(aiMo).padStart(2, "0")}`;
//           scrubLog("POSTAI-YEAR", `Year correction: AI="${rev.month}" (yr=${aiYr}) but statement says month${aiMo}=year${parserYr} → "${fixed}"`);
          rev.month = fixed;
        } else if (parserYr && parserYr === aiYr) {
//           scrubLog("POSTAI-YEAR", `AI year correct for ${rev.month}: matches statement (month${aiMo}=year${parserYr})`);
        }
      }
    }
  }

  if (validated.monthlyRevenues && validated.monthlyRevenues.length > 1) {
    const revAmounts = validated.monthlyRevenues.map((r: any) => r.revenue ?? r.rev ?? 0).filter((v: number) => v > 0);
    if (revAmounts.length > 1) {
      const sortedAmounts = [...revAmounts].sort((a, b) => a - b);
      const median = sortedAmounts[Math.floor(sortedAmounts.length / 2)];
      if (median > 0) {
        for (const rev of validated.monthlyRevenues) {
          const amt = rev.revenue ?? rev.rev ?? 0;
          const ratio = amt / median;
          if (ratio > 10 && amt > 500000) {
            // console.log(`[Outlier] Month ${rev.month}: $${amt.toFixed(2)} is ${ratio.toFixed(1)}x median ($${median.toFixed(2)}), capping to median — extreme outlier`);
            if (rev.revenue !== undefined) rev.revenue = median;
            else rev.rev = median;
          } else if (ratio > 5 && amt > 100000) {
            // console.log(`[Outlier] Month ${rev.month}: $${amt.toFixed(2)} is ${ratio.toFixed(1)}x median ($${median.toFixed(2)}). Flagging for review instead of capping.`);
            rev.needsReview = true;
            rev.reviewReason = `Unusually high (${ratio.toFixed(1)}x median) — may be seasonal or data error`;
            rev.aiAmount = amt;
            rev.parserAmount = median;
          }
        }
      }
    }
    const totalFromMonthly = validated.monthlyRevenues.reduce((sum: number, r: any) => sum + (r.revenue ?? r.rev ?? 0), 0);
    const avg = totalFromMonthly / validated.monthlyRevenues.length;
    if (avg > 0 && validated.grossRevenue) {
      if (Math.abs(validated.grossRevenue - avg) / avg > 0.2) {
        validated.grossRevenue = Math.round(avg);
      }
    }
  }

  try {
    const missedLoans = await detectMissedRecurringDebits(rawText, validated);
    if (missedLoans.length > 0) {
      if (!validated.loanDetails) validated.loanDetails = [];
      validated.loanDetails.push(...missedLoans);
      validated.hasLoans = true;
//       scrubLog("PostAI-Validation", `Added ${missedLoans.length} missed loan(s) from programmatic scan`);
    }
  } catch (e: any) {
    // console.error(`[PostAI-Validation] Recurring debit scan error:`, e.message);
  }

  try {
    detectFundedAmountsFromCredits(rawText, validated);
  } catch (e: any) {
    // console.error(`[PostAI-Validation] Funded amount scan error:`, e.message);
  }

  try {
    const engineResult = extractAllTransactions(rawText);
    integrateTransactionEngineResults(engineResult, validated, rawText);
  } catch (e: any) {
    // console.error(`[PostAI-Validation] Transaction engine error:`, e.message);
  }

  try {
    if (rawText && validated.monthlyRevenues && Array.isArray(validated.monthlyRevenues)) {
      let detectedBank = identifyBank(rawText);
      if (!detectedBank?.useDetailedDepositTotal && validated.bankName) {
        const aiBankTemplate = findBankByName(validated.bankName);
        if (aiBankTemplate?.useDetailedDepositTotal) {
          // console.log(`[PostAI-NavyFed] Text detection returned "${detectedBank?.name ?? 'none'}" but AI says "${validated.bankName}". Using AI bank template.`);
          detectedBank = aiBankTemplate;
        }
      }

      if (detectedBank?.useDetailedDepositTotal) {
        const summaryTablePattern = /(?:Summary\s+of\s+(?:your\s+)?deposit\s+accounts|Deposits?\s*\/\s*Credits?)/i;
        const hasSummaryTable = summaryTablePattern.test(rawText);
        // console.log(`[PostAI-NavyFed] Bank: ${detectedBank.name}, useDetailedDepositTotal=true, hasSummaryTable=${hasSummaryTable}`);

        if (hasSummaryTable) {
          const lines = rawText.split("\n");

          const summaryTotals: number[] = [];
          const summarySubAccountAmounts: number[] = [];
          let inSummaryTable = false;
          let pendingLabel: "totals" | "subaccount" | null = null;
          let pendingAmounts: number[] = [];

          const flushPending = () => {
            if (pendingLabel && pendingAmounts.length >= 2) {
              const depCredits = pendingAmounts[1];
              if (pendingLabel === "totals") {
                summaryTotals.push(depCredits);
                // console.log(`[PostAI-NavyFed] Summary "Totals" row (accumulated): Deposits/Credits=$${depCredits} (all: ${pendingAmounts.map(a => `$${a}`).join(", ")})`);
              } else {
                summarySubAccountAmounts.push(depCredits);
                // console.log(`[PostAI-NavyFed] Sub-account row (accumulated): Deposits/Credits=$${depCredits} (all: ${pendingAmounts.map(a => `$${a}`).join(", ")})`);
              }
            }
            pendingLabel = null;
            pendingAmounts = [];
          };

          for (let i = 0; i < lines.length; i++) {
            const line = lines[i].trim();
            if (!line) continue;

            if (/Summary\s+of\s+(?:your\s+)?deposit\s+accounts/i.test(line)) {
              flushPending();
              inSummaryTable = true;
              // console.log(`[PostAI-NavyFed] Found summary table start at line ${i}: "${line}"`);
              continue;
            }

            if (inSummaryTable) {
              if (/^(DEPOSITS?\s+AND|CHECKS?\s+PAID|ELECTRONIC|OTHER\s+DEBITS|DAILY\s+BALANCE|========|REMITTANCE\s+RECEIVED)/i.test(line)) {
                flushPending();
                // console.log(`[PostAI-NavyFed] Summary table ended at line ${i}: "${line.slice(0, 60)}"`);
                inSummaryTable = false;
                continue;
              }

              const amounts = [...line.matchAll(/\$?([\d,]+\.\d{2})/g)].map(m => parseFloat(m[1].replace(/,/g, "")));
              const isSubAccountLabel = /\b(checking|savings|money\s*market|certificate|share|business|membership)\b/i.test(line);
              const isTotalsLabel = /^\s*Totals?\b/i.test(line);

              if (isTotalsLabel || isSubAccountLabel) {
                flushPending();
                pendingLabel = isTotalsLabel ? "totals" : "subaccount";
                pendingAmounts = [...amounts];
                if (amounts.length >= 2) {
                  flushPending();
                } else {
                  // console.log(`[PostAI-NavyFed] ${isTotalsLabel ? "Totals" : "Sub-account"} label at line ${i} (${amounts.length} amts so far). Line: "${line.slice(0, 80)}"`);
                }
              } else if (pendingLabel && amounts.length > 0) {
                pendingAmounts.push(...amounts);
                if (pendingAmounts.length >= 2) {
                  flushPending();
                }
              } else if (!pendingLabel && amounts.length >= 2) {
                const prevLine = (i > 0 ? lines[i - 1].trim() : "");
                if (/\b(checking|savings|money\s*market|certificate|share|business|membership)\b/i.test(prevLine)) {
                  summarySubAccountAmounts.push(amounts[1]);
                  // console.log(`[PostAI-NavyFed] Sub-account row (amounts after acct#): Deposits/Credits=$${amounts[1]} ("${line.slice(0, 60)}")`);
                } else if (/^\s*Totals?\b/i.test(prevLine)) {
                  summaryTotals.push(amounts[1]);
                  // console.log(`[PostAI-NavyFed] Summary "Totals" (amounts after label): Deposits/Credits=$${amounts[1]}`);
                }
              } else if (amounts.length === 0 && !pendingLabel) {
                if (!/^\s*\d{5,}/.test(line) && !/previous|balance|ending|withdrawal|deposit|credit/i.test(line)) {
                }
              }
            }
          }
          flushPending();

          if (summaryTotals.length === 0) {
            // console.log(`[PostAI-NavyFed] Line-by-line parsing found 0 totals. Trying regex fallback...`);
            const totalsRegex = /Totals?\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})/gi;
            let totalsMatch;
            while ((totalsMatch = totalsRegex.exec(rawText)) !== null) {
              const depositsCredits = parseFloat(totalsMatch[2].replace(/,/g, ""));
              if (depositsCredits > 0) {
                summaryTotals.push(depositsCredits);
                // console.log(`[PostAI-NavyFed] Regex fallback found Totals Deposits/Credits=$${depositsCredits}`);
              }
            }
            if (summaryTotals.length === 0) {
              const singleAmtTotals = /Totals?\s+\$?([\d,]+\.\d{2})/gi;
              let singleMatch;
              while ((singleMatch = singleAmtTotals.exec(rawText)) !== null) {
                const amt = parseFloat(singleMatch[1].replace(/,/g, ""));
                if (amt > 0) {
                  summaryTotals.push(amt);
                  // console.log(`[PostAI-NavyFed] Regex fallback (single) found Totals=$${amt}`);
                }
              }
            }
          }

          const monthNames: Record<string, number> = { january: 1, february: 2, march: 3, april: 4, may: 5, june: 6, july: 7, august: 8, september: 9, october: 10, november: 11, december: 12, jan: 1, feb: 2, mar: 3, apr: 4, jun: 6, jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12 };
          let visionTotalsUsed = false;
          const visionByMonth: { monthNum: number; total: number; fileName: string }[] = [];
          if (summaryTotals.length === 0) {
            // console.log(`[PostAI-NavyFed] All text parsing failed. Attempting vision read of summary table...`);
            const stmtHeaderRegex = /STATEMENT\s+\d+:\s+"([^"]+)"/g;
            let hdrMatch;
            const navyFedFiles: string[] = [];
            while ((hdrMatch = stmtHeaderRegex.exec(rawText)) !== null) {
              const fn = hdrMatch[1];
              if (/navy\s*federal|nfcu/i.test(fn)) navyFedFiles.push(fn);
            }
            let allDocsCache: typeof documentsTable.$inferSelect[] | null = null;
            for (const fn of navyFedFiles) {
              let docResult: { buffer: Buffer; fileName: string } | null = getBufferFromDoc(`uploads/${fn}`);
              if (!docResult) {
                try {
                  if (!allDocsCache) allDocsCache = await db.select().from(documentsTable);
                  const matchDoc = allDocsCache.find(d => d.name === fn || (d.url && d.url.includes(fn)));
                  if (matchDoc) {
                    docResult = await getBufferFromDocAsync(matchDoc.url, matchDoc.storageKey);
                  }
                } catch {}
              }
              if (!docResult) continue;
              const visionResult = await visionReadSummaryTable(docResult.buffer, fn);
              if (visionResult && visionResult.totals > 0) {
                summaryTotals.push(visionResult.totals);
                for (const sa of visionResult.subAccounts) {
                  summarySubAccountAmounts.push(sa);
                }
                const fnLower = fn.toLowerCase();
                let fileMonth = 0;
                for (const [name, num] of Object.entries(monthNames)) {
                  if (fnLower.includes(name)) { fileMonth = num; break; }
                }
                visionByMonth.push({ monthNum: fileMonth, total: visionResult.totals, fileName: fn });
                // console.log(`[PostAI-NavyFed] Vision found Totals=$${visionResult.totals} month=${fileMonth} from ${fn}`);
              }
            }
            if (visionByMonth.length > 0) visionTotalsUsed = true;
          }

          // console.log(`[PostAI-NavyFed] Summary parsing complete: ${summaryTotals.length} totals, ${summarySubAccountAmounts.length} sub-accounts. Totals: [${summaryTotals.map(t => `$${t}`).join(", ")}], Sub-accounts: [${summarySubAccountAmounts.map(s => `$${s}`).join(", ")}], visionUsed=${visionTotalsUsed}`);

          if (summaryTotals.length > 0) {
            if (visionTotalsUsed && visionByMonth.length > 0) {
              const aiYears = validated.monthlyRevenues.map(mr => {
                const m = (mr.month || "").match(/(\d{4})/);
                return m ? parseInt(m[1]) : new Date().getFullYear();
              });
              const baseYear = Math.max(...aiYears, new Date().getFullYear() - 1);
              const visionMonthMap = new Map<string, number>();
              for (const vm of visionByMonth) {
                if (vm.monthNum === 0) continue;
                let year = baseYear;
                const otherMonths = visionByMonth.filter(v => v !== vm && v.monthNum > 0).map(v => v.monthNum);
                if (otherMonths.length > 0) {
                  const maxOther = Math.max(...otherMonths);
                  if (vm.monthNum < maxOther && maxOther >= 10 && vm.monthNum <= 3) {
                    year = baseYear + 1;
                  }
                }
                const key = `${year}-${String(vm.monthNum).padStart(2, "0")}`;
                visionMonthMap.set(key, vm.total);
                // console.log(`[PostAI-NavyFed] Vision month mapping: ${key} → $${vm.total}`);
              }
              let anyMatched = false;
              for (const mr of validated.monthlyRevenues) {
                const monthStr = mr.month || "";
                const mMatch = monthStr.match(/(\w+)\s*(\d{4})/);
                if (!mMatch) continue;
                const aiMonthName = mMatch[1].toLowerCase();
                const aiYear = parseInt(mMatch[2]);
                const aiMonthNum = monthNames[aiMonthName] || 0;
                if (aiMonthNum === 0) continue;
                const aiKey = `${aiYear}-${String(aiMonthNum).padStart(2, "0")}`;
                if (visionMonthMap.has(aiKey)) {
                  const oldRev = mr.revenue ?? mr.rev ?? 0;
                  const visionTotal = visionMonthMap.get(aiKey)!;
                  // console.log(`[PostAI-NavyFed] Vision match by month: ${aiKey} revenue $${oldRev} → $${visionTotal}`);
                  if (mr.revenue !== undefined) mr.revenue = visionTotal;
                  else mr.rev = visionTotal;
                  visionMonthMap.delete(aiKey);
                  anyMatched = true;
                }
              }
              if (!anyMatched || visionMonthMap.size > 0) {
                // console.log(`[PostAI-NavyFed] Month matching incomplete (matched=${anyMatched}, remaining=${visionMonthMap.size}). Rebuilding months from vision.`);
                const visionEntries = visionByMonth.filter(vm => vm.monthNum > 0).sort((a, b) => {
                  let ya = baseYear, yb = baseYear;
                  const allNums = visionByMonth.map(v => v.monthNum).filter(n => n > 0);
                  const maxM = Math.max(...allNums);
                  if (a.monthNum < maxM && maxM >= 10 && a.monthNum <= 3) ya = baseYear + 1;
                  if (b.monthNum < maxM && maxM >= 10 && b.monthNum <= 3) yb = baseYear + 1;
                  return (ya * 100 + a.monthNum) - (yb * 100 + b.monthNum);
                });
                const monthLabels = ["", "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
                const revenues = validated.monthlyRevenues.filter(mr => (mr.revenue ?? mr.rev ?? 0) > 0 || mr.month);
                revenues.sort((a, b) => {
                  const ka = (a.month || "").replace(/\D+/g, "");
                  const kb = (b.month || "").replace(/\D+/g, "");
                  return ka.localeCompare(kb);
                });
                for (let i = 0; i < Math.min(visionEntries.length, validated.monthlyRevenues.length); i++) {
                  const ve = visionEntries[i];
                  const mr = validated.monthlyRevenues[i];
                  const allNums = visionByMonth.map(v => v.monthNum).filter(n => n > 0);
                  const maxM = Math.max(...allNums);
                  let veYear = baseYear;
                  if (ve.monthNum < maxM && maxM >= 10 && ve.monthNum <= 3) veYear = baseYear + 1;
                  const oldRev = mr.revenue ?? mr.rev ?? 0;
                  const oldMonth = mr.month || "";
                  const newMonth = `${monthLabels[ve.monthNum]} ${veYear}`;
                  // console.log(`[PostAI-NavyFed] Vision rebuild: "${oldMonth}" $${oldRev} → "${newMonth}" $${ve.total}`);
                  mr.month = newMonth;
                  if (mr.revenue !== undefined) mr.revenue = ve.total;
                  else mr.rev = ve.total;
                }
              }
            } else {
              for (const mr of validated.monthlyRevenues) {
                const rev = mr.revenue ?? mr.rev ?? 0;
                if (rev <= 0) continue;

                let corrected = false;
                for (const totalDep of summaryTotals) {
                  if (Math.abs(rev - totalDep) / Math.max(rev, totalDep) < 0.02) {
                    // console.log(`[PostAI-NavyFed] Revenue $${rev} already matches summary total $${totalDep} ✓`);
                    corrected = true;
                    break;
                  }

                  const isSubAccount = summarySubAccountAmounts.some(sa => Math.abs(sa - rev) / Math.max(sa, rev) < 0.02);
                  if (isSubAccount && totalDep > rev) {
                    // console.log(`[PostAI-NavyFed] Revenue $${rev} matches sub-account. Correcting to summary total $${totalDep}`);
                    if (mr.revenue !== undefined) mr.revenue = totalDep;
                    else mr.rev = totalDep;
                    corrected = true;
                    break;
                  }

                  if (!isSubAccount && summarySubAccountAmounts.length === 0 && totalDep > rev && rev > totalDep * 0.5) {
                    // console.log(`[PostAI-NavyFed] Revenue $${rev} < summary total $${totalDep} (no sub-accounts detected). Correcting to total.`);
                    if (mr.revenue !== undefined) mr.revenue = totalDep;
                    else mr.rev = totalDep;
                    corrected = true;
                    break;
                  }
                }
                if (!corrected) {
                  // console.log(`[PostAI-NavyFed] Revenue $${rev} — no correction applied. Totals: [${summaryTotals.join(",")}], SubAccts: [${summarySubAccountAmounts.join(",")}]`);
                }
              }
            }
          }

          const depositTxns: number[] = [];
          let inDepositSection = false;
          const depositSectionHeader = detectedBank.sectionHeaders?.deposits;

          for (let i = 0; i < lines.length; i++) {
            const line = lines[i].trim();
            if (!line) continue;

            if (depositSectionHeader && depositSectionHeader.test(line)) {
              inDepositSection = true;
              continue;
            }
            if (inDepositSection && detectedBank.sectionHeaders?.withdrawals?.test(line)) {
              inDepositSection = false;
              continue;
            }
            if (inDepositSection && /^(checks?\s+paid|electronic\s+(payments?|withdrawals?)|other\s+debits?|daily\s+balance)/i.test(line)) {
              inDepositSection = false;
              continue;
            }

            if (inDepositSection) {
              const totalDepMatch = line.match(/Total\s+Deposits?\s*:?\s*\$?([\d,]+\.\d{2})/i);
              if (totalDepMatch) {
                const totalDep = parseFloat(totalDepMatch[1].replace(/,/g, ""));
                if (totalDep > 0) {
                  // console.log(`[PostAI-NavyFed] Found "Total Deposits" in detailed section: $${totalDep}`);
                  for (const mr of validated.monthlyRevenues) {
                    const rev = mr.revenue ?? mr.rev ?? 0;
                    if (rev > totalDep * 2) {
                      // console.log(`[PostAI-NavyFed] Correcting revenue $${rev} → $${totalDep} (was using inflated Deposits/Credits from summary table)`);
                      if (mr.revenue !== undefined) mr.revenue = totalDep;
                      else mr.rev = totalDep;
                    }
                  }
                }
                inDepositSection = false;
                continue;
              }

              const dateMatch = line.match(/^(\d{1,2}\/\d{1,2})/);
              if (dateMatch) {
                const amts = [...line.matchAll(/\$?([\d,]+\.\d{2})/g)].map(m => parseFloat(m[1].replace(/,/g, "")));
                const depositAmts = amts.filter(a => a > 0 && a < 50000000);
                if (depositAmts.length > 0) {
                  const desc = line.toLowerCase();
                  const isTransfer = /\b(transfer|xfer|trfr|tfr)\b/i.test(desc);
                  const isLoanFunding = /\b(ondeck|on\s*deck|kabbage|bluevine|fundbox|forward|credibly|kapitus|mantis|everest|mulligan|itria|pearl)\b/i.test(desc);
                  if (!isTransfer && !isLoanFunding) {
                    depositTxns.push(...depositAmts);
                  }
                }
              }
            }
          }

          if (depositTxns.length > 0) {
            const summedDeposits = Math.round(depositTxns.reduce((s, v) => s + v, 0) * 100) / 100;
            for (const mr of validated.monthlyRevenues) {
              const rev = mr.revenue ?? mr.rev ?? 0;
              if (rev > 0 && summedDeposits > 0 && rev > summedDeposits * 3) {
                // console.log(`[PostAI-NavyFed] Revenue $${rev} likely from summary Deposits/Credits. Actual deposit transactions sum to $${summedDeposits}. Correcting.`);
                if (mr.revenue !== undefined) mr.revenue = summedDeposits;
                else mr.rev = summedDeposits;
              }
            }
          }
        }
      }

      const templateDepositPatterns = detectedBank?.summaryPatterns?.totalDeposits;
      const hasMultipleDepositCategories = templateDepositPatterns && templateDepositPatterns.length >= 2;

      const depositCategoryPatterns = hasMultipleDepositCategories
        ? templateDepositPatterns
        : [
            /(?:\d+\s+)?Other\s+Credits?\s+(?:for:?\s*)?\$?([\d,]+\.\d{2})/i,
            /(?:\d+\s+)?ATM\s*\/?\s*DEBIT\s+Deposits?:?\s*\$?([\d,]+\.\d{2})/i,
            /(?:\d+\s+)?(?:Direct|Electronic|Wire|ACH|Mobile|Customer)\s+(?:Deposits?|Credits?)\s*(?:for:?\s*)?\$?([\d,]+\.\d{2})/i,
          ];

      interface DepHit { amount: number; lineIdx: number; label: string; patIdx: number }
      const lines = rawText.split("\n");
      const allHits: DepHit[] = [];
      for (let i = 0; i < lines.length; i++) {
        for (let p = 0; p < depositCategoryPatterns.length; p++) {
          const pat = new RegExp(depositCategoryPatterns[p].source, depositCategoryPatterns[p].flags.replace("g", "") || "i");
          const m = lines[i].match(pat);
          if (m) {
            const groups = Array.from(m).slice(1).filter(g => g != null);
            const amtStr = groups[groups.length - 1];
            const amt = parseFloat(amtStr.replace(/,/g, ""));
            if (amt > 0 && amt < 50000000) {
              allHits.push({ amount: amt, lineIdx: i, label: m[0].trim(), patIdx: p });
            }
          }
        }
      }

      if (allHits.length >= 2) {
        const hitsByPattern = new Map<number, DepHit[]>();
        for (const h of allHits) {
          if (!hitsByPattern.has(h.patIdx)) hitsByPattern.set(h.patIdx, []);
          hitsByPattern.get(h.patIdx)!.push(h);
        }

        const distinctPatternsMatched = hitsByPattern.size;
        if (distinctPatternsMatched >= 2) {
          let depositCorrected = false;

          for (const mr of validated.monthlyRevenues) {
            const rev = mr.revenue ?? mr.rev ?? 0;
            if (rev <= 0) continue;

            const matchingHit = allHits.find(h => Math.abs(rev - h.amount) / h.amount < 0.02);

            if (matchingHit) {
              const otherCategoryHits = allHits.filter(h => h.patIdx !== matchingHit.patIdx);
              if (otherCategoryHits.length === 0) continue;

              const bestOtherByPattern = new Map<number, DepHit>();
              for (const oh of otherCategoryHits) {
                const existing = bestOtherByPattern.get(oh.patIdx);
                if (!existing || Math.abs(oh.lineIdx - matchingHit.lineIdx) < Math.abs(existing.lineIdx - matchingHit.lineIdx)) {
                  bestOtherByPattern.set(oh.patIdx, oh);
                }
              }

              let additionalDeposits = 0;
              const addedLabels: string[] = [];
              for (const [, oh] of bestOtherByPattern) {
                additionalDeposits += oh.amount;
                addedLabels.push(`${oh.label}=$${oh.amount}`);
              }

              if (additionalDeposits > 0) {
                const corrected = Math.round((rev + additionalDeposits) * 100) / 100;
                // console.log(`[PostAI-DepositSum] Revenue $${rev} matched "${matchingHit.label}" but found additional deposit categories: ${addedLabels.join(" + ")}. Total: $${corrected}${hasMultipleDepositCategories ? ` (template: ${detectedBank!.name})` : ""}`);
                if (mr.revenue !== undefined) mr.revenue = corrected;
                else mr.rev = corrected;
                depositCorrected = true;
              }
            } else if (hasMultipleDepositCategories) {
              const groupedByCluster: DepHit[][] = [];
              const sortedHits = [...allHits].sort((a, b) => a.lineIdx - b.lineIdx);
              let currentCluster: DepHit[] = [sortedHits[0]];
              for (let i = 1; i < sortedHits.length; i++) {
                if (sortedHits[i].lineIdx - sortedHits[i - 1].lineIdx <= 10) {
                  currentCluster.push(sortedHits[i]);
                } else {
                  if (currentCluster.length >= 2) groupedByCluster.push(currentCluster);
                  currentCluster = [sortedHits[i]];
                }
              }
              if (currentCluster.length >= 2) groupedByCluster.push(currentCluster);

              interface ClusterResult { total: number; labels: string[]; patternCount: number }
              let bestCluster: ClusterResult | null = null;
              let bestClusterDiff = Infinity;

              for (const cluster of groupedByCluster) {
                const clusterPatterns = new Set(cluster.map(h => h.patIdx));
                if (clusterPatterns.size < 2) continue;

                const bestPerPattern = new Map<number, DepHit>();
                for (const h of cluster) {
                  const existing = bestPerPattern.get(h.patIdx);
                  if (!existing || h.amount > existing.amount) {
                    bestPerPattern.set(h.patIdx, h);
                  }
                }

                let computedTotal = 0;
                const labels: string[] = [];
                for (const [, h] of bestPerPattern) {
                  computedTotal += h.amount;
                  labels.push(`${h.label}=$${h.amount.toLocaleString()}`);
                }
                computedTotal = Math.round(computedTotal * 100) / 100;

                const diff = Math.abs(rev - computedTotal) / Math.max(rev, computedTotal);
                if (diff < bestClusterDiff) {
                  bestClusterDiff = diff;
                  bestCluster = { total: computedTotal, labels, patternCount: clusterPatterns.size };
                }
              }

              if (bestCluster && bestClusterDiff > 0.05 && bestCluster.patternCount >= 2) {
                // console.log(`[PostAI-DepositSum] AI revenue $${rev.toLocaleString()} doesn't match any category. Best bank summary cluster: ${bestCluster.labels.join(" + ")} = $${bestCluster.total.toLocaleString()} (template: ${detectedBank!.name}). Overriding.`);
                if (mr.revenue !== undefined) mr.revenue = bestCluster.total;
                else mr.rev = bestCluster.total;
                depositCorrected = true;
              }
            }
          }

          if (depositCorrected && validated.grossRevenue !== undefined) {
            const newGross = validated.monthlyRevenues.reduce((s: number, mr: any) => s + (mr.revenue ?? mr.rev ?? 0), 0);
            if (newGross > validated.grossRevenue) {
              // console.log(`[PostAI-DepositSum] Recomputing grossRevenue: $${validated.grossRevenue} → $${newGross}`);
              validated.grossRevenue = newGross;
            }
          }
        }
      }
    }
  } catch (e: any) {
    // console.error(`[PostAI-DepositSum] Split deposit sum error:`, e.message);
  }

  try {
    if (validated.loanDetails && Array.isArray(validated.loanDetails) && rawText) {
      for (const loan of validated.loanDetails) {
        const lenderName = (loan.lender || "").trim();
        if (!lenderName || lenderName.length < 3) continue;
        const amount = loan.amount;
        if (!amount || amount <= 0) continue;

        const lenderTokens = lenderName.toLowerCase().replace(/[^a-z0-9\s]/g, "").split(/\s+/).filter((t: string) => t.length >= 3);
        if (lenderTokens.length === 0) continue;

        const lines = rawText.split("\n");
        const amountsNearLender: number[] = [];
        for (let i = 0; i < lines.length; i++) {
          const lineLower = lines[i].toLowerCase();
          const hasLender = lenderTokens.some((t: string) => lineLower.includes(t));
          if (!hasLender) continue;
          const windowLines = lines.slice(Math.max(0, i), Math.min(lines.length, i + 2)).join(" ");
          const amtMatches = windowLines.match(/[\d,]+\.\d{2}/g);
          if (amtMatches) {
            for (const amtStr of amtMatches) {
              const val = parseFloat(amtStr.replace(/,/g, ""));
              if (val >= 10 && val < 100000) amountsNearLender.push(val);
            }
          }
        }

        if (amountsNearLender.length > 0) {
          const amountCounts = new Map<number, number>();
          for (const a of amountsNearLender) {
            const rounded = Math.round(a * 100) / 100;
            amountCounts.set(rounded, (amountCounts.get(rounded) || 0) + 1);
          }

          let mostCommon = 0, mostCommonCount = 0;
          for (const [val, cnt] of amountCounts) {
            if (cnt > mostCommonCount) { mostCommon = val; mostCommonCount = cnt; }
          }

          const aiAmountAppearsForLender = amountsNearLender.some(a => Math.abs(a - amount) < 0.02);
          if (!aiAmountAppearsForLender && mostCommonCount >= 2) {
            // console.log(`[PostAI-LoanCheck] ${lenderName}: AI said $${amount} but it never appears near lender in text. Most common amount near lender: $${mostCommon} (${mostCommonCount}x). Correcting.`);
            loan.amount = mostCommon;
            loan._correctedFrom = amount;
          } else if (!aiAmountAppearsForLender && mostCommonCount === 1 && amountsNearLender.length <= 3) {
            const closest = amountsNearLender.reduce((best, a) => {
              const diff = Math.abs(a - amount);
              const bestDiff = Math.abs(best - amount);
              return diff < bestDiff ? a : best;
            }, amountsNearLender[0]);
            if (Math.abs(closest - amount) / Math.max(closest, amount) > 0.2) {
              // console.log(`[PostAI-LoanCheck] ${lenderName}: AI said $${amount} but closest in text is $${closest}. Correcting.`);
              loan.amount = closest;
              loan._correctedFrom = amount;
            }
          } else if (!aiAmountAppearsForLender) {
            const allAmountsInDoc = [...rawText.matchAll(/[\d,]+\.\d{2}/g)].map(m => parseFloat(m[0].replace(/,/g, "")));
            const amountExistsAnywhere = allAmountsInDoc.some(a => Math.abs(a - amount) < 0.02);
            if (!amountExistsAnywhere) {
              // console.log(`[PostAI-LoanCheck] ${lenderName}: AI said $${amount} but this amount does NOT exist ANYWHERE in statement. Hallucinated amount — using most common near lender: $${mostCommon} (${mostCommonCount}x).`);
              loan.amount = mostCommon;
              loan._correctedFrom = amount;
              loan._hallucinated = true;
            }
          }
        } else {
          const allAmountsInDoc = [...rawText.matchAll(/[\d,]+\.\d{2}/g)].map(m => parseFloat(m[0].replace(/,/g, "")));
          const amountExistsAnywhere = allAmountsInDoc.some(a => Math.abs(a - amount) < 0.02);
          if (!amountExistsAnywhere) {
            // console.log(`[PostAI-LoanCheck] ${lenderName}: AI said $${amount} but no amounts found near lender AND amount doesn't exist in statement. Marking as hallucinated.`);
            loan._hallucinated = true;
          }
        }
      }

      const lenderGroups = new Map<string, typeof validated.loanDetails>();
      for (const loan of validated.loanDetails!) {
        const key = (loan.lender || "").toLowerCase().replace(/[^a-z]/g, "");
        if (!lenderGroups.has(key)) lenderGroups.set(key, []);
        lenderGroups.get(key)!.push(loan);
      }
      for (const [key, group] of lenderGroups) {
        if (group.length <= 1) continue;
        const hallucinated = group.filter((l: any) => l._hallucinated);
        if (hallucinated.length > 0 && hallucinated.length < group.length) {
          for (const h of hallucinated) {
            // console.log(`[PostAI-LoanCheck] Removing hallucinated duplicate for "${h.lender}" ($${h.amount}) — another verified entry exists.`);
            h._remove = true;
          }
        }
      }
      validated.loanDetails = validated.loanDetails!.filter((l: any) => !l._remove);
    }
  } catch (e: any) {
    // console.error(`[PostAI-LoanCheck] Loan amount verification error:`, e.message);
  }

  const ORIGINATING_BANKS = /\b(celtic\s*bank|webbank|web\s*bank|cross\s*river)\b/i;

  const CREDIT_INDICATORS = /\b(deposit|credit|incoming|transfer\s*in|ach\s*credit|wire\s*in|direct\s*dep|funds?\s*received)\b/i;
  const DEBIT_INDICATORS = /\b(debit|payment|withdrawal|ach\s*debit|autopay|pymt|pmt|w\/d)\b/i;
  const GENERIC_TOKENS = new Set(["capital", "funding", "fund", "advance", "financial", "group", "solutions", "business", "merchant", "national", "global", "direct", "first", "american", "united"]);

  try {
    if (validated.loanDetails && Array.isArray(validated.loanDetails) && rawText) {
      for (const loan of validated.loanDetails) {
        const lenderName = (loan.lender || "").trim();
        if (!lenderName) continue;

        if (ORIGINATING_BANKS.test(lenderName)) continue;

        if (loan.fundedAmount && loan.fundedAmount > 0) {
          const lenderTokens = lenderName.toLowerCase().replace(/[^a-z0-9\s]/g, "").split(/\s+/).filter((t: string) => t.length >= 3);
          if (lenderTokens.length === 0) continue;

          const distinctiveTokens = lenderTokens.filter((t: string) => !GENERIC_TOKENS.has(t));
          const tokensToUse = distinctiveTokens.length > 0 ? distinctiveTokens : lenderTokens;
          const requiredMatches = tokensToUse.length >= 2 ? 2 : 1;

          const lines = rawText.split("\n");
          let depositVerified = false;
          for (let i = 0; i < lines.length; i++) {
            const line = lines[i];
            const amtMatch = line.match(/[\d,]+\.\d{2}/g);
            if (!amtMatch) continue;
            const lineAmounts = amtMatch.map((m: string) => parseFloat(m.replace(/,/g, "")));
            const hasMatchingAmount = lineAmounts.some((a: number) => Math.abs(a - loan.fundedAmount) < 0.02);
            if (!hasMatchingAmount) continue;

            const windowText = lines.slice(Math.max(0, i - 1), Math.min(lines.length, i + 3)).join(" ").toLowerCase();

            if (DEBIT_INDICATORS.test(windowText) && !CREDIT_INDICATORS.test(windowText)) continue;

            const matchingTokenCount = tokensToUse.filter((t: string) => windowText.includes(t)).length;
            if (matchingTokenCount >= requiredMatches) {
              const originatingBankOnly = ORIGINATING_BANKS.test(windowText) && !tokensToUse.some((t: string) => {
                const cleaned = windowText.replace(ORIGINATING_BANKS, "");
                return cleaned.includes(t);
              });
              if (!originatingBankOnly) {
                depositVerified = true;
                break;
              }
            }
          }

          if (!depositVerified) {
//             scrubLog("PostAI-FundedCheck", `${lenderName}: fundedAmount $${loan.fundedAmount} could not be verified — deposit description does not explicitly name this lender. Clearing funded amount.`);
            loan.fundedAmount = null;
            loan.funded_amount = null;
            loan.fundedDate = null;
            loan.funded_date = null;
          }
        }
      }
    }
  } catch (e: any) {
    // console.error(`[PostAI-FundedCheck] Funded amount verification error:`, e.message);
  }

  let detectedBankForDeposit: ReturnType<typeof identifyBank> = null;
  try {
    detectedBankForDeposit = identifyBank(rawText);
//     scrubLog("POSTAI-BANK", `identifyBank from rawText → ${detectedBankForDeposit ? `"${detectedBankForDeposit.name}" (has ${detectedBankForDeposit.summaryPatterns?.totalDeposits?.length || 0} deposit patterns)` : "null (no match)"}`);
    if (!detectedBankForDeposit && validated.bankName) {
      detectedBankForDeposit = findBankByName(validated.bankName);
      if (detectedBankForDeposit) {
//         scrubLog("POSTAI-BANK", `Fallback: AI bank name "${validated.bankName}" → template "${detectedBankForDeposit.name}"`);
      } else {
//         scrubLog("POSTAI-BANK", `Fallback: AI bank name "${validated.bankName}" → no template found`);
      }
    }
    if (detectedBankForDeposit) {
      const aiBankName = (validated.bankName || "").trim();
      const genericWords = new Set(["bank", "first", "national", "community", "federal", "credit", "union", "savings", "the", "of", "and"]);
      const aiTokens = aiBankName.toLowerCase().split(/\s+/).filter((t: string) => !genericWords.has(t) && t.length > 1);
      const detTokens = detectedBankForDeposit.name.toLowerCase().split(/\s+/).filter((t: string) => !genericWords.has(t) && t.length > 1);
      const aiMatchesDetected = aiTokens.length > 0 && detTokens.length > 0 && aiTokens.some((t: string) => detTokens.includes(t));
      const aiHasSpecificName = aiTokens.length > 0 && !/^(unknown|local\s*bank|the\s*bank)$/i.test(aiBankName);
      if (!aiBankName || /unknown|local\s*bank/i.test(aiBankName)) {
//         scrubLog("PostAI-Validation", `Bank name override: "${aiBankName}" → "${detectedBankForDeposit.name}" (template match from statement text)`);
        validated.bankName = detectedBankForDeposit.name;
      } else if (!aiMatchesDetected && aiHasSpecificName) {
        const aiTemplate = findBankByName(aiBankName);
        if (aiTemplate) {
//           scrubLog("PostAI-Validation", `Bank name conflict: AI="${aiBankName}" vs template="${detectedBankForDeposit.name}". Using AI's bank template "${aiTemplate.name}".`);
          detectedBankForDeposit = aiTemplate;
        } else {
//           scrubLog("PostAI-Validation", `Bank name conflict: AI="${aiBankName}" vs template="${detectedBankForDeposit.name}". No template for AI bank, setting null.`);
          detectedBankForDeposit = null;
        }
      } else if (!aiMatchesDetected) {
//         scrubLog("PostAI-Validation", `Bank name override: "${aiBankName}" → "${detectedBankForDeposit.name}" (template match from statement text)`);
        validated.bankName = detectedBankForDeposit.name;
      }
      if (validated.monthlyRevenues) {
        for (const mr of validated.monthlyRevenues) {
          if (!mr.bankName || /unknown|local\s*bank/i.test(mr.bankName)) {
            mr.bankName = validated.bankName || (detectedBankForDeposit?.name ?? aiBankName);
          }
        }
      }
    }
  } catch (e: any) {
    // console.error(`[PostAI-Validation] Bank name detection error:`, e.message);
  }

  try {
    if (detectedBankForDeposit && validated.monthlyRevenues && validated.monthlyRevenues.length > 0 && rawText) {
      const templateDepositPats = detectedBankForDeposit.summaryPatterns?.totalDeposits;
      if (templateDepositPats && templateDepositPats.length > 0) {
        const lines = rawText.split("\n");

        interface DepSummary { amount: number; lineIdx: number }
        const bankDeposits: DepSummary[] = [];
        for (let i = 0; i < lines.length; i++) {
          for (const pat of templateDepositPats) {
            const m = lines[i].match(pat);
            if (m) {
              const groups = m.filter((_, idx) => idx > 0).map(g => g ? parseFloat(g.replace(/,/g, "")) : NaN).filter(v => !isNaN(v));
              const amt = groups.length > 1 ? groups[groups.length - 1] : groups[0];
              if (amt > 0 && amt < 50000000) {
                bankDeposits.push({ amount: amt, lineIdx: i });
              }
            }
          }
        }

        if (bankDeposits.length > 0) {
          const deduped: DepSummary[] = [];
          const sorted = [...bankDeposits].sort((a, b) => a.lineIdx - b.lineIdx);
          for (const bd of sorted) {
            const nearCluster = deduped.find(d => Math.abs(d.lineIdx - bd.lineIdx) <= 15);
            if (nearCluster) {
              if (bd.amount > nearCluster.amount) {
                nearCluster.amount = bd.amount;
                nearCluster.lineIdx = bd.lineIdx;
              }
            } else {
              deduped.push({ ...bd });
            }
          }
          const bankDepsToUse = deduped;
//           scrubLog("PostAI-DepositSummary", `${detectedBankForDeposit!.name}: Found ${bankDeposits.length} raw hits, ${bankDepsToUse.length} after dedup: ${bankDepsToUse.map(bd => `$${bd.amount.toLocaleString()}`).join(", ")}`);

          const sortedBankDeps = [...bankDepsToUse].sort((a, b) => b.amount - a.amount);
          const sortedRevs = [...validated.monthlyRevenues]
            .map((mr: any, idx: number) => ({ mr, idx, rev: mr.revenue ?? mr.rev ?? 0 }))
            .filter(e => e.rev > 0)
            .sort((a, b) => b.rev - a.rev);

          const usedBankIdx = new Set<number>();
          for (const entry of sortedRevs) {
            const aiRev = entry.rev;

            let bestIdx = -1;
            let bestDiff = Infinity;
            for (let j = 0; j < sortedBankDeps.length; j++) {
              if (usedBankIdx.has(j)) continue;
              const diff = Math.abs(sortedBankDeps[j].amount - aiRev);
              if (diff < bestDiff) { bestDiff = diff; bestIdx = j; }
            }

            if (bestIdx === -1) continue;
            const bankAmt = sortedBankDeps[bestIdx].amount;
            const ratio = Math.max(bankAmt, aiRev) / Math.min(bankAmt, aiRev);
            if (ratio < 1.05) {
              usedBankIdx.add(bestIdx);
              continue;
            }

            let assignIdx = bestIdx;
            if (sortedBankDeps.length > 1 && sortedRevs.length > 1) {
              const revRank = sortedRevs.indexOf(entry);
              for (let j = 0; j < sortedBankDeps.length; j++) {
                if (usedBankIdx.has(j)) continue;
                const jRank = j;
                if (Math.abs(jRank - revRank) < Math.abs(assignIdx - revRank)) {
                  assignIdx = j;
                }
              }
            }

            const assignedAmt = sortedBankDeps[assignIdx].amount;
            const assignRatio = Math.max(assignedAmt, aiRev) / Math.min(assignedAmt, aiRev);
            if (assignRatio >= 1.15) {
//               scrubLog("PostAI-DepositSummary", `${detectedBankForDeposit!.name}: AI revenue $${aiRev.toLocaleString()} → bank summary $${assignedAmt.toLocaleString()} (${assignRatio.toFixed(2)}x off). Correcting to bank summary.`);
              if (entry.mr.revenue !== undefined) entry.mr.revenue = assignedAmt;
              else entry.mr.rev = assignedAmt;
            }
            usedBankIdx.add(assignIdx);
          }
        }
      }
    }
  } catch (e: any) {
    // console.error(`[PostAI-DepositSummary] Deposit summary correction error:`, e.message);
  }

  try {
    if (validated.monthlyRevenues && validated.monthlyRevenues.length > 0 && rawText) {
      const lines = rawText.split("\n");
      const genericDepositPatterns = [
        /(?:total\s+)?deposits?\s+(?:and\s+(?:other\s+)?)?credits?\s*:?\s*\$?([\d,]+\.\d{2})/i,
        /(?:total\s+)?deposits?\s*:?\s*\$?([\d,]+\.\d{2})/i,
        /(?:total\s+)?credits?\s*:?\s*\$?([\d,]+\.\d{2})/i,
        /deposits?\s+and\s+additions?\s*:?\s*\$?([\d,]+\.\d{2})/i,
        /total\s+additions?\s*:?\s*\$?([\d,]+\.\d{2})/i,
        /(\d+)\s+deposits?\s+(?:and\s+(?:other\s+)?)?credits?\s+\$?([\d,]+\.\d{2})/i,
        /(\d+)\s+deposits?\s*\$?([\d,]+\.\d{2})/i,
        /(\d+)\s+Credit\(?s?\)?\s+This\s+Period\s+\$?([\d,]+\.\d{2})/i,
      ];

      interface GenericDepHit { amount: number; lineIdx: number; label: string }
      const genericHits: GenericDepHit[] = [];

      for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        if (/\b(check|cheque|withdrawal|debit|payment|fee|charge|service)\b/i.test(line) &&
            !/deposit/i.test(line)) continue;
        for (const pat of genericDepositPatterns) {
          const m = line.match(pat);
          if (m) {
            const groups = m.filter((_, idx) => idx > 0).map(g => g ? parseFloat(g.replace(/,/g, "")) : NaN).filter(v => !isNaN(v));
            const amt = groups.length > 1 ? groups[groups.length - 1] : groups[0];
            if (amt > 100 && amt < 50000000) {
              const alreadyHit = genericHits.some(h => Math.abs(h.amount - amt) < 0.01 && Math.abs(h.lineIdx - i) < 3);
              if (!alreadyHit) {
                genericHits.push({ amount: amt, lineIdx: i, label: line.trim().substring(0, 60) });
              }
            }
          }
        }
      }

      if (genericHits.length > 0) {
        for (const mr of validated.monthlyRevenues) {
          const aiRev = mr.revenue ?? mr.rev ?? 0;
          if (aiRev <= 0) continue;

          const closeMatch = genericHits.find(h => Math.abs(h.amount - aiRev) / Math.max(aiRev, h.amount) < 0.05);
          if (closeMatch) continue;

          const plausibleHits = genericHits.filter(h => h.amount > aiRev * 2 && h.amount < 50000000);
          if (plausibleHits.length > 0) {
            const bestHit = plausibleHits.reduce((best, h) => {
              const neighbors = genericHits.filter(oh => Math.abs(oh.lineIdx - h.lineIdx) <= 15 && oh !== h);
              const bestNeighbors = genericHits.filter(oh => Math.abs(oh.lineIdx - best.lineIdx) <= 15 && oh !== best);
              return neighbors.length > bestNeighbors.length ? h : best;
            });

            const ratio = bestHit.amount / aiRev;
            if (ratio >= 3 && ratio < 50) {
              // console.log(`[PostAI-GenericDeposit] AI revenue $${aiRev.toLocaleString()} << bank text deposit $${bestHit.amount.toLocaleString()} (${ratio.toFixed(1)}x higher). Line: "${bestHit.label}". Flagging for review.`);
              mr.needsReview = true;
              mr.reviewReason = `AI deposit ($${aiRev.toLocaleString()}) vs bank summary ($${bestHit.amount.toLocaleString()})`;
              mr.aiAmount = aiRev;
              mr.parserAmount = bestHit.amount;
              reviewFlags.push({
                month: mr.month,
                account: mr.account || mr.acctLast4,
                aiAmount: aiRev,
                parserAmount: bestHit.amount,
                usedValue: aiRev,
                reason: `Generic deposit mismatch (${ratio.toFixed(1)}x)`
              });
            } else if (ratio >= 50) {
              // console.log(`[PostAI-GenericDeposit] SKIPPING review: parser amount $${bestHit.amount.toLocaleString()} is ${ratio.toFixed(0)}x the AI value $${aiRev.toLocaleString()} — clearly a misparse (account number, running total, etc). Trusting AI.`);
            }
          }
        }
      }
    }
  } catch (e: any) {
    // console.error(`[PostAI-GenericDeposit] Generic deposit validation error:`, e.message);
  }

  if (reviewFlags!.length > 0) {
    validated.depositReviewFlags = reviewFlags;
//     scrubLog("PostAI-Validation", `${reviewFlags!.length} deposit(s) flagged for manual review`);
  }

  if (validated.loanDetails && validated.loanDetails.length > 0) {
    const beforeCount = validated.loanDetails.length;
    validated.loanDetails = validated.loanDetails.filter((l: any) => {
      const hasFundedAmount = l.fundedAmount && l.fundedAmount > 0;
      if ((!l.amount || l.amount <= 0) && !hasFundedAmount) {
        // console.log(`[PostAI-Sanitize] Removing invalid loan "${l.lender}": amount=${l.amount}, no funded amount`);
        return false;
      }
      if (l.frequency && l.frequency !== "daily" && l.frequency !== "weekly" && !hasFundedAmount) {
        // console.log(`[PostAI-Sanitize] Removing invalid loan "${l.lender}": frequency="${l.frequency}", no funded amount`);
        return false;
      }
      return true;
    });
    if (validated.loanDetails.length !== beforeCount) {
      // console.log(`[PostAI-Sanitize] Removed ${beforeCount - validated.loanDetails.length} invalid loan entries`);
      validated.hasLoans = validated.loanDetails.length > 0;
    }
  }

  if (rawText && validated.loanDetails && validated.loanDetails.length > 0) {
//     scrubLog("POSTAI-FINAL", `Running finalAmountVerification on ${validated.loanDetails.length} loan(s)...`);
    finalAmountVerification(validated.loanDetails, rawText);
  }

  if (rawText) {
//     scrubLog("POSTAI-FINAL", `Running finalIntegrityCheck...`);
    finalIntegrityCheck(validated, rawText);
  }

//   scrubLog("POSTAI", `===== postAIValidation COMPLETE =====`);
//   scrubLog("POSTAI", `Output: ${(validated.monthlyRevenues || []).length} revenues, ${(validated.loanDetails || []).length} loans`);
  for (const mr of (validated.monthlyRevenues || [])) {
    const rev = mr.revenue ?? mr.rev ?? 0;
//     scrubLog("POSTAI", `  Revenue: ${mr.month} acct=${mr.account || "?"} bank="${mr.bankName || "?"}" $${rev.toLocaleString()}${mr.needsReview ? " [REVIEW]" : ""}`);
  }
  for (const loan of (validated.loanDetails || [])) {
//     scrubLog("POSTAI", `  Loan: "${loan.lender}" $${loan.amount || 0} freq=${loan.frequency || "?"} occ=${loan.occurrences || 0}`);
  }

  return validated;
}

function finalAmountVerification(loanDetails: any[], rawText: string): void {
  if (!loanDetails || loanDetails.length === 0 || !rawText) return;

//   scrubLog("FinalVerify", `===== FINAL AMOUNT VERIFICATION: ${loanDetails.length} loans =====`);

  const lines = rawText.split("\n");

  for (const loan of loanDetails) {
    const lenderName = (loan.lender || "").trim();
    if (!lenderName || lenderName.length < 3) continue;
    if (!loan.amount || loan.amount <= 0) continue;

    const lenderLower = lenderName.toLowerCase();
    const lenderNorm = lenderLower.replace(/[^a-z0-9]/g, "");

    if (lenderNorm.length < 4) {
//       scrubLog("FinalVerify", `${lenderName}: normalized name too short ("${lenderNorm}") — skipping`);
      continue;
    }

    const lenderPrefix = lenderNorm.slice(0, Math.min(lenderNorm.length, 14));
    const lenderWords = lenderName.toLowerCase().split(/\s+/).filter(
      (w: string) => w.length >= 4 && !/^(the|and|inc|llc|corp|daily|weekly|ach|debit|credit|payment)$/i.test(w)
    );

    const amountsByLine: number[] = [];

    for (let i = 0; i < lines.length; i++) {
      const lineNorm = lines[i].toLowerCase().replace(/[^a-z0-9]/g, "");

      let matched = false;

      if (lineNorm.includes(lenderPrefix)) {
        matched = true;
      }

      if (!matched && lenderWords.length >= 2) {
        const lineLower = lines[i].toLowerCase();
        const wordBoundaryMatches = lenderWords.filter((w: string) => {
          const wbRegex = new RegExp(`\\b${w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}`, "i");
          return wbRegex.test(lineLower);
        });
        if (wordBoundaryMatches.length >= Math.ceil(lenderWords.length * 0.8)) {
          matched = true;
        }
      }

      if (!matched) continue;

      const depositKeywords = /\b(deposit|credit|transfer\s+from|incoming|wire\s+in|refund|mobile\s+deposit)\b/i;
      if (depositKeywords.test(lines[i]) && !/\b(debit|payment|autopay|ach\s*debit|withdrawal)\b/i.test(lines[i])) continue;

      let amounts = [...lines[i].matchAll(/\$?([\d,]+\.\d{2})/g)]
        .map(m => parseFloat(m[1].replace(/,/g, "")))
        .filter(a => a > 1 && a < 500000);

      if (amounts.length === 0) {
        for (let adj = 1; adj <= 2; adj++) {
          const nextLine = i + adj < lines.length ? lines[i + adj] : "";
          const prevLine = i - adj >= 0 ? lines[i - adj] : "";
          const adjAmounts = [...(nextLine + " " + prevLine).matchAll(/\$?([\d,]+\.\d{2})/g)]
            .map(m => parseFloat(m[1].replace(/,/g, "")))
            .filter(a => a > 1 && a < 500000);
          if (adjAmounts.length > 0) { amounts = adjAmounts; break; }
        }
        if (amounts.length === 0) continue;
      }

      let txnAmount: number;
      if (amounts.length === 1) {
        txnAmount = amounts[0];
      } else if (amounts.length === 2) {
        txnAmount = amounts[0];
      } else {
        txnAmount = amounts[amounts.length - 2];
      }

      amountsByLine.push(txnAmount);
    }

    if (amountsByLine.length < 3) {
//       scrubLog("FinalVerify", `${lenderName}: only ${amountsByLine.length} text matches — skipping (need 3+)`);
      continue;
    }

    const freq = new Map<number, number>();
    for (const a of amountsByLine) {
      const key = Math.round(a * 100) / 100;
      freq.set(key, (freq.get(key) || 0) + 1);
    }

    const sorted = [...freq.entries()].sort((a, b) => b[1] - a[1]);
    const topAmount = sorted[0][0];
    const topCount = sorted[0][1];
    const secondCount = sorted.length > 1 ? sorted[1][1] : 0;

    const currentAmount = Math.round(loan.amount * 100) / 100;
    const diff = Math.abs(currentAmount - topAmount);

    if (diff < 0.01) {
//       scrubLog("FinalVerify", `${lenderName}: CONFIRMED $${currentAmount} (${topCount}x in text, ${amountsByLine.length} total matches)`);
      continue;
    }

    const currentInText = freq.get(currentAmount) || 0;

    if (currentInText > 0) {
//       scrubLog("FinalVerify", `${lenderName}: current $${currentAmount} appears ${currentInText}x in text (top is $${topAmount} at ${topCount}x). Keeping current — may be renewal/increase. All: ${sorted.map(([a, c]) => `$${a}x${c}`).join(", ")}`);
      continue;
    }

    if (topCount >= 3 && topCount >= secondCount * 2) {
//       scrubLog("FinalVerify", `*** CORRECTING ${lenderName}: $${currentAmount} (0x in text) → $${topAmount} (${topCount}x, ${(topCount / amountsByLine.length * 100).toFixed(0)}% of matches). All: ${sorted.map(([a, c]) => `$${a}x${c}`).join(", ")}`);
      loan.amount = topAmount;
      loan.payment = topAmount;
    } else {
//       scrubLog("FinalVerify", `${lenderName}: $${currentAmount} not found in text but no dominant alternative. Top: $${topAmount}x${topCount}, 2nd: $${sorted.length > 1 ? sorted[1][0] : 0}x${secondCount}. All: ${sorted.map(([a, c]) => `$${a}x${c}`).join(", ")}`);
    }
  }
//   scrubLog("FinalVerify", `===== VERIFICATION COMPLETE =====`);
}

function applyParserDrivenPaidOff(validated: AnalysisResult, engineRecurring: any[]): void {
  if (!validated.loanDetails || validated.loanDetails.length === 0 || engineRecurring.length === 0) return;

  const periodsWithData = engineRecurring
    .map((r: any) => r.statementPeriodEnd || "")
    .filter((p: string) => p.length > 0);
  if (periodsWithData.length === 0) return;

  function parsePeriodToSortKey(period: string): string {
    const m = period.match(/(\d{1,2})\/(\d{1,2})\/(\d{2,4})/);
    if (m) {
      const yr = m[3].length === 2 ? "20" + m[3] : m[3];
      return `${yr}-${m[1].padStart(2, "0")}-${m[2].padStart(2, "0")}`;
    }
    const mWord = period.match(/(\w+)\s+(\d{1,2}),?\s+(\d{4})/);
    if (mWord) {
      const monthNames: Record<string, string> = { january:"01",february:"02",march:"03",april:"04",may:"05",june:"06",july:"07",august:"08",september:"09",october:"10",november:"11",december:"12" };
      const mo = monthNames[mWord[1].toLowerCase()] || "01";
      return `${mWord[3]}-${mo}-${mWord[2].padStart(2, "0")}`;
    }
    return period;
  }

  function periodToMonthLabel(period: string): { yearMonth: string; label: string } {
    const m = period.match(/(\d{1,2})\/(\d{1,2})\/(\d{2,4})/);
    if (m) {
      const yr = m[3].length === 2 ? "20" + m[3] : m[3];
      const mo = m[1].padStart(2, "0");
      const monthLabels: Record<string, string> = { "01":"Jan","02":"Feb","03":"Mar","04":"Apr","05":"May","06":"Jun","07":"Jul","08":"Aug","09":"Sep","10":"Oct","11":"Nov","12":"Dec" };
      return { yearMonth: `${yr}-${mo}`, label: `${monthLabels[mo] || mo} ${yr}` };
    }
    const mWord = period.match(/(\w+)\s+\d{1,2},?\s+(\d{4})/);
    if (mWord) {
      const monthNames: Record<string, string> = { january:"01",february:"02",march:"03",april:"04",may:"05",june:"06",july:"07",august:"08",september:"09",october:"10",november:"11",december:"12" };
      const mo = monthNames[mWord[1].toLowerCase()] || "01";
      const monthLabels: Record<string, string> = { "01":"Jan","02":"Feb","03":"Mar","04":"Apr","05":"May","06":"Jun","07":"Jul","08":"Aug","09":"Sep","10":"Oct","11":"Nov","12":"Dec" };
      return { yearMonth: `${mWord[2]}-${mo}`, label: `${monthLabels[mo]} ${mWord[2]}` };
    }
    return { yearMonth: period, label: period };
  }

  const sortedPeriods = [...new Set(periodsWithData)].sort((a, b) => parsePeriodToSortKey(b).localeCompare(parsePeriodToSortKey(a)));
  const mostRecentPeriod = sortedPeriods[0];

  for (const loan of validated.loanDetails) {
    if (loan.possiblyPaidOff) continue;

    const loanKey = normalizeLenderKey(loan.lender || "");
    if (!loanKey) continue;

    const loanInMostRecent = engineRecurring.some((r: any) => {
      const rKey = normalizeLenderKey(r.lender || r.shortName || "");
      const periodMatch = (r.statementPeriodEnd || "") === mostRecentPeriod;
      return periodMatch && (rKey.includes(loanKey) || loanKey.includes(rKey));
    });

    if (!loanInMostRecent) {
      const foundInPeriods: string[] = [];
      for (const r of engineRecurring) {
        const rKey = normalizeLenderKey(r.lender || r.shortName || "");
        if (rKey.includes(loanKey) || loanKey.includes(rKey)) {
          const period = (r as any).statementPeriodEnd || "";
          if (period && !foundInPeriods.includes(period)) foundInPeriods.push(period);
        }
      }

      if (foundInPeriods.length > 0) {
        const sortedFound = foundInPeriods.sort((a, b) => parsePeriodToSortKey(b).localeCompare(parsePeriodToSortKey(a)));
        const lastSeenPeriod = sortedFound[0];
        const { yearMonth, label } = periodToMonthLabel(lastSeenPeriod);
        loan.possiblyPaidOff = true;
        loan.lastSeenMonth = yearMonth;
        loan.lastSeenLabel = label;
        console.log(`[ParserPaidOff] "${loan.lender}" NOT in most recent statement (${mostRecentPeriod}). Last seen: ${label} — marking as paid off`);
      }
    }
  }
}

function integrateTransactionEngineResults(engine: TransactionEngineResult, validated: AnalysisResult, rawText?: string): void {
  if (!engine.transactions.length && !engine.recurringPayments.length && !engine.fundingDeposits.length) return;

  // console.log(`[TxnEngine-Integration] Starting: ${engine.transactions.length} txns, ${engine.recurringPayments.length} recurring, ${engine.fundingDeposits.length} funding deposits`);

  const existingLenders = new Set(
    (validated.loanDetails || []).map((l: any) => normalizeLenderKey(l.lender || ""))
  );

  for (const rp of engine.recurringPayments) {
    const normalizedKey = normalizeLenderKey(rp.lender);

    const matchingLoans = (validated.loanDetails || []).filter((l: any) => {
      const ek = normalizeLenderKey(l.lender || "");
      return ek === normalizedKey || (ek.length >= 3 && (normalizedKey.includes(ek) || ek.includes(normalizedKey)));
    });

    const amountMatchesExisting = matchingLoans.some((l: any) => {
      const existingAmt = l.amount || l.payment || 0;
      if (existingAmt === 0) return false;
      const ratio = Math.max(existingAmt, rp.amount) / Math.min(existingAmt, rp.amount);
      return ratio <= 1.20;
    });

    if (amountMatchesExisting) {
      const existingLoan = matchingLoans.find((l: any) => {
        const existingAmt = l.amount || l.payment || 0;
        if (existingAmt === 0) return false;
        const ratio = Math.max(existingAmt, rp.amount) / Math.min(existingAmt, rp.amount);
        return ratio <= 1.20;
      });
      if (existingLoan) {
        const existingOcc = existingLoan.occurrences || 0;
        if (rp.occurrences > existingOcc && rp.confidence === "high") {
          // console.log(`[TxnEngine-Integration] CORRECTING "${existingLoan.lender}": occurrences ${existingOcc} → ${rp.occurrences}, amount $${existingLoan.amount} → $${rp.amount}`);
          existingLoan.occurrences = rp.occurrences;
          existingLoan.amount = rp.amount;
          existingLoan.payment = rp.amount;
          existingLoan.frequency = rp.frequency;
          existingLoan.confidence = "high";
          (existingLoan as any).engineVerified = true;
        }
      }
      continue;
    }

    const isKnownLender = KNOWN_LENDER_SHORTNAMES.has(rp.shortName?.toLowerCase() || normalizeLenderKey(rp.lender));
    const minOccRequired = isKnownLender ? 2 : 3;
    const meetsThreshold = rp.occurrences >= minOccRequired;

    if (meetsThreshold) {
      if (!validated.loanDetails) validated.loanDetails = [];
      const isSecondPosition = matchingLoans.length > 0;

      if (isSecondPosition) {
        const primaryLoan = matchingLoans.reduce((best: any, l: any) => (!best || (l.occurrences || 0) > (best.occurrences || 0)) ? l : best, null);
        const primaryOcc = primaryLoan?.occurrences || 0;
        const primaryAmt = primaryLoan?.amount || primaryLoan?.payment || 0;
        const secAmtRatio = primaryAmt > 0 ? Math.max(rp.amount, primaryAmt) / Math.min(rp.amount, primaryAmt) : 1;
        if (rp.occurrences <= 5 && primaryOcc >= 10 && secAmtRatio > 2) {
          // console.log(`[TxnEngine-Integration] SKIPPING second position "${rp.lender}" $${rp.amount} x${rp.occurrences} — primary has ${primaryOcc} occ at $${primaryAmt}, ratio=${secAmtRatio.toFixed(1)}x — likely one-time payment`);
          continue;
        }
      }

      const newLoan = {
        lender: rp.lender,
        amount: rp.amount,
        payment: rp.amount,
        frequency: rp.frequency,
        occurrences: rp.occurrences,
        confidence: rp.confidence,
        reasoning: isSecondPosition
          ? `Transaction engine detected SECOND position: ${rp.occurrences} payments of $${rp.amount.toFixed(2)} (${rp.frequency}) — concurrent with existing $${matchingLoans[0]?.amount || 0} position. Dates: ${rp.dates.slice(0, 10).join(", ")}`
          : `Transaction engine detected ${rp.occurrences} payments of $${rp.amount.toFixed(2)} (${rp.frequency}) on dates: ${rp.dates.slice(0, 10).join(", ")}`,
        engineVerified: true,
      };
      validated.loanDetails.push(newLoan as any);
      validated.hasLoans = true;
      if (!isSecondPosition) existingLenders.add(normalizedKey);
      // console.log(`[TxnEngine-Integration] ADDED ${isSecondPosition ? "second position" : "new loan"}: "${rp.lender}" $${rp.amount} x${rp.occurrences} (${rp.frequency})`);
    }
  }

  for (const fd of engine.fundingDeposits) {
    const fdKey = normalizeLenderKey(fd.lender);
    for (const loan of (validated.loanDetails || [])) {
      const loanKey = normalizeLenderKey(loan.lender || "");
      if (loanKey.includes(fdKey) || fdKey.includes(loanKey)) {
        if (!loan.fundedAmount || fd.amount > (loan.fundedAmount || 0)) {
          (loan as any).fundedAmount = fd.amount;
          (loan as any).funded_amount = fd.amount;
          (loan as any).fundedDate = fd.date;
          (loan as any).funded_date = fd.date;
          // console.log(`[TxnEngine-Integration] Matched funding $${fd.amount.toLocaleString()} on ${fd.date} to "${loan.lender}"`);
        }
        break;
      }
    }
  }

  if (engine.totalCredits > 0 && engine.credits.length >= 5 && validated.monthlyRevenues && validated.monthlyRevenues.length > 0) {
    const totalAI = validated.monthlyRevenues.reduce((s: number, r: any) => s + (r.revenue ?? r.rev ?? 0), 0);
    if (totalAI > 0 && engine.totalCredits > totalAI * 1.3 && engine.credits.length >= 10) {
      // console.log(`[TxnEngine-Integration] Engine credits $${Math.round(engine.totalCredits).toLocaleString()} >> AI total $${Math.round(totalAI).toLocaleString()} (${engine.credits.length} credit txns)`);

      if (validated.monthlyRevenues.length === 1) {
        const rev = validated.monthlyRevenues[0];
        const currentAmt = rev.revenue ?? rev.rev ?? 0;
        if (engine.totalCredits > currentAmt * 1.5) {
          // console.log(`[TxnEngine-Integration] OVERRIDE single month: AI=$${Math.round(currentAmt).toLocaleString()} → Engine=$${Math.round(engine.totalCredits).toLocaleString()}`);
          if (rev.revenue !== undefined) rev.revenue = engine.totalCredits;
          else rev.rev = engine.totalCredits;
        }
      }
    } else {
      for (const rev of validated.monthlyRevenues) {
        const currentAmt = rev.revenue ?? rev.rev ?? 0;
        if (currentAmt > 0 && engine.totalCredits > 0) {
          const diff = Math.abs(engine.totalCredits - currentAmt);
          const ratio = diff / Math.max(currentAmt, 1);
          if (ratio > 0.1) {
            // console.log(`[TxnEngine-Integration] Deposit cross-check: AI/bank=$${Math.round(currentAmt).toLocaleString()}, engine credits=$${Math.round(engine.totalCredits).toLocaleString()} (diff ${(ratio * 100).toFixed(1)}%)`);
          }
        }
      }
    }
  }

  if (validated.loanDetails) {
    for (const loan of validated.loanDetails) {
      if ((loan as any).engineVerified) continue;
      const loanKey = normalizeLenderKey(loan.lender || "");
      const engineMatch = engine.recurringPayments.find(rp => {
        const rpKey = normalizeLenderKey(rp.lender);
        return rpKey === loanKey || rpKey.includes(loanKey) || loanKey.includes(rpKey);
      });
      if (engineMatch) {
        (loan as any).engineVerified = true;
      }
    }
  }

  if (rawText) {
    correctLoanAmountsFromText(validated.loanDetails || [], rawText);
    validated.loanDetails = splitMultiPositionLenders(validated.loanDetails || [], rawText);
    validated.loanDetails = removeDuplicateLenderEntries(validated.loanDetails || [], "PostSplit-Dedup");
  }
}

function finalIntegrityCheck(validated: AnalysisResult, rawText: string): void {
//   scrubLog("QA", `===== QUALITY ASSURANCE ENGINE =====`);
  const trace = getActiveTrace();
  let issuesFound = 0;
  let issuesFixed = 0;

  const allAmountsInText = new Set<number>();
  const amountLines: Map<number, string[]> = new Map();
  const lines = rawText.split("\n");
  for (let i = 0; i < lines.length; i++) {
    for (const m of lines[i].matchAll(/\$?([\d,]+\.\d{2})/g)) {
      const val = parseFloat(m[1].replace(/,/g, ""));
      if (val > 0 && val < 50_000_000) {
        const rounded = Math.round(val * 100) / 100;
        allAmountsInText.add(rounded);
        if (!amountLines.has(rounded)) amountLines.set(rounded, []);
        amountLines.get(rounded)!.push(lines[i].trim());
      }
    }
  }

  const extractedAcct = extractAccountNumberFromText(rawText);
  const allAccounts = extractAllAccountLast4s(rawText);
  const isMultiAccountText = allAccounts.length > 1;
  if (isMultiAccountText) {
//     scrubLog("QA", `Multi-account text detected (${allAccounts.length} accounts: ${allAccounts.join(", ")}). Account auto-fill disabled.`);
  }

  if (validated.monthlyRevenues && validated.monthlyRevenues.length > 0) {
    for (const mr of validated.monthlyRevenues) {
      const rev = mr.revenue ?? mr.rev ?? 0;
      const revKey = Math.round(rev * 100) / 100;

      if (rev <= 0) {
//         scrubLog("QA", `ISSUE: Revenue for ${mr.month} is $0 or negative ($${rev}). Flagging for review.`);
        trace?.warn("QA_REVENUE", `${mr.month}: $0 or negative ($${rev}) — flagged for review`);
        mr.needsReview = true;
        mr.reviewReason = mr.reviewReason || `Zero/negative revenue ($${rev})`;
        issuesFound++;
        continue;
      }

      if (!allAmountsInText.has(revKey)) {
        const closestInText = [...allAmountsInText].reduce((best, a) => {
          return Math.abs(a - rev) < Math.abs(best - rev) ? a : best;
        }, 0);
        const pctDiff = closestInText > 0 ? Math.abs(closestInText - rev) / Math.max(closestInText, rev) : 1;
        if (pctDiff < 0.005) {
//           scrubLog("QA", `FIX: Revenue $${rev} → rounding match $${closestInText} (${(pctDiff * 100).toFixed(3)}% off)`);
          trace?.fix("QA_REVENUE", `${mr.month}: $${rev} → rounding corrected to $${closestInText}`);
          if (mr.revenue !== undefined) mr.revenue = closestInText;
          else mr.rev = closestInText;
          issuesFixed++;
        } else {
//           scrubLog("QA", `ISSUE: Revenue $${rev} for ${mr.month} not found in raw text. Closest: $${closestInText} (${(pctDiff * 100).toFixed(1)}% off)`);
          trace?.warn("QA_REVENUE", `${mr.month}: $${rev} not in raw text. Closest: $${closestInText} (${(pctDiff * 100).toFixed(1)}% off)`);
          issuesFound++;
        }
      }

      const acct = (mr.account || "").replace(/[\s-]/g, "").replace(/\D/g, "");
      if ((!acct || acct.length < 4) && extractedAcct && !isMultiAccountText) {
//         scrubLog("QA", `FIX: Revenue ${mr.month} missing account → "${extractedAcct}"`);
        mr.account = extractedAcct;
        issuesFixed++;
      }
    }

    const revs = validated.monthlyRevenues.map((r: any) => r.revenue ?? r.rev ?? 0).filter((v: number) => v > 0);
    if (revs.length >= 2) {
      const avg = revs.reduce((a: number, b: number) => a + b, 0) / revs.length;
      for (let i = 0; i < validated.monthlyRevenues.length; i++) {
        const mr = validated.monthlyRevenues[i];
        const r = mr.revenue ?? mr.rev ?? 0;
        if (r <= 0) continue;
        const ratio = r / avg;
        if (ratio > 5 || ratio < 0.1) {
          const desc = ratio > 5 ? `${ratio.toFixed(1)}x above` : `${(1/ratio).toFixed(1)}x below`;
//           scrubLog("QA", `ISSUE: Revenue ${mr.month} = $${r.toLocaleString()} is ${ratio > 5 ? `${ratio.toFixed(1)}x higher` : `${(1/ratio).toFixed(1)}x lower`} than average $${avg.toLocaleString()}. Flagging.`);
          trace?.warn("QA_REVENUE", `${mr.month}: $${r.toLocaleString()} outlier (${desc} avg $${avg.toLocaleString()}) — flagged`);
          mr.needsReview = true;
          mr.reviewReason = mr.reviewReason || `Revenue outlier (${desc} average)`;
          issuesFound++;
        }
      }
    }

    const monthSet = new Set<string>();
    for (const mr of validated.monthlyRevenues) {
      const mKey = `${(mr.month || "").slice(0, 7)}_${(mr.account || "").slice(-4)}`;
      if (monthSet.has(mKey)) {
//         scrubLog("QA", `ISSUE: Duplicate revenue entry for ${mr.month} account ${mr.account}`);
        issuesFound++;
      }
      monthSet.add(mKey);
    }
  }

  if (extractedAcct) {
    const currentAcct = ((validated as any).accountNumber || "").replace(/[\s-]/g, "").slice(-4);
    if (!currentAcct || currentAcct.length < 4) {
//       scrubLog("QA", `FIX: Top-level accountNumber missing → "${extractedAcct}"`);
      (validated as any).accountNumber = extractedAcct;
      issuesFixed++;
    }
  }

  if (validated.loanDetails && validated.loanDetails.length > 0) {
    for (const loan of validated.loanDetails) {
      const lenderName = (loan.lender || "").toLowerCase();
      const amt = loan.amount || 0;
      const amtKey = Math.round(amt * 100) / 100;

      if (amt > 0) {
        const lenderInLines = lenderName.length >= 3 ? lines.filter(l => l.toLowerCase().includes(lenderName.slice(0, Math.min(lenderName.length, 15)))) : [];
        let amountNearLender = false;
        if (lenderInLines.length > 0) {
          for (const ll of lenderInLines) {
            const lineAmounts = [...ll.matchAll(/\$?([\d,]+\.\d{2})/g)].map(m => parseFloat(m[1].replace(/,/g, "")));
            if (lineAmounts.some(a => Math.abs(a - amt) / Math.max(a, amt) < 0.01)) {
              amountNearLender = true;
              break;
            }
          }
        }

        if (!allAmountsInText.has(amtKey)) {
          const closestInText = [...allAmountsInText].reduce((best, a) => {
            return Math.abs(a - amt) < Math.abs(best - amt) ? a : best;
          }, 0);
          const pctDiff = closestInText > 0 ? Math.abs(closestInText - amt) / Math.max(closestInText, amt) : 1;
          if (pctDiff < 0.005) {
//             scrubLog("QA", `FIX: Loan "${loan.lender}" amount $${amt} → rounding match $${closestInText}`);
            loan.amount = closestInText;
            loan.payment = closestInText;
            issuesFixed++;
          } else if (pctDiff > 0.5) {
//             scrubLog("QA", `CRITICAL: Loan "${loan.lender}" amount $${amt} likely hallucinated (>50% off from any text amount $${closestInText}). Setting low confidence.`);
            trace?.error("QA_LOAN", `"${loan.lender}" $${amt} likely hallucinated — >50% off from any text amount ($${closestInText}). Set low confidence.`);
            loan.confidence = "low";
            issuesFound++;
          } else {
//             scrubLog("QA", `ISSUE: Loan "${loan.lender}" amount $${amt} not in text. Closest: $${closestInText} (${(pctDiff * 100).toFixed(1)}% off)`);
            issuesFound++;
          }
        } else if (!amountNearLender && lenderInLines.length > 0) {
//           scrubLog("QA", `NOTE: Loan "${loan.lender}" amount $${amt} exists in text but not on same line as lender name.`);
        }
      }

      if (amt > 0 && loan.frequency === "daily" && amt > 5000) {
//         scrubLog("QA", `ISSUE: Daily loan "${loan.lender}" has unusually high amount $${amt}/day. Verify frequency.`);
        issuesFound++;
      }
      if (amt > 0 && loan.frequency === "weekly" && amt > 25000) {
//         scrubLog("QA", `ISSUE: Weekly loan "${loan.lender}" has unusually high amount $${amt}/week. Verify frequency.`);
        issuesFound++;
      }

      const funded = loan.fundedAmount || loan.funded_amount || 0;
      const fundedKey = Math.round(funded * 100) / 100;
      if (funded > 0) {
        if (!allAmountsInText.has(fundedKey)) {
          const closestInText = [...allAmountsInText].reduce((best, a) => {
            return Math.abs(a - funded) < Math.abs(best - funded) ? a : best;
          }, 0);
          const pctDiff = closestInText > 0 ? Math.abs(closestInText - funded) / Math.max(closestInText, funded) : 1;
          if (pctDiff < 0.005) {
//             scrubLog("QA", `FIX: Loan "${loan.lender}" funded $${funded} → rounding match $${closestInText}`);
            loan.fundedAmount = closestInText;
            loan.funded_amount = closestInText;
            issuesFixed++;
          } else if (pctDiff > 0.1) {
//             scrubLog("QA", `FIX: Loan "${loan.lender}" funded $${funded} not in text (${(pctDiff * 100).toFixed(1)}% off). Clearing fabricated funded amount.`);
            loan.fundedAmount = null;
            loan.funded_amount = null;
            issuesFixed++;
          }
        }

        if (funded > 0 && amt > 0) {
          const fundedToPayment = funded / amt;
          if (loan.frequency === "daily" && (fundedToPayment < 5 || fundedToPayment > 200)) {
//             scrubLog("QA", `ISSUE: Loan "${loan.lender}" funded/daily ratio = ${fundedToPayment.toFixed(0)} (expected 20-150). Funded $${funded} may be wrong.`);
            issuesFound++;
          }
        }
      }

      const loanAcct = (loan.account || "").replace(/[\s-]/g, "").replace(/\D/g, "");
      if ((!loanAcct || loanAcct.length < 4) && extractedAcct && !isMultiAccountText) {
        loan.account = extractedAcct;
        issuesFixed++;
      }
    }

    const lenderGroups = new Map<string, any[]>();
    for (const loan of validated.loanDetails) {
      const key = normalizeLenderKey(loan.lender || "");
      if (!lenderGroups.has(key)) lenderGroups.set(key, []);
      lenderGroups.get(key)!.push(loan);
    }
    for (const [normalizedKey, group] of lenderGroups) {
      if (group.length <= 1) continue;
      issuesFound++;
      const amtClusters: any[][] = [];
      for (const loan of group) {
        const amt = loan.amount || 0;
        const existingCluster = amtClusters.find(c =>
          c.some((cl: any) => {
            const clAmt = cl.amount || 0;
            if (amt <= 0 || clAmt <= 0) return Math.round(amt) === Math.round(clAmt);
            return Math.max(amt, clAmt) / Math.min(amt, clAmt) <= 1.05;
          })
        );
        if (existingCluster) {
          existingCluster.push(loan);
        } else {
          amtClusters.push([loan]);
        }
      }
      for (const cluster of amtClusters) {
        if (cluster.length <= 1) continue;
        const keepLoan = cluster.reduce((best: any, d: any) => {
          if (d.fundedAmount && !best.fundedAmount) return d;
          if ((d.occurrences || 0) > (best.occurrences || 0)) return d;
          return best;
        }, cluster[0]);
        for (const d of cluster) {
          if (d !== keepLoan) {
            keepLoan.occurrences = Math.max(keepLoan.occurrences || 0, d.occurrences || 0);
            if (!keepLoan.fundedAmount && d.fundedAmount) { keepLoan.fundedAmount = d.fundedAmount; keepLoan.funded_amount = d.fundedAmount; }
            if (!keepLoan.fundedDate && d.fundedDate) keepLoan.fundedDate = d.fundedDate;
          }
        }
        trace?.fix("QA_LOAN", `Removed ${cluster.length - 1} duplicate(s) of "${keepLoan.lender}" ($${keepLoan.amount} ${keepLoan.frequency})`);
        const removeSet = new Set(cluster.filter((l: any) => l !== keepLoan));
        validated.loanDetails = validated.loanDetails.filter((l: any) => !removeSet.has(l));
        issuesFixed++;
      }
    }

    const fuzzyRemoveIdxs = new Set<number>();
    for (let i = 0; i < validated.loanDetails.length; i++) {
      if (fuzzyRemoveIdxs.has(i)) continue;
      const a = validated.loanDetails[i];
      const aKey = normalizeLenderKey(a.lender || "");
      const aAmt = a.amount || a.payment || 0;
      for (let j = i + 1; j < validated.loanDetails.length; j++) {
        if (fuzzyRemoveIdxs.has(j)) continue;
        const b = validated.loanDetails[j];
        const bKey = normalizeLenderKey(b.lender || "");
        const bAmt = b.amount || b.payment || 0;
        if (aAmt <= 0 || bAmt <= 0) continue;
        const amtRatio = Math.max(aAmt, bAmt) / Math.min(aAmt, bAmt);
        if (amtRatio > 1.05) continue;

        const aWords = (a.lender || "").toLowerCase().replace(/[^a-z0-9\s]/g, " ").trim().split(/\s+/).filter((w: string) => w.length >= 2 && !/^(the|and|inc|llc|corp|daily|weekly|ach|debit|credit|ccd|ppd|web|co|id|payment|source|sour|onetime)$/i.test(w));
        const bWords = (b.lender || "").toLowerCase().replace(/[^a-z0-9\s]/g, " ").trim().split(/\s+/).filter((w: string) => w.length >= 2 && !/^(the|and|inc|llc|corp|daily|weekly|ach|debit|credit|ccd|ppd|web|co|id|payment|source|sour|onetime)$/i.test(w));
        const sharedWords = aWords.filter((w: string) => bWords.some((bw: string) => bw === w || bw.includes(w) || w.includes(bw)));
        const minLen = Math.min(aWords.length, bWords.length);
        const wordOverlap = minLen > 0 && sharedWords.length >= Math.max(1, Math.ceil(minLen * 0.5));
        const keyOverlap = aKey === bKey || aKey.includes(bKey) || bKey.includes(aKey);

        if (wordOverlap || keyOverlap) {
          const keepIdx = (a.occurrences || 0) >= (b.occurrences || 0) ? i : j;
          const removeIdx = keepIdx === i ? j : i;
          const keepLoan = validated.loanDetails[keepIdx];
          const removeLoan = validated.loanDetails[removeIdx];
          keepLoan.occurrences = Math.max(keepLoan.occurrences || 0, removeLoan.occurrences || 0);
          if (removeLoan.fundedAmount && !keepLoan.fundedAmount) {
            (keepLoan as any).fundedAmount = removeLoan.fundedAmount;
            (keepLoan as any).funded_amount = removeLoan.fundedAmount;
          }
//           scrubLog("QA", `FIX: Fuzzy dedup — "${removeLoan.lender}" ($${bAmt}) is same as "${keepLoan.lender}" ($${aAmt}). Removing duplicate.`);
          trace?.fix("QA_LOAN", `Fuzzy dedup: "${removeLoan.lender}" merged into "${keepLoan.lender}" (same $${aAmt}, ${a.frequency})`);
          fuzzyRemoveIdxs.add(removeIdx);
          issuesFixed++;
        }
      }
    }
    if (fuzzyRemoveIdxs.size > 0) {
      validated.loanDetails = validated.loanDetails.filter((_: any, idx: number) => !fuzzyRemoveIdxs.has(idx));
    }

    if (validated.loanDetails.length > 0 && validated.monthlyRevenues && validated.monthlyRevenues.length > 1) {
      const sortedRevMonths = [...validated.monthlyRevenues]
        .map((r: any) => (r.month || "").trim())
        .filter((m: string) => /^20\d{2}-\d{2}$/.test(m))
        .sort()
        .reverse();
      const mostRecentMonth = sortedRevMonths[0];

      if (mostRecentMonth) {
        const stmtSections: { month: string; text: string }[] = [];
        const sectionRegex = /={5,}\s*STATEMENT\s+\d+:\s*"([^"]+)"\s*={5,}([\s\S]*?)(?=={5,}\s*(?:END\s+)?STATEMENT|$)/gi;
        let match;
        while ((match = sectionRegex.exec(rawText)) !== null) {
          const filename = match[1] || "";
          const sectionText = (match[2] || "").toLowerCase();
          let sectionMonth = "";
          const monthNames: Record<string, string> = { jan:"01",feb:"02",mar:"03",apr:"04",may:"05",jun:"06",jul:"07",aug:"08",sep:"09",oct:"10",nov:"11",dec:"12" };
          const fnLower = filename.toLowerCase();
          const dateMatch = fnLower.match(/(20\d{2})(\d{2})(\d{2})/);
          if (dateMatch) {
            sectionMonth = `${dateMatch[1]}-${dateMatch[2]}`;
          } else {
            for (const [abbr, num] of Object.entries(monthNames)) {
              const pattern = new RegExp(`(${abbr}\\w*)\\s*(20\\d{2})|(20\\d{2})\\s*(${abbr}\\w*)`, "i");
              const m2 = fnLower.match(pattern);
              if (m2) {
                const yr = m2[2] || m2[3];
                sectionMonth = `${yr}-${num}`;
                break;
              }
            }
          }
          if (!sectionMonth) {
            for (const revMonth of sortedRevMonths) {
              const [yr, mo] = revMonth.split("-");
              const moNum = parseInt(mo);
              const moName = Object.keys(monthNames).find(k => monthNames[k] === mo) || "";
              const fullMonthNames: Record<string, string> = { "01":"january","02":"february","03":"march","04":"april","05":"may","06":"june","07":"july","08":"august","09":"september","10":"october","11":"november","12":"december" };
              const fullName = fullMonthNames[mo] || "";
              const header = sectionText.slice(0, 2000);
              if (header.includes(`${fullName} ${yr}`) || header.includes(`${fullName}, ${yr}`) ||
                  header.includes(`${moName} ${yr}`) || header.includes(`${mo}/${yr}`) ||
                  header.includes(`${moNum}/${yr}`)) {
                sectionMonth = revMonth;
                break;
              }
            }
          }
          if (sectionMonth) {
            stmtSections.push({ month: sectionMonth, text: sectionText });
          }
        }

        if (stmtSections.length > 0) {
          const mostRecentSections = stmtSections.filter(s => s.month === mostRecentMonth);

          for (const loan of validated.loanDetails) {
            const lenderName = (loan.lender || "").toLowerCase().replace(/[^a-z0-9\s]/g, " ").trim();
            const allWords = lenderName.split(/\s+/).filter((w: string) => w.length >= 2 && !/^(the|and|inc|llc|corp|daily|weekly|ach|debit|credit|ccd|ppd|web|co|id)$/i.test(w));
            const lenderWords = allWords.filter((w: string) => w.length >= 3);
            const distinctiveWords = lenderWords.filter((w: string) => w.length >= 4 && !/^(capital|advance|funding|finance|lending|financial|payment|group|services?)$/i.test(w));
            let searchTerms: string[];
            if (distinctiveWords.length > 0) {
              searchTerms = distinctiveWords;
            } else if (lenderWords.length > 0) {
              searchTerms = lenderWords.slice(0, 2);
            } else if (allWords.length > 0) {
              searchTerms = allWords.slice(0, 2);
            } else {
              continue;
            }
            if (searchTerms.length === 0) continue;

            const termInText = (text: string, term: string): boolean => {
              try {
                return new RegExp(`\\b${term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, "i").test(text);
              } catch { return text.includes(term); }
            };

            const foundInMostRecent = mostRecentSections.some(section =>
              searchTerms.some((term: string) => termInText(section.text, term))
            );

            if (!foundInMostRecent) {
              const monthsFound: string[] = [];
              for (const section of stmtSections) {
                if (searchTerms.some((term: string) => termInText(section.text, term))) {
                  if (!monthsFound.includes(section.month)) monthsFound.push(section.month);
                }
              }
              if (monthsFound.length > 0) {
                monthsFound.sort().reverse();
                const lastSeen = monthsFound[0];
                const [lsYr, lsMo] = lastSeen.split("-");
                const monthLabels: Record<string, string> = { "01":"Jan","02":"Feb","03":"Mar","04":"Apr","05":"May","06":"Jun","07":"Jul","08":"Aug","09":"Sep","10":"Oct","11":"Nov","12":"Dec" };
                const label = `${monthLabels[lsMo] || lsMo} ${lsYr}`;
                loan.possiblyPaidOff = true;
                loan.lastSeenMonth = lastSeen;
                loan.lastSeenLabel = label;
//                 scrubLog("QA", `FLAG: "${loan.lender}" NOT found in most recent month (${mostRecentMonth}). Last seen: ${label}. Possibly paid off.`);
                trace?.warn("QA_LOAN", `"${loan.lender}" not in most recent statement (${mostRecentMonth}). Last seen: ${label} — possibly paid off`);
                issuesFound++;
              }
            }

            const aiAmt = loan.amount || 0;
            if (aiAmt > 0 && (loan.frequency === "daily" || loan.frequency === "weekly")) {
              const txnAmounts: number[] = [];
              for (const section of stmtSections) {
                const lines = section.text.split("\n");
                for (const line of lines) {
                  if (!searchTerms.some((term: string) => termInText(line, term))) continue;
                  const amtMatches = [...line.matchAll(/(?<![0-9])(\d[\d,]*\.\d{2})(?!\d)/g)];
                  for (const am of amtMatches) {
                    const val = parseFloat(am[1].replace(/,/g, ""));
                    if (val >= 10 && val <= 50000 && Math.abs(val - aiAmt) > 0.01) {
                      txnAmounts.push(val);
                    }
                  }
                }
              }
              if (txnAmounts.length >= 3) {
                const sorted = [...txnAmounts].sort((a, b) => a - b);
                const median = sorted[Math.floor(sorted.length / 2)];
                const amtCounts = new Map<number, number>();
                for (const a of txnAmounts) {
                  const rounded = Math.round(a * 100) / 100;
                  amtCounts.set(rounded, (amtCounts.get(rounded) || 0) + 1);
                }
                let modeAmt = median;
                let modeCount = 0;
                for (const [amt, cnt] of amtCounts) {
                  if (cnt > modeCount) { modeCount = cnt; modeAmt = amt; }
                }
                const bestAmt = modeCount >= 3 ? modeAmt : median;
                const ratio = Math.max(bestAmt, aiAmt) / Math.min(bestAmt, aiAmt);
                let aiAmtAlsoInText = false;
                for (const section of stmtSections) {
                  const lines2 = section.text.split("\n");
                  for (const line2 of lines2) {
                    if (!searchTerms.some((term: string) => termInText(line2, term))) continue;
                    const amtMatches2 = [...line2.matchAll(/(?<![0-9])(\d[\d,]*\.\d{2})(?!\d)/g)];
                    for (const am2 of amtMatches2) {
                      const val2 = parseFloat(am2[1].replace(/,/g, ""));
                      if (Math.abs(val2 - aiAmt) <= 0.01) { aiAmtAlsoInText = true; break; }
                    }
                    if (aiAmtAlsoInText) break;
                  }
                  if (aiAmtAlsoInText) break;
                }
                if (ratio >= 3 && !aiAmtAlsoInText) {
                  trace?.fix("QA_LOAN", `"${loan.lender}" amount corrected: AI $${aiAmt} → text $${bestAmt} (${txnAmounts.length} txn matches)`);
                  loan.amount = bestAmt;
                  issuesFixed++;
                }
              }
            }
          }
        }
      }
    }
  }

  if (validated.loanDetails && validated.loanDetails.length > 0) {
    const rawLines = rawText.toLowerCase().split("\n");
    for (const loan of validated.loanDetails) {
      const aiAmt = loan.amount || 0;
      if (aiAmt <= 0 || (loan.frequency !== "daily" && loan.frequency !== "weekly")) continue;
      if ((loan as any)._amtCorrected) continue;

      const lenderName = (loan.lender || "").toLowerCase().replace(/[^a-z0-9\s]/g, " ").trim();
      const allWordsAmt = lenderName.split(/\s+/).filter((w: string) => w.length >= 2 && !/^(the|and|inc|llc|corp|daily|weekly|ach|debit|credit|ccd|ppd|web|co|id)$/i.test(w));
      const lenderWordsAmt = allWordsAmt.filter((w: string) => w.length >= 3);
      const textWords = lenderWordsAmt.filter((w: string) => /[a-z]/i.test(w));
      const distinctiveWords2 = textWords.filter((w: string) => w.length >= 4 && !/^(capital|advance|funding|finance|lending|financial|payment|group|services?)$/i.test(w));
      const searchTerms: string[] = [];
      if (distinctiveWords2.length > 0) {
        searchTerms.push(...distinctiveWords2);
        const shortButImportant = textWords.filter((w: string) => w.length === 3 && !distinctiveWords2.includes(w));
        if (shortButImportant.length > 0 && distinctiveWords2.length === 1) {
          searchTerms.push(...shortButImportant.slice(0, 1));
        }
      } else if (textWords.length > 0) {
        searchTerms.push(...textWords.filter((w: string) => w.length >= 3).slice(0, 2));
      } else if (allWordsAmt.length > 0) {
        searchTerms.push(...allWordsAmt.slice(0, 2));
      }
      if (searchTerms.length === 0) continue;

      const termTest = (line: string, term: string): boolean => {
        try { return new RegExp(`\\b${term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`, "i").test(line); }
        catch { return line.includes(term); }
      };

      const txnAmounts: number[] = [];
      let aiAmtLineCount = 0;
      for (const line of rawLines) {
        const matched = searchTerms.length >= 2
          ? searchTerms.every((t: string) => termTest(line, t))
          : searchTerms.some((t: string) => termTest(line, t));
        if (!matched) continue;
        const amtMatches = [...line.matchAll(/(?<![0-9])(\d[\d,]*\.\d{2})(?!\d)/g)];
        let aiAmtOnThisLine = false;
        for (const am of amtMatches) {
          const val = parseFloat(am[1].replace(/,/g, ""));
          if (val >= 10 && val <= 50000) {
            if (Math.abs(val - aiAmt) <= 0.01) {
              aiAmtOnThisLine = true;
            } else {
              txnAmounts.push(val);
            }
          }
        }
        if (aiAmtOnThisLine) aiAmtLineCount++;
      }
      if (txnAmounts.length >= 3 && aiAmtLineCount < 3) {
        const sorted = [...txnAmounts].sort((a, b) => a - b);
        const median = sorted[Math.floor(sorted.length / 2)];
        const modeCounts = new Map<number, number>();
        for (const a of txnAmounts) {
          const r = Math.round(a * 100) / 100;
          modeCounts.set(r, (modeCounts.get(r) || 0) + 1);
        }
        let modeAmt = median, modeCount = 0;
        for (const [amt, cnt] of modeCounts) {
          if (cnt > modeCount) { modeCount = cnt; modeAmt = amt; }
        }
        const bestAmt = modeCount >= 3 ? modeAmt : median;
        const ratio = Math.max(bestAmt, aiAmt) / Math.min(bestAmt, aiAmt);
        if (ratio >= 3 && modeCount >= 3) {
          trace?.fix("QA_LOAN", `"${loan.lender}" amount corrected: AI $${aiAmt} → text $${bestAmt} (${txnAmounts.length} txn matches, aiAmt appeared on ${aiAmtLineCount} lines)`);
          loan.amount = bestAmt;
          (loan as any)._amtCorrected = true;
          issuesFixed++;
        }
      }
    }
  }

  if (validated.loanDetails && validated.loanDetails.length > 0) {
    for (const loan of validated.loanDetails) {
      if (loan.fundedAmount || loan.funded_amount) continue;
      const reasoning = (loan.reasoning || "");
      const fundMatch = reasoning.match(/[Ff]unding\s+(?:deposit|amount)\s+of\s+\$?([\d,]+(?:\.\d{2})?)/);
      if (fundMatch) {
        const fundedAmt = parseFloat(fundMatch[1].replace(/,/g, ""));
        if (fundedAmt >= 5000 && fundedAmt <= 500000) {
          loan.fundedAmount = fundedAmt;
          loan.funded_amount = fundedAmt;
          const dateMatch = reasoning.match(/(?:on|dated?)\s+(\d{1,2}\/\d{1,2}(?:\/\d{2,4})?)/);
          if (dateMatch) {
            loan.fundedDate = dateMatch[1];
            loan.funded_date = dateMatch[1];
          }
          trace?.fix("QA_LOAN", `"${loan.lender}" funded amount $${fundedAmt.toLocaleString()} extracted from AI reasoning`);
          issuesFixed++;
        }
      }
      const increaseMatch = reasoning.match(/(?:payment\s+)?amount\s+increased?\s+(?:from\s+\$?[\d,]+(?:\.\d{2})?\s+)?to\s+\$?([\d,]+(?:\.\d{2})?)/i)
        || reasoning.match(/(?:increased|new|current|updated)\s+(?:payment\s+)?amount[^$]*\$?([\d,]+(?:\.\d{2})?)/i)
        || reasoning.match(/\$?([\d,]+(?:\.\d{2})?)\s+.*?(?:increased|new|updated)\s+amount/i);
      if (increaseMatch) {
        const newAmt = parseFloat(increaseMatch[1].replace(/,/g, ""));
        if (newAmt > 0 && newAmt <= 50000 && newAmt !== loan.amount && Math.abs(newAmt - loan.amount) / Math.max(newAmt, loan.amount) < 0.35) {
          trace?.fix("QA_LOAN", `"${loan.lender}" payment updated $${loan.amount} → $${newAmt} (renewal/increase detected in reasoning)`);
          loan.amount = newAmt;
          issuesFixed++;
        }
      }
    }
  }

  if (validated.monthlyRevenues && validated.monthlyRevenues.length > 0) {
    const months = validated.monthlyRevenues.map((r: any) => r.month || "").filter(Boolean);
    const datePatterns = [...rawText.matchAll(/\b(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+(20\d{2})/gi)];
    const mmYYpatterns = [...rawText.matchAll(/\b(0[1-9]|1[0-2])\/\d{1,2}\/(2[0-9])\b/g)];
    const fullYearPatterns = [...rawText.matchAll(/\b(0[1-9]|1[0-2])\/\d{1,2}\/(20\d{2})\b/g)];

    if (datePatterns.length > 0 || mmYYpatterns.length > 0 || fullYearPatterns.length > 0) {
      const monthNames: Record<string, string> = {
        "january": "01", "february": "02", "march": "03", "april": "04",
        "may": "05", "june": "06", "july": "07", "august": "08",
        "september": "09", "october": "10", "november": "11", "december": "12"
      };
      const textMonths = new Set<string>();
      for (const dp of datePatterns) {
        const mm = monthNames[dp[1].toLowerCase()];
        if (mm) textMonths.add(`${dp[2]}-${mm}`);
      }
      for (const mmyy of mmYYpatterns) {
        textMonths.add(`20${mmyy[2]}-${mmyy[1]}`);
      }
      for (const fy of fullYearPatterns) {
        textMonths.add(`${fy[2]}-${fy[1]}`);
      }
      for (const m of months) {
        const normalized = m.trim().slice(0, 7);
        if (normalized.length === 7 && !textMonths.has(normalized) && textMonths.size > 0) {
//           scrubLog("QA", `ISSUE: Revenue month "${m}" not found in statement dates. Text months: ${[...textMonths].join(", ")}`);
          issuesFound++;
        }
      }
    }

    for (const mr of validated.monthlyRevenues) {
      const m = (mr.month || "").trim();
      if (!/^(20\d{2})-(0[1-9]|1[0-2])$/.test(m)) {
//         scrubLog("QA", `ISSUE: Revenue month "${m}" has invalid format (expected YYYY-MM)`);
        issuesFound++;
      }
    }
  }

  if (validated.hasLoans && (!validated.loanDetails || validated.loanDetails.length === 0)) {
//     scrubLog("QA", `FIX: hasLoans=true but loanDetails is empty. Setting hasLoans=false.`);
    validated.hasLoans = false;
    issuesFixed++;
  }
  if (!validated.hasLoans && validated.loanDetails && validated.loanDetails.length > 0) {
//     scrubLog("QA", `FIX: hasLoans=false but ${validated.loanDetails.length} loans found. Setting hasLoans=true.`);
    validated.hasLoans = true;
    issuesFixed++;
  }

  const monthCount = Math.max(1, (validated.monthlyRevenues || []).length);
  const totalRevenues = (validated.monthlyRevenues || []).reduce((sum: number, r: any) => sum + (r.revenue ?? r.rev ?? 0), 0);
  const avgMonthlyRevenue = totalRevenues / monthCount;
  const monthlyLoanPayments = (validated.loanDetails || []).reduce((sum: number, l: any) => {
    const amt = l.amount || 0;
    const freq = l.frequency || "daily";
    return sum + (freq === "daily" ? amt * 22 : freq === "weekly" ? amt * 4 : amt);
  }, 0);
  if (avgMonthlyRevenue > 0 && monthlyLoanPayments > avgMonthlyRevenue * 0.8) {
//     scrubLog("QA", `WARNING: Monthly loan payments ($${monthlyLoanPayments.toLocaleString()}) exceed 80% of avg monthly revenue ($${avgMonthlyRevenue.toLocaleString()}/mo). Verify loan amounts.`);
  }

  trace?.info("QA_SUMMARY", `${issuesFound} issues found, ${issuesFixed} auto-fixed`);
//   scrubLog("QA", `===== QA COMPLETE: ${issuesFound} issues found, ${issuesFixed} auto-fixed =====`);
}

function detectFundedAmountsFromCredits(rawText: string, analysis: AnalysisResult): void {
  if (!analysis.loanDetails) analysis.loanDetails = [];

  const fundingLenderPattern = /\b(ondeck|on\s*deck|odk|kabbage|bluevine|fundbox|can\s*capital|rapid\s*financ|credibly|libertas|yellowstone|pearl\s*capital|forward\s*fin|forwardfinusa|fora\s*financial|kalamata|national\s*funding|capytal|fox\s*fund|mantis|everest|cfg\s*merchant|mulligan|itria|cloudfund|navitas|greenbox|world\s*business|breakout|headway|behalf|vox\s*fund|wynwood|qfs|jmb\s*capital|unique\s*fund|samson|kings\s*capital|stage\s*adv|7even|cashable|vitalcap|vital\s*capital|vcg|zen\s*fund|ace\s*fund|aspire\s*fund|breeze\s*advance|canfield|clara\s*capital|compass\s*fund|daytona|diamond\s*advance|elevate\s*fund|epic\s*advance|expansion\s*capital|family\s*fund|fenix\s*capital|figure\s*lending|fresh\s*fund|funding\s*metrics|giggle\s*financ|gotorro|highland|hightower|honor\s*capital|idea\s*247|ifund|immediate\s*advance|immediate\s*capital|iou\s*central|kapitus|gmfunding|gm\s*funding|sq\s*advance|w\s+funding|lcf|legend\s*advance|lendbuzz|lendistry|lg\s*funding|liberty\s*fund|litefund|millstone|mr\s*advance|newco|newport\s*business|nitro\s*advance|oak\s*capital|ocean\s*advance|olympus|one\s*river|orange\s*advance|overton|parkside|path\s*2\s*capital|power\s*fund|premium\s*merchant|prosperum|ram\s*(?:payment|capital)|readycap|reboost|redwood\s*business|reliance\s*financial|retro\s*advance|revenued|rocket\s*capital|specialty\s*capital|stellar\s*advance|suncoast|swift\s*fund|tbf\s*group|the\s*fundworks|thefundworks|fundworks|triton|trupath|ufce|ufs|upfunding|vader|wave\s*advance|webfunder|westwood|wide\s*merchant|pipe\s*capital|ssmb|coast\s*fund|fintegra|altfunding|alt\s*funding|funding\s*futures|mako\s*fund|main\s*street\s*group|integra\s*fund)\b/i;

  const LENDER_SHORT_NAMES: Record<string, string> = {
    "ondeck": "ondeck", "on deck": "ondeck", "odk": "ondeck", "ondeck capital": "ondeck", "on deck capital": "ondeck",
    "forward": "forward", "forwardfinusa": "forward", "forwardfin": "forward", "forward fin": "forward", "forwardfinance": "forward", "forwardfinance3": "forward",
    "fundworks": "fundworks", "the fundworks": "fundworks", "thefundworks": "fundworks", "the fundworks financial": "fundworks",
    "fundbox": "fundbox", "bluevine": "bluevine", "kabbage": "kabbage",
    "kapitus": "kapitus", "kap servicing": "kapitus", "kap servic": "kapitus", "kapitus servicin": "kapitus",
    "lcf": "lcf", "lcf group": "lcf", "ufce": "ufce", "parkside": "parkside",
    "mantis": "mantis", "everest": "everest", "fox": "fox", "fox funding": "fox",
    "mulligan": "mulligan", "pearl": "pearl", "itria": "itria",
    "stage": "stage", "vox": "vox", "vcg": "vcg", "vcg capital": "vcg", "vcg funding": "vcg",
    "headway": "headway", "credibly": "credibly", "libertas": "libertas",
    "yellowstone": "yellowstone", "fintegra": "fintegra", "fintegra llc": "fintegra", "fintegra funding": "fintegra",
    "mako": "mako", "mako funding": "mako", "mako capital": "mako",
    "olympus": "olympus", "olympus business capital": "olympus", "olympus capital": "olympus",
    "funding futures": "futures", "funding futures llc": "futures",
    "ram payment": "ram", "ram": "ram",
    "gmfunding": "gmfunding", "gm funding": "gmfunding", "gmfunding daily": "gmfunding", "gm funding daily": "gmfunding",
    "sq advance": "square", "daily sq advance": "square", "sq capital": "square", "sq loan": "square", "square capital": "square",
    "w funding": "wfunding", "daily w funding": "wfunding", "daily funding": "wfunding",
    "revenued": "revenued", "revenued llc": "revenued",
    "cfg": "cfg", "cfgmerchant": "cfg", "cfg merchant": "cfg", "cfgms": "cfg",
    "national funding": "national",
    "fora financial": "fora", "forafinancial": "fora",
    "ebf holdings": "ebf", "ebf": "ebf",
    "cobalt funding": "cobalt", "cobalt fund": "cobalt",
    "lendistry": "lendistry",
    "essentia": "essentia", "essentia funding": "essentia",
    "shopify capital": "shopify", "shopify": "shopify",
    "sba eid loan": "sba", "sba eidl loan": "sba", "sba loan": "sba",
    "lendingclub": "lendingclub", "lendingclub bank": "lendingclub",
    "celtic bank": "celtic",
    "kalamata": "kalamata", "kalamata capital": "kalamata",
    "mint funding": "mint",
    "rival funding": "rival",
    "honor capital": "honor",
    "legend advance": "legend",
    "bizfund": "bizfund",
    "kanmon": "kanmon",
    "fundomate": "fundomate", "fundomate techno": "fundomate",
    "capremium": "capremium", "capital premium": "capremium", "cap premium": "capremium",
    "acv capital": "acv", "acvcapital": "acv",
    "fratello": "fratello", "fratello funding": "fratello",
    "ascentra": "ascentra", "ascentra funding": "ascentra",
    "luminar": "luminar", "luminar funding": "luminar",
    "greenbridge": "greenbridge", "greenbridgecap": "greenbridge",
    "vitalcap": "vitalcap", "vitalcap fund": "vitalcap", "vital capital": "vitalcap",
    "pirs capital": "pirs",
    "zen funding": "zen",
    "altfunding": "alt", "alt funding": "alt",
    "stash capital": "stashcap", "stashcap": "stashcap",
    "intuit financing": "intuit", "webbank/intuit": "intuit",
    "american express loan": "amexloan",
  };

  function shortName(desc: string): string {
    const lower = desc.toLowerCase();
    for (const [key, short] of Object.entries(LENDER_SHORT_NAMES)) {
      if (lower.includes(key)) return short;
    }
    return lower.replace(/[^a-z]/g, "").slice(0, 8);
  }

  const lines = rawText.split("\n");
  const credits: { date: string; description: string; amount: number }[] = [];
  const creditKeywords = /\b(deposit|credit|transfer\s+from|incoming|wire\s+in|refund|mobile\s+deposit)\b/i;

  for (let li = 0; li < lines.length; li++) {
    const trimmed = lines[li].trim();
    if (!trimmed) continue;
    const dateMatch = trimmed.match(/^(\d{1,2})\/(\d{1,2})(?:\/(\d{2,4}))?\s+/);
    if (!dateMatch) continue;

    const desc = trimmed.slice(dateMatch[0].length);
    const lenderMatch = desc.match(fundingLenderPattern);
    if (!lenderMatch) continue;

    const debitIndicators = /\b(debit|payment|withdrawal|ach\s*debit|autopay|pymt|pmt|w\/d|check|deducted)\b/i;
    if (debitIndicators.test(desc) && !creditKeywords.test(desc)) continue;

    const windowText = [desc];
    for (let j = 1; j <= 2; j++) {
      if (li + j < lines.length) {
        const nextLine = lines[li + j].trim();
        if (nextLine && !/^\d{1,2}\/\d{1,2}/.test(nextLine)) {
          windowText.push(nextLine);
        } else break;
      }
    }
    const fullDesc = windowText.join(" ");
    const amounts = [...fullDesc.matchAll(/\$?([\d,]+\.\d{2})/g)].map(m => parseFloat(m[1].replace(/,/g, "")));
    const largeAmounts = amounts.filter(a => a >= 2500);
    if (largeAmounts.length === 0) continue;

    const fundedAmt = Math.max(...largeAmounts);
    const mo = parseInt(dateMatch[1]);
    const day = parseInt(dateMatch[2]);
    if (mo < 1 || mo > 12 || day < 1 || day > 31) continue;

    const monthAbbrs: Record<number, string> = { 1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec" };
    const yr = dateMatch[3] || "";
    const yrShort = yr.length === 4 ? yr.slice(2) : yr;
    const fundedDate = `${monthAbbrs[mo] || ""}${yrShort ? " " + (yr.length === 4 ? yr : "20" + yrShort) : ""}`;

    const matchedLenderKey = shortName(lenderMatch[0]);
    credits.push({ date: `${mo}/${day}`, description: fullDesc, amount: fundedAmt });

    let matchedLoan = false;
    for (const loan of analysis.loanDetails) {
      const loanKey = shortName(loan.lender || "");
      if (loanKey === matchedLenderKey || loanKey.includes(matchedLenderKey) || matchedLenderKey.includes(loanKey)) {
        if (!loan.fundedAmount || fundedAmt > (loan.fundedAmount || 0)) {
          (loan as any).fundedAmount = fundedAmt;
          (loan as any).funded_amount = fundedAmt;
          (loan as any).fundedDate = fundedDate;
          (loan as any).funded_date = fundedDate;
          // console.log(`[FundedScan] Matched credit $${fundedAmt.toLocaleString()} on ${mo}/${day} to lender "${loan.lender}" (${matchedLenderKey})`);
        }
        matchedLoan = true;
        break;
      }
    }
    if (!matchedLoan && fundedAmt >= 5000) {
      // console.log(`[FundedScan] Found $${fundedAmt.toLocaleString()} funding deposit from "${lenderMatch[0]}" on ${mo}/${day} — no existing loan entry, adding as discovered position`);
      analysis.loanDetails.push({
        lender: lenderMatch[0].trim(),
        amount: 0,
        frequency: "unknown",
        confidence: "medium",
        fundedAmount: fundedAmt,
        funded_amount: fundedAmt,
        fundedDate: fundedDate,
        funded_date: fundedDate,
        reasoning: `Funding deposit of $${fundedAmt.toLocaleString()} detected on ${mo}/${day} via raw text scan. No matching debit pattern found — may be paid off or recently funded.`,
      } as any);
    }
  }
}

interface ParsedDebit {
  date: string;
  description: string;
  amount: number;
  month: string;
}

function parseDebitsFromText(rawText: string): ParsedDebit[] {
  const debits: ParsedDebit[] = [];
  const lines = rawText.split("\n");
  const monthAbbrs: Record<number, string> = { 1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec" };

  const depositKeywords = /\b(deposit|credit|transfer\s+from|incoming|wire\s+in|interest\s+paid|refund|mobile\s+deposit)\b/i;
  const debitKeywords = /\b(debit|payment|autopay|ach\s+debit|withdrawal|scd\s+debit|dbcrd|debit\s+pos|debit\s+pur|check\s+paid|ach\s+pymt|autopay)\b/i;

  let currentYear = "";
  const yearMatch = rawText.match(/(?:20\d{2})/);
  if (yearMatch) currentYear = yearMatch[0];

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const pipeMatch = line.match(/^\s*(\d{1,2}\/\d{1,2}(?:\/\d{2,4})?)\s*\|\s*(.+?)\s*\|\s*\$?([\d,]+\.\d{2})/);
    if (pipeMatch) {
      const dateParts = pipeMatch[1].match(/^(\d{1,2})\/(\d{1,2})(?:\/(\d{2,4}))?/);
      if (dateParts) {
        const mo = parseInt(dateParts[1]);
        const day = parseInt(dateParts[2]);
        const yr = dateParts[3] || currentYear;
        if (mo >= 1 && mo <= 12 && day >= 1 && day <= 31) {
          const desc = pipeMatch[2].trim();
          const amount = parseFloat(pipeMatch[3].replace(/,/g, ""));
          if (desc && amount > 0 && amount < 500000) {
            if (!(depositKeywords.test(desc) && !debitKeywords.test(desc))) {
              const yrShort = (yr.length === 4 ? yr.slice(2) : yr) || currentYear.slice(2);
              const fullDate = `${String(mo).padStart(2,"0")}/${String(day).padStart(2,"0")}/${yrShort}`;
              debits.push({ date: fullDate, description: desc, amount, month: monthAbbrs[mo] || "" });
            }
          }
        }
      }
      continue;
    }

    let dateMatch = line.match(/^(\d{1,2})\/(\d{1,2})(?:\/(\d{2,4}))?\s+/);
    if (!dateMatch) {
      const noSpaceMatch = line.match(/^(\d{1,2})\/(\d{1,2})(?:\/(\d{2,4}))([A-Za-z])/);
      if (noSpaceMatch) {
        dateMatch = noSpaceMatch;
      }
    }
    if (!dateMatch) continue;

    const mo = parseInt(dateMatch[1]);
    const day = parseInt(dateMatch[2]);
    const yr = dateMatch[3] || currentYear;
    if (mo < 1 || mo > 12 || day < 1 || day > 31) continue;

    const dateLen = `${dateMatch[1]}/${dateMatch[2]}${dateMatch[3] ? `/${dateMatch[3]}` : ""}`.length;
    const afterDate = line.slice(dateLen).trim();

    const allAmounts = [...afterDate.matchAll(/\-?\$?([\d,]+\.\d{2})/g)].map(m => ({
      value: parseFloat(m[1].replace(/,/g, "")),
      index: m.index!,
      raw: m[0],
    })).filter(a => a.value > 0 && a.value < 500000);

    if (allAmounts.length === 0) continue;

    let txnAmount: number;
    if (allAmounts.length === 1) {
      txnAmount = allAmounts[0].value;
    } else if (allAmounts.length === 2) {
      txnAmount = allAmounts[0].value;
    } else {
      txnAmount = allAmounts[allAmounts.length - 2].value;
    }

    let desc = afterDate.replace(/\-?\$?[\d,]+\.\d{2}/g, "").trim();
    if (!desc) continue;

    if (i + 1 < lines.length) {
      const nextLine = lines[i + 1].trim();
      if (nextLine && !/^\d{1,2}\/\d{1,2}/.test(nextLine) && !/^\s*$/.test(nextLine)) {
        const isAddress = /^\d{1,5}\s+[A-Z].*\b(ST|AVE|BLVD|RD|DR|LN|CT|PL|WAY|CIR|PKWY|HWY|STE|APT)\b/i.test(nextLine) ||
          /\b[A-Z]{2}\s+\d{5}\b/.test(nextLine) ||
          /^\d{3}-\d{3}-\d{4}/.test(nextLine);
        const isStructural = /^(total|balance|summary|continued|page\s*\d|deposits?\s+this\s+period|debits?\s+this\s+period|credits?\s+this\s+period|statement\s+ending|checking\s+account)/i.test(nextLine);
        if (!isAddress && !isStructural && nextLine.length <= 60 && /[A-Za-z]{3,}/.test(nextLine)) {
          desc = desc + " " + nextLine.replace(/\s*#\d+\s*$/, "").trim();
        }
      }
    }

    if (depositKeywords.test(desc) && !debitKeywords.test(desc)) continue;

    const yrShort = yr.length === 4 ? yr.slice(2) : yr;
    const fullDate = `${String(mo).padStart(2,"0")}/${String(day).padStart(2,"0")}/${yrShort}`;
    debits.push({ date: fullDate, description: desc, amount: txnAmount, month: monthAbbrs[mo] || "" });
  }

  return debits;
}

async function detectMissedRecurringDebits(rawText: string, analysis: AnalysisResult): Promise<any[]> {
  const debits = parseDebitsFromText(rawText);
  if (debits.length < 5) return [];

  const existingLenders = new Set(
    (analysis.loanDetails || []).map((l: any) => normalizeLenderKey(l.lender || ""))
  );

  const amountGroups = new Map<number, ParsedDebit[]>();
  for (const d of debits) {
    const key = Math.round(d.amount * 100) / 100;
    if (!amountGroups.has(key)) amountGroups.set(key, []);
    amountGroups.get(key)!.push(d);
  }

  let lenderVerdicts: Map<string, { verdict: "confirmed" | "rejected"; notes?: string }>;
  try {
    lenderVerdicts = await getLenderVerdicts();
  } catch {
    lenderVerdicts = new Map();
  }

  const knownLenderPattern = /\b(ondeck|on\s*deck|odk\s*capital|kabbage|paypal\s*working|square\s*capital|sq\s*advance|bluevine|fundbox|can\s*capital|rapid\s*financ|credibly|libertas|yellowstone|pearl\s*capital|forward\s*fin|forwardfinusa|fora\s*financial|kalamata|kalamatacapital|national\s*funding|capytal|capitalize|capitalized\s*equipment|fox\s*fund|mantis|everest\s*business|cfg\s*merchant|cfgms|byline|mulligan|reliant|clearview|itria|cloudfund|navitas|ascentium|tvt\s*capital|united\s*capital|greenbox|world\s*business|biz2credit|lendio|fundation|celtic\s*bank|webbank|cross\s*river|breakout|headway|behalf|payability|newtek|smartbiz|vox\s*fund|wynwood|platinum\s*rapid|green\s*capital|qfs|jmb\s*capital|unique\s*fund|samson|kings\s*capital|fleetcor|stage\s*adv|7even|cashable|vitalcap|vital\s*capital|vcg|zen\s*funding|ace\s*funding|acg\s*llc|advancesyndicate|app\s*funding|arcadia\s*servic|arf\s*financial|arsenal\s*fund|aspire\s*fund|aspirefunding|biz\s*capital|bizfund|black\s*rok|breeze\s*advance|bretton|byzflex|byzfund|byzwash|canfield|capybara|clara\s*capital|compass\s*fund|crc\s*edge|credit\s*key|daytona\s*fund|diamond\s*advance|dlp\s*fund|ebf\s*holdings|eg\s*capital|elevate\s*fund|epic\s*advance|essential\s*fund|expansion\s*capital|expansion\s*ff|expansioncap|family\s*fund|fast\s*business\s*cash|fdm001|fdm|fenix\s*capital|figure\s*lending|forever\s*fund|fosforus|fresh\s*fund|fresh\s*percent|funding\s*metrics|gh\s*kapital|giggle\s*financ|gmfunding|gotorro|highland\s*hill|hightower\s*capital|hightowercapital|honor\s*capital|hwcrcvbls|i\s*got\s*funded|gotfunded|idea\s*247|idea\s*financial|ifund\s*expert|immediate\s*advance|immediate\s*capital|iou\s*central|kapitus|w\s+funding|lcf\s*group|lcf|legend\s*advance|lendbuzz|lending\s*servic|lendistry|lendr|lg\s*funding|liberty\s*fund|litefund|mca\s*servic|millstone|mr\s*advance|newco\s*capital|newport\s*business|nitro\s*advance|oak\s*capital|ocean\s*advance|olympus\s*business|one\s*river|orange\s*advance|overton|parkside\s*fund|path\s*2\s*capital|power\s*fund|premium\s*merchant|prosperum|prosperity\s*fund|ram\s*(?:payment|capital)|readycap|reboost|redwood\s*business|reliance\s*financial|retro\s*advance|revenued|rocket\s*capital|rocketcapitalny|samsonservic|sbfs|secure\s*account|servicing\s*by\s*kap|kap\s*servic|simply\s*fund|simply2|snap\s*financ|spartan\s*capital|specialty\s*capital|stellar\s*advance|suncoast\s*fund|swift\s*fund|tbf\s*group|the\s*fundworks|thefundworks|thoro\s*corp|top\s*choice|triton|trupath|tucker\s*albin|ufce|ufs|upfunding|vader|wave\s*advance|webfunder|westwood\s*fund|wide\s*merchant|zen\s*fund|pipe\s*capital|ssmb\s*financial|coast\s*fund|fintegra|altfunding|funding\s*futures|olympus\s*business|olympus\s*capital|mako\s*fund|main\s*street\s*group|travelers\s*suites|integra\s*fund|1\s*dc\s*fund|1st\s*alliance|fratello|ascentra|luminar|kif\s*fund|greenbridge|arbitrage|jrg\s*capital|aurum|pdm|pfg|stashcap|stash\s*cap|merchadv|merchant\s*adv|lily\s*fund|mckenzie|purpletree|purple\s*tree|lexio|global\s*fund|monetaria|trustify|bluetie|seamless\s*fund|liquidbee|belltower|palisade|marlin|xuper|fundfi|slim\s*fund|steady\s*fund|dib\s*capital)/i;
  const lenderNamePattern = /\b(capital|advance|funding|finance|lending|financial|finserv)\b/i;
  const equipmentPattern = /\b(equipment\s*financ|truck\s*financ|truck\s*leas|equipment\s*leas|commercial\s*vehicle|fleet\s*financ|m&t\s*equipment)\b/i;
  const notLoanPattern = /\b(insurance|utility|utilities|electric|gas\s+co|water|phone|internet|rent|lease|payroll|gusto|adp|paychex|tax|irs|stripe|square\s+processing|square\s+inc|sq\s*\*|clover|visa|mastercard|amex|discover|chase\s+card|capital\s+one\s+card|capital\s+one\s+online|capital\s+pmt|capital\s+payment|capital\s+one\s+pmt|chase\s+pmt|bank\s+pmt|zelle|venmo|cashapp|cash\s+app|apple\s+pay|google\s+pay|samsung\s+pay|wire\s+transfer|money\s+transfer|payment\s+sent|booster\s+fuel|gasoline|diesel|authorized\s+on|recurring\s+payment\s+authorized|online\s+payment|robinhood|coinbase|webull|e\s*trade|etrade|schwab|fidelity|td\s*ameritrade|ally\s*invest|acorns|sofi\s*invest|carmax\s*auto|allstate|geico|state\s*farm|fleetsmarts|clicklease|lease\s+services)\b/i;
  const lenderKeywordsForScan = /\b(capital|advance|funding|finance|lending|financial|finserv|loan|mca)\b/i;

  const missedLoans: any[] = [];

  for (const [amount, group] of amountGroups) {
    if (group.length < 3) continue;
    if (amount < 50 || amount > 100000) continue;

    const uniqueDates = new Set(group.map(d => d.date));
    if (uniqueDates.size < 3) continue;

    const descriptions = group.map(d => d.description);
    const commonDesc = findCommonPayee(descriptions);
    if (!commonDesc || commonDesc.length < 3) continue;

    if (/^(on|to|at|in|of|for|the|a|an|service\s*charge|monthly\s*fee|maintenance):?\s*$/i.test(commonDesc) || isGenericBankTerm(commonDesc)) continue;
    if (equipmentPattern.test(commonDesc)) continue;
    const bankInternalHardBlock = /\b(capital\s+pmt|capital\s+payment|capital\s+one\s+pmt|capital\s+one\s+online|capital\s+one\s+card|chase\s+pmt|bank\s+pmt|wells\s+fargo\s+pmt)\b/i;
    if (bankInternalHardBlock.test(commonDesc)) continue;
    if (notLoanPattern.test(commonDesc) && !lenderKeywordsForScan.test(commonDesc)) continue;

    const normalizedPayee = normalizeLenderKey(commonDesc);
    if (existingLenders.has(normalizedPayee)) continue;

    let alreadyDetected = false;
    for (const existing of existingLenders) {
      if (existing.length >= 3 && (normalizedPayee.includes(existing) || existing.includes(normalizedPayee))) {
        alreadyDetected = true;
        break;
      }
    }
    if (alreadyDetected) continue;

    let isKnownLender = knownLenderPattern.test(commonDesc);
    let hasLenderKeyword = lenderNamePattern.test(commonDesc);

    let learnedVerdict: string | null = null;
    if (!GENERIC_BANK_TERMS.test(commonDesc.trim())) {
      for (const [ruleKey, rule] of lenderVerdicts) {
        if (GENERIC_BANK_TERMS.test(ruleKey)) continue;
        if (ruleKey.length < 4) continue;
        if (normalizedPayee.includes(ruleKey) || ruleKey.includes(normalizedPayee)) {
          learnedVerdict = rule.verdict;
          break;
        }
      }
    }

    if (learnedVerdict === "rejected") continue;

    const sortedDates = [...uniqueDates].sort();
    let frequency: "daily" | "weekly" | "monthly" | null = null;

    const monthCounts = new Map<string, number>();
    for (const d of group) {
      const moKey = d.date.slice(0, 2);
      monthCounts.set(moKey, (monthCounts.get(moKey) || 0) + 1);
    }
    const avgPerMonth = group.length / Math.max(monthCounts.size, 1);

    const dateObjects = sortedDates.map(d => {
      const [mm, dd, yy] = d.split("/");
      const fullYr = parseInt(yy) < 50 ? 2000 + parseInt(yy) : 1900 + parseInt(yy);
      return new Date(fullYr, parseInt(mm) - 1, parseInt(dd));
    }).filter(d => !isNaN(d.getTime()));

    if (dateObjects.length >= 3) {
      const intervals: number[] = [];
      for (let j = 1; j < dateObjects.length; j++) {
        const diffDays = (dateObjects[j].getTime() - dateObjects[j - 1].getTime()) / (1000 * 60 * 60 * 24);
        if (diffDays > 0 && diffDays < 60) intervals.push(diffDays);
      }
      if (intervals.length >= 2) {
        const sortedIntervals = [...intervals].sort((a, b) => a - b);
        const medianInterval = sortedIntervals[Math.floor(sortedIntervals.length / 2)];
        const dailyCount = intervals.filter(g => g >= 0.5 && g <= 3).length;
        const weeklyCount = intervals.filter(g => g >= 5 && g <= 10).length;
        const monthlyCount = intervals.filter(g => g >= 25 && g <= 35).length;
        if (medianInterval <= 2 && dailyCount >= intervals.length * 0.6) frequency = "daily";
        else if (medianInterval >= 5 && medianInterval <= 10 && weeklyCount >= intervals.length * 0.5) frequency = "weekly";
        else if (medianInterval >= 25 && medianInterval <= 35 && monthlyCount >= intervals.length * 0.5) frequency = "monthly";
        else if (medianInterval <= 3 && dailyCount >= 3) frequency = "daily";
        else if (medianInterval <= 10) frequency = "weekly";
        else if (medianInterval >= 20) frequency = "monthly";
      }
    }

    if (!frequency) {
      if (avgPerMonth >= 15) frequency = "daily";
      else if (avgPerMonth >= 3 && avgPerMonth <= 6) frequency = "weekly";
      else if (avgPerMonth >= 1 && avgPerMonth < 3) frequency = "monthly";
    }

    if (!frequency) continue;

    const shouldAdd = isKnownLender || learnedVerdict === "confirmed" || hasLenderKeyword ||
      (frequency === "daily" && group.length >= 10) ||
      (frequency === "weekly" && group.length >= 3 && /autopay|ach|pymt|payment|debit/i.test(commonDesc)) ||
      (frequency === "monthly" && group.length >= 3 && isKnownLender);

    if (!shouldAdd) continue;

    const confidence = isKnownLender || learnedVerdict === "confirmed" ? "high" : hasLenderKeyword ? "medium" : "low";

    // console.log(`[PostAI-Scan] Found missed recurring debit: "${commonDesc}" $${amount} x${group.length} (${frequency}, ${avgPerMonth.toFixed(1)}/mo, confidence=${confidence})`);

    missedLoans.push({
      lender: commonDesc,
      amount,
      frequency,
      occurrences: group.length,
      fundedAmount: null,
      fundedDate: null,
      account: (analysis as any).accountNumber || "",
      confidence,
      reasoning: `[Programmatic scan] Found ${group.length} recurring debits of exactly $${amount.toLocaleString("en-US", { minimumFractionDigits: 2 })} to "${commonDesc}". Pattern: ${frequency} (~${avgPerMonth.toFixed(1)}/month). Dates: ${sortedDates.join(", ")}. ${learnedVerdict === "confirmed" ? "Confirmed as loan by AI Learning rules." : ""}`,
    });
  }

  const allExistingLenders = new Set([
    ...existingLenders,
    ...missedLoans.map(l => normalizeLenderKey(l.lender || "")),
  ]);
  const lenderNameScan = /\b(vox\s*funding|fox\s*capital|mantis\s*funding|everest\s*business|ondeck|on\s*deck|kabbage|bluevine|fundbox|credibly|libertas|yellowstone|pearl\s*capital|forward\s*fin|forwardfinusa|fora\s*financial|kalamata|national\s*funding|capytal|capitalize|cfg\s*merchant|cfgms|mulligan|reliant|clearview|itria|cloudfund|navitas|tvt\s*capital|united\s*capital|greenbox|wynwood|platinum\s*rapid|qfs|jmb\s*capital|unique\s*funding|samson|kings\s*capital|stage\s*adv|stage\s*funding|7even|cashable|vitalcap|vital\s*capital|vcg\s*capital|vcg\s*funding|zen\s*funding|fintegra|altfunding|alt\s*funding|funding\s*futures|olympus\s*business|olympus\s*capital|mako\s*fund)\b/i;

  for (const d of debits) {
    const match = d.description.match(lenderNameScan);
    if (!match) continue;

    const lenderName = match[0].trim();
    const normalizedLender = normalizeLenderKey(lenderName);

    let alreadyCovered = false;
    for (const existing of allExistingLenders) {
      if (existing.length >= 3 && (normalizedLender.includes(existing) || existing.includes(normalizedLender))) {
        alreadyCovered = true;
        break;
      }
    }
    if (alreadyCovered) continue;

    const sameNameDebits = debits.filter(dd => {
      const m = dd.description.match(lenderNameScan);
      return m && normalizeLenderKey(m[0].trim()) === normalizedLender;
    });

    if (sameNameDebits.length < 2) continue;

    const amounts = sameNameDebits.map(dd => Math.round(dd.amount * 100) / 100);
    const modeMap: Record<number, number> = {};
    for (const a of amounts) modeMap[a] = (modeMap[a] || 0) + 1;
    let modeAmt = 0, modeCount = 0;
    for (const [k, v] of Object.entries(modeMap)) {
      if (v > modeCount) { modeAmt = parseFloat(k); modeCount = v; }
    }

    const sameAmountDebits = sameNameDebits.filter(dd => Math.round(dd.amount * 100) / 100 === modeAmt);
    if (sameAmountDebits.length < 2) continue;

    const uniqueDates = [...new Set(sameAmountDebits.map(dd => dd.date))];
    const avgPerMo = sameAmountDebits.length / Math.max(new Set(sameAmountDebits.map(dd => dd.date.slice(0, 2))).size, 1);

    let freq: "daily" | "weekly" | "monthly" | null = null;
    if (avgPerMo >= 15) freq = "daily";
    else if (avgPerMo >= 3 && avgPerMo <= 6) freq = "weekly";
    else if (sameAmountDebits.length >= 3 && avgPerMo >= 3) freq = "weekly";
    else if (sameAmountDebits.length >= 3 && avgPerMo >= 1) freq = "monthly";

    if (!freq) continue;

    allExistingLenders.add(normalizedLender);
    // console.log(`[PostAI-LenderScan] Found lender by name: "${lenderName}" $${modeAmt} x${sameAmountDebits.length} (${freq})`);

    missedLoans.push({
      lender: lenderName,
      amount: modeAmt,
      frequency: freq,
      occurrences: sameAmountDebits.length,
      fundedAmount: null,
      fundedDate: null,
      account: (analysis as any).accountNumber || "",
      confidence: "high",
      reasoning: `[Lender-name scan] Found "${lenderName}" in ${sameNameDebits.length} debit description(s). ${sameAmountDebits.length} payments of exactly $${modeAmt.toLocaleString("en-US", { minimumFractionDigits: 2 })}. Pattern: ${freq} (~${avgPerMo.toFixed(1)}/month). Known MCA/lending company.`,
    });
  }

  return missedLoans;
}

function findCommonPayee(descriptions: string[]): string | null {
  if (descriptions.length === 0) return null;

  const cleanDesc = (d: string) => d.replace(/\d{1,2}\/\d{1,2}(\/\d{2,4})?\s*/g, "").replace(/\s+/g, " ").trim();

  const cleaned = descriptions.map(cleanDesc);

  const wordCounts = new Map<string, number>();
  for (const desc of cleaned) {
    const words = desc.split(/\s+/);
    const seen = new Set<string>();
    for (const w of words) {
      const key = w.toUpperCase();
      if (key.length < 2 || seen.has(key)) continue;
      seen.add(key);
      wordCounts.set(key, (wordCounts.get(key) || 0) + 1);
    }
  }

  const threshold = descriptions.length * 0.6;
  const commonWords: string[] = [];
  for (const [word, count] of wordCounts) {
    if (count >= threshold) commonWords.push(word);
  }

  if (commonWords.length === 0) return null;

  const firstDesc = cleaned[0].split(/\s+/);
  const orderedCommon: string[] = [];
  for (const w of firstDesc) {
    if (commonWords.includes(w.toUpperCase())) orderedCommon.push(w);
  }

  const result = orderedCommon.slice(0, 6).join(" ").trim();
  const skipWords = /^(SCD|DBCRD|ACH|PUR|DEBIT|AP|AUT|POS|NET)$/i;
  const meaningful = orderedCommon.filter(w => !skipWords.test(w));

  return meaningful.length > 0 ? meaningful.slice(0, 5).join(" ").trim() : null;
}

export async function analyzeSingleLead(leadId: number, options?: { onlyNew?: boolean; forceLegacy?: boolean }): Promise<{
  analysis: AnalysisResult;
  savedAnalysis: any;
  confirmations: any[];
  skippedCount?: number;
}> {
  const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
  if (!lead) throw new Error("Lead not found");

  const trace = new ScrubTrace(leadId, (lead as any).businessName || `Lead#${leadId}`);
  activeScrubTraces.set(leadId, trace);
  currentActiveLeadId = leadId;
  trace.info("START", `Beginning scrub for "${(lead as any).businessName || leadId}"`);

  const structuredTrace = new StructuredTraceCollector(leadId, 0);
  activeStructuredTraces.set(leadId, structuredTrace);
  // console.log(`\n${"=".repeat(80)}`);
  scrubLog("SCRUB-START", `Lead ${leadId} "${(lead as any).businessName}" — ${new Date().toISOString()}`);
  // console.log(`${"=".repeat(80)}`);

  const documents = await db.select().from(documentsTable)
    .where(eq(documentsTable.leadId, leadId));
  let bankStatements = documents.filter(d => d.type === "bank_statement");
  if (bankStatements.length === 0) throw new Error("No bank statements found");
  trace.info("DOCS", `Found ${bankStatements.length} bank statement(s): ${bankStatements.map(d => d.name).join(", ")}`);
//   scrubLog("SCRUB-DOCS", `Lead ${leadId}: ${documents.length} total documents, ${bankStatements.length} bank statements`);
  for (const doc of bankStatements) {
//     scrubLog("SCRUB-DOCS", `  → doc.id=${doc.id} "${doc.name}" type=${doc.type} storageKey=${doc.storageKey || "none"} url=${doc.url?.slice(0, 80)}`);
  }

  let skippedCount = 0;
  const existingAnalyses = await db.select().from(bankStatementAnalysesTable)
    .where(eq(bankStatementAnalysesTable.leadId, leadId));

  if (options?.onlyNew && existingAnalyses.length > 0) {
    const latestAnalysisTime = Math.max(...existingAnalyses.map(a => new Date(a.createdAt).getTime()));
    const newDocs = bankStatements.filter(d => new Date(d.createdAt).getTime() > latestAnalysisTime);
    if (newDocs.length === 0) {
      throw new Error("No new bank statements to analyze — all documents have already been scrubbed");
    }
    skippedCount = bankStatements.length - newDocs.length;
//     scrubLog("Analysis", `Lead ${leadId}: onlyNew mode — analyzing ${newDocs.length} new doc(s), skipping ${skippedCount} already-analyzed`);
    bankStatements = newDocs;
  }

  if (!_cachedLearningContext || Date.now() - _cachedLearningContextAt > 60000) {
    _cachedLearningContext = await getLearningContext();
    _cachedLearningContextAt = Date.now();
  }
  const fullPrompt = ANALYSIS_PROMPT + _cachedLearningContext;

  const HEADER_OVERHEAD = 500;
  const PER_DOC_WRAPPER_OVERHEAD = 200;
  const MAX_CHARS_PER_BATCH = 160000 - HEADER_OVERHEAD;
  const batches: typeof bankStatements[] = [];
  let currentBatch: typeof bankStatements = [];
  let currentBatchSize = 0;

  const docTexts = new Map<number, string>();
//   scrubLog("Analysis", `Using pdfplumber + structured parser pipeline`);

  const pdfDocs: { doc: typeof bankStatements[0]; filePath: string; fileName: string }[] = [];
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "batch-extract-"));
  let cachedCount = 0;

  for (const doc of bankStatements) {
    const cachedText = (doc as any).cachedRawText;
    const hasColumnDetection = cachedText && /\S\s{2,}\d{1,3}(?:,\d{3})*\.\d{2}/.test(cachedText.slice(0, 5000));

    if (cachedText && cachedText.length > 100 && hasColumnDetection) {
      let text = normalizePdfAmounts(cachedText);
      if (isTextGarbled(text)) {
        try {
          await db.update(documentsTable).set({ cachedRawText: null }).where(eq(documentsTable.id, doc.id));
        } catch {}
      } else {
        const header = text.slice(0, 2000).toLowerCase();
        if (/asset\s+report/i.test(header) && !/(?:statement\s+period|account\s+summary|beginning\s+balance|ending\s+balance)/i.test(header)) {
          continue;
        }
        if (text.length > 100) {
          try {
            const parsed = parseStatementData(text);
            console.log("[Parsed Statement]", parsed);
            const formatted = formatParsedDataForPrompt(parsed);
            if (formatted) {
              text = text + "\n" + formatted;
            }
          } catch {}
        }
        docTexts.set(doc.id, text);
        cachedCount++;
        continue;
      }
    } else if (cachedText && cachedText.length > 100) {
      console.log(`[SCRUB-CACHE] doc.id=${doc.id} "${doc.name}": clearing old cached text, will re-extract with column detection`);
      try {
        await db.update(documentsTable).set({ cachedRawText: null }).where(eq(documentsTable.id, doc.id));
      } catch {}
    }
    const result = await getBufferFromDocAsync(doc.url, doc.storageKey);
    if (!result) {
//       scrubLog("SCRUB-FILE", `doc.id=${doc.id} "${doc.name}": FILE NOT ACCESSIBLE — storageKey=${doc.storageKey ? "present" : "none"}, url=${doc.url ? "present" : "missing"}`);
      continue;
    }
//     scrubLog("SCRUB-FILE", `doc.id=${doc.id} "${doc.name}": loaded file buffer (${result.buffer.length} bytes), fileName="${result.fileName}"`);
    const ext = path.extname(result.fileName).toLowerCase();
    if (ext === ".pdf") {
      const safeName = result.fileName.replace(/[^a-zA-Z0-9._-]/g, "_");
      const tmpPath = path.join(tmpDir, `${doc.id}_${safeName}`);
      fs.writeFileSync(tmpPath, result.buffer);
      pdfDocs.push({ doc, filePath: tmpPath, fileName: result.fileName });
    } else if (ext === ".csv" || ext === ".txt") {
      const csvText = result.buffer.toString("utf-8").slice(0, 50000);
      docTexts.set(doc.id, csvText);
      try {
        await db.update(documentsTable).set({ cachedRawText: csvText }).where(eq(documentsTable.id, doc.id));
      } catch {}
    }
  }

  if (cachedCount > 0) {
//     scrubLog("Analysis", `Used cached text for ${cachedCount}/${bankStatements.length} documents (skipping OCR)`);
  }

  if (pdfDocs.length > 0) {
    const batchFiles = pdfDocs.map(p => ({ key: String(p.doc.id), filePath: p.filePath, fileName: p.fileName }));
//     scrubLog("SCRUB-EXTRACT", `Batch-extracting ${pdfDocs.length} PDFs with pdfplumber: ${pdfDocs.map(p => `"${p.fileName}"`).join(", ")}`);
    const batchResults = await extractBatchWithPdfplumber(batchFiles);

    for (const p of pdfDocs) {
      let text = batchResults.get(String(p.doc.id)) || "";
//       scrubLog("SCRUB-EXTRACT", `doc.id=${p.doc.id} "${p.doc.name}": extracted ${text.length} chars, method=${text.slice(0, 50).replace(/\n/g, " ")}`);
      if (text.startsWith("[OCR error:") || text === "[OCR produced no text]" || text === "[OCR: pdftoppm produced no images]") {
//         scrubLog("SCRUB-EXTRACT", `doc.id=${p.doc.id} "${p.doc.name}": EXTRACTION FAILED — ${text}`);
        continue;
      }
      const header = text.slice(0, 2000).toLowerCase();
      if (/asset\s+report/i.test(header) && !/(?:statement\s+period|account\s+summary|beginning\s+balance|ending\s+balance)/i.test(header)) {
//         scrubLog("SCRUB-EXTRACT", `doc.id=${p.doc.id} "${p.doc.name}": SKIPPED — Asset Report`);
        continue;
      }
      try {
        await db.update(documentsTable).set({ cachedRawText: text }).where(eq(documentsTable.id, p.doc.id));
//         scrubLog("SCRUB-EXTRACT", `doc.id=${p.doc.id} "${p.doc.name}": cached raw text saved to DB`);
      } catch {}
      text = normalizePdfAmounts(text);
      if (text.length > 100) {
        try {
          const parsed = parseStatementData(text);
          const formatted = formatParsedDataForPrompt(parsed);
          if (formatted) {
//             scrubLog("SCRUB-PARSER", `doc.id=${p.doc.id} "${p.fileName}": appended structured parsed data (${formatted.length} chars)`);
            text = text + "\n" + formatted;
          } else {
//             scrubLog("SCRUB-PARSER", `doc.id=${p.doc.id} "${p.fileName}": structured parser returned no data`);
          }
        } catch (e: any) {
          // console.warn(`[SCRUB-PARSER] doc.id=${p.doc.id} "${p.fileName}": structured parsing FAILED — ${e.message}`);
        }
      }
      docTexts.set(p.doc.id, text);
    }
  }

  try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}

  scrubLog("Re-Scrub", `▶ STEP 3/7 DONE: Text ready for ${docTexts.size}/${bankStatements.length} docs (${cachedCount} cached, ${pdfDocs.length} freshly extracted)`);

  for (const doc of bankStatements) {
    const text = docTexts.get(doc.id);
    if (!text) continue;
    const detectedBank = identifyBank(text);
    const isOcr = /^\[(?:OCR|PDF OCR)/i.test(text.trim());
    const pageCount = (text.match(/--- Page \d+/gi) || []).length || 1;
    if (!structuredTrace.stageA.detectedBankName && detectedBank) {
      structuredTrace.stageA.detectedBankName = detectedBank.name;
    }
    structuredTrace.stageA.pdfType = isOcr ? "scanned" : "text";
    structuredTrace.stageA.pageCount += pageCount;

    const acctMatch = text.match(/(?:account|acct)\s*(?:#|number|no\.?)?\s*[:.]?\s*\*{0,}(\d{4})/i);
    if (acctMatch) {
      structuredTrace.stageA.accountNumberCandidates.push({
        value: acctMatch[1], source: `doc:${doc.id}`, selected: true, reasoning: "regex match",
      });
    }

    const periodMatch = text.match(/(?:statement\s+period|for\s+the\s+period)\s*[:\s]*(\w+\s+\d{1,2}(?:,?\s*\d{4})?)\s*(?:to|-|through)\s*(\w+\s+\d{1,2}(?:,?\s*\d{4})?)/i);
    if (periodMatch) {
      structuredTrace.stageA.statementPeriodCandidates.push({
        value: `${periodMatch[1]} - ${periodMatch[2]}`, source: `doc:${doc.id}`, selected: true, reasoning: "period regex",
      });
    }

    structuredTrace.stageB.pages.push({
      pageNumber: 1,
      rawTextLength: text.length,
      ocrUsed: isOcr,
      ocrFallback: isOcr,
      snippets: {
        accountNumber: acctMatch ? acctMatch[0].slice(0, 100) : undefined,
        statementPeriod: periodMatch ? periodMatch[0].slice(0, 100) : undefined,
      },
    });
  }

  const docsToSplit: typeof bankStatements = [];
  for (const doc of bankStatements) {
    if (!doc.name.toLowerCase().endsWith(".pdf")) continue;
    const text = docTexts.get(doc.id);
    if (!text) continue;
    const acctLast4s = extractAllAccountLast4s(text);
    if (acctLast4s.length > 1) {
//       scrubLog("Analysis", `Multi-account PDF detected: "${doc.name}" has accounts: ${acctLast4s.join(", ")}`);
      docsToSplit.push(doc);
    }
  }

  if (docsToSplit.length > 0) {
    let splitCounter = 0;
    for (const doc of docsToSplit) {
      try {
        let buffer: Buffer;
        const normalized = doc.url.startsWith("/") ? doc.url.slice(1) : doc.url;
        const filePath = path.resolve(process.cwd(), normalized);
        const uploadsRoot = path.resolve(process.cwd(), "uploads");

        if (filePath.startsWith(uploadsRoot) && fs.existsSync(filePath)) {
          buffer = fs.readFileSync(filePath);
        } else if (doc.storageKey) {
          const { getFileFromStorage } = await import("../../utils/fileStorage");
          const result = await getFileFromStorage(doc.storageKey);
          buffer = result.buffer;
        } else continue;

        const { split, newDocs } = await splitMultiAccountPdf(buffer, {
          id: doc.id, name: doc.name, leadId: doc.leadId, url: doc.url, storageKey: doc.storageKey,
        });

        if (split && newDocs.length > 1) {
          const uploadsDir = path.join(process.cwd(), "uploads", "bank_statements");
          if (!fs.existsSync(uploadsDir)) fs.mkdirSync(uploadsDir, { recursive: true });

          const insertedDocs: { id: number; text: string; name: string }[] = [];
          let allInserted = true;

          for (const newDoc of newDocs) {
            splitCounter++;
            const safeName = newDoc.name.replace(/[^a-zA-Z0-9._-]/g, "_");
            const newPath = path.join(uploadsDir, `${Date.now()}_${splitCounter}_${safeName}`);
            fs.writeFileSync(newPath, newDoc.buffer);
            const relUrl = path.relative(process.cwd(), newPath);

            try {
              const [inserted] = await db.insert(documentsTable).values({
                leadId: doc.leadId,
                name: newDoc.name,
                url: relUrl,
                type: "bank_statement",
              }).returning();

              const text = await extractTextFromDocument(relUrl, null);
              insertedDocs.push({ id: inserted.id, text, name: newDoc.name });
            } catch (insertErr: any) {
              // console.error(`[PDF Split] Failed to insert ${newDoc.name}:`, insertErr.message);
              allInserted = false;
              break;
            }
          }

          if (allInserted && insertedDocs.length === newDocs.length) {
            for (const ins of insertedDocs) {
              docTexts.set(ins.id, ins.text);
              bankStatements.push({ id: ins.id, leadId: doc.leadId, name: ins.name, url: "", type: "bank_statement" } as any);
              // console.log(`[PDF Split] Saved split doc: ${ins.name} (id=${ins.id})`);
            }
            bankStatements = bankStatements.filter(d => d.id !== doc.id);
            docTexts.delete(doc.id);
            await db.delete(documentsTable).where(eq(documentsTable.id, doc.id));
            // console.log(`[PDF Split] Removed original multi-account doc: ${doc.name} (id=${doc.id})`);
          } else {
            // console.warn(`[PDF Split] Partial insert for ${doc.name}, keeping original`);
          }
        }
      } catch (e: any) {
        // console.error(`[PDF Split] Error processing ${doc.name}:`, e.message);
      }
    }
  }

  if (lead.businessName && bankStatements.length > 1) {
    const bizName = lead.businessName.toLowerCase().replace(/[^a-z0-9\s]/g, "").trim();
    const bizNorm = bizName.replace(/\s+/g, "");
    const bizWords = bizName.split(/\s+/).filter(w => w.length > 1 && !["llc","inc","corp","co","ltd","dba","the","and","of"].includes(w));
    const significantWords = bizWords.filter(w => w.length >= 3);

    const verified: typeof bankStatements = [];
    const rejected: string[] = [];

    for (const doc of bankStatements) {
      const text = docTexts.get(doc.id);
      if (!text) { verified.push(doc); continue; }

      const textNorm = text.toLowerCase().replace(/[^a-z0-9\s]/g, "").replace(/\s+/g, " ");
      const textCompact = textNorm.replace(/\s+/g, "");

      if (textCompact.includes(bizNorm)) {
        verified.push(doc);
        continue;
      }

      if (significantWords.length >= 2) {
        const wordsFound = significantWords.filter(w => textNorm.includes(w));
        if (wordsFound.length >= Math.ceil(significantWords.length * 0.7) && wordsFound.length >= 2) {
          verified.push(doc);
          continue;
        }
      }

      const fnNorm = doc.name.toLowerCase().replace(/[^a-z0-9]/g, "");
      if (fnNorm.includes(bizNorm) || bizNorm.includes(fnNorm.replace(/\d+/g, "").replace(/bnkstmtom/g, "").replace(/pdf$/g, "").replace(/compressed$/g, "").trim())) {
        verified.push(doc);
        continue;
      }

      rejected.push(doc.name);
    }

    if (verified.length > 0 && rejected.length > 0) {
//       scrubLog("SCRUB-VERIFY", `Lead ${leadId}: Content-verified ${verified.length}/${bankStatements.length} docs for "${lead.businessName}"`);
//       scrubLog("SCRUB-VERIFY", `  Verified: ${verified.map(d => d.name).join(", ")}`);
//       scrubLog("SCRUB-VERIFY", `  Rejected: ${rejected.join(", ")}`);
      bankStatements = verified;
    } else {
//       scrubLog("SCRUB-VERIFY", `Lead ${leadId}: All ${bankStatements.length} docs verified for "${lead.businessName}"`);
    }
  }

  scrubLog("Re-Scrub", `▶ STEP 4/7: Verifying documents & grouping by account`);
  const readableDocs = bankStatements.filter(d => docTexts.has(d.id));
//   scrubLog("SCRUB-READABLE", `${readableDocs.length}/${bankStatements.length} docs have readable text`);
  if (readableDocs.length === 0) throw new Error("No bank statements could be read");

  const acctGroups = new Map<string, typeof readableDocs>();
  for (const doc of readableDocs) {
    const text = docTexts.get(doc.id) || "";
    let acct = extractAccountNumberFromText(text);
    if (!acct) {
      acct = extractAccountFromFilename(doc.name);
      if (acct) {
//         scrubLog("Analysis", `Account from filename for "${doc.name}": *${acct}`);
      }
    } else {
//       scrubLog("Analysis", `Account from text for "${doc.name}": *${acct}`);
    }
    acct = acct || "unknown";
    if (!acctGroups.has(acct)) acctGroups.set(acct, []);
    acctGroups.get(acct)!.push(doc);
  }

  if (acctGroups.size > 1) {
//     scrubLog("Analysis", `Lead ${leadId}: Found ${acctGroups.size} accounts: ${Array.from(acctGroups.entries()).map(([k, v]) => `**${k} (${v.length} docs)`).join(", ")}`);
    for (const [acct, docs] of acctGroups) {
      let groupBatch: typeof readableDocs = [];
      let groupBatchSize = 0;
      for (const doc of docs) {
        const textLen = docTexts.get(doc.id)!.length + PER_DOC_WRAPPER_OVERHEAD;
        if (groupBatchSize + textLen > MAX_CHARS_PER_BATCH && groupBatch.length > 0) {
          batches.push(groupBatch);
          groupBatch = [];
          groupBatchSize = 0;
        }
        groupBatch.push(doc);
        groupBatchSize += textLen;
      }
      if (groupBatch.length > 0) batches.push(groupBatch);
    }
  } else {
    for (const doc of readableDocs) {
      const textLen = docTexts.get(doc.id)!.length + PER_DOC_WRAPPER_OVERHEAD;
      if (currentBatchSize + textLen > MAX_CHARS_PER_BATCH && currentBatch.length > 0) {
        batches.push(currentBatch);
        currentBatch = [];
        currentBatchSize = 0;
      }
      currentBatch.push(doc);
      currentBatchSize += textLen;
    }
    if (currentBatch.length > 0) batches.push(currentBatch);
  }

  scrubLog("Re-Scrub", `▶ STEP 4/7 DONE: ${readableDocs.length} docs → ${acctGroups.size} account group(s) → ${batches.length} AI batch(es)`);
//   scrubLog("SCRUB-BATCH", `Lead ${leadId}: ${readableDocs.length} readable docs → ${acctGroups.size} account group(s) → ${batches.length} AI batch(es)`);
  for (let bi = 0; bi < batches.length; bi++) {
//     scrubLog("SCRUB-BATCH", `  Batch ${bi + 1}: ${batches[bi].map(d => `"${d.name}" (id=${d.id})`).join(", ")}`);
  }

  const DOC_CONCURRENCY = 3;
  const batchResults: { idx: number; analysis: AnalysisResult; savedAnalysis: any; confirmations: any[] }[] = [];
  let failedBatchCount = 0;

  scrubLog("Re-Scrub", `▶ STEP 5/7: Running AI analysis on ${batches.length} batch(es)...`);
  const indexedBatches = batches.map((batch, idx) => ({ batch, idx }));
  const batchQueue = [...indexedBatches];

  const processBatch = async () => {
    while (batchQueue.length > 0) {
      const item = batchQueue.shift();
      if (!item) break;
      const { batch, idx } = item;
      const label = `[Batch ${idx + 1}/${batches.length}]`;
      try {
        scrubLog("Analysis", `${label} Processing ${batch.length} doc(s): ${batch.map(d => d.name).join(", ")}`);
        trace.info("BATCH", `Processing batch ${idx + 1}/${batches.length}: ${batch.map(d => d.name).join(", ")}`);
        const { analysis, aiResponse, rawText } = await analyzeDocumentBatch(lead, batch, docTexts, label, fullPrompt, structuredTrace);

        const revCount = (analysis.monthlyRevenues || []).length;
        const loanCount = (analysis.loanDetails || []).length;
        trace.info("AI_RESULT", `AI returned: ${revCount} month(s) revenue, ${loanCount} loan(s), hasLoans=${analysis.hasLoans}`);
        for (const mr of (analysis.monthlyRevenues || [])) {
          const rev = mr.revenue ?? mr.rev ?? 0;
          trace.info("REVENUE", `${mr.month} acct=${mr.account || "?"}: $${rev.toLocaleString()}${mr.needsReview ? " [NEEDS REVIEW]" : ""}`);
        }
        for (const loan of (analysis.loanDetails || [])) {
          trace.info("LOAN", `"${loan.lender}" $${loan.amount || 0} ${loan.frequency || "?"} confidence=${loan.confidence || "?"} occurrences=${loan.occurrences || "?"}`);
        }

        const sanitize = (s: string | null | undefined) => s ? s.replace(/\x00/g, "") : null;

        const docId = batch[0]?.id;

        const PER_DOC_NOT_LOAN = /\b(online\s*(?:banking|transfer)|payment\s+to\b|transfer\s+to\s+(?:sav|chk|checking|savings)|apple\s+cash|self\s+financial|intuit\s+financ|isuzu\s+financ|styku|next\s+insur|pmnt\s+sent|bkofamerica|ascentium\s*capital|leasechg|lease\s+pymt|loan\s+pymt|td\s+auto|orig\s+co\b|autopay\s+to|credit\s*card|card\s+ending|insurance|payroll|adp\b|gusto\b|paychex|irs\b|rent\b|lease\b|utility|electric|daily\s+sq\s+advance|sq\s+advance\b|ally\s+id:|withdrwl\b|orig\s+co\s+finance|figure\s+lending)\b/i;
        const docLoanDetails = (analysis.loanDetails || []).filter((loan: any) => {
          if (!loan.amount || loan.amount <= 0) return false;
          const name = (loan.lender || "").trim();
          if (name.length < 3) return false;
          if (PER_DOC_NOT_LOAN.test(name)) return false;
          if (/^\$[\d,.]+\s/.test(name)) return false;
          if (loan.amount < 25) return false;
          if (loan.frequency === "daily" && loan.amount > 15000) return false;
          if (loan.frequency === "weekly" && loan.amount > 25000) return false;
          if (loan.frequency === "monthly" && loan.amount > 50000) return false;
          return true;
        });

        const analysisValues = {
          leadId,
          documentId: docId,
          hasLoans: docLoanDetails.length > 0,
          hasOnDeck: docLoanDetails.some((l: any) => /ondeck/i.test(l.lender || "")),
          loanDetails: docLoanDetails,
          monthlyRevenues: analysis.monthlyRevenues || [],
          avgDailyBalance: String(Number(analysis.avgDailyBalance) || 0),
          revenueTrend: analysis.revenueTrend || "stable",
          riskFactors: analysis.riskFactors || [],
          riskScore: analysis.riskScore || "B1",
          grossRevenue: String(Number(analysis.grossRevenue) || 0),
          negativeDays: analysis.negativeDays || [],
          nsfCount: Number(analysis.nsfCount) || 0,
          hasExistingLoans: docLoanDetails.length > 0,
          bankName: analysis.bankName || null,
          accountNumber: analysis.accountNumber || null,
          businessNameOnStatement: analysis.businessNameOnStatement || null,
          statementMonth: (analysis.monthlyRevenues && analysis.monthlyRevenues.length > 0) 
            ? analysis.monthlyRevenues[0].month 
            : null,
          aiRawAnalysis: sanitize(aiResponse),
          extractedStatementText: sanitize(rawText),
        };

        let savedAnalysis;
        if (docId) {
          const [upserted] = await db.transaction(async (tx) => {
            await tx.execute(sql`SELECT pg_advisory_xact_lock(${leadId}, ${docId})`);
            await tx.delete(bankStatementAnalysesTable)
              .where(sql`${bankStatementAnalysesTable.leadId} = ${leadId} AND ${bankStatementAnalysesTable.documentId} = ${docId}`);
            return tx.insert(bankStatementAnalysesTable).values([analysisValues]).returning();
          });
          savedAnalysis = upserted;
//           scrubLog("Analysis", `Lead ${leadId}: Upserted analysis for document ${docId} (id=${upserted.id})`);
        } else {
          const [inserted] = await db.insert(bankStatementAnalysesTable).values([analysisValues]).returning();
          savedAnalysis = inserted;
        }

        let confs: any[] = [];
        if (analysis.loanDetails && analysis.loanDetails.length > 0) {
          const verdicts = await getCachedVerdicts();
          const sbaPattern = /\bsba\b/i;
          const safelistPattern = /\b(ondeck|on\s*deck|odk\s*capital|kabbage|paypal\s*working|square\s*capital|sq\s*advance|bluevine|fundbox|can\s*capital|rapid\s*financ|credibly|libertas|yellowstone|pearl\s*capital|forward\s*fin|fora\s*financial|kalamata|national\s*funding|fox\s*fund|mantis|everest\s*business|cfg\s*merchant|cfgms|mulligan|reliant|clearview|itria|cloudfund|navitas|tvt\s*capital|greenbox|world\s*business|biz2credit|lendio|fundation|celtic\s*bank|webbank|breakout|headway|behalf|payability|newtek|smartbiz|vox\s*fund|wynwood|platinum\s*rapid|green\s*capital|qfs|jmb\s*capital|unique\s*fund|samson|kings\s*capital|stage\s*adv|7even|cashable|vitalcap|vital\s*capital|vcg|zen\s*funding|ace\s*funding|acg\s*llc|app\s*funding|aspire\s*fund|biz\s*capital|breeze\s*advance|bretton|canfield|capybara|clara\s*capital|compass\s*fund|credit\s*key|daytona\s*fund|diamond\s*advance|family\s*fund\w*\b|fresh\s*fund|funding\s*metrics|gotorro|hightower\s*capital|honor\s*capital|i\s*got\s*funded|idea\s*financial|ifund|immediate\s*advance|immediate\s*capital|iou\s*central|kapitus|w\s+funding|lcf|legend\s*advance|lending\s*servic|lendistry|lg\s*funding|liberty\s*fund|litefund|millstone|mr\s*advance|newco\s*capital|newport\s*business|nitro\s*advance|ocean\s*advance|olympus\s*business|one\s*river|orange\s*advance|parkside\s*fund|power\s*fund|premium\s*merchant|prosperum|ram\s*payment|ram\s*capital|readycap|reboost|redwood\s*business|retro\s*advance|revenued|rocket\s*capital|secure\s*account|simply\s*fund|snap\s*financ|spartan\s*capital|specialty\s*capital|stellar\s*advance|suncoast\s*fund|swift\s*fund|the\s*fundworks|top\s*choice|triton|trupath|wave\s*advance|webfunder|westwood\s*fund|wide\s*merchant|pipe\s*capital|ssmb\s*financial|coast\s*fund|fintegra|altfunding|olympus\s*capital|mako\s*fund|integra\s*fund|fratello|ascentra|luminar|kif\s*fund|greenbridge|jrg\s*capital|aurum|stashcap|stash\s*cap|lily\s*fund|mckenzie|lexio|global\s*fund|monetaria|trustify|bluetie|seamless\s*fund|liquidbee|belltower|palisade|marlin|fundfi|slim\s*fund|steady\s*fund|dib\s*capital)/i;
          const confirmationValues = analysis.loanDetails.map((loan: any, i: number) => {
            const lenderRaw = (loan?.lender || "").trim();
            const key = normalizeLenderKey(lenderRaw);
            let known = key ? verdicts.get(key) : undefined;
            if (!known && sbaPattern.test(lenderRaw)) known = { verdict: "confirmed" as const };
            if (!known && safelistPattern.test(lenderRaw)) known = { verdict: "confirmed" as const, notes: "known-lender-safelist" };

            if (known) {
              return {
                analysisId: savedAnalysis.id,
                leadId,
                leadBusinessName: lead.businessName || null,
                findingType: "loan" as const,
                findingIndex: i,
                originalValue: loan,
                status: known.verdict as "confirmed" | "rejected",
                adminNotes: known.verdict === "confirmed"
                  ? (known.notes === "known-lender-safelist" ? "Auto-confirmed: recognized known lender" : "Auto-confirmed: lender previously verified as loan")
                  : `Auto-rejected: ${known.notes || "previously rejected by admin"}`,
                confirmedAt: new Date(),
              };
            }
            return {
              analysisId: savedAnalysis.id,
              leadId,
              leadBusinessName: lead.businessName || null,
              findingType: "loan" as const,
              findingIndex: i,
              originalValue: loan,
              status: "pending" as const,
            };
          });
          await db.insert(underwritingConfirmationsTable).values(confirmationValues);
          confs = await db.select().from(underwritingConfirmationsTable)
            .where(eq(underwritingConfirmationsTable.analysisId, savedAnalysis.id));
          const autoResolved = confirmationValues.filter(c => c.status !== "pending").length;
          if (autoResolved > 0) {
//             scrubLog("Analysis", `Lead ${leadId}: Auto-resolved ${autoResolved}/${confirmationValues.length} findings from learned rules`);
          }
        }

        batchResults.push({ idx, analysis, savedAnalysis, confirmations: confs });
//         scrubLog("SCRUB-BATCH-DONE", `${label} analysis.id=${savedAnalysis.id} — ${analysis.monthlyRevenues?.length || 0} months, ${analysis.loanDetails?.length || 0} loans, ${confs.length} confirmations`);
        for (const conf of confs) {
          const val = conf.originalValue as any;
//           scrubLog("SCRUB-CONFIRMATION", `  "${val?.lender || "?"}" $${val?.amount || 0} → status=${conf.status}${conf.adminNotes ? ` (${conf.adminNotes.slice(0, 60)})` : ""}`);
        }
      } catch (e: any) {
        failedBatchCount++;
        const errDetail = e.cause ? `${e.message} | cause: ${e.cause}` : e.message;
        console.error(`[Analysis] ${label} Error:`, errDetail);
        if (e.code) console.error(`[Analysis] ${label} PG code:`, e.code, e.detail || "");
      }
    }
  };

  const workers = Array.from(
    { length: Math.min(DOC_CONCURRENCY, batches.length) },
    () => processBatch()
  );
  await Promise.all(workers);

  if (batchResults.length === 0) {
    trace.error("BATCH", `All ${batches.length} statement batches failed`);
    throw new Error("All statement analyses failed");
  }
  if (failedBatchCount > 0) {
    trace.warn("BATCH", `${failedBatchCount} of ${batches.length} batch(es) failed — results may be incomplete`);
    // console.warn(`[Analysis] Lead ${leadId}: ${failedBatchCount} of ${batches.length} batch(es) failed — results may be incomplete`);
  }

  batchResults.sort((a, b) => a.idx - b.idx);
  scrubLog("Re-Scrub", `▶ STEP 6/7: Merging ${batchResults.length} batch result(s) & computing risk score`);
//   scrubLog("SCRUB-MERGE", `Merging ${batchResults.length} batch result(s) for lead ${leadId}`);

  const newAnalyses = batchResults.map(r => r.analysis);
  const savedAnalyses = batchResults.map(r => r.savedAnalysis);
  const allConfirmations = batchResults.flatMap(r => r.confirmations);

  const allAnalysesToMerge: AnalysisResult[] = [];

  if (options?.onlyNew && existingAnalyses.length > 0) {
    for (const ea of existingAnalyses) {
      allAnalysesToMerge.push({
        hasLoans: ea.hasLoans || false,
        hasOnDeck: ea.hasOnDeck || false,
        loanDetails: (ea.loanDetails as any[]) || [],
        monthlyRevenues: (ea.monthlyRevenues as any[]) || [],
        avgDailyBalance: ea.avgDailyBalance ? Number(ea.avgDailyBalance) : 0,
        revenueTrend: (ea.revenueTrend as string) || "stable",
        negativeDays: (ea.negativeDays as any[]) || [],
        nsfCount: ea.nsfCount || 0,
        riskFactors: (ea.riskFactors as string[]) || [],
        riskScore: (ea.riskScore as string) || "B1",
        grossRevenue: ea.grossRevenue ? Number(ea.grossRevenue) : 0,
        hasExistingLoans: ea.hasExistingLoans || false,
        bankName: null,
      });
    }
  }
  allAnalysesToMerge.push(...newAnalyses);

  const mergedAllRevenues: any[] = [];
  const mergedAllLoans: any[] = [];
  let mergedGross = 0;
  let mergedNsfCount = 0;
  const mergedNegDays: any[] = [];
  let mergedHasLoans = false;
  let mergedHasOnDeck = false;
  const mergedRiskFactors: string[] = [];
  let worstRisk = "A1";
  const riskOrder = ["A1", "A2", "B1", "B2", "C"];

  for (const a of allAnalysesToMerge) {
    mergedAllRevenues.push(...(a.monthlyRevenues || []));
    for (const loan of (a.loanDetails || [])) {
      const lk = normalizeLenderKey(loan.lender || "");
      const loanAmt = loan.amount || 0;
      const lenderKeysMatch = (a: string, b: string): boolean => {
        if (a === b) return true;
        if (a.length >= 6 && b.length >= 6) {
          if (a.startsWith(b) || b.startsWith(a)) return true;
          const shorter = a.length <= b.length ? a : b;
          const longer = a.length > b.length ? a : b;
          if (shorter.length >= 8 && longer.includes(shorter)) return true;
          let common = 0;
          for (let i = 0; i < Math.min(a.length, b.length); i++) {
            if (a[i] === b[i]) common++; else break;
          }
          if (common >= 8 && common >= Math.min(a.length, b.length) * 0.6) return true;
        }
        return false;
      };
      const existingLoan = mergedAllLoans.find((el: any) => {
        const elk = normalizeLenderKey(el.lender || "");
        if (!lenderKeysMatch(elk, lk)) return false;
        const elAmt = el.amount || 0;
        if (loanAmt <= 0 || elAmt <= 0) return Math.round(loanAmt) === Math.round(elAmt);
        const ratio = Math.max(loanAmt, elAmt) / Math.min(loanAmt, elAmt);
        return ratio <= 1.05;
      });
      if (existingLoan) {
        existingLoan.occurrences = (existingLoan.occurrences || 0) + (loan.occurrences || 0);
        if (!existingLoan.fundedAmount && loan.fundedAmount) existingLoan.fundedAmount = loan.fundedAmount;
        if (!existingLoan.fundedDate && loan.fundedDate) existingLoan.fundedDate = loan.fundedDate;
      } else {
        mergedAllLoans.push({ ...loan });
      }
    }
    mergedGross += a.grossRevenue || 0;
    mergedNsfCount += a.nsfCount || 0;
    mergedNegDays.push(...(a.negativeDays || []));
    if (a.hasLoans) mergedHasLoans = true;
    if (a.hasOnDeck) mergedHasOnDeck = true;
    for (const rf of (a.riskFactors || [])) {
      if (!mergedRiskFactors.includes(rf)) mergedRiskFactors.push(rf);
    }
    if (riskOrder.indexOf(a.riskScore || "B1") > riskOrder.indexOf(worstRisk)) {
      worstRisk = a.riskScore || "B1";
    }
  }

  const monthlyTotals = new Map<string, number>();
  for (const mr of mergedAllRevenues) {
    const month = (mr.month || "").toLowerCase().trim();
    if (!month) continue;
    const existing = monthlyTotals.get(month) || 0;
    monthlyTotals.set(month, existing + (mr.revenue || 0));
  }
  const sortedMonths = [...monthlyTotals.entries()]
    .sort((a, b) => b[0].localeCompare(a[0]));
  const recent3 = sortedMonths.slice(0, 3);
  const computedGross = recent3.length > 0
    ? Math.round(recent3.reduce((s, [, v]) => s + v, 0) / recent3.length)
    : mergedGross;

  const NOT_LOAN_NAMES = /\b(online\s*(?:banking|transfer)|payment\s+to\b|transfer\s+to\s+(?:sav|chk|checking|savings)|apple\s+cash|self\s+financial|intuit\s+financ|isuzu\s+financ|styku|next\s+insur|pmnt\s+sent|bkofamerica|ascentium|leasechg|lease\s+pymt|loan\s+pymt|td\s+auto|orig\s+co\s+name.*(?:visa|bk\s+of\s+amer|ins\b)|online\s*(?:pay|banking|bill)|autopay\s+to|credit\s*card|card\s+ending|insurance|payroll|adp\b|gusto\b|paychex|irs\b|rent\b|lease\b|utility|electric|ally\s+id:|withdrwl\b|orig\s+co\s+finance|figure\s+lending)\b/i;
  const filteredLoans = mergedAllLoans.filter((loan: any) => {
    if (!loan.amount || loan.amount <= 0) return false;
    const name = (loan.lender || "");
    if (NOT_LOAN_NAMES.test(name)) return false;
    if (name.length < 3) return false;
    if (loan.frequency === "daily" && loan.amount >= 5000) return false;
    if (loan.frequency === "weekly" && loan.amount > 25000) return false;
    if (loan.frequency === "monthly" && loan.amount > 50000) return false;
    if ((!loan.frequency || loan.frequency === "unknown") && loan.amount > 50000) return false;
    if (/^\$[\d,.]+\s/.test(loan.lender || "")) return false;
    return true;
  });

  const mergedAnalysis: AnalysisResult = {
    hasLoans: filteredLoans.length > 0,
    hasOnDeck: filteredLoans.some((l: any) => /ondeck/i.test(l.lender || "")),
    loanDetails: filteredLoans,
    monthlyRevenues: mergedAllRevenues,
    avgDailyBalance: allAnalysesToMerge.reduce((s, a) => s + (a.avgDailyBalance || 0), 0) / allAnalysesToMerge.length,
    revenueTrend: allAnalysesToMerge[0]?.revenueTrend || "stable",
    negativeDays: mergedNegDays,
    nsfCount: mergedNsfCount,
    riskFactors: mergedRiskFactors,
    riskScore: worstRisk,
    grossRevenue: computedGross,
    hasExistingLoans: filteredLoans.length > 0,
    bankName: allAnalysesToMerge[0]?.bankName || null,
    accountNumber: allAnalysesToMerge[0]?.accountNumber || null,
    businessNameOnStatement: allAnalysesToMerge[0]?.businessNameOnStatement || null,
  };

  const totalLoanPayments = filteredLoans.reduce((sum: number, l: any) => {
    const amt = l.amount || 0;
    const freq = (l.frequency || "").toLowerCase();
    const multiplier = freq === "daily" ? 22 : freq === "weekly" ? 4.33 : 1;
    return sum + Math.round(amt * multiplier);
  }, 0);

  let estimatedApproval = 0;
  if (recent3.length > 0) {
    const avgRevenueForApproval = recent3.reduce((s, [, v]) => s + v, 0) / recent3.length;
    estimatedApproval = Math.round(avgRevenueForApproval * 0.25);
  }

  scrubLog("Re-Scrub", `▶ STEP 7/7: Saving final results to DB — risk=${worstRisk}, loans=${filteredLoans.length}, gross=$${computedGross?.toLocaleString() || 0}, loanPmts=$${totalLoanPayments.toLocaleString()}`);
  await db.update(leadsTable).set({
    riskCategory: worstRisk,
    hasExistingLoans: filteredLoans.length > 0,
    hasOnDeck: filteredLoans.some((l: any) => /ondeck/i.test(l.lender || "")),
    loanCount: filteredLoans.length,
    loanDetails: filteredLoans,
    totalLoanPayments: String(totalLoanPayments),
    avgDailyBalance: String(roundToTwo(mergedAnalysis.avgDailyBalance)),
    revenueTrend: mergedAnalysis.revenueTrend,
    grossRevenue: String(roundToTwo(computedGross || lead.grossRevenue)),
    estimatedApproval: estimatedApproval ? String(estimatedApproval) : null,
    bankName: mergedAnalysis.bankName,
    accountNumber: mergedAnalysis.accountNumber,
  }).where(eq(leadsTable.id, leadId));

  // Add a warning note if business name doesn't match
  if (mergedAnalysis.businessNameOnStatement && lead.businessName) {
    const sName = mergedAnalysis.businessNameOnStatement.toLowerCase().replace(/[^a-z0-9]/g, "");
    const lName = lead.businessName.toLowerCase().replace(/[^a-z0-9]/g, "");
    if (sName.length > 0 && lName.length > 0 && !sName.includes(lName) && !lName.includes(sName)) {
      const warning = `[WARNING] Bank Statement Name Mismatch: Lead says "${lead.businessName}", but statement says "${mergedAnalysis.businessNameOnStatement}".`;
      const currentNotes = lead.notes || "";
      if (!currentNotes.includes(warning)) {
        await db.update(leadsTable).set({ notes: (currentNotes ? currentNotes + "\n" + warning : warning) }).where(eq(leadsTable.id, leadId));
      }
    }
  }

  autoConfirmKnownLenders().catch(e => console.error("[Auto-Confirm] Error after analysis:", e.message));

  const finalRevs = mergedAnalysis.monthlyRevenues || [];
  const finalLoans = mergedAnalysis.loanDetails || [];
  trace.info("FINAL_RESULT", `${finalRevs.length} month(s), ${finalLoans.length} loan(s), hasLoans=${mergedAnalysis.hasLoans}`);
  for (const mr of finalRevs) {
    const rev = mr.revenue ?? mr.rev ?? 0;
    trace.info("FINAL_REVENUE", `${mr.month} acct=${mr.account || "?"}: $${rev.toLocaleString()}${mr.needsReview ? " ⚠ NEEDS REVIEW: " + (mr.reviewReason || "") : ""}`);
  }
  for (const loan of finalLoans) {
    trace.info("FINAL_LOAN", `"${loan.lender}" $${loan.amount || 0}/${loan.frequency || "?"} funded=$${loan.fundedAmount || 0} conf=${loan.confidence || "?"}`);
  }
  // console.log(`[TRACE] ===== ${trace.getSummary()} =====`);

  // console.log(`\n${"=".repeat(80)}`);
//   scrubLog("SCRUB-COMPLETE", `Lead ${leadId} "${(lead as any).businessName}" — ${new Date().toISOString()}`);
//   scrubLog("SCRUB-COMPLETE", `${finalRevs.length} month(s) revenue, ${finalLoans.length} loan(s), riskScore=${mergedAnalysis.riskScore}, grossRevenue=$${mergedAnalysis.grossRevenue?.toLocaleString() || 0}`);
  for (const mr of finalRevs) {
    const rev = mr.revenue ?? mr.rev ?? 0;
//     scrubLog("SCRUB-COMPLETE", `  Revenue: ${mr.month} acct=${mr.account || "?"} $${rev.toLocaleString()}${mr.needsReview ? " [REVIEW: " + (mr.reviewReason || "") + "]" : ""}`);
  }
  for (const loan of finalLoans) {
//     scrubLog("SCRUB-COMPLETE", `  Loan: "${loan.lender}" $${loan.amount || 0} freq=${loan.frequency || "?"} funded=$${loan.fundedAmount || 0}`);
  }
  // console.log(`${"=".repeat(80)}\n`);

  structuredTrace.bankName = structuredTrace.stageA.detectedBankName;
  for (const loan of finalLoans) {
    structuredTrace.addLoanCandidate({
      normalizedName: normalizeLenderName(loan.lender || ""),
      matchedLenderDbName: loan.lender || "",
      lender: loan.lender || "",
      amount: loan.amount || 0,
      frequency: loan.frequency || "unknown",
      source: "final",
      transactions: (loan.dates || []).map((d: string) => ({ date: d, amount: loan.amount || 0, description: loan.lender || "" })),
      guessedFrequency: loan.frequency || "unknown",
      selectedLoanAmount: loan.amount || 0,
      selectionReasoning: `${loan.occurrences || 0} occurrences, confidence=${loan.confidence || "unknown"}`,
      amountSource: "parsed_rows",
      rejected: false,
      accountsFoundIn: loan.accountsFoundIn || [],
    });
    structuredTrace.addNumericSource({
      fieldName: `loan_amount:${loan.lender}`,
      finalValue: loan.amount || 0,
      sourceStage: "E",
      sourceRawText: `${loan.lender} ${loan.frequency || ""} x${loan.occurrences || 0}`,
      sourceRule: "parser_extraction",
      modifiedByAi: false,
    });
  }
  for (const mr of finalRevs) {
    const rev = mr.revenue ?? mr.rev ?? 0;
    structuredTrace.addNumericSource({
      fieldName: `monthly_revenue_${mr.month}`,
      finalValue: rev,
      sourceStage: "C",
      sourceRawText: `${mr.month} acct=${mr.account || "?"}`,
      sourceRule: mr.reviewReason ? "needs_review" : "parser_extraction",
      modifiedByAi: false,
    });
  }
  structuredTrace.stageC.finalDepositTotal = computedGross || 0;
  structuredTrace.addNumericSource({
    fieldName: "gross_deposits",
    finalValue: computedGross || 0,
    sourceStage: "C",
    sourceRawText: `Computed from ${finalRevs.length} month(s)`,
    sourceRule: "revenue_aggregation",
    modifiedByAi: false,
  });
  structuredTrace.documentId = savedAnalyses[0]?.documentId || 0;

  await saveTraceToDb(structuredTrace);

  return { analysis: mergedAnalysis, savedAnalysis: savedAnalyses[0], confirmations: allConfirmations, skippedCount };
}

export async function runConcurrentBatch<T>(
  items: T[],
  processor: (item: T) => Promise<void>,
  concurrency: number = CONCURRENCY_LIMIT
): Promise<void> {
  const queue = [...items];
  const workers = Array.from({ length: Math.min(concurrency, queue.length) }, async () => {
    while (queue.length > 0 && !scrubCancelled) {
      const item = queue.shift();
      if (item !== undefined) {
        await processor(item);
      }
    }
  });
  await Promise.all(workers);
  if (scrubCancelled) {
    // console.log(`[Scrub] Cancelled — ${queue.length} items skipped`);
  }
}
