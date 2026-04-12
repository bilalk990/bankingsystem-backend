import { Router, type IRouter } from "express";
import { eq, and, sql, desc } from "drizzle-orm";
import { db, leadsTable, documentsTable, bankStatementAnalysesTable, underwritingConfirmationsTable, dealsTable, notificationsTable, usersTable, scrubTracesTable } from "../../configs/database";
import { requireAuth, requireAdmin, requirePermission } from "../../middlewares/authMiddleware";
import { tryAutoApprove } from "./underwritingController";
import {
  analyzeSingleLead,
  backgroundJobs,
  buildCombinedTextHeader,
  extractTextFromDocument,
  getLearningContext,
  ANALYSIS_PROMPT,
  parseAIResponse,
  callAIWithRetry,
  runConcurrentBatch,
  extractMonthFromSection,
  extractDepositSummaryFromSection,
  sumCreditTransactionsFromSection,
  balanceEquationFallback,
  parseBalanceFromSection,
  extractAccountNumberFromText,
  getScrubTraceLog,
  type BackgroundJob,
} from "./coreController";
import { identifyBank } from "../../services/bankTemplates";

const router: IRouter = Router();

router.post("/leads/:id/analyze", requireAuth, async (req, res): Promise<void> => {
  try {
    const leadId = parseInt(req.params.id, 10);
    const user = (req as any).user;

    const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!lead) { res.status(404).json({ error: "Lead not found" }); return; }
    if (user.role === "rep" && lead.assignedToId !== user.id) {
      res.status(403).json({ error: "Access denied" }); return;
    }

    const onlyNew = req.query.onlyNew === "true" || req.body?.onlyNew === true;
    const { analysis, savedAnalysis, confirmations, skippedCount } = await analyzeSingleLead(leadId, { onlyNew });

    res.json({
      analysis: savedAnalysis,
      confirmations,
      riskScore: analysis.riskScore,
      summary: analysis.summary,
      verificationNotes: analysis.verificationNotes,
      skippedCount,
    });
  } catch (e: any) {
    console.error("Analysis error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.post("/leads/:id/confirm-finding", requireAuth, requirePermission("ai_learning"), async (req, res): Promise<void> => {
  try {
    const leadId = parseInt(req.params.id, 10);
    const user = (req as any).user;
    const { confirmationId, status, adminLabel, adminNotes } = req.body;

    if (!confirmationId || !status) {
      res.status(400).json({ error: "confirmationId and status are required" }); return;
    }
    if (!["confirmed", "rejected", "relabeled"].includes(status)) {
      res.status(400).json({ error: "status must be confirmed, rejected, or relabeled" }); return;
    }
    if ((status === "rejected" || status === "relabeled") && !adminNotes?.trim()) {
      res.status(400).json({ error: "adminNotes is required when marking a finding as wrong — explain why so the AI can learn" }); return;
    }
    if (status === "relabeled" && !adminLabel) {
      res.status(400).json({ error: "adminLabel is required when relabeling" }); return;
    }

    const [confirmation] = await db.select().from(underwritingConfirmationsTable)
      .where(and(eq(underwritingConfirmationsTable.id, confirmationId), eq(underwritingConfirmationsTable.leadId, leadId)));
    if (!confirmation) { res.status(404).json({ error: "Confirmation not found" }); return; }

    const [updated] = await db.update(underwritingConfirmationsTable).set({
      status, adminLabel: adminLabel || null, adminNotes: adminNotes || null,
      confirmedById: user.id, confirmedAt: new Date(),
    }).where(eq(underwritingConfirmationsTable.id, confirmationId)).returning();

    const val = confirmation.originalValue as any;
    const lenderName = (val?.lender || "").trim();
    if (lenderName && ["confirmed", "rejected"].includes(status)) {
      const { saveLenderRule, invalidateVerdictCache } = await import("./coreController");
      await saveLenderRule(lenderName, status, adminNotes || undefined, user.id);
      invalidateVerdictCache();
    }

    const allConfirmations = await db.select().from(underwritingConfirmationsTable)
      .where(eq(underwritingConfirmationsTable.analysisId, confirmation.analysisId));
    const confirmedLoans = allConfirmations.filter(c => c.status === "confirmed");
    const rejectedCount = allConfirmations.filter(c => c.status === "rejected").length;
    const pendingCount = allConfirmations.filter(c => c.status === "pending").length;

    if (pendingCount === 0) {
      const actualLoanCount = confirmedLoans.length + allConfirmations.filter(c => c.status === "relabeled").length;
      await db.update(leadsTable).set({ hasExistingLoans: actualLoanCount > 0, loanCount: actualLoanCount })
        .where(eq(leadsTable.id, leadId));
    }

    res.json({
      confirmation: updated,
      stats: {
        total: allConfirmations.length, confirmed: confirmedLoans.length,
        rejected: rejectedCount, pending: pendingCount,
        relabeled: allConfirmations.filter(c => c.status === "relabeled").length,
      },
    });
  } catch (e: any) { console.error("Confirm finding error:", e); res.status(500).json({ error: e.message }); return; }

  try {
    const { autoConfirmKnownLenders } = await import("./coreController");
    autoConfirmKnownLenders().catch(e => console.error("[Auto-Confirm] Error:", e.message));
  } catch (_) {}
});

router.post("/leads/:id/reject-lender", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const { lenderName, reason } = req.body;
    if (!lenderName) { res.status(400).json({ error: "lenderName is required" }); return; }
    const user = (req as any).user;
    const { saveLenderRule, invalidateVerdictCache } = await import("./coreController");
    await saveLenderRule(lenderName, "rejected", reason || "Not a loan — rejected from scrubbing", user.id);
    invalidateVerdictCache();
    res.json({ success: true, message: `"${lenderName}" marked as not a loan` });
  } catch (e: any) { console.error("Reject lender error:", e); res.status(500).json({ error: e.message }); }
});

router.post("/auto-confirm", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const { autoConfirmKnownLenders } = await import("./coreController");
    const count = await autoConfirmKnownLenders();
    res.json({ autoConfirmed: count, message: `Auto-resolved ${count} pending findings based on learned rules` });
  } catch (e: any) { console.error("Auto-confirm error:", e); res.status(500).json({ error: e.message }); }
});

router.get("/leads/:id/confirmations", requireAuth, async (req, res): Promise<void> => {
  const leadId = parseInt(req.params.id, 10);
  const user = (req as any).user;
  const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
  if (!lead) { res.status(404).json({ error: "Lead not found" }); return; }
  if (user.role === "rep" && lead.assignedToId !== user.id) { res.status(403).json({ error: "Access denied" }); return; }

  const confirmations = await db.select().from(underwritingConfirmationsTable)
    .where(eq(underwritingConfirmationsTable.leadId, leadId))
    .orderBy(sql`${underwritingConfirmationsTable.createdAt} DESC`);
  res.json(confirmations);
});

router.get("/leads/:id/analysis", requireAuth, async (req, res): Promise<void> => {
  const leadId = parseInt(req.params.id, 10);
  const user = (req as any).user;
  const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
  if (!lead) { res.status(404).json({ error: "Lead not found" }); return; }
  if (user.role === "rep" && lead.assignedToId !== user.id) { res.status(403).json({ error: "Access denied" }); return; }

  const analyses = await db.select().from(bankStatementAnalysesTable)
    .where(eq(bankStatementAnalysesTable.leadId, leadId))
    .orderBy(sql`${bankStatementAnalysesTable.createdAt} DESC`);
  res.json(analyses);
});

router.post("/underwriting/batch-analyze", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const { leadIds } = req.body;
    if (!leadIds || !Array.isArray(leadIds) || leadIds.length === 0) {
      res.status(400).json({ error: "leadIds array is required" }); return;
    }

    const results: { leadId: number; businessName: string; status: string; riskScore?: string; error?: string }[] = [];

    await runConcurrentBatch(leadIds, async (leadId: number) => {
      try {
        const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
        if (!lead) { results.push({ leadId, businessName: "Unknown", status: "error", error: "Lead not found" }); return; }

        const existingAnalysis = await db.select().from(bankStatementAnalysesTable)
          .where(eq(bankStatementAnalysesTable.leadId, leadId)).limit(1);
        if (existingAnalysis.length > 0) {
          results.push({ leadId, businessName: lead.businessName, status: "already_analyzed", riskScore: existingAnalysis[0].riskScore || undefined });
          return;
        }

        const { analysis } = await analyzeSingleLead(leadId);
        const autoResult = await tryAutoApprove(leadId);
        if (!autoResult.autoApproved) {
          await db.update(leadsTable).set({ status: "scrubbing_review" }).where(eq(leadsTable.id, leadId));
        }
        results.push({ leadId, businessName: lead.businessName, status: autoResult.autoApproved ? "auto_approved" : "analyzed", riskScore: analysis.riskScore });
      } catch (e: any) {
        const errorMsg = e.message === "No bank statements found" ? "No bank statements" : e.message;
        const [lead] = await db.select({ businessName: leadsTable.businessName }).from(leadsTable).where(eq(leadsTable.id, leadId)).catch(() => [{ businessName: "Unknown" }]);
        results.push({ leadId, businessName: lead?.businessName || "Unknown", status: e.message === "No bank statements found" ? "skipped" : "error", error: errorMsg });
      }
    }, 3);

    res.json({
      total: leadIds.length,
      analyzed: results.filter(r => r.status === "analyzed" || r.status === "auto_approved").length,
      autoApproved: results.filter(r => r.status === "auto_approved").length,
      skipped: results.filter(r => r.status === "skipped").length,
      alreadyDone: results.filter(r => r.status === "already_analyzed").length,
      errors: results.filter(r => r.status === "error").length,
      results,
    });
  } catch (e: any) { console.error("Batch analysis error:", e); res.status(500).json({ error: e.message }); }
});

router.post("/underwriting/batch-analyze-background", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const { leadIds } = req.body;
    if (!leadIds || !Array.isArray(leadIds) || leadIds.length === 0) {
      res.status(400).json({ error: "leadIds array is required" }); return;
    }

    const jobId = `job_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const job: BackgroundJob = {
      id: jobId, status: "running", total: leadIds.length,
      processed: 0, currentLead: "Starting...", results: [], startedAt: Date.now(),
    };
    backgroundJobs.set(jobId, job);

    res.json({ jobId, total: leadIds.length, message: "Background analysis started" });

    (async () => {
      try {
        await runConcurrentBatch(leadIds, async (leadId: number) => {
          try {
            const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
            if (!lead) { job.results.push({ leadId, businessName: "Unknown", status: "error", error: "Lead not found" }); job.processed++; return; }
            job.currentLead = lead.businessName;

            const existingAnalysis = await db.select().from(bankStatementAnalysesTable)
              .where(eq(bankStatementAnalysesTable.leadId, leadId)).limit(1);
            if (existingAnalysis.length > 0) {
              if (lead.status !== "scrubbing_review" && lead.status !== "scrubbed") {
                await db.update(leadsTable).set({ status: "scrubbing_review" }).where(eq(leadsTable.id, leadId));
              }
              job.results.push({ leadId, businessName: lead.businessName, status: "already_analyzed", riskScore: existingAnalysis[0].riskScore || undefined });
              job.processed++; return;
            }

            const { analysis } = await analyzeSingleLead(leadId);
            const autoResult = await tryAutoApprove(leadId);
            if (!autoResult.autoApproved) {
              await db.update(leadsTable).set({ status: "scrubbing_review" }).where(eq(leadsTable.id, leadId));
            }
            job.results.push({ leadId, businessName: lead.businessName, status: autoResult.autoApproved ? "auto_approved" : "analyzed", riskScore: analysis.riskScore });
          } catch (e: any) {
            console.error(`[Scrub] Lead ${leadId} error: ${e.message}`);
            job.results.push({ leadId, businessName: "Unknown", status: e.message === "No bank statements found" ? "skipped" : "error", error: e.message });
          }
          job.processed++;
        }, 3);

        job.status = "completed";
        job.completedAt = Date.now();
        const autoCount = job.results.filter(r => r.status === "auto_approved").length;
        job.currentLead = autoCount > 0 ? `All done! (${autoCount} auto-approved)` : "All done!";
        setTimeout(() => backgroundJobs.delete(jobId), 30 * 60 * 1000);
      } catch (e: any) { job.status = "error"; job.error = e.message; job.completedAt = Date.now(); }
    })();
  } catch (e: any) { console.error("Background batch start error:", e); res.status(500).json({ error: e.message }); }
});

router.get("/underwriting/job-status/:jobId", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  const job = backgroundJobs.get(req.params.jobId);
  if (!job) { res.status(404).json({ error: "Job not found or expired" }); return; }
  res.json({
    id: job.id, status: job.status, total: job.total, processed: job.processed,
    currentLead: job.currentLead,
    progress: job.total > 0 ? Math.round((job.processed / job.total) * 100) : 0,
    analyzed: job.results.filter(r => r.status === "analyzed").length,
    skipped: job.results.filter(r => r.status === "skipped").length,
    alreadyDone: job.results.filter(r => r.status === "already_analyzed").length,
    errors: job.results.filter(r => r.status === "error").length,
    results: job.results, startedAt: job.startedAt, completedAt: job.completedAt,
    elapsed: job.completedAt ? job.completedAt - job.startedAt : Date.now() - job.startedAt,
  });
});

router.get("/underwriting/active-jobs", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  const activeJobs = [...backgroundJobs.values()]
    .filter(j => j.status === "running")
    .map(j => ({
      id: j.id, total: j.total, processed: j.processed,
      currentLead: j.currentLead,
      progress: j.total > 0 ? Math.round((j.processed / j.total) * 100) : 0,
      startedAt: j.startedAt,
    }));
  res.json(activeJobs);
});

router.get("/underwriting/unanalyzed-leads", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    const result = await db.execute(sql`
      SELECT DISTINCT l.id
      FROM leads l
      JOIN documents d ON d.lead_id = l.id AND d.type = 'bank_statement'
      LEFT JOIN bank_statement_analyses bsa ON bsa.lead_id = l.id
      WHERE bsa.id IS NULL
    `);
    const rows = (result as any).rows || result;
    const leadIds = rows.map((r: any) => r.id);
    res.json({ count: leadIds.length, leadIds });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

router.post("/underwriting/resume-analysis", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    const running = [...backgroundJobs.values()].find(j => j.status === "running");
    if (running) {
      res.json({ jobId: running.id, total: running.total, message: "Analysis already running", alreadyRunning: true });
      return;
    }

    const result = await db.execute(sql`
      SELECT DISTINCT l.id
      FROM leads l
      JOIN documents d ON d.lead_id = l.id AND d.type = 'bank_statement'
      LEFT JOIN bank_statement_analyses bsa ON bsa.lead_id = l.id
      WHERE bsa.id IS NULL
    `);
    const rows = (result as any).rows || result;
    const leadIds = rows.map((r: any) => r.id);
    if (leadIds.length === 0) {
      res.json({ jobId: null, total: 0, message: "All leads already analyzed" });
      return;
    }

    const jobId = `job_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const job: BackgroundJob = {
      id: jobId, status: "running", total: leadIds.length,
      processed: 0, currentLead: "Resuming...", results: [], startedAt: Date.now(),
    };
    backgroundJobs.set(jobId, job);
    res.json({ jobId, total: leadIds.length, message: `Resuming analysis for ${leadIds.length} leads` });

    (async () => {
      try {
        await runConcurrentBatch(leadIds, async (leadId: number) => {
          try {
            const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
            if (!lead) { job.results.push({ leadId, businessName: "Unknown", status: "error", error: "Lead not found" }); job.processed++; return; }
            job.currentLead = lead.businessName;

            const existingAnalysis = await db.select().from(bankStatementAnalysesTable)
              .where(eq(bankStatementAnalysesTable.leadId, leadId)).limit(1);
            if (existingAnalysis.length > 0) {
              job.results.push({ leadId, businessName: lead.businessName, status: "already_analyzed", riskScore: existingAnalysis[0].riskScore || undefined });
              job.processed++; return;
            }

            const { analysis } = await analyzeSingleLead(leadId);
            const autoResult = await tryAutoApprove(leadId);
            if (!autoResult.autoApproved) {
              await db.update(leadsTable).set({ status: "scrubbing_review" }).where(eq(leadsTable.id, leadId));
            }
            job.results.push({ leadId, businessName: lead.businessName, status: autoResult.autoApproved ? "auto_approved" : "analyzed", riskScore: analysis.riskScore });
          } catch (e: any) {
            job.results.push({ leadId, businessName: "Unknown", status: e.message === "No bank statements found" ? "skipped" : "error", error: e.message });
          }
          job.processed++;
        }, 5);

        job.status = "completed";
        job.completedAt = Date.now();
        const autoCountResume = job.results.filter(r => r.status === "auto_approved").length;
        job.currentLead = autoCountResume > 0 ? `All done! (${autoCountResume} auto-approved)` : "All done!";
        setTimeout(() => backgroundJobs.delete(jobId), 30 * 60 * 1000);
      } catch (e: any) { job.status = "error"; job.error = e.message; job.completedAt = Date.now(); }
    })();
  } catch (e: any) { res.status(500).json({ error: e.message }); }
});

router.get("/leads/:id/extraction-preview", requireAuth, async (req, res): Promise<void> => {
  try {
    const leadId = parseInt(req.params.id, 10);
    const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!lead) { res.status(404).json({ error: "Lead not found" }); return; }

    const docs = await db.select().from(documentsTable)
      .where(and(eq(documentsTable.leadId, leadId), eq(documentsTable.type, "bank_statement")));

    if (docs.length === 0) { res.json({ statements: [], leadName: lead.businessName }); return; }

    const statements: any[] = [];
    for (const doc of docs) {
      try {
        const text = await extractTextFromDocument(doc.url, doc.storageKey);
        if (text.startsWith("[Document at") || text.startsWith("[OCR error:") || text === "[OCR produced no text]" || text === "[OCR: pdftoppm produced no images]") {
          const reason = text.startsWith("[Document at") ? "file_not_accessible" : "extraction_failed";
          console.warn(`[ExtractionPreview] ${doc.name}: ${reason} - storageKey=${doc.storageKey || 'none'}, url=${doc.url}`);
          statements.push({ docId: doc.id, fileName: doc.name, error: text, extractionMethod: reason });
          continue;
        }

        const monthKey = extractMonthFromSection(text);
        const depositSummary = extractDepositSummaryFromSection(text);
        const creditSum = sumCreditTransactionsFromSection(text);
        const balCalc = balanceEquationFallback(text);
        const beginBal = parseBalanceFromSection(text, "begin");
        const endBal = parseBalanceFromSection(text, "end");
        const account = extractAccountNumberFromText(text);

        let depositTotal = depositSummary > 0 ? depositSummary : creditSum;
        let depositSource = depositSummary > 0 ? "bank summary label" : (creditSum > 0 ? "credit transaction sum" : "none");
        if (depositTotal <= 0 && balCalc > 0) {
          depositTotal = balCalc;
          depositSource = "balance equation";
        }

        let fullMonth = "";
        if (monthKey) {
          const parts = monthKey.split("-");
          const mo = parseInt(parts[0]);
          const yr = parts[1];
          const fullYr = yr.length === 2 ? `20${yr}` : yr;
          fullMonth = `${fullYr}-${String(mo).padStart(2, "0")}`;
        }

        const extractionMethod = text.includes("[PDF:") ?
          (text.includes("pdfplumber") ? "pdfplumber" : text.includes("ocrmypdf") ? "ocrmypdf" : "pdf-parse") : "pdf-parse";

        statements.push({
          docId: doc.id,
          fileName: doc.name,
          charCount: text.length,
          extractionMethod,
          month: fullMonth || null,
          monthRaw: monthKey || null,
          deposits: {
            total: depositTotal,
            summaryLabel: depositSummary,
            creditTransactionSum: creditSum,
            balanceEquation: balCalc > 0 ? balCalc : null,
            source: depositSource,
          },
          balances: {
            beginning: isNaN(beginBal) ? null : beginBal,
            ending: isNaN(endBal) ? null : endBal,
          },
          account: account || null,
          textPreview: text.slice(0, 2000),
        });
      } catch (e: any) {
        statements.push({ docId: doc.id, fileName: doc.name, error: e.message, extractionMethod: "error" });
      }
    }

    res.json({ leadName: lead.businessName, leadId, statements });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

router.get("/scrub-trace/:leadId", requireAuth, async (req, res): Promise<void> => {
  try {
    const leadId = parseInt(req.params.leadId, 10);
    const user = (req as any).user;
    const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!lead) { res.status(404).json({ error: "Lead not found" }); return; }
    if (user.role === "rep" && lead.assignedToId !== user.id) { res.status(403).json({ error: "Access denied" }); return; }

    const traces = await db.select().from(scrubTracesTable)
      .where(eq(scrubTracesTable.leadId, leadId))
      .orderBy(desc(scrubTracesTable.createdAt))
      .limit(1);

    if (traces.length > 0) {
      const trace = traces[0];
      res.json({
        leadId,
        trace: {
          id: trace.id,
          bankName: trace.bankName,
          traceData: trace.traceData,
          numericSources: trace.numericSources,
          summary: trace.summary,
          aiModifiedAnyNumeric: trace.aiModifiedAnyNumeric,
          createdAt: trace.createdAt,
        },
      });
      return;
    }

    const traceLog = getScrubTraceLog(leadId);
    if (traceLog) {
      res.json({ leadId, log: traceLog, message: "Legacy trace (in-memory only)" });
      return;
    }

    res.json({ leadId, trace: null, message: "No trace data — scrub this lead first" });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

type StatementVerdict = "GOOD" | "GOOD_LOW_CONFIDENCE" | "MISSING_DEPOSIT" | "WRONG_MONTH" | "DUPLICATE_COLLISION" | "OCR_FAILURE" | "PARSE_FAILURE" | "NEEDS_RERUN";

interface StatementAudit {
  documentId: number;
  fileName: string;
  bankName: string | null;
  accountLast4: string | null;
  statementMonth: string | null;
  statementPeriod: string | null;
  depositFound: boolean;
  depositAmount: number;
  depositSource: string;
  confidence: "high" | "medium" | "low" | "none";
  verdict: StatementVerdict;
  aiFinding: string;
  fixSuggestion: string;
  candidates: Array<{ source: string; amount: number; selected: boolean }>;
  textLength: number;
  isOcr: boolean;
}

interface MonthSummary {
  month: string;
  status: "correct" | "missing" | "duplicate" | "suspicious" | "no_deposit";
  depositAmount: number;
  accountLast4: string | null;
  sourceFile: string | null;
  explanation: string;
  documentsForMonth: string[];
}

router.get("/leads/:id/deposit-audit", requireAuth, async (req, res): Promise<void> => {
  try {
    const leadId = parseInt(req.params.id, 10);
    const user = (req as any).user;
    const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!lead) { res.status(404).json({ error: "Lead not found" }); return; }
    if (user.role === "rep" && lead.assignedToId !== user.id) { res.status(403).json({ error: "Access denied" }); return; }

    const analyses = await db.select().from(bankStatementAnalysesTable)
      .where(eq(bankStatementAnalysesTable.leadId, leadId));

    const docs = await db.select().from(documentsTable)
      .where(and(eq(documentsTable.leadId, leadId), eq(documentsTable.type, "bank_statement")));

    const analysisDocIds = new Set(analyses.map(a => a.documentId).filter(Boolean));

    interface AuditSource {
      docId: number | null;
      analysisId: number | null;
      fileName: string;
      text: string;
      scrubGrossRevenue: number;
      scrubMonth: string | null;
      scrubAccountLast4: string;
      scrubBankName: string;
    }

    const sources: AuditSource[] = [];

    for (const a of analyses) {
      const doc = docs.find(d => d.id === a.documentId);
      let text = a.extractedStatementText || "";
      if ((!text || text.length < 100) && doc) {
        text = doc.cachedRawText || "";
        if (!text || text.length < 100) {
          try { text = await extractTextFromDocument(doc.url, doc.storageKey); } catch { text = ""; }
        }
      }

      let scrubAcct = "";
      let scrubBank = "";
      if (a.aiRawAnalysis) {
        try {
          const raw = parseAIResponse(a.aiRawAnalysis);
          if (raw.accountNumber) {
            const cleaned = String(raw.accountNumber).replace(/\D/g, "");
            if (cleaned.length >= 4) scrubAcct = cleaned.slice(-4);
          }
          if (raw.bankName) scrubBank = String(raw.bankName).trim();
        } catch {}
      }

      sources.push({
        docId: a.documentId,
        analysisId: a.id,
        fileName: doc?.name || `analysis_${a.id}`,
        text,
        scrubGrossRevenue: (a.grossRevenue as number) || 0,
        scrubMonth: a.statementMonth || null,
        scrubAccountLast4: scrubAcct,
        scrubBankName: scrubBank,
      });
    }

    for (const doc of docs) {
      if (analysisDocIds.has(doc.id)) continue;
      let text = doc.cachedRawText || "";
      if (!text || text.length < 100) {
        try { text = await extractTextFromDocument(doc.url, doc.storageKey); } catch { text = ""; }
      }
      sources.push({
        docId: doc.id,
        analysisId: null,
        fileName: doc.name || `doc_${doc.id}`,
        text,
        scrubGrossRevenue: 0,
        scrubMonth: null,
        scrubAccountLast4: "",
        scrubBankName: "",
      });
    }

    if (sources.length === 0) {
      res.json({ statements: [], monthSummary: [], flags: [], rootCauses: [] });
      return;
    }

    const statements: StatementAudit[] = [];

    for (const src of sources) {
      const text = src.text;
      const textLen = text.length;
      const isOcr = /^\[(?:OCR|PDF OCR)/i.test(text.trim());
      const bankTemplate = text.length > 100 ? identifyBank(text) : null;
      const bankName = bankTemplate?.name || src.scrubBankName || null;
      const accountLast4 = extractAccountNumberFromText(text) || src.scrubAccountLast4 || null;

      let monthKey = text.length > 200 ? extractMonthFromSection(text) : "";
      let statementMonth: string | null = null;
      let statementPeriod: string | null = null;

      if (monthKey) {
        const parts = monthKey.split("-");
        const mo = parseInt(parts[0]);
        const yr = parts[1]?.length === 2 ? `20${parts[1]}` : parts[1];
        statementMonth = `${yr}-${String(mo).padStart(2, "0")}`;

        const periodMatch = text.slice(0, 3000).match(/(?:statement\s+period|period\s+ending|activity\s+through|for\s+the\s+period)[:\s]*(.+?)(?:\n|$)/i);
        statementPeriod = periodMatch ? periodMatch[1].trim().slice(0, 80) : null;
      }
      if (!statementMonth && src.scrubMonth) {
        const parts = src.scrubMonth.split("-");
        if (parts.length === 2) {
          const mo = parseInt(parts[0]);
          const yr = parts[1]?.length === 2 ? `20${parts[1]}` : parts[1];
          if (mo >= 1 && mo <= 12) statementMonth = `${yr}-${String(mo).padStart(2, "0")}`;
        }
      }

      const candidates: StatementAudit["candidates"] = [];
      let depositAmount = 0;
      let depositSource = "none";
      let confidence: StatementAudit["confidence"] = "none";

      if (text.length >= 200) {
        const summaryDeposit = extractDepositSummaryFromSection(text);
        if (summaryDeposit > 0) {
          candidates.push({ source: "summary_extraction", amount: summaryDeposit, selected: true });
          depositAmount = summaryDeposit;
          depositSource = "summary_extraction";
          confidence = "high";
        }

        const creditSum = sumCreditTransactionsFromSection(text);
        if (creditSum > 0) {
          const isBest = depositAmount === 0;
          candidates.push({ source: "credit_sum", amount: creditSum, selected: isBest });
          if (isBest) { depositAmount = creditSum; depositSource = "credit_sum"; confidence = "medium"; }
        }

        const balCalc = balanceEquationFallback(text);
        if (balCalc > 0) {
          const isBest = depositAmount === 0;
          candidates.push({ source: "balance_equation", amount: balCalc, selected: isBest });
          if (isBest) { depositAmount = balCalc; depositSource = "balance_equation"; confidence = "medium"; }
        }

        if (depositAmount > 0 && balCalc > 0 && depositSource === "summary_extraction") {
          const ratio = depositAmount / balCalc;
          if (ratio < 0.5 || ratio > 1.5) confidence = "low";
        }
      }

      if (src.scrubGrossRevenue > 0) {
        const alreadyHas = candidates.some(c => Math.abs(c.amount - src.scrubGrossRevenue) < 0.01);
        if (!alreadyHas) {
          const isBest = depositAmount === 0;
          candidates.push({ source: "ai_scrub", amount: src.scrubGrossRevenue, selected: isBest });
          if (isBest) { depositAmount = src.scrubGrossRevenue; depositSource = "ai_scrub"; confidence = "medium"; }
        }
      }

      const fileNameMonth = (() => {
        const fn = (src.fileName || "").toLowerCase();
        const monthNames: Record<string, number> = {
          jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6,
          jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12,
          january: 1, february: 2, march: 3, april: 4,
          june: 6, july: 7, august: 8, september: 9,
          october: 10, november: 11, december: 12,
        };
        for (const [name, num] of Object.entries(monthNames)) {
          const idx = fn.indexOf(name);
          if (idx >= 0) {
            const after = fn.slice(idx + name.length).replace(/[^0-9]/g, " ").trim();
            const yrMatch = after.match(/^(\d{2,4})/);
            if (yrMatch) {
              const yr = yrMatch[1].length === 4 ? yrMatch[1] : `20${yrMatch[1]}`;
              return `${yr}-${String(num).padStart(2, "0")}`;
            }
          }
        }
        const dateMatch = fn.match(/(\d{4})(\d{2})(\d{2})/);
        if (dateMatch) return `${dateMatch[1]}-${dateMatch[2]}`;
        return null;
      })();

      let verdict: StatementVerdict = "GOOD";
      let aiFinding = "";
      let fixSuggestion = "";

      if (textLen < 100) {
        verdict = "OCR_FAILURE";
        aiFinding = `Text extraction failed — only ${textLen} characters extracted from this file. The PDF may be image-only or corrupted.`;
        fixSuggestion = "Re-run OCR on this file or re-upload the bank statement.";
      } else if (!statementMonth) {
        verdict = "PARSE_FAILURE";
        aiFinding = "Could not detect the statement month/year from the document header. The date format may be non-standard or the header was not captured.";
        fixSuggestion = "Improve month parsing for this bank's header format, or manually assign the month.";
      } else if (fileNameMonth && statementMonth !== fileNameMonth) {
        verdict = "WRONG_MONTH";
        aiFinding = `Month extracted from statement text (${statementMonth}) does not match the month implied by the filename (${fileNameMonth}). The file may have been mislabeled or the parser picked up a date from the wrong section.`;
        fixSuggestion = `Verify which month this statement actually covers. Filename suggests ${fileNameMonth} but content parsing returned ${statementMonth}.`;
      } else if (depositAmount <= 0) {
        verdict = "MISSING_DEPOSIT";
        aiFinding = `Statement for ${statementMonth} was found but no deposit total could be extracted. ${bankName ? `Bank: ${bankName}.` : "Bank not identified."} No summary section, credit sum, or balance equation produced a valid result.`;
        fixSuggestion = bankName
          ? `Check if ${bankName}'s deposit summary format is supported. May need to widen deposit pattern matching or fall back to transaction-level credit summation.`
          : "Identify the bank first, then add support for its deposit summary format.";
      } else if (confidence === "low") {
        verdict = "GOOD_LOW_CONFIDENCE";
        aiFinding = `Deposit of $${depositAmount.toLocaleString()} was extracted but the balance equation cross-check shows a significant mismatch (expected ~$${Math.round(balanceEquationFallback(text)).toLocaleString()}). The chosen value may be from the wrong section.`;
        fixSuggestion = "Verify the deposit total against the actual statement. The extraction may have picked up a value from a non-deposit section.";
      } else {
        aiFinding = `Deposit of $${depositAmount.toLocaleString()} extracted successfully from ${depositSource.replace(/_/g, " ")}. ${bankName ? `Bank: ${bankName}, ` : ""}Account: ${accountLast4 || "unknown"}.`;
        fixSuggestion = "No action needed.";
      }

      if (!src.analysisId && src.docId && depositAmount <= 0) {
        verdict = "NEEDS_RERUN";
        aiFinding = `This document (${src.fileName}) has not been processed by the scrubbing engine yet. No analysis record exists.`;
        fixSuggestion = "Run or re-run scrubbing for this lead to process this statement.";
      }

      statements.push({
        documentId: src.docId || 0,
        fileName: src.fileName,
        bankName,
        accountLast4,
        statementMonth,
        statementPeriod,
        depositFound: depositAmount > 0,
        depositAmount,
        depositSource,
        confidence,
        verdict,
        aiFinding,
        fixSuggestion,
        candidates,
        textLength: textLen,
        isOcr,
      });
    }

    const monthMap = new Map<string, StatementAudit[]>();
    for (const s of statements) {
      if (s.statementMonth) {
        const existing = monthMap.get(s.statementMonth) || [];
        existing.push(s);
        monthMap.set(s.statementMonth, existing);
      }
    }

    for (const s of statements) {
      if (s.statementMonth && (monthMap.get(s.statementMonth)?.length || 0) > 1) {
        if (s.verdict === "GOOD" || s.verdict === "GOOD_LOW_CONFIDENCE") {
          s.verdict = "DUPLICATE_COLLISION";
          const others = monthMap.get(s.statementMonth)!.filter(o => o.documentId !== s.documentId);
          s.aiFinding = `Multiple files map to ${s.statementMonth}: this file and ${others.map(o => o.fileName).join(", ")}. Deposit values: ${monthMap.get(s.statementMonth)!.map(o => "$" + o.depositAmount.toLocaleString()).join(" vs ")}. This may cause wrong deposit selection.`;
          s.fixSuggestion = "Check if these are truly different statements or duplicates of the same month. If different accounts, they should be separated. If duplicates, remove the extra file.";
        }
      }
    }

    const amountMap = new Map<number, StatementAudit[]>();
    for (const s of statements) {
      if (s.depositAmount > 0) {
        const rounded = Math.round(s.depositAmount * 100);
        const existing = amountMap.get(rounded) || [];
        existing.push(s);
        amountMap.set(rounded, existing);
      }
    }
    for (const [, group] of amountMap) {
      if (group.length > 1 && new Set(group.map(s => s.statementMonth)).size > 1) {
        for (const s of group) {
          if (s.verdict === "GOOD") {
            s.verdict = "GOOD_LOW_CONFIDENCE";
            s.aiFinding += ` WARNING: Same deposit amount ($${s.depositAmount.toLocaleString()}) appears in another month (${group.filter(g => g.documentId !== s.documentId).map(g => g.statementMonth).join(", ")}). This could indicate cross-file contamination.`;
            s.fixSuggestion = "Verify this is not the same deposit reused across different months. Check if extraction happened globally instead of per-statement.";
          }
        }
      }
    }

    const allMonths = [...monthMap.keys()].sort();
    const monthSummary: MonthSummary[] = [];

    if (allMonths.length >= 1) {
      const earliest = allMonths[0];
      const latest = allMonths[allMonths.length - 1];
      const [ey, em] = earliest.split("-").map(Number);
      const [ly, lm] = latest.split("-").map(Number);
      let y = ey, m = em;
      while (y < ly || (y === ly && m <= lm)) {
        const key = `${y}-${String(m).padStart(2, "0")}`;
        const stmts = monthMap.get(key) || [];

        if (stmts.length === 0) {
          const hasFile = statements.some(s => !s.statementMonth && s.textLength > 200);
          monthSummary.push({
            month: key,
            status: "missing",
            depositAmount: 0,
            accountLast4: null,
            sourceFile: null,
            explanation: hasFile
              ? `${key}: No statement assigned to this month, but there are unassigned files that might belong here. Check files with failed month detection.`
              : `${key}: No statement found for this month. The month is missing from the uploaded files.`,
            documentsForMonth: [],
          });
        } else if (stmts.length > 1) {
          const best = stmts.reduce((a, b) => (a.confidence === "high" && b.confidence !== "high") ? a : (b.depositAmount > a.depositAmount ? b : a));
          monthSummary.push({
            month: key,
            status: "duplicate",
            depositAmount: best.depositAmount,
            accountLast4: best.accountLast4,
            sourceFile: best.fileName,
            explanation: `${key}: ${stmts.length} files mapped to this month. Using $${best.depositAmount.toLocaleString()} from ${best.fileName}. Other file(s): ${stmts.filter(s => s.documentId !== best.documentId).map(s => `${s.fileName} ($${s.depositAmount.toLocaleString()})`).join(", ")}.`,
            documentsForMonth: stmts.map(s => s.fileName),
          });
        } else {
          const s = stmts[0];
          if (s.depositAmount <= 0) {
            monthSummary.push({
              month: key,
              status: "no_deposit",
              depositAmount: 0,
              accountLast4: s.accountLast4,
              sourceFile: s.fileName,
              explanation: `${key}: Statement found (${s.fileName}) but deposit extraction failed. ${s.aiFinding}`,
              documentsForMonth: [s.fileName],
            });
          } else if (s.confidence === "low") {
            monthSummary.push({
              month: key,
              status: "suspicious",
              depositAmount: s.depositAmount,
              accountLast4: s.accountLast4,
              sourceFile: s.fileName,
              explanation: `${key}: Deposit $${s.depositAmount.toLocaleString()} found but confidence is low. ${s.aiFinding}`,
              documentsForMonth: [s.fileName],
            });
          } else {
            monthSummary.push({
              month: key,
              status: "correct",
              depositAmount: s.depositAmount,
              accountLast4: s.accountLast4,
              sourceFile: s.fileName,
              explanation: `${key}: Correct — $${s.depositAmount.toLocaleString()} from ${s.fileName}.`,
              documentsForMonth: [s.fileName],
            });
          }
        }

        m++;
        if (m > 12) { m = 1; y++; }
      }
    }

    const flags: string[] = [];
    const missingMonths = monthSummary.filter(m => m.status === "missing");
    if (missingMonths.length > 0) flags.push(`${missingMonths.length} month(s) missing: ${missingMonths.map(m => m.month).join(", ")}`);

    const dupes = monthSummary.filter(m => m.status === "duplicate");
    if (dupes.length > 0) flags.push(`${dupes.length} month(s) have duplicate files: ${dupes.map(m => m.month).join(", ")}`);

    const noDeposit = statements.filter(s => s.verdict === "MISSING_DEPOSIT");
    if (noDeposit.length > 0) flags.push(`${noDeposit.length} file(s) have no deposit extracted`);

    const ocrFail = statements.filter(s => s.verdict === "OCR_FAILURE");
    if (ocrFail.length > 0) flags.push(`${ocrFail.length} file(s) failed OCR/text extraction`);

    const parseFail = statements.filter(s => s.verdict === "PARSE_FAILURE");
    if (parseFail.length > 0) flags.push(`${parseFail.length} file(s) could not detect statement month`);

    const rootCauses: Array<{ issue: string; mostLikely: string; secondMostLikely: string; confidence: string }> = [];

    for (const s of statements.filter(s => s.verdict !== "GOOD")) {
      let mostLikely = "", secondMostLikely = "", conf = "medium";
      switch (s.verdict) {
        case "OCR_FAILURE":
          mostLikely = "PDF is image-only and OCR failed to extract text";
          secondMostLikely = "File is corrupted or not a bank statement";
          conf = "high";
          break;
        case "PARSE_FAILURE":
          mostLikely = "Statement header format not recognized by month parser";
          secondMostLikely = "Statement period is in an unusual position in the document";
          conf = "medium";
          break;
        case "MISSING_DEPOSIT":
          mostLikely = "Deposit summary section format not supported for this bank";
          secondMostLikely = "Statement is a non-standard format (e.g., savings account, credit card)";
          conf = "medium";
          break;
        case "WRONG_MONTH":
          mostLikely = "Month parser picked up a date from the wrong section of the document";
          secondMostLikely = "File was mislabeled with wrong month in the filename";
          conf = "medium";
          break;
        case "DUPLICATE_COLLISION":
          mostLikely = "Same month detected from two different files (possibly same statement uploaded twice)";
          secondMostLikely = "Month detection assigned wrong month to one of the files";
          conf = "medium";
          break;
        case "GOOD_LOW_CONFIDENCE":
          mostLikely = "Deposit candidate came from wrong section or balance equation disagrees";
          secondMostLikely = "Multiple deposit summary lines and wrong one selected";
          conf = "low";
          break;
        case "NEEDS_RERUN":
          mostLikely = "Previous analysis was interrupted or used outdated logic";
          secondMostLikely = "Text cache was stale";
          conf = "medium";
          break;
      }
      if (mostLikely) {
        rootCauses.push({ issue: `${s.fileName}: ${s.verdict}`, mostLikely, secondMostLikely, confidence: conf });
      }
    }

    res.json({ statements, monthSummary, flags, rootCauses });
  } catch (e: any) {
    console.error("[Deposit Audit] Error:", e.message);
    res.status(500).json({ error: e.message });
  }
});

export default router;
