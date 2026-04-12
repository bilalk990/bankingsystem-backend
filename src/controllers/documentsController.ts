import { Router, type IRouter } from "express";
import { eq, isNull, and, sql, inArray } from "drizzle-orm";
import { db, documentsTable, leadsTable, bankStatementAnalysesTable } from "../configs/database";
import {
  GetLeadDocumentsParams,
  GetLeadDocumentsResponse,
} from "../validationSchemas";
import { requireAuth } from "../middlewares/authMiddleware";
import multer from "multer";
import path from "path";
import fs from "fs";
import { openai } from "../integrations/openai";

const router: IRouter = Router();

const UPLOADS_ROOT = path.resolve(process.cwd(), "uploads");

async function checkLeadAccess(leadId: number, user: any, res: any): Promise<boolean> {
  const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
  if (!lead) {
    res.status(404).json({ error: "Lead not found" });
    return false;
  }
  if (user.role === "rep" && lead.assignedToId !== user.id) {
    res.status(403).json({ error: "Access denied" });
    return false;
  }
  return true;
}

function isSafePath(filePath: string): boolean {
  const resolved = path.resolve(filePath);
  return resolved.startsWith(UPLOADS_ROOT);
}

const storage = multer.diskStorage({
  destination: (_req, _file, cb) => {
    const dir = path.join(UPLOADS_ROOT, "documents");
    fs.mkdirSync(dir, { recursive: true });
    cb(null, dir);
  },
  filename: (_req, file, cb) => {
    const ext = path.extname(file.originalname);
    const name = `${Date.now()}-${Math.random().toString(36).slice(2)}${ext}`;
    cb(null, name);
  },
});

const upload = multer({
  storage,
  limits: { fileSize: 50 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    const allowed = [".pdf", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".tiff"];
    const ext = path.extname(file.originalname).toLowerCase();
    if (allowed.includes(ext)) {
      cb(null, true);
    } else {
      cb(new Error(`File type ${ext} not allowed`));
    }
  },
});

router.get("/leads/:id/documents", requireAuth, async (req, res): Promise<void> => {
  const params = GetLeadDocumentsParams.safeParse(req.params);
  if (!params.success) {
    res.status(400).json({ error: params.error.message });
    return;
  }

  const user = (req as any).user;
  if (!(await checkLeadAccess(params.data.id, user, res))) return;

  const docs = await db.select().from(documentsTable)
    .where(eq(documentsTable.leadId, params.data.id))
    .orderBy(documentsTable.createdAt);

  res.json(docs);
});

router.post("/leads/:id/documents/upload", requireAuth, upload.single("file"), async (req, res): Promise<void> => {
  const leadId = parseInt(req.params.id);
  if (isNaN(leadId)) {
    res.status(400).json({ error: "Invalid lead ID" });
    return;
  }

  const user = (req as any).user;
  if (!(await checkLeadAccess(leadId, user, res))) return;

  const file = req.file;
  if (!file) {
    res.status(400).json({ error: "No file uploaded" });
    return;
  }

  const docType = req.body.type || "other";
  const docName = req.body.name || file.originalname;
  const relativeUrl = `uploads/documents/${file.filename}`;

  const [doc] = await db.insert(documentsTable).values({
    leadId,
    type: docType,
    name: docName,
    url: relativeUrl,
  }).returning();

  res.status(201).json(doc);
});

router.post("/leads/:id/documents/classify", requireAuth, async (req, res): Promise<void> => {
  const leadId = parseInt(req.params.id);
  if (isNaN(leadId)) {
    res.status(400).json({ error: "Invalid lead ID" });
    return;
  }

  const user = (req as any).user;
  if (!(await checkLeadAccess(leadId, user, res))) return;

  const docs = await db.select().from(documentsTable)
    .where(eq(documentsTable.leadId, leadId));

  if (docs.length === 0) {
    res.json({ results: [], missing: ["bank_statement", "id_document", "void_check"] });
    return;
  }

  const results = [];

  for (const doc of docs) {
    if (doc.classifiedType && doc.classifiedAt) {
      results.push({
        id: doc.id,
        name: doc.name,
        originalType: doc.type,
        classifiedType: doc.classifiedType,
        confidence: doc.classificationConfidence,
        mismatch: doc.mismatch,
        alreadyClassified: true,
      });
      continue;
    }

    try {
      const classification = await classifyDocument(doc);

      await db.update(documentsTable).set({
        classifiedType: classification.type,
        classificationConfidence: classification.confidence,
        classifiedAt: new Date(),
        mismatch: classification.type !== doc.type,
      }).where(eq(documentsTable.id, doc.id));

      results.push({
        id: doc.id,
        name: doc.name,
        originalType: doc.type,
        classifiedType: classification.type,
        confidence: classification.confidence,
        mismatch: classification.type !== doc.type,
        alreadyClassified: false,
      });
    } catch (err: any) {
      results.push({
        id: doc.id,
        name: doc.name,
        originalType: doc.type,
        classifiedType: null,
        confidence: null,
        mismatch: false,
        error: err.message,
      });
    }
  }

  const classifiedTypes = results
    .filter(r => r.classifiedType)
    .map(r => r.classifiedType);

  const requiredTypes = ["bank_statement", "id_document", "void_check"];
  const missing = requiredTypes.filter(t => !classifiedTypes.includes(t));

  res.json({ results, missing });
});

router.post("/documents/:id/reclassify", requireAuth, async (req, res): Promise<void> => {
  const id = parseInt(req.params.id);
  const user = (req as any).user;
  const [doc] = await db.select().from(documentsTable).where(eq(documentsTable.id, id));
  if (!doc) {
    res.status(404).json({ error: "Document not found" });
    return;
  }
  if (!(await checkLeadAccess(doc.leadId, user, res))) return;

  try {
    const classification = await classifyDocument(doc);

    const [updated] = await db.update(documentsTable).set({
      classifiedType: classification.type,
      classificationConfidence: classification.confidence,
      classifiedAt: new Date(),
      mismatch: classification.type !== doc.type,
    }).where(eq(documentsTable.id, id)).returning();

    res.json(updated);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

router.patch("/documents/:id/type", requireAuth, async (req, res): Promise<void> => {
  const id = parseInt(req.params.id);
  const user = (req as any).user;
  const { type } = req.body;
  if (!type) {
    res.status(400).json({ error: "type is required" });
    return;
  }

  const [doc] = await db.select().from(documentsTable).where(eq(documentsTable.id, id));
  if (!doc) {
    res.status(404).json({ error: "Document not found" });
    return;
  }
  if (!(await checkLeadAccess(doc.leadId, user, res))) return;

  const [updated] = await db.update(documentsTable).set({
    type,
    classifiedType: type,
    mismatch: false,
  }).where(eq(documentsTable.id, id)).returning();

  res.json(updated);
});

router.get("/documents/:id/file", requireAuth, async (req, res): Promise<void> => {
  const id = parseInt(req.params.id);
  if (isNaN(id)) {
    res.status(400).json({ error: "Invalid document ID" });
    return;
  }

  const user = (req as any).user;
  const [doc] = await db.select().from(documentsTable).where(eq(documentsTable.id, id));
  if (!doc) {
    res.status(404).json({ error: "Document not found" });
    return;
  }
  if (!(await checkLeadAccess(doc.leadId, user, res))) return;

  const ext = path.extname(doc.name || doc.url).toLowerCase();
  const mimeTypes: Record<string, string> = {
    ".pdf": "application/pdf",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".tiff": "image/tiff",
  };

  if (doc.storageKey) {
    try {
      const { getFileFromStorage } = await import("../utils/fileStorage");
      const { buffer, contentType } = await getFileFromStorage(doc.storageKey);
      res.setHeader("Content-Type", contentType || mimeTypes[ext] || "application/octet-stream");
      res.setHeader("Content-Disposition", `inline; filename="${doc.name || "document"}"`);
      res.send(buffer);
      return;
    } catch (e: any) {
      console.error(`[Documents] Storage retrieval failed for ${doc.storageKey}:`, e.message);
    }
  }

  const filePath = path.resolve(process.cwd(), doc.url.startsWith("/") ? doc.url.slice(1) : doc.url);
  if (!isSafePath(filePath) || !fs.existsSync(filePath)) {
    res.status(404).json({ error: "File not in cloud storage yet. Re-upload the bank statements ZIP to restore these files — they'll be permanently saved this time." });
    return;
  }

  if (doc.type === "bank_statement" && !doc.storageKey) {
    try {
      const { isStorageConfigured, uploadLocalFileAndGetKey } = await import("../utils/fileStorage");
      if (isStorageConfigured()) {
        const storagePath = doc.url.replace(/^\/uploads\/extracted\//, "");
        const key = await uploadLocalFileAndGetKey(filePath, storagePath);
        if (key) {
          await db.update(documentsTable).set({ storageKey: key }).where(eq(documentsTable.id, doc.id));
        }
      }
    } catch {}
  }

  res.setHeader("Content-Type", mimeTypes[ext] || "application/octet-stream");
  res.setHeader("Content-Disposition", `inline; filename="${doc.name || path.basename(filePath)}"`);
  fs.createReadStream(filePath).pipe(res);
});

router.delete("/documents/:id", requireAuth, async (req, res): Promise<void> => {
  const id = parseInt(req.params.id);
  const user = (req as any).user;
  const [doc] = await db.select().from(documentsTable).where(eq(documentsTable.id, id));
  if (!doc) {
    res.status(404).json({ error: "Document not found" });
    return;
  }
  if (!(await checkLeadAccess(doc.leadId, user, res))) return;

  if (doc.storageKey) {
    try {
      const { deleteFileFromStorage } = await import("../utils/fileStorage");
      await deleteFileFromStorage(doc.storageKey);
    } catch (e: any) {
      console.error(`[Documents] Failed to delete from storage: ${e.message}`);
    }
  }

  const filePath = path.resolve(process.cwd(), doc.url.startsWith("/") ? doc.url.slice(1) : doc.url);
  if (isSafePath(filePath) && fs.existsSync(filePath)) {
    fs.unlinkSync(filePath);
  }

  await db.delete(bankStatementAnalysesTable).where(eq(bankStatementAnalysesTable.documentId, id));
  await db.delete(bankStatementAnalysesTable).where(
    and(eq(bankStatementAnalysesTable.leadId, doc.leadId), isNull(bankStatementAnalysesTable.documentId))
  );

  await db.delete(documentsTable).where(eq(documentsTable.id, id));

  const remainingBankStmts = await db.select({ id: documentsTable.id }).from(documentsTable)
    .where(and(eq(documentsTable.leadId, doc.leadId), eq(documentsTable.type, "bank_statement")));
  if (remainingBankStmts.length === 0) {
    await db.delete(bankStatementAnalysesTable).where(eq(bankStatementAnalysesTable.leadId, doc.leadId));
    const [currentLead] = await db.select({ status: leadsTable.status }).from(leadsTable).where(eq(leadsTable.id, doc.leadId));
    if (currentLead && ["scrubbing_review", "scrubbed"].includes(currentLead.status || "")) {
      await db.update(leadsTable).set({
        status: "new",
        sheetWritebackStatus: null,
        hasExistingLoans: false,
        loanCount: 0,
        loanDetails: [],
        grossRevenue: null,
        avgDailyBalance: null,
        revenueTrend: null,
        riskCategory: null,
        estimatedApproval: null,
      }).where(eq(leadsTable.id, doc.leadId));
    }
  }

  res.json({ success: true });
});

router.delete("/leads/:leadId/bank-statements", requireAuth, async (req, res): Promise<void> => {
  const leadId = parseInt(req.params.leadId);
  const user = (req as any).user;
  if (!(await checkLeadAccess(leadId, user, res))) return;

  const bankStmts = await db.select().from(documentsTable)
    .where(and(eq(documentsTable.leadId, leadId), eq(documentsTable.type, "bank_statement")));

  for (const doc of bankStmts) {
    if (doc.storageKey) {
      try {
        const { deleteFileFromStorage } = await import("../utils/fileStorage");
        await deleteFileFromStorage(doc.storageKey);
      } catch (e: any) {
        console.error(`[Documents] Failed to delete from storage: ${e.message}`);
      }
    }
    const filePath = path.resolve(process.cwd(), doc.url.startsWith("/") ? doc.url.slice(1) : doc.url);
    if (isSafePath(filePath) && fs.existsSync(filePath)) {
      try { fs.unlinkSync(filePath); } catch {}
    }
  }

  if (bankStmts.length > 0) {
    const docIds = bankStmts.map(d => d.id);
    await db.delete(bankStatementAnalysesTable).where(inArray(bankStatementAnalysesTable.documentId, docIds));
  }
  await db.delete(bankStatementAnalysesTable).where(eq(bankStatementAnalysesTable.leadId, leadId));
  await db.delete(documentsTable)
    .where(and(eq(documentsTable.leadId, leadId), eq(documentsTable.type, "bank_statement")));

  const [currentLead] = await db.select({ status: leadsTable.status }).from(leadsTable).where(eq(leadsTable.id, leadId));
  if (currentLead && ["scrubbing_review", "scrubbed"].includes(currentLead.status || "")) {
    await db.update(leadsTable).set({
      status: "new",
      sheetWritebackStatus: null,
      hasExistingLoans: false,
      loanCount: 0,
      loanDetails: [],
      grossRevenue: null,
      avgDailyBalance: null,
      revenueTrend: null,
      riskCategory: null,
      estimatedApproval: null,
    }).where(eq(leadsTable.id, leadId));
  }

  res.json({ success: true, deleted: bankStmts.length });
});

async function classifyDocument(doc: typeof documentsTable.$inferSelect): Promise<{ type: string; confidence: string }> {
  const filePath = path.resolve(process.cwd(), doc.url.startsWith("/") ? doc.url.slice(1) : doc.url);
  const ext = path.extname(filePath).toLowerCase();

  const isImage = [".png", ".jpg", ".jpeg", ".gif", ".webp", ".tiff"].includes(ext);
  const isPdf = ext === ".pdf";

  let contentForAI: any[];

  if (isImage && fs.existsSync(filePath)) {
    const buffer = fs.readFileSync(filePath);
    const base64 = buffer.toString("base64");
    const mimeType = ext === ".png" ? "image/png" : ext === ".gif" ? "image/gif" : ext === ".webp" ? "image/webp" : "image/jpeg";

    contentForAI = [
      {
        type: "text",
        text: `Classify this document. The file is named "${doc.name}". Determine what type of document this is.`,
      },
      {
        type: "image_url",
        image_url: { url: `data:${mimeType};base64,${base64}` },
      },
    ];
  } else if (isPdf && fs.existsSync(filePath)) {
    let pdfText = "";
    try {
      const pdfParse = (await import("pdf-parse")).default;
      const buffer = fs.readFileSync(filePath);
      const data = await pdfParse(buffer);
      pdfText = data.text || "";
    } catch {
      pdfText = "[Could not extract PDF text]";
    }

    contentForAI = [
      {
        type: "text",
        text: `Classify this document. The file is named "${doc.name}". Here is the extracted text content:\n\n${pdfText.slice(0, 8000)}`,
      },
    ];
  } else {
    return {
      type: guessTypeFromName(doc.name),
      confidence: "low",
    };
  }

  const completion = await openai.chat.completions.create({
    model: "gpt-5-mini",
    max_completion_tokens: 300,
    messages: [
      {
        role: "system",
        content: `You are a document classifier for a cash advance / MCA business. Classify the uploaded document into exactly ONE of these types:

- "bank_statement" — Bank account statements showing transactions, balances, deposits, withdrawals. Monthly or multi-month bank statements from any bank.
- "id_document" — Government-issued photo ID such as driver's license, passport, state ID, or any identification document with a photo.
- "void_check" — A voided check or bank check image showing routing number and account number. May also be a direct deposit form or bank letter with account details.
- "application" — A signed merchant cash advance application or contract.
- "other" — Anything that doesn't clearly fit the above categories (tax returns, leases, proof of ownership, etc.)

Respond with ONLY valid JSON: {"type": "<type>", "confidence": "<high|medium|low>", "reason": "<brief reason>"}

Confidence guide:
- "high" — You are very certain about the classification
- "medium" — Likely this type but some ambiguity
- "low" — Uncertain, guessing based on limited information`,
      },
      {
        role: "user",
        content: contentForAI,
      },
    ],
  });

  const response = completion.choices[0]?.message?.content || "{}";
  try {
    const cleaned = response.replace(/```json\n?/g, "").replace(/```\n?/g, "").trim();
    const parsed = JSON.parse(cleaned);
    const validTypes = ["bank_statement", "id_document", "void_check", "application", "other"];
    return {
      type: validTypes.includes(parsed.type) ? parsed.type : "other",
      confidence: ["high", "medium", "low"].includes(parsed.confidence) ? parsed.confidence : "medium",
    };
  } catch {
    return {
      type: guessTypeFromName(doc.name),
      confidence: "low",
    };
  }
}

function guessTypeFromName(name: string): string {
  const lower = name.toLowerCase();
  if (lower.includes("statement") || lower.includes("bank")) return "bank_statement";
  if (lower.includes("id") || lower.includes("license") || lower.includes("passport") || lower.includes("dl")) return "id_document";
  if (lower.includes("void") || lower.includes("check") || lower.includes("cheque")) return "void_check";
  if (lower.includes("application") || lower.includes("contract")) return "application";
  return "other";
}

router.post("/documents/migrate-to-storage", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  if (user.role !== "super_admin" && user.role !== "admin") {
    res.status(403).json({ error: "Admin only" });
    return;
  }

  try {
    const { isStorageConfigured, uploadLocalFileAndGetKey } = await import("../utils/fileStorage");
    if (!isStorageConfigured()) {
      res.status(400).json({ error: "Object storage not configured" });
      return;
    }

    const docs = await db.select({ id: documentsTable.id, url: documentsTable.url, name: documentsTable.name })
      .from(documentsTable)
      .where(and(isNull(documentsTable.storageKey)));

    let uploaded = 0, skipped = 0;
    for (const doc of docs) {
      const localPath = path.resolve(process.cwd(), doc.url.startsWith("/") ? doc.url.slice(1) : doc.url);
      if (!fs.existsSync(localPath)) { skipped++; continue; }
      const storagePath = doc.url.replace(/^\/uploads\/extracted\//, "");
      const key = await uploadLocalFileAndGetKey(localPath, storagePath);
      if (key) {
        await db.update(documentsTable).set({ storageKey: key }).where(eq(documentsTable.id, doc.id));
        uploaded++;
      } else { skipped++; }
    }
    res.json({ total: docs.length, uploaded, skipped, missing: skipped });
  } catch (e: any) {
    console.error("[Migrate Storage]", e.message);
    res.status(500).json({ error: e.message });
  }
});

export default router;
