import { Router, type IRouter } from "express";
import multer from "multer";
import { parse } from "csv-parse/sync";
import ExcelJS from "exceljs";
import yauzl from "yauzl";
import path from "path";
import fs from "fs";
import { eq, or, and, sql, lt } from "drizzle-orm";
import { db, leadsTable, documentsTable, importBatchesTable, bankStatementAnalysesTable, uploadBatchesTable } from "../configs/database";
import { desc, inArray, count, isNull } from "drizzle-orm";
import { requireAuth, requireAdmin, requirePermission } from "../middlewares/authMiddleware";
import { encryptLeadFields, hmacHash, normalizeSsnForHash } from "../utils/encryption";
import { analyzeSingleLead, runConcurrentBatch, backgroundJobs, type BackgroundJob, resetScrubCancel } from "./analysis/coreController";
import { isStorageConfigured, uploadLocalFileAndGetKey } from "../utils/fileStorage";

const router: IRouter = Router();

async function uploadDocsToStorage(extractDir: string, job?: ZipImportJob) {
  if (!isStorageConfigured()) {
    console.log("[Storage Upload] Object storage not configured, skipping");
    return;
  }
  try {
    const docsWithoutStorage = await db.select({ id: documentsTable.id, url: documentsTable.url, name: documentsTable.name })
      .from(documentsTable)
      .where(and(
        isNull(documentsTable.storageKey),
        sql`${documentsTable.url} LIKE ${`/uploads/extracted/${path.basename(extractDir)}%`}`
      ));

    if (docsWithoutStorage.length === 0) return;
    console.log(`[Storage Upload] Uploading ${docsWithoutStorage.length} files to persistent storage...`);
    if (job) {
      job.phase = "storing";
      (job as any).storedFiles = 0;
      (job as any).totalFilesToStore = docsWithoutStorage.length;
    }

    let uploaded = 0;
    for (const doc of docsWithoutStorage) {
      const localPath = path.resolve(process.cwd(), doc.url.startsWith("/") ? doc.url.slice(1) : doc.url);
      const storagePath = doc.url.replace(/^\/uploads\/extracted\//, "");
      const key = await uploadLocalFileAndGetKey(localPath, storagePath);
      if (key) {
        await db.update(documentsTable).set({ storageKey: key }).where(eq(documentsTable.id, doc.id));
        uploaded++;
        if (job) (job as any).storedFiles = uploaded;
      }
    }
    console.log(`[Storage Upload] Done: ${uploaded}/${docsWithoutStorage.length} files uploaded to storage`);
  } catch (e: any) {
    console.error("[Storage Upload] Error:", e.message);
  }
}

const UPLOADS_DIR = path.join(process.cwd(), "uploads");
const TEMP_DIR = path.join(UPLOADS_DIR, "temp");
if (!fs.existsSync(TEMP_DIR)) fs.mkdirSync(TEMP_DIR, { recursive: true });

function cleanupOldExtractDirs(maxAgeMs = 24 * 60 * 60 * 1000) {
  try {
    const extractedDir = path.join(UPLOADS_DIR, "extracted");
    if (!fs.existsSync(extractedDir)) return;
    const entries = fs.readdirSync(extractedDir);
    const now = Date.now();
    let cleaned = 0;
    for (const entry of entries) {
      const fullPath = path.join(extractedDir, entry);
      try {
        const stat = fs.statSync(fullPath);
        if (stat.isDirectory() && (now - stat.mtimeMs > maxAgeMs)) {
          fs.rmSync(fullPath, { recursive: true, force: true });
          cleaned++;
        }
      } catch {}
    }
    if (cleaned > 0) console.log(`[Cleanup] Removed ${cleaned} old extract directories`);

    const tempEntries = fs.readdirSync(TEMP_DIR);
    for (const entry of tempEntries) {
      const fullPath = path.join(TEMP_DIR, entry);
      try {
        const stat = fs.statSync(fullPath);
        if (now - stat.mtimeMs > maxAgeMs) {
          if (stat.isDirectory()) fs.rmSync(fullPath, { recursive: true, force: true });
          else fs.unlinkSync(fullPath);
        }
      } catch {}
    }
  } catch (e: any) {
    console.error("[Cleanup] Error cleaning old dirs:", e.message);
  }
}

cleanupOldExtractDirs();

const csvUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 50 * 1024 * 1024 },
});

const zipUpload = multer({
  storage: multer.diskStorage({
    destination: (_req, _file, cb) => cb(null, TEMP_DIR),
    filename: (_req, file, cb) => cb(null, `zip_${Date.now()}_${file.originalname.replace(/[^a-zA-Z0-9._-]/g, "_")}`),
  }),
  limits: { fileSize: 10 * 1024 * 1024 * 1024 },
});

function normalizeColumnName(col: string): string {
  return col.toLowerCase().replace(/[\s_\-\/\\()]+/g, "").replace(/[^\w]/g, "").trim();
}

function findColumn(row: Record<string, string>, ...candidates: string[]): string | undefined {
  const normalizedCandidates = candidates.map(normalizeColumnName);
  for (const [key, val] of Object.entries(row)) {
    const nk = normalizeColumnName(key);
    if (normalizedCandidates.includes(nk)) {
      return val?.trim() || undefined;
    }
  }
  return undefined;
}

function parseNumber(val: string | undefined): number | undefined {
  if (!val) return undefined;
  const cleaned = val.replace(/[$,\s]/g, "");
  const num = parseFloat(cleaned);
  return isNaN(num) ? undefined : num;
}

function normalizePhone(phone: string): string {
  return phone.replace(/[^\d]/g, "").replace(/^1(\d{10})$/, "$1");
}

function normalizeSSN(ssn: string): string {
  return ssn.replace(/[^\d]/g, "");
}

const SSN_RE = /^\d{3}-\d{2}-\d{4}$/;
const EIN_RE = /^\d{2}-\d{5,7}\d*$/;
const PHONE_RE = /^\(?\d{3}\)?\s*[-.]?\s*\d{3}\s*[-.]?\s*\d{4}/;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const DATE_RE = /^\d{1,2}\/\d{1,2}\/\d{2,4}$/;
const CREDIT_RE = /^\d{3}$/;

interface SmartParsedRow {
  businessName: string;
  firstName: string;
  lastName: string;
  ssn?: string;
  email?: string;
  dob?: string;
  phone?: string;
  phone2?: string;
  ein?: string;
  creditScore?: number;
  requestedAmount?: number;
  term?: number;
  submissionDate?: string;
  businessStartDate?: string;
  stackingNotes?: string;
  flag?: string;
}

function isSmartFormat(headerRow: string[]): boolean {
  const h2 = (headerRow[2] || "").trim().toLowerCase();
  const h3 = (headerRow[3] || "").trim().toLowerCase();
  return (h2 === "b name" || h2 === "business name" || h2 === "bname") &&
    (h3 === "name" || h3 === "first name" || h3 === "firstname");
}

function classifyDate(dateStr: string): "dob" | "businessStart" | "submission" {
  const parts = dateStr.split("/");
  const year = parseInt(parts[2] || "0", 10);
  const fullYear = year < 100 ? (year > 50 ? 1900 + year : 2000 + year) : year;
  if (fullYear >= 2024) return "submission";
  if (fullYear >= 2000) return "businessStart";
  if (fullYear >= 1980) return "dob";
  if (fullYear >= 1940) return "dob";
  return "businessStart";
}

function smartParseRow(cells: string[]): SmartParsedRow {
  const row: SmartParsedRow = {
    businessName: (cells[2] || "").trim(),
    firstName: (cells[3] || "").trim(),
    lastName: (cells[4] || "").trim(),
    stackingNotes: (cells[1] || "").trim() || undefined,
  };

  const manualContent = (cells[1] || "").trim();
  if (manualContent) {
    // Strategy 1: Look for term-amount patterns like "12-119k" or "6-50,000"
    const termAmountMatch = manualContent.match(/^(\d+)\s*-\s*(\d+(?:[.,]\d+)?)\s*([kK]?)/);
    if (termAmountMatch) {
      const termVal = parseInt(termAmountMatch[1], 10);
      if (termVal > 0 && termVal <= 60) row.term = termVal;

      let amtVal = parseFloat(termAmountMatch[2].replace(/,/g, ""));
      if (termAmountMatch[3].toLowerCase() === "k") amtVal *= 1000;
      if (amtVal >= 1000) row.requestedAmount = amtVal;
    }

    // Strategy 2: Look for standalone terms like "6mo" or "24 months"
    if (!row.term) {
      const standaloneTermMatch = manualContent.match(/\b(\d+)\s*(?:mo|months|m)\b/i);
      if (standaloneTermMatch) {
        const termVal = parseInt(standaloneTermMatch[1], 10);
        if (termVal > 0 && termVal <= 60) row.term = termVal;
      }
    }
  }

  const phones: string[] = [];
  const dates: { val: string; type: string }[] = [];
  const flags: string[] = [];

  for (let i = 5; i < cells.length; i++) {
    const val = (cells[i] || "").trim();
    if (!val) continue;

    if (!row.ssn && SSN_RE.test(val)) {
      row.ssn = val;
    } else if (!row.email && EMAIL_RE.test(val)) {
      row.email = val;
    } else if (PHONE_RE.test(val) && phones.length < 2) {
      phones.push(val);
    } else if (!row.ein && EIN_RE.test(val)) {
      row.ein = val;
    } else if (CREDIT_RE.test(val)) {
      const cs = parseInt(val, 10);
      if (cs >= 300 && cs <= 900) {
        row.creditScore = cs;
      }
    } else if (DATE_RE.test(val)) {
      dates.push({ val, type: classifyDate(val) });
    } else {
      const num = parseNumber(val);
      if (num && num >= 1000 && !row.requestedAmount) {
        row.requestedAmount = num;
      } else if (val.length <= 5 && /^[A-Z]/.test(val)) {
        flags.push(val);
      }
    }
  }

  if (phones.length > 0) row.phone = phones[0];
  if (phones.length > 1) row.phone2 = phones[1];

  const dobDate = dates.find(d => d.type === "dob");
  const bizDate = dates.find(d => d.type === "businessStart");
  const subDate = dates.find(d => d.type === "submission");
  if (dobDate) row.dob = dobDate.val;
  if (bizDate) row.businessStartDate = bizDate.val;
  if (subDate) row.submissionDate = subDate.val;

  if (flags.length > 0) row.flag = flags.join(", ");

  return row;
}

function deduplicateSmartRows(rows: SmartParsedRow[]): SmartParsedRow[] {
  const grouped = new Map<string, SmartParsedRow[]>();

  for (const row of rows) {
    const key = row.ssn
      ? normalizeSSN(row.ssn)
      : `${row.businessName}|${row.firstName}|${row.lastName}`.toLowerCase();
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key)!.push(row);
  }

  const deduped: SmartParsedRow[] = [];
  for (const [, group] of grouped) {
    group.sort((a, b) => {
      const dateA = a.submissionDate ? new Date(a.submissionDate).getTime() : 0;
      const dateB = b.submissionDate ? new Date(b.submissionDate).getTime() : 0;
      return dateB - dateA;
    });
    const latest = { ...group[0] };
    if (group.length > 1) {
      const allNotes = group.map(r => r.stackingNotes).filter(Boolean);
      if (allNotes.length > 1) {
        latest.stackingNotes = allNotes.join(" | ");
      }
      for (const older of group.slice(1)) {
        if (!latest.email && older.email) latest.email = older.email;
        if (!latest.phone && older.phone) latest.phone = older.phone;
        if (!latest.ssn && older.ssn) latest.ssn = older.ssn;
        if (!latest.ein && older.ein) latest.ein = older.ein;
        if (!latest.creditScore && older.creditScore) latest.creditScore = older.creditScore;
        if (!latest.dob && older.dob) latest.dob = older.dob;
        if (!latest.term && older.term) latest.term = older.term;
        if (!latest.requestedAmount && older.requestedAmount) latest.requestedAmount = older.requestedAmount;
      }
    }
    deduped.push(latest);
  }
  return deduped;
}

router.post("/import/csv", requireAuth, requireAdmin, requirePermission("import"), csvUpload.single("file"), async (req, res): Promise<void> => {
  try {
    if (!req.file) {
      res.status(400).json({ error: "No file uploaded" });
      return;
    }

    const fileName = (req.file.originalname || "").toLowerCase();
    const isExcel = fileName.endsWith(".xlsx") || fileName.endsWith(".xls");

    let rawRows: string[][];
    const csvContent = !isExcel ? req.file.buffer.toString("utf-8") : "";

    if (isExcel) {
      if (fileName.endsWith(".xls")) {
        res.status(400).json({ error: "Legacy .xls format is not supported. Please convert your file to .xlsx and re-upload." });
        return;
      }
      try {
        const workbook = new ExcelJS.Workbook();
        await workbook.xlsx.load(req.file.buffer as any);
        const worksheet = workbook.worksheets[0];
        if (!worksheet) {
          res.status(400).json({ error: "Excel file has no sheets" });
          return;
        }
        rawRows = [];
        worksheet.eachRow({ includeEmpty: false }, (row) => {
          const cells = (row.values as any[]).slice(1).map((cell: any) => {
            if (cell === null || cell === undefined) return "";
            if (typeof cell === "object") {
              if (Array.isArray(cell.richText)) return cell.richText.map((r: any) => r.text ?? "").join("");
              if (cell.text !== undefined) return String(cell.text);
              if (cell.result !== undefined) return String(cell.result);
              if (cell.error !== undefined) return "";
            }
            return String(cell);
          });
          rawRows.push(cells);
        });
        rawRows = rawRows.filter(r => r.some(c => String(c || "").trim()));
      } catch (e: any) {
        res.status(400).json({ error: `Excel parsing error: ${e.message}` });
        return;
      }
    } else {
      try {
        rawRows = parse(csvContent, {
          columns: false,
          skip_empty_lines: true,
          trim: true,
          relax_column_count: true,
          bom: true,
        });
      } catch (e: any) {
        res.status(400).json({ error: `CSV parsing error: ${e.message}` });
        return;
      }
    }

    if (rawRows.length <= 1) {
      res.status(400).json({ error: "File is empty or has no data rows" });
      return;
    }

    const headerRow = rawRows[0];
    const useSmartFormat = isSmartFormat(headerRow);

    const [batch] = await db.insert(importBatchesTable).values({
      fileName: req.file.originalname || "import.csv",
      totalRecords: rawRows.length - 1,
      status: "processing",
    }).returning();

    const results = {
      imported: 0,
      duplicates: 0,
      skippedDuplicatesInFile: 0,
      errors: [] as string[],
      leads: [] as number[],
      format: useSmartFormat ? "auto-detected (deal sheet)" : "standard headers",
    };

    const existingSsnRows = await db.select({ ssnHash: leadsTable.ssnHash }).from(leadsTable).where(sql`${leadsTable.ssnHash} IS NOT NULL`);
    const existingSsnHashes = new Set(existingSsnRows.map(r => r.ssnHash));

    if (useSmartFormat) {
      const dataRows = rawRows.slice(1).filter(r => r.some(c => c.trim()));
      const parsed = dataRows.map(r => smartParseRow(r));
      const deduped = deduplicateSmartRows(parsed);
      results.skippedDuplicatesInFile = parsed.length - deduped.length;

      const validRows: any[] = [];
      for (let i = 0; i < deduped.length; i++) {
        const row = deduped[i];
        if (!row.businessName && !row.firstName) {
          results.errors.push(`Row ${i + 1}: Missing business name and owner name`);
          continue;
        }
        const ownerName = [row.firstName, row.lastName].filter(Boolean).join(" ").trim();
        const phone = row.phone ? normalizePhone(row.phone) : "";
        if (!phone && !row.ssn) {
          results.errors.push(`Row ${i + 1}: Missing phone and SSN for ${row.businessName || ownerName}`);
          continue;
        }
        if (row.ssn) {
          const ssnHashVal = hmacHash(normalizeSsnForHash(row.ssn));
          if (existingSsnHashes.has(ssnHashVal)) {
            results.duplicates++;
            continue;
          }
          existingSsnHashes.add(ssnHashVal);
        }
        validRows.push(encryptLeadFields({
          businessName: row.businessName || ownerName,
          ownerName: ownerName || row.businessName,
          phone: row.phone || "",
          email: row.email,
          ssn: row.ssn,
          dob: row.dob,
          taxId: row.ein,
          creditScore: row.creditScore,
          requestedAmount: row.requestedAmount ? String(row.requestedAmount) : undefined,
          term: row.term,
          businessStartDate: row.businessStartDate,
          notes: row.stackingNotes,
          importBatchId: batch.id,
          importDate: new Date(),
          status: "new",
          source: "csv_import",
        }));
      }

      const BATCH = 200;
      for (let i = 0; i < validRows.length; i += BATCH) {
        const chunk = validRows.slice(i, i + BATCH);
        try {
          const inserted = await db.insert(leadsTable).values(chunk).returning({ id: leadsTable.id });
          results.imported += inserted.length;
          results.leads.push(...inserted.map(l => l.id));
        } catch (e: any) {
          for (const row of chunk) {
            try {
              const [lead] = await db.insert(leadsTable).values(row).returning({ id: leadsTable.id });
              results.imported++;
              results.leads.push(lead.id);
            } catch (e2: any) {
              results.errors.push(`Lead ${row.businessName}: ${e2.message}`);
            }
          }
        }
      }
    } else {
      const records: Record<string, string>[] = parse(csvContent, {
        columns: true,
        skip_empty_lines: true,
        trim: true,
        relax_column_count: true,
        bom: true,
      });

      const validRows: any[] = [];
      for (let i = 0; i < records.length; i++) {
        const row = records[i];
        const businessName = findColumn(row, "businessname", "business", "companyname", "company", "dbaname", "dba", "bname") || "";
        const ownerName = findColumn(row, "ownername", "owner", "name", "fullname", "firstname", "contactname") || "";
        const phone = findColumn(row, "phone", "phonenumber", "telephone", "cell", "mobile", "number") || "";
        const email = findColumn(row, "email", "emailaddress", "owneremail") || undefined;
        const ssn = findColumn(row, "ssn", "socialsecuritynumber", "socialsecurity", "social") || undefined;
        const dob = findColumn(row, "dob", "dateofbirth", "birthday", "birthdate") || undefined;
        const taxId = findColumn(row, "taxid", "taxidnumber", "ein", "fein", "federaltaxid") || undefined;
        const monthlyRevenue = parseNumber(findColumn(row, "monthlyrevenue", "monthlyincome", "revenue", "monthlygross", "grossrevenue", "gross"));
        const requestedAmount = parseNumber(findColumn(row, "requestedamount", "amount", "loanamount", "fundingamount", "requested"));
        const industry = findColumn(row, "industry", "businesstype", "type") || undefined;
        const address = findColumn(row, "address", "streetaddress", "street") || undefined;
        const city = findColumn(row, "city") || undefined;
        const state = findColumn(row, "state", "st") || undefined;
        const zip = findColumn(row, "zip", "zipcode", "postalcode") || undefined;
        const creditScore = parseNumber(findColumn(row, "creditscore", "credit", "fico"));

        if (!businessName && !ownerName) {
          results.errors.push(`Row ${i + 1}: Missing business name and owner name`);
          continue;
        }
        if (!phone) {
          results.errors.push(`Row ${i + 1}: Missing phone number`);
          continue;
        }
        if (ssn) {
          const ssnHashVal = hmacHash(normalizeSsnForHash(ssn));
          if (existingSsnHashes.has(ssnHashVal)) {
            results.duplicates++;
            continue;
          }
          existingSsnHashes.add(ssnHashVal);
        }
        validRows.push(encryptLeadFields({
          businessName: businessName || ownerName,
          ownerName: ownerName || businessName,
          phone,
          email,
          ssn,
          dob,
          taxId,
          monthlyRevenue,
          grossRevenue: monthlyRevenue,
          requestedAmount,
          industry,
          address,
          city,
          state,
          zip,
          creditScore: creditScore ? Math.round(creditScore) : undefined,
          importBatchId: batch.id,
          importDate: new Date(),
          status: "new",
        }));
      }

      const BATCH = 200;
      for (let i = 0; i < validRows.length; i += BATCH) {
        const chunk = validRows.slice(i, i + BATCH);
        try {
          const inserted = await db.insert(leadsTable).values(chunk).returning({ id: leadsTable.id });
          results.imported += inserted.length;
          results.leads.push(...inserted.map(l => l.id));
        } catch (e: any) {
          for (const row of chunk) {
            try {
              const [lead] = await db.insert(leadsTable).values(row).returning({ id: leadsTable.id });
              results.imported++;
              results.leads.push(lead.id);
            } catch (e2: any) {
              results.errors.push(`Row: ${e2.message}`);
            }
          }
        }
      }
    }

    await db.update(importBatchesTable).set({
      processedRecords: results.imported,
      duplicatesFound: results.duplicates,
      status: "completed",
      errors: results.errors.length > 0 ? results.errors.slice(0, 100) : null,
    }).where(eq(importBatchesTable.id, batch.id));

    res.json({
      batchId: batch.id,
      totalRows: rawRows.length - 1,
      imported: results.imported,
      duplicates: results.duplicates,
      skippedDuplicatesInFile: results.skippedDuplicatesInFile,
      format: results.format,
      errors: results.errors.slice(0, 50),
      leadIds: results.leads.slice(0, 100),
    });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

function extractBusinessFromFolder(raw: string): string {
  let name = raw.replace(/\.[^.]+$/, "");

  name = name.replace(/^\d{1,2}[-\s]?(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*[-\s]?\d{2,4}[-\s_]*/i, "");

  name = name.replace(/^\d+[_\s]+/, "");

  name = name.replace(/[_]+/g, " ").replace(/([a-z])([A-Z])/g, "$1 $2");

  name = name.replace(/[\s,_]*(BNK|STMT|STMTS?|OM|DM|BANK|STATEMENT|STATEMENTS)[\s,_]*(BNK|STMT|STMTS?|OM|DM|BANK|STATEMENT|STATEMENTS|[A-Z0-9_]*)*$/i, "");

  name = name.replace(/[-_,\s]+$/, "").replace(/^\s+/, "");

  return name.trim();
}

function cleanFolderName(name: string): string {
  return name
    .replace(/[-_]+/g, " ")
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .replace(/\s+/g, " ")
    .replace(/\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s*\d{1,2}/gi, "")
    .replace(/\d{1,2}\s*[-\/]\s*\d{1,2}\s*[-\/]?\s*\d{0,4}/g, "")
    .replace(/\b(llc|inc|corp|co|ltd|dba)\b/gi, "")
    .replace(/\b(bnk|stmt|stmts|on|bank|statement|statements)\b/gi, "")
    .replace(/\b\d{4,}\b/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

let leadCacheData: { id: number; businessName: string; dba: string | null; ownerName: string | null }[] = [];
let leadCacheTime = 0;

async function getLeadCache() {
  if (Date.now() - leadCacheTime < 30_000 && leadCacheData.length > 0) return leadCacheData;
  leadCacheData = await db.select({
    id: leadsTable.id,
    businessName: leadsTable.businessName,
    dba: leadsTable.dba,
    ownerName: leadsTable.ownerName,
  }).from(leadsTable);
  leadCacheTime = Date.now();
  return leadCacheData;
}

function normalizeForMatch(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9]/g, "").trim();
}

function matchLeadInMemory(leads: typeof leadCacheData, search: string): { id: number; businessName: string } | null {
  const s = search.toLowerCase().trim();
  if (s.length < 2) return null;

  const sNorm = normalizeForMatch(s);

  for (const l of leads) {
    const bnNorm = normalizeForMatch(l.businessName || "");
    const dbaNorm = normalizeForMatch(l.dba || "");

    if (bnNorm && sNorm === bnNorm) return { id: l.id, businessName: l.businessName };
    if (dbaNorm && sNorm === dbaNorm) return { id: l.id, businessName: l.businessName };
  }

  const sClean = normalizeForMatch(cleanFolderName(s));
  if (sClean.length >= 4) {
    for (const l of leads) {
      const bnClean = normalizeForMatch(cleanFolderName(l.businessName || ""));
      const dbaClean = normalizeForMatch(cleanFolderName(l.dba || ""));

      if (bnClean && sClean === bnClean) return { id: l.id, businessName: l.businessName };
      if (dbaClean && sClean === dbaClean) return { id: l.id, businessName: l.businessName };
    }

    for (const l of leads) {
      const bnClean = normalizeForMatch(cleanFolderName(l.businessName || ""));
      const dbaClean = normalizeForMatch(cleanFolderName(l.dba || ""));

      if (bnClean.length >= 5 && sClean.length >= 5) {
        if (sClean.includes(bnClean) || bnClean.includes(sClean)) {
          return { id: l.id, businessName: l.businessName };
        }
      }
      if (dbaClean.length >= 5 && sClean.length >= 5) {
        if (sClean.includes(dbaClean) || dbaClean.includes(sClean)) {
          return { id: l.id, businessName: l.businessName };
        }
      }
    }
  }

  return null;
}

async function fuzzyMatchLead(folderName: string): Promise<{ id: number; businessName: string } | null> {
  const leads = await getLeadCache();

  const extracted = extractBusinessFromFolder(folderName);
  if (extracted && extracted.length >= 2) {
    const match = matchLeadInMemory(leads, extracted);
    if (match) {
      console.log(`[Match] "${folderName}" -> extracted "${extracted}" => matched lead "${match.businessName}" (#${match.id})`);
      return match;
    }
  }

  const cleaned = cleanFolderName(folderName);
  if (cleaned && cleaned.length >= 2) {
    const match = matchLeadInMemory(leads, cleaned);
    if (match) {
      console.log(`[Match] "${folderName}" -> cleaned "${cleaned}" => matched lead "${match.businessName}" (#${match.id})`);
      return match;
    }
  }

  const rawCleaned = folderName.replace(/[-_]+/g, " ").replace(/([a-z])([A-Z])/g, "$1 $2").replace(/\s+/g, " ").trim();
  if (rawCleaned && rawCleaned.length >= 2) {
    const rawMatch = matchLeadInMemory(leads, rawCleaned);
    if (rawMatch) {
      console.log(`[Match] "${folderName}" -> raw "${rawCleaned}" => matched lead "${rawMatch.businessName}" (#${rawMatch.id})`);
      return rawMatch;
    }
  }

  console.log(`[Match] UNMATCHED: "${folderName}" -> extracted="${extracted}", cleaned="${cleaned}"`);
  return null;
}

interface ZipImportJob {
  id: string;
  status: "processing" | "completed" | "error";
  fileName: string;
  totalEntries: number;
  totalFolders: number;
  processedFolders: number;
  currentFolder: string;
  phase: "extracting" | "extracting_inner" | "matching" | "scrubbing" | "done" | "storing";
  matched: number;
  matchedFolders: number;
  totalFiles: number;
  result: any | null;
  error: string | null;
  startedAt: string;
  completedAt: string | null;
  extractedEntries: number;
  innerZipsTotal: number;
  innerZipsDone: number;
  scrubbedLeads: number;
  totalLeadsToScrub: number;
  currentScrubLead: string;
  startedBy?: string;
}

const zipImportJobs = new Map<string, ZipImportJob>();

function cleanOldZipJobs() {
  const ONE_HOUR = 60 * 60 * 1000;
  for (const [id, job] of zipImportJobs) {
    if (job.completedAt && Date.now() - new Date(job.completedAt).getTime() > ONE_HOUR) {
      zipImportJobs.delete(id);
    }
    if (job.status === "processing" && Date.now() - new Date(job.startedAt).getTime() > 120 * 60 * 1000) {
      job.status = "error";
      job.error = "Processing timed out";
      job.completedAt = new Date().toISOString();
    }
  }
}

router.post("/import/clear-stuck-jobs", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    zipImportJobs.clear();
    const result = await db.update(uploadBatchesTable)
      .set({ status: "error", error: "Manually cleared — was stuck", completedAt: new Date() })
      .where(eq(uploadBatchesTable.status, "processing"));
    res.json({ success: true, message: "Cleared all stuck jobs" });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

router.get("/import/active-zip-jobs", requireAuth, async (_req, res): Promise<void> => {
  cleanOldZipJobs();
  const active = [...zipImportJobs.values()].filter(j => j.status === "processing");
  if (active.length > 0) {
    res.json(active.map(j => ({ id: j.id, phase: j.phase, fileName: j.fileName, startedBy: j.startedBy || "Unknown", startedAt: j.startedAt })));
    return;
  }
  try {
    const thirtyMinAgo = new Date(Date.now() - 30 * 60 * 1000);
    const processingBatches = await db.select().from(uploadBatchesTable)
      .where(and(
        eq(uploadBatchesTable.status, "processing"),
        sql`${uploadBatchesTable.createdAt} > ${thirtyMinAgo}`
      ))
      .limit(1);
    if (processingBatches.length > 0) {
      const b = processingBatches[0];
      res.json([{ id: `db-batch-${b.id}`, phase: "processing", fileName: b.fileName, startedAt: b.createdAt?.toISOString(), dbBatch: true }]);
      return;
    }
  } catch {}
  res.json([]);
});

router.get("/import/zip-job/:jobId", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  cleanOldZipJobs();
  const job = zipImportJobs.get((req.params as any).jobId);
  if (!job) {
    res.status(404).json({ error: "Job not found" });
    return;
  }
  const elapsedMs = Date.now() - new Date(job.startedAt).getTime();
  res.json({ ...job, elapsedMs });
});

router.get("/import/upload-history", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    const thirtyMinAgo = new Date(Date.now() - 30 * 60 * 1000);
    await db.update(uploadBatchesTable)
      .set({ status: "error", error: "Timed out — processing took too long", completedAt: new Date() })
      .where(and(
        eq(uploadBatchesTable.status, "processing"),
        lt(uploadBatchesTable.createdAt, thirtyMinAgo)
      ));

    const batches = await db.select().from(uploadBatchesTable)
      .orderBy(desc(uploadBatchesTable.createdAt))
      .limit(50);
    res.json({ batches });
  } catch (e: any) { res.status(500).json({ error: e.message }); }
});

router.delete("/import/upload-batch/:id", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const batchId = parseInt((req.params as any).id, 10);
    const [batch] = await db.select().from(uploadBatchesTable).where(eq(uploadBatchesTable.id, batchId));
    if (!batch) { res.status(404).json({ error: "Upload batch not found" }); return; }

    const matchDetails = (batch.matchDetails as any[]) || [];
    const allDocIds: number[] = [];
    const allLeadIds: number[] = [];
    for (const m of matchDetails) {
      if (m.documentIds) allDocIds.push(...m.documentIds);
      if (m.leadId) allLeadIds.push(m.leadId);
    }

    if (allDocIds.length > 0) {
      await db.delete(documentsTable).where(inArray(documentsTable.id, allDocIds));
    }

    const uniqueLeadIds = [...new Set(allLeadIds)];
    for (const leadId of uniqueLeadIds) {
      const remainingDocs = await db.select({ id: documentsTable.id }).from(documentsTable)
        .where(and(eq(documentsTable.leadId, leadId), eq(documentsTable.type, "bank_statement")))
        .limit(1);
      if (remainingDocs.length === 0) {
        await db.delete(bankStatementAnalysesTable).where(eq(bankStatementAnalysesTable.leadId, leadId));
      }
    }

    if (batch.extractDir && fs.existsSync(batch.extractDir)) {
      try { fs.rmSync(batch.extractDir, { recursive: true, force: true }); } catch {}
    }

    await db.delete(uploadBatchesTable).where(eq(uploadBatchesTable.id, batchId));

    res.json({
      success: true,
      deletedDocuments: allDocIds.length,
      affectedLeads: uniqueLeadIds.length,
    });
  } catch (e: any) {
    console.error("Delete upload batch error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.get("/import/all-history", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    const leadImports = await db.select().from(importBatchesTable)
      .orderBy(desc(importBatchesTable.createdAt)).limit(50);

    const statementUploads = await db.select().from(uploadBatchesTable)
      .orderBy(desc(uploadBatchesTable.createdAt)).limit(50);

    const combined: any[] = [];

    const batchIds = leadImports.map(li => li.id);
    const scrubbedByBatch: Record<number, number> = {};
    if (batchIds.length > 0) {
      const rows = await db.select({
        importBatchId: leadsTable.importBatchId,
        count: count(),
      }).from(leadsTable)
        .where(and(
          inArray(leadsTable.importBatchId, batchIds),
          inArray(leadsTable.status, ["scrubbed", "underwriting", "submitted", "funded"])
        ))
        .groupBy(leadsTable.importBatchId);
      for (const r of rows) {
        if (r.importBatchId) scrubbedByBatch[r.importBatchId] = Number(r.count);
      }
    }

    for (const li of leadImports) {
      combined.push({
        id: `lead-${li.id}`,
        batchId: li.id,
        type: "leads",
        fileName: li.fileName,
        status: li.status,
        date: li.createdAt,
        totalRecords: li.totalRecords || 0,
        processedRecords: li.processedRecords || 0,
        duplicatesFound: li.duplicatesFound || 0,
        scrubbedCount: scrubbedByBatch[li.id] || 0,
        errors: li.errors,
      });
    }

    const allStmtLeadIds: number[] = [];
    for (const su of statementUploads) {
      const matchDetails = (su.matchDetails as any[]) || [];
      for (const m of matchDetails) { if (m.leadId) allStmtLeadIds.push(m.leadId); }
    }
    const uniqueStmtLeadIds = [...new Set(allStmtLeadIds)];
    const scrubbedLeadSet = new Set<number>();
    if (uniqueStmtLeadIds.length > 0) {
      const rows = await db.select({ id: leadsTable.id }).from(leadsTable)
        .where(and(
          inArray(leadsTable.id, uniqueStmtLeadIds),
          inArray(leadsTable.status, ["scrubbing_review", "scrubbed", "underwriting", "submitted", "funded"])
        ));
      for (const r of rows) { scrubbedLeadSet.add(r.id); }
    }

    const analysisLeadSet = new Set<number>();
    if (uniqueStmtLeadIds.length > 0) {
      const analysisRows = await db.selectDistinct({ leadId: bankStatementAnalysesTable.leadId })
        .from(bankStatementAnalysesTable)
        .where(inArray(bankStatementAnalysesTable.leadId, uniqueStmtLeadIds));
      for (const r of analysisRows) { if (r.leadId) analysisLeadSet.add(r.leadId); }
    }

    for (const su of statementUploads) {
      const matchDetails = (su.matchDetails as any[]) || [];
      const leadIds = [...new Set(matchDetails.map((m: any) => m.leadId).filter(Boolean))];
      const scrubbedCount = leadIds.filter(id => scrubbedLeadSet.has(id) || analysisLeadSet.has(id)).length;

      combined.push({
        id: `stmt-${su.id}`,
        batchId: su.id,
        type: "statements",
        fileName: su.fileName,
        status: su.status,
        date: su.createdAt,
        totalFolders: su.totalFolders || 0,
        matchedFolders: su.matchedFolders || 0,
        matchedFiles: su.matchedFiles || 0,
        unmatchedFolders: su.unmatchedFolders || [],
        matchDetails,
        sourceTier: su.sourceTier,
        scrubbedCount,
        leadCount: leadIds.length,
      });
    }

    combined.sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());

    res.json({ imports: combined });
  } catch (e: any) {
    console.error("All history error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.post("/import/scrub-statements/:batchId", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const batchId = parseInt((req.params as any).batchId, 10);
    const [batch] = await db.select().from(uploadBatchesTable).where(eq(uploadBatchesTable.id, batchId));
    if (!batch) { res.status(404).json({ error: "Upload batch not found" }); return; }

    const matchDetails = (batch.matchDetails as any[]) || [];
    const leadIds = [...new Set(matchDetails.map((m: any) => m.leadId).filter(Boolean))] as number[];
    if (leadIds.length === 0) { res.status(400).json({ error: "No matched leads in this batch" }); return; }

    const running = [...backgroundJobs.values()].find(j => j.status === "running");
    if (running) { res.status(409).json({ error: "A scrubbing job is already running. Please wait for it to complete." }); return; }

    const unanalyzed: number[] = [];
    for (const leadId of leadIds) {
      const existing = await db.select({ id: bankStatementAnalysesTable.id })
        .from(bankStatementAnalysesTable)
        .where(eq(bankStatementAnalysesTable.leadId, leadId))
        .limit(1);
      if (existing.length === 0) unanalyzed.push(leadId);
    }

    if (unanalyzed.length === 0) {
      for (const leadId of leadIds) {
        await db.update(leadsTable).set({ status: "scrubbing_review" })
          .where(and(eq(leadsTable.id, leadId), eq(leadsTable.status, "new")));
      }
      res.json({ message: "All leads already scrubbed", total: leadIds.length, scrubbed: leadIds.length });
      return;
    }

    const jobId = `manual_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    resetScrubCancel();
    const job: BackgroundJob = {
      id: jobId, status: "running", total: unanalyzed.length,
      processed: 0, currentLead: "Starting scrubbing...", results: [], startedAt: Date.now(),
    };
    backgroundJobs.set(jobId, job);

    console.log(`[Manual-Scrub] Starting AI analysis for ${unanalyzed.length}/${leadIds.length} leads (job: ${jobId})`);

    (async () => {
      try {
        await runConcurrentBatch(unanalyzed, async (leadId: number) => {
          let leadName = "Unknown";
          try {
            const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
            if (!lead) { job.results.push({ leadId, businessName: "Unknown", status: "error", error: "Lead not found" }); return; }
            leadName = lead.businessName || "Unknown";
            job.currentLead = leadName;

            const { analysis } = await analyzeSingleLead(leadId);
            const isEmpty = !analysis.grossRevenue && !analysis.hasExistingLoans;
            if (isEmpty) {
              console.log(`[Manual-Scrub] Empty analysis for "${leadName}" (no revenue, no loans) — resetting to new`);
              await db.update(leadsTable).set({ status: "new" }).where(eq(leadsTable.id, leadId));
              job.results.push({ leadId, businessName: leadName, status: "empty", riskScore: analysis.riskScore });
            } else {
              await db.update(leadsTable).set({ status: "scrubbing_review" }).where(eq(leadsTable.id, leadId));
              job.results.push({ leadId, businessName: leadName, status: "analyzed", riskScore: analysis.riskScore });
            }
          } catch (e: any) {
            try {
              const existing = await db.select({ id: bankStatementAnalysesTable.id, riskScore: bankStatementAnalysesTable.riskScore })
                .from(bankStatementAnalysesTable).where(eq(bankStatementAnalysesTable.leadId, leadId)).limit(1);
              if (existing.length > 0) {
                await db.update(leadsTable).set({ status: "scrubbing_review" }).where(eq(leadsTable.id, leadId));
                job.results.push({ leadId, businessName: leadName, status: "analyzed", riskScore: existing[0].riskScore ?? undefined, note: "partial" });
              } else {
                job.results.push({ leadId, businessName: leadName, status: "error", error: e.message });
              }
            } catch (dbErr: any) {
              job.results.push({ leadId, businessName: leadName, status: "error", error: e.message });
            }
          } finally {
            job.processed++;
          }
        }, 5);

        job.status = "completed";
        job.completedAt = Date.now();
        job.currentLead = "All done!";
        console.log(`[Manual-Scrub] Completed: ${job.results.filter(r => r.status === "analyzed").length} analyzed, ${job.results.filter(r => r.status === "error").length} errors`);
        setTimeout(() => backgroundJobs.delete(jobId), 30 * 60 * 1000);
      } catch (e: any) {
        job.status = "error"; job.error = e.message; job.completedAt = Date.now();
        console.error("[Manual-Scrub] Error:", e.message);
      }
    })();

    res.json({ message: "Scrubbing started", jobId, total: unanalyzed.length, alreadyScrubbed: leadIds.length - unanalyzed.length });
  } catch (e: any) {
    console.error("Scrub trigger error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.delete("/import/lead-batch/:id", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const batchId = parseInt((req.params as any).id, 10);
    const [batch] = await db.select().from(importBatchesTable).where(eq(importBatchesTable.id, batchId));
    if (!batch) { res.status(404).json({ error: "Import batch not found" }); return; }

    const leads = await db.select({ id: leadsTable.id }).from(leadsTable)
      .where(eq(leadsTable.importBatchId, batchId));

    const leadIds = leads.map(l => l.id);

    if (leadIds.length > 0) {
      try { await db.execute(sql`DELETE FROM submissions WHERE deal_id IN (SELECT id FROM deals WHERE lead_id IN (SELECT id FROM leads WHERE import_batch_id = ${batchId}))`); } catch (e: any) { console.log("Skip submissions:", e.message); }
      const depTables = [
        "underwriting_confirmations", "bank_statement_analyses", "documents",
        "activities", "deals", "calls", "lead_messages", "notifications",
        "smart_reminders", "tasks", "renewal_suggestions",
      ];
      for (const table of depTables) {
        try {
          await db.execute(sql`DELETE FROM ${sql.raw(table)} WHERE lead_id IN (SELECT id FROM leads WHERE import_batch_id = ${batchId})`);
        } catch (e: any) { console.log(`Skip ${table}:`, e.message); }
      }
      await db.execute(sql`DELETE FROM leads WHERE import_batch_id = ${batchId}`);
    }

    await db.delete(importBatchesTable).where(eq(importBatchesTable.id, batchId));

    res.json({ success: true, deletedLeads: leadIds.length });
  } catch (e: any) {
    console.error("Delete lead batch error:", e);
    res.status(500).json({ error: e.message });
  }
});

const chunkUpload = multer({
  storage: multer.diskStorage({
    destination: (_req, _file, cb) => {
      const chunksDir = path.join(TEMP_DIR, "chunks");
      if (!fs.existsSync(chunksDir)) fs.mkdirSync(chunksDir, { recursive: true });
      cb(null, chunksDir);
    },
    filename: (_req, file, cb) => cb(null, `chunk_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`),
  }),
  limits: { fileSize: 6 * 1024 * 1024 },
});

router.post("/import/chunk-upload", requireAuth, requireAdmin, requirePermission("import"), chunkUpload.single("chunk"), async (req, res): Promise<void> => {
  try {
    const { uploadId, chunkIndex, totalChunks, fileName } = req.body;
    if (!req.file || !uploadId || chunkIndex == null || !totalChunks) {
      res.status(400).json({ error: "Missing chunk data" });
      return;
    }

    const uploadDir = path.join(TEMP_DIR, `chunked_${uploadId}`);
    if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir, { recursive: true });

    const chunkPath = path.join(uploadDir, `chunk_${String(chunkIndex).padStart(6, "0")}`);
    fs.renameSync(req.file.path, chunkPath);

    const markerPath = path.join(uploadDir, `done_${String(chunkIndex).padStart(6, "0")}`);
    fs.writeFileSync(markerPath, "");

    const metaPath = path.join(uploadDir, "meta.json");
    const metaData = { fileName, totalChunks: parseInt(totalChunks) };
    fs.writeFileSync(metaPath, JSON.stringify(metaData));

    const doneFiles = fs.readdirSync(uploadDir).filter((f: string) => f.startsWith("done_")).length;
    const allReceived = doneFiles >= parseInt(totalChunks);
    res.json({ received: true, chunkIndex: parseInt(chunkIndex), allReceived });
  } catch (e: any) {
    console.error("Chunk upload error:", e);
    res.status(500).json({ error: e.message || "Chunk upload failed" });
  }
});

async function isImportActive(): Promise<{ active: boolean; fileName?: string }> {
  const activeInMemory = [...zipImportJobs.values()].filter(j => j.status === "processing");
  if (activeInMemory.length > 0) return { active: true, fileName: activeInMemory[0].fileName };
  try {
    const thirtyMinAgo = new Date(Date.now() - 30 * 60 * 1000);
    const processingBatches = await db.select({ fileName: uploadBatchesTable.fileName })
      .from(uploadBatchesTable)
      .where(and(eq(uploadBatchesTable.status, "processing"), sql`${uploadBatchesTable.createdAt} > ${thirtyMinAgo}`))
      .limit(1);
    if (processingBatches.length > 0) return { active: true, fileName: processingBatches[0].fileName };
  } catch {}
  return { active: false };
}

router.post("/import/chunk-complete", requireAuth, requireAdmin, requirePermission("import"), async (req, res): Promise<void> => {
  try {
    const lockCheck = await isImportActive();
    if (lockCheck.active) {
      res.status(409).json({ error: `An import is already in progress (${lockCheck.fileName}). Please wait for it to finish.` });
      return;
    }

    const { uploadId } = req.body;
    if (!uploadId) { res.status(400).json({ error: "Missing uploadId" }); return; }

    const uploadDir = path.join(TEMP_DIR, `chunked_${uploadId}`);
    const metaPath = path.join(uploadDir, "meta.json");
    if (!fs.existsSync(metaPath)) { res.status(404).json({ error: "Upload not found" }); return; }

    const meta = JSON.parse(fs.readFileSync(metaPath, "utf-8"));
    const doneFiles = fs.readdirSync(uploadDir).filter((f: string) => f.startsWith("done_")).length;
    if (doneFiles < meta.totalChunks) {
      res.status(400).json({ error: `Missing chunks: received ${doneFiles}/${meta.totalChunks}` });
      return;
    }

    const assembledPath = path.join(TEMP_DIR, `zip_${Date.now()}_${meta.fileName.replace(/[^a-zA-Z0-9._-]/g, "_")}`);
    for (let i = 0; i < meta.totalChunks; i++) {
      const chunkPath = path.join(uploadDir, `chunk_${String(i).padStart(6, "0")}`);
      if (!fs.existsSync(chunkPath)) {
        res.status(400).json({ error: `Missing chunk ${i}` });
        return;
      }
    }
    const writeStream = fs.createWriteStream(assembledPath);
    for (let i = 0; i < meta.totalChunks; i++) {
      const chunkPath = path.join(uploadDir, `chunk_${String(i).padStart(6, "0")}`);
      await new Promise<void>((resolve, reject) => {
        const readStream = fs.createReadStream(chunkPath);
        readStream.pipe(writeStream, { end: false });
        readStream.on("end", resolve);
        readStream.on("error", reject);
      });
    }
    await new Promise<void>((resolve, reject) => {
      writeStream.on("finish", resolve);
      writeStream.on("error", reject);
      writeStream.end();
    });

    fs.rmSync(uploadDir, { recursive: true, force: true });

    const user = (req as any).user;
    const jobId = `zip-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const job: ZipImportJob = {
      id: jobId,
      status: "processing",
      fileName: meta.fileName,
      totalEntries: 0,
      totalFolders: 0,
      processedFolders: 0,
      currentFolder: "",
      phase: "extracting",
      matched: 0,
      matchedFolders: 0,
      totalFiles: 0,
      result: null,
      error: null,
      startedAt: new Date().toISOString(),
      completedAt: null,
      extractedEntries: 0,
      innerZipsTotal: 0,
      innerZipsDone: 0,
      scrubbedLeads: 0,
      totalLeadsToScrub: 0,
      currentScrubLead: "",
      startedBy: user?.fullName || user?.email || "Unknown",
    };
    zipImportJobs.set(jobId, job);

    const [batch] = await db.insert(uploadBatchesTable).values({
      fileName: meta.fileName, status: "processing",
    }).returning();

    res.json({ jobId, status: "processing" });

    processZipInBackground(job, assembledPath, meta.fileName, batch.id).catch(err => {
      job.status = "error";
      job.error = err.message || "Unknown error";
      job.completedAt = new Date().toISOString();
    });
  } catch (e: any) {
    console.error("Chunk complete error:", e);
    res.status(500).json({ error: e.message || "Assembly failed" });
  }
});

router.post("/import/bank-statements", requireAuth, requireAdmin, requirePermission("import"), zipUpload.single("file"), async (req, res): Promise<void> => {
  try {
    const lockCheck = await isImportActive();
    if (lockCheck.active) {
      res.status(409).json({ error: `An import is already in progress (${lockCheck.fileName}). Please wait for it to finish.` });
      return;
    }

    if (!req.file) {
      res.status(400).json({ error: "No ZIP file uploaded" });
      return;
    }

    const user = (req as any).user;
    const jobId = `zip-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const tempFilePath = req.file.path;
    const originalFileName = req.file.originalname || "";

    const job: ZipImportJob = {
      id: jobId,
      status: "processing",
      fileName: originalFileName,
      totalEntries: 0,
      totalFolders: 0,
      processedFolders: 0,
      currentFolder: "",
      phase: "extracting",
      matched: 0,
      matchedFolders: 0,
      totalFiles: 0,
      result: null,
      error: null,
      startedAt: new Date().toISOString(),
      completedAt: null,
      extractedEntries: 0,
      innerZipsTotal: 0,
      innerZipsDone: 0,
      scrubbedLeads: 0,
      totalLeadsToScrub: 0,
      currentScrubLead: "",
      startedBy: user?.fullName || user?.email || "Unknown",
    };
    zipImportJobs.set(jobId, job);

    const [batch] = await db.insert(uploadBatchesTable).values({
      fileName: originalFileName, status: "processing",
    }).returning();

    res.json({ jobId, status: "processing" });

    processZipInBackground(job, tempFilePath, originalFileName, batch.id).catch(err => {
      job.status = "error";
      job.error = err.message || "Unknown error";
      job.completedAt = new Date().toISOString();
    });
  } catch (e: any) {
    console.error("Bank statement import error:", e);
    res.status(500).json({ error: e.message });
  }
});

function openZipStreaming(zipPath: string): Promise<yauzl.ZipFile> {
  return new Promise((resolve, reject) => {
    yauzl.open(zipPath, { lazyEntries: true, autoClose: false }, (err, zipfile) => {
      if (err || !zipfile) return reject(err || new Error("Failed to open ZIP"));
      resolve(zipfile);
    });
  });
}

function streamEntryToDisk(zipfile: yauzl.ZipFile, entry: yauzl.Entry, destPath: string): Promise<void> {
  return new Promise((resolve, reject) => {
    zipfile.openReadStream(entry, (err, readStream) => {
      if (err || !readStream) return reject(err || new Error("No read stream"));
      const dir = path.dirname(destPath);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      const ws = fs.createWriteStream(destPath);
      readStream.pipe(ws);
      ws.on("finish", resolve);
      ws.on("error", reject);
      readStream.on("error", reject);
    });
  });
}

const VALID_EXTS = new Set([".pdf", ".png", ".jpg", ".jpeg", ".tiff", ".tif", ".csv", ".xls", ".xlsx", ".doc", ".docx", ".heic", ".webp"]);

function extractBusinessNameFromInnerZip(zipFileName: string): string {
  const extracted = extractBusinessFromFolder(zipFileName);
  return extracted || zipFileName.replace(/\.zip$/i, "").trim() || zipFileName;
}

async function extractInnerZip(innerZipPath: string, extractDir: string, businessName: string): Promise<ExtractedFile[]> {
  const results: ExtractedFile[] = [];

  const INNER_ZIP_TIMEOUT = 120000;
  const extractPromise = (async () => {
    const innerZipfile = await openZipStreaming(innerZipPath);

    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        try { innerZipfile.close(); } catch {}
        reject(new Error(`Inner ZIP extraction timed out after ${INNER_ZIP_TIMEOUT / 1000}s`));
      }, INNER_ZIP_TIMEOUT);

      innerZipfile.on("error", (err) => { clearTimeout(timeout); reject(err); });
      innerZipfile.on("end", () => { clearTimeout(timeout); resolve(); });

      const processInnerEntry = (entry: yauzl.Entry) => {
        const entryName = entry.fileName;
        if (/\/$/.test(entryName)) { innerZipfile.readEntry(); return; }

        const baseName = path.basename(entryName);
        if (baseName.startsWith(".") || baseName.startsWith("__")) { innerZipfile.readEntry(); return; }

        const ext = path.extname(baseName).toLowerCase();
        if (!VALID_EXTS.has(ext)) { innerZipfile.readEntry(); return; }

        const safeName = baseName.replace(/[<>:"/\\|?*]/g, "_");
        const safeFolder = businessName.replace(/[<>:"/\\|?*]/g, "_");
        const diskPath = path.join(extractDir, safeFolder, safeName);

        streamEntryToDisk(innerZipfile, entry, diskPath).then(() => {
          results.push({ entryName: `${businessName}/${baseName}`, diskPath, fileName: baseName, folderName: businessName, tier: null });
          innerZipfile.readEntry();
        }).catch((err) => {
          console.error(`[ZIP Import] Failed to extract inner entry ${entryName}:`, err.message);
          innerZipfile.readEntry();
        });
      };

      innerZipfile.on("entry", processInnerEntry);
      innerZipfile.readEntry();
    });

    innerZipfile.close();
  })();

  await extractPromise;
  return results;
}

const isDateOrGenericFolder = (name: string): boolean => {
  const n = name.trim();
  if (/^A[1-9]$/i.test(n)) return true;
  if (/^\d{1,2}[-\/]\d{1,2}[-\/]\d{2,4}$/.test(n)) return true;
  if (/^\d{4}[-\/]\d{1,2}[-\/]\d{1,2}$/.test(n)) return true;
  if (/^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*[\s_-]*\d{0,4}$/i.test(n)) return true;
  if (/^(q[1-4]|quarter|batch|upload|files|documents|statements|data|export|onedrive|downloads?)[\s_-]*/i.test(n)) return true;
  if (/^OneDrive/i.test(n)) return true;
  if (/^\d+$/.test(n)) return true;
  return false;
};

interface ExtractedFile {
  entryName: string;
  diskPath: string;
  fileName: string;
  folderName: string | null;
  tier: string | null;
}

async function autoStartScrubbing(leadIds: number[], zipJob: ZipImportJob) {
  const running = [...backgroundJobs.values()].find(j => j.status === "running");
  if (running) {
    console.log("[Auto-Scrub] Analysis job already running, will queue for resume later");
    zipJob.status = "completed";
    zipJob.phase = "done";
    zipJob.completedAt = new Date().toISOString();
    return;
  }

  const unanalyzed: number[] = [];
  for (const leadId of leadIds) {
    const existing = await db.select({ id: bankStatementAnalysesTable.id })
      .from(bankStatementAnalysesTable)
      .where(eq(bankStatementAnalysesTable.leadId, leadId))
      .limit(1);
    if (existing.length === 0) unanalyzed.push(leadId);
  }

  if (unanalyzed.length === 0) {
    console.log("[Auto-Scrub] All matched leads already analyzed");
    zipJob.status = "completed";
    zipJob.phase = "done";
    zipJob.completedAt = new Date().toISOString();
    return;
  }

  const jobId = `auto_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  resetScrubCancel();
  const job: BackgroundJob = {
    id: jobId, status: "running", total: unanalyzed.length,
    processed: 0, currentLead: "Starting auto-scrubbing...", results: [], startedAt: Date.now(),
  };
  backgroundJobs.set(jobId, job);

  zipJob.phase = "scrubbing";
  zipJob.status = "processing";
  zipJob.totalLeadsToScrub = unanalyzed.length;
  zipJob.scrubbedLeads = 0;
  zipJob.completedAt = null;

  console.log(`[Auto-Scrub] Starting AI analysis for ${unanalyzed.length} leads (job: ${jobId})`);

  try {
    await runConcurrentBatch(unanalyzed, async (leadId: number) => {
      let leadName = "Unknown";
      try {
        const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
        if (!lead) { job.results.push({ leadId, businessName: "Unknown", status: "error", error: "Lead not found" }); job.processed++; return; }
        leadName = lead.businessName || "Unknown";
        job.currentLead = leadName;
        zipJob.currentScrubLead = leadName;

        const { analysis } = await analyzeSingleLead(leadId);
        const isEmpty = !analysis.grossRevenue && !analysis.hasExistingLoans;
        if (isEmpty) {
          console.log(`[Auto-Scrub] Empty analysis for "${leadName}" (no revenue, no loans) — resetting to new`);
          await db.update(leadsTable).set({ status: "new" }).where(eq(leadsTable.id, leadId));
          job.results.push({ leadId, businessName: leadName, status: "empty", riskScore: analysis.riskScore });
        } else {
          await db.update(leadsTable).set({ status: "scrubbing_review" }).where(eq(leadsTable.id, leadId));
          job.results.push({ leadId, businessName: leadName, status: "analyzed", riskScore: analysis.riskScore });
        }
      } catch (e: any) {
        try {
          const existing = await db.select({ id: bankStatementAnalysesTable.id, riskScore: bankStatementAnalysesTable.riskScore })
            .from(bankStatementAnalysesTable).where(eq(bankStatementAnalysesTable.leadId, leadId)).limit(1);
          if (existing.length > 0) {
            await db.update(leadsTable).set({ status: "scrubbing_review" }).where(eq(leadsTable.id, leadId));
            job.results.push({ leadId, businessName: leadName, status: "analyzed", riskScore: existing[0].riskScore ?? undefined, note: "partial — some batches failed" });
          } else {
            job.results.push({ leadId, businessName: leadName, status: e.message === "No bank statements found" ? "skipped" : "error", error: e.message });
          }
        } catch (dbErr: any) {
          console.error(`[Auto-Scrub] DB error in catch for lead ${leadId}:`, dbErr.message);
          job.results.push({ leadId, businessName: leadName, status: "error", error: e.message });
        }
      }
      job.processed++;
      zipJob.scrubbedLeads = job.processed;
    }, 5);

    job.status = "completed";
    job.completedAt = Date.now();
    job.currentLead = "All done!";
    zipJob.phase = "done";
    zipJob.status = "completed";
    zipJob.completedAt = new Date().toISOString();
    console.log(`[Auto-Scrub] Completed: ${job.results.filter(r => r.status === "analyzed").length} analyzed, ${job.results.filter(r => r.status === "error").length} errors`);
    setTimeout(() => backgroundJobs.delete(jobId), 30 * 60 * 1000);
  } catch (e: any) {
    job.status = "error"; job.error = e.message; job.completedAt = Date.now();
    zipJob.phase = "done"; zipJob.status = "completed"; zipJob.completedAt = new Date().toISOString();
    console.error("[Auto-Scrub] Error:", e.message);
  }
}

export async function resumeIncompleteScrubbing() {
  try {
    if (isStorageConfigured()) {
      try {
        const docsWithoutStorage = await db.select({ id: documentsTable.id, url: documentsTable.url, name: documentsTable.name })
          .from(documentsTable)
          .where(and(
            eq(documentsTable.type, "bank_statement"),
            isNull(documentsTable.storageKey),
          ));
        if (docsWithoutStorage.length > 0) {
          console.log(`[Resume-Scrub] Found ${docsWithoutStorage.length} docs without storage keys, uploading first...`);
          let uploaded = 0;
          for (const doc of docsWithoutStorage) {
            try {
              const localPath = path.resolve(process.cwd(), doc.url.startsWith("/") ? doc.url.slice(1) : doc.url);
              if (fs.existsSync(localPath)) {
                const storagePath = doc.url.replace(/^\/uploads\/extracted\//, "");
                const key = await uploadLocalFileAndGetKey(localPath, storagePath);
                if (key) {
                  await db.update(documentsTable).set({ storageKey: key }).where(eq(documentsTable.id, doc.id));
                  uploaded++;
                }
              }
            } catch {}
          }
          if (uploaded > 0) console.log(`[Resume-Scrub] Uploaded ${uploaded} docs to storage`);
        }
      } catch (e: any) {
        console.error("[Resume-Scrub] Storage upload check error:", e.message);
      }
    }

    const result = await db.execute(sql`
      SELECT DISTINCT d.lead_id
      FROM documents d
      LEFT JOIN bank_statement_analyses bsa ON bsa.lead_id = d.lead_id
      WHERE d.type = 'bank_statement'
        AND bsa.id IS NULL
        AND d.lead_id IS NOT NULL
    `);
    const rows = (result as any).rows || result;
    if (!Array.isArray(rows) || rows.length === 0) {
      console.log("[Resume-Scrub] No incomplete scrub jobs found.");
      return;
    }

    const leadIds = rows.map((r: any) => Number(r.lead_id)).filter((id: number) => !isNaN(id));
    console.log(`[Resume-Scrub] Found ${leadIds.length} leads with bank statements but no analysis — resuming scrubbing...`);

    const running = [...backgroundJobs.values()].find(j => j.status === "running");
    if (running) {
      console.log("[Resume-Scrub] A scrub job is already running, skipping resume.");
      return;
    }

    const jobId = `resume_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    resetScrubCancel();
    const job: BackgroundJob = {
      id: jobId, status: "running", total: leadIds.length,
      processed: 0, currentLead: "Resuming scrubbing...", results: [], startedAt: Date.now(),
    };
    backgroundJobs.set(jobId, job);

    await runConcurrentBatch(leadIds, async (leadId: number) => {
      let leadName = "Unknown";
      try {
        const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
        if (!lead) { job.results.push({ leadId, businessName: "Unknown", status: "error", error: "Lead not found" }); return; }
        leadName = lead.businessName || "Unknown";
        job.currentLead = leadName;

        const { analysis } = await analyzeSingleLead(leadId);
        const isEmpty = !analysis.grossRevenue && !analysis.hasExistingLoans;
        if (isEmpty) {
          console.log(`[Resume-Scrub] Empty analysis for "${leadName}" (no revenue, no loans) — resetting to new`);
          await db.update(leadsTable).set({ status: "new" }).where(eq(leadsTable.id, leadId));
          job.results.push({ leadId, businessName: leadName, status: "empty", riskScore: analysis.riskScore });
        } else {
          await db.update(leadsTable).set({ status: "scrubbing_review" }).where(eq(leadsTable.id, leadId));
          job.results.push({ leadId, businessName: leadName, status: "analyzed", riskScore: analysis.riskScore });
        }
      } catch (e: any) {
        try {
          const existing = await db.select({ id: bankStatementAnalysesTable.id, riskScore: bankStatementAnalysesTable.riskScore })
            .from(bankStatementAnalysesTable).where(eq(bankStatementAnalysesTable.leadId, leadId)).limit(1);
          if (existing.length > 0) {
            await db.update(leadsTable).set({ status: "scrubbing_review" }).where(eq(leadsTable.id, leadId));
            job.results.push({ leadId, businessName: leadName, status: "analyzed", riskScore: existing[0].riskScore ?? undefined, note: "partial" });
          } else {
            job.results.push({ leadId, businessName: leadName, status: e.message === "No bank statements found" ? "skipped" : "error", error: e.message });
          }
        } catch (dbErr: any) {
          console.error(`[Resume-Scrub] DB error in catch for lead ${leadId}:`, dbErr.message);
          job.results.push({ leadId, businessName: leadName, status: "error", error: e.message });
        }
      } finally {
        job.processed++;
      }
    }, 5);

    job.status = "completed";
    job.completedAt = Date.now();
    job.currentLead = "All done!";
    const analyzed = job.results.filter(r => r.status === "analyzed").length;
    const errors = job.results.filter(r => r.status === "error").length;
    console.log(`[Resume-Scrub] Completed: ${analyzed} analyzed, ${errors} errors, ${job.results.filter(r => r.status === "skipped").length} skipped`);
    setTimeout(() => backgroundJobs.delete(jobId), 30 * 60 * 1000);
  } catch (e: any) {
    console.error("[Resume-Scrub] Error:", e.message);
  }
}

async function processZipInBackground(job: ZipImportJob, tempFilePath: string, originalFileName: string, batchId?: number) {
  const extractDir = path.join(UPLOADS_DIR, "extracted", `batch-${Date.now()}`);
  fs.mkdirSync(extractDir, { recursive: true });

  cleanupOldExtractDirs(60 * 60 * 1000);

  try {
    let detectedSourceTier: string | null = null;
    if (/\bA1\b/i.test(originalFileName)) detectedSourceTier = "A1";
    else if (/\bA2\b/i.test(originalFileName)) detectedSourceTier = "A2";

    job.phase = "extracting";

    const zipfile = await openZipStreaming(tempFilePath);
    const extractedFiles: ExtractedFile[] = [];
    let totalEntries = 0;

    const innerZipPaths: { diskPath: string; businessName: string; tier: string | null }[] = [];
    let skippedExts: string[] = [];

    await new Promise<void>((resolve, reject) => {
      zipfile.on("error", reject);
      zipfile.on("end", resolve);

      const processEntry = (entry: yauzl.Entry) => {
        totalEntries++;
        job.totalEntries = totalEntries;
        job.extractedEntries = totalEntries;

        const entryName = entry.fileName;
        if (/\/$/.test(entryName)) {
          zipfile.readEntry();
          return;
        }

        const baseName = path.basename(entryName);
        if (baseName.startsWith(".") || baseName.startsWith("__")) {
          zipfile.readEntry();
          return;
        }

        const ext = path.extname(baseName).toLowerCase();

        if (ext === ".zip") {
          const businessName = extractBusinessNameFromInnerZip(baseName);
          const pathParts = entryName.replace(/\\/g, "/").split("/").filter(p => p);
          let innerTier: string | null = null;
          for (const pp of pathParts) {
            if (/^A1$/i.test(pp.trim())) { innerTier = "A1"; break; }
            if (/^A2$/i.test(pp.trim())) { innerTier = "A2"; break; }
          }
          const safeInnerName = baseName.replace(/[^a-zA-Z0-9._-]/g, "_").replace(/_+/g, "_");
          const innerDiskPath = path.join(extractDir, "__inner_zips", safeInnerName);
          streamEntryToDisk(zipfile, entry, innerDiskPath).then(() => {
            innerZipPaths.push({ diskPath: innerDiskPath, businessName, tier: innerTier });
            zipfile.readEntry();
          }).catch((err) => {
            console.error(`[ZIP Import] Failed to extract inner zip ${businessName}:`, err);
            zipfile.readEntry();
          });
          return;
        }

        if (!VALID_EXTS.has(ext)) {
          if (skippedExts.length < 10) skippedExts.push(`${ext}:${baseName}`);
          zipfile.readEntry();
          return;
        }

        const parts = entryName.replace(/\\/g, "/").split("/").filter(p => p && !p.startsWith(".") && !p.startsWith("__"));

        let entryTier: string | null = null;
        for (const part of parts) {
          if (/^A1$/i.test(part)) { entryTier = "A1"; break; }
          if (/^A2$/i.test(part)) { entryTier = "A2"; break; }
        }

        let folderName: string | null = null;
        if (parts.length >= 2) {
          const parentFolder = parts[parts.length - 2];
          if (!isDateOrGenericFolder(parentFolder)) {
            folderName = parentFolder;
          } else {
            for (let i = parts.length - 3; i >= 0; i--) {
              if (!isDateOrGenericFolder(parts[i])) {
                folderName = parts[i];
                break;
              }
            }
          }
          if (!folderName) folderName = parentFolder;
        }

        const safeName = baseName.replace(/[<>:"/\\|?*]/g, "_");
        const safeFolder = folderName ? folderName.replace(/[<>:"/\\|?*]/g, "_") : "__toplevel";
        const diskPath = path.join(extractDir, safeFolder, safeName);

        streamEntryToDisk(zipfile, entry, diskPath).then(() => {
          extractedFiles.push({ entryName, diskPath, fileName: baseName, folderName, tier: entryTier });
          zipfile.readEntry();
        }).catch((err) => {
          console.error(`[ZIP Import] Failed to extract ${entryName}:`, err);
          zipfile.readEntry();
        });
      };

      zipfile.on("entry", processEntry);
      zipfile.readEntry();
    });

    zipfile.close();
    if (fs.existsSync(tempFilePath)) {
      try { fs.unlinkSync(tempFilePath); } catch {}
    }

    if (innerZipPaths.length > 0) {
      console.log(`[ZIP Import] Found ${innerZipPaths.length} inner ZIP files, processing sequentially for reliability...`);
      job.phase = "extracting_inner";
      job.innerZipsTotal = innerZipPaths.length;
      job.innerZipsDone = 0;
      const tierCounts: Record<string, number> = {};
      let innerErrors = 0;
      const GC_BATCH_SIZE = 25;

      for (let i = 0; i < innerZipPaths.length; i++) {
        const iz = innerZipPaths[i];
        try {
          job.currentFolder = `${iz.businessName} (${i + 1}/${innerZipPaths.length})`;
          const innerFiles = await extractInnerZip(iz.diskPath, extractDir, iz.businessName);
          for (const f of innerFiles) f.tier = iz.tier || f.tier;
          if (iz.tier) tierCounts[iz.tier] = (tierCounts[iz.tier] || 0) + 1;
          extractedFiles.push(...innerFiles);
        } catch (err: any) {
          innerErrors++;
          console.error(`[ZIP Import] Inner ZIP failed (${i + 1}/${innerZipPaths.length}) ${iz.businessName}:`, err.message);
        } finally {
          try { if (fs.existsSync(iz.diskPath)) fs.unlinkSync(iz.diskPath); } catch {}
          job.innerZipsDone = i + 1;
        }

        if (i > 0 && i % GC_BATCH_SIZE === 0) {
          try { if (global.gc) global.gc(); } catch {}
          await new Promise(r => setTimeout(r, 10));
        }

        if (i > 0 && i % 50 === 0) {
          const memMB = Math.round(process.memoryUsage.rss() / 1024 / 1024);
          console.log(`[ZIP Import] Inner ZIP progress: ${i + 1}/${innerZipPaths.length} done, ${extractedFiles.length} files extracted, ${innerErrors} errors, RSS: ${memMB}MB`);
        }
      }

      console.log(`[ZIP Import] Tier breakdown: ${JSON.stringify(tierCounts)}`);
      const innerZipDir = path.join(extractDir, "__inner_zips");
      try { fs.rmSync(innerZipDir, { recursive: true, force: true }); } catch {}
      console.log(`[ZIP Import] Inner ZIP extraction complete: ${extractedFiles.length} total files, ${innerErrors} errors out of ${innerZipPaths.length}`);
    }

    if (skippedExts.length > 0) {
      console.log(`[ZIP Import] Skipped extensions sample: ${skippedExts.slice(0, 10).join(", ")}`);
    }
    console.log(`[ZIP Import] Extracted ${extractedFiles.length} valid files from ${totalEntries} entries (${innerZipPaths.length} inner ZIPs)`);

    const folderMap: Record<string, ExtractedFile[]> = {};
    const folderTiers: Record<string, string> = {};
    const topLevelFiles: ExtractedFile[] = [];

    const samplePaths = extractedFiles.slice(0, 10).map(f => f.entryName);
    const extractedFileCount = extractedFiles.length;

    for (const ef of extractedFiles) {
      if (ef.folderName) {
        if (!folderMap[ef.folderName]) folderMap[ef.folderName] = [];
        folderMap[ef.folderName].push(ef);
        if (ef.tier) folderTiers[ef.folderName] = ef.tier;
      } else {
        topLevelFiles.push(ef);
      }
    }
    extractedFiles.length = 0;
    try { if (global.gc) global.gc(); } catch {}

    job.totalFolders = Object.keys(folderMap).length + (topLevelFiles.length > 0 && Object.keys(folderMap).length === 0 ? topLevelFiles.length : 0);
    job.phase = "matching";

    console.log(`[ZIP Import] File: ${originalFileName}, Entries: ${totalEntries}, Valid files: ${extractedFileCount}, Folders found: ${Object.keys(folderMap).length}, Top-level files: ${topLevelFiles.length}`);
    const results = {
      totalFolders: Object.keys(folderMap).length,
      totalFiles: 0,
      matched: 0,
      matchedFolders: 0,
      unmatchedFolders: [] as string[],
      matchDetails: [] as { folder: string; leadId: number; businessName: string; fileCount: number; documentIds: number[] }[],
      topLevelFiles: topLevelFiles.length,
      sourceTier: detectedSourceTier,
      debug: {
        totalEntries,
        samplePaths,
        folderNames: Object.keys(folderMap).slice(0, 20),
      },
    };

    if (Object.keys(folderMap).length > 0) {
      for (const [folderName, folderFiles] of Object.entries(folderMap)) {
        job.processedFolders++;
        job.currentFolder = folderName;

        const lead = await fuzzyMatchLead(folderName);

        if (!lead) {
          results.unmatchedFolders.push(folderName);
          continue;
        }

        results.matchedFolders++;
        job.matchedFolders = results.matchedFolders;
        const docIds: number[] = [];

        const tierForFolder = folderTiers[folderName] || detectedSourceTier;
        if (tierForFolder) {
          const updateFields: Record<string, any> = { sourceTier: tierForFolder };
          if (tierForFolder === "A1") {
            updateFields.riskCategory = "low";
          } else if (tierForFolder === "A2") {
            updateFields.riskCategory = "medium";
          }
          await db.update(leadsTable).set(updateFields).where(eq(leadsTable.id, lead.id));
        }

        const existingDocs = await db.select({ id: documentsTable.id, name: documentsTable.name })
          .from(documentsTable)
          .where(and(eq(documentsTable.leadId, lead.id), eq(documentsTable.type, "bank_statement")));
        const existingNames = new Set(existingDocs.map(d => d.name));

        const newFiles = folderFiles.filter(ef => !existingNames.has(ef.fileName));
        const insertValues = newFiles.map(ef => {
          const relPath = path.relative(extractDir, ef.diskPath).replace(/\\/g, "/");
          return {
            leadId: lead.id,
            type: "bank_statement" as const,
            name: ef.fileName,
            url: `/uploads/extracted/${path.basename(extractDir)}/${relPath}`,
          };
        });

        const reuploadedDocs = existingDocs.filter(d => folderFiles.some(f => f.fileName === d.name));
        for (const doc of reuploadedDocs) {
          docIds.push(doc.id);
          const matchedFile = folderFiles.find(f => f.fileName === doc.name);
          if (matchedFile) {
            const relPath = path.relative(extractDir, matchedFile.diskPath).replace(/\\/g, "/");
            const newUrl = `/uploads/extracted/${path.basename(extractDir)}/${relPath}`;
            await db.update(documentsTable).set({ url: newUrl, storageKey: null }).where(eq(documentsTable.id, doc.id));
          }
        }

        if (insertValues.length > 0) {
          const docs = await db.insert(documentsTable).values(insertValues).returning();
          for (const doc of docs) docIds.push(doc.id);
        }
        results.totalFiles += folderFiles.length;
        results.matched += folderFiles.length;
        job.totalFiles = results.totalFiles;
        job.matched = results.matched;

        results.matchDetails.push({
          folder: folderName,
          leadId: lead.id,
          businessName: lead.businessName,
          fileCount: folderFiles.length,
          documentIds: docIds,
        });
      }
    }

    if (results.matchedFolders === 0 && results.unmatchedFolders.length > 0 && originalFileName) {
      const zipNameCleaned = path.parse(originalFileName).name;
      const zipLead = await fuzzyMatchLead(zipNameCleaned);
      if (zipLead) {
        const allUnmatched = [...results.unmatchedFolders];
        results.unmatchedFolders = [];
        for (const folderName of allUnmatched) {
          const folderFiles = folderMap[folderName];
          if (!folderFiles) continue;
          results.matchedFolders++;
          job.matchedFolders = results.matchedFolders;
          job.processedFolders++;
          job.currentFolder = folderName;
          const docIds: number[] = [];
          const tierForFolder = folderTiers[folderName] || detectedSourceTier;
          if (tierForFolder) {
            const updateFields: Record<string, any> = { sourceTier: tierForFolder };
            if (tierForFolder === "A1") {
              updateFields.riskCategory = "low";
            } else if (tierForFolder === "A2") {
              updateFields.riskCategory = "medium";
            }
            await db.update(leadsTable).set(updateFields).where(eq(leadsTable.id, zipLead.id));
          }
          const insertVals = folderFiles.map(ef => {
            const relPath = path.relative(extractDir, ef.diskPath).replace(/\\/g, "/");
            return {
              leadId: zipLead.id,
              type: "bank_statement" as const,
              name: ef.fileName,
              url: `/uploads/extracted/${path.basename(extractDir)}/${relPath}`,
            };
          });
          if (insertVals.length > 0) {
            const docs = await db.insert(documentsTable).values(insertVals).returning();
            for (const doc of docs) docIds.push(doc.id);
            results.totalFiles += insertVals.length;
            results.matched += insertVals.length;
            job.totalFiles = results.totalFiles;
            job.matched = results.matched;
          }
          results.matchDetails.push({
            folder: folderName,
            leadId: zipLead.id,
            businessName: zipLead.businessName,
            fileCount: folderFiles.length,
            documentIds: docIds,
          });
        }
      }
    }

    if (topLevelFiles.length > 0 && Object.keys(folderMap).length === 0) {
      for (const ef of topLevelFiles) {
        results.totalFiles++;
        job.totalFiles = results.totalFiles;
        job.processedFolders++;
        job.currentFolder = ef.fileName;

        const nameWithoutExt = path.parse(ef.fileName).name.replace(/[-_]+/g, " ").replace(/\s+/g, " ").trim();
        const lead = await fuzzyMatchLead(nameWithoutExt);

        if (!lead) {
          results.unmatchedFolders.push(ef.fileName);
          continue;
        }

        if (detectedSourceTier) {
          const updateFields: Record<string, any> = { sourceTier: detectedSourceTier };
          if (detectedSourceTier === "A1") {
            updateFields.riskCategory = "low";
          } else if (detectedSourceTier === "A2") {
            updateFields.riskCategory = "medium";
          }
          await db.update(leadsTable).set(updateFields).where(eq(leadsTable.id, lead.id));
        }

        const relPath = path.relative(extractDir, ef.diskPath).replace(/\\/g, "/");
        const [doc] = await db.insert(documentsTable).values({
          leadId: lead.id,
          type: "bank_statement",
          name: ef.fileName,
          url: `/uploads/extracted/${path.basename(extractDir)}/${relPath}`,
        }).returning();

        results.matched++;
        job.matched = results.matched;
        results.matchDetails.push({
          folder: ef.fileName,
          leadId: lead.id,
          businessName: lead.businessName,
          fileCount: 1,
          documentIds: [doc.id],
        });
      }
    }

    if (fs.existsSync(tempFilePath)) {
      fs.unlinkSync(tempFilePath);
    }

    job.result = results;
    console.log(`[ZIP Import] Completed: ${results.matchedFolders}/${results.totalFolders} folders matched, ${results.matched} files`);

    if (batchId) {
      try {
        await db.update(uploadBatchesTable).set({
          status: "completed",
          totalFolders: results.totalFolders,
          matchedFolders: results.matchedFolders,
          totalFiles: results.totalFiles || results.matched,
          matchedFiles: results.matched,
          unmatchedFolders: results.unmatchedFolders,
          matchDetails: results.matchDetails,
          sourceTier: results.sourceTier,
          extractDir: extractDir,
          completedAt: new Date(),
        }).where(eq(uploadBatchesTable.id, batchId));
      } catch (e: any) { console.error("[Upload Batch] Failed to update batch:", e.message); }
    }

    try {
      await uploadDocsToStorage(extractDir, job);
    } catch (err: any) {
      console.error("[Storage Upload] Failed:", err.message);
    }

    const matchedLeadIds = [...new Set(results.matchDetails.filter((m: any) => m.leadId).map((m: any) => m.leadId))];
    if (matchedLeadIds.length > 0) {
      try {
        await autoStartScrubbing(matchedLeadIds as number[], job);
      } catch (err: any) {
        console.error("[Auto-Scrub] Failed to start:", err.message);
        job.status = "completed";
        job.phase = "done";
        job.completedAt = new Date().toISOString();
      }
    } else {
      job.status = "completed";
      job.phase = "done";
      job.completedAt = new Date().toISOString();
    }
  } catch (e: any) {
    if (fs.existsSync(tempFilePath)) {
      try { fs.unlinkSync(tempFilePath); } catch {}
    }
    console.error("Bank statement import error:", e);
    job.status = "error";
    job.error = e.message;
    job.completedAt = new Date().toISOString();
    if (batchId) {
      try {
        await db.update(uploadBatchesTable).set({
          status: "error", error: e.message, completedAt: new Date(),
        }).where(eq(uploadBatchesTable.id, batchId));
      } catch {}
    }
  }
}

const folderUpload = multer({
  storage: multer.diskStorage({
    destination: (_req, _file, cb) => cb(null, TEMP_DIR),
    filename: (_req, file, cb) => cb(null, `folder_${Date.now()}_${Math.random().toString(36).slice(2, 6)}_${file.originalname.replace(/[^a-zA-Z0-9._-]/g, "_")}`),
  }),
  limits: { fileSize: 500 * 1024 * 1024 },
});

const folderStagingJobs = new Map<string, { dir: string; relativePaths: string[]; createdAt: number }>();

setInterval(() => {
  const oneHourAgo = Date.now() - 60 * 60 * 1000;
  for (const [id, job] of folderStagingJobs) {
    if (job.createdAt < oneHourAgo) {
      folderStagingJobs.delete(id);
      try { fs.rmSync(job.dir, { recursive: true, force: true }); } catch {}
    }
  }
}, 10 * 60 * 1000);

router.post("/import/folder-batch-upload", requireAuth, requireAdmin, requirePermission("import"), folderUpload.array("files", 200), async (req, res): Promise<void> => {
  const uploadedFiles = req.files as Express.Multer.File[] | undefined;
  try {
    if (!uploadedFiles || uploadedFiles.length === 0) {
      res.status(400).json({ error: "No files uploaded" });
      return;
    }

    let relativePaths: string[];
    try {
      relativePaths = JSON.parse(req.body.relativePaths || "[]");
      if (!Array.isArray(relativePaths) || relativePaths.length !== uploadedFiles.length) {
        res.status(400).json({ error: "relativePaths must match file count" });
        return;
      }
    } catch {
      res.status(400).json({ error: "Invalid relativePaths JSON" });
      return;
    }

    const batchId = req.body.batchId || `fb-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    let staging = folderStagingJobs.get(batchId);
    if (!staging) {
      staging = { dir: path.join(TEMP_DIR, `folder_staging_${batchId}`), relativePaths: [], createdAt: Date.now() };
      fs.mkdirSync(staging.dir, { recursive: true });
      folderStagingJobs.set(batchId, staging);
    }

    for (let i = 0; i < uploadedFiles.length; i++) {
      const file = uploadedFiles[i];
      const destName = `${staging.relativePaths.length}_${file.originalname.replace(/[^a-zA-Z0-9._-]/g, "_")}`;
      const destPath = path.join(staging.dir, destName);
      fs.renameSync(file.path, destPath);
      staging.relativePaths.push(relativePaths[i] || file.originalname);
    }

    res.json({ batchId, received: uploadedFiles.length, totalStaged: staging.relativePaths.length });
  } catch (e: any) {
    if (uploadedFiles) {
      for (const f of uploadedFiles) {
        try { if (fs.existsSync(f.path)) fs.unlinkSync(f.path); } catch {}
      }
    }
    console.error("Folder batch upload error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.post("/import/folder-batch-finalize", requireAuth, requireAdmin, requirePermission("import"), async (req, res): Promise<void> => {
  let stagingDir: string | null = null;
  try {
    const { batchId } = req.body;
    if (!batchId) { res.status(400).json({ error: "batchId required" }); return; }

    const staging = folderStagingJobs.get(batchId);
    if (!staging) { res.status(404).json({ error: "Batch not found or expired" }); return; }
    stagingDir = staging.dir;

    const lockCheck = await isImportActive();
    if (lockCheck.active) {
      res.status(409).json({ error: `An import is already in progress (${lockCheck.fileName}). Please wait for it to finish.` });
      return;
    }

    folderStagingJobs.delete(batchId);

    const stagedFiles = fs.readdirSync(staging.dir).filter((f: string) => !f.startsWith(".")).sort();
    if (stagedFiles.length === 0) {
      res.status(400).json({ error: "No files staged" });
      return;
    }

    const uploadedFiles: { path: string; originalname: string }[] = stagedFiles.map((f: string) => ({
      path: path.join(staging.dir, f),
      originalname: f.replace(/^\d+_/, ""),
    }));
    const relativePaths = staging.relativePaths;

    const extractDir = path.join(UPLOADS_DIR, "extracted", `batch-${Date.now()}`);
    const extractDirResolved = path.resolve(extractDir);
    fs.mkdirSync(extractDir, { recursive: true });

    const ALLOWED_EXTS = [".pdf", ".png", ".jpg", ".jpeg", ".tiff", ".tif", ".csv", ".xls", ".xlsx", ".doc", ".docx", ".heic", ".webp", ".zip"];

    const folderMap: Record<string, { file: { path: string; originalname: string }; relativePath: string }[]> = {};
    const folderTiers: Record<string, string> = {};
    const topLevelFiles: { file: { path: string; originalname: string }; relativePath: string }[] = [];
    let globalTier: string | null = null;

    const isDateOrGenericFolder = (name: string): boolean => {
      const n = name.trim();
      if (/^A[1-9]$/i.test(n)) return true;
      if (/^\d{1,2}[-\/]\d{1,2}[-\/]\d{2,4}$/.test(n)) return true;
      if (/^\d{4}[-\/]\d{1,2}[-\/]\d{1,2}$/.test(n)) return true;
      if (/^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*[\s_-]*\d{0,4}$/i.test(n)) return true;
      if (/^(q[1-4]|quarter|batch|upload|files|documents|statements|data|export|onedrive|downloads?)[\s_-]*/i.test(n)) return true;
      if (/^OneDrive/i.test(n)) return true;
      if (/^\d+$/.test(n)) return true;
      return false;
    };

    const sanitizeSegment = (s: string): string => {
      if (s === "." || s === "..") return "";
      return s.replace(/[<>:"/\\|?*]/g, "_");
    };

    let fileCounter = 0;

    for (let i = 0; i < uploadedFiles.length; i++) {
      const file = uploadedFiles[i];
      const relPath = relativePaths[i] || file.originalname;
      const fileName = path.basename(relPath);
      if (fileName.startsWith(".") || fileName.startsWith("__")) continue;
      const ext = path.extname(fileName).toLowerCase();
      if (!ALLOWED_EXTS.includes(ext)) continue;

      const parts = relPath.replace(/\\/g, "/").split("/")
        .map((p: string) => sanitizeSegment(p))
        .filter((p: string) => p && !p.startsWith(".") && !p.startsWith("__"));

      let entryTier: string | null = null;
      for (const part of parts) {
        if (/^A1$/i.test(part)) { entryTier = "A1"; break; }
        if (/^A2$/i.test(part)) { entryTier = "A2"; break; }
      }
      if (entryTier && !globalTier) globalTier = entryTier;

      let folderName: string | null = null;
      if (parts.length >= 2) {
        const partsWithoutFile = parts.slice(0, -1);
        const parentFolder = partsWithoutFile[partsWithoutFile.length - 1];
        if (!isDateOrGenericFolder(parentFolder)) {
          folderName = parentFolder;
        } else {
          for (let j = partsWithoutFile.length - 2; j >= 0; j--) {
            if (!isDateOrGenericFolder(partsWithoutFile[j])) {
              folderName = partsWithoutFile[j];
              break;
            }
          }
        }
        if (!folderName && partsWithoutFile.length > 1 && !isDateOrGenericFolder(parentFolder)) {
          folderName = parentFolder;
        }
      }

      if (folderName) {
        if (!folderMap[folderName]) folderMap[folderName] = [];
        folderMap[folderName].push({ file, relativePath: relPath });
        if (entryTier) folderTiers[folderName] = entryTier;
      } else {
        topLevelFiles.push({ file, relativePath: relPath });
      }
    }

    console.log(`[Folder Import] Files: ${uploadedFiles.length}, Folders found: ${Object.keys(folderMap).length}, Top-level files: ${topLevelFiles.length}`);
    console.log(`[Folder Import] Folder names:`, Object.keys(folderMap).slice(0, 20));

    const topLevelZips = topLevelFiles.filter(f => path.extname(f.file.originalname).toLowerCase() === ".zip");
    const results = {
      totalFolders: Object.keys(folderMap).length + (Object.keys(folderMap).length === 0 ? topLevelZips.length : 0),
      totalFiles: 0,
      matched: 0,
      matchedFolders: 0,
      unmatchedFolders: [] as string[],
      matchDetails: [] as { folder: string; leadId: number; businessName: string; fileCount: number; documentIds: number[] }[],
      topLevelFiles: topLevelFiles.length,
      sourceTier: globalTier,
      debug: {
        totalEntries: uploadedFiles.length,
        samplePaths: relativePaths.slice(0, 10),
        folderNames: Object.keys(folderMap).slice(0, 20),
      },
    };

    if (Object.keys(folderMap).length > 0) {
      for (const [folderName, folderEntries] of Object.entries(folderMap)) {
        const lead = await fuzzyMatchLead(folderName);
        if (lead) {
          results.matchedFolders++;
          const docIds: number[] = [];
          const tierForFolder = folderTiers[folderName] || globalTier;
          if (tierForFolder) {
            const updateFields: Record<string, any> = { sourceTier: tierForFolder };
            if (tierForFolder === "A1") updateFields.riskCategory = "low";
            else if (tierForFolder === "A2") updateFields.riskCategory = "medium";
            await db.update(leadsTable).set(updateFields).where(eq(leadsTable.id, lead.id));
          }
          const safeFolderName = sanitizeSegment(folderName);
          const folderDir = path.join(extractDir, safeFolderName);
          const folderDirResolved = path.resolve(folderDir);
          if (!folderDirResolved.startsWith(extractDirResolved)) continue;
          fs.mkdirSync(folderDir, { recursive: true });
          for (const { file } of folderEntries) {
            const fileName = path.basename(file.originalname);
            results.totalFiles++;
            fileCounter++;
            const uniqueFileName = `${fileCounter}_${fileName}`;
            const destPath = path.join(folderDir, uniqueFileName);
            fs.copyFileSync(file.path, destPath);
            const relUrl = `/uploads/extracted/${path.basename(extractDir)}/${path.basename(folderDir)}/${uniqueFileName}`;
            const [doc] = await db.insert(documentsTable).values({
              leadId: lead.id,
              type: "bank_statement",
              name: fileName,
              url: relUrl,
            }).returning();
            docIds.push(doc.id);
            results.matched++;
          }
          results.matchDetails.push({
            folder: folderName,
            leadId: lead.id,
            businessName: lead.businessName,
            fileCount: folderEntries.length,
            documentIds: docIds,
          });
        } else {
          const hasZips = folderEntries.some(e => path.extname(e.file.originalname).toLowerCase() === ".zip");
          if (hasZips) {
            console.log(`[Folder Import] Folder "${folderName}" unmatched — processing ${folderEntries.length} ZIPs individually`);
            results.totalFolders = results.totalFolders - 1 + folderEntries.length;
            for (const { file } of folderEntries) {
              const fileName = path.basename(file.originalname);
              const fileExt = path.extname(fileName).toLowerCase();
              results.totalFiles++;
              if (fileExt === ".zip") {
                const businessName = extractBusinessFromFolder(fileName);
                if (!businessName || businessName.length < 2) {
                  results.unmatchedFolders.push(fileName);
                  continue;
                }
                const zipLead = await fuzzyMatchLead(businessName);
                if (!zipLead) {
                  results.unmatchedFolders.push(fileName);
                  continue;
                }
                const tierForEntry = folderTiers[folderName] || globalTier;
                if (tierForEntry) {
                  const updateFields: Record<string, any> = { sourceTier: tierForEntry };
                  if (tierForEntry === "A1") updateFields.riskCategory = "low";
                  else if (tierForEntry === "A2") updateFields.riskCategory = "medium";
                  await db.update(leadsTable).set(updateFields).where(eq(leadsTable.id, zipLead.id));
                }
                results.matchedFolders++;
                const docIds: number[] = [];
                try {
                  const extracted = await extractInnerZip(file.path, extractDir, businessName);
                  for (const ef of extracted) {
                    const relUrl = `/uploads/extracted/${path.basename(extractDir)}/${businessName.replace(/[<>:"/\\|?*]/g, "_")}/${path.basename(ef.diskPath)}`;
                    const [doc] = await db.insert(documentsTable).values({
                      leadId: zipLead.id,
                      type: "bank_statement",
                      name: ef.fileName,
                      url: relUrl,
                    }).returning();
                    docIds.push(doc.id);
                    results.matched++;
                  }
                } catch (zipErr: any) {
                  console.error(`[Folder Import] Failed to extract ZIP ${fileName}:`, zipErr.message);
                }
                results.matchDetails.push({
                  folder: businessName,
                  leadId: zipLead.id,
                  businessName: zipLead.businessName,
                  fileCount: docIds.length,
                  documentIds: docIds,
                });
              }
            }
          } else {
            results.unmatchedFolders.push(folderName);
          }
        }
      }
    }

    if (topLevelFiles.length > 0 && Object.keys(folderMap).length === 0) {
      for (const { file } of topLevelFiles) {
        const fileName = path.basename(file.originalname);
        results.totalFiles++;
        const ext = path.extname(fileName).toLowerCase();

        if (ext === ".zip") {
          const businessName = extractBusinessFromFolder(fileName);
          if (!businessName || businessName.length < 2) {
            results.unmatchedFolders.push(fileName);
            continue;
          }
          const lead = await fuzzyMatchLead(businessName);
          if (!lead) {
            results.unmatchedFolders.push(fileName);
            continue;
          }
          if (globalTier) {
            const updateFields: Record<string, any> = { sourceTier: globalTier };
            if (globalTier === "A1") updateFields.riskCategory = "low";
            else if (globalTier === "A2") updateFields.riskCategory = "medium";
            await db.update(leadsTable).set(updateFields).where(eq(leadsTable.id, lead.id));
          }
          results.matchedFolders++;
          const docIds: number[] = [];
          try {
            const extracted = await extractInnerZip(file.path, extractDir, businessName);
            for (const ef of extracted) {
              const relUrl = `/uploads/extracted/${path.basename(extractDir)}/${businessName.replace(/[<>:"/\\|?*]/g, "_")}/${path.basename(ef.diskPath)}`;
              const [doc] = await db.insert(documentsTable).values({
                leadId: lead.id,
                type: "bank_statement",
                name: ef.fileName,
                url: relUrl,
              }).returning();
              docIds.push(doc.id);
              results.matched++;
            }
          } catch (zipErr: any) {
            console.error(`[Folder Import] Failed to extract ZIP ${fileName}:`, zipErr.message);
          }
          results.matchDetails.push({
            folder: businessName,
            leadId: lead.id,
            businessName: lead.businessName,
            fileCount: docIds.length,
            documentIds: docIds,
          });
          continue;
        }

        const nameWithoutExt = path.parse(fileName).name.replace(/[-_]+/g, " ").replace(/\s+/g, " ").trim();
        const lead = await fuzzyMatchLead(nameWithoutExt);
        if (!lead) {
          results.unmatchedFolders.push(fileName);
          continue;
        }
        fileCounter++;
        const uniqueFileName = `${fileCounter}_${fileName}`;
        const destPath = path.join(extractDir, uniqueFileName);
        fs.copyFileSync(file.path, destPath);
        const [doc] = await db.insert(documentsTable).values({
          leadId: lead.id,
          type: "bank_statement",
          name: fileName,
          url: `/uploads/extracted/${path.basename(extractDir)}/${uniqueFileName}`,
        }).returning();
        results.matched++;
        results.matchDetails.push({
          folder: fileName,
          leadId: lead.id,
          businessName: lead.businessName,
          fileCount: 1,
          documentIds: [doc.id],
        });
      }
    }

    try { fs.rmSync(staging.dir, { recursive: true, force: true }); } catch {}

    try {
      await db.insert(uploadBatchesTable).values({
        fileName: relativePaths[0]?.split("/")[0] || "Folder Upload",
        status: "completed",
        totalFolders: results.totalFolders,
        matchedFolders: results.matchedFolders,
        totalFiles: results.totalFiles,
        matchedFiles: results.matched,
        unmatchedFolders: results.unmatchedFolders,
        matchDetails: results.matchDetails,
        sourceTier: results.sourceTier,
        extractDir: extractDir,
        completedAt: new Date(),
      });
    } catch (e: any) { console.error("[Upload Batch] Failed to create folder batch:", e.message); }

    try {
      await uploadDocsToStorage(extractDir);
    } catch (err: any) {
      console.error("[Storage Upload] Folder upload storage failed:", err.message);
    }

    console.log(`[Upload] ${results.matchDetails.length} folders matched — frontend will start scrubbing via background job`);
    res.json(results);
  } catch (e: any) {
    if (stagingDir) { try { fs.rmSync(stagingDir, { recursive: true, force: true }); } catch {} }
    console.error("Folder bank statement import error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.post("/import/csv-update", requireAuth, requireAdmin, requirePermission("import"), csvUpload.single("file"), async (req, res): Promise<void> => {
  try {
    if (!req.file) {
      res.status(400).json({ error: "No file uploaded" });
      return;
    }

    const fileName = (req.file.originalname || "").toLowerCase();
    const isExcel = fileName.endsWith(".xlsx") || fileName.endsWith(".xls");

    let rawRows: string[][];
    const csvContent = !isExcel ? req.file.buffer.toString("utf-8") : "";

    if (isExcel) {
      if (fileName.endsWith(".xls")) {
        res.status(400).json({ error: "Legacy .xls format is not supported. Please convert your file to .xlsx and re-upload." });
        return;
      }
      try {
        const workbook = new ExcelJS.Workbook();
        await workbook.xlsx.load(req.file.buffer as any);
        const worksheet = workbook.worksheets[0];
        if (!worksheet) { res.status(400).json({ error: "Excel file has no sheets" }); return; }
        rawRows = [];
        worksheet.eachRow({ includeEmpty: false }, (row) => {
          const cells = (row.values as any[]).slice(1).map((cell: any) => {
            if (cell === null || cell === undefined) return "";
            if (typeof cell === "object") {
              if (Array.isArray(cell.richText)) return cell.richText.map((r: any) => r.text ?? "").join("");
              if (cell.text !== undefined) return String(cell.text);
              if (cell.result !== undefined) return String(cell.result);
              if (cell.error !== undefined) return "";
            }
            return String(cell);
          });
          rawRows.push(cells);
        });
        rawRows = rawRows.filter(r => r.some(c => String(c || "").trim()));
      } catch (e: any) {
        res.status(400).json({ error: `Excel parsing error: ${e.message}` });
        return;
      }
    } else {
      try {
        rawRows = parse(csvContent, {
          columns: false,
          skip_empty_lines: true,
          trim: true,
          relax_column_count: true,
          bom: true,
        });
      } catch (e: any) {
        res.status(400).json({ error: `CSV parsing error: ${e.message}` });
        return;
      }
    }

    const headerRow = rawRows[0];
    const useSmartFormat = isSmartFormat(headerRow);

    const results = {
      updated: 0,
      newLeads: 0,
      errors: [] as string[],
      format: useSmartFormat ? "auto-detected (deal sheet)" : "standard headers",
    };

    if (useSmartFormat) {
      const dataRows = rawRows.slice(1).filter(r => r.some(c => c.trim()));
      const parsed = dataRows.map(r => smartParseRow(r));
      const deduped = deduplicateSmartRows(parsed);

      for (let i = 0; i < deduped.length; i++) {
        const row = deduped[i];
        try {
          const ownerName = [row.firstName, row.lastName].filter(Boolean).join(" ").trim();
          const phone = row.phone ? normalizePhone(row.phone) : "";
          const normalizedSsn = row.ssn ? normalizeSSN(row.ssn) : undefined;

          if (!phone && !normalizedSsn) {
            results.errors.push(`${row.businessName || ownerName}: Missing phone and SSN`);
            continue;
          }

          const matchConditions = [];
          if (phone) matchConditions.push(sql`REGEXP_REPLACE(${leadsTable.phone}, '[^0-9]', '', 'g') = ${phone}`);
          if (normalizedSsn) matchConditions.push(sql`REGEXP_REPLACE(COALESCE(${leadsTable.ssn}, ''), '[^0-9]', '', 'g') = ${normalizedSsn}`);

          const existing = await db.select({ id: leadsTable.id }).from(leadsTable)
            .where(or(...matchConditions))
            .limit(1);

          if (existing.length > 0) {
            const updates: any = {};
            if (row.requestedAmount) updates.requestedAmount = row.requestedAmount;
            if (row.creditScore) updates.creditScore = row.creditScore;
            if (row.email) updates.email = row.email;
            if (row.ein) updates.taxId = row.ein;
            if (row.dob) updates.dob = row.dob;
            if (row.stackingNotes) updates.notes = row.stackingNotes;
            if (row.businessStartDate) updates.businessStartDate = row.businessStartDate;

            if (Object.keys(updates).length > 0) {
              await db.update(leadsTable).set(updates).where(eq(leadsTable.id, existing[0].id));
              results.updated++;
            }
          } else {
            if (!row.businessName && !ownerName) {
              results.errors.push(`Row ${i + 1}: Missing business name and owner name`);
              continue;
            }
            await db.insert(leadsTable).values({
              businessName: row.businessName || ownerName,
              ownerName: ownerName || row.businessName,
              phone: row.phone || "",
              email: row.email,
              ssn: row.ssn,
              dob: row.dob,
              taxId: row.ein,
              creditScore: row.creditScore,
              requestedAmount: row.requestedAmount ? String(row.requestedAmount) : undefined,
              businessStartDate: row.businessStartDate,
              notes: row.stackingNotes,
              status: "new",
              source: "csv_sync",
            });
            results.newLeads++;
          }
        } catch (e: any) {
          results.errors.push(`${row.businessName}: ${e.message}`);
        }
      }
    } else {
      const records: Record<string, string>[] = parse(csvContent, {
        columns: true,
        skip_empty_lines: true,
        trim: true,
        relax_column_count: true,
        bom: true,
      });

      for (let i = 0; i < records.length; i++) {
        const row = records[i];
        try {
          const businessName = findColumn(row, "businessname", "business", "companyname", "company", "dbaname", "dba", "bname") || "";
          const ownerName = findColumn(row, "ownername", "owner", "name", "fullname", "firstname", "contactname") || "";
          const phone = findColumn(row, "phone", "phonenumber", "telephone", "cell", "mobile", "number") || "";
          const email = findColumn(row, "email", "emailaddress", "owneremail") || undefined;
          const ssn = findColumn(row, "ssn", "socialsecuritynumber", "socialsecurity", "social") || undefined;
          const monthlyRevenue = parseNumber(findColumn(row, "monthlyrevenue", "monthlyincome", "revenue", "monthlygross", "grossrevenue", "gross"));
          const requestedAmount = parseNumber(findColumn(row, "requestedamount", "amount", "loanamount", "fundingamount", "requested"));
          const creditScore = parseNumber(findColumn(row, "creditscore", "credit", "fico"));
          const industry = findColumn(row, "industry", "businesstype", "type") || undefined;
          const address = findColumn(row, "address", "streetaddress", "street") || undefined;
          const city = findColumn(row, "city") || undefined;
          const state = findColumn(row, "state", "st") || undefined;
          const zip = findColumn(row, "zip", "zipcode", "postalcode") || undefined;

          if (!phone) {
            results.errors.push(`Row ${i + 1}: Missing phone number`);
            continue;
          }

          const normalizedPhone = normalizePhone(phone);
          const normalizedSsn = ssn ? normalizeSSN(ssn) : undefined;

          const matchConditions = [];
          if (normalizedPhone) matchConditions.push(sql`REGEXP_REPLACE(${leadsTable.phone}, '[^0-9]', '', 'g') = ${normalizedPhone}`);
          if (normalizedSsn) matchConditions.push(sql`REGEXP_REPLACE(COALESCE(${leadsTable.ssn}, ''), '[^0-9]', '', 'g') = ${normalizedSsn}`);

          const existing = await db.select({ id: leadsTable.id }).from(leadsTable)
            .where(or(...matchConditions))
            .limit(1);

          if (existing.length > 0) {
            const updates: any = {};
            if (monthlyRevenue) { updates.monthlyRevenue = String(monthlyRevenue); updates.grossRevenue = String(monthlyRevenue); }
            if (requestedAmount) updates.requestedAmount = String(requestedAmount);
            if (creditScore) updates.creditScore = Math.round(creditScore);
            if (email) updates.email = email;
            if (industry) updates.industry = industry;
            if (address) updates.address = address;
            if (city) updates.city = city;
            if (state) updates.state = state;
            if (zip) updates.zip = zip;

            if (Object.keys(updates).length > 0) {
              await db.update(leadsTable).set(updates).where(eq(leadsTable.id, existing[0].id));
              results.updated++;
            }
          } else {
            if (!businessName && !ownerName) {
              results.errors.push(`Row ${i + 1}: Missing business name and owner name for new lead`);
              continue;
            }
            await db.insert(leadsTable).values({
              businessName: businessName || ownerName,
              ownerName: ownerName || businessName,
              phone: normalizedPhone || phone,
              email,
              ssn: normalizedSsn || ssn,
              monthlyRevenue: monthlyRevenue ? String(monthlyRevenue) : undefined,
              grossRevenue: monthlyRevenue ? String(monthlyRevenue) : undefined,
              requestedAmount: requestedAmount ? String(requestedAmount) : undefined,
              industry,
              address,
              city,
              state,
              zip,
              creditScore: creditScore ? Math.round(creditScore) : undefined,
              status: "new",
            });
            results.newLeads++;
          }
        } catch (e: any) {
          results.errors.push(`Row ${i + 1}: ${e.message}`);
        }
      }
    }

    res.json({
      totalRows: rawRows.length - 1,
      updated: results.updated,
      newLeads: results.newLeads,
      format: results.format,
      errors: results.errors.slice(0, 50),
    });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

router.get("/import/today-sync-count", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);
    const result = await db.select({ count: sql<number>`count(*)::int` })
      .from(leadsTable)
      .where(and(
        eq(leadsTable.source, "google_sheets"),
        sql`${leadsTable.importDate} >= ${todayStart.toISOString()}`
      ));
    res.json({ count: result[0]?.count || 0 });
  } catch {
    res.json({ count: 0 });
  }
});

router.get("/import/batches", requireAuth, requireAdmin, requirePermission("import"), async (_req, res): Promise<void> => {
  const batches = await db.select().from(importBatchesTable).orderBy(sql`${importBatchesTable.createdAt} DESC`);
  res.json(batches);
});

router.post("/import/distribute", requireAuth, requireAdmin, requirePermission("import"), async (req, res): Promise<void> => {
  try {
    const { leadsPerRep = 5 } = req.body;

    const reps = await db.select().from(
      (await import("../configs/database")).usersTable
    ).where(
      and(
        eq((await import("../configs/database")).usersTable.role, "rep"),
        eq((await import("../configs/database")).usersTable.active, true)
      )
    );

    if (reps.length === 0) {
      res.status(400).json({ error: "No active sales reps found" });
      return;
    }

    const unassignedLeads = await db.select({ id: leadsTable.id })
      .from(leadsTable)
      .where(sql`${leadsTable.assignedToId} IS NULL`)
      .orderBy(sql`${leadsTable.createdAt} ASC`);

    if (unassignedLeads.length === 0) {
      res.status(400).json({ error: "No unassigned leads to distribute" });
      return;
    }

    let assigned = 0;
    let leadIndex = 0;

    for (const rep of reps) {
      const leadsForRep = unassignedLeads.slice(leadIndex, leadIndex + leadsPerRep);
      if (leadsForRep.length === 0) break;

      for (const lead of leadsForRep) {
        await db.update(leadsTable)
          .set({ assignedToId: rep.id })
          .where(eq(leadsTable.id, lead.id));
        assigned++;
      }

      leadIndex += leadsPerRep;
    }

    res.json({
      totalAssigned: assigned,
      repsAssigned: Math.min(reps.length, Math.ceil(unassignedLeads.length / leadsPerRep)),
      leadsPerRep,
    });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

export default router;
