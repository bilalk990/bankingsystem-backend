import { Router, type IRouter } from "express";
import { requireAuth, requireAdmin } from "../middlewares/authMiddleware";
import { db, leadsTable, appSettingsTable, documentsTable } from "../configs/database";
import { sql, eq, isNull } from "drizzle-orm";
import { getUncachableGoogleSheetClient, isGoogleSheetsConnected } from "../services/googleSheetsService";
import { encryptLeadFields } from "../utils/encryption";
import * as fs from "fs";
import * as path from "path";

const router: IRouter = Router();

const DEFAULT_FIELD_MAP: Record<string, string> = {
  "business name": "businessName",
  "company name": "businessName",
  "business": "businessName",
  "company": "businessName",
  "merchant": "businessName",
  "dba": "dba",
  "owner name": "ownerName",
  "owner": "ownerName",
  "full name": "ownerName",
  "contact name": "ownerName",
  "first name": "_firstName",
  "last name": "_lastName",
  "email": "email",
  "email address": "email",
  "phone": "phone",
  "phone number": "phone",
  "cell": "phone",
  "mobile": "phone",
  "requested amount": "requestedAmount",
  "amount": "requestedAmount",
  "amount requested": "requestedAmount",
  "funding amount": "requestedAmount",
  "approval principal": "requestedAmount",
  "monthly revenue": "monthlyRevenue",
  "revenue": "monthlyRevenue",
  "industry": "industry",
  "business type": "industry",
  "state": "state",
  "city": "city",
  "address": "address",
  "credit score": "creditScore",
  "fico": "creditScore",
  "ssn": "ssn",
  "social": "ssn",
  "social security": "ssn",
  "ein": "ein",
  "tax id": "ein",
  "years in business": "yearsInBusiness",
  "time in business": "yearsInBusiness",
  "business start": "businessStartDate",
  "business start date": "businessStartDate",
  "start date": "businessStartDate",
  "dob": "dob",
  "date of birth": "dob",
  "birthdate": "dob",
  "ownership": "ownershipPct",
  "ownership %": "ownershipPct",
  "ownership pct": "ownershipPct",
  "notes": "notes",
  "source": "source",
  "lead source": "source",
  "status": "status",
  "submitted at": "_submittedAt",
  "submitted": "_submittedAt",
};

function normalizePhone(phone: string): string {
  const digits = phone.replace(/\D/g, "");
  if (digits.length === 10) return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  if (digits.length === 11 && digits[0] === "1") return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  return phone;
}

function normalizeEmail(email: string): string {
  return email.toLowerCase().trim();
}

interface WatchConfig {
  spreadsheetId: string;
  sheetName: string;
  lastRowCount: number;
  enabled: boolean;
  intervalMs: number;
  lastSync: string;
  fieldMapping: Record<string, string>;
  allSheets?: boolean;
  sheetRowCounts?: Record<string, number>;
}

let activeWatch: WatchConfig | null = null;
let watchInterval: ReturnType<typeof setInterval> | null = null;
let pollInFlight = false;

interface SyncJob {
  id: string;
  connectionId: string;
  sourceName: string;
  status: "running" | "completed" | "error";
  startedAt: string;
  completedAt: string | null;
  currentTab: string | null;
  tabsTotal: number;
  tabsProcessed: number;
  imported: number;
  updated: number;
  skipped: number;
  errors: number;
  totalRows: number;
  sheetDetails: { sheetName: string; imported: number; updated: number; skipped: number; errors: number; rows: number }[];
  error: string | null;
}

const syncJobs = new Map<string, SyncJob>();

function cleanOldJobs() {
  const ONE_HOUR = 60 * 60 * 1000;
  const STALE_RUNNING = 10 * 60 * 1000;
  for (const [id, job] of syncJobs) {
    if (job.completedAt && Date.now() - new Date(job.completedAt).getTime() > ONE_HOUR) {
      syncJobs.delete(id);
    }
    if (job.status === "running" && Date.now() - new Date(job.startedAt).getTime() > STALE_RUNNING) {
      job.status = "error";
      job.error = "Sync timed out or was interrupted";
      job.completedAt = new Date().toISOString();
    }
  }
}

async function runSyncJobInBackground(jobId: string, conn: any, connections: any[]) {
  const job = syncJobs.get(jobId);
  if (!job) return;

  try {
    const connected = await isGoogleSheetsConnected();
    if (!connected) {
      job.status = "error";
      job.error = "Google Sheets not connected";
      job.completedAt = new Date().toISOString();
      return;
    }

    const sheetsClient = await getUncachableGoogleSheetClient();
    let meta;
    try {
      meta = await Promise.race([
        sheetsClient.spreadsheets.get({ spreadsheetId: conn.spreadsheetId }),
        new Promise((_, reject) => setTimeout(() => reject(new Error("Google Sheets API timed out after 30s")), 30000)),
      ]) as any;
    } catch (metaErr: any) {
      job.status = "error";
      job.error = metaErr.message || "Failed to connect to Google Sheets";
      job.completedAt = new Date().toISOString();
      console.error(`[Sync Job ${jobId}] Metadata fetch failed:`, metaErr.message);
      return;
    }
    const sheetTabs = (meta.data.sheets || []).map((s: any) => s.properties?.title).filter(Boolean) as string[];

    job.tabsTotal = sheetTabs.length;

    for (const tabName of sheetTabs) {
      job.currentTab = tabName;
      try {
        const data = await Promise.race([
          sheetsClient.spreadsheets.values.get({
            spreadsheetId: conn.spreadsheetId,
            range: `'${tabName}'`,
          }),
          new Promise((_, reject) => setTimeout(() => reject(new Error(`Timeout fetching tab "${tabName}"`)), 30000)),
        ]) as any;

        const allRows = data.data.values || [];
        if (allRows.length <= 1) { job.tabsProcessed++; continue; }

        const headers = allRows[0].map((h: string) => (h || "").toString().trim());
        const hasRelevantColumn = headers.some(h => DEFAULT_FIELD_MAP[h.toLowerCase().trim()] !== undefined);
        if (!hasRelevantColumn) { job.tabsProcessed++; continue; }

        const dataRows = allRows.slice(1);
        const mapping = buildMapping(headers);
        const result = await syncRows(dataRows, headers, mapping, 2, undefined, conn.sourceName);

        job.imported += result.imported;
        job.updated += result.updated;
        job.skipped += result.skipped;
        job.errors += result.errors;
        job.totalRows += dataRows.length;
        job.sheetDetails.push({
          sheetName: tabName,
          imported: result.imported,
          updated: result.updated,
          skipped: result.skipped,
          errors: result.errors,
          rows: dataRows.length,
        });

        for (const detail of result.details) {
          if (detail.action === "imported" && detail.leadId && detail.businessName && detail.businessName !== "Unknown") {
            await autoMatchBankStatements(detail.leadId, detail.businessName);
          }
        }
      } catch (tabErr: any) {
        console.error(`[Sync Job ${jobId}] Error on tab "${tabName}":`, tabErr.message);
      }
      job.tabsProcessed++;
    }

    try {
      const freshRow = await db.select().from(appSettingsTable).where(eq(appSettingsTable.key, "sheet_connections")).limit(1);
      const freshConnections = freshRow.length > 0 ? JSON.parse(freshRow[0].value) : [];
      const freshConn = freshConnections.find((c: any) => c.id === conn.id);
      if (freshConn) {
        freshConn.lastSync = new Date().toISOString();
        await db.insert(appSettingsTable)
          .values({ key: "sheet_connections", value: JSON.stringify(freshConnections) })
          .onConflictDoUpdate({
            target: appSettingsTable.key,
            set: { value: JSON.stringify(freshConnections), updatedAt: new Date() },
          });
      }
    } catch (persistErr: any) {
      console.error(`[Sync Job ${jobId}] Failed to persist lastSync:`, persistErr.message);
    }

    job.status = "completed";
    job.completedAt = new Date().toISOString();
    job.currentTab = null;
    console.log(`[Sync Job ${jobId}] Complete: ${job.imported} imported, ${job.updated} updated from ${job.tabsProcessed} tabs`);
  } catch (e: any) {
    job.status = "error";
    job.error = e.message;
    job.completedAt = new Date().toISOString();
    console.error(`[Sync Job ${jobId}] Fatal error:`, e.message);
  }
}

async function persistWatch(config: WatchConfig | null) {
  if (config) {
    await db.insert(appSettingsTable)
      .values({ key: "google_sheets_watch", value: JSON.stringify(config) })
      .onConflictDoUpdate({
        target: appSettingsTable.key,
        set: { value: JSON.stringify(config), updatedAt: new Date() },
      });
  } else {
    await db.delete(appSettingsTable).where(eq(appSettingsTable.key, "google_sheets_watch"));
  }
}

const UPLOADS_DIR = path.resolve(process.cwd(), "uploads");

async function autoMatchBankStatements(leadId: number, businessName: string) {
  if (!businessName) return;
  try {
    const extractedDir = path.join(UPLOADS_DIR, "extracted");
    if (!fs.existsSync(extractedDir)) return;

    const batches = fs.readdirSync(extractedDir).filter(d =>
      fs.statSync(path.join(extractedDir, d)).isDirectory()
    );

    const existingDocs = await db.select({ url: documentsTable.url })
      .from(documentsTable)
      .where(eq(documentsTable.leadId, leadId));
    const existingUrls = new Set(existingDocs.map(d => d.url));

    const normalizedBiz = businessName.toLowerCase().replace(/[^a-z0-9\s]/g, "").trim();
    const bizWords = normalizedBiz.split(/\s+/).filter(w => w.length > 2);
    let matched = 0;

    for (const batch of batches) {
      const batchPath = path.join(extractedDir, batch);
      const folders = fs.readdirSync(batchPath).filter(f =>
        fs.statSync(path.join(batchPath, f)).isDirectory()
      );

      for (const folder of folders) {
        const normalizedFolder = folder.toLowerCase().replace(/[-_]+/g, " ").replace(/[^a-z0-9\s]/g, "").trim();

        const isMatch = normalizedFolder.includes(normalizedBiz) ||
          normalizedBiz.includes(normalizedFolder) ||
          (bizWords.length >= 2 && bizWords.every(w => normalizedFolder.includes(w)));

        if (!isMatch) continue;

        const folderPath = path.join(batchPath, folder);
        const files = fs.readdirSync(folderPath).filter(f => {
          const ext = path.extname(f).toLowerCase();
          return [".pdf", ".jpg", ".jpeg", ".png", ".csv", ".xlsx", ".xls"].includes(ext);
        });

        for (const file of files) {
          const relUrl = `/uploads/extracted/${batch}/${folder}/${file}`;
          if (existingUrls.has(relUrl)) continue;

          try {
            await db.insert(documentsTable).values({
              leadId,
              type: "bank_statement",
              name: file,
              url: relUrl,
            });
            existingUrls.add(relUrl);
            matched++;
          } catch (insertErr: any) {
            if (insertErr.code === "23505") continue;
            throw insertErr;
          }
        }
      }
    }

    if (matched > 0) {
      await db.update(leadsTable)
        .set({ bankStatementsStatus: "uploaded" })
        .where(eq(leadsTable.id, leadId));
      console.log(`[Auto-Match] Linked ${matched} bank statement file(s) to lead "${businessName}" (ID: ${leadId})`);
    }
  } catch (e: any) {
    console.error(`[Auto-Match] Error for lead "${businessName}":`, e.message);
  }
}

let connectionWatchInterval: ReturnType<typeof setInterval> | null = null;
let connectionPollInFlight = false;

async function pollConnectionsForNewRows() {
  if (connectionPollInFlight) return;
  connectionPollInFlight = true;

  try {
    const connected = await isGoogleSheetsConnected();
    if (!connected) return;

    const row = await db.select().from(appSettingsTable).where(eq(appSettingsTable.key, "sheet_connections")).limit(1);
    const connections = row.length > 0 ? JSON.parse(row[0].value) : [];
    if (connections.length === 0) return;

    const trackerRow = await db.select().from(appSettingsTable).where(eq(appSettingsTable.key, "connection_row_tracker")).limit(1);
    const rowTracker: Record<string, Record<string, number>> = trackerRow.length > 0 ? JSON.parse(trackerRow[0].value) : {};

    let totalNewImported = 0;
    let totalNewUpdated = 0;
    let trackerChanged = false;

    const sheetsClient = await getUncachableGoogleSheetClient();

    for (const conn of connections) {
      try {
        const meta = await sheetsClient.spreadsheets.get({ spreadsheetId: conn.spreadsheetId });
        const sheetTabs = (meta.data.sheets || []).map((s: any) => s.properties?.title).filter(Boolean) as string[];

        if (!rowTracker[conn.id]) rowTracker[conn.id] = {};

        for (const tabName of sheetTabs) {
          try {
            const data = await sheetsClient.spreadsheets.values.get({
              spreadsheetId: conn.spreadsheetId,
              range: `'${tabName}'`,
            });

            const allRows = data.data.values || [];
            if (allRows.length <= 1) continue;

            const headers = allRows[0].map((h: string) => (h || "").toString().trim());
            const hasRelevantColumn = headers.some(h => DEFAULT_FIELD_MAP[h.toLowerCase().trim()] !== undefined);
            if (!hasRelevantColumn) continue;

            const dataRows = allRows.slice(1);
            const currentCount = dataRows.length;
            const lastCount = rowTracker[conn.id][tabName] || 0;

            if (lastCount === 0) {
              rowTracker[conn.id][tabName] = currentCount;
              trackerChanged = true;
              continue;
            }

            if (currentCount > lastCount) {
              const newRows = dataRows.slice(lastCount);
              console.log(`[Connection Watch] "${conn.sourceName}" tab "${tabName}": ${newRows.length} new row(s)`);

              const mapping = buildMapping(headers);
              const result = await syncRows(newRows, headers, mapping, lastCount + 2, undefined, conn.sourceName);

              totalNewImported += result.imported;
              totalNewUpdated += result.updated;

              for (const detail of result.details) {
                if (detail.action === "imported" && detail.leadId && detail.businessName && detail.businessName !== "Unknown") {
                  await autoMatchBankStatements(detail.leadId, detail.businessName);
                }
              }

              rowTracker[conn.id][tabName] = currentCount;
              trackerChanged = true;
            }
          } catch (tabErr: any) {
            // skip tab errors silently
          }
        }
      } catch (connErr: any) {
        console.error(`[Connection Watch] Error polling "${conn.sourceName}":`, connErr.message);
      }
    }

    if (trackerChanged) {
      await db.insert(appSettingsTable)
        .values({ key: "connection_row_tracker", value: JSON.stringify(rowTracker) })
        .onConflictDoUpdate({
          target: appSettingsTable.key,
          set: { value: JSON.stringify(rowTracker), updatedAt: new Date() },
        });
    }

    if (totalNewImported + totalNewUpdated > 0) {
      console.log(`[Connection Watch] Total: ${totalNewImported} imported, ${totalNewUpdated} updated`);
    }
  } catch (e: any) {
    console.error("[Connection Watch] Poll error:", e.message);
  } finally {
    connectionPollInFlight = false;
  }
}

function startConnectionWatch() {
  if (connectionWatchInterval) return;
  connectionWatchInterval = setInterval(pollConnectionsForNewRows, 2 * 60 * 1000);
  console.log("[Connection Watch] Started — polling every 2 minutes for new rows across all connected sheets");
  setTimeout(pollConnectionsForNewRows, 10000);
}

setTimeout(startConnectionWatch, 5000);

async function restoreWatch() {
  try {
    const row = await db.select().from(appSettingsTable).where(eq(appSettingsTable.key, "google_sheets_watch")).limit(1);
    if (row.length > 0) {
      const config = JSON.parse(row[0].value) as WatchConfig;
      if (config.enabled) {
        const connected = await isGoogleSheetsConnected();
        if (!connected) {
          console.log("[Google Sheets Watcher] Skipping auto-restore — not connected");
          return;
        }
        activeWatch = config;
        watchInterval = setInterval(pollForNewRows, config.intervalMs);
        console.log(`[Google Sheets Watcher] Auto-restored watch for "${config.sheetName}" (${config.lastRowCount} rows, polling every ${config.intervalMs / 60000} min)`);
        setTimeout(pollForNewRows, 5000);
      }
    }
  } catch (e: any) {
    console.error("[Google Sheets Watcher] Failed to restore watch:", e.message);
  }
}

setTimeout(restoreWatch, 3000);

function buildMapping(headers: string[], customMapping?: Record<string, string>): Record<number, string> {
  const finalMapping: Record<number, string> = {};
  for (let i = 0; i < headers.length; i++) {
    const header = headers[i];
    if (customMapping?.[header]) {
      finalMapping[i] = customMapping[header];
    } else {
      const normalized = header.toLowerCase().trim();
      if (DEFAULT_FIELD_MAP[normalized]) {
        finalMapping[i] = DEFAULT_FIELD_MAP[normalized];
      }
    }
  }
  return finalMapping;
}

async function syncRows(rows: string[][], headers: string[], mapping: Record<number, string>, startRowLabel: number, assignToId?: number, sourceName?: string) {
  const results = {
    imported: 0,
    updated: 0,
    skipped: 0,
    errors: 0,
    details: [] as Array<{ row: number; action: string; businessName: string; error?: string }>,
  };

  for (let rowIdx = 0; rowIdx < rows.length; rowIdx++) {
    const row = rows[rowIdx];
    const leadData: Record<string, any> = {};

    for (const [colIdx, field] of Object.entries(mapping)) {
      const value = row[parseInt(colIdx)];
      if (value !== undefined && value !== null && value !== "") {
        leadData[field] = value.toString().trim();
      }
    }

    if (leadData._firstName || leadData._lastName) {
      const first = leadData._firstName || "";
      const last = leadData._lastName || "";
      leadData.ownerName = `${first} ${last}`.trim();
      delete leadData._firstName;
      delete leadData._lastName;
    }

    let submittedDate: Date | null = null;
    if (leadData._submittedAt) {
      const parts = leadData._submittedAt.toString().split("/");
      if (parts.length >= 2) {
        const month = parseInt(parts[0]);
        const day = parseInt(parts[1]);
        const year = parts.length >= 3 ? parseInt(parts[2]) : new Date().getFullYear();
        submittedDate = new Date(year, month - 1, day, 12, 0, 0);
        if (isNaN(submittedDate.getTime())) submittedDate = null;
      }
    }
    delete leadData._submittedAt;

    const hasIdentifier = !!(leadData.businessName || leadData.ownerName || leadData.phone || leadData.email);
    if (!hasIdentifier) {
      results.skipped++;
      continue;
    }

    const allValues = Object.values(leadData).filter(v => v && typeof v === "string" && v.trim().length > 0);
    if (allValues.length === 0) {
      results.skipped++;
      continue;
    }

    try {
      if (leadData.phone) leadData.phone = normalizePhone(leadData.phone);
      if (leadData.email) leadData.email = normalizeEmail(leadData.email);
      if (leadData.requestedAmount) leadData.requestedAmount = parseFloat(leadData.requestedAmount.toString().replace(/[$,]/g, "")) || null;
      if (leadData.monthlyRevenue) leadData.monthlyRevenue = parseFloat(leadData.monthlyRevenue.toString().replace(/[$,]/g, "")) || null;
      if (leadData.creditScore) leadData.creditScore = parseInt(leadData.creditScore.toString()) || null;
      if (leadData.yearsInBusiness) leadData.yearsInBusiness = parseFloat(leadData.yearsInBusiness.toString()) || null;
      if (leadData.ownershipPct) {
        const pct = parseFloat(leadData.ownershipPct.toString().replace(/%/g, ""));
        leadData.ownershipPct = isNaN(pct) ? null : pct;
      }

      let existingLead = null;

      if (leadData.businessName) {
        const found = await db.select({ id: leadsTable.id })
          .from(leadsTable)
          .where(sql`LOWER(${leadsTable.businessName}) = LOWER(${leadData.businessName})`)
          .limit(1);
        if (found.length > 0) existingLead = found[0];
      }

      if (!existingLead && leadData.phone) {
        const phoneDigits = leadData.phone.replace(/\D/g, "");
        const found = await db.select({ id: leadsTable.id })
          .from(leadsTable)
          .where(sql`REPLACE(REPLACE(REPLACE(REPLACE(${leadsTable.phone}, '(', ''), ')', ''), '-', ''), ' ', '') = ${phoneDigits}`)
          .limit(1);
        if (found.length > 0) existingLead = found[0];
      }

      if (!existingLead && leadData.email) {
        const found = await db.select({ id: leadsTable.id })
          .from(leadsTable)
          .where(sql`LOWER(${leadsTable.email}) = LOWER(${leadData.email})`)
          .limit(1);
        if (found.length > 0) existingLead = found[0];
      }

      if (!existingLead && leadData.ssn && leadData.ssn.length >= 4) {
        const ssnDigits = leadData.ssn.replace(/\D/g, "");
        const isDummy = /^(\d)\1+$/.test(ssnDigits);
        if (!isDummy) {
          const { hmacHash, normalizeSsnForHash } = await import("../utils/encryption");
          const ssnHashVal = hmacHash(normalizeSsnForHash(leadData.ssn));
          const found = await db.select({ id: leadsTable.id })
            .from(leadsTable)
            .where(eq(leadsTable.ssnHash, ssnHashVal))
            .limit(1);
          if (found.length > 0) existingLead = found[0];
        }
      }

      if (existingLead) {
        const updateFields: Record<string, any> = {};
        for (const [key, value] of Object.entries(leadData)) {
          if (value !== null && value !== undefined && value !== "" && !key.startsWith("_")) {
            updateFields[key] = value;
          }
        }
        if (Object.keys(updateFields).length > 0) {
          const encryptedUpdate = encryptLeadFields(updateFields);
          await db.update(leadsTable).set(encryptedUpdate).where(eq(leadsTable.id, existingLead.id));
        }
        results.updated++;
        results.details.push({ row: startRowLabel + rowIdx, action: "updated", businessName: leadData.businessName || "Unknown" });
      } else {
        const insertData: any = {};
        for (const [key, value] of Object.entries(leadData)) {
          if (!key.startsWith("_") && value !== null && value !== undefined && value !== "") {
            insertData[key] = value;
          }
        }
        insertData.businessName = insertData.businessName || insertData.ownerName || "Unknown Business";
        insertData.ownerName = insertData.ownerName || insertData.businessName || "Unknown";
        insertData.phone = insertData.phone || "";
        insertData.status = insertData.status || "new";
        insertData.source = sourceName || insertData.source || "google_sheets";
        if (assignToId) insertData.assignedToId = assignToId;
        if (submittedDate) insertData.createdAt = submittedDate;
        insertData.position = results.imported + results.updated + results.skipped + 1;

        const encryptedInsert = encryptLeadFields(insertData);
        const [inserted] = await db.insert(leadsTable).values(encryptedInsert).returning({ id: leadsTable.id });
        results.imported++;
        results.details.push({ row: startRowLabel + rowIdx, action: "imported", businessName: leadData.businessName || "Unknown", leadId: inserted?.id });
      }
    } catch (e: any) {
      results.errors++;
      if (results.errors <= 5) {
        console.error(`[Sync Row Error] Row ${startRowLabel + rowIdx}, biz="${leadData.businessName}": ${e.message}`);
      }
      results.details.push({ row: startRowLabel + rowIdx, action: "error", businessName: leadData.businessName || "Unknown", error: e.message });
    }
  }

  return results;
}

async function pollForNewRows() {
  if (!activeWatch || !activeWatch.enabled || pollInFlight) return;
  pollInFlight = true;

  try {
    const connected = await isGoogleSheetsConnected();
    if (!connected) return;

    const sheetsClient = await getUncachableGoogleSheetClient();

    if (activeWatch.allSheets) {
      const meta = await sheetsClient.spreadsheets.get({ spreadsheetId: activeWatch.spreadsheetId });
      const sheetTabs = (meta.data.sheets || []).map((s: any) => s.properties?.title).filter(Boolean) as string[];

      if (!activeWatch.sheetRowCounts) activeWatch.sheetRowCounts = {};

      let totalImported = 0, totalUpdated = 0;

      for (const tabName of sheetTabs) {
        try {
          const data = await sheetsClient.spreadsheets.values.get({
            spreadsheetId: activeWatch.spreadsheetId,
            range: `'${tabName}'`,
          });

          const allRows = data.data.values || [];
          if (allRows.length <= 1) continue;

          const headers = allRows[0].map((h: string) => (h || "").toString().trim());
          const hasBusinessColumn = headers.some(h => {
            const n = h.toLowerCase().trim();
            return DEFAULT_FIELD_MAP[n] !== undefined;
          });
          if (!hasBusinessColumn) continue;

          const dataRows = allRows.slice(1);
          const currentCount = dataRows.length;
          const lastCount = activeWatch.sheetRowCounts[tabName] || 0;

          if (currentCount > lastCount) {
            const newRows = dataRows.slice(lastCount);
            console.log(`[Google Sheets Watcher] Tab "${tabName}": ${newRows.length} new row(s). Syncing...`);
            const mapping = buildMapping(headers, activeWatch.fieldMapping);
            const result = await syncRows(newRows, headers, mapping, lastCount + 2);
            totalImported += result.imported;
            totalUpdated += result.updated;
            activeWatch.sheetRowCounts[tabName] = currentCount;
          }
        } catch (tabErr: any) {
          console.error(`[Google Sheets Watcher] Error reading tab "${tabName}":`, tabErr.message);
        }
      }

      if (totalImported + totalUpdated > 0) {
        console.log(`[Google Sheets Watcher] Multi-sheet sync: ${totalImported} imported, ${totalUpdated} updated`);
      }
      activeWatch.lastSync = new Date().toISOString();
      await persistWatch(activeWatch);
    } else {
      const data = await sheetsClient.spreadsheets.values.get({
        spreadsheetId: activeWatch.spreadsheetId,
        range: `'${activeWatch.sheetName}'`,
      });

      const allRows = data.data.values || [];
      if (allRows.length <= 1) return;

      const headers = allRows[0].map((h: string) => (h || "").toString().trim());
      const dataRows = allRows.slice(1);
      const currentCount = dataRows.length;

      if (currentCount > activeWatch.lastRowCount) {
        const newRows = dataRows.slice(activeWatch.lastRowCount);
        console.log(`[Google Sheets Watcher] Found ${newRows.length} new row(s). Syncing...`);

        const mapping = buildMapping(headers, activeWatch.fieldMapping);
        const result = await syncRows(newRows, headers, mapping, activeWatch.lastRowCount + 2);

        console.log(`[Google Sheets Watcher] Sync complete: ${result.imported} imported, ${result.updated} updated, ${result.errors} errors`);

        const successCount = result.imported + result.updated + result.skipped;
        activeWatch.lastRowCount = activeWatch.lastRowCount + successCount;
        activeWatch.lastSync = new Date().toISOString();
        await persistWatch(activeWatch);
      }
    }
  } catch (e: any) {
    console.error("[Google Sheets Watcher] Poll error:", e.message);
  } finally {
    pollInFlight = false;
  }
}

router.get("/google-sheets/status", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    const connected = await isGoogleSheetsConnected();
    res.json({
      connected,
      connectionWatchActive: !!connectionWatchInterval,
      watch: activeWatch ? {
        enabled: activeWatch.enabled,
        spreadsheetId: activeWatch.spreadsheetId,
        sheetName: activeWatch.sheetName,
        lastRowCount: activeWatch.lastRowCount,
        lastSync: activeWatch.lastSync,
        intervalMs: activeWatch.intervalMs,
      } : null,
    });
  } catch (e: any) {
    res.json({ connected: false, error: e.message });
  }
});

router.get("/google-sheets/connections", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    const row = await db.select().from(appSettingsTable).where(eq(appSettingsTable.key, "sheet_connections")).limit(1);
    const connections = row.length > 0 ? JSON.parse(row[0].value) : [];
    res.json({ connections });
  } catch (e: any) {
    res.json({ connections: [] });
  }
});

router.post("/google-sheets/connections", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const { spreadsheetUrl, sourceName } = req.body;
    if (!spreadsheetUrl || !sourceName) {
      res.status(400).json({ error: "Spreadsheet URL and source name are required" });
      return;
    }

    const match = spreadsheetUrl.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/);
    const spreadsheetId = match ? match[1] : (/^[a-zA-Z0-9_-]{20,}$/.test(spreadsheetUrl.trim()) ? spreadsheetUrl.trim() : null);
    if (!spreadsheetId) {
      res.status(400).json({ error: "Invalid spreadsheet URL" });
      return;
    }

    const connected = await isGoogleSheetsConnected();
    if (!connected) {
      res.status(400).json({ error: "Google Sheets not connected" });
      return;
    }

    const sheetsClient = await getUncachableGoogleSheetClient();
    const meta = await sheetsClient.spreadsheets.get({ spreadsheetId });
    const title = meta.data.properties?.title || "Untitled";
    const sheetTabs = (meta.data.sheets || []).map((s: any) => s.properties?.title).filter(Boolean) as string[];

    const row = await db.select().from(appSettingsTable).where(eq(appSettingsTable.key, "sheet_connections")).limit(1);
    const connections = row.length > 0 ? JSON.parse(row[0].value) : [];

    const existing = connections.find((c: any) => c.spreadsheetId === spreadsheetId);
    if (existing) {
      res.status(400).json({ error: "This spreadsheet is already connected" });
      return;
    }

    const newConnection = {
      id: `conn_${Date.now()}`,
      spreadsheetId,
      spreadsheetTitle: title,
      sourceName: sourceName.trim(),
      sheetTabs,
      addedAt: new Date().toISOString(),
      lastSync: null as string | null,
      autoWatch: false,
    };

    connections.push(newConnection);
    await db.insert(appSettingsTable)
      .values({ key: "sheet_connections", value: JSON.stringify(connections) })
      .onConflictDoUpdate({
        target: appSettingsTable.key,
        set: { value: JSON.stringify(connections), updatedAt: new Date() },
      });

    res.json({ connection: newConnection });
  } catch (e: any) {
    console.error("Add sheet connection error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.delete("/google-sheets/connections/:connectionId", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const { connectionId } = req.params;
    const row = await db.select().from(appSettingsTable).where(eq(appSettingsTable.key, "sheet_connections")).limit(1);
    const connections = row.length > 0 ? JSON.parse(row[0].value) : [];
    const filtered = connections.filter((c: any) => c.id !== connectionId);

    await db.insert(appSettingsTable)
      .values({ key: "sheet_connections", value: JSON.stringify(filtered) })
      .onConflictDoUpdate({
        target: appSettingsTable.key,
        set: { value: JSON.stringify(filtered), updatedAt: new Date() },
      });

    res.json({ success: true });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

router.post("/google-sheets/connections/:connectionId/sync", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const { connectionId } = req.params;
    const row = await db.select().from(appSettingsTable).where(eq(appSettingsTable.key, "sheet_connections")).limit(1);
    const connections = row.length > 0 ? JSON.parse(row[0].value) : [];
    const conn = connections.find((c: any) => c.id === connectionId);

    if (!conn) {
      res.status(404).json({ error: "Connection not found" });
      return;
    }

    for (const [, existingJob] of syncJobs) {
      if (existingJob.connectionId === connectionId && existingJob.status === "running") {
        res.json({ jobId: existingJob.id, message: "Sync already in progress" });
        return;
      }
    }

    cleanOldJobs();

    const jobId = `sync_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const job: SyncJob = {
      id: jobId,
      connectionId,
      sourceName: conn.sourceName,
      status: "running",
      startedAt: new Date().toISOString(),
      completedAt: null,
      currentTab: null,
      tabsTotal: 0,
      tabsProcessed: 0,
      imported: 0,
      updated: 0,
      skipped: 0,
      errors: 0,
      totalRows: 0,
      sheetDetails: [],
      error: null,
    };
    syncJobs.set(jobId, job);

    runSyncJobInBackground(jobId, conn, connections);

    res.json({ jobId, message: "Sync started in background" });
  } catch (e: any) {
    console.error("Connection sync error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.get("/google-sheets/sync-jobs", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  cleanOldJobs();
  const jobs = Array.from(syncJobs.values())
    .sort((a, b) => new Date(b.startedAt).getTime() - new Date(a.startedAt).getTime())
    .slice(0, 10);
  res.json({ jobs });
});

router.get("/google-sheets/sync-jobs/:jobId", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  const job = syncJobs.get(req.params.jobId);
  if (!job) {
    res.status(404).json({ error: "Job not found" });
    return;
  }
  res.json(job);
});

router.post("/google-sheets/list-sheets", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const connected = await isGoogleSheetsConnected();
    if (!connected) {
      res.status(400).json({ error: "Google Sheets not connected" });
      return;
    }

    const { spreadsheetId } = req.body;
    if (!spreadsheetId) {
      res.status(400).json({ error: "spreadsheetId required" });
      return;
    }

    const sheets = await getUncachableGoogleSheetClient();
    const meta = await sheets.spreadsheets.get({ spreadsheetId });

    const sheetList = (meta.data.sheets || []).map((s: any) => ({
      title: s.properties?.title || "Sheet1",
      sheetId: s.properties?.sheetId,
      rowCount: s.properties?.gridProperties?.rowCount,
      columnCount: s.properties?.gridProperties?.columnCount,
    }));

    res.json({
      title: meta.data.properties?.title || "Untitled",
      sheets: sheetList,
    });
  } catch (e: any) {
    console.error("Google Sheets list error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.post("/google-sheets/preview", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const connected = await isGoogleSheetsConnected();
    if (!connected) {
      res.status(400).json({ error: "Google Sheets not connected" });
      return;
    }

    const { spreadsheetId, sheetName, headerRow = 1 } = req.body;
    if (!spreadsheetId) {
      res.status(400).json({ error: "spreadsheetId required" });
      return;
    }

    const actualSheetName = sheetName || "Sheet1";
    const sheets = await getUncachableGoogleSheetClient();

    let range: string;
    try {
      range = `'${actualSheetName}'!1:50`;
      await sheets.spreadsheets.values.get({ spreadsheetId, range });
    } catch {
      const meta = await sheets.spreadsheets.get({ spreadsheetId });
      const firstSheet = meta.data.sheets?.[0]?.properties?.title || "Sheet1";
      range = `'${firstSheet}'!1:50`;
    }

    const data = await sheets.spreadsheets.values.get({ spreadsheetId, range });
    const rows = data.data.values || [];
    if (rows.length < headerRow) {
      res.status(400).json({ error: "Sheet appears empty or header row not found" });
      return;
    }

    const headers = rows[headerRow - 1].map((h: string) => (h || "").toString().trim());
    const sampleRows = rows.slice(headerRow, headerRow + 5);
    const totalRows = rows.length - headerRow;

    const autoMapping: Record<string, string> = {};
    for (const header of headers) {
      const normalized = header.toLowerCase().trim();
      if (DEFAULT_FIELD_MAP[normalized]) {
        autoMapping[header] = DEFAULT_FIELD_MAP[normalized];
      }
    }

    res.json({
      headers,
      sampleRows,
      totalRows,
      autoMapping,
      sheetName: range.split("!")[0].replace(/'/g, ""),
    });
  } catch (e: any) {
    console.error("Google Sheets preview error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.post("/google-sheets/sync", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const connected = await isGoogleSheetsConnected();
    if (!connected) {
      res.status(400).json({ error: "Google Sheets not connected" });
      return;
    }

    const { spreadsheetId, sheetName, headerRow = 1, fieldMapping, assignToId } = req.body;
    if (!spreadsheetId) {
      res.status(400).json({ error: "spreadsheetId required" });
      return;
    }

    const actualSheetName = sheetName || "Sheet1";
    const sheets = await getUncachableGoogleSheetClient();

    let resolvedSheetName = actualSheetName;
    try {
      await sheets.spreadsheets.values.get({ spreadsheetId, range: `'${actualSheetName}'!1:1` });
      resolvedSheetName = actualSheetName;
    } catch {
      const meta = await sheets.spreadsheets.get({ spreadsheetId });
      resolvedSheetName = meta.data.sheets?.[0]?.properties?.title || "Sheet1";
    }

    const data = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: `'${resolvedSheetName}'`,
    });

    const allRows = data.data.values || [];
    if (allRows.length <= headerRow) {
      res.status(400).json({ error: "No data rows found" });
      return;
    }

    const headers = allRows[headerRow - 1].map((h: string) => (h || "").toString().trim());
    const dataRows = allRows.slice(headerRow);

    const mapping = buildMapping(headers, fieldMapping);
    const results = await syncRows(dataRows, headers, mapping, headerRow + 1, assignToId);

    res.json({ ...results, totalRows: dataRows.length });
  } catch (e: any) {
    console.error("Google Sheets sync error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.post("/google-sheets/sync-all", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const connected = await isGoogleSheetsConnected();
    if (!connected) {
      res.status(400).json({ error: "Google Sheets not connected" });
      return;
    }

    const { spreadsheetId, fieldMapping, assignToId } = req.body;
    if (!spreadsheetId) {
      res.status(400).json({ error: "spreadsheetId required" });
      return;
    }

    const sheetsClient = await getUncachableGoogleSheetClient();
    const meta = await sheetsClient.spreadsheets.get({ spreadsheetId });
    const sheetTabs = (meta.data.sheets || []).map((s: any) => s.properties?.title).filter(Boolean) as string[];

    const allResults = {
      imported: 0,
      updated: 0,
      skipped: 0,
      errors: 0,
      totalRows: 0,
      sheetsProcessed: 0,
      sheetsSkipped: 0,
      sheetDetails: [] as { sheetName: string; imported: number; updated: number; skipped: number; errors: number; rows: number }[],
    };

    for (const tabName of sheetTabs) {
      try {
        const data = await sheetsClient.spreadsheets.values.get({
          spreadsheetId,
          range: `'${tabName}'`,
        });

        const allRows = data.data.values || [];
        if (allRows.length <= 1) {
          allResults.sheetsSkipped++;
          continue;
        }

        const headers = allRows[0].map((h: string) => (h || "").toString().trim());
        const hasRelevantColumn = headers.some(h => {
          const n = h.toLowerCase().trim();
          return DEFAULT_FIELD_MAP[n] !== undefined;
        });

        if (!hasRelevantColumn) {
          allResults.sheetsSkipped++;
          continue;
        }

        const dataRows = allRows.slice(1);
        const mapping = buildMapping(headers, fieldMapping);
        const result = await syncRows(dataRows, headers, mapping, 2, assignToId);

        allResults.imported += result.imported;
        allResults.updated += result.updated;
        allResults.skipped += result.skipped;
        allResults.errors += result.errors;
        allResults.totalRows += dataRows.length;
        allResults.sheetsProcessed++;
        allResults.sheetDetails.push({
          sheetName: tabName,
          imported: result.imported,
          updated: result.updated,
          skipped: result.skipped,
          errors: result.errors,
          rows: dataRows.length,
        });

        console.log(`[Google Sheets Sync-All] Tab "${tabName}": ${result.imported} imported, ${result.updated} updated, ${result.skipped} skipped`);
      } catch (tabErr: any) {
        console.error(`[Google Sheets Sync-All] Error on tab "${tabName}":`, tabErr.message);
        allResults.sheetsSkipped++;
      }
    }

    console.log(`[Google Sheets Sync-All] Complete: ${allResults.sheetsProcessed} sheets, ${allResults.imported} imported, ${allResults.updated} updated`);
    res.json(allResults);
  } catch (e: any) {
    console.error("Google Sheets sync-all error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.post("/google-sheets/watch", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const connected = await isGoogleSheetsConnected();
    if (!connected) {
      res.status(400).json({ error: "Google Sheets not connected" });
      return;
    }

    const { spreadsheetId, sheetName, fieldMapping, intervalMinutes = 5, allSheets = false } = req.body;
    if (!spreadsheetId) {
      res.status(400).json({ error: "spreadsheetId required" });
      return;
    }

    const sheetsClient = await getUncachableGoogleSheetClient();

    if (watchInterval) {
      clearInterval(watchInterval);
      watchInterval = null;
    }

    if (allSheets) {
      const meta = await sheetsClient.spreadsheets.get({ spreadsheetId });
      const sheetTabs = (meta.data.sheets || []).map((s: any) => s.properties?.title).filter(Boolean) as string[];

      const sheetRowCounts: Record<string, number> = {};
      let totalRows = 0;

      for (const tabName of sheetTabs) {
        try {
          const data = await sheetsClient.spreadsheets.values.get({ spreadsheetId, range: `'${tabName}'` });
          const rows = data.data.values || [];
          const count = rows.length > 1 ? rows.length - 1 : 0;
          sheetRowCounts[tabName] = count;
          totalRows += count;
        } catch {
          sheetRowCounts[tabName] = 0;
        }
      }

      activeWatch = {
        spreadsheetId,
        sheetName: "ALL",
        lastRowCount: totalRows,
        enabled: true,
        intervalMs: Math.max(1, intervalMinutes) * 60 * 1000,
        lastSync: new Date().toISOString(),
        fieldMapping: fieldMapping || {},
        allSheets: true,
        sheetRowCounts,
      };

      watchInterval = setInterval(pollForNewRows, activeWatch.intervalMs);
      await persistWatch(activeWatch);

      console.log(`[Google Sheets Watcher] Started watching ALL ${sheetTabs.length} tabs (${totalRows} total rows, polling every ${intervalMinutes} min)`);

      res.json({
        message: `Now watching all ${sheetTabs.length} sheet tabs. ${totalRows} total rows. New rows will auto-import every ${intervalMinutes} minutes.`,
        sheetTabs: sheetTabs.length,
        currentRows: totalRows,
        intervalMinutes,
      });
    } else {
      let resolvedSheetName = sheetName || "Sheet1";
      try {
        await sheetsClient.spreadsheets.values.get({ spreadsheetId, range: `'${resolvedSheetName}'!1:1` });
      } catch {
        const meta = await sheetsClient.spreadsheets.get({ spreadsheetId });
        resolvedSheetName = meta.data.sheets?.[0]?.properties?.title || "Sheet1";
      }

      const data = await sheetsClient.spreadsheets.values.get({
        spreadsheetId,
        range: `'${resolvedSheetName}'`,
      });

      const allRows = data.data.values || [];
      const currentRowCount = allRows.length > 1 ? allRows.length - 1 : 0;

      activeWatch = {
        spreadsheetId,
        sheetName: resolvedSheetName,
        lastRowCount: currentRowCount,
        enabled: true,
        intervalMs: Math.max(1, intervalMinutes) * 60 * 1000,
        lastSync: new Date().toISOString(),
        fieldMapping: fieldMapping || {},
      };

      watchInterval = setInterval(pollForNewRows, activeWatch.intervalMs);
      await persistWatch(activeWatch);

      console.log(`[Google Sheets Watcher] Started watching "${resolvedSheetName}" (${currentRowCount} existing rows, polling every ${intervalMinutes} min)`);

      res.json({
        message: `Now watching your sheet. Currently ${currentRowCount} rows. New rows will auto-import every ${intervalMinutes} minutes.`,
        sheetName: resolvedSheetName,
        currentRows: currentRowCount,
        intervalMinutes,
      });
    }
  } catch (e: any) {
    console.error("Watch setup error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.post("/google-sheets/unwatch", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  if (watchInterval) {
    clearInterval(watchInterval);
    watchInterval = null;
  }
  if (activeWatch) {
    activeWatch.enabled = false;
  }
  activeWatch = null;
  await persistWatch(null);
  console.log("[Google Sheets Watcher] Stopped watching.");
  res.json({ message: "Stopped watching the sheet." });
});

router.post("/google-sheets/poll-now", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  if (!activeWatch || !activeWatch.enabled) {
    res.status(400).json({ error: "No active watch. Start watching a sheet first." });
    return;
  }
  try {
    const sheets = await getUncachableGoogleSheetClient();
    const data = await sheets.spreadsheets.values.get({
      spreadsheetId: activeWatch.spreadsheetId,
      range: `'${activeWatch.sheetName}'`,
    });

    const allRows = data.data.values || [];
    if (allRows.length <= 1) {
      res.json({ message: "Sheet is empty", newRows: 0 });
      return;
    }

    const headers = allRows[0].map((h: string) => (h || "").toString().trim());
    const dataRows = allRows.slice(1);
    const currentCount = dataRows.length;

    if (currentCount > activeWatch.lastRowCount) {
      const newRows = dataRows.slice(activeWatch.lastRowCount);
      const mapping = buildMapping(headers, activeWatch.fieldMapping);
      const result = await syncRows(newRows, headers, mapping, activeWatch.lastRowCount + 2);

      activeWatch.lastRowCount = currentCount;
      activeWatch.lastSync = new Date().toISOString();
      await persistWatch(activeWatch);

      res.json({ message: `Synced ${newRows.length} new rows`, ...result, totalNow: currentCount });
    } else {
      activeWatch.lastSync = new Date().toISOString();
      await persistWatch(activeWatch);
      res.json({ message: "No new rows found", newRows: 0, totalNow: currentCount });
    }
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

export default router;
