import { db } from "../configs/database";
import { sql } from "drizzle-orm";

async function cleanAllBankStatements() {
  try {
    const result = await db.execute(sql`SELECT value FROM app_settings WHERE key = 'bank_stmt_cleanup_v3'`);
    const rows = (result as any).rows || result;
    if (Array.isArray(rows) && rows.length > 0) return;

    console.log("[Cleanup] Wiping ALL bank statement documents and analyses...");

    await db.execute(sql`DELETE FROM underwriting_confirmations`);
    await db.execute(sql`DELETE FROM bank_statement_analyses`);
    await db.execute(sql`DELETE FROM documents WHERE type = 'bank_statement'`);
    try { await db.execute(sql`DELETE FROM upload_batches`); } catch {}

    console.log("[Cleanup] All bank statements, analyses, and upload batches cleared.");

    await db.execute(sql`INSERT INTO app_settings (key, value) VALUES ('bank_stmt_cleanup_v3', 'true') ON CONFLICT (key) DO UPDATE SET value = 'true'`);
  } catch (e: any) {
    console.error("[Cleanup] Bank statement cleanup error:", e.message);
  }
}

async function enableAdminAiLearning() {
  try {
    const result = await db.execute(sql`SELECT value FROM app_settings WHERE key = 'admin_ai_learning_v1'`);
    const rows = (result as any).rows || result;
    if (Array.isArray(rows) && rows.length > 0) return;

    await db.execute(sql`UPDATE role_permissions SET enabled = true, updated_at = NOW() WHERE role = 'admin' AND permission = 'ai_learning'`);
    await db.execute(sql`INSERT INTO app_settings (key, value) VALUES ('admin_ai_learning_v1', 'true') ON CONFLICT (key) DO UPDATE SET value = 'true'`);
    console.log("[Cleanup] Enabled ai_learning for admin role.");
  } catch (e: any) {
    console.error("[Cleanup] Enable admin ai_learning error:", e.message);
  }
}

async function fixOrphanedScrubbingLeads() {
  try {
    const result = await db.execute(sql`SELECT value FROM app_settings WHERE key = 'fix_orphan_scrub_v2'`);
    const rows = (result as any).rows || result;
    if (Array.isArray(rows) && rows.length > 0) return;

    await db.execute(sql`DELETE FROM underwriting_confirmations`);
    await db.execute(sql`DELETE FROM bank_statement_analyses`);
    await db.execute(sql`DELETE FROM documents WHERE type = 'bank_statement'`);
    try { await db.execute(sql`DELETE FROM upload_batches`); } catch {}
    console.log("[Cleanup] Wiped all remaining bank statements, analyses, and confirmations.");

    await db.execute(sql`
      UPDATE leads SET status = 'new', sheet_writeback_status = NULL
      WHERE status IN ('scrubbing_review', 'scrubbed')
    `);
    console.log("[Cleanup] Reset all scrubbing_review/scrubbed leads to 'new'.");

    await db.execute(sql`INSERT INTO app_settings (key, value) VALUES ('fix_orphan_scrub_v2', 'true') ON CONFLICT (key) DO UPDATE SET value = 'true'`);
  } catch (e: any) {
    console.error("[Cleanup] Fix orphaned scrubbing leads error:", e.message);
  }
}

async function clearAllScrubbingData() {
  try {
    const result = await db.execute(sql`SELECT value FROM app_settings WHERE key = 'clear_scrub_v4'`);
    const rows = (result as any).rows || result;
    if (Array.isArray(rows) && rows.length > 0) return;

    console.log("[Cleanup] Clearing ALL scrubbing data (analyses, confirmations, bank stmt docs)...");

    await db.execute(sql`DELETE FROM underwriting_confirmations`);
    await db.execute(sql`DELETE FROM bank_statement_analyses`);
    await db.execute(sql`DELETE FROM documents WHERE type = 'bank_statement'`);
    await db.execute(sql`
      UPDATE leads SET status = 'new', sheet_writeback_status = NULL,
        risk_category = NULL, gross_revenue = NULL, monthly_revenue = NULL,
        has_existing_loans = false, loan_count = 0, avg_daily_balance = NULL,
        revenue_trend = NULL, estimated_approval = NULL
      WHERE status IN ('scrubbing_review', 'scrubbed')
    `);
    try { await db.execute(sql`DELETE FROM upload_batches`); } catch {}

    await db.execute(sql`INSERT INTO app_settings (key, value) VALUES ('clear_scrub_v4', 'true') ON CONFLICT (key) DO UPDATE SET value = 'true'`);
    console.log("[Cleanup] All scrubbing data cleared.");
  } catch (e: any) {
    console.error("[Cleanup] clearAllScrubbingData error:", e.message);
  }
}

async function clearScrubV5() {
  try {
    try {
      const result = await db.execute(sql`SELECT value FROM app_settings WHERE key = 'clear_scrub_v5'`);
      const rows = (result as any).rows || result;
      if (Array.isArray(rows) && rows.length > 0) {
        return;
      }
    } catch {
      console.log("[Cleanup v5] app_settings table not ready, marking as done to skip.");
      try {
        await db.execute(sql`INSERT INTO app_settings (key, value) VALUES ('clear_scrub_v5', 'true') ON CONFLICT (key) DO UPDATE SET value = 'true'`);
      } catch {}
      return;
    }

    console.log("[Cleanup v5] Already completed or skipped.");
    await db.execute(sql`INSERT INTO app_settings (key, value) VALUES ('clear_scrub_v5', 'true') ON CONFLICT (key) DO UPDATE SET value = 'true'`);
  } catch (e: any) {
    console.error("[Cleanup v5] Error:", e.message);
  }
}

async function migrateConfirmationsFKs() {
  try {
    const result = await db.execute(sql`SELECT value FROM app_settings WHERE key = 'confirmations_fk_v1'`);
    const rows = (result as any).rows || result;
    if (Array.isArray(rows) && rows.length > 0) return;

    console.log("[Migration] Changing underwriting_confirmations FKs from CASCADE to SET NULL...");

    await db.execute(sql`ALTER TABLE underwriting_confirmations ALTER COLUMN analysis_id DROP NOT NULL`);
    await db.execute(sql`ALTER TABLE underwriting_confirmations ALTER COLUMN lead_id DROP NOT NULL`);

    try {
      await db.execute(sql`ALTER TABLE underwriting_confirmations DROP CONSTRAINT IF EXISTS underwriting_confirmations_analysis_id_bank_statement_analyses_id_fk`);
      await db.execute(sql`ALTER TABLE underwriting_confirmations DROP CONSTRAINT IF EXISTS underwriting_confirmations_lead_id_leads_id_fk`);
    } catch {}

    try {
      await db.execute(sql`ALTER TABLE underwriting_confirmations ADD CONSTRAINT underwriting_confirmations_analysis_id_bank_statement_analyses_id_fk FOREIGN KEY (analysis_id) REFERENCES bank_statement_analyses(id) ON DELETE SET NULL`);
      await db.execute(sql`ALTER TABLE underwriting_confirmations ADD CONSTRAINT underwriting_confirmations_lead_id_leads_id_fk FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE SET NULL`);
    } catch {}

    try {
      await db.execute(sql`ALTER TABLE underwriting_confirmations ADD COLUMN IF NOT EXISTS lead_business_name TEXT`);
      await db.execute(sql`
        UPDATE underwriting_confirmations uc
        SET lead_business_name = l.business_name
        FROM leads l
        WHERE uc.lead_id = l.id AND uc.lead_business_name IS NULL
      `);
    } catch {}

    await db.execute(sql`INSERT INTO app_settings (key, value) VALUES ('confirmations_fk_v1', 'true') ON CONFLICT (key) DO UPDATE SET value = 'true'`);
    console.log("[Migration] Confirmations FKs updated to SET NULL, lead_business_name backfilled.");
  } catch (e: any) {
    console.error("[Migration] confirmations FK error:", e.message);
  }
}

async function fixOrphanAnalysesV1() {
  try {
    const result = await db.execute(sql`SELECT value FROM app_settings WHERE key = 'fix_orphan_analyses_v1'`);
    const rows = (result as any).rows || result;
    if (Array.isArray(rows) && rows.length > 0) return;

    console.log("[Cleanup] Fixing orphan analyses (document_id IS NULL) and stuck scrubbing leads...");

    await db.execute(sql`
      DELETE FROM underwriting_confirmations
      WHERE analysis_id IN (SELECT id FROM bank_statement_analyses WHERE document_id IS NULL)
    `);
    await db.execute(sql`DELETE FROM bank_statement_analyses WHERE document_id IS NULL`);

    await db.execute(sql`
      UPDATE leads SET
        status = 'new',
        sheet_writeback_status = NULL,
        has_existing_loans = false,
        loan_count = 0,
        loan_details = '[]'::jsonb,
        gross_revenue = NULL,
        avg_daily_balance = NULL,
        revenue_trend = NULL,
        risk_category = NULL,
        estimated_approval = NULL
      WHERE status = 'scrubbing_review'
        AND id NOT IN (
          SELECT DISTINCT lead_id FROM bank_statement_analyses WHERE lead_id IS NOT NULL
        )
    `);

    await db.execute(sql`INSERT INTO app_settings (key, value) VALUES ('fix_orphan_analyses_v1', 'true') ON CONFLICT (key) DO UPDATE SET value = 'true'`);
    console.log("[Cleanup] Orphan analyses fixed, stuck leads reset.");
  } catch (e: any) {
    console.error("[Cleanup] fixOrphanAnalysesV1 error:", e.message);
  }
}

async function deduplicateAnalysisRows() {
  try {
    const result = await db.execute(sql`SELECT value FROM app_settings WHERE key = 'dedup_analyses_v1'`);
    const rows = (result as any).rows || result;
    if (Array.isArray(rows) && rows.length > 0) return;

    console.log("[Cleanup] Deduplicating bank_statement_analyses rows...");

    const dupesByDoc = await db.execute(sql`
      DELETE FROM bank_statement_analyses
      WHERE id IN (
        SELECT id FROM (
          SELECT id, ROW_NUMBER() OVER (
            PARTITION BY lead_id, document_id
            ORDER BY created_at DESC
          ) AS rn
          FROM bank_statement_analyses
          WHERE document_id IS NOT NULL
        ) ranked
        WHERE rn > 1
      )
    `);
    const docDupeCount = (dupesByDoc as any).rowCount || 0;

    const dupesByRevenue = await db.execute(sql`
      DELETE FROM bank_statement_analyses
      WHERE id IN (
        SELECT id FROM (
          SELECT id, ROW_NUMBER() OVER (
            PARTITION BY lead_id, monthly_revenues::text
            ORDER BY created_at DESC
          ) AS rn
          FROM bank_statement_analyses
        ) ranked
        WHERE rn > 1
      )
    `);
    const revDupeCount = (dupesByRevenue as any).rowCount || 0;

    console.log(`[Cleanup] Deleted ${docDupeCount} duplicate rows by document_id, ${revDupeCount} by identical monthly_revenues.`);

    try {
      await db.execute(sql`
        CREATE UNIQUE INDEX IF NOT EXISTS idx_bsa_lead_document_unique
        ON bank_statement_analyses (lead_id, document_id)
        WHERE document_id IS NOT NULL
      `);
      console.log("[Cleanup] Created unique index on (lead_id, document_id).");
    } catch (idxErr: any) {
      console.warn("[Cleanup] Could not create unique index:", idxErr.message);
    }

    await db.execute(sql`INSERT INTO app_settings (key, value) VALUES ('dedup_analyses_v1', 'true') ON CONFLICT (key) DO UPDATE SET value = 'true'`);
  } catch (e: any) {
    console.error("[Cleanup] deduplicateAnalysisRows error:", e.message);
  }
}

async function cleanGenericLenderRules() {
  try {
    const genericTerms = ['withdrawal','withdrawals','deposit','deposits','transfer','transfers','check','checks','debit','debits','credit','credits','payment','payments','purchase','purchases','atm','pos','wire','ach','online','mobile','counter','teller','overdraft','fee','fees','charge','charges','misc','other','adjustment','correction','reversal','refund','authorized','pending','posted'];
    const placeholders = genericTerms.map(t => `'${t}'`).join(',');
    const result = await db.execute(sql.raw(`DELETE FROM lender_rules WHERE lower(trim(lender_name)) IN (${placeholders}) OR regexp_replace(lower(trim(lender_name)), '[^a-z0-9]', '', 'g') IN (${placeholders})`));
    const count = (result as any).rowCount || 0;
    if (count > 0) {
      console.log(`[Cleanup] Removed ${count} generic lender rule(s) (withdrawal, deposit, etc.)`);
    }
  } catch (e: any) {
    console.error("[Cleanup] Error cleaning generic lender rules:", e.message);
  }
}

export async function oneTimeDataWipe() {
  if (process.env.NODE_ENV !== "production") return;

  await cleanAllBankStatements();
  await enableAdminAiLearning();
  await fixOrphanedScrubbingLeads();
  await clearAllScrubbingData();
  await clearScrubV5();
  await migrateConfirmationsFKs();
  await fixOrphanAnalysesV1();
  await deduplicateAnalysisRows();
  await cleanGenericLenderRules();

  try {
    const result = await db.execute(sql`SELECT value FROM app_settings WHERE key = 'data_wiped_v3'`);
    const rows = (result as any).rows || result;
    if (Array.isArray(rows) && rows.length > 0) {
      console.log("[Cleanup] Already wiped, skipping.");
      return;
    }

    console.log("[Cleanup] Running one-time production data wipe (v3)...");

    await db.execute(sql`UPDATE underwriting_confirmations SET analysis_id = NULL, lead_id = NULL WHERE analysis_id IS NOT NULL OR lead_id IS NOT NULL`);
    await db.execute(sql`TRUNCATE TABLE bank_statement_analyses CASCADE`);
    await db.execute(sql`TRUNCATE TABLE documents CASCADE`);
    try { await db.execute(sql`TRUNCATE TABLE upload_batches CASCADE`); } catch {}
    await db.execute(sql`TRUNCATE TABLE deals CASCADE`);
    await db.execute(sql`TRUNCATE TABLE notifications CASCADE`);
    try { await db.execute(sql`TRUNCATE TABLE activities CASCADE`); } catch {}
    try { await db.execute(sql`TRUNCATE TABLE submissions CASCADE`); } catch {}
    try { await db.execute(sql`TRUNCATE TABLE commissions CASCADE`); } catch {}
    try { await db.execute(sql`TRUNCATE TABLE calls CASCADE`); } catch {}
    try { await db.execute(sql`TRUNCATE TABLE lead_messages CASCADE`); } catch {}
    try { await db.execute(sql`TRUNCATE TABLE smart_reminders CASCADE`); } catch {}
    try { await db.execute(sql`TRUNCATE TABLE tasks CASCADE`); } catch {}
    try { await db.execute(sql`TRUNCATE TABLE stipulations CASCADE`); } catch {}
    await db.execute(sql`TRUNCATE TABLE leads CASCADE`);
    try { await db.execute(sql`TRUNCATE TABLE import_batches CASCADE`); } catch {}

    await db.execute(sql`INSERT INTO app_settings (key, value) VALUES ('data_wiped_v3', 'true') ON CONFLICT (key) DO UPDATE SET value = 'true'`);
    await db.execute(sql`INSERT INTO app_settings (key, value) VALUES ('scrub_writeback', '{"enabled":true,"spreadsheetId":"1yQzoMXXWsJpd_OS5XAkGR3dMoY427urGEw2nrn4_16k","sheetName":"Sheet1","writeColumn":"A"}') ON CONFLICT (key) DO UPDATE SET value = 'true'`);

    console.log("[Cleanup] Production data wiped successfully (v3). Scrub writeback configured.");
  } catch (e: any) {
    console.error("[Cleanup] Error during wipe:", e.message);
  }
}
