import { db, leadsTable } from "../configs/database";
import { isNull, sql } from "drizzle-orm";
import { encrypt, hmacHash, isEncrypted, normalizeSsnForHash } from "../utils/encryption";
import { eq } from "drizzle-orm";

const SENSITIVE_FIELDS = [
  "ssn", "dob", "taxId", "ein",
  "driversLicense", "dlState", "dlExpiry",
  "accountNumber", "routingNumber",
] as const;

async function encryptExistingData() {
  console.log("Starting encryption of existing lead data...");

  const leads = await db.select().from(leadsTable);
  console.log(`Found ${leads.length} leads to process`);

  let encrypted = 0;
  let skipped = 0;
  let errors = 0;

  for (const lead of leads) {
    try {
      const updates: Record<string, any> = {};
      let needsUpdate = false;

      for (const field of SENSITIVE_FIELDS) {
        const value = (lead as any)[field];
        if (value && typeof value === "string" && !isEncrypted(value)) {
          updates[field] = encrypt(value);
          needsUpdate = true;
        }
      }

      if (lead.ssn && typeof lead.ssn === "string" && !isEncrypted(lead.ssn)) {
        const normalized = normalizeSsnForHash(lead.ssn);
        if (normalized.length > 0) {
          updates.ssnHash = hmacHash(normalized);
        }
      }

      if (needsUpdate) {
        await db.update(leadsTable).set(updates).where(eq(leadsTable.id, lead.id));
        encrypted++;
      } else {
        skipped++;
      }
    } catch (e: any) {
      console.error(`Error encrypting lead ${lead.id}:`, e.message);
      errors++;
    }
  }

  console.log(`\nEncryption complete:`);
  console.log(`  Encrypted: ${encrypted}`);
  console.log(`  Skipped (already encrypted or no sensitive data): ${skipped}`);
  console.log(`  Errors: ${errors}`);

  process.exit(0);
}

encryptExistingData().catch((e) => {
  console.error("Fatal error:", e);
  process.exit(1);
});
