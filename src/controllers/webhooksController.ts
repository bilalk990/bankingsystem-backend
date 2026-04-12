import { Router, type IRouter } from "express";
import { eq, or, and, sql, ilike, desc, gte, count } from "drizzle-orm";
import crypto from "crypto";
import {
  db,
  leadsTable,
  documentsTable,
  webhooksTable,
  leadMessagesTable,
  bankStatementAnalysesTable,
  activitiesTable,
} from "../configs/database";
import { requireAuth, requireAdmin, requirePermission } from "../middlewares/authMiddleware";
import { encryptLeadFields, decryptLeadFields, hmacHash, normalizeSsnForHash } from "../utils/encryption";

const router: IRouter = Router();

function generateWebhookKey(): string {
  return crypto.randomBytes(32).toString("hex");
}

function normalizePhone(phone: string): string {
  return phone.replace(/\D/g, "").slice(-10);
}

function normalizeEmail(email: string): string {
  return email.toLowerCase().trim();
}

function fuzzyBusinessMatch(name1: string, name2: string): number {
  const n1 = name1.toLowerCase().replace(/[^a-z0-9]/g, "");
  const n2 = name2.toLowerCase().replace(/[^a-z0-9]/g, "");
  if (n1 === n2) return 100;
  if (n1.includes(n2) || n2.includes(n1)) return 85;

  let matches = 0;
  const words1 = name1.toLowerCase().split(/\s+/).filter(w => w.length > 2);
  const words2 = name2.toLowerCase().split(/\s+/).filter(w => w.length > 2);
  for (const w1 of words1) {
    for (const w2 of words2) {
      if (w1 === w2) matches++;
    }
  }
  if (words1.length > 0 && words2.length > 0) {
    return Math.round((matches / Math.max(words1.length, words2.length)) * 100);
  }
  return 0;
}

async function findExistingLead(data: {
  phone?: string;
  email?: string;
  ssn?: string;
  businessName?: string;
}): Promise<{ lead: any; matchType: string; confidence: number } | null> {
  if (data.phone) {
    const normalized = normalizePhone(data.phone);
    if (normalized.length === 10) {
      const matches = await db.select().from(leadsTable)
        .where(sql`REPLACE(REPLACE(REPLACE(REPLACE(${leadsTable.phone}, '-', ''), '(', ''), ')', ''), ' ', '') LIKE ${'%' + normalized}`);
      if (matches.length === 1) return { lead: matches[0], matchType: "phone", confidence: 100 };
      if (matches.length > 1) return { lead: matches[0], matchType: "phone_multiple", confidence: 90 };
    }
  }

  if (data.email) {
    const normalized = normalizeEmail(data.email);
    const matches = await db.select().from(leadsTable)
      .where(sql`LOWER(${leadsTable.email}) = ${normalized}`);
    if (matches.length === 1) return { lead: matches[0], matchType: "email", confidence: 100 };
    if (matches.length > 1) return { lead: matches[0], matchType: "email_multiple", confidence: 85 };
  }

  if (data.ssn) {
    const cleanSsn = data.ssn.replace(/\D/g, "");
    if (cleanSsn.length >= 9) {
      const ssnHashVal = hmacHash(normalizeSsnForHash(data.ssn));
      const matches = await db.select().from(leadsTable)
        .where(eq(leadsTable.ssnHash, ssnHashVal));
      if (matches.length >= 1) {
        const decrypted = decryptLeadFields(matches[0]);
        return { lead: { ...matches[0], ...decrypted }, matchType: "ssn", confidence: 95 };
      }
    }
  }

  if (data.businessName) {
    const allLeads = await db.select({
      id: leadsTable.id,
      businessName: leadsTable.businessName,
      dba: leadsTable.dba,
      ownerName: leadsTable.ownerName,
      phone: leadsTable.phone,
      email: leadsTable.email,
    }).from(leadsTable);

    let bestMatch: any = null;
    let bestScore = 0;

    for (const lead of allLeads) {
      const score1 = fuzzyBusinessMatch(data.businessName, lead.businessName);
      const score2 = lead.dba ? fuzzyBusinessMatch(data.businessName, lead.dba) : 0;
      const score = Math.max(score1, score2);
      if (score > bestScore && score >= 75) {
        bestScore = score;
        bestMatch = lead;
      }
    }

    if (bestMatch) {
      const fullLead = await db.select().from(leadsTable).where(eq(leadsTable.id, bestMatch.id)).limit(1);
      return { lead: fullLead[0], matchType: "business_name", confidence: bestScore };
    }
  }

  return null;
}

async function checkBankStatementFreshness(leadId: number): Promise<{
  status: string;
  coveredMonths: string[];
  missingMonths: string[];
  isComplete: boolean;
}> {
  const now = new Date();
  const requiredMonths: string[] = [];
  for (let i = 0; i < 4; i++) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    requiredMonths.push(`${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`);
  }

  const analyses = await db.select().from(bankStatementAnalysesTable)
    .where(eq(bankStatementAnalysesTable.leadId, leadId))
    .orderBy(desc(bankStatementAnalysesTable.createdAt));

  const coveredMonths: Set<string> = new Set();

  for (const a of analyses) {
    const revenues = a.monthlyRevenues as any[];
    if (revenues && Array.isArray(revenues)) {
      for (const r of revenues) {
        if (r.month) coveredMonths.add(r.month);
      }
    }
  }

  const docs = await db.select().from(documentsTable)
    .where(and(
      eq(documentsTable.leadId, leadId),
      eq(documentsTable.type, "bank_statement")
    ));

  for (const doc of docs) {
    const nameMatch = doc.name.match(/(\d{4}[-_]?\d{2})/);
    if (nameMatch) {
      coveredMonths.add(nameMatch[1].replace("_", "-"));
    }
  }

  const coveredArr = Array.from(coveredMonths);
  const missingMonths = requiredMonths.filter(m => !coveredArr.includes(m));

  let status: string;
  if (docs.length === 0) {
    status = "none";
  } else if (missingMonths.length === 0) {
    status = "complete";
  } else if (missingMonths.length <= 1) {
    status = "partial";
  } else {
    status = "outdated";
  }

  return { status, coveredMonths: coveredArr, missingMonths, isComplete: missingMonths.length === 0 };
}

router.get("/webhooks", requireAuth, requireAdmin, requirePermission("webhooks"), async (_req, res) => {
  try {
    const webhooks = await db.select().from(webhooksTable).orderBy(desc(webhooksTable.createdAt));
    res.json(webhooks);
  } catch (e: any) {
    res.status(500).json({ error: "Failed to fetch webhooks" });
  }
});

router.post("/webhooks", requireAuth, requireAdmin, requirePermission("webhooks"), async (req, res) => {
  try {
    const { name, type, fieldMapping } = req.body;
    if (!name || !type) return res.status(400).json({ error: "Name and type are required" });

    const validTypes = ["new_leads", "applications", "retarget_responses", "bank_statements", "lead_updates"];
    if (!validTypes.includes(type)) {
      return res.status(400).json({ error: `Invalid type. Must be one of: ${validTypes.join(", ")}` });
    }

    const webhookKey = generateWebhookKey();
    const [webhook] = await db.insert(webhooksTable).values({
      name,
      type,
      webhookKey,
      fieldMapping: fieldMapping || null,
      isActive: true,
      createdById: (req as any).user.id,
    }).returning();

    res.json(webhook);
  } catch (e: any) {
    res.status(500).json({ error: "Failed to create webhook" });
  }
});

router.patch("/webhooks/:id", requireAuth, requireAdmin, requirePermission("webhooks"), async (req, res) => {
  try {
    const id = parseInt(String(req.params.id), 10);
    if (isNaN(id)) return res.status(400).json({ error: "Invalid webhook ID" });

    const { name, isActive, fieldMapping } = req.body;
    const updates: any = {};
    if (name !== undefined) updates.name = name;
    if (isActive !== undefined) updates.isActive = isActive;
    if (fieldMapping !== undefined) updates.fieldMapping = fieldMapping;

    const [updated] = await db.update(webhooksTable).set(updates)
      .where(eq(webhooksTable.id, id)).returning();

    if (!updated) return res.status(404).json({ error: "Webhook not found" });
    res.json(updated);
  } catch (e: any) {
    res.status(500).json({ error: "Failed to update webhook" });
  }
});

router.delete("/webhooks/:id", requireAuth, requireAdmin, requirePermission("webhooks"), async (req, res) => {
  try {
    const id = parseInt(String(req.params.id), 10);
    if (isNaN(id)) return res.status(400).json({ error: "Invalid webhook ID" });

    const [deleted] = await db.delete(webhooksTable).where(eq(webhooksTable.id, id)).returning();
    if (!deleted) return res.status(404).json({ error: "Webhook not found" });
    res.json({ success: true });
  } catch (e: any) {
    res.status(500).json({ error: "Failed to delete webhook" });
  }
});

router.post("/webhooks/receive/:key", async (req, res) => {
  try {
    const { key } = req.params;
    const [webhook] = await db.select().from(webhooksTable)
      .where(eq(webhooksTable.webhookKey, key));

    if (!webhook) return res.status(404).json({ error: "Invalid webhook" });
    if (!webhook.isActive) return res.status(403).json({ error: "Webhook is disabled" });

    const payload = req.body;
    if (!payload || typeof payload !== "object") {
      return res.status(400).json({ error: "Invalid payload" });
    }

    await db.update(webhooksTable).set({
      lastTriggered: new Date(),
      triggerCount: (webhook.triggerCount || 0) + 1,
    }).where(eq(webhooksTable.id, webhook.id));

    const mapping = webhook.fieldMapping as Record<string, string> | null;
    const mapped: Record<string, string> = {};
    if (mapping) {
      for (const [ourField, theirField] of Object.entries(mapping)) {
        if (payload[theirField] !== undefined) {
          mapped[ourField] = String(payload[theirField]);
        }
      }
    }

    const data = { ...payload, ...mapped };

    const extractField = (keys: string[]): string | undefined => {
      for (const k of keys) {
        const val = data[k] || data[k.toLowerCase()] || data[k.toUpperCase()];
        if (val) return String(val).trim();
      }
      return undefined;
    };

    const phone = extractField(["phone", "Phone", "phone_number", "phoneNumber", "mobile", "cell"]);
    const email = extractField(["email", "Email", "email_address", "emailAddress"]);
    const businessName = extractField(["businessName", "business_name", "Business Name", "business", "company", "Company"]);
    const ownerName = extractField(["ownerName", "owner_name", "Owner Name", "name", "Name", "full_name", "fullName"]);
    const ssn = extractField(["ssn", "SSN", "social_security"]);
    const message = extractField(["message", "Message", "response", "Response", "text", "body", "content"]);

    const matchResult = await findExistingLead({ phone, email, ssn, businessName });

    let result: any = { webhookId: webhook.id, type: webhook.type };

    if (webhook.type === "retarget_responses") {
      if (matchResult) {
        const lead = matchResult.lead;

        if (message) {
          await db.insert(leadMessagesTable).values({
            leadId: lead.id,
            source: `webhook:${webhook.name}`,
            direction: "inbound",
            content: message,
            senderName: ownerName || lead.ownerName,
            metadata: payload,
            isHotTrigger: true,
            webhookId: webhook.id,
          });
        }

        const freshness = await checkBankStatementFreshness(lead.id);

        await db.update(leadsTable).set({
          isHot: true,
          hotTriggeredAt: new Date(),
          bankStatementsStatus: freshness.status,
        }).where(eq(leadsTable.id, lead.id));

        await db.insert(activitiesTable).values({
          type: "hot_lead",
          description: `🔥 ${lead.businessName} triggered as HOT via retarget webhook "${webhook.name}". Match: ${matchResult.matchType} (${matchResult.confidence}% confidence). Statements: ${freshness.status}${freshness.missingMonths.length > 0 ? ` — Missing: ${freshness.missingMonths.join(", ")}` : ""}`,
          leadId: lead.id,
        });

        result = {
          ...result,
          action: "hot_lead_tagged",
          leadId: lead.id,
          businessName: lead.businessName,
          matchType: matchResult.matchType,
          matchConfidence: matchResult.confidence,
          bankStatements: freshness,
          isHot: true,
          duplicates: matchResult.matchType.includes("multiple") ? "ALERT: Multiple matches found — admin review needed" : null,
        };
      } else {
        const newLeadData: any = {
          businessName: businessName || "Unknown Business",
          ownerName: ownerName || "Unknown",
          phone: phone || "0000000000",
          email: email || null,
          status: "new",
          source: `webhook:${webhook.name}`,
          isHot: true,
          hotTriggeredAt: new Date(),
        };

        const encryptedRetargetLead = encryptLeadFields(newLeadData);
        const [newLead] = await db.insert(leadsTable).values(encryptedRetargetLead).returning();

        if (message) {
          await db.insert(leadMessagesTable).values({
            leadId: newLead.id,
            source: `webhook:${webhook.name}`,
            direction: "inbound",
            content: message,
            senderName: ownerName,
            metadata: payload,
            isHotTrigger: true,
            webhookId: webhook.id,
          });
        }

        await db.insert(activitiesTable).values({
          type: "lead_created",
          description: `🔥 New HOT lead created via retarget webhook "${webhook.name}": ${newLeadData.businessName}. No existing match found.`,
          leadId: newLead.id,
        });

        result = {
          ...result,
          action: "new_hot_lead_created",
          leadId: newLead.id,
          businessName: newLeadData.businessName,
          isHot: true,
        };
      }
    } else if (webhook.type === "new_leads" || webhook.type === "applications") {
      const requestedAmount = extractField(["requestedAmount", "requested_amount", "amount", "Amount", "funding_amount"]);
      const monthlyRevenue = extractField(["monthlyRevenue", "monthly_revenue", "revenue", "Revenue", "gross_revenue"]);
      const creditScore = extractField(["creditScore", "credit_score", "Credit Score", "fico", "FICO"]);
      const industry = extractField(["industry", "Industry", "business_type", "businessType"]);
      const stateVal = extractField(["state", "State", "business_state"]);
      const city = extractField(["city", "City", "business_city"]);
      const address = extractField(["address", "Address", "business_address", "street"]);
      const zip = extractField(["zip", "Zip", "zipCode", "zip_code", "postal"]);
      const dba = extractField(["dba", "DBA", "doing_business_as"]);
      const ein = extractField(["ein", "EIN", "tax_id", "taxId", "federal_tax_id"]);
      const dob = extractField(["dob", "DOB", "date_of_birth", "dateOfBirth", "birthday"]);
      const yearsInBusiness = extractField(["yearsInBusiness", "years_in_business", "Years in Business", "time_in_business"]);
      const businessStartDate = extractField(["businessStartDate", "business_start_date", "start_date", "date_started"]);
      const ownershipPct = extractField(["ownershipPct", "ownership_pct", "ownership_percentage", "ownership"]);
      const homeAddress = extractField(["homeAddress", "home_address", "personal_address", "residential_address"]);
      const homeCity = extractField(["homeCity", "home_city"]);
      const homeState = extractField(["homeState", "home_state"]);
      const homeZip = extractField(["homeZip", "home_zip"]);
      const driversLicense = extractField(["driversLicense", "drivers_license", "dl_number", "license_number"]);
      const dlState = extractField(["dlState", "dl_state", "license_state"]);
      const dlExpiry = extractField(["dlExpiry", "dl_expiry", "license_expiry"]);
      const bankName = extractField(["bankName", "bank_name", "bank"]);
      const accountNumber = extractField(["accountNumber", "account_number", "account"]);
      const routingNumber = extractField(["routingNumber", "routing_number", "routing"]);
      const businessTypeVal = extractField(["businessType", "business_type", "entity_type", "Business Type"]);

      const bankStatementFiles = extractField(["bank_statements", "bankStatements", "statements", "uploaded_statements"]);
      const bankStatementMonthsRaw = extractField(["bank_statement_months", "statementMonths", "statement_months", "months_provided"]);

      let bankStatementMonthsData: { covered: string[], missing: string[] } | null = null;

      if (bankStatementMonthsRaw) {
        try {
          const parsed = JSON.parse(bankStatementMonthsRaw);
          if (Array.isArray(parsed)) {
            const now = new Date();
            const required: string[] = [];
            for (let i = 0; i < 4; i++) {
              const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
              required.push(`${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`);
            }
            const missing = required.filter(m => !parsed.includes(m));
            bankStatementMonthsData = { covered: parsed, missing };
          }
        } catch { }
      }

      if (matchResult && matchResult.confidence >= 90) {
        const lead = matchResult.lead;

        const updates: any = {};
        const decryptedLead = decryptLeadFields(lead);
        if (email && !lead.email) updates.email = email;
        if (requestedAmount) updates.requestedAmount = parseFloat(requestedAmount);
        if (monthlyRevenue) updates.monthlyRevenue = parseFloat(monthlyRevenue);
        if (creditScore) updates.creditScore = parseInt(creditScore);
        if (industry) updates.industry = industry;
        if (dba && !lead.dba) updates.dba = dba;
        if (ein && !decryptedLead.ein) updates.ein = ein;
        if (dob && !decryptedLead.dob) updates.dob = dob;
        if (ssn && !decryptedLead.ssn) updates.ssn = ssn;
        if (yearsInBusiness) updates.yearsInBusiness = parseFloat(yearsInBusiness);
        if (businessStartDate) updates.businessStartDate = businessStartDate;
        if (ownershipPct) updates.ownershipPct = parseFloat(ownershipPct);
        if (address && !lead.address) updates.address = address;
        if (city && !lead.city) updates.city = city;
        if (stateVal && !lead.state) updates.state = stateVal;
        if (zip && !lead.zip) updates.zip = zip;
        if (homeAddress && !lead.homeAddress) updates.homeAddress = homeAddress;
        if (homeCity && !lead.homeCity) updates.homeCity = homeCity;
        if (homeState && !lead.homeState) updates.homeState = homeState;
        if (homeZip && !lead.homeZip) updates.homeZip = homeZip;
        if (driversLicense && !decryptedLead.driversLicense) updates.driversLicense = driversLicense;
        if (dlState && !decryptedLead.dlState) updates.dlState = dlState;
        if (dlExpiry && !decryptedLead.dlExpiry) updates.dlExpiry = dlExpiry;
        if (bankName && !lead.bankName) updates.bankName = bankName;
        if (accountNumber && !decryptedLead.accountNumber) updates.accountNumber = accountNumber;
        if (routingNumber && !decryptedLead.routingNumber) updates.routingNumber = routingNumber;
        if (businessTypeVal && !lead.businessType) updates.businessType = businessTypeVal;

        if (bankStatementMonthsData) {
          updates.bankStatementMonths = bankStatementMonthsData;
          updates.bankStatementsStatus = bankStatementMonthsData.missing.length === 0 ? "complete" : bankStatementMonthsData.missing.length <= 1 ? "partial" : "outdated";
        }

        if (webhook.type === "applications") {
          if (lead.status === "new") updates.status = "contacted";
          updates.isHot = true;
          updates.hotTriggeredAt = new Date();
        }

        if (Object.keys(updates).length > 0) {
          const encryptedUpdates = encryptLeadFields(updates);
          await db.update(leadsTable).set(encryptedUpdates).where(eq(leadsTable.id, lead.id));
        }

        const missingInfo = bankStatementMonthsData?.missing.length
          ? ` Missing bank statements: ${bankStatementMonthsData.missing.join(", ")}.`
          : "";

        await db.insert(activitiesTable).values({
          type: webhook.type === "applications" ? "application_received" : "lead_updated",
          description: `${webhook.type === "applications" ? "📋 Application received" : "Lead updated"} via webhook "${webhook.name}". Match: ${matchResult.matchType} (${matchResult.confidence}% confidence). Updated ${Object.keys(updates).length} fields.${missingInfo}${matchResult.matchType.includes("multiple") ? " ⚠️ DUPLICATE ALERT: Multiple matches found!" : ""}`,
          leadId: lead.id,
        });

        result = {
          ...result,
          action: "existing_lead_updated",
          leadId: lead.id,
          businessName: lead.businessName,
          matchType: matchResult.matchType,
          matchConfidence: matchResult.confidence,
          fieldsUpdated: Object.keys(updates),
          bankStatements: bankStatementMonthsData,
          duplicates: matchResult.matchType.includes("multiple") ? "ALERT: Multiple matches found — admin review needed" : null,
        };
      } else {
        if (!businessName && !ownerName) {
          return res.status(400).json({ error: "At least businessName or ownerName is required for new leads" });
        }

        const newLeadData: any = {
          businessName: businessName || `${ownerName}'s Business`,
          ownerName: ownerName || "Unknown",
          phone: phone || "0000000000",
          email: email || null,
          ssn: ssn || null,
          dba: dba || null,
          ein: ein || null,
          dob: dob || null,
          status: webhook.type === "applications" ? "contacted" : "new",
          source: `webhook:${webhook.name}`,
          requestedAmount: requestedAmount ? parseFloat(requestedAmount) : null,
          monthlyRevenue: monthlyRevenue ? parseFloat(monthlyRevenue) : null,
          creditScore: creditScore ? parseInt(creditScore) : null,
          industry: industry || null,
          businessType: businessTypeVal || null,
          yearsInBusiness: yearsInBusiness ? parseFloat(yearsInBusiness) : null,
          businessStartDate: businessStartDate || null,
          ownershipPct: ownershipPct ? parseFloat(ownershipPct) : null,
          address: address || null,
          city: city || null,
          state: stateVal || null,
          zip: zip || null,
          homeAddress: homeAddress || null,
          homeCity: homeCity || null,
          homeState: homeState || null,
          homeZip: homeZip || null,
          driversLicense: driversLicense || null,
          dlState: dlState || null,
          dlExpiry: dlExpiry || null,
          bankName: bankName || null,
          accountNumber: accountNumber || null,
          routingNumber: routingNumber || null,
          bankStatementMonths: bankStatementMonthsData || null,
          bankStatementsStatus: bankStatementMonthsData
            ? (bankStatementMonthsData.missing.length === 0 ? "complete" : bankStatementMonthsData.missing.length <= 1 ? "partial" : "outdated")
            : "none",
          isHot: webhook.type === "applications",
          hotTriggeredAt: webhook.type === "applications" ? new Date() : null,
        };

        if (matchResult && matchResult.confidence >= 50) {
          await db.insert(activitiesTable).values({
            type: "duplicate_alert",
            description: `⚠️ POTENTIAL DUPLICATE: New lead "${newLeadData.businessName}" via webhook "${webhook.name}" has ${matchResult.confidence}% match with existing lead #${matchResult.lead.id} "${matchResult.lead.businessName}" (matched by ${matchResult.matchType}). Admin review recommended.`,
            leadId: matchResult.lead.id,
          });
          newLeadData.notes = `[POTENTIAL DUPLICATE] ${matchResult.confidence}% match with lead #${matchResult.lead.id} "${matchResult.lead.businessName}" (${matchResult.matchType})`;
        }

        const encryptedNewLeadData = encryptLeadFields(newLeadData);
        const [newLead] = await db.insert(leadsTable).values(encryptedNewLeadData).returning();

        const missingInfo = bankStatementMonthsData?.missing.length
          ? ` Missing bank statements: ${bankStatementMonthsData.missing.join(", ")}.`
          : "";

        await db.insert(activitiesTable).values({
          type: webhook.type === "applications" ? "application_received" : "lead_created",
          description: `${webhook.type === "applications" ? "📋 New application received" : "New lead created"} via webhook "${webhook.name}": ${newLeadData.businessName}.${missingInfo}`,
          leadId: newLead.id,
        });

        result = {
          ...result,
          action: "new_lead_created",
          leadId: newLead.id,
          businessName: newLeadData.businessName,
          bankStatements: bankStatementMonthsData,
          potentialDuplicate: matchResult ? {
            existingLeadId: matchResult.lead.id,
            existingBusinessName: matchResult.lead.businessName,
            confidence: matchResult.confidence,
            matchType: matchResult.matchType,
          } : null,
        };
      }
    } else if (webhook.type === "lead_updates") {
      if (!matchResult) {
        return res.status(404).json({ error: "No matching lead found for update" });
      }

      const lead = matchResult.lead;
      const updates: any = {};
      const statusField = extractField(["status", "Status"]);
      if (statusField) updates.status = statusField;
      const requestedAmount = extractField(["requestedAmount", "requested_amount", "amount"]);
      if (requestedAmount) updates.requestedAmount = parseFloat(requestedAmount);
      const notes = extractField(["notes", "Notes", "note"]);
      if (notes) updates.notes = (lead.notes ? lead.notes + "\n" : "") + notes;

      if (Object.keys(updates).length > 0) {
        await db.update(leadsTable).set(updates).where(eq(leadsTable.id, lead.id));
      }

      result = {
        ...result,
        action: "lead_updated",
        leadId: lead.id,
        businessName: lead.businessName,
        fieldsUpdated: Object.keys(updates),
      };
    }

    res.json({ success: true, ...result });
  } catch (e: any) {
    console.error("Webhook receive error:", e);
    res.status(500).json({ error: "Failed to process webhook" });
  }
});

router.get("/leads/:id/statement-freshness", requireAuth, async (req, res) => {
  try {
    const leadId = parseInt(String(req.params.id), 10);
    if (isNaN(leadId)) return res.status(400).json({ error: "Invalid lead ID" });

    const user = (req as any).user;
    const [lead] = await db.select({ assignedToId: leadsTable.assignedToId }).from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!lead) return res.status(404).json({ error: "Lead not found" });
    if (user.role === "rep" && lead.assignedToId !== user.id) return res.status(403).json({ error: "Not authorized" });

    const freshness = await checkBankStatementFreshness(leadId);

    await db.update(leadsTable).set({ bankStatementsStatus: freshness.status })
      .where(eq(leadsTable.id, leadId));

    res.json(freshness);
  } catch (e: any) {
    res.status(500).json({ error: "Failed to check statement freshness" });
  }
});

router.get("/webhooks/:id/logs", requireAuth, requireAdmin, requirePermission("webhooks"), async (req, res) => {
  try {
    const webhookId = parseInt(String(req.params.id), 10);
    if (isNaN(webhookId)) return res.status(400).json({ error: "Invalid webhook ID" });

    const [webhook] = await db.select({ name: webhooksTable.name }).from(webhooksTable).where(eq(webhooksTable.id, webhookId));
    if (!webhook) return res.status(404).json({ error: "Webhook not found" });

    const logs = await db.select({
      id: activitiesTable.id,
      type: activitiesTable.type,
      description: activitiesTable.description,
      leadId: activitiesTable.leadId,
      createdAt: activitiesTable.createdAt,
    }).from(activitiesTable)
      .where(sql`${activitiesTable.description} LIKE ${'%webhook%'} AND ${activitiesTable.description} LIKE ${'%' + webhook.name + '%'}`)
      .orderBy(desc(activitiesTable.createdAt))
      .limit(50);

    res.json(logs);
  } catch (e: any) {
    res.status(500).json({ error: "Failed to fetch webhook logs" });
  }
});

export { checkBankStatementFreshness };
export default router;
