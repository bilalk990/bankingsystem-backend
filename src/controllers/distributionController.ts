import { Router, type IRouter } from "express";
import { eq, and } from "drizzle-orm";
import { db, leadsTable, fundersTable, dealsTable, documentsTable, bankStatementAnalysesTable, notificationsTable } from "../configs/database";
import { requireAuth } from "../middlewares/authMiddleware";
import { decryptLeadFields } from "../utils/encryption";
import { getUncachableGmailClient } from "../services/gmailService";
import path from "path";
import fs from "fs";

const router: IRouter = Router();

function sanitizeHeader(val: string): string {
  return val.replace(/[\r\n]/g, "").trim();
}

const UPLOADS_ROOT = path.resolve(process.cwd(), "uploads");

function isSafePath(filePath: string): boolean {
  const resolved = path.resolve(filePath);
  return resolved.startsWith(UPLOADS_ROOT) || resolved.startsWith(process.cwd());
}

router.get("/funders/active-list", requireAuth, async (_req, res): Promise<void> => {
  try {
    const funders = await db.select({
      id: fundersTable.id,
      name: fundersTable.name,
      type: fundersTable.type,
    }).from(fundersTable).where(eq(fundersTable.active, true)).orderBy(fundersTable.name);
    res.json({ funders });
  } catch (e: any) { res.status(500).json({ error: e.message }); }
});

router.post("/leads/:id/select-funder", requireAuth, async (req, res): Promise<void> => {
  try {
    const leadId = parseInt(req.params.id, 10);
    if (isNaN(leadId)) { res.status(400).json({ error: "Invalid lead ID" }); return; }

    const funderId = parseInt(req.body.funderId, 10);
    if (isNaN(funderId)) { res.status(400).json({ error: "Invalid funder ID" }); return; }

    const user = (req as any).user;

    let [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!lead) { res.status(404).json({ error: "Lead not found" }); return; }

    if (user.role === "rep" && lead.assignedToId !== user.id) {
      res.status(403).json({ error: "Not authorized for this lead" }); return;
    }

    if (lead.status !== "scrubbed") {
      res.status(400).json({ error: "Lead must be in scrubbed status to select a funder" }); return;
    }

    lead = decryptLeadFields(lead);

    const [funder] = await db.select().from(fundersTable)
      .where(and(eq(fundersTable.id, funderId), eq(fundersTable.active, true)));
    if (!funder) { res.status(404).json({ error: "Funder not found or inactive" }); return; }

    const isBridgeCapital = funder.type === "in_house" || /bridge\s*(capital|consolidat)/i.test(funder.name);

    if (isBridgeCapital) {
      await db.update(leadsTable).set({ status: "underwriting" }).where(eq(leadsTable.id, leadId));
      res.json({ action: "underwriting", funderId: funder.id, funderName: funder.name });
    } else {
      const analyses = await db.select().from(bankStatementAnalysesTable)
        .where(eq(bankStatementAnalysesTable.leadId, leadId));

      const allMonthlyRevenues: any[] = [];
      const allLoans: any[] = [];
      for (const a of analyses) {
        for (const mr of ((a.monthlyRevenues as any[]) || [])) allMonthlyRevenues.push(mr);
        for (const loan of ((a.loanDetails as any[]) || [])) allLoans.push(loan);
      }

      const depositsStr = allMonthlyRevenues.map(mr => {
        const amt = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", minimumFractionDigits: 0 }).format(mr.revenue || 0);
        return `${mr.month || "?"}: ${amt}`;
      }).join("\n") || "N/A";

      const loansStr = allLoans.map((l, i) => {
        const amt = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", minimumFractionDigits: 0 }).format(l.amount || 0);
        return `${i + 1}. ${l.lender || l.name || "Unknown"} - ${amt}/${l.frequency || "monthly"}`;
      }).join("\n") || "None detected";

      const docs = await db.select().from(documentsTable)
        .where(eq(documentsTable.leadId, leadId));

      const bankStatements = docs.filter(d => d.type === "bank_statement");
      const voidedChecks = docs.filter(d => d.type === "voided_check" || d.name?.toLowerCase().includes("void"));

      const emailSubject = `New Submission - ${lead.businessName} → ${funder.name}`;
      const emailBody = `Hi Ops Team,

Please find a new submission below for ${funder.name}:

BUSINESS INFORMATION
Business Name: ${lead.businessName}
DBA: ${lead.dba || "N/A"}
Owner: ${lead.ownerName}
Phone: ${lead.phone}
Email: ${lead.email}
Industry: ${lead.industry || "N/A"}
State: ${lead.state || "N/A"}
Time in Business: ${lead.timeInBusiness || "N/A"}
Credit Score: ${lead.creditScore || "N/A"}

REQUESTED AMOUNT
${lead.requestedAmount ? new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", minimumFractionDigits: 0 }).format(Number(lead.requestedAmount)) : "N/A"}

SELECTED FUNDER
${funder.name}

MONTHLY DEPOSITS
${depositsStr}

EXISTING POSITIONS
${loansStr}

ATTACHED DOCUMENTS
Bank Statements: ${bankStatements.length} file(s)
Voided Check: ${voidedChecks.length} file(s)
Other Documents: ${docs.length - bankStatements.length - voidedChecks.length} file(s)

Submitted by: ${user.fullName} (${user.email})

Best regards,
Bridge Capital CRM`;

      res.json({
        action: "email",
        funderId: funder.id,
        funderName: funder.name,
        emailDraft: {
          to: "ops@bridgeconsolidation.com",
          subject: emailSubject,
          body: emailBody,
          attachmentCount: docs.length,
          attachments: docs.map(d => ({ id: d.id, name: d.name, type: d.type })),
        },
      });
    }
  } catch (e: any) {
    console.error("Select funder error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.post("/leads/:id/send-submission-email", requireAuth, async (req, res): Promise<void> => {
  try {
    const leadId = parseInt(req.params.id, 10);
    if (isNaN(leadId)) { res.status(400).json({ error: "Invalid lead ID" }); return; }

    const { to, subject, body } = req.body;
    if (!subject || !body) { res.status(400).json({ error: "Subject and body are required" }); return; }

    const funderId = parseInt(req.body.funderId, 10);
    const user = (req as any).user;

    let [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!lead) { res.status(404).json({ error: "Lead not found" }); return; }

    if (user.role === "rep" && lead.assignedToId !== user.id) {
      res.status(403).json({ error: "Not authorized for this lead" }); return;
    }

    if (lead.status !== "scrubbed") {
      res.status(400).json({ error: "Lead must be in scrubbed status" }); return;
    }

    lead = decryptLeadFields(lead);

    const [funder] = isNaN(funderId) ? [null] :
      await db.select().from(fundersTable).where(eq(fundersTable.id, funderId));

    const funderName = funder?.name || req.body.funderName || "Unknown Funder";
    const recipientEmail = sanitizeHeader(to || "ops@bridgeconsolidation.com");
    const safeSubject = sanitizeHeader(subject);

    const gmail = await getUncachableGmailClient();

    const attachmentParts: string[] = [];
    const boundary = `boundary_${Date.now()}_${Math.random().toString(36).slice(2)}`;

    const docs = await db.select().from(documentsTable)
      .where(eq(documentsTable.leadId, leadId));

    for (const doc of docs) {
      if (!doc.url) continue;
      const filePath = path.join(process.cwd(), doc.url.startsWith("/") ? doc.url.slice(1) : doc.url);
      if (!isSafePath(filePath) || !fs.existsSync(filePath)) continue;

      const fileData = fs.readFileSync(filePath);
      const base64Data = fileData.toString("base64");
      const ext = path.extname(doc.name || "").toLowerCase();
      let mimeType = "application/octet-stream";
      if (ext === ".pdf") mimeType = "application/pdf";
      else if ([".png", ".jpg", ".jpeg"].includes(ext)) mimeType = `image/${ext.slice(1)}`;
      else if (ext === ".csv") mimeType = "text/csv";

      const safeName = (doc.name || "document").replace(/["\r\n]/g, "");

      attachmentParts.push(
        `--${boundary}\r\n` +
        `Content-Type: ${mimeType}\r\n` +
        `Content-Disposition: attachment; filename="${safeName}"\r\n` +
        `Content-Transfer-Encoding: base64\r\n\r\n` +
        `${base64Data}\r\n`
      );
    }

    let rawEmail: string;
    if (attachmentParts.length > 0) {
      rawEmail = [
        `From: me`,
        `To: ${recipientEmail}`,
        `Subject: ${safeSubject}`,
        `MIME-Version: 1.0`,
        `Content-Type: multipart/mixed; boundary="${boundary}"`,
        ``,
        `--${boundary}`,
        `Content-Type: text/plain; charset="UTF-8"`,
        `Content-Transfer-Encoding: 7bit`,
        ``,
        body,
        ``,
        ...attachmentParts,
        `--${boundary}--`,
      ].join("\r\n");
    } else {
      rawEmail = [
        `From: me`,
        `To: ${recipientEmail}`,
        `Subject: ${safeSubject}`,
        `Content-Type: text/plain; charset="UTF-8"`,
        ``,
        body,
      ].join("\r\n");
    }

    const encodedEmail = Buffer.from(rawEmail).toString("base64url");

    await gmail.users.messages.send({
      userId: "me",
      requestBody: { raw: encodedEmail },
    });

    await db.update(leadsTable).set({ status: "submitted" }).where(eq(leadsTable.id, leadId));

    const [deal] = await db.insert(dealsTable).values({
      leadId,
      repId: user.id,
      stage: "submitted",
      amount: Number(lead.requestedAmount) || 0,
      funderId: funder?.id || null,
      funderName: funderName,
      fundingSource: "out_house",
      notes: `Submitted to ${funderName} via email to ${recipientEmail}`,
    }).returning();

    await db.insert(notificationsTable).values({
      userId: user.id,
      title: "Submission Sent",
      message: `${lead.businessName} submitted to ${funderName}`,
      type: "deal",
      relatedId: deal.id,
    });

    console.log(`[Distribution] Email sent for lead ${lead.businessName} to ${funderName} by ${user.fullName}`);

    res.json({ success: true, dealId: deal.id, emailSent: true });
  } catch (e: any) {
    console.error("Send submission email error:", e);
    res.status(500).json({ error: e.message });
  }
});

export default router;
