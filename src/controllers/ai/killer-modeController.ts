import { Router, type IRouter } from "express";
import { eq, desc, sql, inArray } from "drizzle-orm";
import { db, leadsTable, callsTable, leadMessagesTable, documentsTable } from "../../configs/database";
import { requireAuth } from "../../middlewares/authMiddleware";
import { callClaude } from "./helpersController";

const router: IRouter = Router();

const STATE_TIMEZONES: Record<string, string> = {
  AL: "America/Chicago", AK: "America/Anchorage", AZ: "America/Phoenix", AR: "America/Chicago",
  CA: "America/Los_Angeles", CO: "America/Denver", CT: "America/New_York", DE: "America/New_York",
  FL: "America/New_York", GA: "America/New_York", HI: "Pacific/Honolulu", ID: "America/Boise",
  IL: "America/Chicago", IN: "America/Indiana/Indianapolis", IA: "America/Chicago", KS: "America/Chicago",
  KY: "America/New_York", LA: "America/Chicago", ME: "America/New_York", MD: "America/New_York",
  MA: "America/New_York", MI: "America/Detroit", MN: "America/Chicago", MS: "America/Chicago",
  MO: "America/Chicago", MT: "America/Denver", NE: "America/Chicago", NV: "America/Los_Angeles",
  NH: "America/New_York", NJ: "America/New_York", NM: "America/Denver", NY: "America/New_York",
  NC: "America/New_York", ND: "America/Chicago", OH: "America/New_York", OK: "America/Chicago",
  OR: "America/Los_Angeles", PA: "America/New_York", RI: "America/New_York", SC: "America/New_York",
  SD: "America/Chicago", TN: "America/Chicago", TX: "America/Chicago", UT: "America/Denver",
  VT: "America/New_York", VA: "America/New_York", WA: "America/Los_Angeles", WV: "America/New_York",
  WI: "America/Chicago", WY: "America/Denver", DC: "America/New_York",
};

function getLocalHour(state: string | null): number | null {
  if (!state) return null;
  const tz = STATE_TIMEZONES[state.toUpperCase()];
  if (!tz) return null;
  try {
    const now = new Date();
    const fmt = new Intl.DateTimeFormat("en-US", { timeZone: tz, hour: "numeric", hour12: false });
    return parseInt(fmt.format(now));
  } catch { return null; }
}

function isCallableTime(state: string | null): boolean {
  const hour = getLocalHour(state);
  if (hour === null) return true;
  return hour >= 8 && hour < 21;
}

router.post("/ai/killer-queue", requireAuth, async (req, res): Promise<void> => {
  try {
    const user = (req as any).user;
    const rawSkip = req.body?.skipLeadIds;
    const skipLeadIds: number[] = Array.isArray(rawSkip) ? rawSkip.filter((id: any) => typeof id === "number") : [];
    const completedCount = typeof req.body?.completedCount === "number" ? req.body.completedCount : 0;

    const allLeads = await db.select().from(leadsTable)
      .where(eq(leadsTable.assignedToId, user.id));

    const activeLeads = allLeads.filter(l =>
      !["funded", "declined", "dead", "not_interested"].includes(l.status || "") &&
      !skipLeadIds.includes(l.id)
    );

    if (activeLeads.length === 0) {
      res.json({ queue: [], done: true, message: "You've gone through all your leads! Great work!" });
      return;
    }

    const leadIds = activeLeads.map(l => l.id);
    const [allCalls, allMessages, allDocs] = await Promise.all([
      leadIds.length > 0 ? db.select().from(callsTable)
        .where(inArray(callsTable.leadId, leadIds))
        .orderBy(desc(callsTable.createdAt)) : [],
      leadIds.length > 0 ? db.select({
        id: leadMessagesTable.id,
        leadId: leadMessagesTable.leadId,
        direction: leadMessagesTable.direction,
        isRead: leadMessagesTable.isRead,
        createdAt: leadMessagesTable.createdAt,
      }).from(leadMessagesTable)
        .where(inArray(leadMessagesTable.leadId, leadIds))
        .orderBy(desc(leadMessagesTable.createdAt)) : [],
      leadIds.length > 0 ? db.select({
        leadId: documentsTable.leadId,
        type: documentsTable.type,
      }).from(documentsTable)
        .where(inArray(documentsTable.leadId, leadIds)) : [],
    ]);

    const now = new Date();
    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const scored = activeLeads.map(lead => {
      const leadCalls = allCalls.filter(c => c.leadId === lead.id);
      const leadMsgs = allMessages.filter(m => m.leadId === lead.id);
      const leadDocs = allDocs.filter(d => d.leadId === lead.id);
      const callsToday = leadCalls.filter(c => new Date(c.createdAt) >= today);
      const noAnswerToday = callsToday.filter(c => c.outcome === "no_answer").length;
      const lastCall = leadCalls[0];
      const hasUnreadInbound = leadMsgs.some(m => m.direction === "inbound" && !m.isRead);
      const hasBankStatements = leadDocs.some(d => d.type === "bank_statement");
      const bankMonths = lead.bankStatementMonths as any;
      const missingMonths = bankMonths?.missing || [];
      const callable = isCallableTime(lead.state);
      const localHour = getLocalHour(lead.state);
      const ageHours = (now.getTime() - new Date(lead.createdAt).getTime()) / 3600000;
      const lastCallAge = lastCall ? (now.getTime() - new Date(lastCall.createdAt).getTime()) / 3600000 : null;

      const overdueCallback = leadCalls.find(c =>
        c.callbackAt && new Date(c.callbackAt) <= now && c.outcome === "callback"
      );

      let score = 0;
      let action = "call";
      let reason = "";
      let actionDetail = "";

      if (overdueCallback) {
        score += 200;
        reason = "Overdue callback — you promised to call back!";
        action = "call";
      }

      if (hasUnreadInbound) {
        score += 180;
        reason = "They replied! Respond immediately.";
        action = "call";
      }

      if (lead.isHot) score += 100;

      if (lead.estimatedApproval && lead.estimatedApproval > 0) {
        score += 80;
        if (lead.status === "approved") {
          reason = reason || `Approved for ${formatCurr(lead.estimatedApproval)} — close this deal!`;
          action = "call";
        }
      }

      if (ageHours < 1) {
        score += 90;
        reason = reason || "Brand new lead — call within 5 minutes!";
      }

      if (missingMonths.length > 0) {
        score += 50;
        actionDetail = `Missing statements: ${missingMonths.join(", ")}`;
        reason = reason || `Need bank statements (${missingMonths.length} months missing)`;
      }

      if (!hasBankStatements && lead.status !== "new") {
        score += 40;
        reason = reason || "No bank statements uploaded — request them";
      }

      if (noAnswerToday >= 3) {
        score -= 30;
        action = "text";
        reason = reason || "Called 3+ times today — switch to text";
      } else if (noAnswerToday >= 1 && lastCallAge !== null && lastCallAge < 1) {
        score -= 20;
        action = "text";
        reason = reason || "No answer recently — text them first, call again in 1 hour";
      }

      if (!callable) {
        score -= 100;
        action = "text";
        const stateLabel = lead.state || "unknown";
        reason = `Too early/late in ${stateLabel} (${localHour !== null ? localHour + ":00 local" : "unknown time"}) — text instead`;
      }

      if (lead.status === "new" && leadCalls.length === 0) {
        score += 60;
        reason = reason || "New lead — make first contact";
      }

      if (lead.status === "callback" && !overdueCallback) {
        score += 30;
        reason = reason || "Callback status — follow up";
      }

      const statusInfo = buildStatusInfo(lead, hasBankStatements, missingMonths);

      return {
        id: lead.id,
        businessName: lead.businessName,
        dba: lead.dba,
        ownerName: lead.ownerName,
        phone: lead.phone,
        email: lead.email,
        status: lead.status,
        state: lead.state,
        industry: lead.industry,
        isHot: lead.isHot,
        requestedAmount: lead.requestedAmount,
        monthlyRevenue: lead.monthlyRevenue,
        creditScore: lead.creditScore,
        estimatedApproval: lead.estimatedApproval,
        approvalConfidence: lead.approvalConfidence,
        riskCategory: lead.riskCategory,
        hasExistingLoans: lead.hasExistingLoans,
        loanCount: lead.loanCount,
        notes: lead.notes,
        score,
        action,
        reason: reason || "Active lead — reach out",
        actionDetail,
        statusInfo,
        callable,
        localHour,
        totalCalls: leadCalls.length,
        callsToday: callsToday.length,
        noAnswerToday,
        hasUnreadInbound,
        hasBankStatements,
        missingMonths,
        lastCallOutcome: lastCall?.outcome || null,
        lastContactedAt: lead.lastContactedAt,
      };
    });

    scored.sort((a, b) => b.score - a.score);

    res.json({
      queue: scored.slice(0, 30),
      totalRemaining: scored.length,
      completedCount,
      done: false,
    });
  } catch (e: any) {
    console.error("Killer queue error:", e);
    res.status(500).json({ error: "Failed to generate killer queue" });
  }
});

function formatCurr(n: number) {
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", minimumFractionDigits: 0 }).format(n);
}

function buildStatusInfo(lead: any, hasBankStatements: boolean, missingMonths: string[]) {
  const items: { label: string; status: "done" | "missing" | "warning" | "info" }[] = [];

  if (lead.estimatedApproval > 0) {
    items.push({ label: `Approved: ${formatCurr(lead.estimatedApproval)}`, status: "done" });
  }

  if (hasBankStatements && missingMonths.length === 0) {
    items.push({ label: "Bank Statements ✓", status: "done" });
  } else if (missingMonths.length > 0) {
    items.push({ label: `Missing: ${missingMonths.join(", ")}`, status: "missing" });
  } else {
    items.push({ label: "No Bank Statements", status: "missing" });
  }

  if (lead.creditScore) {
    items.push({ label: `Credit: ${lead.creditScore}`, status: lead.creditScore >= 600 ? "done" : "warning" });
  }

  if (lead.hasExistingLoans) {
    items.push({ label: `${lead.loanCount || "?"} existing loans`, status: "warning" });
  }

  if (lead.requestedAmount) {
    items.push({ label: `Requested: ${formatCurr(lead.requestedAmount)}`, status: "info" });
  }

  return items;
}

export default router;
