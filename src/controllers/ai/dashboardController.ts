import { Router, type IRouter } from "express";
import { eq, desc, sql, inArray } from "drizzle-orm";
import { db, leadsTable, callsTable, leadMessagesTable } from "../../configs/database";
import { requireAuth } from "../../middlewares/authMiddleware";
import { callClaude } from "./helpersController";

const router: IRouter = Router();

router.post("/ai/smart-dashboard", requireAuth, async (req, res): Promise<void> => {
  try {
    const user = (req as any).user;
    const isAdminRole = user.role === "admin" || user.role === "super_admin";

    let leadCondition;
    if (!isAdminRole) {
      leadCondition = eq(leadsTable.assignedToId, user.id);
    }

    const leads = leadCondition
      ? await db.select().from(leadsTable).where(leadCondition)
      : await db.select().from(leadsTable);

    const hotLeads = leads.filter(l => l.isHot);
    const leadsWithMissingStatements = leads.filter(l => {
      const months = l.bankStatementMonths as any;
      return months?.missing?.length > 0;
    });

    const allCalls = await db.select().from(callsTable)
      .orderBy(desc(callsTable.createdAt))
      .limit(200);

    const allMessages = await db.select({
      id: leadMessagesTable.id,
      leadId: leadMessagesTable.leadId,
      direction: leadMessagesTable.direction,
      content: leadMessagesTable.content,
      isRead: leadMessagesTable.isRead,
      isHotTrigger: leadMessagesTable.isHotTrigger,
      senderName: leadMessagesTable.senderName,
      createdAt: leadMessagesTable.createdAt,
    }).from(leadMessagesTable)
      .orderBy(desc(leadMessagesTable.createdAt))
      .limit(200);

    const leadIds = leads.map(l => l.id);
    const relevantCalls = allCalls.filter(c => leadIds.includes(c.leadId));
    const relevantMessages = allMessages.filter(m => leadIds.includes(m.leadId));

    const unreadReplies = relevantMessages.filter(m => m.direction === "inbound" && !m.isRead);

    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const leadsNeedingAttention: any[] = [];

    for (const lead of leads) {
      const leadCalls = relevantCalls.filter(c => c.leadId === lead.id);
      const leadMsgs = relevantMessages.filter(m => m.leadId === lead.id);
      const callsToday = leadCalls.filter(c => new Date(c.createdAt) >= today);
      const noAnswerToday = callsToday.filter(c => c.outcome === "no_answer").length;
      const hasUnreadReply = leadMsgs.some(m => m.direction === "inbound" && !m.isRead);
      const bankMonths = lead.bankStatementMonths as any;
      const missingMonths = bankMonths?.missing || [];

      let priority = 0;
      let reason = "";

      if (hasUnreadReply) {
        priority = 100;
        reason = "Lead replied to your message — respond immediately!";
      } else if (lead.isHot && noAnswerToday >= 2) {
        priority = 95;
        reason = `HOT lead, called ${noAnswerToday}x today with no answer — text them now!`;
      } else if (lead.isHot && leadCalls.length === 0) {
        priority = 90;
        reason = "HOT lead with no calls yet — call immediately!";
      } else if (lead.isHot && noAnswerToday >= 1) {
        priority = 85;
        reason = "HOT lead, no answer — try again or text.";
      } else if (missingMonths.length > 0 && lead.status !== "declined") {
        priority = 70;
        reason = `Missing bank statements: ${missingMonths.join(", ")} — reach out to collect.`;
      } else if (lead.status === "new" && leadCalls.length === 0) {
        priority = 60;
        reason = "New lead — make first contact.";
      } else if (lead.status === "contacted" && !leadCalls.some(c => new Date(c.createdAt) >= today)) {
        priority = 40;
        reason = "No activity today — follow up.";
      }

      if (priority > 0) {
        leadsNeedingAttention.push({
          id: lead.id,
          businessName: lead.businessName,
          ownerName: lead.ownerName,
          phone: lead.phone,
          status: lead.status,
          isHot: lead.isHot,
          priority,
          reason,
          callsToday: callsToday.length,
          noAnswerToday,
          hasUnreadReply,
          missingStatements: missingMonths,
          requestedAmount: lead.requestedAmount,
          lastContactedAt: lead.lastContactedAt,
        });
      }
    }

    leadsNeedingAttention.sort((a, b) => b.priority - a.priority);

    const topLeads = leadsNeedingAttention.slice(0, 10);

    const context = {
      repName: user.fullName || user.username,
      actionItems: topLeads,
      stats: {
        totalLeads: leads.length,
        hotLeads: hotLeads.length,
        leadsWithMissingStatements: leadsWithMissingStatements.length,
        unreadReplies: unreadReplies.length,
        callsToday: relevantCalls.filter(c => new Date(c.createdAt) >= today).length,
        messagesToday: relevantMessages.filter(m => new Date(m.createdAt) >= today).length,
      },
      currentTime: new Date().toISOString(),
      dayOfWeek: ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"][new Date().getDay()],
    };

    const system = `You are Bridge Capital's AI sales coach for a $1 billion MCA operation. Generate a personalized action plan for the sales rep. Be direct, specific, and actionable. Reference specific leads by name and explain exactly what to do.

Return JSON:
{
  "greeting": "Personalized, motivating greeting for the rep",
  "topPriority": { "leadId": number, "businessName": string, "action": string, "message": string },
  "actionPlan": [
    { "leadId": number, "businessName": string, "action": "call|text|email|collect_docs", "urgency": "critical|high|medium", "message": "Exact message or call script", "reason": "Why this action right now" }
  ],
  "dailyGoal": "Specific goal for the day based on current pipeline",
  "motivationalTip": "Quick sales tip relevant to their current situation"
}`;

    const aiResult = await callClaude(system, JSON.stringify(context), { jsonMode: true });

    let parsed;
    try {
      parsed = JSON.parse(aiResult);
    } catch {
      parsed = {
        greeting: `Good ${new Date().getHours() < 12 ? "morning" : "afternoon"}! Let's close some deals today.`,
        topPriority: null,
        actionPlan: [],
        dailyGoal: "Follow up with all hot leads.",
        motivationalTip: "Persistence pays — most deals close after the 5th touch.",
      };
    }

    res.json({
      aiPlan: parsed,
      actionItems: leadsNeedingAttention.slice(0, 20),
      stats: context.stats,
    });
  } catch (e: any) {
    console.error("AI smart dashboard error:", e);
    res.status(500).json({ error: "Failed to generate dashboard intelligence" });
  }
});

router.post("/ai/smart-queue", requireAuth, async (req, res): Promise<void> => {
  try {
    const user = (req as any).user;
    const isAdmin = user.role === "admin" || user.role === "super_admin";

    const leadCondition = isAdmin ? undefined : eq(leadsTable.assignedToId, user.id);
    const allLeads = leadCondition
      ? await db.select().from(leadsTable).where(leadCondition)
      : await db.select().from(leadsTable);

    const activeLeads = allLeads.filter(l => !["funded", "declined", "dead"].includes(l.status || ""));
    const activeLeadIds = activeLeads.map(l => l.id);

    const allCalls = activeLeadIds.length > 0
      ? await db.select().from(callsTable)
          .where(inArray(callsTable.leadId, activeLeadIds))
          .orderBy(desc(callsTable.createdAt))
      : [];

    const allMessages = activeLeadIds.length > 0
      ? await db.select({
          id: leadMessagesTable.id,
          leadId: leadMessagesTable.leadId,
          direction: leadMessagesTable.direction,
          content: leadMessagesTable.content,
          messageType: leadMessagesTable.messageType,
          createdAt: leadMessagesTable.createdAt,
        }).from(leadMessagesTable)
          .where(inArray(leadMessagesTable.leadId, activeLeadIds))
          .orderBy(desc(leadMessagesTable.createdAt))
      : [];

    const now = new Date();
    const enriched = activeLeads.map(lead => {
      const leadCalls = allCalls.filter(c => c.leadId === lead.id);
      const leadMessages = allMessages.filter(m => m.leadId === lead.id);
      const ageHours = (now.getTime() - new Date(lead.createdAt).getTime()) / (1000 * 60 * 60);
      const lastContact = lead.lastContactedAt
        ? (now.getTime() - new Date(lead.lastContactedAt).getTime()) / (1000 * 60 * 60)
        : null;
      const noAnswerCount = leadCalls.filter(c => c.outcome === "no_answer").length;
      const lastCallOutcome = leadCalls[0]?.outcome || "none";
      const hasCallback = leadCalls.some(c => c.callbackAt && new Date(c.callbackAt) > now);
      const overdueCallback = leadCalls.find(c => c.callbackAt && new Date(c.callbackAt) <= now && c.outcome === "callback");
      const lastInbound = leadMessages.filter(m => m.direction === "inbound")[0];
      const hasUnrepliedInbound = lastInbound && (!leadMessages.find(m =>
        m.direction === "outbound" && new Date(m.createdAt) > new Date(lastInbound.createdAt)
      ));

      return {
        id: lead.id,
        businessName: lead.businessName,
        ownerName: lead.ownerName,
        phone: lead.phone,
        email: lead.email,
        status: lead.status,
        isHot: lead.isHot,
        requestedAmount: lead.requestedAmount,
        monthlyRevenue: lead.monthlyRevenue,
        industry: lead.industry,
        creditScore: lead.creditScore,
        riskCategory: lead.riskCategory,
        source: lead.source,
        ageHours: Math.round(ageHours),
        hoursSinceContact: lastContact ? Math.round(lastContact) : null,
        noAnswerCount,
        totalCalls: leadCalls.length,
        totalMessages: leadMessages.length,
        lastCallOutcome,
        hasCallback,
        overdueCallback: overdueCallback ? {
          scheduledFor: overdueCallback.callbackAt?.toISOString(),
          notes: overdueCallback.notes,
        } : null,
        hasUnrepliedInbound,
        lastInboundMessage: lastInbound ? lastInbound.content?.substring(0, 100) : null,
        lastContactedAt: lead.lastContactedAt?.toISOString() || null,
        createdAt: lead.createdAt.toISOString(),
      };
    });

    const system = `You are Bridge Capital's AI Sales Commander — the brain behind a $1B merchant cash advance sales operation. You analyze an entire rep's lead portfolio and tell them EXACTLY what to do, in what order, and WHY.

You are analyzing ${enriched.length} active leads for this rep. Current time: ${now.toISOString()}, ${["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"][now.getDay()]}, hour ${now.getHours()}.

Your job: Create a SMART ACTION QUEUE — a prioritized list of the top actions this rep should take RIGHT NOW, considering:

1. OVERDUE CALLBACKS — Any lead with an overdue callback is TOP PRIORITY. The rep promised to call back.
2. HOT LEADS (isHot=true) — These leads submitted applications, they WANT funding. Highest urgency.
3. UNREPLIED INBOUND MESSAGES — Someone texted/emailed back. RESPOND IMMEDIATELY.
4. FRESH LEADS (< 1 hour old) — Brand new, call within 5 minutes for best conversion.
5. NO ANSWER LEADS — Smart re-contact. If <2 hours since last attempt, suggest text instead. If 2+ hours, suggest calling again. If 3+ no-answers today, switch to text/email.
6. STALE LEADS (2+ days old, not funded) — Resurface with text/email approach, not calls. They've been sitting too long.
7. WORKLOAD AWARENESS — If the rep has few leads (<5), be aggressive about re-contacting older leads. If they have many (20+), focus on the hottest opportunities first.

Smart callback timing rules:
- Hot lead, no answer: Try again in 10 minutes
- Warm lead, no answer: Try again in 30 minutes
- Lead from yesterday, no answer: Try again in 1-2 hours
- Lead 2+ days old: Text/email, don't call unless they responded
- Weekend: Prefer text over calls
- After 6pm: Prefer text, schedule calls for next morning

Return JSON:
{
  "actionQueue": [
    {
      "leadId": number,
      "priority": 1-N (1=do first),
      "action": "call" | "text" | "email" | "callback" | "follow_up_text" | "follow_up_email",
      "reason": "Short clear reason — e.g. 'Overdue callback from 2 hours ago' or 'Hot lead, no answer x2, text them now'",
      "suggestedMessage": "Pre-written message if action is text/email (personalized with lead details)",
      "suggestedScript": "Brief call talking points if action is call",
      "urgencyLevel": "critical" | "high" | "medium" | "low",
      "smartTiming": "Now" | "In 10 min" | "In 30 min" | "In 1 hour" | "Tomorrow 9am",
      "callbackSuggestion": "If no answer, suggest: 'Set callback for 10 min' etc."
    }
  ],
  "queueSummary": "Quick summary like 'You have 3 critical actions, 2 overdue callbacks, and 5 leads to text'",
  "repAdvice": "Coaching advice for the rep based on their portfolio — e.g. 'You have a lot of no-answers today, switch to texting for the next hour then circle back with calls'",
  "staleLeadStrategy": "Strategy for the oldest leads in the portfolio",
  "estimatedCallsToday": number,
  "estimatedTextsToday": number
}

Be SPECIFIC. Reference business names, amounts, and concrete data. Maximum 15 items in actionQueue. Prioritize ruthlessly.`;

    const result = await callClaude(system, JSON.stringify(enriched), { jsonMode: true, maxTokens: 8192 });

    let parsed;
    try {
      parsed = JSON.parse(result);
    } catch {
      parsed = {
        actionQueue: enriched.slice(0, 10).map((l, i) => ({
          leadId: l.id,
          priority: i + 1,
          action: l.noAnswerCount > 2 ? "text" : "call",
          reason: l.isHot ? "Hot lead — call immediately" : "Active lead needs contact",
          suggestedMessage: "",
          suggestedScript: "",
          urgencyLevel: l.isHot ? "critical" : "medium",
          smartTiming: "Now",
          callbackSuggestion: "Set callback for 30 min if no answer",
        })),
        queueSummary: `${enriched.length} active leads to work through`,
        repAdvice: "Focus on hot leads first, then work through no-answers.",
        staleLeadStrategy: "Text older leads instead of calling.",
        estimatedCallsToday: Math.min(enriched.length, 20),
        estimatedTextsToday: Math.min(enriched.filter(l => l.noAnswerCount > 2).length, 10),
      };
    }

    const enrichedQueue = (parsed.actionQueue || []).map((item: any) => {
      const lead = enriched.find(l => l.id === item.leadId);
      return { ...item, lead: lead || null };
    });

    res.json({ ...parsed, actionQueue: enrichedQueue });
  } catch (e: any) {
    console.error("AI smart queue error:", e);
    res.status(500).json({ error: "Failed to generate smart queue" });
  }
});

export default router;
