import { Router, type IRouter } from "express";
import { eq, desc, sql } from "drizzle-orm";
import { db, leadsTable, callsTable, dealsTable, leadMessagesTable } from "../../configs/database";
import { requireAuth, checkLeadOwnership } from "../../middlewares/authMiddleware";
import { callClaude, getLeadContext } from "./helpersController";

const router: IRouter = Router();

router.post("/ai/generate-script/:id", requireAuth, async (req, res) => {
  try {
    const leadId = parseInt(req.params.id);
    if (isNaN(leadId)) return res.status(400).json({ error: "Invalid lead ID" });
    const user = (req as any).user;
    if (!(await checkLeadOwnership(leadId, user, res))) return;
    const { type } = req.body;
    const leadData = await getLeadContext(leadId);
    if (!leadData) return res.status(404).json({ error: "Lead not found" });

    const validTypes = ["cold_call", "follow_up", "email_intro", "email_follow_up", "sms", "objection_playbook"];
    const scriptType = validTypes.includes(type) ? type : "cold_call";

    const prompts: Record<string, string> = {
      cold_call: `Generate a natural, persuasive cold call script for this merchant cash advance opportunity. Include:
- Opening hook (reference something specific about their business)
- Discovery questions (3-4 targeted questions)
- Value proposition tailored to their situation
- Objection handling (2-3 common objections with responses)
- Close/next steps

Make it conversational, not robotic. Include [PAUSE] markers for natural breaks. Use the merchant's actual name and business details.`,

      follow_up: `Generate a follow-up call script. This is a warm call — they've been contacted before. Include:
- Re-engagement opener referencing previous contact
- Status check questions
- Updated value proposition
- Urgency elements
- Next steps

Reference their call history and current status.`,

      email_intro: `Generate a professional introductory email for this merchant. Include:
- Subject line (compelling, not spammy)
- Opening that references their business specifically
- Brief value proposition (2-3 sentences)
- Social proof / credibility elements
- Clear call-to-action
- Professional signature placeholder

Keep it under 150 words. Make it feel personal, not templated.`,

      email_follow_up: `Generate a follow-up email for this merchant. Include:
- Subject line referencing previous contact
- Brief re-engagement opener
- Additional value point or case study reference
- Sense of urgency without being pushy
- Easy response mechanism (yes/no question)

Keep it under 100 words.`,

      sms: `Generate a brief, professional SMS message for this merchant. Include:
- Must be under 160 characters
- Reference their business name
- Clear value prop
- Simple CTA (reply YES or call number)

Generate 3 variations.`,

      objection_playbook: `Generate an objection handling playbook for this specific merchant. Based on their profile, anticipate the top 5 most likely objections and provide:
- The likely objection
- Why they might say this (based on their data)
- Response strategy
- Reframe/redirect technique
- Follow-up question after handling

Make responses natural and conversational.`,
    };

    const script = await callClaude(
      `You are Bridge Capital AI — an expert MCA sales coach. You generate highly personalized, effective sales scripts that close deals. Use the merchant's specific data to make every script feel custom-written. Format with markdown.`,
      `${prompts[scriptType] || prompts.cold_call}\n\nMERCHANT DATA:\n${JSON.stringify(leadData, null, 2)}`
    );

    res.json({ script: script || "Unable to generate script.", type: scriptType, leadId, merchantName: leadData.businessName });
  } catch (e: any) {
    console.error("AI script gen error:", e);
    res.status(500).json({ error: "Failed to generate script" });
  }
});

router.post("/ai/outreach-suggestion/:id", requireAuth, async (req, res): Promise<void> => {
  try {
    const leadId = parseInt(req.params.id);
    if (isNaN(leadId)) { res.status(400).json({ error: "Invalid lead ID" }); return; }

    const user = (req as any).user;
    const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!lead) { res.status(404).json({ error: "Lead not found" }); return; }
    if (user.role === "rep" && lead.assignedToId !== user.id) {
      res.status(403).json({ error: "Not authorized" }); return;
    }

    const calls = await db.select().from(callsTable)
      .where(eq(callsTable.leadId, leadId))
      .orderBy(desc(callsTable.createdAt))
      .limit(20);

    const messages = await db.select().from(leadMessagesTable)
      .where(eq(leadMessagesTable.leadId, leadId))
      .orderBy(desc(leadMessagesTable.createdAt))
      .limit(20);

    const deals = await db.select().from(dealsTable)
      .where(eq(dealsTable.leadId, leadId));

    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const callsToday = calls.filter(c => new Date(c.createdAt) >= today);
    const noAnswerCount = calls.filter(c => c.outcome === "no_answer").length;
    const noAnswerToday = callsToday.filter(c => c.outcome === "no_answer").length;
    const lastCallOutcome = calls[0]?.outcome || "none";
    const lastCallTime = calls[0]?.createdAt || null;
    const lastMessage = messages[0];
    const inboundMessages = messages.filter(m => m.direction === "inbound");
    const outboundMessages = messages.filter(m => m.direction === "outbound");
    const lastInbound = inboundMessages[0];

    const bankStmtMonths = lead.bankStatementMonths as any;
    const missingStatements = bankStmtMonths?.missing || [];

    const context = {
      lead: {
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
        yearsInBusiness: lead.yearsInBusiness,
        bankStatementsStatus: lead.bankStatementsStatus,
        missingBankStatements: missingStatements,
        estimatedApproval: lead.estimatedApproval,
        approvalConfidence: lead.approvalConfidence,
        source: lead.source,
        notes: lead.notes,
        riskCategory: lead.riskCategory,
      },
      activity: {
        totalCalls: calls.length,
        callsToday: callsToday.length,
        noAnswerCount,
        noAnswerToday,
        lastCallOutcome,
        lastCallTime: lastCallTime ? lastCallTime.toISOString() : null,
        totalMessages: messages.length,
        inboundMessageCount: inboundMessages.length,
        outboundMessageCount: outboundMessages.length,
        lastInboundMessage: lastInbound ? {
          content: lastInbound.content,
          time: lastInbound.createdAt.toISOString(),
          isHotTrigger: lastInbound.isHotTrigger,
        } : null,
        lastOutboundMessage: outboundMessages[0] ? {
          content: outboundMessages[0].content,
          time: outboundMessages[0].createdAt.toISOString(),
        } : null,
        callOutcomes: calls.slice(0, 10).map(c => ({ outcome: c.outcome, notes: c.notes, time: c.createdAt.toISOString() })),
        recentMessages: messages.slice(0, 10).map(m => ({
          direction: m.direction,
          content: m.content,
          time: m.createdAt.toISOString(),
        })),
      },
      deals: deals.map(d => ({ stage: d.stage, amount: d.amount })),
      currentTime: new Date().toISOString(),
      dayOfWeek: ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"][new Date().getDay()],
      currentHour: new Date().getHours(),
    };

    const system = `You are Bridge Capital's AI outreach strategist for a $1 billion merchant cash advance operation. You analyze lead data, call history, message history, and application details to recommend the perfect next action.

Your job is to tell the sales rep EXACTLY what to do next — who to call, when to text, what to say — with surgical precision. You're aggressive but smart. Every lead is money. Applications are HOT — they filled out the form, they want funding.

Rules:
- If they filled out an application and no one answered after 2 calls today, TEXT THEM IMMEDIATELY. They're interested.
- If they replied to a message, they're engaged — respond RIGHT NOW.
- If bank statements are missing, tell the rep which months and craft a message asking for them specifically.
- If the lead is hot (isHot=true), treat with MAXIMUM URGENCY.
- Always reference specific details from their application/conversation to make messages personal.
- Never be generic. Use the business name, owner name, specific amounts.
- Consider time of day — don't suggest calling at 10pm.
- If it's a weekend, prefer text over calls.
- Track conversation flow — don't repeat what was already said.

Return JSON with this exact structure:
{
  "urgency": "critical" | "high" | "medium" | "low",
  "recommendedAction": "call" | "text" | "email" | "wait",
  "reasoning": "Clear explanation of why this action, referencing specific data points",
  "suggestedMessage": "The exact message to send (if text/email)",
  "suggestedScript": "Brief call script if recommending a call",
  "timing": "Send now" | "Wait X minutes" | "Schedule for tomorrow morning" | etc,
  "followUpPlan": [
    { "action": "text|call|email", "timing": "in X hours/days", "message": "suggested content" }
  ],
  "missingInfo": ["List of missing data that would help close this deal"],
  "closeability": 1-100,
  "temperatureLabel": "On Fire" | "Hot" | "Warm" | "Cool" | "Cold" | "Dead"
}`;

    const result = await callClaude(system, JSON.stringify(context), { jsonMode: true });

    let parsed;
    try {
      parsed = JSON.parse(result);
    } catch {
      parsed = {
        urgency: "medium",
        recommendedAction: "call",
        reasoning: "Unable to analyze — try calling the lead directly.",
        suggestedMessage: "",
        suggestedScript: "",
        timing: "Send now",
        followUpPlan: [],
        missingInfo: [],
        closeability: 50,
        temperatureLabel: "Warm",
      };
    }

    res.json(parsed);
  } catch (e: any) {
    console.error("AI outreach suggestion error:", e);
    res.status(500).json({ error: "Failed to generate outreach suggestion" });
  }
});

export default router;
