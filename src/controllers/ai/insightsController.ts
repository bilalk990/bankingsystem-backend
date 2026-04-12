import { Router, type IRouter } from "express";
import { eq, desc, sql } from "drizzle-orm";
import { db, leadsTable, dealsTable } from "../../configs/database";
import { requireAuth, checkLeadOwnership } from "../../middlewares/authMiddleware";
import { callClaude, getBusinessSnapshot, getLeadContext } from "./helpersController";

const router: IRouter = Router();

router.post("/ai/insights", requireAuth, async (req, res) => {
  try {
    const snapshot = await getBusinessSnapshot();

    const raw = await callClaude(
      `You are Bridge Capital AI. Generate 4-5 concise, actionable business insights based on the data provided. Each insight should have:
- A short title (3-5 words)
- A brief description (1-2 sentences)
- A type: "opportunity" (green), "warning" (amber), "trend" (blue), or "action" (gold)
- A priority: "high", "medium", or "low"

Return as JSON: {"insights": [{"title": "...", "description": "...", "type": "opportunity|warning|trend|action", "priority": "high|medium|low"}]}

Be specific with numbers. Reference actual data. Identify patterns, risks, and opportunities. Think like a VP of Sales reviewing the daily brief.`,
      `Generate business insights from this data:\n${JSON.stringify(snapshot, null, 2)}\n\nToday: ${new Date().toLocaleDateString()}`,
      { jsonMode: true }
    );

    let insights;
    try {
      const parsed = JSON.parse(raw);
      insights = parsed.insights || parsed;
      if (!Array.isArray(insights)) insights = [insights];
    } catch {
      insights = [{ title: "Analysis Ready", description: "Your business data has been analyzed. Ask the AI assistant for specific insights.", type: "trend", priority: "medium" }];
    }

    res.json({ insights });
  } catch (e: any) {
    console.error("AI insights error:", e);
    res.status(500).json({ error: "Failed to generate insights" });
  }
});

router.post("/ai/lead-summary/:id", requireAuth, async (req, res) => {
  try {
    const leadId = parseInt(req.params.id);
    if (isNaN(leadId)) return res.status(400).json({ error: "Invalid lead ID" });
    const user = (req as any).user;
    if (!(await checkLeadOwnership(leadId, user, res))) return;
    const leadData = await getLeadContext(leadId);
    if (!leadData) return res.status(404).json({ error: "Lead not found" });

    const raw = await callClaude(
      `You are Bridge Capital AI. Generate a concise merchant intelligence summary. Return JSON with:
{
  "summary": "2-3 sentence executive summary of this merchant",
  "strengthScore": 0-100,
  "keyStrengths": ["strength 1", "strength 2"],
  "keyRisks": ["risk 1", "risk 2"],
  "recommendedActions": [{"action": "what to do", "priority": "high|medium|low", "reason": "why"}],
  "suggestedAmount": number or null,
  "suggestedFactorRate": number or null,
  "dealProbability": 0-100,
  "nextBestAction": "single most important next step",
  "talkingPoints": ["point for the call 1", "point 2", "point 3"]
}

Be specific, use the actual data. If data is missing, note it as a risk.`,
      `Generate intelligence summary:\n${JSON.stringify(leadData, null, 2)}`,
      { jsonMode: true }
    );

    let summary;
    try {
      summary = JSON.parse(raw);
    } catch {
      summary = { summary: "Unable to generate summary. Please try again.", strengthScore: 0, keyStrengths: [], keyRisks: ["Analysis failed"], recommendedActions: [], nextBestAction: "Review lead data manually" };
    }

    res.json(summary);
  } catch (e: any) {
    console.error("AI lead summary error:", e);
    res.status(500).json({ error: "Failed to generate lead summary" });
  }
});

router.post("/ai/pipeline-intel", requireAuth, async (req, res) => {
  try {
    const snapshot = await getBusinessSnapshot();

    const activeDeals = await db.select({
      id: dealsTable.id,
      leadId: dealsTable.leadId,
      stage: dealsTable.stage,
      amount: dealsTable.amount,
      factorRate: dealsTable.factorRate,
      funderId: dealsTable.funderId,
      createdAt: dealsTable.createdAt,
      businessName: leadsTable.businessName,
      riskCategory: leadsTable.riskCategory,
      creditScore: leadsTable.creditScore,
      monthlyRevenue: leadsTable.monthlyRevenue,
    })
      .from(dealsTable)
      .leftJoin(leadsTable, eq(dealsTable.leadId, leadsTable.id))
      .where(sql`${dealsTable.stage} NOT IN ('funded', 'declined')`)
      .orderBy(desc(dealsTable.amount))
      .limit(20);

    const raw = await callClaude(
      `You are Bridge Capital AI. Analyze the pipeline and return JSON:
{
  "pipelineHealth": "strong|moderate|weak",
  "healthScore": 0-100,
  "totalPipelineValue": number,
  "expectedCloseValue": number,
  "predictions": [{"dealId": number, "businessName": "...", "probability": 0-100, "suggestedAction": "...", "risk": "low|medium|high"}],
  "bottlenecks": ["issue 1", "issue 2"],
  "recommendations": [{"action": "...", "impact": "high|medium|low", "reason": "..."}],
  "weeklyForecast": "1-2 sentence forecast for next 7 days"
}`,
      `Analyze pipeline:\nSnapshot: ${JSON.stringify(snapshot, null, 2)}\nActive Deals: ${JSON.stringify(activeDeals, null, 2)}`,
      { jsonMode: true }
    );

    let intel;
    try {
      intel = JSON.parse(raw);
    } catch {
      intel = { pipelineHealth: "unknown", healthScore: 0, predictions: [], recommendations: [] };
    }

    res.json(intel);
  } catch (e: any) {
    console.error("AI pipeline intel error:", e);
    res.status(500).json({ error: "Failed to generate pipeline intelligence" });
  }
});

router.post("/ai/auto-status/:id", requireAuth, async (req, res) => {
  try {
    const leadId = parseInt(req.params.id);
    if (isNaN(leadId)) return res.status(400).json({ error: "Invalid lead ID" });
    const user = (req as any).user;
    if (!(await checkLeadOwnership(leadId, user, res))) return;
    const leadData = await getLeadContext(leadId);
    if (!leadData) return res.status(404).json({ error: "Lead not found" });

    const raw = await callClaude(
      `You are Bridge Capital AI. Based on the lead's data, call history, deals, and documents, suggest the optimal status and next action. Return JSON:
{
  "currentStatus": "their current status",
  "suggestedStatus": "recommended status",
  "confidence": 0-100,
  "reasoning": "why this status change",
  "suggestedNextAction": "what to do next",
  "suggestedFollowUpDate": "YYYY-MM-DD or null",
  "automations": [{"action": "create_deal|send_email|schedule_call|request_docs", "details": "..."}]
}

Valid statuses: new, contacted, no_answer, callback, qualified, not_interested, funded`,
      `Analyze lead and suggest status:\n${JSON.stringify(leadData, null, 2)}`,
      { jsonMode: true }
    );

    let suggestion;
    try {
      suggestion = JSON.parse(raw);
    } catch {
      suggestion = { currentStatus: leadData.status, suggestedStatus: leadData.status, confidence: 0, reasoning: "Unable to analyze" };
    }

    res.json(suggestion);
  } catch (e: any) {
    console.error("AI auto-status error:", e);
    res.status(500).json({ error: "Failed to generate status suggestion" });
  }
});

router.post("/ai/estimate-approval/:id", requireAuth, async (req, res) => {
  try {
    const leadId = parseInt(req.params.id);
    if (isNaN(leadId)) return res.status(400).json({ error: "Invalid lead ID" });

    const user = (req as any).user;
    const [leadCheck] = await db.select({ assignedToId: leadsTable.assignedToId }).from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!leadCheck) return res.status(404).json({ error: "Lead not found" });
    if (user.role === "rep" && leadCheck.assignedToId !== user.id) return res.status(403).json({ error: "Not authorized" });

    const leadData = await getLeadContext(leadId);
    if (!leadData) return res.status(404).json({ error: "Lead not found" });

    const fundedDeals = await db.select({
      amount: dealsTable.amount,
      factorRate: dealsTable.factorRate,
      funderId: dealsTable.funderId,
      leadId: dealsTable.leadId,
      businessName: leadsTable.businessName,
      riskCategory: leadsTable.riskCategory,
      creditScore: leadsTable.creditScore,
      monthlyRevenue: leadsTable.monthlyRevenue,
      avgDailyBalance: leadsTable.avgDailyBalance,
      hasExistingLoans: leadsTable.hasExistingLoans,
      loanCount: leadsTable.loanCount,
      industry: leadsTable.industry,
    }).from(dealsTable)
      .leftJoin(leadsTable, eq(dealsTable.leadId, leadsTable.id))
      .where(eq(dealsTable.stage, "funded"))
      .orderBy(desc(dealsTable.createdAt))
      .limit(100);

    const raw = await callClaude(
      `You are Bridge Capital's AI underwriting engine. Based on historical funded deals and this merchant's profile, estimate an approval amount.

RULES:
- If you have fewer than 5 funded deals for reference, set confidence to "low" and note that more data is needed
- If you have 5-20 funded deals, set confidence to "medium"
- If you have 20+ funded deals, set confidence based on how closely this merchant matches patterns
- NEVER overestimate. Be conservative. It's better to underpromise than overpromise
- Factor in: monthly revenue, credit score, risk category, existing loans, avg daily balance, industry, years in business
- The estimated amount should be what you think this merchant would ACTUALLY get approved for based on the patterns you see
- If data is missing (no credit score, no revenue, etc.), lower the confidence and note what's needed

Return JSON:
{
  "estimatedApproval": number,
  "confidence": "low" | "medium" | "high",
  "confidenceScore": 0-100,
  "reasoning": "2-3 sentence explanation of how you arrived at this number",
  "keyFactors": [{"factor": "what influenced the estimate", "impact": "positive|negative|neutral"}],
  "comparableDeals": [{"businessName": "...", "amount": number, "similarity": "why similar"}],
  "suggestedFactorRate": number or null,
  "suggestedTerm": "X months" or null,
  "missingData": ["list of data that would improve accuracy"],
  "adminReviewNeeded": true/false,
  "adminQuestion": "specific question for admin if review needed" or null
}`,
      `MERCHANT TO EVALUATE:\n${JSON.stringify(leadData, null, 2)}\n\nHISTORICAL FUNDED DEALS (${fundedDeals.length} total):\n${JSON.stringify(fundedDeals, null, 2)}`,
      { jsonMode: true }
    );

    let estimate;
    try {
      estimate = JSON.parse(raw);
    } catch {
      estimate = {
        estimatedApproval: 0,
        confidence: "low",
        confidenceScore: 0,
        reasoning: "Unable to generate estimate. Insufficient data.",
        keyFactors: [],
        missingData: ["Analysis failed"],
        adminReviewNeeded: true,
      };
    }

    if (estimate.estimatedApproval > 0) {
      await db.update(leadsTable).set({
        estimatedApproval: estimate.estimatedApproval,
        approvalConfidence: estimate.confidenceScore,
      }).where(eq(leadsTable.id, leadId));
    }

    res.json(estimate);
  } catch (e: any) {
    console.error("AI approval estimation error:", e);
    res.status(500).json({ error: "Failed to estimate approval" });
  }
});

export default router;
