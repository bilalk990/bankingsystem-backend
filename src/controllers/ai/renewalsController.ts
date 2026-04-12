import { Router, type IRouter } from "express";
import { eq, and, desc } from "drizzle-orm";
import {
  db,
  leadsTable,
  dealsTable,
  usersTable,
  activitiesTable,
  renewalSuggestionsTable,
} from "../../configs/database";
import { requireAuth } from "../../middlewares/authMiddleware";
import { callClaude } from "./helpersController";

const router: IRouter = Router();

router.post("/ai/renewal-intel", requireAuth, async (req, res): Promise<void> => {
  try {
    const user = (req as any).user;
    const isAdminUser = user.role === "admin" || user.role === "super_admin";

    const repCondition = isAdminUser ? undefined : eq(dealsTable.repId, user.id);

    const allFundedDeals = await db.select({
      id: dealsTable.id,
      leadId: dealsTable.leadId,
      repId: dealsTable.repId,
      repName: usersTable.fullName,
      stage: dealsTable.stage,
      amount: dealsTable.amount,
      factorRate: dealsTable.factorRate,
      paybackAmount: dealsTable.paybackAmount,
      term: dealsTable.term,
      fundedDate: dealsTable.fundedDate,
      renewalEligibleDate: dealsTable.renewalEligibleDate,
      paymentsCompleted: dealsTable.paymentsCompleted,
      totalPayments: dealsTable.totalPayments,
      paymentFrequency: dealsTable.paymentFrequency,
      commission: dealsTable.commission,
      businessName: leadsTable.businessName,
      ownerName: leadsTable.ownerName,
      phone: leadsTable.phone,
      email: leadsTable.email,
      industry: leadsTable.industry,
      businessType: leadsTable.businessType,
      monthlyRevenue: leadsTable.monthlyRevenue,
      grossRevenue: leadsTable.grossRevenue,
      yearsInBusiness: leadsTable.yearsInBusiness,
      riskCategory: leadsTable.riskCategory,
      avgDailyBalance: leadsTable.avgDailyBalance,
      revenueTrend: leadsTable.revenueTrend,
      loanCount: leadsTable.loanCount,
      hasExistingLoans: leadsTable.hasExistingLoans,
      status: leadsTable.status,
      state: leadsTable.state,
      createdAt: dealsTable.createdAt,
    })
      .from(dealsTable)
      .leftJoin(leadsTable, eq(dealsTable.leadId, leadsTable.id))
      .leftJoin(usersTable, eq(dealsTable.repId, usersTable.id))
      .where(repCondition ? and(eq(dealsTable.stage, "funded"), repCondition) : eq(dealsTable.stage, "funded"))
      .orderBy(dealsTable.fundedDate);

    if (allFundedDeals.length === 0) {
      res.json({ suggestions: [], patterns: [], stats: { totalFunded: 0, repeatFunders: 0, avgCycleDays: 0 } });
      return;
    }

    const existingSuggestions = await db.select({
      leadId: renewalSuggestionsTable.leadId,
      dealId: renewalSuggestionsTable.dealId,
      status: renewalSuggestionsTable.status,
    })
      .from(renewalSuggestionsTable)
      .where(repCondition ? eq(renewalSuggestionsTable.repId, user.id) : undefined);

    const activeSuggestionKeys = new Set(
      existingSuggestions
        .filter(s => s.status === "pending" || s.status === "sent")
        .map(s => `${s.leadId}-${s.dealId}`)
    );

    const byLead = new Map<number, any[]>();
    for (const d of allFundedDeals) {
      if (!byLead.has(d.leadId)) byLead.set(d.leadId, []);
      byLead.get(d.leadId)!.push(d);
    }

    const repeatFunders: any[] = [];
    const singleFunders: any[] = [];
    const fundingCycles: { industry: string; businessType: string; cycleDays: number; amount: number; monthlyRevenue: number; }[] = [];

    for (const [leadId, deals] of byLead) {
      if (deals.length > 1) {
        repeatFunders.push({ leadId, deals, count: deals.length });
        for (let i = 1; i < deals.length; i++) {
          const prev = deals[i - 1];
          const curr = deals[i];
          if (prev.fundedDate && curr.fundedDate) {
            const cycleDays = Math.round(
              (new Date(curr.fundedDate).getTime() - new Date(prev.fundedDate).getTime()) / (1000 * 60 * 60 * 24)
            );
            if (cycleDays > 0 && cycleDays < 365) {
              fundingCycles.push({
                industry: curr.industry || "unknown",
                businessType: curr.businessType || "unknown",
                cycleDays,
                amount: curr.amount,
                monthlyRevenue: curr.monthlyRevenue || 0,
              });
            }
          }
        }
      } else {
        singleFunders.push({ leadId, deal: deals[0] });
      }
    }

    const industryPatterns: Record<string, { avgCycleDays: number; count: number; avgAmount: number }> = {};
    for (const cycle of fundingCycles) {
      const key = cycle.industry.toLowerCase();
      if (!industryPatterns[key]) industryPatterns[key] = { avgCycleDays: 0, count: 0, avgAmount: 0 };
      industryPatterns[key].count++;
      industryPatterns[key].avgCycleDays += cycle.cycleDays;
      industryPatterns[key].avgAmount += cycle.amount;
    }
    for (const key of Object.keys(industryPatterns)) {
      industryPatterns[key].avgCycleDays = Math.round(industryPatterns[key].avgCycleDays / industryPatterns[key].count);
      industryPatterns[key].avgAmount = Math.round(industryPatterns[key].avgAmount / industryPatterns[key].count);
    }

    const overallAvgCycle = fundingCycles.length > 0
      ? Math.round(fundingCycles.reduce((s, c) => s + c.cycleDays, 0) / fundingCycles.length)
      : 45;

    const candidates: any[] = [];

    for (const { leadId, deal } of singleFunders) {
      if (!deal.fundedDate) continue;
      const daysSinceFunded = Math.round(
        (Date.now() - new Date(deal.fundedDate).getTime()) / (1000 * 60 * 60 * 24)
      );
      const ind = (deal.industry || "").toLowerCase();
      const pattern = industryPatterns[ind];
      const cycleDays = pattern ? pattern.avgCycleDays : overallAvgCycle;
      const daysUntilRenewal = cycleDays - daysSinceFunded;

      if (daysUntilRenewal <= 14 && daysSinceFunded >= 20) {
        const alreadySuggested = activeSuggestionKeys.has(`${leadId}-${deal.id}`);
        if (!alreadySuggested) {
          candidates.push({
            ...deal,
            daysSinceFunded,
            cycleDays,
            daysUntilRenewal,
            patternSource: pattern ? `${pattern.count} repeat funders in ${deal.industry}` : "overall portfolio avg",
            estimatedAmount: pattern ? pattern.avgAmount : deal.amount,
          });
        }
      }
    }

    for (const { leadId, deals } of repeatFunders) {
      const latest = deals[deals.length - 1];
      if (!latest.fundedDate) continue;
      const daysSinceFunded = Math.round(
        (Date.now() - new Date(latest.fundedDate).getTime()) / (1000 * 60 * 60 * 24)
      );
      const dealCycles: number[] = [];
      for (let i = 1; i < deals.length; i++) {
        if (deals[i].fundedDate && deals[i - 1].fundedDate) {
          dealCycles.push(Math.round(
            (new Date(deals[i].fundedDate).getTime() - new Date(deals[i - 1].fundedDate).getTime()) / (1000 * 60 * 60 * 24)
          ));
        }
      }
      const avgCycle = dealCycles.length > 0
        ? Math.round(dealCycles.reduce((s, c) => s + c, 0) / dealCycles.length)
        : overallAvgCycle;
      const daysUntilRenewal = avgCycle - daysSinceFunded;

      if (daysUntilRenewal <= 14 && daysSinceFunded >= 20) {
        const alreadySuggested = activeSuggestionKeys.has(`${leadId}-${latest.id}`);
        if (!alreadySuggested) {
          candidates.push({
            ...latest,
            daysSinceFunded,
            cycleDays: avgCycle,
            daysUntilRenewal,
            isRepeatFunder: true,
            timesFunded: deals.length,
            patternSource: `This business has funded ${deals.length} times, avg every ${avgCycle} days`,
            estimatedAmount: Math.round(deals.reduce((s: number, d: any) => s + d.amount, 0) / deals.length),
          });
        }
      }
    }

    candidates.sort((a, b) => a.daysUntilRenewal - b.daysUntilRenewal);
    const topCandidates = candidates.slice(0, 20);

    if (topCandidates.length === 0) {
      const patterns = Object.entries(industryPatterns).map(([industry, data]) => ({
        industry,
        avgCycleDays: data.avgCycleDays,
        repeatCount: data.count,
        avgAmount: data.avgAmount,
      }));

      res.json({
        suggestions: [],
        patterns,
        stats: {
          totalFunded: allFundedDeals.length,
          repeatFunders: repeatFunders.length,
          singleFunders: singleFunders.length,
          avgCycleDays: overallAvgCycle,
          totalCyclesAnalyzed: fundingCycles.length,
        },
      });
      return;
    }

    const prompt = `You are an AI sales advisor for Bridge Capital, a cash advance / merchant cash advance company.

Analyze these funded deals and generate renewal outreach suggestions. These businesses were previously funded and based on industry patterns and their own funding history, they are likely due for another advance.

FUNDING PATTERN DATA:
${JSON.stringify({
  industryPatterns,
  overallAvgCycleDays: overallAvgCycle,
  totalRepeatFunders: repeatFunders.length,
  totalCyclesAnalyzed: fundingCycles.length,
}, null, 2)}

CANDIDATES FOR RENEWAL OUTREACH:
${JSON.stringify(topCandidates.map(c => ({
  dealId: c.id,
  leadId: c.leadId,
  businessName: c.businessName,
  ownerName: c.ownerName,
  industry: c.industry,
  monthlyRevenue: c.monthlyRevenue,
  lastFundedAmount: c.amount,
  daysSinceFunded: c.daysSinceFunded,
  predictedCycleDays: c.cycleDays,
  daysUntilRenewal: c.daysUntilRenewal,
  isRepeatFunder: c.isRepeatFunder || false,
  timesFunded: c.timesFunded || 1,
  patternSource: c.patternSource,
  estimatedAmount: c.estimatedAmount,
  revenueTrend: c.revenueTrend,
  riskCategory: c.riskCategory,
})), null, 2)}

For each candidate, generate:
1. A personalized, natural SMS message (not salesy, warm and helpful — reference their business by name, acknowledge they used funding before, ask if they could use more capital right now)
2. A personalized email subject + body (professional but friendly)
3. A confidence score (1-100) on how likely they are to need renewal
4. The best time of day to reach out (morning/afternoon/evening)
5. Specific reason why NOW is the right time
6. Suggested next funding amount based on patterns

Return JSON:
{
  "suggestions": [
    {
      "dealId": number,
      "leadId": number,
      "confidence": number,
      "urgency": "hot" | "warm" | "upcoming",
      "reason": "string explaining why now",
      "smsMessage": "string",
      "emailSubject": "string",
      "emailBody": "string",
      "bestTimeOfDay": "morning" | "afternoon" | "evening",
      "suggestedAmount": number,
      "talkingPoints": ["point1", "point2"]
    }
  ]
}`;

    const aiResult = await callClaude(
      "You are a renewal intelligence AI for a merchant cash advance company. Generate personalized renewal outreach suggestions.",
      prompt,
      { maxTokens: 8192, jsonMode: true }
    );

    let parsed: any;
    try {
      parsed = JSON.parse(aiResult);
    } catch {
      const jsonMatch = aiResult.match(/\{[\s\S]*\}/);
      parsed = jsonMatch ? JSON.parse(jsonMatch[0]) : { suggestions: [] };
    }

    const enrichedSuggestions = (parsed.suggestions || []).map((s: any) => {
      const candidate = topCandidates.find(c => c.id === s.dealId || c.leadId === s.leadId);
      if (!candidate) return null;
      return {
        ...s,
        businessName: candidate.businessName,
        ownerName: candidate.ownerName,
        phone: candidate.phone,
        email: candidate.email,
        industry: candidate.industry,
        lastFundedAmount: candidate.amount,
        daysSinceFunded: candidate.daysSinceFunded,
        predictedCycleDays: candidate.cycleDays,
        daysUntilRenewal: candidate.daysUntilRenewal,
        isRepeatFunder: candidate.isRepeatFunder || false,
        timesFunded: candidate.timesFunded || 1,
        patternSource: candidate.patternSource,
        repName: candidate.repName,
        repId: candidate.repId,
        revenueTrend: candidate.revenueTrend,
        riskCategory: candidate.riskCategory,
        monthlyRevenue: candidate.monthlyRevenue,
      };
    }).filter(Boolean);

    const patterns = Object.entries(industryPatterns).map(([industry, data]) => ({
      industry,
      avgCycleDays: data.avgCycleDays,
      repeatCount: data.count,
      avgAmount: data.avgAmount,
    }));

    res.json({
      suggestions: enrichedSuggestions,
      patterns,
      stats: {
        totalFunded: allFundedDeals.length,
        repeatFunders: repeatFunders.length,
        singleFunders: singleFunders.length,
        avgCycleDays: overallAvgCycle,
        totalCyclesAnalyzed: fundingCycles.length,
        candidatesFound: candidates.length,
      },
    });
  } catch (e: any) {
    console.error("Renewal intel error:", e);
    res.status(500).json({ error: "Failed to generate renewal intelligence" });
  }
});

router.post("/ai/renewal-intel/act", requireAuth, async (req, res): Promise<void> => {
  try {
    const user = (req as any).user;
    const { leadId, dealId, action, suggestedMessage, suggestedChannel, reason, confidence, patternSource, estimatedAmount } = req.body;

    if (!leadId || !dealId || !action) {
      res.status(400).json({ error: "leadId, dealId, and action are required" });
      return;
    }

    const lead = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId)).limit(1);
    if (!lead.length) {
      res.status(404).json({ error: "Lead not found" });
      return;
    }
    if (user.role === "rep" && lead[0].assignedToId !== user.id) {
      res.status(403).json({ error: "Not authorized for this lead" });
      return;
    }

    if (action === "dismiss") {
      const [suggestion] = await db.insert(renewalSuggestionsTable).values({
        leadId,
        dealId,
        repId: user.id,
        status: "dismissed",
        reason,
        suggestedMessage,
        suggestedChannel: suggestedChannel || "sms",
        confidence,
        patternSource,
        estimatedAmount,
        dismissedAt: new Date(),
      }).returning();
      res.json(suggestion);
      return;
    }

    if (action === "send") {
      const [suggestion] = await db.insert(renewalSuggestionsTable).values({
        leadId,
        dealId,
        repId: user.id,
        status: "sent",
        reason,
        suggestedMessage,
        suggestedChannel: suggestedChannel || "sms",
        confidence,
        patternSource,
        estimatedAmount,
        actedAt: new Date(),
      }).returning();

      const { leadMessagesTable: msgTable } = await import("../../configs/database");
      await db.insert(msgTable).values({
        leadId,
        userId: user.id,
        source: "renewal_intel",
        direction: "outbound",
        messageType: suggestedChannel === "email" ? "email" : "sms",
        content: suggestedMessage,
        aiGenerated: true,
      });

      await db.insert(activitiesTable).values({
        leadId,
        userId: user.id,
        type: "renewal_outreach",
        description: `AI-suggested renewal ${suggestedChannel || "sms"} sent`,
        metadata: { dealId, confidence, patternSource },
      });

      res.json(suggestion);
      return;
    }

    if (action === "schedule") {
      const reachOutDate = req.body.reachOutDate ? new Date(req.body.reachOutDate) : new Date(Date.now() + 7 * 24 * 60 * 60 * 1000);
      const [suggestion] = await db.insert(renewalSuggestionsTable).values({
        leadId,
        dealId,
        repId: user.id,
        status: "pending",
        suggestedReachOutDate: reachOutDate,
        reason,
        suggestedMessage,
        suggestedChannel: suggestedChannel || "sms",
        confidence,
        patternSource,
        estimatedAmount,
      }).returning();
      res.json(suggestion);
      return;
    }

    res.status(400).json({ error: "Invalid action. Use: send, dismiss, or schedule" });
  } catch (e: any) {
    console.error("Renewal act error:", e);
    res.status(500).json({ error: "Failed to process renewal action" });
  }
});

router.get("/ai/renewal-intel/history", requireAuth, async (req, res): Promise<void> => {
  try {
    const user = (req as any).user;
    const isAdminUser = user.role === "admin" || user.role === "super_admin";

    const conditions: any[] = [];
    if (!isAdminUser) {
      conditions.push(eq(renewalSuggestionsTable.repId, user.id));
    }

    const suggestions = await db.select({
      id: renewalSuggestionsTable.id,
      leadId: renewalSuggestionsTable.leadId,
      dealId: renewalSuggestionsTable.dealId,
      repId: renewalSuggestionsTable.repId,
      status: renewalSuggestionsTable.status,
      reason: renewalSuggestionsTable.reason,
      suggestedMessage: renewalSuggestionsTable.suggestedMessage,
      suggestedChannel: renewalSuggestionsTable.suggestedChannel,
      confidence: renewalSuggestionsTable.confidence,
      patternSource: renewalSuggestionsTable.patternSource,
      estimatedAmount: renewalSuggestionsTable.estimatedAmount,
      suggestedReachOutDate: renewalSuggestionsTable.suggestedReachOutDate,
      actedAt: renewalSuggestionsTable.actedAt,
      dismissedAt: renewalSuggestionsTable.dismissedAt,
      createdAt: renewalSuggestionsTable.createdAt,
      businessName: leadsTable.businessName,
      ownerName: leadsTable.ownerName,
    })
      .from(renewalSuggestionsTable)
      .leftJoin(leadsTable, eq(renewalSuggestionsTable.leadId, leadsTable.id))
      .where(conditions.length > 0 ? and(...conditions) : undefined)
      .orderBy(desc(renewalSuggestionsTable.createdAt))
      .limit(100);

    res.json(suggestions);
  } catch (e: any) {
    console.error("Renewal history error:", e);
    res.status(500).json({ error: "Failed to fetch renewal history" });
  }
});

export default router;
