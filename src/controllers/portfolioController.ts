import { Router, type IRouter } from "express";
import { eq, sql, and, count, desc, gte, lte } from "drizzle-orm";
import { db, leadsTable, dealsTable, commissionsTable, fundersTable, usersTable, bankStatementAnalysesTable, submissionsTable } from "../configs/database";
import { requireAuth, requireAdmin } from "../middlewares/authMiddleware";

const router: IRouter = Router();

router.get("/portfolio/health", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    const totalLeads = await db.select({ count: sql<number>`count(*)::int` }).from(leadsTable);
    const totalDeals = await db.select({ count: sql<number>`count(*)::int` }).from(dealsTable);
    const fundedDeals = await db.select({ count: sql<number>`count(*)::int` }).from(dealsTable).where(eq(dealsTable.stage, "funded"));

    const totalFunded = await db.select({
      total: sql<number>`COALESCE(SUM(amount), 0)`,
    }).from(dealsTable).where(eq(dealsTable.stage, "funded"));

    const totalPipeline = await db.select({
      total: sql<number>`COALESCE(SUM(amount), 0)`,
    }).from(dealsTable).where(sql`${dealsTable.stage} NOT IN ('funded', 'declined')`);

    const totalCommissions = await db.select({
      total: sql<number>`COALESCE(SUM(amount), 0)`,
      pending: sql<number>`COALESCE(SUM(CASE WHEN status = 'pending' THEN amount ELSE 0 END), 0)`,
      paid: sql<number>`COALESCE(SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END), 0)`,
    }).from(commissionsTable);

    const riskBreakdown = await db.select({
      riskCategory: leadsTable.riskCategory,
      count: sql<number>`count(*)::int`,
    }).from(leadsTable)
      .where(sql`${leadsTable.riskCategory} IS NOT NULL`)
      .groupBy(leadsTable.riskCategory);

    const industryBreakdown = await db.select({
      industry: leadsTable.industry,
      count: sql<number>`count(*)::int`,
      totalRevenue: sql<number>`COALESCE(SUM(${leadsTable.monthlyRevenue}), 0)`,
    }).from(leadsTable)
      .where(sql`${leadsTable.industry} IS NOT NULL`)
      .groupBy(leadsTable.industry)
      .orderBy(sql`count(*) DESC`)
      .limit(10);

    const stateBreakdown = await db.select({
      state: leadsTable.state,
      count: sql<number>`count(*)::int`,
    }).from(leadsTable)
      .where(sql`${leadsTable.state} IS NOT NULL`)
      .groupBy(leadsTable.state)
      .orderBy(sql`count(*) DESC`)
      .limit(15);

    const conversionRate = (totalLeads[0]?.count || 0) > 0
      ? Math.round(((fundedDeals[0]?.count || 0) / (totalLeads[0]?.count || 0)) * 100)
      : 0;

    const avgDealSize = (fundedDeals[0]?.count || 0) > 0
      ? Math.round(Number(totalFunded[0]?.total || 0) / (fundedDeals[0]?.count || 0))
      : 0;

    const stackedMerchants = await db.execute(sql`
      SELECT l.id, l.business_name, COUNT(d.id)::int as deal_count, SUM(d.amount) as total_exposure
      FROM leads l
      JOIN deals d ON d.lead_id = l.id AND d.stage IN ('funded', 'approved')
      GROUP BY l.id, l.business_name
      HAVING COUNT(d.id) > 1
      ORDER BY total_exposure DESC
      LIMIT 10
    `);

    const repLeaderboard = await db.execute(sql`
      SELECT u.id, u.full_name,
        COALESCE(d_agg.deals, 0)::int as deals,
        COALESCE(d_agg.funded_amount, 0) as funded_amount,
        COALESCE(c_agg.commission_earned, 0) as commission_earned,
        COALESCE(l_agg.leads_assigned, 0)::int as leads_assigned
      FROM users u
      LEFT JOIN (
        SELECT rep_id, COUNT(*)::int as deals,
          SUM(CASE WHEN stage = 'funded' THEN amount ELSE 0 END) as funded_amount
        FROM deals GROUP BY rep_id
      ) d_agg ON d_agg.rep_id = u.id
      LEFT JOIN (
        SELECT rep_id, SUM(amount) as commission_earned
        FROM commissions GROUP BY rep_id
      ) c_agg ON c_agg.rep_id = u.id
      LEFT JOIN (
        SELECT assigned_to_id, COUNT(*)::int as leads_assigned
        FROM leads GROUP BY assigned_to_id
      ) l_agg ON l_agg.assigned_to_id = u.id
      WHERE u.role = 'rep'
      ORDER BY funded_amount DESC
    `);

    const highValueLeads = await db.select({
      id: leadsTable.id,
      businessName: leadsTable.businessName,
      requestedAmount: leadsTable.requestedAmount,
      monthlyRevenue: leadsTable.monthlyRevenue,
      creditScore: leadsTable.creditScore,
      riskCategory: leadsTable.riskCategory,
      status: leadsTable.status,
    }).from(leadsTable)
      .where(sql`${leadsTable.requestedAmount} IS NOT NULL`)
      .orderBy(desc(leadsTable.requestedAmount))
      .limit(10);

    const funderUtilization = await db.execute(sql`
      SELECT f.id, f.name,
        COUNT(s.id)::int as total_submissions,
        COUNT(CASE WHEN s.status = 'approved' THEN 1 END)::int as approved,
        COUNT(CASE WHEN s.status = 'declined' THEN 1 END)::int as declined,
        COUNT(CASE WHEN s.status = 'pending' THEN 1 END)::int as pending,
        COALESCE(SUM(CASE WHEN s.status = 'approved' THEN s.approved_amount ELSE 0 END), 0) as total_approved_amount
      FROM funders f
      LEFT JOIN submissions s ON s.funder_id = f.id
      WHERE f.active = true
      GROUP BY f.id, f.name
      ORDER BY total_submissions DESC
    `);

    const revenueByTrend = await db.select({
      trend: leadsTable.revenueTrend,
      count: sql<number>`count(*)::int`,
      avgRevenue: sql<number>`ROUND(COALESCE(AVG(${leadsTable.monthlyRevenue}), 0))`,
    }).from(leadsTable)
      .where(sql`${leadsTable.revenueTrend} IS NOT NULL`)
      .groupBy(leadsTable.revenueTrend);

    res.json({
      overview: {
        totalLeads: totalLeads[0]?.count || 0,
        totalDeals: totalDeals[0]?.count || 0,
        fundedDeals: fundedDeals[0]?.count || 0,
        totalFunded: Number(totalFunded[0]?.total || 0),
        totalPipeline: Number(totalPipeline[0]?.total || 0),
        totalCommissions: Number(totalCommissions[0]?.total || 0),
        pendingCommissions: Number(totalCommissions[0]?.pending || 0),
        paidCommissions: Number(totalCommissions[0]?.paid || 0),
        conversionRate,
        avgDealSize,
      },
      riskBreakdown: riskBreakdown as any,
      industryBreakdown: industryBreakdown as any,
      stateBreakdown: stateBreakdown as any,
      stackedMerchants: (stackedMerchants as any).rows || stackedMerchants,
      repLeaderboard: (repLeaderboard as any).rows || repLeaderboard,
      highValueLeads: highValueLeads as any,
      funderUtilization: (funderUtilization as any).rows || funderUtilization,
      revenueByTrend: revenueByTrend as any,
    });
  } catch (e: any) {
    console.error("Portfolio health error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.get("/portfolio/lead-scores", requireAuth, async (req, res): Promise<void> => {
  try {
    const user = (req as any).user;

    const baseQuery = db.select({
      id: leadsTable.id,
      businessName: leadsTable.businessName,
      ownerName: leadsTable.ownerName,
      monthlyRevenue: leadsTable.monthlyRevenue,
      requestedAmount: leadsTable.requestedAmount,
      creditScore: leadsTable.creditScore,
      riskCategory: leadsTable.riskCategory,
      avgDailyBalance: leadsTable.avgDailyBalance,
      hasExistingLoans: leadsTable.hasExistingLoans,
      loanCount: leadsTable.loanCount,
      revenueTrend: leadsTable.revenueTrend,
      yearsInBusiness: leadsTable.yearsInBusiness,
      status: leadsTable.status,
      industry: leadsTable.industry,
      phone: leadsTable.phone,
      createdAt: leadsTable.createdAt,
    }).from(leadsTable);

    const leads = user.role === "rep"
      ? await baseQuery.where(eq(leadsTable.assignedToId, user.id)).orderBy(desc(leadsTable.createdAt)).limit(100)
      : await baseQuery.orderBy(desc(leadsTable.createdAt)).limit(100);

    const scoredLeads = leads.map(lead => {
      let score = 50;
      let factors: { factor: string; impact: number; positive: boolean }[] = [];

      if (lead.creditScore) {
        if (lead.creditScore >= 700) { score += 15; factors.push({ factor: "Excellent credit (700+)", impact: 15, positive: true }); }
        else if (lead.creditScore >= 600) { score += 8; factors.push({ factor: "Good credit (600+)", impact: 8, positive: true }); }
        else if (lead.creditScore >= 500) { score += 0; factors.push({ factor: "Fair credit (500+)", impact: 0, positive: false }); }
        else { score -= 10; factors.push({ factor: "Poor credit (<500)", impact: -10, positive: false }); }
      } else {
        factors.push({ factor: "No credit data", impact: -5, positive: false }); score -= 5;
      }

      if (lead.monthlyRevenue) {
        if (lead.monthlyRevenue >= 50000) { score += 12; factors.push({ factor: "High revenue ($50K+/mo)", impact: 12, positive: true }); }
        else if (lead.monthlyRevenue >= 20000) { score += 8; factors.push({ factor: "Good revenue ($20K+/mo)", impact: 8, positive: true }); }
        else if (lead.monthlyRevenue >= 10000) { score += 3; factors.push({ factor: "Moderate revenue ($10K+/mo)", impact: 3, positive: true }); }
        else { score -= 5; factors.push({ factor: "Low revenue (<$10K/mo)", impact: -5, positive: false }); }
      }

      if (lead.revenueTrend === "growing") { score += 8; factors.push({ factor: "Revenue growing", impact: 8, positive: true }); }
      else if (lead.revenueTrend === "declining") { score -= 10; factors.push({ factor: "Revenue declining", impact: -10, positive: false }); }

      if (lead.avgDailyBalance) {
        if (lead.avgDailyBalance >= 10000) { score += 10; factors.push({ factor: "Strong daily balance ($10K+)", impact: 10, positive: true }); }
        else if (lead.avgDailyBalance >= 3000) { score += 5; factors.push({ factor: "OK daily balance ($3K+)", impact: 5, positive: true }); }
        else { score -= 5; factors.push({ factor: "Low daily balance", impact: -5, positive: false }); }
      }

      if (lead.hasExistingLoans) {
        if ((lead.loanCount || 0) >= 3) { score -= 15; factors.push({ factor: `${lead.loanCount} existing loans (heavy stacking)`, impact: -15, positive: false }); }
        else if ((lead.loanCount || 0) >= 2) { score -= 8; factors.push({ factor: `${lead.loanCount} existing loans`, impact: -8, positive: false }); }
        else { score -= 3; factors.push({ factor: "1 existing loan", impact: -3, positive: false }); }
      } else if (lead.hasExistingLoans === false) {
        score += 5; factors.push({ factor: "No existing loans", impact: 5, positive: true });
      }

      if (lead.riskCategory) {
        if (lead.riskCategory === "A1") { score += 10; factors.push({ factor: "A1 risk rating", impact: 10, positive: true }); }
        else if (lead.riskCategory === "A2") { score += 5; factors.push({ factor: "A2 risk rating", impact: 5, positive: true }); }
        else if (lead.riskCategory === "B2") { score -= 5; factors.push({ factor: "B2 risk rating", impact: -5, positive: false }); }
        else if (lead.riskCategory === "C") { score -= 15; factors.push({ factor: "C risk (decline)", impact: -15, positive: false }); }
      }

      if (lead.yearsInBusiness) {
        if (lead.yearsInBusiness >= 5) { score += 5; factors.push({ factor: "5+ years in business", impact: 5, positive: true }); }
        else if (lead.yearsInBusiness >= 2) { score += 2; factors.push({ factor: "2+ years in business", impact: 2, positive: true }); }
        else { score -= 3; factors.push({ factor: "Less than 2 years in business", impact: -3, positive: false }); }
      }

      score = Math.max(0, Math.min(100, score));

      let grade: string;
      if (score >= 85) grade = "A+";
      else if (score >= 75) grade = "A";
      else if (score >= 65) grade = "B+";
      else if (score >= 55) grade = "B";
      else if (score >= 45) grade = "C+";
      else if (score >= 35) grade = "C";
      else grade = "D";

      return {
        ...lead,
        score,
        grade,
        factors: factors.sort((a, b) => Math.abs(b.impact) - Math.abs(a.impact)),
      };
    });

    scoredLeads.sort((a, b) => b.score - a.score);

    res.json(scoredLeads);
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

router.get("/portfolio/commission-forecast", requireAuth, async (req, res): Promise<void> => {
  try {
    const user = (req as any).user;

    const pipelineDeals = user.role === "rep"
      ? await db.select().from(dealsTable)
          .where(and(eq(dealsTable.repId, user.id), sql`${dealsTable.stage} NOT IN ('funded', 'declined')`))
      : await db.select().from(dealsTable)
          .where(sql`${dealsTable.stage} NOT IN ('funded', 'declined')`);

    const stageConversionRates: Record<string, number> = {
      prospect: 0.15,
      application: 0.30,
      underwriting: 0.55,
      approved: 0.85,
    };

    const defaultCommissionRate = 0.10;

    const forecasts = pipelineDeals.map(deal => {
      const convRate = stageConversionRates[deal.stage] || 0.10;
      const commRate = deal.commission ? (deal.commission / deal.amount) : defaultCommissionRate;
      const expectedCommission = deal.amount * commRate * convRate;

      return {
        dealId: deal.id,
        leadId: deal.leadId,
        stage: deal.stage,
        amount: deal.amount,
        commissionRate: commRate,
        conversionProbability: convRate,
        expectedCommission: Math.round(expectedCommission),
        bestCase: Math.round(deal.amount * commRate),
        worstCase: 0,
      };
    });

    const totalExpected = forecasts.reduce((a, f) => a + f.expectedCommission, 0);
    const totalBestCase = forecasts.reduce((a, f) => a + f.bestCase, 0);
    const totalPipelineValue = forecasts.reduce((a, f) => a + f.amount, 0);

    const byStage = Object.entries(stageConversionRates).map(([stage, rate]) => {
      const stageDeals = forecasts.filter(f => f.stage === stage);
      return {
        stage,
        conversionRate: rate,
        dealCount: stageDeals.length,
        pipelineValue: stageDeals.reduce((a, f) => a + f.amount, 0),
        expectedCommission: stageDeals.reduce((a, f) => a + f.expectedCommission, 0),
      };
    });

    const earnedToDate = user.role === "rep"
      ? await db.select({ total: sql<number>`COALESCE(SUM(amount), 0)` }).from(commissionsTable).where(eq(commissionsTable.repId, user.id))
      : await db.select({ total: sql<number>`COALESCE(SUM(amount), 0)` }).from(commissionsTable);

    res.json({
      forecasts,
      summary: {
        totalPipelineValue,
        totalExpected: Math.round(totalExpected),
        totalBestCase: Math.round(totalBestCase),
        earnedToDate: Number(earnedToDate[0]?.total || 0),
        dealCount: forecasts.length,
      },
      byStage,
    });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

router.post("/portfolio/deal-advisor", requireAuth, async (req, res): Promise<void> => {
  try {
    const { leadId } = req.body;
    const user = (req as any).user;

    const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!lead) { res.status(404).json({ error: "Lead not found" }); return; }
    if (user.role === "rep" && lead.assignedToId !== user.id) {
      res.status(403).json({ error: "Access denied" }); return;
    }

    const funders = await db.select().from(fundersTable).where(eq(fundersTable.active, true));
    const existingDeals = await db.select().from(dealsTable).where(eq(dealsTable.leadId, leadId));
    const analyses = await db.select().from(bankStatementAnalysesTable).where(eq(bankStatementAnalysesTable.leadId, leadId)).orderBy(desc(bankStatementAnalysesTable.createdAt)).limit(1);

    const revenue = lead.monthlyRevenue || lead.grossRevenue || 0;
    const credit = lead.creditScore || 0;
    const riskScore = lead.riskCategory || analyses[0]?.riskScore || "B1";
    const avgBalance = lead.avgDailyBalance || analyses[0]?.avgDailyBalance || 0;
    const existingLoanCount = lead.loanCount || 0;
    const existingLoanPayments = (lead.loanDetails as any[])?.reduce((a: number, l: any) => a + (l.amount || 0), 0) || 0;

    const availableRevenue = revenue - existingLoanPayments;
    const maxPaymentCapacity = availableRevenue * 0.20;

    const maxAmountRiskMultiplier: Record<string, number> = { A1: 1.5, A2: 1.2, B1: 1.0, B2: 0.7, C: 0.0 };
    const multiplier = maxAmountRiskMultiplier[riskScore] || 1.0;

    const suggestedAmount = Math.round(Math.min(
      revenue * multiplier,
      lead.requestedAmount || Infinity,
      avgBalance * 3 || Infinity,
    ) / 1000) * 1000;

    const factorRateByRisk: Record<string, number> = { A1: 1.25, A2: 1.30, B1: 1.38, B2: 1.45, C: 0 };
    const suggestedFactorRate = factorRateByRisk[riskScore] || 1.35;

    const suggestedTerm = riskScore === "A1" || riskScore === "A2" ? 12 : riskScore === "B1" ? 9 : 6;
    const paybackAmount = Math.round(suggestedAmount * suggestedFactorRate);
    const dailyPayment = Math.round(paybackAmount / (suggestedTerm * 22));
    const weeklyPayment = Math.round(paybackAmount / (suggestedTerm * 4));
    const estimatedCommission = Math.round(suggestedAmount * 0.10);

    const matchingFunders = funders.filter(f => {
      if (f.minAmount && suggestedAmount < f.minAmount) return false;
      if (f.maxAmount && suggestedAmount > f.maxAmount) return false;
      if (f.minCreditScore && credit < f.minCreditScore) return false;
      if (f.maxPositions && existingLoanCount >= f.maxPositions) return false;
      return true;
    }).map(f => ({
      id: f.id,
      name: f.name,
      factorRate: f.defaultFactorRate,
      commissionPct: f.commissionPct,
      paymentFrequency: f.paymentFrequency,
    })).slice(0, 5);

    const warnings: string[] = [];
    if (riskScore === "C") warnings.push("Risk score is C — recommend declining this deal");
    if (existingLoanCount >= 3) warnings.push(`Merchant has ${existingLoanCount} existing loans — heavy stacking risk`);
    if (avgBalance < 1000 && avgBalance > 0) warnings.push("Very low average daily balance — payment risk");
    if (lead.revenueTrend === "declining") warnings.push("Revenue is declining — increased default risk");
    if (existingLoanPayments > revenue * 0.40) warnings.push("Existing loan payments exceed 40% of revenue");

    const strengths: string[] = [];
    if (credit >= 650) strengths.push(`Strong credit score (${credit})`);
    if (revenue >= 30000) strengths.push(`Healthy monthly revenue ($${(revenue / 1000).toFixed(0)}K)`);
    if (lead.revenueTrend === "growing") strengths.push("Revenue trend is growing");
    if (existingLoanCount === 0) strengths.push("No existing loans — clean merchant");
    if (avgBalance >= 10000) strengths.push(`Strong daily balance ($${(avgBalance / 1000).toFixed(1)}K)`);

    res.json({
      recommendation: {
        amount: suggestedAmount,
        factorRate: suggestedFactorRate,
        term: suggestedTerm,
        paybackAmount,
        dailyPayment,
        weeklyPayment,
        estimatedCommission,
        paymentFrequency: riskScore === "A1" || riskScore === "A2" ? "weekly" : "daily",
        riskScore,
      },
      matchingFunders,
      warnings,
      strengths,
      metrics: {
        monthlyRevenue: revenue,
        creditScore: credit,
        existingLoans: existingLoanCount,
        existingPayments: existingLoanPayments,
        availableRevenue,
        maxPaymentCapacity,
        avgDailyBalance: avgBalance,
      },
      verdict: riskScore === "C" ? "DECLINE" : riskScore === "B2" ? "CAUTION" : "PROCEED",
    });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

router.get("/portfolio/compare", requireAuth, async (req, res): Promise<void> => {
  try {
    const ids = (req.query.ids as string || "").split(",").map(Number).filter(n => !isNaN(n) && n > 0);
    if (ids.length < 2) { res.status(400).json({ error: "Provide at least 2 lead IDs" }); return; }

    const user = (req as any).user;
    const leads = await db.select().from(leadsTable)
      .where(sql`${leadsTable.id} IN (${sql.join(ids.map(id => sql`${id}`), sql`,`)})`);

    if (user.role === "rep") {
      const filtered = leads.filter(l => l.assignedToId === user.id);
      if (filtered.length !== leads.length) {
        res.status(403).json({ error: "Access denied to one or more leads" }); return;
      }
    }

    const enriched = await Promise.all(leads.map(async (lead) => {
      const deals = await db.select().from(dealsTable).where(eq(dealsTable.leadId, lead.id));
      const analyses = await db.select().from(bankStatementAnalysesTable)
        .where(eq(bankStatementAnalysesTable.leadId, lead.id))
        .orderBy(desc(bankStatementAnalysesTable.createdAt)).limit(1);

      return {
        id: lead.id,
        businessName: lead.businessName,
        ownerName: lead.ownerName,
        monthlyRevenue: lead.monthlyRevenue,
        requestedAmount: lead.requestedAmount,
        creditScore: lead.creditScore,
        riskCategory: lead.riskCategory || analyses[0]?.riskScore,
        avgDailyBalance: lead.avgDailyBalance || analyses[0]?.avgDailyBalance,
        revenueTrend: lead.revenueTrend || analyses[0]?.revenueTrend,
        hasExistingLoans: lead.hasExistingLoans,
        loanCount: lead.loanCount,
        industry: lead.industry,
        status: lead.status,
        dealCount: deals.length,
        totalDealAmount: deals.reduce((a, d) => a + d.amount, 0),
        fundedAmount: deals.filter(d => d.stage === "funded").reduce((a, d) => a + d.amount, 0),
      };
    }));

    res.json(enriched);
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

export default router;
