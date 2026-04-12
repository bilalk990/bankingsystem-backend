import { Router, type IRouter } from "express";
import { sql, eq, and, gte, lte, desc, count } from "drizzle-orm";
import { db, dealsTable, leadsTable, commissionsTable, usersTable, activitiesTable, callsTable, leadMessagesTable } from "../configs/database";
import { requireAuth, requireAdmin } from "../middlewares/authMiddleware";

const router: IRouter = Router();

router.get("/analytics/funnel", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;

  const stageFilter = user.role === "rep" ? eq(dealsTable.repId, user.id) : undefined;

  const stages = ["prospect", "application", "underwriting", "approved", "funded", "declined"];
  const results: Record<string, number> = {};

  for (const stage of stages) {
    const where = stageFilter
      ? and(eq(dealsTable.stage, stage), stageFilter)
      : eq(dealsTable.stage, stage);

    const [row] = await db.select({ count: sql<number>`count(*)::int` })
      .from(dealsTable)
      .where(where!);
    results[stage] = row?.count || 0;
  }

  const totalLeads = user.role === "rep"
    ? await db.select({ count: sql<number>`count(*)::int` }).from(leadsTable).where(eq(leadsTable.assignedToId, user.id))
    : await db.select({ count: sql<number>`count(*)::int` }).from(leadsTable);

  res.json({
    totalLeads: totalLeads[0]?.count || 0,
    stages: results,
  });
});

router.get("/analytics/trends", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;

  const monthlyDeals = await db.execute(sql`
    SELECT 
      TO_CHAR(created_at, 'YYYY-MM') as month,
      COUNT(*)::int as count,
      COALESCE(SUM(amount), 0)::float as total_amount,
      COUNT(CASE WHEN stage = 'funded' THEN 1 END)::int as funded_count,
      COALESCE(SUM(CASE WHEN stage = 'funded' THEN amount ELSE 0 END), 0)::float as funded_amount
    FROM deals
    ${user.role === "rep" ? sql`WHERE rep_id = ${user.id}` : sql``}
    GROUP BY TO_CHAR(created_at, 'YYYY-MM')
    ORDER BY month DESC
    LIMIT 12
  `);

  const monthlyLeads = await db.execute(sql`
    SELECT 
      TO_CHAR(created_at, 'YYYY-MM') as month,
      COUNT(*)::int as count
    FROM leads
    ${user.role === "rep" ? sql`WHERE assigned_to_id = ${user.id}` : sql``}
    GROUP BY TO_CHAR(created_at, 'YYYY-MM')
    ORDER BY month DESC
    LIMIT 12
  `);

  res.json({
    monthlyDeals: (monthlyDeals as any).rows || monthlyDeals,
    monthlyLeads: (monthlyLeads as any).rows || monthlyLeads,
  });
});

router.get("/analytics/velocity", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;

  const avgDealSize = await db.execute(sql`
    SELECT 
      COALESCE(AVG(amount), 0)::float as avg_amount,
      COALESCE(AVG(CASE WHEN stage = 'funded' THEN amount END), 0)::float as avg_funded_amount,
      COUNT(CASE WHEN stage = 'funded' THEN 1 END)::int as total_funded,
      COALESCE(SUM(CASE WHEN stage = 'funded' THEN amount ELSE 0 END), 0)::float as total_funded_amount,
      COUNT(*)::int as total_deals,
      COALESCE(AVG(EXTRACT(EPOCH FROM (updated_at - created_at)) / 86400), 0)::float as avg_days_in_pipeline
    FROM deals
    ${user.role === "rep" ? sql`WHERE rep_id = ${user.id}` : sql``}
  `);

  const leadsByStatus = await db.execute(sql`
    SELECT status, COUNT(*)::int as count
    FROM leads
    ${user.role === "rep" ? sql`WHERE assigned_to_id = ${user.id}` : sql``}
    GROUP BY status
  `);

  const leadsBySource = await db.execute(sql`
    SELECT COALESCE(source, 'manual') as source, COUNT(*)::int as count
    FROM leads
    ${user.role === "rep" ? sql`WHERE assigned_to_id = ${user.id}` : sql``}
    GROUP BY source
  `);

  const topReps = user.role === "admin" ? await db.execute(sql`
    SELECT 
      u.full_name,
      COUNT(d.id)::int as deal_count,
      COUNT(CASE WHEN d.stage = 'funded' THEN 1 END)::int as funded_count,
      COALESCE(SUM(CASE WHEN d.stage = 'funded' THEN d.amount ELSE 0 END), 0)::float as total_funded,
      COALESCE(SUM(c.amount), 0)::float as total_commissions
    FROM users u
    LEFT JOIN deals d ON d.rep_id = u.id
    LEFT JOIN commissions c ON c.rep_id = u.id AND c.status = 'paid'
    WHERE u.role = 'rep'
    GROUP BY u.id, u.full_name
    ORDER BY total_funded DESC
    LIMIT 10
  `) : null;

  res.json({
    metrics: (avgDealSize as any).rows?.[0] || avgDealSize[0] || {},
    leadsByStatus: (leadsByStatus as any).rows || leadsByStatus,
    leadsBySource: (leadsBySource as any).rows || leadsBySource,
    topReps: topReps ? ((topReps as any).rows || topReps) : [],
  });
});

router.get("/analytics/renewals", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;

  const now = new Date();
  const thirtyDaysFromNow = new Date(now.getTime() + 30 * 24 * 60 * 60 * 1000);
  const sixtyDaysFromNow = new Date(now.getTime() + 60 * 24 * 60 * 60 * 1000);

  const baseWhere = user.role === "rep" ? eq(dealsTable.repId, user.id) : undefined;

  const renewals = await db.select({
    id: dealsTable.id,
    leadId: dealsTable.leadId,
    businessName: leadsTable.businessName,
    amount: dealsTable.amount,
    stage: dealsTable.stage,
    fundedDate: dealsTable.fundedDate,
    renewalEligibleDate: dealsTable.renewalEligibleDate,
    paymentsCompleted: dealsTable.paymentsCompleted,
    totalPayments: dealsTable.totalPayments,
    repName: usersTable.fullName,
  }).from(dealsTable)
    .leftJoin(leadsTable, eq(dealsTable.leadId, leadsTable.id))
    .leftJoin(usersTable, eq(dealsTable.repId, usersTable.id))
    .where(and(
      eq(dealsTable.stage, "funded"),
      baseWhere || sql`true`,
    ))
    .orderBy(dealsTable.renewalEligibleDate);

  const categorized = {
    overdue: renewals.filter(r => r.renewalEligibleDate && new Date(r.renewalEligibleDate) < now),
    upcoming: renewals.filter(r => r.renewalEligibleDate && new Date(r.renewalEligibleDate) >= now && new Date(r.renewalEligibleDate) <= thirtyDaysFromNow),
    future: renewals.filter(r => r.renewalEligibleDate && new Date(r.renewalEligibleDate) > thirtyDaysFromNow && new Date(r.renewalEligibleDate) <= sixtyDaysFromNow),
    eligible: renewals.filter(r => {
      if (!r.totalPayments || !r.paymentsCompleted) return false;
      return r.paymentsCompleted / r.totalPayments >= 0.5;
    }),
  };

  res.json(categorized);
});

router.get("/analytics/funder-match/:leadId", requireAuth, async (req, res): Promise<void> => {
  const leadId = parseInt(req.params.leadId);
  const user = (req as any).user;

  const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
  if (!lead) {
    res.status(404).json({ error: "Lead not found" });
    return;
  }

  if (user.role === "rep" && lead.assignedToId !== user.id) {
    res.status(403).json({ error: "Access denied" });
    return;
  }

  const { fundersTable } = await import("../configs/database");
  const funders = await db.select().from(fundersTable).where(eq(fundersTable.active, true));

  const matches = funders.map(funder => {
    let score = 0;
    let maxScore = 0;
    const reasons: string[] = [];
    const disqualifiers: string[] = [];

    if (funder.minAmount !== null && funder.maxAmount !== null && lead.requestedAmount) {
      maxScore += 25;
      if (lead.requestedAmount >= (funder.minAmount || 0) && lead.requestedAmount <= (funder.maxAmount || Infinity)) {
        score += 25;
        reasons.push("Amount within range");
      } else {
        disqualifiers.push(`Amount ${lead.requestedAmount < (funder.minAmount || 0) ? "below minimum" : "above maximum"}`);
      }
    }

    if (funder.minCreditScore !== null && lead.creditScore) {
      maxScore += 25;
      if (lead.creditScore >= (funder.minCreditScore || 0)) {
        score += 25;
        reasons.push("Credit score meets requirement");
      } else {
        disqualifiers.push(`Credit score below ${funder.minCreditScore}`);
      }
    }

    if (funder.minTimeInBusiness !== null && lead.yearsInBusiness) {
      maxScore += 25;
      const monthsInBusiness = lead.yearsInBusiness * 12;
      if (monthsInBusiness >= (funder.minTimeInBusiness || 0)) {
        score += 25;
        reasons.push("Time in business meets requirement");
      } else {
        disqualifiers.push(`Needs ${funder.minTimeInBusiness} months, has ${Math.round(monthsInBusiness)}`);
      }
    }

    if (funder.industries && lead.industry) {
      maxScore += 15;
      const allowedIndustries = funder.industries as string[];
      if (Array.isArray(allowedIndustries) && (allowedIndustries.length === 0 || allowedIndustries.includes(lead.industry))) {
        score += 15;
        reasons.push("Industry accepted");
      } else {
        disqualifiers.push("Industry not accepted");
      }
    }

    if (funder.maxPositions && lead.loanCount !== null) {
      maxScore += 10;
      if ((lead.loanCount || 0) < funder.maxPositions) {
        score += 10;
        reasons.push(`Position ${(lead.loanCount || 0) + 1} within limit`);
      } else {
        disqualifiers.push(`Max positions (${funder.maxPositions}) exceeded`);
      }
    }

    const matchPct = maxScore > 0 ? Math.round((score / maxScore) * 100) : 50;

    return {
      funder: {
        id: funder.id,
        name: funder.name,
        minAmount: funder.minAmount,
        maxAmount: funder.maxAmount,
        defaultFactorRate: funder.defaultFactorRate,
        commissionPct: funder.commissionPct,
        paymentFrequency: funder.paymentFrequency,
      },
      matchScore: matchPct,
      reasons,
      disqualifiers,
    };
  });

  matches.sort((a, b) => b.matchScore - a.matchScore);

  res.json({ lead: { id: lead.id, businessName: lead.businessName }, matches });
});

router.get("/analytics/export/:type", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  const user = (req as any).user;
  const exportType = req.params.type;

  let data: any[] = [];
  let headers: string[] = [];

  if (exportType === "leads") {
    const filter = user.role === "rep" ? eq(leadsTable.assignedToId, user.id) : undefined;
    const leads = filter
      ? await db.select().from(leadsTable).where(filter)
      : await db.select().from(leadsTable);
    headers = ["ID", "Business Name", "Owner", "Phone", "Email", "Status", "Risk Category", "Requested Amount", "Monthly Revenue", "Credit Score", "Industry", "State", "Created"];
    data = leads.map(l => [
      l.id, l.businessName, l.ownerName, l.phone, l.email || "", l.status, l.riskCategory || "",
      l.requestedAmount || "", l.monthlyRevenue || l.grossRevenue || "", l.creditScore || "",
      l.industry || "", l.state || "", l.createdAt?.toISOString().split("T")[0] || "",
    ]);
  } else if (exportType === "deals") {
    const filter = user.role === "rep" ? eq(dealsTable.repId, user.id) : undefined;
    const deals = await db.select({
      id: dealsTable.id,
      businessName: leadsTable.businessName,
      amount: dealsTable.amount,
      stage: dealsTable.stage,
      factorRate: dealsTable.factorRate,
      paybackAmount: dealsTable.paybackAmount,
      repName: usersTable.fullName,
      fundedDate: dealsTable.fundedDate,
      createdAt: dealsTable.createdAt,
    }).from(dealsTable)
      .leftJoin(leadsTable, eq(dealsTable.leadId, leadsTable.id))
      .leftJoin(usersTable, eq(dealsTable.repId, usersTable.id))
      .where(filter || sql`true`);
    headers = ["ID", "Business", "Amount", "Stage", "Factor Rate", "Payback", "Rep", "Funded Date", "Created"];
    data = deals.map(d => [
      d.id, d.businessName || "", d.amount, d.stage, d.factorRate || "", d.paybackAmount || "",
      d.repName || "", d.fundedDate?.toISOString().split("T")[0] || "", d.createdAt?.toISOString().split("T")[0] || "",
    ]);
  } else if (exportType === "commissions") {
    const filter = user.role === "rep" ? eq(commissionsTable.repId, user.id) : undefined;
    const commissions = await db.select({
      id: commissionsTable.id,
      amount: commissionsTable.amount,
      percentage: commissionsTable.percentage,
      status: commissionsTable.status,
      repName: usersTable.fullName,
      dealAmount: dealsTable.amount,
      businessName: leadsTable.businessName,
      createdAt: commissionsTable.createdAt,
    }).from(commissionsTable)
      .leftJoin(usersTable, eq(commissionsTable.repId, usersTable.id))
      .leftJoin(dealsTable, eq(commissionsTable.dealId, dealsTable.id))
      .leftJoin(leadsTable, eq(dealsTable.leadId, leadsTable.id))
      .where(filter || sql`true`);
    headers = ["ID", "Business", "Deal Amount", "Commission", "Percentage", "Status", "Rep", "Created"];
    data = commissions.map(c => [
      c.id, c.businessName || "", c.dealAmount || "", c.amount, c.percentage || "",
      c.status, c.repName || "", c.createdAt?.toISOString().split("T")[0] || "",
    ]);
  } else {
    res.status(400).json({ error: "Invalid export type" });
    return;
  }

  const csv = [headers.join(","), ...data.map(row => row.map((v: any) => `"${String(v).replace(/"/g, '""')}"`).join(","))].join("\n");

  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename=${exportType}_export_${new Date().toISOString().split("T")[0]}.csv`);
  res.send(csv);
});

router.get("/analytics/leaderboard", requireAuth, async (req, res): Promise<void> => {
  try {
    const period = (req.query.period as string) || "month";
    let dateFilter = sql`true`;
    const now = new Date();

    if (period === "week") {
      const weekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
      dateFilter = sql`d.created_at >= ${weekAgo.toISOString()}`;
    } else if (period === "month") {
      const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
      dateFilter = sql`d.created_at >= ${monthStart.toISOString()}`;
    } else if (period === "quarter") {
      const quarterStart = new Date(now.getFullYear(), Math.floor(now.getMonth() / 3) * 3, 1);
      dateFilter = sql`d.created_at >= ${quarterStart.toISOString()}`;
    }

    const repStats = await db.execute(sql`
      SELECT 
        u.id as rep_id,
        u.full_name as rep_name,
        u.role,
        u.commission_pct,
        u.email,
        u.active,
        COALESCE((SELECT COUNT(*) FROM deals d WHERE d.rep_id = u.id AND ${dateFilter}), 0)::int as total_deals,
        COALESCE((SELECT COUNT(*) FROM deals d WHERE d.rep_id = u.id AND d.stage = 'funded' AND ${dateFilter}), 0)::int as funded_deals,
        COALESCE((SELECT SUM(d.amount) FROM deals d WHERE d.rep_id = u.id AND d.stage = 'funded' AND ${dateFilter}), 0)::float as funded_volume,
        COALESCE((SELECT SUM(d.commission) FROM deals d WHERE d.rep_id = u.id AND d.stage = 'funded' AND ${dateFilter}), 0)::float as total_commission,
        COALESCE((SELECT COUNT(*) FROM calls c WHERE c.user_id = u.id ${
          period === "all" ? sql`` :
          period === "week" ? sql`AND c.created_at >= NOW() - INTERVAL '7 days'` :
          period === "quarter" ? sql`AND c.created_at >= DATE_TRUNC('quarter', NOW())` :
          sql`AND c.created_at >= DATE_TRUNC('month', NOW())`
        }), 0)::int as total_calls,
        COALESCE((SELECT COUNT(*) FROM lead_messages lm WHERE lm.user_id = u.id AND lm.direction = 'outbound' ${
          period === "all" ? sql`` :
          period === "week" ? sql`AND lm.created_at >= NOW() - INTERVAL '7 days'` :
          period === "quarter" ? sql`AND lm.created_at >= DATE_TRUNC('quarter', NOW())` :
          sql`AND lm.created_at >= DATE_TRUNC('month', NOW())`
        }), 0)::int as messages_sent,
        COALESCE((SELECT COUNT(DISTINCT DATE(c.created_at)) FROM calls c WHERE c.user_id = u.id AND c.created_at >= NOW() - INTERVAL '30 days'), 0)::int as active_days_30,
        COALESCE((SELECT COUNT(*) FROM leads l WHERE l.assigned_to_id = u.id AND l.status NOT IN ('funded', 'not_interested')), 0)::int as active_leads,
        COALESCE((SELECT AVG(EXTRACT(EPOCH FROM (d.updated_at - d.created_at)) / 86400) FROM deals d WHERE d.rep_id = u.id AND d.stage = 'funded'), 0)::float as avg_days_to_fund,
        COALESCE((SELECT COUNT(*) FROM deals d WHERE d.rep_id = u.id AND d.stage = 'prospect' AND ${dateFilter}), 0)::int as prospect_count,
        COALESCE((SELECT COUNT(*) FROM deals d WHERE d.rep_id = u.id AND d.stage = 'application' AND ${dateFilter}), 0)::int as application_count,
        COALESCE((SELECT COUNT(*) FROM deals d WHERE d.rep_id = u.id AND d.stage = 'underwriting' AND ${dateFilter}), 0)::int as underwriting_count,
        COALESCE((SELECT COUNT(*) FROM deals d WHERE d.rep_id = u.id AND d.stage = 'approved' AND ${dateFilter}), 0)::int as approved_count,
        COALESCE((SELECT COUNT(*) FROM deals d WHERE d.rep_id = u.id AND d.stage = 'declined' AND ${dateFilter}), 0)::int as declined_count
      FROM users u
      WHERE u.role IN ('rep', 'admin')
      ORDER BY funded_volume DESC
    `);

    const rows = (repStats as any).rows || repStats;

    const leaderboard = rows.map((r: any, i: number) => {
      const winRate = r.total_deals > 0 ? Math.round((r.funded_deals / r.total_deals) * 100) : 0;
      const avgDealSize = r.funded_deals > 0 ? Math.round(r.funded_volume / r.funded_deals) : 0;
      const activityScore = Math.min(100, Math.round(
        (r.total_calls * 2 + r.messages_sent * 1.5 + r.funded_deals * 20 + r.active_days_30 * 3) / 2
      ));

      const badges: string[] = [];
      if (r.funded_deals >= 10) badges.push("deal_machine");
      if (r.funded_deals >= 5) badges.push("closer");
      if (r.total_calls >= 100) badges.push("phone_warrior");
      if (r.total_calls >= 50) badges.push("dialer");
      if (winRate >= 50) badges.push("sharpshooter");
      if (r.active_days_30 >= 20) badges.push("consistent");
      if (r.active_days_30 >= 25) badges.push("iron_will");
      if (r.funded_volume >= 500000) badges.push("half_million_club");
      if (r.funded_volume >= 1000000) badges.push("million_dollar_rep");
      if (r.messages_sent >= 50) badges.push("communicator");
      if (r.avg_days_to_fund > 0 && r.avg_days_to_fund <= 7) badges.push("speed_demon");

      return {
        rank: i + 1,
        repId: r.rep_id,
        repName: r.rep_name,
        role: r.role,
        email: r.email,
        commissionPct: r.commission_pct ?? 10,
        active: r.active,
        totalDeals: r.total_deals,
        fundedDeals: r.funded_deals,
        fundedVolume: r.funded_volume,
        totalCommission: r.total_commission,
        totalCalls: r.total_calls,
        messagesSent: r.messages_sent,
        activeDays30: r.active_days_30,
        activeLeads: r.active_leads,
        avgDaysToFund: Math.round(r.avg_days_to_fund * 10) / 10,
        winRate,
        avgDealSize,
        activityScore,
        badges,
        pipeline: {
          prospect: r.prospect_count,
          application: r.application_count,
          underwriting: r.underwriting_count,
          approved: r.approved_count,
          funded: r.funded_deals,
          declined: r.declined_count,
        },
      };
    });

    const teamStats = {
      totalFundedVolume: leaderboard.reduce((s: number, r: any) => s + r.fundedVolume, 0),
      totalDeals: leaderboard.reduce((s: number, r: any) => s + r.fundedDeals, 0),
      totalCalls: leaderboard.reduce((s: number, r: any) => s + r.totalCalls, 0),
      avgWinRate: leaderboard.length > 0
        ? Math.round(leaderboard.reduce((s: number, r: any) => s + r.winRate, 0) / leaderboard.length)
        : 0,
    };

    res.json({ leaderboard, teamStats, period });
  } catch (e: any) {
    console.error("Leaderboard error:", e);
    res.status(500).json({ error: "Failed to generate leaderboard" });
  }
});

router.get("/analytics/deal-velocity", requireAuth, async (req, res): Promise<void> => {
  try {
    const user = (req as any).user;
    const repFilter = user.role === "rep" ? sql`AND d.rep_id = ${user.id}` : sql``;

    const stageVelocity = await db.execute(sql`
      WITH stage_transitions AS (
        SELECT 
          a.metadata->>'to' as to_stage,
          a.metadata->>'from' as from_stage,
          EXTRACT(EPOCH FROM (a.created_at - LAG(a.created_at) OVER (
            PARTITION BY a.metadata->>'dealId' ORDER BY a.created_at
          ))) / 86400 as days_in_prev_stage
        FROM activities a
        JOIN deals d ON (a.metadata->>'dealId')::int = d.id
        WHERE a.type IN ('deal_stage_change', 'deal_funded', 'deal_created')
        ${repFilter}
      )
      SELECT 
        to_stage as stage,
        ROUND(AVG(days_in_prev_stage)::numeric, 1) as avg_days,
        ROUND(MIN(days_in_prev_stage)::numeric, 1) as min_days,
        ROUND(MAX(days_in_prev_stage)::numeric, 1) as max_days,
        COUNT(*)::int as transitions
      FROM stage_transitions
      WHERE days_in_prev_stage IS NOT NULL AND days_in_prev_stage > 0
      GROUP BY to_stage
      ORDER BY 
        CASE to_stage 
          WHEN 'application' THEN 1 
          WHEN 'underwriting' THEN 2 
          WHEN 'approved' THEN 3 
          WHEN 'funded' THEN 4 
          ELSE 5 
        END
    `);

    const staleDeals = await db.execute(sql`
      SELECT 
        d.id as deal_id,
        d.lead_id,
        l.business_name,
        l.owner_name,
        d.stage,
        d.amount,
        d.updated_at,
        u.full_name as rep_name,
        EXTRACT(DAY FROM NOW() - d.updated_at)::int as days_stale
      FROM deals d
      LEFT JOIN leads l ON d.lead_id = l.id
      LEFT JOIN users u ON d.rep_id = u.id
      WHERE d.stage NOT IN ('funded', 'declined')
      AND d.updated_at < NOW() - INTERVAL '5 days'
      ${repFilter}
      ORDER BY d.updated_at ASC
      LIMIT 20
    `);

    const avgTimeToFund = await db.execute(sql`
      SELECT 
        ROUND(AVG(EXTRACT(EPOCH FROM (d.funded_date - d.created_at)) / 86400)::numeric, 1) as avg_days,
        ROUND(MIN(EXTRACT(EPOCH FROM (d.funded_date - d.created_at)) / 86400)::numeric, 1) as fastest,
        COUNT(*)::int as total_funded
      FROM deals d
      WHERE d.stage = 'funded' AND d.funded_date IS NOT NULL
      ${repFilter}
    `);

    res.json({
      stageVelocity: (stageVelocity as any).rows || stageVelocity,
      staleDeals: (staleDeals as any).rows || staleDeals,
      avgTimeToFund: ((avgTimeToFund as any).rows || avgTimeToFund)[0] || {},
    });
  } catch (e: any) {
    console.error("Deal velocity error:", e);
    res.status(500).json({ error: "Failed to get deal velocity" });
  }
});

export default router;
