import { Router, type IRouter } from "express";
import { eq, sql, count, and, gte, desc, sum, avg } from "drizzle-orm";
import {
  db,
  leadsTable,
  dealsTable,
  callsTable,
  commissionsTable,
  usersTable,
  activitiesTable,
  bankStatementAnalysesTable,
} from "../configs/database";
import { anthropic } from "../integrations/anthropic";
import { acquireAiSlot, releaseAiSlot } from "./analysis/coreController";
import { requireAuth, requireAdmin } from "../middlewares/authMiddleware";

const router: IRouter = Router();
const AI_MODEL = "claude-sonnet-4-6";

async function gatherBusinessData() {
  const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
  const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);

  const [totalLeads] = await db.select({ count: count() }).from(leadsTable);
  const [newLeads] = await db.select({ count: count() }).from(leadsTable).where(eq(leadsTable.status, "new"));
  const [contactedLeads] = await db.select({ count: count() }).from(leadsTable).where(eq(leadsTable.status, "contacted"));
  const [qualifiedLeads] = await db.select({ count: count() }).from(leadsTable).where(eq(leadsTable.status, "qualified"));
  const [fundedLeads] = await db.select({ count: count() }).from(leadsTable).where(eq(leadsTable.status, "funded"));

  const [totalDeals] = await db.select({ count: count() }).from(dealsTable);
  const [fundedDeals] = await db.select({ count: count() }).from(dealsTable).where(eq(dealsTable.stage, "funded"));
  const [declinedDeals] = await db.select({ count: count() }).from(dealsTable).where(eq(dealsTable.stage, "declined"));

  const [fundedAmount] = await db.select({
    total: sql<number>`COALESCE(SUM(${dealsTable.amount}), 0)`,
  }).from(dealsTable).where(eq(dealsTable.stage, "funded"));

  const [totalCommissions] = await db.select({
    total: sql<number>`COALESCE(SUM(${dealsTable.commission}), 0)`,
  }).from(dealsTable).where(eq(dealsTable.stage, "funded"));

  const dealsByStage = await db.select({
    stage: dealsTable.stage,
    count: count(),
  }).from(dealsTable).groupBy(dealsTable.stage);

  const leadsByStatus = await db.select({
    status: leadsTable.status,
    count: count(),
  }).from(leadsTable).groupBy(leadsTable.status);

  const reps = await db.select().from(usersTable).where(eq(usersTable.role, "rep"));
  const repPerformance = await Promise.all(reps.map(async (rep) => {
    const [repCalls] = await db.select({ count: count() }).from(callsTable).where(eq(callsTable.userId, rep.id));
    const [repDeals] = await db.select({ count: count() }).from(dealsTable).where(eq(dealsTable.repId, rep.id));
    const [repFunded] = await db.select({
      total: sql<number>`COALESCE(SUM(${dealsTable.amount}), 0)`,
    }).from(dealsTable).where(and(eq(dealsTable.repId, rep.id), eq(dealsTable.stage, "funded")));
    const [repCommission] = await db.select({
      total: sql<number>`COALESCE(SUM(${dealsTable.commission}), 0)`,
    }).from(dealsTable).where(and(eq(dealsTable.repId, rep.id), eq(dealsTable.stage, "funded")));

    return {
      name: rep.fullName,
      calls: repCalls.count,
      deals: repDeals.count,
      fundedVolume: Number(repFunded.total),
      commissions: Number(repCommission.total),
    };
  }));

  const recentActivities = await db.select({
    type: activitiesTable.type,
    count: count(),
  }).from(activitiesTable)
    .where(gte(activitiesTable.createdAt, thirtyDaysAgo))
    .groupBy(activitiesTable.type);

  const [totalCalls] = await db.select({ count: count() }).from(callsTable);
  const [recentCalls] = await db.select({ count: count() }).from(callsTable).where(gte(callsTable.createdAt, sevenDaysAgo));

  const bankAnalyses = await db.select({
    count: count(),
    avgScore: sql<number>`AVG(${bankStatementAnalysesTable.overallScore})`,
  }).from(bankStatementAnalysesTable);

  return {
    leads: {
      total: totalLeads.count,
      new: newLeads.count,
      contacted: contactedLeads.count,
      qualified: qualifiedLeads.count,
      funded: fundedLeads.count,
      byStatus: leadsByStatus,
    },
    deals: {
      total: totalDeals.count,
      funded: fundedDeals.count,
      declined: declinedDeals.count,
      totalFundedAmount: Number(fundedAmount.total),
      totalCommissions: Number(totalCommissions.total),
      byStage: dealsByStage,
    },
    repPerformance,
    activity: {
      totalCalls: totalCalls.count,
      recentCalls7d: recentCalls.count,
      recentByType: recentActivities,
    },
    bankAnalyses: {
      total: bankAnalyses[0]?.count || 0,
      avgScore: Number(bankAnalyses[0]?.avgScore || 0),
    },
  };
}

const REPORT_TYPES: Record<string, { title: string; description: string; systemPrompt: string }> = {
  executive_summary: {
    title: "Executive Summary",
    description: "High-level business overview with key metrics and strategic insights",
    systemPrompt: `You are a senior business analyst for Bridge Capital, a cash advance / MCA (Merchant Cash Advance) company. Generate a professional executive summary report. Include:
- Key performance indicators (funded volume, conversion rates, deal pipeline)
- Trend analysis and notable patterns
- Top-performing reps and areas of concern
- Strategic recommendations
Format the report in clean markdown with headers, bullet points, and bold key numbers. Use professional business language.`,
  },
  sales_performance: {
    title: "Sales Performance Report",
    description: "Rep-level performance breakdown with rankings and coaching insights",
    systemPrompt: `You are a sales analytics expert for Bridge Capital, a cash advance / MCA company. Generate a detailed sales performance report. Include:
- Rep rankings by funded volume, deal count, and conversion rate
- Call activity analysis and efficiency metrics
- Commission earnings breakdown
- Coaching recommendations for underperforming reps
- Recognition for top performers
Format the report in clean markdown with tables, headers, and actionable insights.`,
  },
  pipeline_health: {
    title: "Pipeline Health Report",
    description: "Deal pipeline analysis with bottleneck detection and forecasting",
    systemPrompt: `You are a pipeline analyst for Bridge Capital, a cash advance / MCA company. Generate a pipeline health report. Include:
- Stage-by-stage breakdown with conversion rates between stages
- Pipeline bottleneck identification
- Average deal velocity estimates
- Decline analysis and patterns
- Revenue forecast based on current pipeline
Format the report in clean markdown with data tables and strategic insights.`,
  },
  risk_assessment: {
    title: "Risk Assessment Report",
    description: "Portfolio risk analysis with bank statement insights and decline patterns",
    systemPrompt: `You are a risk analyst for Bridge Capital, a cash advance / MCA company. Generate a risk assessment report. Include:
- Bank statement analysis summary (average scores, flagged accounts)
- Decline rate trends and common decline reasons
- Portfolio concentration risk
- Underwriting quality metrics
- Risk mitigation recommendations
Format the report in clean markdown with clear risk categories and actionable recommendations.`,
  },
  commission_audit: {
    title: "Commission Audit Report",
    description: "Detailed commission tracking with payout verification and projections",
    systemPrompt: `You are a finance analyst for Bridge Capital, a cash advance / MCA company. Generate a commission audit report. Include:
- Total commission payouts by rep
- Commission rate analysis
- Funded volume to commission ratios
- Outstanding or pending commissions
- Month-over-month commission trends
Format the report in clean markdown with financial tables and clear breakdowns.`,
  },
};

router.get("/reports/types", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  const types = Object.entries(REPORT_TYPES).map(([key, val]) => ({
    id: key,
    title: val.title,
    description: val.description,
  }));
  res.json(types);
});

router.post("/reports/generate", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  const { reportType } = req.body;

  if (!reportType || !REPORT_TYPES[reportType]) {
    res.status(400).json({ error: "Invalid report type" });
    return;
  }

  try {
    const data = await gatherBusinessData();
    const config = REPORT_TYPES[reportType];

    await acquireAiSlot();
    let response;
    try {
      response = await anthropic.messages.create({
        model: AI_MODEL,
        max_tokens: 8192,
        system: config.systemPrompt,
        messages: [{
          role: "user",
          content: `Generate a ${config.title} based on the following business data:\n\n${JSON.stringify(data, null, 2)}\n\nToday's date: ${new Date().toLocaleDateString("en-US", { weekday: "long", year: "numeric", month: "long", day: "numeric" })}`,
        }],
      });
    } finally {
      releaseAiSlot();
    }

    const block = response.content[0];
    const content = block.type === "text" ? block.text : "";

    res.json({
      title: config.title,
      content,
      generatedAt: new Date().toISOString(),
      dataSnapshot: {
        totalLeads: data.leads.total,
        fundedDeals: data.deals.funded,
        totalFundedAmount: data.deals.totalFundedAmount,
        totalCommissions: data.deals.totalCommissions,
      },
    });
  } catch (error: any) {
    console.error("Report generation error:", error);
    res.status(500).json({ error: "Failed to generate report" });
  }
});

router.post("/reports/chat", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  const { message, conversationHistory = [] } = req.body;

  if (!message || typeof message !== "string") {
    res.status(400).json({ error: "Message is required" });
    return;
  }

  if (message.length > 2000) {
    res.status(400).json({ error: "Message too long (max 2000 characters)" });
    return;
  }

  if (!Array.isArray(conversationHistory) || conversationHistory.length > 20) {
    res.status(400).json({ error: "Invalid conversation history" });
    return;
  }

  try {
    const data = await gatherBusinessData();

    const systemPrompt = `You are an AI business analyst for Bridge Capital, a cash advance / MCA (Merchant Cash Advance) company. You have access to the company's real-time business data and can answer any questions about performance, sales, pipeline, commissions, risk, and more.

Current business data:
${JSON.stringify(data, null, 2)}

Today's date: ${new Date().toLocaleDateString("en-US", { weekday: "long", year: "numeric", month: "long", day: "numeric" })}

Guidelines:
- Answer questions accurately based on the data provided
- Use specific numbers and percentages when possible
- Provide actionable insights and recommendations
- Format responses in clean markdown when appropriate
- If asked to generate a report, create a comprehensive markdown report
- Be concise but thorough
- You can calculate derived metrics (conversion rates, averages, etc.) from the raw data`;

    const messages = [
      ...conversationHistory.map((msg: any) => ({
        role: msg.role as "user" | "assistant",
        content: msg.content,
      })),
      { role: "user" as const, content: message },
    ];

    await acquireAiSlot();
    let response;
    try {
      response = await anthropic.messages.create({
        model: AI_MODEL,
        max_tokens: 8192,
        system: systemPrompt,
        messages,
      });
    } finally {
      releaseAiSlot();
    }

    const block = response.content[0];
    const content = block.type === "text" ? block.text : "";

    res.json({ content });
  } catch (error: any) {
    console.error("Report chat error:", error);
    res.status(500).json({ error: "Failed to process your question" });
  }
});

export default router;
