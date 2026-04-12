import { Router, type IRouter } from "express";
import { eq, sql, and, desc, gte, count } from "drizzle-orm";
import { db, leadsTable, dealsTable, callsTable, usersTable, commissionsTable, activitiesTable } from "../configs/database";
import { requireAuth, requireAdmin } from "../middlewares/authMiddleware";

const router: IRouter = Router();

router.get("/activity-heatmap", requireAuth, async (req, res): Promise<void> => {
  try {
    const user = (req as any).user;
    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

    const callHeatmap = user.role === "rep"
      ? await db.execute(sql`
          SELECT
            EXTRACT(DOW FROM created_at) AS day_of_week,
            EXTRACT(HOUR FROM created_at) AS hour_of_day,
            COUNT(*)::int AS activity_count
          FROM calls
          WHERE user_id = ${user.id} AND created_at >= ${thirtyDaysAgo}
          GROUP BY day_of_week, hour_of_day
          ORDER BY day_of_week, hour_of_day
        `)
      : await db.execute(sql`
          SELECT
            EXTRACT(DOW FROM created_at) AS day_of_week,
            EXTRACT(HOUR FROM created_at) AS hour_of_day,
            COUNT(*)::int AS activity_count
          FROM calls
          WHERE created_at >= ${thirtyDaysAgo}
          GROUP BY day_of_week, hour_of_day
          ORDER BY day_of_week, hour_of_day
        `);

    const activityHeatmap = user.role === "rep"
      ? await db.execute(sql`
          SELECT
            EXTRACT(DOW FROM created_at) AS day_of_week,
            EXTRACT(HOUR FROM created_at) AS hour_of_day,
            COUNT(*)::int AS activity_count,
            type
          FROM activities
          WHERE user_id = ${user.id} AND created_at >= ${thirtyDaysAgo}
          GROUP BY day_of_week, hour_of_day, type
          ORDER BY day_of_week, hour_of_day
        `)
      : await db.execute(sql`
          SELECT
            EXTRACT(DOW FROM created_at) AS day_of_week,
            EXTRACT(HOUR FROM created_at) AS hour_of_day,
            COUNT(*)::int AS activity_count,
            type
          FROM activities
          WHERE created_at >= ${thirtyDaysAgo}
          GROUP BY day_of_week, hour_of_day, type
          ORDER BY day_of_week, hour_of_day
        `);

    const dailyStats = user.role === "rep"
      ? await db.execute(sql`
          SELECT
            DATE(created_at) AS date,
            COUNT(*)::int AS calls_made
          FROM calls
          WHERE user_id = ${user.id} AND created_at >= ${thirtyDaysAgo}
          GROUP BY DATE(created_at)
          ORDER BY date DESC
        `)
      : await db.execute(sql`
          SELECT
            DATE(created_at) AS date,
            COUNT(*)::int AS calls_made
          FROM calls
          WHERE created_at >= ${thirtyDaysAgo}
          GROUP BY DATE(created_at)
          ORDER BY date DESC
        `);

    const topHours = user.role === "rep"
      ? await db.execute(sql`
          SELECT
            EXTRACT(HOUR FROM created_at)::int AS hour,
            COUNT(*)::int AS total
          FROM calls
          WHERE user_id = ${user.id} AND created_at >= ${thirtyDaysAgo}
          GROUP BY hour
          ORDER BY total DESC
          LIMIT 5
        `)
      : await db.execute(sql`
          SELECT
            EXTRACT(HOUR FROM created_at)::int AS hour,
            COUNT(*)::int AS total
          FROM calls
          WHERE created_at >= ${thirtyDaysAgo}
          GROUP BY hour
          ORDER BY total DESC
          LIMIT 5
        `);

    const rows = (r: any) => r.rows || r;

    res.json({
      callHeatmap: rows(callHeatmap),
      activityHeatmap: rows(activityHeatmap),
      dailyStats: rows(dailyStats),
      topHours: rows(topHours),
    });
  } catch (e: any) {
    console.error("Activity heatmap error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.post("/deal-calculator", requireAuth, async (req, res): Promise<void> => {
  try {
    const { amount, factorRate, term, paymentFrequency, commissionPct } = req.body;

    if (!amount || !factorRate || !term) {
      res.status(400).json({ error: "Amount, factor rate, and term required" });
      return;
    }

    const payback = Math.round(amount * factorRate);
    const profit = payback - amount;
    const apr = ((factorRate - 1) / (term / 12)) * 100;

    let payments: number;
    let paymentAmount: number;
    if (paymentFrequency === "weekly") {
      payments = term * 4;
      paymentAmount = Math.round(payback / payments);
    } else {
      payments = term * 22;
      paymentAmount = Math.round(payback / payments);
    }

    const commission = Math.round(amount * ((commissionPct || 10) / 100));
    const netToMerchant = amount;
    const costOfCapital = payback - amount;

    const scenarios = [
      { name: "Conservative", factorRate: factorRate + 0.05, amount: Math.round(amount * 0.8) },
      { name: "Standard", factorRate, amount },
      { name: "Aggressive", factorRate: Math.max(factorRate - 0.05, 1.10), amount: Math.round(amount * 1.2) },
    ].map(s => ({
      ...s,
      payback: Math.round(s.amount * s.factorRate),
      profit: Math.round(s.amount * s.factorRate - s.amount),
      commission: Math.round(s.amount * ((commissionPct || 10) / 100)),
      dailyPayment: Math.round((s.amount * s.factorRate) / (term * 22)),
      weeklyPayment: Math.round((s.amount * s.factorRate) / (term * 4)),
    }));

    const amortization = [];
    let remaining = payback;
    const pmtAmt = paymentAmount;
    for (let i = 1; i <= Math.min(payments, 60); i++) {
      remaining -= pmtAmt;
      if (remaining < 0) remaining = 0;
      amortization.push({
        payment: i,
        amount: pmtAmt,
        remaining: Math.max(0, remaining),
        pctPaid: Math.round(((payback - remaining) / payback) * 100),
      });
    }

    res.json({
      deal: {
        amount,
        factorRate,
        term,
        paybackAmount: payback,
        profit,
        apr: Math.round(apr * 100) / 100,
        paymentFrequency: paymentFrequency || "daily",
        paymentAmount,
        totalPayments: payments,
        commission,
        netToMerchant,
        costOfCapital,
      },
      scenarios,
      amortization,
    });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

router.get("/performance-goals", requireAuth, async (req, res): Promise<void> => {
  try {
    const user = (req as any).user;
    const now = new Date();
    const startOfMonth = new Date(now.getFullYear(), now.getMonth(), 1);
    const endOfMonth = new Date(now.getFullYear(), now.getMonth() + 1, 0);
    const daysInMonth = endOfMonth.getDate();
    const dayOfMonth = now.getDate();
    const pctMonthComplete = Math.round((dayOfMonth / daysInMonth) * 100);

    const goals = {
      monthlyFundedTarget: 500000,
      monthlyDealsTarget: 10,
      monthlyCallsTarget: 200,
      monthlyCommissionTarget: 50000,
      monthlyNewLeadsTarget: 50,
    };

    const baseCondition = user.role === "rep" ? sql`rep_id = ${user.id}` : sql`1=1`;
    const leadCondition = user.role === "rep" ? sql`assigned_to_id = ${user.id}` : sql`1=1`;

    const fundedThisMonth = await db.execute(sql`
      SELECT COALESCE(SUM(amount), 0) as total, COUNT(*)::int as count
      FROM deals
      WHERE stage = 'funded'
        AND funded_date >= ${startOfMonth}
        AND ${baseCondition}
    `);

    const dealsThisMonth = await db.execute(sql`
      SELECT COUNT(*)::int as count
      FROM deals
      WHERE created_at >= ${startOfMonth}
        AND ${baseCondition}
    `);

    const callsThisMonth = user.role === "rep"
      ? await db.execute(sql`
          SELECT COUNT(*)::int as count FROM calls
          WHERE user_id = ${user.id} AND created_at >= ${startOfMonth}
        `)
      : await db.execute(sql`
          SELECT COUNT(*)::int as count FROM calls
          WHERE created_at >= ${startOfMonth}
        `);

    const commissionsThisMonth = await db.execute(sql`
      SELECT COALESCE(SUM(amount), 0) as total
      FROM commissions
      WHERE created_at >= ${startOfMonth}
        AND ${user.role === "rep" ? sql`rep_id = ${user.id}` : sql`1=1`}
    `);

    const newLeadsThisMonth = await db.execute(sql`
      SELECT COUNT(*)::int as count
      FROM leads
      WHERE created_at >= ${startOfMonth}
        AND ${leadCondition}
    `);

    const rows = (r: any) => (r.rows || r)[0] || {};

    const funded = rows(fundedThisMonth);
    const deals = rows(dealsThisMonth);
    const calls = rows(callsThisMonth);
    const comms = rows(commissionsThisMonth);
    const newLeads = rows(newLeadsThisMonth);

    const metrics = [
      {
        name: "Funded Volume",
        current: Number(funded.total || 0),
        target: goals.monthlyFundedTarget,
        format: "currency",
        icon: "dollar",
      },
      {
        name: "Deals Created",
        current: Number(deals.count || 0),
        target: goals.monthlyDealsTarget,
        format: "number",
        icon: "briefcase",
      },
      {
        name: "Calls Made",
        current: Number(calls.count || 0),
        target: goals.monthlyCallsTarget,
        format: "number",
        icon: "phone",
      },
      {
        name: "Commissions",
        current: Number(comms.total || 0),
        target: goals.monthlyCommissionTarget,
        format: "currency",
        icon: "zap",
      },
      {
        name: "New Leads",
        current: Number(newLeads.count || 0),
        target: goals.monthlyNewLeadsTarget,
        format: "number",
        icon: "users",
      },
    ];

    const overallPct = metrics.length > 0
      ? Math.round(metrics.reduce((a, m) => a + Math.min((m.current / m.target) * 100, 100), 0) / metrics.length)
      : 0;

    res.json({
      metrics,
      pctMonthComplete,
      dayOfMonth,
      daysInMonth,
      daysRemaining: daysInMonth - dayOfMonth,
      overallPct,
      month: now.toLocaleDateString("en-US", { month: "long", year: "numeric" }),
    });
  } catch (e: any) {
    console.error("Performance goals error:", e);
    res.status(500).json({ error: e.message });
  }
});

export default router;
