import { Router, type IRouter } from "express";
import { eq, and, sql, desc, count } from "drizzle-orm";
import { db, underwritingConfirmationsTable, bankStatementAnalysesTable, leadsTable, usersTable, lenderRulesTable } from "../configs/database";
import { requireAuth, requirePermission } from "../middlewares/authMiddleware";

const router: IRouter = Router();

router.get("/ai/learning-center", requireAuth, requirePermission("ai_learning"), async (req, res): Promise<void> => {
  try {
    const user = (req as any).user;

    const totalFindings = await db.select({ count: sql<number>`count(*)::int` }).from(underwritingConfirmationsTable);
    const confirmed = await db.select({ count: sql<number>`count(*)::int` }).from(underwritingConfirmationsTable)
      .where(eq(underwritingConfirmationsTable.status, "confirmed"));
    const rejected = await db.select({ count: sql<number>`count(*)::int` }).from(underwritingConfirmationsTable)
      .where(eq(underwritingConfirmationsTable.status, "rejected"));
    const relabeled = await db.select({ count: sql<number>`count(*)::int` }).from(underwritingConfirmationsTable)
      .where(eq(underwritingConfirmationsTable.status, "relabeled"));
    const pending = await db.select({ count: sql<number>`count(*)::int` }).from(underwritingConfirmationsTable)
      .where(eq(underwritingConfirmationsTable.status, "pending"));

    const reviewed = (confirmed[0]?.count || 0) + (rejected[0]?.count || 0) + (relabeled[0]?.count || 0);
    const accuracy = reviewed > 0
      ? Math.round(((confirmed[0]?.count || 0) / reviewed) * 100)
      : 0;

    const totalAnalyses = await db.select({ count: sql<number>`count(*)::int` }).from(bankStatementAnalysesTable);

    const riskDistribution = await db.select({
      riskScore: bankStatementAnalysesTable.riskScore,
      count: sql<number>`count(*)::int`,
    }).from(bankStatementAnalysesTable)
      .groupBy(bankStatementAnalysesTable.riskScore);

    const recentConfirmations = await db.select({
      id: underwritingConfirmationsTable.id,
      leadId: underwritingConfirmationsTable.leadId,
      businessName: sql<string>`COALESCE(${underwritingConfirmationsTable.leadBusinessName}, ${leadsTable.businessName}, 'Deleted Lead')`,
      findingType: underwritingConfirmationsTable.findingType,
      originalValue: underwritingConfirmationsTable.originalValue,
      status: underwritingConfirmationsTable.status,
      adminLabel: underwritingConfirmationsTable.adminLabel,
      adminNotes: underwritingConfirmationsTable.adminNotes,
      confirmedById: underwritingConfirmationsTable.confirmedById,
      confirmedAt: underwritingConfirmationsTable.confirmedAt,
      createdAt: underwritingConfirmationsTable.createdAt,
      reviewerName: usersTable.fullName,
    }).from(underwritingConfirmationsTable)
      .leftJoin(leadsTable, eq(underwritingConfirmationsTable.leadId, leadsTable.id))
      .leftJoin(usersTable, eq(underwritingConfirmationsTable.confirmedById, usersTable.id))
      .orderBy(desc(underwritingConfirmationsTable.createdAt))
      .limit(200);

    const pendingReview = await db.select({
      id: underwritingConfirmationsTable.id,
      leadId: underwritingConfirmationsTable.leadId,
      businessName: sql<string>`COALESCE(${underwritingConfirmationsTable.leadBusinessName}, ${leadsTable.businessName}, 'Deleted Lead')`,
      findingType: underwritingConfirmationsTable.findingType,
      findingIndex: underwritingConfirmationsTable.findingIndex,
      originalValue: underwritingConfirmationsTable.originalValue,
      status: underwritingConfirmationsTable.status,
      createdAt: underwritingConfirmationsTable.createdAt,
    }).from(underwritingConfirmationsTable)
      .leftJoin(leadsTable, eq(underwritingConfirmationsTable.leadId, leadsTable.id))
      .where(eq(underwritingConfirmationsTable.status, "pending"))
      .orderBy(desc(underwritingConfirmationsTable.createdAt))
      .limit(100);

    const monthlyAccuracy = await db.execute(sql`
      SELECT 
        TO_CHAR(confirmed_at, 'YYYY-MM') as month,
        COUNT(CASE WHEN status = 'confirmed' THEN 1 END)::int as confirmed,
        COUNT(CASE WHEN status = 'rejected' THEN 1 END)::int as rejected,
        COUNT(CASE WHEN status = 'relabeled' THEN 1 END)::int as relabeled,
        COUNT(*)::int as total
      FROM underwriting_confirmations
      WHERE confirmed_at IS NOT NULL
      GROUP BY TO_CHAR(confirmed_at, 'YYYY-MM')
      ORDER BY month DESC
      LIMIT 12
    `);

    const commonFalsePositives = await db.execute(sql`
      SELECT 
        (original_value->>'lender')::text as lender,
        COUNT(*)::int as count
      FROM underwriting_confirmations
      WHERE status = 'rejected' AND original_value->>'lender' IS NOT NULL
      GROUP BY original_value->>'lender'
      ORDER BY count DESC
      LIMIT 10
    `);

    const reviewerStats = await db.execute(sql`
      SELECT 
        u.id as user_id,
        u.full_name as name,
        COUNT(*)::int as total_reviewed,
        COUNT(CASE WHEN uc.status = 'confirmed' THEN 1 END)::int as confirmed,
        COUNT(CASE WHEN uc.status = 'rejected' THEN 1 END)::int as rejected,
        COUNT(CASE WHEN uc.status = 'relabeled' THEN 1 END)::int as relabeled
      FROM underwriting_confirmations uc
      INNER JOIN users u ON uc.confirmed_by_id = u.id
      WHERE uc.status != 'pending'
      GROUP BY u.id, u.full_name
      ORDER BY total_reviewed DESC
    `);

    const myReviewed = await db.select({ count: sql<number>`count(*)::int` }).from(underwritingConfirmationsTable)
      .where(and(
        eq(underwritingConfirmationsTable.confirmedById, user.id),
        sql`${underwritingConfirmationsTable.status} != 'pending'`
      ));

    const lenderRules = await db.select({
      id: lenderRulesTable.id,
      lenderName: lenderRulesTable.lenderName,
      verdict: lenderRulesTable.verdict,
      adminNotes: lenderRulesTable.adminNotes,
      createdAt: lenderRulesTable.createdAt,
      updatedAt: lenderRulesTable.updatedAt,
      reviewerName: usersTable.fullName,
    }).from(lenderRulesTable)
      .leftJoin(usersTable, eq(lenderRulesTable.confirmedById, usersTable.id))
      .orderBy(lenderRulesTable.verdict, lenderRulesTable.lenderName);

    res.json({
      stats: {
        totalFindings: totalFindings[0]?.count || 0,
        confirmed: confirmed[0]?.count || 0,
        rejected: rejected[0]?.count || 0,
        relabeled: relabeled[0]?.count || 0,
        pending: pending[0]?.count || 0,
        accuracy,
        totalAnalyses: totalAnalyses[0]?.count || 0,
        learningDataPoints: reviewed,
        myReviewed: myReviewed[0]?.count || 0,
      },
      riskDistribution: (riskDistribution as any) || [],
      recentConfirmations,
      pendingReview,
      monthlyAccuracy: (monthlyAccuracy as any).rows || monthlyAccuracy,
      commonFalsePositives: (commonFalsePositives as any).rows || commonFalsePositives,
      reviewerStats: (reviewerStats as any).rows || reviewerStats,
      lenderRules,
    });
  } catch (e: any) {
    console.error("AI learning center error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.post("/ai/bulk-review", requireAuth, requirePermission("ai_learning"), async (req, res): Promise<void> => {
  try {
    const { reviews } = req.body;
    const user = (req as any).user;

    if (!Array.isArray(reviews) || reviews.length === 0) {
      res.status(400).json({ error: "reviews array required" });
      return;
    }

    let processed = 0;
    for (const review of reviews) {
      const { id, status, adminLabel, adminNotes } = review;
      if (!id || !["confirmed", "rejected", "relabeled"].includes(status)) continue;

      const [existing] = await db.select().from(underwritingConfirmationsTable)
        .where(eq(underwritingConfirmationsTable.id, id));

      await db.update(underwritingConfirmationsTable).set({
        status,
        adminLabel: adminLabel || null,
        adminNotes: adminNotes || null,
        confirmedById: user.id,
        confirmedAt: new Date(),
      }).where(eq(underwritingConfirmationsTable.id, id));

      if (existing && ["confirmed", "rejected"].includes(status)) {
        const val = existing.originalValue as any;
        const lenderName = (val?.lender || "").trim();
        if (lenderName) {
          const { saveLenderRule, invalidateVerdictCache } = await import("./analysis/coreController");
          await saveLenderRule(lenderName, status, adminNotes || undefined, user.id);
          invalidateVerdictCache();
        }
      }
      processed++;
    }

    try {
      const { autoConfirmKnownLenders } = await import("./analysis/coreController");
      autoConfirmKnownLenders().catch(e => console.error("[Auto-Confirm] Error:", e.message));
    } catch (_) {}

    res.json({ processed });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

router.patch("/ai/lender-rule/:id", requireAuth, requirePermission("ai_learning"), async (req, res): Promise<void> => {
  try {
    const id = parseInt(req.params.id);
    const user = (req as any).user;
    const { verdict, adminNotes } = req.body;
    if (!id || !["confirmed", "rejected"].includes(verdict)) {
      res.status(400).json({ error: "Valid id and verdict (confirmed/rejected) required" });
      return;
    }
    const [updated] = await db.update(lenderRulesTable).set({
      verdict,
      adminNotes: adminNotes !== undefined ? adminNotes : undefined,
      confirmedById: user.id,
      updatedAt: new Date(),
    }).where(eq(lenderRulesTable.id, id)).returning();

    if (!updated) { res.status(404).json({ error: "Rule not found" }); return; }

    const { invalidateVerdictCache, autoConfirmKnownLenders } = await import("./analysis/coreController");
    invalidateVerdictCache();
    autoConfirmKnownLenders().catch(e => console.error("[Auto-Confirm] Error:", e.message));

    res.json({ rule: updated });
  } catch (e: any) { res.status(500).json({ error: e.message }); }
});

router.delete("/ai/lender-rule/:id", requireAuth, requirePermission("ai_learning"), async (req, res): Promise<void> => {
  try {
    const id = parseInt(req.params.id);
    if (!id) { res.status(400).json({ error: "Valid id required" }); return; }
    const [deleted] = await db.delete(lenderRulesTable).where(eq(lenderRulesTable.id, id)).returning();
    if (!deleted) { res.status(404).json({ error: "Rule not found" }); return; }

    const { invalidateVerdictCache } = await import("./analysis/coreController");
    invalidateVerdictCache();

    res.json({ success: true, deleted });
  } catch (e: any) { res.status(500).json({ error: e.message }); }
});

router.get("/ai/analysis-history", requireAuth, async (req, res): Promise<void> => {
  try {
    const user = (req as any).user;
    const limit = Math.min(parseInt(req.query.limit as string) || 20, 50);

    const baseQuery = db.select({
      id: bankStatementAnalysesTable.id,
      leadId: bankStatementAnalysesTable.leadId,
      businessName: leadsTable.businessName,
      hasLoans: bankStatementAnalysesTable.hasLoans,
      riskScore: bankStatementAnalysesTable.riskScore,
      grossRevenue: bankStatementAnalysesTable.grossRevenue,
      avgDailyBalance: bankStatementAnalysesTable.avgDailyBalance,
      revenueTrend: bankStatementAnalysesTable.revenueTrend,
      riskFactors: bankStatementAnalysesTable.riskFactors,
      loanDetails: bankStatementAnalysesTable.loanDetails,
      createdAt: bankStatementAnalysesTable.createdAt,
    }).from(bankStatementAnalysesTable)
      .leftJoin(leadsTable, eq(bankStatementAnalysesTable.leadId, leadsTable.id));

    const analyses = user.role === "rep"
      ? await baseQuery
          .where(eq(leadsTable.assignedToId, user.id))
          .orderBy(desc(bankStatementAnalysesTable.createdAt))
          .limit(limit)
      : await baseQuery
          .orderBy(desc(bankStatementAnalysesTable.createdAt))
          .limit(limit);

    res.json(analyses);
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

export default router;
