import { Router, type IRouter } from "express";
import { eq, sql, count, and, gte, desc, isNull, ne } from "drizzle-orm";
import { db, leadsTable, dealsTable, callsTable, usersTable, documentsTable, activitiesTable, bankStatementAnalysesTable, underwritingConfirmationsTable, notificationsTable } from "../configs/database";
import { GetStatsResponse } from "../validationSchemas";
import { requireAuth, requireAdmin } from "../middlewares/authMiddleware";
import { maskSsn } from "../utils/encryption";

const router: IRouter = Router();

router.get("/stats", requireAuth, async (_req, res): Promise<void> => {
  const totalLeads = await db.select({ count: count() }).from(leadsTable);
  const newLeads = await db.select({ count: count() }).from(leadsTable).where(eq(leadsTable.status, "new"));
  const contactedLeads = await db.select({ count: count() }).from(leadsTable).where(eq(leadsTable.status, "contacted"));
  const qualifiedLeads = await db.select({ count: count() }).from(leadsTable).where(eq(leadsTable.status, "qualified"));

  const totalDeals = await db.select({ count: count() }).from(dealsTable);
  const fundedDeals = await db.select({ count: count() }).from(dealsTable).where(eq(dealsTable.stage, "funded"));

  const fundedAmountResult = await db.select({
    total: sql<number>`COALESCE(SUM(${dealsTable.amount}), 0)`,
  }).from(dealsTable).where(eq(dealsTable.stage, "funded"));

  const commissionResult = await db.select({
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
    const repCalls = await db.select({ count: count() }).from(callsTable).where(eq(callsTable.userId, rep.id));
    const repDeals = await db.select({ count: count() }).from(dealsTable).where(eq(dealsTable.repId, rep.id));
    const repFunded = await db.select({
      total: sql<number>`COALESCE(SUM(${dealsTable.amount}), 0)`,
    }).from(dealsTable).where(eq(dealsTable.repId, rep.id));

    return {
      repId: rep.id,
      repName: rep.fullName,
      totalCalls: repCalls[0].count,
      totalDeals: repDeals[0].count,
      fundedAmount: Number(repFunded[0].total),
    };
  }));

  res.json(GetStatsResponse.parse({
    totalLeads: totalLeads[0].count,
    newLeads: newLeads[0].count,
    contactedLeads: contactedLeads[0].count,
    qualifiedLeads: qualifiedLeads[0].count,
    totalDeals: totalDeals[0].count,
    fundedDeals: fundedDeals[0].count,
    totalFundedAmount: Number(fundedAmountResult[0].total),
    totalCommission: Number(commissionResult[0].total),
    dealsByStage,
    leadsByStatus,
    repPerformance,
  }));
});

router.get("/stats/admin-ops", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);

    const [newLeadsToday] = await db.select({ count: count() }).from(leadsTable)
      .where(gte(leadsTable.createdAt, todayStart));

    const newLeadsList = await db.select({
      id: leadsTable.id,
      businessName: leadsTable.businessName,
      ownerName: leadsTable.ownerName,
      source: leadsTable.source,
      status: leadsTable.status,
      createdAt: leadsTable.createdAt,
      assignedToId: leadsTable.assignedToId,
    }).from(leadsTable)
      .where(gte(leadsTable.createdAt, todayStart))
      .orderBy(desc(leadsTable.createdAt))
      .limit(20);

    const [bankStatementsToday] = await db.select({ count: count() }).from(documentsTable)
      .where(and(
        gte(documentsTable.createdAt, todayStart),
        eq(documentsTable.type, "bank_statement"),
      ));

    const bankStatementDocs = await db.select({
      id: documentsTable.id,
      leadId: documentsTable.leadId,
      name: documentsTable.name,
      mismatch: documentsTable.mismatch,
      classifiedType: documentsTable.classifiedType,
      createdAt: documentsTable.createdAt,
    }).from(documentsTable)
      .where(and(
        gte(documentsTable.createdAt, todayStart),
        eq(documentsTable.type, "bank_statement"),
      ))
      .orderBy(desc(documentsTable.createdAt));

    const matchedStatements = bankStatementDocs.filter(d => d.leadId && !d.mismatch);
    const unmatchedStatements = bankStatementDocs.filter(d => d.mismatch);

    const unmatchedWithLeads = await Promise.all(
      unmatchedStatements.map(async (doc) => {
        const [lead] = await db.select({
          id: leadsTable.id,
          businessName: leadsTable.businessName,
          ownerName: leadsTable.ownerName,
        }).from(leadsTable).where(eq(leadsTable.id, doc.leadId)).limit(1);
        return { ...doc, lead: lead || null };
      })
    );

    const leadsWithBankIssues = await db.select({
      id: leadsTable.id,
      businessName: leadsTable.businessName,
      ownerName: leadsTable.ownerName,
      bankStatementsStatus: leadsTable.bankStatementsStatus,
      bankStatementMonths: leadsTable.bankStatementMonths,
    }).from(leadsTable)
      .where(eq(leadsTable.bankStatementsStatus, "incomplete"));

    const [dealsCreatedToday] = await db.select({ count: count() }).from(dealsTable)
      .where(gte(dealsTable.createdAt, todayStart));

    const dealsSentOut = await db.execute(sql`
      SELECT d.id, d.lead_id, d.rep_id, d.stage, d.amount, d.created_at,
             l.business_name, l.owner_name,
             u.full_name as rep_name
      FROM deals d
      LEFT JOIN leads l ON d.lead_id = l.id
      LEFT JOIN users u ON d.rep_id = u.id
      WHERE d.created_at >= ${todayStart}
      ORDER BY d.created_at DESC
    `);

    const [fundedToday] = await db.select({
      count: count(),
      total: sql<number>`COALESCE(SUM(${dealsTable.amount}), 0)`,
    }).from(dealsTable)
      .where(and(
        eq(dealsTable.stage, "funded"),
        gte(dealsTable.fundedDate, todayStart),
      ));

    const [fundedAllTime] = await db.select({
      count: count(),
      total: sql<number>`COALESCE(SUM(${dealsTable.amount}), 0)`,
    }).from(dealsTable)
      .where(eq(dealsTable.stage, "funded"));

    const managerStats = await db.execute(sql`
      SELECT u.id, u.full_name, u.role,
        (SELECT COUNT(*) FROM leads l WHERE l.assigned_to_id = u.id AND l.created_at >= ${todayStart})::int as leads_assigned_today,
        (SELECT COUNT(*) FROM deals d WHERE d.rep_id = u.id AND d.created_at >= ${todayStart})::int as deals_created_today,
        (SELECT COUNT(*) FROM calls c WHERE c.user_id = u.id AND c.created_at >= ${todayStart})::int as calls_today,
        (SELECT COUNT(*) FROM deals d WHERE d.rep_id = u.id AND d.stage = 'funded')::int as total_funded,
        (SELECT COALESCE(SUM(d.amount), 0) FROM deals d WHERE d.rep_id = u.id AND d.stage = 'funded')::float as funded_volume,
        (SELECT COUNT(DISTINCT l.id) FROM leads l WHERE l.assigned_to_id = u.id AND l.status NOT IN ('funded', 'not_interested'))::int as active_leads
      FROM users u
      WHERE u.role IN ('admin', 'rep')
      ORDER BY u.role ASC, u.full_name ASC
    `);

    const managerRows = (managerStats as any).rows || managerStats;

    const [pendingUnderwriting] = await db.select({ count: count() }).from(dealsTable)
      .where(eq(dealsTable.stage, "underwriting"));

    const [pendingApproval] = await db.select({ count: count() }).from(dealsTable)
      .where(eq(dealsTable.stage, "approved"));

    const recentActivitiesList = await db.select({
      id: activitiesTable.id,
      type: activitiesTable.type,
      description: activitiesTable.description,
      createdAt: activitiesTable.createdAt,
      leadId: activitiesTable.leadId,
      userId: activitiesTable.userId,
    }).from(activitiesTable)
      .where(gte(activitiesTable.createdAt, todayStart))
      .orderBy(desc(activitiesTable.createdAt))
      .limit(15);

    res.json({
      today: {
        newLeads: newLeadsToday.count,
        newLeadsList: newLeadsList,
        bankStatementsReceived: bankStatementsToday.count,
        bankStatementsMatched: matchedStatements.length,
        bankStatementsUnmatched: unmatchedWithLeads,
        leadsWithIncompleteStatements: leadsWithBankIssues,
        dealsCreated: dealsCreatedToday.count,
        dealsSentOut: (dealsSentOut as any).rows || dealsSentOut,
        fundedDeals: fundedToday.count,
        fundedAmount: Number(fundedToday.total),
      },
      allTime: {
        totalFundedDeals: fundedAllTime.count,
        totalFundedAmount: Number(fundedAllTime.total),
      },
      team: managerRows,
      pipeline: {
        pendingUnderwriting: pendingUnderwriting.count,
        pendingApproval: pendingApproval.count,
      },
      recentActivity: recentActivitiesList,
    });
  } catch (e: any) {
    console.error("Admin ops error:", e);
    res.status(500).json({ error: "Failed to fetch admin operations data" });
  }
});

router.get("/stats/manager-hub", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const currentUserId = (req as any).user?.id;
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);

    const [totalLeadsAllTime] = await db.select({ count: count() }).from(leadsTable);
    const [newLeadsToday] = await db.select({ count: count() }).from(leadsTable)
      .where(gte(leadsTable.createdAt, todayStart));

    const [bankStmtsToday] = await db.select({ count: count() }).from(documentsTable)
      .where(and(gte(documentsTable.createdAt, todayStart), eq(documentsTable.type, "bank_statement")));
    const [bankStmtsAll] = await db.select({ count: count() }).from(documentsTable)
      .where(eq(documentsTable.type, "bank_statement"));

    const allBankStmtDocs = await db.select({
      id: documentsTable.id,
      leadId: documentsTable.leadId,
      name: documentsTable.name,
      mismatch: documentsTable.mismatch,
      createdAt: documentsTable.createdAt,
    }).from(documentsTable)
      .where(eq(documentsTable.type, "bank_statement"))
      .orderBy(desc(documentsTable.createdAt));

    const matchedCount = allBankStmtDocs.filter(d => d.leadId && !d.mismatch).length;
    const unmatchedDocs = allBankStmtDocs.filter(d => d.mismatch);

    const unmatchedWithLeads = await Promise.all(
      unmatchedDocs.slice(0, 20).map(async (doc) => {
        const [lead] = await db.select({
          id: leadsTable.id, businessName: leadsTable.businessName, ownerName: leadsTable.ownerName,
        }).from(leadsTable).where(eq(leadsTable.id, doc.leadId)).limit(1);
        return { ...doc, lead: lead || null };
      })
    );

    const leadsIncomplete = await db.select({
      id: leadsTable.id, businessName: leadsTable.businessName, ownerName: leadsTable.ownerName,
      bankStatementsStatus: leadsTable.bankStatementsStatus, bankStatementMonths: leadsTable.bankStatementMonths,
      createdAt: leadsTable.createdAt,
    }).from(leadsTable).where(eq(leadsTable.bankStatementsStatus, "incomplete")).orderBy(desc(leadsTable.createdAt));

    const [analyzedCount] = await db.select({ count: sql<number>`COUNT(DISTINCT ${bankStatementAnalysesTable.leadId})` })
      .from(bankStatementAnalysesTable);

    const [pendingUwCount] = await db.select({ count: count() }).from(underwritingConfirmationsTable)
      .where(eq(underwritingConfirmationsTable.status, "pending"));

    const leadsNeedingUw = await db.execute(sql`
      SELECT DISTINCT ON (l.id) l.id, l.business_name, l.owner_name, l.phone, l.email,
             l.requested_amount, l.monthly_revenue, l.risk_category,
             l.gross_revenue, l.avg_daily_balance, l.revenue_trend,
             l.has_existing_loans, l.loan_count, l.industry, l.status,
             l.bank_statements_status, l.created_at, l.source,
             l.credit_score, l.ssn, l.ein, l.state, l.city,
             (SELECT COUNT(*) FROM underwriting_confirmations uc WHERE uc.lead_id = l.id AND uc.status = 'pending')::int as pending_findings,
             (SELECT COUNT(*) FROM underwriting_confirmations uc WHERE uc.lead_id = l.id)::int as total_findings,
             (SELECT COUNT(*) FROM underwriting_confirmations uc WHERE uc.lead_id = l.id AND uc.status IN ('confirmed', 'rejected', 'relabeled'))::int as reviewed_findings,
             (SELECT COUNT(*) FROM underwriting_confirmations uc WHERE uc.lead_id = l.id AND uc.status = 'confirmed' AND uc.finding_type = 'loan')::int as confirmed_loans,
             bsa.negative_days,
             bsa.nsf_count,
             bsa.risk_score as ai_risk_score
      FROM leads l
      INNER JOIN bank_statement_analyses bsa ON bsa.lead_id = l.id
      WHERE l.assigned_to_id IS NULL
        AND l.status IN ('new', 'underwriting', 'contacted')
      ORDER BY l.id, l.created_at DESC
    `);

    const readyToDistribute = await db.execute(sql`
      SELECT DISTINCT l.id, l.business_name, l.owner_name, l.phone, l.email,
             l.requested_amount, l.monthly_revenue, l.risk_category,
             l.gross_revenue, l.industry, l.status, l.created_at, l.source,
             l.has_existing_loans, l.loan_count
      FROM leads l
      INNER JOIN bank_statement_analyses bsa ON bsa.lead_id = l.id
      WHERE l.assigned_to_id IS NULL
        AND l.status IN ('new', 'underwriting', 'contacted', 'qualified')
        AND NOT EXISTS (
          SELECT 1 FROM underwriting_confirmations uc
          WHERE uc.lead_id = l.id AND uc.status = 'pending'
        )
      ORDER BY l.created_at DESC
    `);

    const reps = await db.select({
      id: usersTable.id,
      fullName: usersTable.fullName,
      role: usersTable.role,
      active: usersTable.active,
    }).from(usersTable)
      .where(and(eq(usersTable.role, "rep"), eq(usersTable.active, true)));

    const repWorkload = await Promise.all(reps.map(async (rep) => {
      const [leadCount] = await db.select({ count: count() }).from(leadsTable)
        .where(and(eq(leadsTable.assignedToId, rep.id), sql`${leadsTable.status} NOT IN ('funded', 'not_interested')`));
      const [dealCount] = await db.select({ count: count() }).from(dealsTable)
        .where(eq(dealsTable.repId, rep.id));
      const [fundedCount] = await db.select({
        count: count(),
        total: sql<number>`COALESCE(SUM(${dealsTable.amount}), 0)`,
      }).from(dealsTable)
        .where(and(eq(dealsTable.repId, rep.id), eq(dealsTable.stage, "funded")));
      return {
        id: rep.id,
        fullName: rep.fullName,
        activeLeads: leadCount.count,
        totalDeals: dealCount.count,
        fundedDeals: fundedCount.count,
        fundedAmount: Number(fundedCount.total),
      };
    }));

    const [fundedAll] = await db.select({
      count: count(),
      total: sql<number>`COALESCE(SUM(${dealsTable.amount}), 0)`,
    }).from(dealsTable).where(eq(dealsTable.stage, "funded"));

    const [fundedToday] = await db.select({
      count: count(),
      total: sql<number>`COALESCE(SUM(${dealsTable.amount}), 0)`,
    }).from(dealsTable).where(and(eq(dealsTable.stage, "funded"), gte(dealsTable.fundedDate, todayStart)));

    const approvedDeals = await db.execute(sql`
      SELECT d.id, d.amount, d.factor_rate, d.term, d.stage, d.created_at, d.funding_source, d.funder_name,
             l.id as lead_id, l.business_name, l.owner_name, l.phone, l.risk_category, l.industry,
             u.full_name as rep_name
      FROM deals d
      JOIN leads l ON l.id = d.lead_id
      LEFT JOIN users u ON u.id = d.rep_id
      WHERE d.stage = 'approved'
      ORDER BY d.created_at DESC
    `);
    const approvedRows = (approvedDeals as any).rows || approvedDeals;

    const [distributedToday] = await db.select({ count: count() }).from(leadsTable)
      .where(and(
        sql`${leadsTable.assignedToId} IS NOT NULL`,
        gte(leadsTable.lastContactedAt, todayStart),
      ));

    const uwRowsRaw = (leadsNeedingUw as any).rows || leadsNeedingUw;
    const uwRowsAll = uwRowsRaw.map((r: any) => ({ ...r, ssn: maskSsn(r.ssn) }));
    const distRows = (readyToDistribute as any).rows || readyToDistribute;

    const ONLINE_THRESHOLD_MS = 3 * 60 * 1000;
    const onlineThreshold = new Date(Date.now() - ONLINE_THRESHOLD_MS);
    const onlineUnderwriters = await db.select({
      id: usersTable.id,
      fullName: usersTable.fullName,
      role: usersTable.role,
    }).from(usersTable)
      .where(and(
        eq(usersTable.active, true),
        eq(usersTable.canUnderwrite, true),
        gte(usersTable.lastSeenAt, onlineThreshold),
      ));

    let uwRows = uwRowsAll;
    if (currentUserId && onlineUnderwriters.length > 1) {
      const sortedIds = onlineUnderwriters.map(u => u.id).sort((a, b) => a - b);
      const myIndex = sortedIds.indexOf(currentUserId);
      if (myIndex >= 0) {
        uwRows = uwRowsAll.filter((_: any, idx: number) => idx % sortedIds.length === myIndex);
      }
    }

    res.json({
      stats: {
        totalLeads: totalLeadsAllTime.count,
        newLeadsToday: newLeadsToday.count,
        bankStatementsToday: bankStmtsToday.count,
        bankStatementsTotal: bankStmtsAll.count,
        matched: matchedCount,
        unmatched: unmatchedDocs.length,
        analyzed: Number(analyzedCount.count),
        pendingFindings: Number(pendingUwCount.count),
        readyToDistribute: distRows.length,
        distributedToday: distributedToday.count,
        fundedTodayCount: fundedToday.count,
        fundedTodayAmount: Number(fundedToday.total),
        fundedAllCount: fundedAll.count,
        fundedAllAmount: Number(fundedAll.total),
        needsUwReview: uwRows.length,
        totalUwQueue: uwRowsAll.length,
        onlineUnderwriters: onlineUnderwriters.length,
      },
      unmatchedStatements: unmatchedWithLeads,
      incompleteStatements: leadsIncomplete.slice(0, 20),
      underwritingQueue: uwRows,
      readyToDistribute: distRows,
      approvedDeals: approvedRows,
      repWorkload: repWorkload.sort((a, b) => a.activeLeads - b.activeLeads),
      onlineUnderwriterNames: onlineUnderwriters.map(u => u.fullName),
    });
  } catch (e: any) {
    console.error("Manager hub error:", e);
    res.status(500).json({ error: "Failed to fetch manager hub data" });
  }
});

router.post("/manager/distribute", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const { leadIds, mode, repId } = req.body;
    if (!leadIds || !Array.isArray(leadIds) || leadIds.length === 0) {
      res.status(400).json({ error: "leadIds array is required" });
      return;
    }
    if (!mode || !["specific", "even", "least_loaded"].includes(mode)) {
      res.status(400).json({ error: "mode must be specific, even, or least_loaded" });
      return;
    }

    const reps = await db.select().from(usersTable)
      .where(and(eq(usersTable.role, "rep"), eq(usersTable.active, true)));

    if (reps.length === 0) {
      res.status(400).json({ error: "No active reps available" });
      return;
    }

    const uniqueIds = [...new Set(leadIds.map(Number).filter(Boolean))];
    const assignments: { leadId: number; repId: number; repName: string; businessName: string }[] = [];

    if (mode === "specific") {
      if (!repId) { res.status(400).json({ error: "repId required for specific mode" }); return; }
      const [rep] = await db.select().from(usersTable)
        .where(and(eq(usersTable.id, repId), eq(usersTable.active, true)));
      if (!rep) { res.status(400).json({ error: "Rep not found" }); return; }

      for (const lid of uniqueIds) {
        const [lead] = await db.select({ businessName: leadsTable.businessName }).from(leadsTable).where(eq(leadsTable.id, lid));
        if (!lead) continue;
        await db.update(leadsTable).set({ assignedToId: repId, status: "contacted", lastContactedAt: new Date() }).where(eq(leadsTable.id, lid));
        assignments.push({ leadId: lid, repId, repName: rep.fullName, businessName: lead.businessName });
      }
    } else if (mode === "even") {
      let repIdx = 0;
      for (const lid of uniqueIds) {
        const rep = reps[repIdx % reps.length];
        const [lead] = await db.select({ businessName: leadsTable.businessName }).from(leadsTable).where(eq(leadsTable.id, lid));
        if (!lead) continue;
        await db.update(leadsTable).set({ assignedToId: rep.id, status: "contacted", lastContactedAt: new Date() }).where(eq(leadsTable.id, lid));
        assignments.push({ leadId: lid, repId: rep.id, repName: rep.fullName, businessName: lead.businessName });
        repIdx++;
      }
    } else {
      const repCounts = await Promise.all(reps.map(async (rep) => {
        const [c] = await db.select({ count: count() }).from(leadsTable)
          .where(and(eq(leadsTable.assignedToId, rep.id), sql`${leadsTable.status} NOT IN ('funded', 'not_interested')`));
        return { ...rep, leadCount: c.count };
      }));
      repCounts.sort((a, b) => a.leadCount - b.leadCount);

      for (const lid of uniqueIds) {
        const rep = repCounts[0];
        const [lead] = await db.select({ businessName: leadsTable.businessName }).from(leadsTable).where(eq(leadsTable.id, lid));
        if (!lead) continue;
        await db.update(leadsTable).set({ assignedToId: rep.id, status: "contacted", lastContactedAt: new Date() }).where(eq(leadsTable.id, lid));
        assignments.push({ leadId: lid, repId: rep.id, repName: rep.fullName, businessName: lead.businessName });
        rep.leadCount++;
        repCounts.sort((a, b) => a.leadCount - b.leadCount);
      }
    }

    const repGroups = new Map<number, { name: string; leads: string[] }>();
    for (const a of assignments) {
      if (!repGroups.has(a.repId)) repGroups.set(a.repId, { name: a.repName, leads: [] });
      repGroups.get(a.repId)!.leads.push(a.businessName);
    }
    for (const [rId, group] of repGroups) {
      await db.insert(notificationsTable).values({
        userId: rId,
        type: "leads_assigned",
        title: `${group.leads.length} New Lead${group.leads.length > 1 ? "s" : ""} Assigned!`,
        message: group.leads.length <= 3
          ? `You got: ${group.leads.join(", ")}. Time to close!`
          : `${group.leads.length} hot leads just dropped in your queue. Let's get it!`,
        link: "/leads",
      });
    }

    res.json({ distributed: assignments.length, assignments });
  } catch (e: any) {
    console.error("Distribute error:", e);
    res.status(500).json({ error: "Failed to distribute leads" });
  }
});

router.get("/manager/rep-config", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    const reps = await db.select({
      id: usersTable.id, fullName: usersTable.fullName, email: usersTable.email,
      phone: usersTable.phone, active: usersTable.active, role: usersTable.role,
      repTier: usersTable.repTier, riskPreference: usersTable.riskPreference,
      minDealAmount: usersTable.minDealAmount, maxDealAmount: usersTable.maxDealAmount,
      googleSheetUrl: usersTable.googleSheetUrl, googleSheetTab: usersTable.googleSheetTab,
      autoAssignEnabled: usersTable.autoAssignEnabled, commissionPct: usersTable.commissionPct,
      lastSeenAt: usersTable.lastSeenAt,
    }).from(usersTable).where(eq(usersTable.role, "rep"));
    res.json(reps);
  } catch (e: any) { res.status(500).json({ error: e.message }); }
});

router.patch("/manager/rep-config/:id", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const repId = parseInt(req.params.id);
    const { repTier, riskPreference, minDealAmount, maxDealAmount, googleSheetUrl, googleSheetTab, autoAssignEnabled } = req.body;

    const [rep] = await db.select().from(usersTable).where(and(eq(usersTable.id, repId), eq(usersTable.role, "rep")));
    if (!rep) { res.status(404).json({ error: "Rep not found" }); return; }

    const validTiers = ["top", "mid", "standard"];
    const validRiskPrefs = ["any", "low", "high"];

    const updates: Record<string, any> = {};
    if (repTier !== undefined) {
      if (!validTiers.includes(repTier)) { res.status(400).json({ error: "repTier must be top, mid, or standard" }); return; }
      updates.repTier = repTier;
    }
    if (riskPreference !== undefined) {
      if (!validRiskPrefs.includes(riskPreference)) { res.status(400).json({ error: "riskPreference must be any, low, or high" }); return; }
      updates.riskPreference = riskPreference;
    }
    if (minDealAmount !== undefined) {
      const val = minDealAmount === null || minDealAmount === "" ? null : Number(minDealAmount);
      if (val !== null && (isNaN(val) || val < 0)) { res.status(400).json({ error: "Invalid min deal amount" }); return; }
      updates.minDealAmount = val;
    }
    if (maxDealAmount !== undefined) {
      const val = maxDealAmount === null || maxDealAmount === "" ? null : Number(maxDealAmount);
      if (val !== null && (isNaN(val) || val < 0)) { res.status(400).json({ error: "Invalid max deal amount" }); return; }
      updates.maxDealAmount = val;
    }
    if (updates.minDealAmount != null && updates.maxDealAmount != null && updates.minDealAmount > updates.maxDealAmount) {
      res.status(400).json({ error: "Min deal amount cannot exceed max deal amount" }); return;
    }
    if (googleSheetUrl !== undefined) updates.googleSheetUrl = googleSheetUrl || null;
    if (googleSheetTab !== undefined) updates.googleSheetTab = googleSheetTab || null;
    if (autoAssignEnabled !== undefined) updates.autoAssignEnabled = !!autoAssignEnabled;

    if (Object.keys(updates).length === 0) { res.status(400).json({ error: "No fields to update" }); return; }

    const [updated] = await db.update(usersTable).set(updates).where(eq(usersTable.id, repId)).returning();
    res.json({ success: true, rep: { id: updated.id, fullName: updated.fullName, repTier: updated.repTier, riskPreference: updated.riskPreference, minDealAmount: updated.minDealAmount, maxDealAmount: updated.maxDealAmount, googleSheetUrl: updated.googleSheetUrl, googleSheetTab: updated.googleSheetTab, autoAssignEnabled: updated.autoAssignEnabled } });
  } catch (e: any) { res.status(500).json({ error: e.message }); }
});

router.get("/manager/rep-config/:id/stats", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const repId = parseInt(req.params.id);
    const [rep] = await db.select().from(usersTable).where(and(eq(usersTable.id, repId), eq(usersTable.role, "rep")));
    if (!rep) { res.status(404).json({ error: "Rep not found" }); return; }

    const [activeLeads] = await db.select({ count: sql<number>`count(*)::int` }).from(leadsTable)
      .where(and(eq(leadsTable.assignedToId, repId), sql`${leadsTable.status} NOT IN ('funded', 'not_interested', 'dead')`));
    const [totalLeads] = await db.select({ count: sql<number>`count(*)::int` }).from(leadsTable)
      .where(eq(leadsTable.assignedToId, repId));
    const [fundedDeals] = await db.select({ count: sql<number>`count(*)::int`, total: sql<number>`COALESCE(SUM(amount), 0)::float` }).from(dealsTable)
      .where(and(eq(dealsTable.repId, repId), eq(dealsTable.stage, "funded")));
    const [allDeals] = await db.select({ count: sql<number>`count(*)::int` }).from(dealsTable)
      .where(eq(dealsTable.repId, repId));

    res.json({
      activeLeads: activeLeads.count,
      totalLeads: totalLeads.count,
      fundedDeals: fundedDeals.count,
      totalFunded: fundedDeals.total,
      totalDeals: allDeals.count,
    });
  } catch (e: any) { res.status(500).json({ error: e.message }); }
});

router.post("/manager/rep-config", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const { fullName, email, phone, password } = req.body;
    if (!fullName || !email) { res.status(400).json({ error: "Full name and email are required" }); return; }

    const existing = await db.select({ id: usersTable.id }).from(usersTable).where(eq(usersTable.email, email));
    if (existing.length > 0) { res.status(400).json({ error: "A user with this email already exists" }); return; }

    const bcrypt = await import("bcryptjs");
    const hashedPassword = await bcrypt.hash(password || "changeme", 10);

    const [newRep] = await db.insert(usersTable).values({
      fullName,
      email,
      phone: phone || null,
      password: hashedPassword,
      role: "rep",
      active: true,
      repTier: "standard",
      riskPreference: "any",
      autoAssignEnabled: true,
      tempPassword: !password,
    }).returning();

    res.json({
      success: true,
      rep: { id: newRep.id, fullName: newRep.fullName, email: newRep.email, phone: newRep.phone,
        repTier: newRep.repTier, riskPreference: newRep.riskPreference, autoAssignEnabled: newRep.autoAssignEnabled,
        minDealAmount: newRep.minDealAmount, maxDealAmount: newRep.maxDealAmount,
        googleSheetUrl: newRep.googleSheetUrl, googleSheetTab: newRep.googleSheetTab,
        active: newRep.active, role: newRep.role }
    });
  } catch (e: any) { res.status(500).json({ error: e.message }); }
});

router.get("/stats/lead-sources", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    const sourceStats = await db.execute(sql`
      SELECT
        ls.source,
        ls.total_leads,
        ls.new_count,
        ls.contacted_count,
        ls.qualified_count,
        ls.funded_count,
        ls.in_pipeline,
        ls.first_lead_at,
        ls.latest_lead_at,
        COALESCE(ds.total_deals, 0)::int as total_deals,
        COALESCE(ds.funded_deals, 0)::int as funded_deals,
        COALESCE(ds.funded_amount, 0)::float as funded_amount
      FROM (
        SELECT
          COALESCE(l.source, 'manual') as source,
          COUNT(*)::int as total_leads,
          COUNT(CASE WHEN l.status = 'new' THEN 1 END)::int as new_count,
          COUNT(CASE WHEN l.status = 'contacted' THEN 1 END)::int as contacted_count,
          COUNT(CASE WHEN l.status = 'qualified' THEN 1 END)::int as qualified_count,
          COUNT(CASE WHEN l.status = 'funded' THEN 1 END)::int as funded_count,
          COUNT(CASE WHEN l.status NOT IN ('funded', 'not_interested', 'dead') THEN 1 END)::int as in_pipeline,
          MIN(l.created_at) as first_lead_at,
          MAX(l.created_at) as latest_lead_at
        FROM leads l
        GROUP BY COALESCE(l.source, 'manual')
      ) ls
      LEFT JOIN (
        SELECT
          COALESCE(l2.source, 'manual') as source,
          COUNT(d.id)::int as total_deals,
          COUNT(CASE WHEN d.stage = 'funded' THEN 1 END)::int as funded_deals,
          COALESCE(SUM(CASE WHEN d.stage = 'funded' THEN d.amount ELSE 0 END), 0)::float as funded_amount
        FROM deals d
        JOIN leads l2 ON l2.id = d.lead_id
        GROUP BY COALESCE(l2.source, 'manual')
      ) ds ON ds.source = ls.source
      ORDER BY ls.total_leads DESC
    `);

    const rows = (sourceStats as any).rows || sourceStats;

    const sources = rows.map((r: any) => ({
      source: r.source,
      totalLeads: r.total_leads,
      newCount: r.new_count,
      contactedCount: r.contacted_count,
      qualifiedCount: r.qualified_count,
      fundedCount: r.funded_count,
      inPipeline: r.in_pipeline,
      totalDeals: r.total_deals,
      fundedDeals: r.funded_deals,
      fundedAmount: Number(r.funded_amount),
      conversionRate: r.total_leads > 0 ? ((r.funded_count / r.total_leads) * 100) : 0,
      firstLeadAt: r.first_lead_at,
      latestLeadAt: r.latest_lead_at,
    }));

    const totalLeads = sources.reduce((s: number, r: any) => s + r.totalLeads, 0);
    const totalFunded = sources.reduce((s: number, r: any) => s + r.fundedAmount, 0);
    const topSource = sources.length > 0 ? sources[0].source : null;

    res.json({ sources, totalLeads, totalFunded, topSource });
  } catch (e: any) {
    console.error("Lead sources error:", e);
    res.status(500).json({ error: "Failed to fetch lead source stats" });
  }
});

router.get("/stats/follow-up-alerts", requireAuth, async (req, res): Promise<void> => {
  try {
    const user = (req as any).user;
    const isManager = user.role === "admin" || user.role === "super_admin";

    const staleLeads = await db.select({
      id: leadsTable.id,
      businessName: leadsTable.businessName,
      ownerName: leadsTable.ownerName,
      status: leadsTable.status,
      lastContactedAt: leadsTable.lastContactedAt,
      createdAt: leadsTable.createdAt,
      phone: leadsTable.phone,
    }).from(leadsTable)
      .where(and(
        ...(!isManager ? [eq(leadsTable.assignedToId, user.id)] : []),
        sql`${leadsTable.status} IN ('new', 'contacted', 'callback', 'qualified')`,
        sql`(${leadsTable.lastContactedAt} IS NULL AND ${leadsTable.createdAt} < NOW() - INTERVAL '2 days')
            OR (${leadsTable.lastContactedAt} < NOW() - INTERVAL '3 days')`
      ))
      .orderBy(sql`COALESCE(${leadsTable.lastContactedAt}, ${leadsTable.createdAt}) ASC`)
      .limit(50);

    const staleDealRows = await db.select({
      dealId: dealsTable.id,
      leadId: dealsTable.leadId,
      businessName: leadsTable.businessName,
      stage: dealsTable.stage,
      amount: dealsTable.amount,
      updatedAt: dealsTable.updatedAt,
    }).from(dealsTable)
      .leftJoin(leadsTable, eq(dealsTable.leadId, leadsTable.id))
      .where(and(
        ...(!isManager ? [eq(dealsTable.repId, user.id)] : []),
        sql`${dealsTable.stage} IN ('prospect', 'application', 'underwriting', 'approved')`,
        sql`${dealsTable.updatedAt} < NOW() - INTERVAL '5 days'`
      ))
      .orderBy(sql`${dealsTable.updatedAt} ASC`)
      .limit(30);

    const callbacksDue = await db.select({
      id: callsTable.id,
      leadId: callsTable.leadId,
      callbackAt: callsTable.callbackAt,
      notes: callsTable.notes,
      leadName: leadsTable.businessName,
    }).from(callsTable)
      .leftJoin(leadsTable, eq(callsTable.leadId, leadsTable.id))
      .where(and(
        eq(callsTable.userId, user.id),
        sql`${callsTable.callbackAt} IS NOT NULL`,
        sql`${callsTable.callbackAt} <= NOW() + INTERVAL '1 day'`,
        sql`${callsTable.callbackAt} >= NOW() - INTERVAL '2 days'`
      ))
      .orderBy(sql`${callsTable.callbackAt} ASC`)
      .limit(20);

    const alerts = [
      ...staleLeads.map(l => ({
        type: "stale_lead" as const,
        priority: l.status === "qualified" ? "high" as const : l.status === "callback" ? "high" as const : "medium" as const,
        title: `Follow up: ${l.businessName}`,
        message: l.lastContactedAt
          ? `Last contacted ${Math.floor((Date.now() - new Date(l.lastContactedAt).getTime()) / 86400000)} days ago`
          : `New lead from ${Math.floor((Date.now() - new Date(l.createdAt!).getTime()) / 86400000)} days ago — never contacted`,
        leadId: l.id,
        data: l,
      })),
      ...staleDealRows.map(d => ({
        type: "stale_deal" as const,
        priority: d.stage === "approved" ? "high" as const : "medium" as const,
        title: `Deal stalled: ${d.businessName}`,
        message: `${d.stage} stage — no activity for ${Math.floor((Date.now() - new Date(d.updatedAt!).getTime()) / 86400000)} days`,
        leadId: d.leadId,
        data: d,
      })),
      ...callbacksDue.map(c => ({
        type: "callback_due" as const,
        priority: new Date(c.callbackAt!).getTime() < Date.now() ? "high" as const : "medium" as const,
        title: `Callback: ${c.leadName}`,
        message: new Date(c.callbackAt!).getTime() < Date.now()
          ? `Overdue callback from ${new Date(c.callbackAt!).toLocaleDateString()}`
          : `Callback scheduled for ${new Date(c.callbackAt!).toLocaleString()}`,
        leadId: c.leadId,
        data: c,
      })),
    ];

    alerts.sort((a, b) => {
      const pri = { high: 0, medium: 1, low: 2 };
      return (pri[a.priority] ?? 1) - (pri[b.priority] ?? 1);
    });

    res.json({
      total: alerts.length,
      highPriority: alerts.filter(a => a.priority === "high").length,
      alerts,
    });
  } catch (e: any) {
    console.error("Follow-up alerts error:", e);
    res.status(500).json({ error: "Failed to fetch follow-up alerts" });
  }
});

router.get("/stats/dashboard", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    const ONLINE_MS = 3 * 60 * 1000;
    const STALE_DAYS = 7;
    const onlineThreshold = new Date(Date.now() - ONLINE_MS);
    const staleThreshold = new Date(Date.now() - STALE_DAYS * 24 * 60 * 60 * 1000);
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);
    const weekStart = new Date();
    weekStart.setDate(weekStart.getDate() - 7);

    const allReps = await db.select({
      id: usersTable.id,
      fullName: usersTable.fullName,
      role: usersTable.role,
      active: usersTable.active,
      lastSeenAt: usersTable.lastSeenAt,
      repTier: usersTable.repTier,
      riskPreference: usersTable.riskPreference,
      autoAssignEnabled: usersTable.autoAssignEnabled,
    }).from(usersTable)
      .where(and(eq(usersTable.role, "rep"), eq(usersTable.active, true)));

    const repDetails = await Promise.all(allReps.map(async (rep) => {
      const [activeLeads] = await db.select({ count: count() }).from(leadsTable)
        .where(and(eq(leadsTable.assignedToId, rep.id), sql`${leadsTable.status} NOT IN ('funded', 'not_interested', 'dead')`));
      const [fundedDeals] = await db.select({
        count: count(),
        total: sql<number>`COALESCE(SUM(${dealsTable.amount}), 0)`,
      }).from(dealsTable)
        .where(and(eq(dealsTable.repId, rep.id), eq(dealsTable.stage, "funded")));
      const [totalDeals] = await db.select({ count: count() }).from(dealsTable)
        .where(eq(dealsTable.repId, rep.id));
      const [appsThisWeek] = await db.select({ count: count() }).from(leadsTable)
        .where(and(eq(leadsTable.assignedToId, rep.id), gte(leadsTable.createdAt, weekStart)));
      const [staleLeads] = await db.select({ count: count() }).from(leadsTable)
        .where(and(
          eq(leadsTable.assignedToId, rep.id),
          sql`${leadsTable.status} NOT IN ('funded', 'not_interested', 'dead')`,
          sql`COALESCE(${leadsTable.lastContactedAt}, ${leadsTable.createdAt}) < ${staleThreshold}`,
        ));

      const lastActivity = await db.select({ createdAt: activitiesTable.createdAt })
        .from(activitiesTable)
        .where(eq(activitiesTable.userId, rep.id))
        .orderBy(desc(activitiesTable.createdAt))
        .limit(1);

      const isOnline = rep.lastSeenAt ? rep.lastSeenAt >= onlineThreshold : false;

      return {
        id: rep.id,
        fullName: rep.fullName,
        tier: rep.repTier || "standard",
        riskPreference: rep.riskPreference || "any",
        autoAssign: rep.autoAssignEnabled,
        isOnline,
        lastSeenAt: rep.lastSeenAt,
        lastActivityAt: lastActivity[0]?.createdAt || null,
        activeLeads: Number(activeLeads.count),
        totalDeals: Number(totalDeals.count),
        fundedDeals: Number(fundedDeals.count),
        fundedAmount: Number(fundedDeals.total),
        appsThisWeek: Number(appsThisWeek.count),
        staleLeads: Number(staleLeads.count),
      };
    }));

    const [totalLeads] = await db.select({ count: count() }).from(leadsTable);
    const [newLeadsToday] = await db.select({ count: count() }).from(leadsTable)
      .where(gte(leadsTable.createdAt, todayStart));
    const [newLeadsWeek] = await db.select({ count: count() }).from(leadsTable)
      .where(gte(leadsTable.createdAt, weekStart));
    const [pendingUw] = await db.select({ count: count() }).from(underwritingConfirmationsTable)
      .where(eq(underwritingConfirmationsTable.status, "pending"));
    const leadsAwaitingUw = await db.select({ count: count() }).from(leadsTable)
      .where(and(
        sql`${leadsTable.status} IN ('new', 'underwriting', 'contacted')`,
        isNull(leadsTable.assignedToId),
        sql`EXISTS (SELECT 1 FROM bank_statement_analyses bsa WHERE bsa.lead_id = ${leadsTable.id})`,
      ));

    const [fundedToday] = await db.select({
      count: count(),
      total: sql<number>`COALESCE(SUM(${dealsTable.amount}), 0)`,
    }).from(dealsTable).where(and(eq(dealsTable.stage, "funded"), gte(dealsTable.fundedDate, todayStart)));
    const [fundedWeek] = await db.select({
      count: count(),
      total: sql<number>`COALESCE(SUM(${dealsTable.amount}), 0)`,
    }).from(dealsTable).where(and(eq(dealsTable.stage, "funded"), gte(dealsTable.fundedDate, weekStart)));
    const [fundedAll] = await db.select({
      count: count(),
      total: sql<number>`COALESCE(SUM(${dealsTable.amount}), 0)`,
    }).from(dealsTable).where(eq(dealsTable.stage, "funded"));
    const [approvedDeals] = await db.select({ count: count() }).from(dealsTable)
      .where(eq(dealsTable.stage, "approved"));
    const [defaultedDeals] = await db.select({
      count: count(),
      total: sql<number>`COALESCE(SUM(${dealsTable.amount}), 0)`,
    }).from(dealsTable).where(sql`${dealsTable.defaultStatus} IS NOT NULL`);

    const [unassignedLeads] = await db.select({ count: count() }).from(leadsTable)
      .where(and(
        isNull(leadsTable.assignedToId),
        sql`${leadsTable.status} NOT IN ('funded', 'not_interested', 'dead')`,
      ));

    const alerts: Array<{ type: string; severity: string; message: string; repId?: number; repName?: string; count?: number }> = [];

    for (const rep of repDetails) {
      if (rep.staleLeads > 0) {
        alerts.push({
          type: "stale_leads",
          severity: rep.staleLeads >= 5 ? "critical" : "warning",
          message: `${rep.fullName} has ${rep.staleLeads} lead${rep.staleLeads > 1 ? "s" : ""} with no activity for ${STALE_DAYS}+ days`,
          repId: rep.id,
          repName: rep.fullName,
          count: rep.staleLeads,
        });
      }
      if (!rep.isOnline && rep.activeLeads > 3) {
        alerts.push({
          type: "offline_with_leads",
          severity: "warning",
          message: `${rep.fullName} is offline with ${rep.activeLeads} active leads`,
          repId: rep.id,
          repName: rep.fullName,
          count: rep.activeLeads,
        });
      }
    }

    if (Number(leadsAwaitingUw[0].count) > 0) {
      alerts.push({
        type: "pending_underwriting",
        severity: Number(leadsAwaitingUw[0].count) > 5 ? "critical" : "info",
        message: `${leadsAwaitingUw[0].count} lead${Number(leadsAwaitingUw[0].count) > 1 ? "s" : ""} waiting for underwriting review`,
        count: Number(leadsAwaitingUw[0].count),
      });
    }

    if (Number(approvedDeals.count) > 0) {
      alerts.push({
        type: "ready_to_fund",
        severity: "info",
        message: `${approvedDeals.count} approved deal${Number(approvedDeals.count) > 1 ? "s" : ""} ready to fund`,
        count: Number(approvedDeals.count),
      });
    }

    if (Number(unassignedLeads.count) > 5) {
      alerts.push({
        type: "unassigned_leads",
        severity: "warning",
        message: `${unassignedLeads.count} leads not assigned to any closer`,
        count: Number(unassignedLeads.count),
      });
    }

    alerts.sort((a, b) => {
      const sev = { critical: 0, warning: 1, info: 2 };
      return (sev[a.severity as keyof typeof sev] ?? 3) - (sev[b.severity as keyof typeof sev] ?? 3);
    });

    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

    const fundingTrend = await db.execute(sql`
      SELECT DATE(funded_date) as day,
             COUNT(*)::int as deals,
             COALESCE(SUM(amount), 0)::float as amount
      FROM deals
      WHERE stage = 'funded' AND funded_date >= ${thirtyDaysAgo}
      GROUP BY DATE(funded_date)
      ORDER BY day ASC
    `);
    const fundingTrendRows = ((fundingTrend as any).rows || fundingTrend).map((r: any) => ({
      day: r.day, deals: r.deals, amount: Number(r.amount),
    }));

    const pipelineBreakdown = await db.execute(sql`
      SELECT status, COUNT(*)::int as count
      FROM leads
      WHERE status NOT IN ('dead')
      GROUP BY status
      ORDER BY count DESC
    `);
    const pipelineRows = ((pipelineBreakdown as any).rows || pipelineBreakdown).map((r: any) => ({
      status: r.status, count: r.count,
    }));

    const dealStages = await db.execute(sql`
      SELECT stage, COUNT(*)::int as count, COALESCE(SUM(amount), 0)::float as amount
      FROM deals
      GROUP BY stage
      ORDER BY count DESC
    `);
    const dealStageRows = ((dealStages as any).rows || dealStages).map((r: any) => ({
      stage: r.stage, count: r.count, amount: Number(r.amount),
    }));

    const leadsBySource = await db.execute(sql`
      SELECT COALESCE(source, 'manual') as source, COUNT(*)::int as count
      FROM leads
      GROUP BY COALESCE(source, 'manual')
      ORDER BY count DESC
      LIMIT 8
    `);
    const sourceRows = ((leadsBySource as any).rows || leadsBySource).map((r: any) => ({
      source: r.source, count: r.count,
    }));

    const newLeadsLast30 = await db.execute(sql`
      SELECT DATE(created_at) as day, COUNT(*)::int as count
      FROM leads
      WHERE created_at >= ${thirtyDaysAgo}
      GROUP BY DATE(created_at)
      ORDER BY day ASC
    `);
    const newLeadsTrendRows = ((newLeadsLast30 as any).rows || newLeadsLast30).map((r: any) => ({
      day: r.day, count: r.count,
    }));

    const [monthFunded] = await db.select({
      count: count(),
      total: sql<number>`COALESCE(SUM(${dealsTable.amount}), 0)`,
    }).from(dealsTable).where(and(eq(dealsTable.stage, "funded"), gte(dealsTable.fundedDate, thirtyDaysAgo)));

    const [totalActiveLeads] = await db.select({ count: count() }).from(leadsTable)
      .where(sql`${leadsTable.status} NOT IN ('funded', 'not_interested', 'dead')`);

    const [contactedLeads] = await db.select({ count: count() }).from(leadsTable)
      .where(eq(leadsTable.status, "contacted"));
    const [qualifiedLeads] = await db.select({ count: count() }).from(leadsTable)
      .where(eq(leadsTable.status, "qualified"));

    res.json({
      kpis: {
        totalLeads: Number(totalLeads.count),
        newLeadsToday: Number(newLeadsToday.count),
        newLeadsWeek: Number(newLeadsWeek.count),
        pendingUnderwriting: Number(pendingUw.count),
        awaitingUwReview: Number(leadsAwaitingUw[0].count),
        approvedDeals: Number(approvedDeals.count),
        unassignedLeads: Number(unassignedLeads.count),
        activeLeads: Number(totalActiveLeads.count),
        contactedLeads: Number(contactedLeads.count),
        qualifiedLeads: Number(qualifiedLeads.count),
        fundedToday: { count: Number(fundedToday.count), amount: Number(fundedToday.total) },
        fundedWeek: { count: Number(fundedWeek.count), amount: Number(fundedWeek.total) },
        fundedMonth: { count: Number(monthFunded.count), amount: Number(monthFunded.total) },
        fundedAllTime: { count: Number(fundedAll.count), amount: Number(fundedAll.total) },
        defaults: { count: Number(defaultedDeals.count), amount: Number(defaultedDeals.total) },
      },
      charts: {
        fundingTrend: fundingTrendRows,
        newLeadsTrend: newLeadsTrendRows,
        pipeline: pipelineRows,
        dealStages: dealStageRows,
        leadSources: sourceRows,
      },
      reps: repDetails.sort((a, b) => {
        if (a.isOnline !== b.isOnline) return a.isOnline ? -1 : 1;
        return b.fundedAmount - a.fundedAmount;
      }),
      alerts,
      onlineReps: repDetails.filter(r => r.isOnline).length,
      totalReps: repDetails.length,
    });
  } catch (e: any) {
    console.error("Dashboard error:", e);
    res.status(500).json({ error: "Failed to fetch dashboard data" });
  }
});

export default router;
