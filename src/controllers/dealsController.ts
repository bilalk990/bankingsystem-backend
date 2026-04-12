import { Router, type IRouter } from "express";
import { and, eq, sql, desc, or, isNotNull, inArray } from "drizzle-orm";
import {
  db,
  dealsTable,
  leadsTable,
  usersTable,
  activitiesTable,
  commissionsTable,
  fundersTable,
} from "../configs/database";
import { requireAuth, requireAdmin, checkDealOwnership, checkLeadOwnership } from "../middlewares/authMiddleware";
import {
  CreateDealBody,
  UpdateDealBody,
  UpdateDealParams,
  GetDealsQueryParams,
  GetDealsResponse,
  UpdateDealResponse,
} from "../validationSchemas";
import { toNum, roundToTwo } from "../utils/math";

const router: IRouter = Router();

router.get("/deals", requireAuth, async (req, res): Promise<void> => {
  const query = GetDealsQueryParams.safeParse(req.query);
  const user = (req as any).user;

  let conditions: any[] = [];
  if (query.success && query.data.stage) {
    conditions.push(eq(dealsTable.stage, query.data.stage));
  }
  if (query.success && query.data.repId) {
    conditions.push(eq(dealsTable.repId, query.data.repId));
  }
  if (user.role === "rep") {
    conditions.push(eq(dealsTable.repId, user.id));
  }

  const whereClause = conditions.length > 0 ? and(...conditions) : undefined;

  const rawDeals = await db.select({
    id: dealsTable.id,
    leadId: dealsTable.leadId,
    businessName: leadsTable.businessName,
    repId: dealsTable.repId,
    repName: usersTable.fullName,
    stage: dealsTable.stage,
    amount: dealsTable.amount,
    factorRate: dealsTable.factorRate,
    paybackAmount: dealsTable.paybackAmount,
    term: dealsTable.term,
    commission: dealsTable.commission,
    funderId: dealsTable.funderId,
    fundedDate: dealsTable.fundedDate,
    paymentFrequency: dealsTable.paymentFrequency,
    paymentAmount: dealsTable.paymentAmount,
    totalPayments: dealsTable.totalPayments,
    paymentsCompleted: dealsTable.paymentsCompleted,
    renewalEligibleDate: dealsTable.renewalEligibleDate,
    notes: dealsTable.notes,
    createdAt: dealsTable.createdAt,
    updatedAt: dealsTable.updatedAt,
    grossRevenue: leadsTable.grossRevenue,
    loanCount: leadsTable.loanCount,
    hasExistingLoans: leadsTable.hasExistingLoans,
    riskCategory: leadsTable.riskCategory,
    avgDailyBalance: leadsTable.avgDailyBalance,
    loanDetails: leadsTable.loanDetails,
    revenueTrend: leadsTable.revenueTrend,
    creditScore: leadsTable.creditScore,
    bankStatementsStatus: leadsTable.bankStatementsStatus,
    bankStatementMonths: leadsTable.bankStatementMonths,
    ownerName: leadsTable.ownerName,
    phone: leadsTable.phone,
    email: leadsTable.email,
    monthlyRevenue: leadsTable.monthlyRevenue,
    industry: leadsTable.industry,
    status: leadsTable.status,
    isHot: leadsTable.isHot,
    lastContactedAt: leadsTable.lastContactedAt,
    requestedAmount: leadsTable.requestedAmount,
  }).from(dealsTable)
    .leftJoin(leadsTable, eq(dealsTable.leadId, leadsTable.id))
    .leftJoin(usersTable, eq(dealsTable.repId, usersTable.id))
    .where(whereClause)
    .orderBy(dealsTable.createdAt);

  const funderIds = [...new Set(rawDeals.filter(d => d.funderId).map(d => d.funderId!))];
  let funderMap: Record<number, string> = {};
  if (funderIds.length > 0) {
    const funders = await db.select({ id: fundersTable.id, name: fundersTable.name }).from(fundersTable);
    funderMap = Object.fromEntries(funders.map(f => [f.id, f.name]));
  }

  const deals = rawDeals.map(d => ({
    ...d,
    funderName: d.funderId ? funderMap[d.funderId] || null : null,
  }));

  res.json(deals);
});

router.get("/deals/defaults", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  const defaults = await db.select({
    id: dealsTable.id,
    leadId: dealsTable.leadId,
    businessName: leadsTable.businessName,
    ownerName: leadsTable.ownerName,
    phone: leadsTable.phone,
    email: leadsTable.email,
    repName: usersTable.fullName,
    amount: dealsTable.amount,
    paybackAmount: dealsTable.paybackAmount,
    paymentsCompleted: dealsTable.paymentsCompleted,
    totalPayments: dealsTable.totalPayments,
    paymentAmount: dealsTable.paymentAmount,
    fundedDate: dealsTable.fundedDate,
    defaultStatus: dealsTable.defaultStatus,
    defaultedAt: dealsTable.defaultedAt,
    defaultNotes: dealsTable.defaultNotes,
    defaultAmount: dealsTable.defaultAmount,
    fundingSource: dealsTable.fundingSource,
    funderName: dealsTable.funderName,
    factorRate: dealsTable.factorRate,
  }).from(dealsTable)
    .leftJoin(leadsTable, eq(dealsTable.leadId, leadsTable.id))
    .leftJoin(usersTable, eq(dealsTable.repId, usersTable.id))
    .where(eq(dealsTable.defaultStatus, "defaulted"))
    .orderBy(dealsTable.defaultedAt);

  res.json(defaults);
});

router.post("/deals", requireAuth, async (req, res): Promise<void> => {
  const parsed = CreateDealBody.safeParse(req.body);
  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.message });
    return;
  }

  const user = (req as any).user;
  if (!(await checkLeadOwnership(parsed.data.leadId, user, res))) return;

  const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, parsed.data.leadId));
  if (!lead) {
    res.status(404).json({ error: "Lead not found" });
    return;
  }

  const [deal] = await db.insert(dealsTable).values({
    leadId: parsed.data.leadId,
    repId: user.id,
    amount: String(roundToTwo(parsed.data.amount || lead.requestedAmount)),
    factorRate: parsed.data.factorRate ? String(roundToTwo(parsed.data.factorRate)) : null,
    term: parsed.data.term || (lead as any).term || null,
    notes: parsed.data.notes || lead.notes || null,
  }).returning();

  await db.insert(activitiesTable).values({
    leadId: parsed.data.leadId,
    userId: user.id,
    type: "deal_created",
    description: `New deal created for $${parsed.data.amount.toLocaleString()}`,
    metadata: { dealId: deal.id, amount: parsed.data.amount },
  });

  res.status(201).json({
    ...deal,
    businessName: lead.businessName,
    repName: user.fullName,
  });
});

router.patch("/deals/:id", requireAuth, async (req, res): Promise<void> => {
  const params = UpdateDealParams.safeParse(req.params);
  if (!params.success) {
    res.status(400).json({ error: params.error.message });
    return;
  }

  const parsed = UpdateDealBody.safeParse(req.body);
  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.message });
    return;
  }

  const updateData: any = {};
  const zod = parsed.data as any;
  const raw = req.body;
  for (const field of ["stage", "amount", "factorRate", "paybackAmount", "term", "commission", "notes"]) {
    if (zod[field] !== undefined) {
      if (["amount", "factorRate", "paybackAmount", "commission"].includes(field)) {
        updateData[field] = String(roundToTwo(zod[field]));
      } else {
        updateData[field] = zod[field];
      }
    }
  }
  for (const field of ["funderId", "paymentFrequency", "paymentAmount", "totalPayments", "paymentsCompleted", "contractUrl"]) {
    if (raw[field] !== undefined) {
      if (field === "paymentAmount") {
        updateData[field] = String(roundToTwo(raw[field]));
      } else {
        updateData[field] = raw[field];
      }
    }
  }
  const user = (req as any).user;
  if (user.role === "admin" || user.role === "super_admin" || user.role === "manager") {
    for (const field of ["defaultStatus", "defaultNotes", "defaultAmount"]) {
      if (raw[field] !== undefined) updateData[field] = raw[field];
    }
    if (raw.defaultStatus === "defaulted" && !raw.defaultedAt) {
      updateData.defaultedAt = new Date();
    }
    if (raw.defaultStatus === "resolved") {
      updateData.defaultedAt = null;
    }
  }
  const body = { ...zod, ...raw };

  if (!(await checkDealOwnership(params.data.id, user, res))) return;

  const [existingDeal] = await db.select().from(dealsTable).where(eq(dealsTable.id, params.data.id));
  if (!existingDeal) {
    res.status(404).json({ error: "Deal not found" });
    return;
  }

  if (user.role === "rep" && body.stage === "funded") {
    res.status(403).json({ error: "Only managers can fund deals" });
    return;
  }

  const hasCriticalChanges = 
    (body.amount !== undefined && toNum(body.amount) !== toNum(existingDeal.amount)) ||
    (body.factorRate !== undefined && toNum(body.factorRate) !== toNum(existingDeal.factorRate)) ||
    (body.term !== undefined && body.term !== existingDeal.term) ||
    (body.paymentFrequency !== undefined && body.paymentFrequency !== existingDeal.paymentFrequency);

  if ((body.stage === "funded" && existingDeal.stage !== "funded") || (existingDeal.stage === "funded" && hasCriticalChanges)) {
    if (body.stage === "funded" && existingDeal.stage !== "funded") {
      updateData.fundedDate = new Date();
    }
    
    // Recalculate logic
    const term = body.term !== undefined ? body.term : existingDeal.term || 0;
    const freq = body.paymentFrequency || existingDeal.paymentFrequency || "daily";
    
    // Recalculate total payments (User asked not to change hardcoded 22/4 logic)
    if (term) {
      updateData.totalPayments = freq === "daily" ? term * 22 : freq === "weekly" ? term * 4 : term;
    }
    
    const fundedAmount = toNum(body.amount !== undefined ? body.amount : existingDeal.amount);
    const rate = toNum(body.factorRate !== undefined ? body.factorRate : existingDeal.factorRate || 1);
    
    const newPayback = roundToTwo(fundedAmount * rate);
    if (!updateData.paybackAmount || hasCriticalChanges) {
      updateData.paybackAmount = String(newPayback);
    }
    
    const pbEffective = toNum(updateData.paybackAmount || newPayback);
    const tpEffective = updateData.totalPayments || existingDeal.totalPayments || 0;
    
    if (tpEffective > 0) {
      updateData.paymentAmount = String(roundToTwo(pbEffective / tpEffective));
    }
    
    const months = term || 6;
    updateData.renewalEligibleDate = new Date(Date.now() + months * 30 * 24 * 60 * 60 * 1000 * 0.5);
  }

  const [deal] = await db.update(dealsTable).set(updateData).where(eq(dealsTable.id, params.data.id)).returning();

  const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, deal.leadId));
  const [rep] = await db.select().from(usersTable).where(eq(usersTable.id, deal.repId));

  if (deal.stage === "funded" && lead && existingDeal.stage !== "funded") {
    await db.update(leadsTable).set({ status: "funded" }).where(eq(leadsTable.id, lead.id));

    const commissionPct = toNum(rep?.commissionPct) || 10;
    const commissionAmount = roundToTwo(toNum(deal.amount) * (commissionPct / 100));

    await db.update(dealsTable).set({ 
      commission: String(commissionAmount) 
    }).where(eq(dealsTable.id, deal.id));

    await db.insert(commissionsTable).values({
      dealId: deal.id,
      repId: deal.repId,
      amount: String(commissionAmount),
      percentage: String(commissionPct),
      status: "pending",
    });

    await db.insert(activitiesTable).values({
      leadId: deal.leadId,
      userId: user.id,
      type: "deal_funded",
      description: `Deal funded for $${toNum(deal.amount).toLocaleString()}. Commission of $${commissionAmount.toLocaleString()} (${commissionPct}%) generated.`,
      metadata: { dealId: deal.id, amount: toNum(deal.amount), commission: commissionAmount },
    });
  } else if (body.stage && body.stage !== existingDeal.stage) {
    await db.insert(activitiesTable).values({
      leadId: deal.leadId,
      userId: user.id,
      type: "deal_stage_change",
      description: `Deal moved to ${body.stage}`,
      metadata: { dealId: deal.id, from: existingDeal.stage, to: body.stage },
    });
  }

  res.json({
    ...deal,
    businessName: lead?.businessName || "Unknown",
    repName: rep?.fullName || "Unknown",
  });
});

export default router;
