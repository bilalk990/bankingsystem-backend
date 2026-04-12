import { Router, type IRouter } from "express";
import { eq, and, asc, desc, sql } from "drizzle-orm";
import { db, leadsTable, usersTable, documentsTable, callsTable, dealsTable, bankStatementAnalysesTable, underwritingConfirmationsTable, leadMessagesTable } from "../configs/database";
import {
  CreateLeadBody,
  UpdateLeadBody,
  GetLeadParams,
  UpdateLeadParams,
  DeleteLeadParams,
  GetLeadsQueryParams,
  GetLeadsResponse,
  GetLeadResponse,
  UpdateLeadResponse,
  GetNextLeadResponse,
} from "../validationSchemas";
import { requireAuth, checkLeadOwnership } from "../middlewares/authMiddleware";
import { encryptLeadFields, decryptLeadFields, maskSsn } from "../utils/encryption";
import { logSecurityEvent } from "../utils/security";
import { trackLeadView, trackSearchQuery, trackBulkList, isUserLocked } from "../utils/activityGuard";

const parseNumeric = (val: any): number => {
  if (val === null || val === undefined) return 0;
  if (typeof val === 'number') return isNaN(val) ? 0 : val;
  const cleaned = String(val).replace(/[^0-9.-]/g, '');
  const parsed = parseFloat(cleaned);
  return isNaN(parsed) ? 0 : parsed;
};

const router: IRouter = Router();

router.get("/leads", requireAuth, async (req, res): Promise<void> => {
  const query = GetLeadsQueryParams.safeParse(req.query);
  const user = (req as any).user;

  if (isUserLocked(user.id)) {
    res.status(423).json({ error: "Account temporarily locked due to unusual activity. Please verify your identity.", code: "ACTIVITY_LOCKED" });
    return;
  }

  if (query.success && query.data.search) {
    const status = await trackSearchQuery(user.id, req);
    if (status === "locked") {
      res.status(423).json({ error: "Account temporarily locked due to unusual activity. Please verify your identity.", code: "ACTIVITY_LOCKED" });
      return;
    }
  }

  const bulkStatus = await trackBulkList(user.id, 0, req);
  if (bulkStatus === "locked") {
    res.status(423).json({ error: "Account temporarily locked due to unusual activity. Please verify your identity.", code: "ACTIVITY_LOCKED" });
    return;
  }

  let conditions: any[] = [];
  if (query.success && query.data.status) {
    conditions.push(eq(leadsTable.status, query.data.status));
  }
  if (query.success && query.data.assignedTo) {
    conditions.push(eq(leadsTable.assignedToId, query.data.assignedTo));
  }
  if (query.success && query.data.source) {
    conditions.push(eq(leadsTable.source, query.data.source));
  }
  if (query.success && query.data.riskCategory) {
    conditions.push(eq(leadsTable.riskCategory, query.data.riskCategory));
  }
  if (user.role === "rep") {
    conditions.push(eq(leadsTable.assignedToId, user.id));
  }
  if (query.success && query.data.search) {
    const s = `%${query.data.search}%`;
    conditions.push(
      sql`(LOWER(${leadsTable.businessName}) LIKE LOWER(${s})
        OR LOWER(${leadsTable.ownerName}) LIKE LOWER(${s})
        OR ${leadsTable.phone} LIKE ${s}
        OR LOWER(COALESCE(${leadsTable.dba}, '')) LIKE LOWER(${s}))`
    );
  }

  const whereClause = conditions.length > 0 ? and(...conditions) : undefined;

  const limit = (query.success && query.data.limit) ? Math.min(query.data.limit, 10000) : 10000;
  const offset = (query.success && query.data.offset) || 0;

  const selectFields = {
    id: leadsTable.id,
    businessName: leadsTable.businessName,
    dba: leadsTable.dba,
    ownerName: leadsTable.ownerName,
    email: leadsTable.email,
    phone: leadsTable.phone,
    status: leadsTable.status,
    assignedToId: leadsTable.assignedToId,
    assignedToName: usersTable.fullName,
    requestedAmount: leadsTable.requestedAmount,
    monthlyRevenue: leadsTable.monthlyRevenue,
    industry: leadsTable.industry,
    state: leadsTable.state,
    riskCategory: leadsTable.riskCategory,
    creditScore: leadsTable.creditScore,
    hasExistingLoans: leadsTable.hasExistingLoans,
    loanCount: leadsTable.loanCount,
    avgDailyBalance: leadsTable.avgDailyBalance,
    revenueTrend: leadsTable.revenueTrend,
    grossRevenue: leadsTable.grossRevenue,
    hasOnDeck: leadsTable.hasOnDeck,
    source: leadsTable.source,
    sourceTier: leadsTable.sourceTier,
    importDate: leadsTable.importDate,
    createdAt: leadsTable.createdAt,
    lastContactedAt: leadsTable.lastContactedAt,
    isHot: leadsTable.isHot,
    bankStatementsStatus: leadsTable.bankStatementsStatus,
    bankStatementMonths: leadsTable.bankStatementMonths,
    estimatedApproval: leadsTable.estimatedApproval,
    approvalConfidence: leadsTable.approvalConfidence,
    notes: leadsTable.notes,
  };

  const [countResult, leads] = await Promise.all([
    db.select({ count: sql<number>`COUNT(*)` })
      .from(leadsTable)
      .where(whereClause),
    db.select(selectFields)
      .from(leadsTable)
      .leftJoin(usersTable, eq(leadsTable.assignedToId, usersTable.id))
      .where(whereClause)
      .orderBy(desc(leadsTable.createdAt))
      .limit(limit)
      .offset(offset),
  ]);

  const total = Number(countResult[0]?.count || 0);
  res.json({ leads, total, limit, offset });
});

router.post("/leads", requireAuth, async (req, res): Promise<void> => {
  const parsed = CreateLeadBody.safeParse(req.body);
  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.message });
    return;
  }

  const { bankStatementUrl, idDocumentUrl, voidCheckUrl, ...leadData } = parsed.data;
  const encryptedData = encryptLeadFields(leadData);

  const payload = {
    ...encryptedData,
    requestedAmount: encryptedData.requestedAmount ? String(encryptedData.requestedAmount) : null,
    monthlyRevenue: encryptedData.monthlyRevenue ? String(encryptedData.monthlyRevenue) : null,
    yearsInBusiness: encryptedData.yearsInBusiness ? String(encryptedData.yearsInBusiness) : null,
    creditScore: encryptedData.creditScore ? Number(encryptedData.creditScore) : null,
  };

  const [lead] = await db.insert(leadsTable).values(payload).returning();

  if (bankStatementUrl) {
    await db.insert(documentsTable).values({
      leadId: lead.id,
      type: "bank_statement",
      name: "Bank Statement",
      url: bankStatementUrl,
    });
  }
  if (idDocumentUrl) {
    await db.insert(documentsTable).values({
      leadId: lead.id,
      type: "id_document",
      name: "ID Document",
      url: idDocumentUrl,
    });
  }
  if (voidCheckUrl) {
    await db.insert(documentsTable).values({
      leadId: lead.id,
      type: "void_check",
      name: "Void Check",
      url: voidCheckUrl,
    });
  }

  res.status(201).json(UpdateLeadResponse.parse({
    id: lead.id,
    businessName: lead.businessName,
    ownerName: lead.ownerName,
    email: lead.email,
    phone: lead.phone,
    status: lead.status,
    assignedToId: lead.assignedToId,
    assignedToName: null,
    requestedAmount: lead.requestedAmount,
    monthlyRevenue: lead.monthlyRevenue,
    industry: lead.industry,
    createdAt: lead.createdAt,
    lastContactedAt: lead.lastContactedAt,
  }));
});

router.get("/leads/next", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;

  const conditions = [
    eq(leadsTable.status, "new"),
  ];
  if (user.role === "rep") {
    conditions.push(eq(leadsTable.assignedToId, user.id));
  }

  const [lead] = await db.select().from(leadsTable)
    .where(and(...conditions))
    .orderBy(asc(leadsTable.createdAt))
    .limit(1);

  if (!lead) {
    res.status(404).json({ error: "No more leads" });
    return;
  }

  const docs = await db.select().from(documentsTable).where(eq(documentsTable.leadId, lead.id));
  const calls = await db.select({
    id: callsTable.id,
    leadId: callsTable.leadId,
    userId: callsTable.userId,
    userName: usersTable.fullName,
    outcome: callsTable.outcome,
    notes: callsTable.notes,
    duration: callsTable.duration,
    callbackAt: callsTable.callbackAt,
    createdAt: callsTable.createdAt,
  }).from(callsTable)
    .leftJoin(usersTable, eq(callsTable.userId, usersTable.id))
    .where(eq(callsTable.leadId, lead.id));
  const deals = await db.select({
    id: dealsTable.id,
    leadId: dealsTable.leadId,
    businessName: sql<string>`${leadsTable.businessName}`,
    repId: dealsTable.repId,
    repName: usersTable.fullName,
    stage: dealsTable.stage,
    amount: dealsTable.amount,
    factorRate: dealsTable.factorRate,
    paybackAmount: dealsTable.paybackAmount,
    term: dealsTable.term,
    commission: dealsTable.commission,
    notes: dealsTable.notes,
    createdAt: dealsTable.createdAt,
    updatedAt: dealsTable.updatedAt,
  }).from(dealsTable)
    .leftJoin(usersTable, eq(dealsTable.repId, usersTable.id))
    .leftJoin(leadsTable, eq(dealsTable.leadId, leadsTable.id))
    .where(eq(dealsTable.leadId, lead.id));

  const assignedUser = lead.assignedToId
    ? (await db.select().from(usersTable).where(eq(usersTable.id, lead.assignedToId)))[0]
    : null;

  const decryptedNextLead = decryptLeadFields(lead);
  res.json(GetNextLeadResponse.parse({
    ...decryptedNextLead,
    ssn: maskSsn(lead.ssn),
    assignedToName: assignedUser?.fullName || null,
    documents: docs,
    calls,
    deals,
  }));
});

router.get("/leads/:id", requireAuth, async (req, res): Promise<void> => {
  const params = GetLeadParams.safeParse(req.params);
  if (!params.success) {
    res.status(400).json({ error: params.error.message });
    return;
  }

  const user = (req as any).user;

  if (isUserLocked(user.id)) {
    res.status(423).json({ error: "Account temporarily locked due to unusual activity. Please verify your identity.", code: "ACTIVITY_LOCKED" });
    return;
  }

  const viewStatus = await trackLeadView(user.id, params.data.id, req);
  if (viewStatus === "locked") {
    res.status(423).json({ error: "Account temporarily locked due to unusual activity. Please verify your identity.", code: "ACTIVITY_LOCKED" });
    return;
  }

  const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, params.data.id));
  if (!lead) {
    res.status(404).json({ error: "Lead not found" });
    return;
  }

  if (user.role === "rep" && lead.assignedToId !== user.id) {
    res.status(403).json({ error: "Access denied" });
    return;
  }

  const docs = await db.select().from(documentsTable).where(eq(documentsTable.leadId, lead.id));
  const calls = await db.select({
    id: callsTable.id,
    leadId: callsTable.leadId,
    userId: callsTable.userId,
    userName: usersTable.fullName,
    outcome: callsTable.outcome,
    notes: callsTable.notes,
    duration: callsTable.duration,
    callbackAt: callsTable.callbackAt,
    createdAt: callsTable.createdAt,
  }).from(callsTable)
    .leftJoin(usersTable, eq(callsTable.userId, usersTable.id))
    .where(eq(callsTable.leadId, lead.id));
  const deals = await db.select({
    id: dealsTable.id,
    leadId: dealsTable.leadId,
    businessName: sql<string>`${leadsTable.businessName}`,
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
  }).from(dealsTable)
    .leftJoin(usersTable, eq(dealsTable.repId, usersTable.id))
    .leftJoin(leadsTable, eq(dealsTable.leadId, leadsTable.id))
    .where(eq(dealsTable.leadId, lead.id));

  const assignedUser = lead.assignedToId
    ? (await db.select().from(usersTable).where(eq(usersTable.id, lead.assignedToId)))[0]
    : null;

  const analyses = await db.select().from(bankStatementAnalysesTable)
    .where(eq(bankStatementAnalysesTable.leadId, lead.id))
    .orderBy(sql`${bankStatementAnalysesTable.createdAt} DESC`);

  const confirmations = await db.select().from(underwritingConfirmationsTable)
    .where(eq(underwritingConfirmationsTable.leadId, lead.id))
    .orderBy(sql`${underwritingConfirmationsTable.createdAt} DESC`);

  const leadMessages = await db.select({
    id: leadMessagesTable.id,
    leadId: leadMessagesTable.leadId,
    source: leadMessagesTable.source,
    direction: leadMessagesTable.direction,
    content: leadMessagesTable.content,
    senderName: leadMessagesTable.senderName,
    messageType: leadMessagesTable.messageType,
    aiGenerated: leadMessagesTable.aiGenerated,
    isRead: leadMessagesTable.isRead,
    userId: leadMessagesTable.userId,
    createdAt: leadMessagesTable.createdAt,
  }).from(leadMessagesTable)
    .where(eq(leadMessagesTable.leadId, lead.id))
    .orderBy(leadMessagesTable.createdAt);

  const decryptedLead = decryptLeadFields(lead);
  const maskedSsn = maskSsn(lead.ssn);

  logSecurityEvent("pii_data_accessed", "info",
    `User ${user.email} viewed lead #${lead.id} (${lead.businessName || "unknown"})`, {
      userId: user.id, req,
      metadata: { leadId: lead.id, businessName: lead.businessName, accessType: "lead_detail_view" },
    }).catch(() => {});

  res.json({
    ...decryptedLead,
    ssn: maskedSsn,
    assignedToName: assignedUser?.fullName || null,
    documents: docs,
    calls,
    deals,
    analyses,
    confirmations,
    messages: leadMessages,
  });
});

router.patch("/leads/:id", requireAuth, async (req, res): Promise<void> => {
  const params = UpdateLeadParams.safeParse(req.params);
  if (!params.success) {
    res.status(400).json({ error: params.error.message });
    return;
  }

  const parsed = UpdateLeadBody.safeParse(req.body);
  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.message });
    return;
  }

  const user = (req as any).user;
  if (!(await checkLeadOwnership(params.data.id, user, res))) return;

  if (user.role === "rep") {
    delete (parsed.data as any).assignedToId;
    delete (parsed.data as any).status;
  }

  const encryptedUpdate = encryptLeadFields(parsed.data);

  const updatePayload = {
    ...encryptedUpdate,
    requestedAmount: encryptedUpdate.requestedAmount ? String(encryptedUpdate.requestedAmount) : undefined,
    monthlyRevenue: encryptedUpdate.monthlyRevenue ? String(encryptedUpdate.monthlyRevenue) : undefined,
    yearsInBusiness: encryptedUpdate.yearsInBusiness ? String(encryptedUpdate.yearsInBusiness) : undefined,
    creditScore: encryptedUpdate.creditScore ? Number(encryptedUpdate.creditScore) : undefined,
  };

  const [lead] = await db.update(leadsTable).set(updatePayload).where(eq(leadsTable.id, params.data.id)).returning();
  if (!lead) {
    res.status(404).json({ error: "Lead not found" });
    return;
  }

  const assignedUser = lead.assignedToId
    ? (await db.select().from(usersTable).where(eq(usersTable.id, lead.assignedToId)))[0]
    : null;

  const decryptedUpdatedLead = decryptLeadFields(lead);
  res.json(UpdateLeadResponse.parse({
    ...decryptedUpdatedLead,
    ssn: maskSsn(lead.ssn),
    assignedToName: assignedUser?.fullName || null,
  }));
});

router.delete("/leads/:id", requireAuth, async (req, res): Promise<void> => {
  const params = DeleteLeadParams.safeParse(req.params);
  if (!params.success) {
    res.status(400).json({ error: params.error.message });
    return;
  }

  const user = (req as any).user;
  if (user.role === "rep") {
    res.status(403).json({ error: "Access denied" });
    return;
  }

  const [lead] = await db.delete(leadsTable).where(eq(leadsTable.id, params.data.id)).returning();
  if (!lead) {
    res.status(404).json({ error: "Lead not found" });
    return;
  }

  res.json({ message: "Lead deleted" });
});

export default router;
