import { Router, type IRouter } from "express";
import { eq, and, sql, desc, sum } from "drizzle-orm";
import { db, commissionsTable, dealsTable, usersTable, leadsTable } from "../configs/database";
import { requireAuth } from "../middlewares/authMiddleware";

const router: IRouter = Router();

router.get("/commissions", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;

  let conditions: any[] = [];
  if (user.role === "rep") {
    conditions.push(eq(commissionsTable.repId, user.id));
  }
  if (req.query.repId) {
    const repIdStr = Array.isArray(req.query.repId) ? req.query.repId[0] : req.query.repId;
    if (repIdStr) conditions.push(eq(commissionsTable.repId, parseInt(repIdStr as string)));
  }
  if (req.query.status) {
    const statusStr = Array.isArray(req.query.status) ? req.query.status[0] : req.query.status;
    if (statusStr) conditions.push(eq(commissionsTable.status, statusStr as string));
  }

  const whereClause = conditions.length > 0 ? and(...conditions) : undefined;

  const commissions = await db.select({
    id: commissionsTable.id,
    dealId: commissionsTable.dealId,
    repId: commissionsTable.repId,
    repName: usersTable.fullName,
    businessName: leadsTable.businessName,
    type: commissionsTable.type,
    amount: commissionsTable.amount,
    percentage: commissionsTable.percentage,
    status: commissionsTable.status,
    paidAt: commissionsTable.paidAt,
    dealAmount: dealsTable.amount,
    notes: commissionsTable.notes,
    createdAt: commissionsTable.createdAt,
  }).from(commissionsTable)
    .leftJoin(dealsTable, eq(commissionsTable.dealId, dealsTable.id))
    .leftJoin(usersTable, eq(commissionsTable.repId, usersTable.id))
    .leftJoin(leadsTable, eq(dealsTable.leadId, leadsTable.id))
    .where(whereClause)
    .orderBy(desc(commissionsTable.createdAt));

  res.json(commissions);
});

router.get("/commissions/summary", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;

  const reps = await db.select().from(usersTable).where(eq(usersTable.role, "rep"));

  const summaries = [];
  for (const rep of reps) {
    if (user.role === "rep" && rep.id !== user.id) continue;

    const [pending] = await db.select({
      total: sql<number>`COALESCE(SUM(${commissionsTable.amount}), 0)`,
      count: sql<number>`COUNT(*)`,
    }).from(commissionsTable)
      .where(and(eq(commissionsTable.repId, rep.id), eq(commissionsTable.status, "pending")));

    const [paid] = await db.select({
      total: sql<number>`COALESCE(SUM(${commissionsTable.amount}), 0)`,
      count: sql<number>`COUNT(*)`,
    }).from(commissionsTable)
      .where(and(eq(commissionsTable.repId, rep.id), eq(commissionsTable.status, "paid")));

    summaries.push({
      repId: rep.id,
      repName: rep.fullName,
      pendingAmount: Number(pending.total),
      pendingCount: Number(pending.count),
      paidAmount: Number(paid.total),
      paidCount: Number(paid.count),
      totalEarned: Number(pending.total) + Number(paid.total),
    });
  }

  res.json(summaries);
});

router.post("/commissions", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  if (user.role !== "admin") {
    res.status(403).json({ error: "Admin only" });
    return;
  }

  const { dealId, repId, type, amount, percentage, notes } = req.body;

  if (!dealId || !repId || !amount) {
    res.status(400).json({ error: "dealId, repId, and amount are required" });
    return;
  }

  const [commission] = await db.insert(commissionsTable).values({
    dealId,
    repId,
    type: type || "funding",
    amount: String(amount),
    percentage: percentage ? String(percentage) : null,
    notes: notes || null,
  }).returning();

  res.status(201).json(commission);
});

router.patch("/commissions/:id", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  if (user.role !== "admin") {
    res.status(403).json({ error: "Admin only" });
    return;
  }

  const id = parseInt(req.params.id);
  const { status, amount, notes } = req.body;

  const updateData: any = {};
  if (status) {
    updateData.status = status;
    if (status === "paid") updateData.paidAt = new Date();
  }
  if (amount !== undefined) updateData.amount = String(amount);
  if (notes !== undefined) updateData.notes = notes;

  const [commission] = await db.update(commissionsTable).set(updateData).where(eq(commissionsTable.id, id)).returning();
  if (!commission) {
    res.status(404).json({ error: "Commission not found" });
    return;
  }

  res.json(commission);
});

export default router;
