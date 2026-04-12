import { Router, type IRouter } from "express";
import { eq, sql, count } from "drizzle-orm";
import { db, fundersTable, submissionsTable, dealsTable } from "../configs/database";
import { requireAuth } from "../middlewares/authMiddleware";

const router: IRouter = Router();

router.get("/funders", requireAuth, async (req, res): Promise<void> => {
  const funders = await db.select().from(fundersTable).orderBy(fundersTable.name);

  const dealStats = await db
    .select({
      funderId: submissionsTable.funderId,
      totalSubmissions: count(),
      approved: sql<number>`count(*) filter (where ${submissionsTable.status} = 'approved')`.as("approved"),
      declined: sql<number>`count(*) filter (where ${submissionsTable.status} = 'declined')`.as("declined"),
      pending: sql<number>`count(*) filter (where ${submissionsTable.status} = 'pending')`.as("pending"),
      totalFunded: sql<number>`coalesce(sum(${submissionsTable.approvedAmount}), 0)`.as("total_funded"),
    })
    .from(submissionsTable)
    .groupBy(submissionsTable.funderId);

  const statsMap = new Map(dealStats.map(s => [s.funderId, s]));

  const result = funders.map(f => ({
    ...f,
    dealStats: statsMap.get(f.id) || {
      funderId: f.id,
      totalSubmissions: 0,
      approved: 0,
      declined: 0,
      pending: 0,
      totalFunded: 0,
    },
  }));

  res.json(result);
});

router.post("/funders", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  if (user.role !== "admin" && user.role !== "super_admin" && user.role !== "manager") {
    res.status(403).json({ error: "Admin only" });
    return;
  }

  const { name, type, description, contactName, contactEmail, contactPhone, website, minAmount, maxAmount, minCreditScore, minTimeInBusiness, industries, states, maxPositions, defaultFactorRate, commissionPct, paymentFrequency, notes } = req.body;

  if (!name) {
    res.status(400).json({ error: "Funder name is required" });
    return;
  }

  const [funder] = await db.insert(fundersTable).values({
    name,
    type: type || "direct",
    description: description || null,
    contactName: contactName || null,
    contactEmail: contactEmail || null,
    contactPhone: contactPhone || null,
    website: website || null,
    minAmount: minAmount || null,
    maxAmount: maxAmount || null,
    minCreditScore: minCreditScore || null,
    minTimeInBusiness: minTimeInBusiness || null,
    industries: industries || null,
    states: states || null,
    maxPositions: maxPositions || 4,
    defaultFactorRate: defaultFactorRate || null,
    commissionPct: commissionPct || null,
    paymentFrequency: paymentFrequency || "daily",
    notes: notes || null,
  }).returning();

  res.status(201).json(funder);
});

router.patch("/funders/:id", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  if (user.role !== "admin" && user.role !== "super_admin" && user.role !== "manager") {
    res.status(403).json({ error: "Admin only" });
    return;
  }

  const id = parseInt(req.params.id);
  const updateData: any = {};
  const fields = ["name", "type", "description", "contactName", "contactEmail", "contactPhone", "website", "minAmount", "maxAmount", "minCreditScore", "minTimeInBusiness", "industries", "states", "maxPositions", "defaultFactorRate", "commissionPct", "paymentFrequency", "notes", "active"];

  for (const field of fields) {
    if (req.body[field] !== undefined) updateData[field] = req.body[field];
  }

  const [funder] = await db.update(fundersTable).set(updateData).where(eq(fundersTable.id, id)).returning();
  if (!funder) {
    res.status(404).json({ error: "Funder not found" });
    return;
  }

  res.json(funder);
});

router.delete("/funders/:id", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  if (user.role !== "admin" && user.role !== "super_admin" && user.role !== "manager") {
    res.status(403).json({ error: "Admin only" });
    return;
  }

  const id = parseInt(req.params.id);
  const [funder] = await db.delete(fundersTable).where(eq(fundersTable.id, id)).returning();
  if (!funder) {
    res.status(404).json({ error: "Funder not found" });
    return;
  }

  res.json({ success: true });
});

export default router;
