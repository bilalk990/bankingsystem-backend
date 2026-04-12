import { Router, type IRouter } from "express";
import { eq } from "drizzle-orm";
import { db, stipulationsTable, dealsTable, activitiesTable } from "../configs/database";
import { requireAuth } from "../middlewares/authMiddleware";

async function checkDealAccess(dealId: number, user: any, res: any): Promise<boolean> {
  const [deal] = await db.select().from(dealsTable).where(eq(dealsTable.id, dealId));
  if (!deal) { res.status(404).json({ error: "Deal not found" }); return false; }
  if (user.role === "rep" && deal.repId !== user.id) { res.status(403).json({ error: "Access denied" }); return false; }
  return true;
}

const router: IRouter = Router();

const DEFAULT_STIPULATIONS = [
  { name: "Signed Application", type: "document" },
  { name: "3 Months Bank Statements", type: "document" },
  { name: "Valid Photo ID (Front & Back)", type: "document" },
  { name: "Voided Check", type: "document" },
  { name: "Credit Authorization", type: "document" },
  { name: "Business Tax Returns", type: "document" },
  { name: "Proof of Ownership", type: "document" },
  { name: "Landlord Contact / Lease Agreement", type: "document" },
];

router.get("/deals/:dealId/stipulations", requireAuth, async (req, res): Promise<void> => {
  const dealId = parseInt(String(req.params.dealId), 10);
  const user = (req as any).user;
  if (!(await checkDealAccess(dealId, user, res))) return;
  const stips = await db.select().from(stipulationsTable).where(eq(stipulationsTable.dealId, dealId)).orderBy(stipulationsTable.createdAt);
  res.json(stips);
});

router.post("/deals/:dealId/stipulations/init", requireAuth, async (req, res): Promise<void> => {
  const dealId = parseInt(String(req.params.dealId), 10);
  const user = (req as any).user;
  if (!(await checkDealAccess(dealId, user, res))) return;

  const [deal] = await db.select().from(dealsTable).where(eq(dealsTable.id, dealId));

  const existing = await db.select().from(stipulationsTable).where(eq(stipulationsTable.dealId, dealId));
  if (existing.length > 0) {
    res.json(existing);
    return;
  }

  const stips = await db.insert(stipulationsTable).values(
    DEFAULT_STIPULATIONS.map(s => ({ ...s, dealId, required: true }))
  ).returning();

  await db.insert(activitiesTable).values({
    leadId: deal!.leadId,
    userId: user.id,
    type: "stipulations_created",
    description: `Stipulation checklist created with ${stips.length} items`,
    metadata: { dealId },
  });

  res.status(201).json(stips);
});

router.post("/deals/:dealId/stipulations", requireAuth, async (req, res): Promise<void> => {
    const dealId = parseInt(String(req.params.dealId), 10);
  const user = (req as any).user;
  if (!(await checkDealAccess(dealId, user, res))) return;
  const { name, type, required } = req.body;

  if (!name) {
    res.status(400).json({ error: "Stipulation name is required" });
    return;
  }

  const [stip] = await db.insert(stipulationsTable).values({
    dealId,
    name,
    type: type || "document",
    required: required !== false,
  }).returning();

  res.status(201).json(stip);
});

router.patch("/stipulations/:id", requireAuth, async (req, res): Promise<void> => {
  const id = parseInt(String(req.params.id), 10);
  const user = (req as any).user;
  const { completed, notes } = req.body;

  const [existingStip] = await db.select().from(stipulationsTable).where(eq(stipulationsTable.id, id));
  if (!existingStip) {
    res.status(404).json({ error: "Stipulation not found" });
    return;
  }
  if (!(await checkDealAccess(existingStip.dealId, user, res))) return;

  const updateData: any = {};
  if (completed !== undefined) {
    updateData.completed = completed;
    updateData.completedAt = completed ? new Date() : null;
    updateData.completedById = completed ? user.id : null;
  }
  if (notes !== undefined) updateData.notes = notes;

  const [stip] = await db.update(stipulationsTable).set(updateData).where(eq(stipulationsTable.id, id)).returning();
  if (!stip) {
    res.status(404).json({ error: "Stipulation not found" });
    return;
  }

  const [deal] = await db.select().from(dealsTable).where(eq(dealsTable.id, stip.dealId));
  if (deal) {
    await db.insert(activitiesTable).values({
      leadId: deal.leadId,
      userId: user.id,
      type: "stipulation_update",
      description: `${completed ? "Completed" : "Unchecked"} stipulation: ${stip.name}`,
      metadata: { stipulationId: id, completed },
    });
  }

  res.json(stip);
});

router.delete("/stipulations/:id", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  if (user.role !== "admin" && user.role !== "super_admin" && user.role !== "manager") {
    res.status(403).json({ error: "Admin only" });
    return;
  }

  const id = parseInt(String(req.params.id), 10);
  const [stip] = await db.delete(stipulationsTable).where(eq(stipulationsTable.id, id)).returning();
  if (!stip) {
    res.status(404).json({ error: "Stipulation not found" });
    return;
  }

  res.json({ success: true });
});

export default router;
