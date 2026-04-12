import { Router, type IRouter } from "express";
import { eq, sql } from "drizzle-orm";
import { db, referralSourcesTable, leadsTable } from "../configs/database";
import { requireAuth } from "../middlewares/authMiddleware";

const router: IRouter = Router();

router.get("/referral-sources", requireAuth, async (req, res): Promise<void> => {
  const sources = await db.select().from(referralSourcesTable).orderBy(referralSourcesTable.name);
  res.json(sources);
});

router.post("/referral-sources", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  if (user.role !== "admin") {
    res.status(403).json({ error: "Admin only" });
    return;
  }

  const { name, type, contactName, contactEmail, contactPhone, commissionPct, notes } = req.body;
  if (!name) {
    res.status(400).json({ error: "Name is required" });
    return;
  }

  const [source] = await db.insert(referralSourcesTable).values({
    name,
    type: type || "iso",
    contactName: contactName || null,
    contactEmail: contactEmail || null,
    contactPhone: contactPhone || null,
    commissionPct: commissionPct || null,
    notes: notes || null,
  }).returning();

  res.status(201).json(source);
});

router.patch("/referral-sources/:id", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  if (user.role !== "admin") {
    res.status(403).json({ error: "Admin only" });
    return;
  }

  const id = parseInt(req.params.id);
  const updateData: any = {};
  const fields = ["name", "type", "contactName", "contactEmail", "contactPhone", "commissionPct", "active", "notes"];
  for (const field of fields) {
    if (req.body[field] !== undefined) updateData[field] = req.body[field];
  }

  const [source] = await db.update(referralSourcesTable).set(updateData).where(eq(referralSourcesTable.id, id)).returning();
  if (!source) {
    res.status(404).json({ error: "Referral source not found" });
    return;
  }

  res.json(source);
});

router.delete("/referral-sources/:id", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  if (user.role !== "admin") {
    res.status(403).json({ error: "Admin only" });
    return;
  }

  const id = parseInt(req.params.id);
  const [source] = await db.delete(referralSourcesTable).where(eq(referralSourcesTable.id, id)).returning();
  if (!source) {
    res.status(404).json({ error: "Referral source not found" });
    return;
  }

  res.json({ success: true });
});

export default router;
