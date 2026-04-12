import { Router, type IRouter } from "express";
import { eq, desc } from "drizzle-orm";
import { db, templatesTable } from "../configs/database";
import { requireAuth } from "../middlewares/authMiddleware";

const router: IRouter = Router();

router.get("/templates", requireAuth, async (req, res): Promise<void> => {
  const templates = await db.select()
    .from(templatesTable)
    .orderBy(desc(templatesTable.updatedAt));

  res.json(templates);
});

router.post("/templates", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  if (user.role !== "admin") {
    res.status(403).json({ error: "Admin only" });
    return;
  }

  const { name, category, subject, body, type } = req.body;
  if (!name || !body) {
    res.status(400).json({ error: "Name and body required" });
    return;
  }

  const [template] = await db.insert(templatesTable)
    .values({ name, category: category || "general", subject, body, type: type || "email", createdById: user.id })
    .returning();

  res.json(template);
});

router.put("/templates/:id", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  if (user.role !== "admin") {
    res.status(403).json({ error: "Admin only" });
    return;
  }

  const id = parseInt(String(req.params.id), 10);
  const { name, category, subject, body, type } = req.body;

  const [template] = await db.update(templatesTable)
    .set({ name, category, subject, body, type })
    .where(eq(templatesTable.id, id))
    .returning();

  res.json(template);
});

router.delete("/templates/:id", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  if (user.role !== "admin") {
    res.status(403).json({ error: "Admin only" });
    return;
  }

  const id = parseInt(String(req.params.id), 10);
  await db.delete(templatesTable).where(eq(templatesTable.id, id));
  res.json({ success: true });
});

export default router;
