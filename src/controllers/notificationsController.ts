import { Router, type IRouter } from "express";
import { eq, desc, and, sql } from "drizzle-orm";
import { db, notificationsTable } from "../configs/database";
import { requireAuth } from "../middlewares/authMiddleware";

const router: IRouter = Router();

router.get("/notifications", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  const limit = Math.min(parseInt(req.query.limit as string) || 20, 50);
  const unreadOnly = req.query.unread === "true";

  const conditions = [eq(notificationsTable.userId, user.id)];
  if (unreadOnly) conditions.push(eq(notificationsTable.read, false));

  const notifications = await db.select()
    .from(notificationsTable)
    .where(and(...conditions))
    .orderBy(desc(notificationsTable.createdAt))
    .limit(limit);

  const unreadCount = await db.select({ count: sql<number>`count(*)::int` })
    .from(notificationsTable)
    .where(and(eq(notificationsTable.userId, user.id), eq(notificationsTable.read, false)));

  res.json({ notifications, unreadCount: unreadCount[0]?.count || 0 });
});

router.post("/notifications/:id/read", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  const id = parseInt(req.params.id);

  await db.update(notificationsTable)
    .set({ read: true })
    .where(and(eq(notificationsTable.id, id), eq(notificationsTable.userId, user.id)));

  res.json({ success: true });
});

router.post("/notifications/read-all", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;

  await db.update(notificationsTable)
    .set({ read: true })
    .where(eq(notificationsTable.userId, user.id));

  res.json({ success: true });
});

router.delete("/notifications/:id", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  const id = parseInt(req.params.id);

  await db.delete(notificationsTable)
    .where(and(eq(notificationsTable.id, id), eq(notificationsTable.userId, user.id)));

  res.json({ success: true });
});

export default router;
