import { Router, type IRouter } from "express";
import { db, smartRemindersTable, notificationsTable, leadsTable } from "../configs/database";
import { eq, and, lte, sql } from "drizzle-orm";
import { requireAuth } from "../middlewares/authMiddleware";

const router: IRouter = Router();

router.get("/smart-reminders", requireAuth, async (req, res) => {
  try {
    const user = (req as any).user;
    const reminders = await db.select({
      id: smartRemindersTable.id,
      leadId: smartRemindersTable.leadId,
      type: smartRemindersTable.type,
      title: smartRemindersTable.title,
      message: smartRemindersTable.message,
      triggerAt: smartRemindersTable.triggerAt,
      triggered: smartRemindersTable.triggered,
      dismissed: smartRemindersTable.dismissed,
      metadata: smartRemindersTable.metadata,
      createdAt: smartRemindersTable.createdAt,
      businessName: leadsTable.businessName,
    }).from(smartRemindersTable)
      .leftJoin(leadsTable, eq(smartRemindersTable.leadId, leadsTable.id))
      .where(and(
        eq(smartRemindersTable.userId, user.id),
        eq(smartRemindersTable.dismissed, false)
      ))
      .orderBy(smartRemindersTable.triggerAt);

    res.json(reminders);
  } catch (e: any) {
    res.status(500).json({ error: "Failed to fetch reminders" });
  }
});

router.get("/smart-reminders/pending", requireAuth, async (req, res) => {
  try {
    const user = (req as any).user;
    const now = new Date();
    
    const pending = await db.select({
      id: smartRemindersTable.id,
      leadId: smartRemindersTable.leadId,
      type: smartRemindersTable.type,
      title: smartRemindersTable.title,
      message: smartRemindersTable.message,
      triggerAt: smartRemindersTable.triggerAt,
      metadata: smartRemindersTable.metadata,
      businessName: leadsTable.businessName,
    }).from(smartRemindersTable)
      .leftJoin(leadsTable, eq(smartRemindersTable.leadId, leadsTable.id))
      .where(and(
        eq(smartRemindersTable.userId, user.id),
        eq(smartRemindersTable.triggered, false),
        eq(smartRemindersTable.dismissed, false),
        lte(smartRemindersTable.triggerAt, now)
      ))
      .orderBy(smartRemindersTable.triggerAt);

    if (pending.length > 0) {
      await db.update(smartRemindersTable)
        .set({ triggered: true })
        .where(and(
          eq(smartRemindersTable.userId, user.id),
          eq(smartRemindersTable.triggered, false),
          lte(smartRemindersTable.triggerAt, now)
        ));

      for (const r of pending) {
        await db.insert(notificationsTable).values({
          userId: user.id,
          type: "system",
          title: r.title,
          message: `${r.message}${r.businessName ? ` — ${r.businessName}` : ""}`,
          link: r.leadId ? `/leads/${r.leadId}` : undefined,
        });
      }
    }

    res.json({ count: pending.length, reminders: pending });
  } catch (e: any) {
    res.status(500).json({ error: "Failed to check pending reminders" });
  }
});

router.post("/smart-reminders/:id/dismiss", requireAuth, async (req, res) => {
  try {
    const user = (req as any).user;
    const id = parseInt(req.params.id);
    
    await db.update(smartRemindersTable)
      .set({ dismissed: true })
      .where(and(eq(smartRemindersTable.id, id), eq(smartRemindersTable.userId, user.id)));

    res.json({ success: true });
  } catch (e: any) {
    res.status(500).json({ error: "Failed to dismiss reminder" });
  }
});

export default router;

export async function createSmartReminder(userId: number, leadId: number | null, type: string, title: string, message: string, triggerAt: Date, metadata?: any) {
  await db.insert(smartRemindersTable).values({
    userId,
    leadId,
    type,
    title,
    message,
    triggerAt,
    metadata,
  });
}
