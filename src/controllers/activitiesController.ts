import { Router, type IRouter } from "express";
import { eq, desc, and } from "drizzle-orm";
import { db, activitiesTable, usersTable, leadsTable } from "../configs/database";
import { requireAuth } from "../middlewares/authMiddleware";

const router: IRouter = Router();

router.get("/activities", requireAuth, async (req, res): Promise<void> => {
  const limit = Math.min(parseInt(req.query.limit as string) || 20, 50);
  const user = (req as any).user;

  const baseQuery = db.select({
    id: activitiesTable.id,
    leadId: activitiesTable.leadId,
    userId: activitiesTable.userId,
    userName: usersTable.fullName,
    type: activitiesTable.type,
    description: activitiesTable.description,
    metadata: activitiesTable.metadata,
    createdAt: activitiesTable.createdAt,
  }).from(activitiesTable)
    .leftJoin(usersTable, eq(activitiesTable.userId, usersTable.id));

  const activities = user.role === "rep"
    ? await baseQuery
        .innerJoin(leadsTable, eq(activitiesTable.leadId, leadsTable.id))
        .where(eq(leadsTable.assignedToId, user.id))
        .orderBy(desc(activitiesTable.createdAt))
        .limit(limit)
    : await baseQuery
        .orderBy(desc(activitiesTable.createdAt))
        .limit(limit);

  res.json(activities);
});

router.get("/leads/:leadId/activities", requireAuth, async (req, res): Promise<void> => {
  const leadId = parseInt(req.params.leadId);
  const user = (req as any).user;

  if (user.role === "rep") {
    const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!lead || lead.assignedToId !== user.id) {
      res.status(403).json({ error: "Access denied" });
      return;
    }
  }

  const activities = await db.select({
    id: activitiesTable.id,
    leadId: activitiesTable.leadId,
    userId: activitiesTable.userId,
    userName: usersTable.fullName,
    type: activitiesTable.type,
    description: activitiesTable.description,
    metadata: activitiesTable.metadata,
    createdAt: activitiesTable.createdAt,
  }).from(activitiesTable)
    .leftJoin(usersTable, eq(activitiesTable.userId, usersTable.id))
    .where(eq(activitiesTable.leadId, leadId))
    .orderBy(desc(activitiesTable.createdAt))
    .limit(100);

  res.json(activities);
});

export default router;
