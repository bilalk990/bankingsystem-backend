import { Router, type IRouter } from "express";
import { db, tasksTable, usersTable, leadsTable, smartRemindersTable, callsTable } from "../configs/database";
import { eq, and, or, desc, asc, gte, lte } from "drizzle-orm";
import { requireAuth, requireAdmin } from "../middlewares/authMiddleware";

const router: IRouter = Router();

router.get("/tasks", requireAuth, async (req, res) => {
  try {
    const user = (req as any).user;
    const isAdmin = user.role === "admin" || user.role === "super_admin";

    const conditions = isAdmin
      ? undefined
      : or(eq(tasksTable.assignedToId, user.id), eq(tasksTable.assignedById, user.id));

    const tasks = await db
      .select({
        id: tasksTable.id,
        title: tasksTable.title,
        description: tasksTable.description,
        priority: tasksTable.priority,
        status: tasksTable.status,
        dueDate: tasksTable.dueDate,
        assignedToId: tasksTable.assignedToId,
        assignedById: tasksTable.assignedById,
        leadId: tasksTable.leadId,
        completedAt: tasksTable.completedAt,
        createdAt: tasksTable.createdAt,
        assignedToName: usersTable.fullName,
      })
      .from(tasksTable)
      .leftJoin(usersTable, eq(tasksTable.assignedToId, usersTable.id))
      .where(conditions)
      .orderBy(asc(tasksTable.dueDate), desc(tasksTable.createdAt));

    res.json(tasks);
  } catch (e: any) {
    console.error("Tasks list error:", e);
    res.status(500).json({ error: "Failed to fetch tasks" });
  }
});

router.get("/tasks/calendar", requireAuth, async (req, res) => {
  try {
    const user = (req as any).user;
    const isAdmin = user.role === "admin" || user.role === "super_admin";
    const { start, end } = req.query;

    const startDate = start ? new Date(start as string) : new Date(new Date().getFullYear(), new Date().getMonth(), 1);
    const endDate = end ? new Date(end as string) : new Date(new Date().getFullYear(), new Date().getMonth() + 1, 0, 23, 59, 59);

    const taskConditions = isAdmin
      ? and(gte(tasksTable.dueDate, startDate), lte(tasksTable.dueDate, endDate))
      : and(
          or(eq(tasksTable.assignedToId, user.id), eq(tasksTable.assignedById, user.id)),
          gte(tasksTable.dueDate, startDate),
          lte(tasksTable.dueDate, endDate)
        );

    const tasks = await db
      .select({
        id: tasksTable.id,
        title: tasksTable.title,
        description: tasksTable.description,
        priority: tasksTable.priority,
        status: tasksTable.status,
        dueDate: tasksTable.dueDate,
        assignedToId: tasksTable.assignedToId,
        assignedToName: usersTable.fullName,
        leadId: tasksTable.leadId,
        completedAt: tasksTable.completedAt,
      })
      .from(tasksTable)
      .leftJoin(usersTable, eq(tasksTable.assignedToId, usersTable.id))
      .where(taskConditions)
      .orderBy(asc(tasksTable.dueDate));

    const reminderConditions = isAdmin
      ? and(
          eq(smartRemindersTable.dismissed, false),
          gte(smartRemindersTable.triggerAt, startDate),
          lte(smartRemindersTable.triggerAt, endDate)
        )
      : and(
          eq(smartRemindersTable.userId, user.id),
          eq(smartRemindersTable.dismissed, false),
          gte(smartRemindersTable.triggerAt, startDate),
          lte(smartRemindersTable.triggerAt, endDate)
        );

    const reminders = await db
      .select({
        id: smartRemindersTable.id,
        title: smartRemindersTable.title,
        message: smartRemindersTable.message,
        type: smartRemindersTable.type,
        triggerAt: smartRemindersTable.triggerAt,
        leadId: smartRemindersTable.leadId,
        businessName: leadsTable.businessName,
        triggered: smartRemindersTable.triggered,
      })
      .from(smartRemindersTable)
      .leftJoin(leadsTable, eq(smartRemindersTable.leadId, leadsTable.id))
      .where(reminderConditions)
      .orderBy(asc(smartRemindersTable.triggerAt));

    const callConditions = isAdmin
      ? and(
          gte(callsTable.callbackAt, startDate),
          lte(callsTable.callbackAt, endDate)
        )
      : and(
          eq(callsTable.userId, user.id),
          gte(callsTable.callbackAt, startDate),
          lte(callsTable.callbackAt, endDate)
        );

    const callbacks = await db
      .select({
        id: callsTable.id,
        callbackAt: callsTable.callbackAt,
        notes: callsTable.notes,
        outcome: callsTable.outcome,
        leadId: callsTable.leadId,
        businessName: leadsTable.businessName,
      })
      .from(callsTable)
      .leftJoin(leadsTable, eq(callsTable.leadId, leadsTable.id))
      .where(callConditions)
      .orderBy(asc(callsTable.callbackAt));

    res.json({ tasks, reminders, callbacks });
  } catch (e: any) {
    console.error("Calendar error:", e);
    res.status(500).json({ error: "Failed to fetch calendar data" });
  }
});

router.post("/tasks", requireAuth, async (req, res) => {
  try {
    const user = (req as any).user;
    const { title, description, priority, dueDate, assignedToId, leadId } = req.body;

    if (!title) return res.status(400).json({ error: "Title is required" });

    const validPriorities = ["low", "medium", "high"];
    const taskPriority = validPriorities.includes(priority) ? priority : "medium";

    const isAdmin = user.role === "admin" || user.role === "super_admin";
    const targetAssignee = isAdmin && assignedToId ? assignedToId : user.id;

    const [task] = await db
      .insert(tasksTable)
      .values({
        title,
        description: description || null,
        priority: taskPriority,
        dueDate: dueDate ? new Date(dueDate) : null,
        assignedToId: targetAssignee,
        assignedById: user.id,
        leadId: leadId || null,
      })
      .returning();

    res.json(task);
  } catch (e: any) {
    res.status(500).json({ error: "Failed to create task" });
  }
});

router.patch("/tasks/:id", requireAuth, async (req, res) => {
  try {
    const user = (req as any).user;
    const id = parseInt(String(req.params.id), 10);
    const { title, description, priority, status, dueDate, assignedToId } = req.body;

    const [existing] = await db.select().from(tasksTable).where(eq(tasksTable.id, id));
    if (!existing) return res.status(404).json({ error: "Task not found" });

    const isAdmin = user.role === "admin" || user.role === "super_admin";
    if (!isAdmin && existing.assignedToId !== user.id && existing.assignedById !== user.id) {
      return res.status(403).json({ error: "Not authorized" });
    }

    const validPriorities = ["low", "medium", "high"];
    const validStatuses = ["pending", "in_progress", "completed"];

    const updates: any = {};
    if (title !== undefined) updates.title = title;
    if (description !== undefined) updates.description = description;
    if (priority !== undefined && validPriorities.includes(priority)) updates.priority = priority;
    if (status !== undefined && validStatuses.includes(status)) {
      updates.status = status;
      if (status === "completed") updates.completedAt = new Date();
      if (status === "pending" || status === "in_progress") updates.completedAt = null;
    }
    if (dueDate !== undefined) updates.dueDate = dueDate ? new Date(dueDate) : null;
    if (assignedToId !== undefined && isAdmin) updates.assignedToId = assignedToId;

    const [updated] = await db.update(tasksTable).set(updates).where(eq(tasksTable.id, id)).returning();
    res.json(updated);
  } catch (e: any) {
    res.status(500).json({ error: "Failed to update task" });
  }
});

router.delete("/tasks/:id", requireAuth, async (req, res) => {
  try {
    const user = (req as any).user;
    const id = parseInt(String(req.params.id), 10);
    const isAdmin = user.role === "admin" || user.role === "super_admin";

    const [existing] = await db.select().from(tasksTable).where(eq(tasksTable.id, id));
    if (!existing) return res.status(404).json({ error: "Task not found" });

    if (!isAdmin && existing.assignedById !== user.id) {
      return res.status(403).json({ error: "Only the creator or admin can delete tasks" });
    }

    await db.delete(tasksTable).where(eq(tasksTable.id, id));
    res.json({ success: true });
  } catch (e: any) {
    res.status(500).json({ error: "Failed to delete task" });
  }
});

export default router;
