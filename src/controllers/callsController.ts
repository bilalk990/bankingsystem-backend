import { Router, type IRouter } from "express";
import { eq } from "drizzle-orm";
import { db, callsTable, leadsTable, usersTable, activitiesTable } from "../configs/database";
import {
  LogCallParams,
  LogCallBody,
  GetLeadCallsParams,
  GetLeadCallsResponse,
} from "../validationSchemas";
import { requireAuth, checkLeadOwnership } from "../middlewares/authMiddleware";
import { createSmartReminder } from "./smart-remindersController";

const router: IRouter = Router();

router.get("/leads/:id/calls", requireAuth, async (req, res): Promise<void> => {
  const params = GetLeadCallsParams.safeParse(req.params);
  if (!params.success) {
    res.status(400).json({ error: params.error.message });
    return;
  }

  const user = (req as any).user;
  if (!(await checkLeadOwnership(params.data.id, user, res))) return;

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
    .where(eq(callsTable.leadId, params.data.id))
    .orderBy(callsTable.createdAt);

  res.json(GetLeadCallsResponse.parse(calls));
});

router.post("/leads/:id/calls", requireAuth, async (req, res): Promise<void> => {
  const params = LogCallParams.safeParse(req.params);
  if (!params.success) {
    res.status(400).json({ error: params.error.message });
    return;
  }

  const parsed = LogCallBody.safeParse(req.body);
  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.message });
    return;
  }

  const user = (req as any).user;
  if (!(await checkLeadOwnership(params.data.id, user, res))) return;

  const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, params.data.id));
  if (!lead) {
    res.status(404).json({ error: "Lead not found" });
    return;
  }

  const [call] = await db.insert(callsTable).values({
    leadId: params.data.id,
    userId: user.id,
    outcome: parsed.data.outcome,
    notes: parsed.data.notes || null,
    duration: parsed.data.duration || null,
    callbackAt: parsed.data.callbackAt ? new Date(parsed.data.callbackAt) : null,
  }).returning();

  const isNote = parsed.data.outcome === "note";
  const isCallInitiated = parsed.data.outcome === "call_initiated";

  if (!isNote && !isCallInitiated) {
    let newStatus = lead.status;
    if (parsed.data.outcome === "no_answer") newStatus = "no_answer";
    else if (parsed.data.outcome === "callback") newStatus = "callback";
    else if (parsed.data.outcome === "interested") newStatus = "qualified";
    else if (parsed.data.outcome === "not_interested") newStatus = "not_interested";
    else if (parsed.data.outcome === "voicemail") newStatus = "contacted";

    await db.update(leadsTable).set({
      status: newStatus,
      lastContactedAt: new Date(),
    }).where(eq(leadsTable.id, params.data.id));
  }

  if (isCallInitiated) {
    await db.update(leadsTable).set({
      lastContactedAt: new Date(),
    }).where(eq(leadsTable.id, params.data.id));
  }

  await db.insert(activitiesTable).values({
    leadId: params.data.id,
    userId: user.id,
    type: isNote ? "note" : isCallInitiated ? "call_initiated" : "call",
    description: isNote
      ? `Note added: ${(parsed.data.notes || "").substring(0, 100)}`
      : isCallInitiated
      ? `Call initiated to ${lead.businessName || "lead"}`
      : `Call logged: ${parsed.data.outcome}${parsed.data.duration ? ` (${parsed.data.duration}s)` : ""}`,
    metadata: isNote
      ? { callId: call.id, noteExcerpt: (parsed.data.notes || "").substring(0, 200) }
      : { callId: call.id, outcome: parsed.data.outcome, duration: parsed.data.duration },
  });

  if (parsed.data.outcome === "no_answer") {
    const triggerAt = new Date(Date.now() + 20 * 60 * 1000);
    await createSmartReminder(
      user.id,
      params.data.id,
      "follow_up_call",
      "Follow Up — No Answer",
      `Try calling ${lead.businessName || "this lead"} again. Last attempt got no answer.`,
      triggerAt,
      { outcome: "no_answer", callId: call.id }
    );
  }

  if (parsed.data.outcome === "callback" && parsed.data.callbackAt) {
    const cbTime = new Date(parsed.data.callbackAt);
    await createSmartReminder(
      user.id,
      params.data.id,
      "callback",
      "Scheduled Callback",
      `Callback scheduled with ${lead.businessName || "this lead"}.`,
      cbTime,
      { outcome: "callback", callId: call.id }
    );
  }

  if (parsed.data.notes && parsed.data.notes.length > 10) {
    const interestSignals = ["interested", "wants to move forward", "ready to fund", "send docs", "send application", "very interested", "hot", "wants offer", "sign up", "ready"];
    const noteLower = parsed.data.notes.toLowerCase();
    const hasInterest = interestSignals.some(s => noteLower.includes(s));
    if (hasInterest) {
      await createSmartReminder(
        user.id,
        params.data.id,
        "interest_detected",
        "Interest Detected",
        `Notes suggest strong interest from ${lead.businessName || "this lead"}: "${parsed.data.notes.substring(0, 80)}..."`,
        new Date(Date.now() + 5 * 60 * 1000),
        { noteExcerpt: parsed.data.notes.substring(0, 200) }
      );
    }
  }

  const callWithUser = {
    ...call,
    userName: user.fullName,
  };

  res.status(201).json(callWithUser);
});

export default router;
