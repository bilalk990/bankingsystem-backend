import { Router, type IRouter } from "express";
import { eq, and } from "drizzle-orm";
import { db, submissionsTable, dealsTable, fundersTable, leadsTable, activitiesTable } from "../configs/database";
import { requireAuth } from "../middlewares/authMiddleware";

async function checkDealAccess(dealId: number, user: any, res: any): Promise<boolean> {
  const [deal] = await db.select().from(dealsTable).where(eq(dealsTable.id, dealId));
  if (!deal) { res.status(404).json({ error: "Deal not found" }); return false; }
  if (user.role === "rep" && deal.repId !== user.id) { res.status(403).json({ error: "Access denied" }); return false; }
  return true;
}

const router: IRouter = Router();

router.get("/deals/:dealId/submissions", requireAuth, async (req, res): Promise<void> => {
  const dealId = parseInt(String(req.params.dealId), 10);
  const user = (req as any).user;
  if (!(await checkDealAccess(dealId, user, res))) return;

  const submissions = await db.select({
    id: submissionsTable.id,
    dealId: submissionsTable.dealId,
    funderId: submissionsTable.funderId,
    funderName: fundersTable.name,
    status: submissionsTable.status,
    submittedAt: submissionsTable.submittedAt,
    approvedAmount: submissionsTable.approvedAmount,
    approvedFactorRate: submissionsTable.approvedFactorRate,
    approvedTerm: submissionsTable.approvedTerm,
    declineReason: submissionsTable.declineReason,
    notes: submissionsTable.notes,
    respondedAt: submissionsTable.respondedAt,
  }).from(submissionsTable)
    .leftJoin(fundersTable, eq(submissionsTable.funderId, fundersTable.id))
    .where(eq(submissionsTable.dealId, dealId))
    .orderBy(submissionsTable.submittedAt);

  res.json(submissions);
});

router.post("/deals/:dealId/submissions", requireAuth, async (req, res): Promise<void> => {
  const dealId = parseInt(String(req.params.dealId), 10);
  const user = (req as any).user;
  const { funderId, notes } = req.body;

  if (!funderId) {
    res.status(400).json({ error: "Funder ID is required" });
    return;
  }

  const [deal] = await db.select().from(dealsTable).where(eq(dealsTable.id, dealId));
  if (!deal) {
    res.status(404).json({ error: "Deal not found" });
    return;
  }
  if (user.role === "rep" && deal.repId !== user.id) {
    res.status(403).json({ error: "Access denied" });
    return;
  }

  const [funder] = await db.select().from(fundersTable).where(eq(fundersTable.id, funderId));
  if (!funder) {
    res.status(404).json({ error: "Funder not found" });
    return;
  }

  const [submission] = await db.insert(submissionsTable).values({
    dealId,
    funderId,
    status: "pending",
    notes: notes || null,
  }).returning();

  await db.update(dealsTable).set({ stage: "underwriting" }).where(eq(dealsTable.id, dealId));

  await db.insert(activitiesTable).values({
    leadId: deal.leadId,
    userId: user.id,
    type: "submission",
    description: `Deal submitted to ${funder.name}`,
    metadata: { dealId, funderId, submissionId: submission.id },
  });

  res.status(201).json({ ...submission, funderName: funder.name });
});

router.patch("/submissions/:id", requireAuth, async (req, res): Promise<void> => {
  const id = parseInt(String(req.params.id), 10);
  const user = (req as any).user;

  if (user.role === "rep") {
    res.status(403).json({ error: "Only managers can update submissions" });
    return;
  }

  const { status, approvedAmount, approvedFactorRate, approvedTerm, declineReason, notes } = req.body;

  const updateData: any = {};
  if (status) updateData.status = status;
  if (approvedAmount !== undefined) updateData.approvedAmount = approvedAmount;
  if (approvedFactorRate !== undefined) updateData.approvedFactorRate = approvedFactorRate;
  if (approvedTerm !== undefined) updateData.approvedTerm = approvedTerm;
  if (declineReason !== undefined) updateData.declineReason = declineReason;
  if (notes !== undefined) updateData.notes = notes;
  if (status === "approved" || status === "declined") {
    updateData.respondedAt = new Date();
  }

  const [submission] = await db.update(submissionsTable).set(updateData).where(eq(submissionsTable.id, id)).returning();
  if (!submission) {
    res.status(404).json({ error: "Submission not found" });
    return;
  }

  const [deal] = await db.select().from(dealsTable).where(eq(dealsTable.id, submission.dealId));
  if (deal) {
    const user = (req as any).user;
    const [funder] = await db.select().from(fundersTable).where(eq(fundersTable.id, submission.funderId));
    await db.insert(activitiesTable).values({
      leadId: deal.leadId,
      userId: user.id,
      type: "submission_update",
      description: `Submission to ${funder?.name || "funder"} ${status}`,
      metadata: { submissionId: id, status },
    });

    if (status === "approved" && approvedAmount) {
      await db.update(dealsTable).set({
        stage: "approved",
        funderId: submission.funderId,
        factorRate: approvedFactorRate || deal.factorRate,
        paybackAmount: String(approvedAmount * (approvedFactorRate || deal.factorRate || 1)),
        term: approvedTerm || deal.term,
      }).where(eq(dealsTable.id, deal.id));
    }
  }

  res.json(submission);
});

export default router;
