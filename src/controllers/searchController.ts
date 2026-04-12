import { Router, type IRouter } from "express";
import { sql, or, ilike, eq, and } from "drizzle-orm";
import { db, leadsTable, dealsTable, fundersTable, usersTable } from "../configs/database";
import { requireAuth, requireAdmin } from "../middlewares/authMiddleware";
import { decryptLeadFields, hmacHash, normalizeSsnForHash } from "../utils/encryption";

const router: IRouter = Router();

router.get("/search", requireAuth, async (req, res): Promise<void> => {
  const q = (req.query.q as string || "").trim();
  if (!q || q.length < 2) {
    res.json({ leads: [], deals: [], funders: [], users: [] });
    return;
  }

  const user = (req as any).user;
  const isAdmin = ["admin", "manager", "super_admin"].includes(user.role);
  const pattern = `%${q}%`;

  const ssnDigits = q.replace(/\D/g, "");
  const isSsnQuery = ssnDigits.length >= 4 && /^[\d\s\-]+$/.test(q);

  const leadConditions: any[] = [
    ilike(leadsTable.businessName, pattern),
    ilike(leadsTable.ownerName, pattern),
    ilike(leadsTable.phone, pattern),
    ilike(leadsTable.email, pattern),
  ];

  if (isAdmin) {
    leadConditions.push(
      ilike(leadsTable.dba, pattern),
      ilike(leadsTable.address, pattern),
      ilike(leadsTable.city, pattern),
      ilike(leadsTable.state, pattern),
      ilike(leadsTable.zip, pattern),
      ilike(leadsTable.homeAddress, pattern),
      ilike(leadsTable.homeCity, pattern),
      ilike(leadsTable.homeState, pattern),
      ilike(leadsTable.homeZip, pattern),
      ilike(leadsTable.industry, pattern),
      ilike(leadsTable.bankName, pattern),
      ilike(leadsTable.ein, pattern),
    );
  }

  if (isSsnQuery && ssnDigits.length >= 9) {
    leadConditions.push(eq(leadsTable.ssnHash, hmacHash(normalizeSsnForHash(q))));
  }
  if (isSsnQuery && ssnDigits.length >= 4 && ssnDigits.length < 9) {
    leadConditions.push(sql`RIGHT(${leadsTable.ssn}, 4) = ${ssnDigits.slice(-4)}`);
  }

  const leadWhere = or(...leadConditions);

  const leadFilter = user.role === "rep"
    ? and(leadWhere, eq(leadsTable.assignedToId, user.id))
    : leadWhere;

  const dealJoinFilter = user.role === "rep"
    ? and(
        or(ilike(leadsTable.businessName, pattern), ilike(usersTable.fullName, pattern)),
        eq(dealsTable.repId, user.id)
      )
    : or(ilike(leadsTable.businessName, pattern), ilike(usersTable.fullName, pattern));

  const [leads, deals, funders, users] = await Promise.all([
    db.select({
      id: leadsTable.id,
      businessName: leadsTable.businessName,
      dba: leadsTable.dba,
      ownerName: leadsTable.ownerName,
      phone: leadsTable.phone,
      email: leadsTable.email,
      status: leadsTable.status,
      riskCategory: leadsTable.riskCategory,
      city: leadsTable.city,
      state: leadsTable.state,
      industry: leadsTable.industry,
      bankName: leadsTable.bankName,
      monthlyRevenue: leadsTable.monthlyRevenue,
      hasExistingLoans: leadsTable.hasExistingLoans,
      loanCount: leadsTable.loanCount,
      assignedToId: leadsTable.assignedToId,
    }).from(leadsTable)
      .where(leadFilter!)
      .limit(10),

    db.select({
      id: dealsTable.id,
      leadId: dealsTable.leadId,
      businessName: sql<string>`${leadsTable.businessName}`,
      amount: dealsTable.amount,
      stage: dealsTable.stage,
      repName: usersTable.fullName,
    }).from(dealsTable)
      .leftJoin(leadsTable, sql`${dealsTable.leadId} = ${leadsTable.id}`)
      .leftJoin(usersTable, sql`${dealsTable.repId} = ${usersTable.id}`)
      .where(dealJoinFilter!)
      .limit(5),

    isAdmin
      ? db.select({
          id: fundersTable.id,
          name: fundersTable.name,
          contactEmail: fundersTable.contactEmail,
          active: fundersTable.active,
        }).from(fundersTable)
          .where(or(
            ilike(fundersTable.name, pattern),
            ilike(fundersTable.contactEmail, pattern),
          ))
          .limit(5)
      : Promise.resolve([]),

    isAdmin
      ? db.select({
          id: usersTable.id,
          fullName: usersTable.fullName,
          email: usersTable.email,
          role: usersTable.role,
          active: usersTable.active,
        }).from(usersTable)
          .where(or(
            ilike(usersTable.fullName, pattern),
            ilike(usersTable.email, pattern),
          ))
          .limit(5)
      : Promise.resolve([]),
  ]);

  const enrichedLeads = isAdmin
    ? await Promise.all(leads.map(async (lead) => {
        let assignedRepName: string | null = null;
        if (lead.assignedToId) {
          const [rep] = await db.select({ fullName: usersTable.fullName }).from(usersTable).where(eq(usersTable.id, lead.assignedToId));
          assignedRepName = rep?.fullName || null;
        }
        return { ...lead, assignedRepName };
      }))
    : leads;

  res.json({ leads: enrichedLeads, deals, funders, users });
});

router.get("/search/deep", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  const q = (req.query.q as string || "").trim();
  if (!q || q.length < 2) {
    res.json({ results: [] });
    return;
  }

  const pattern = `%${q}%`;

  const leadWhere = or(
    ilike(leadsTable.businessName, pattern),
    ilike(leadsTable.ownerName, pattern),
    ilike(leadsTable.phone, pattern),
    ilike(leadsTable.email, pattern),
    ilike(leadsTable.address, pattern),
    ilike(leadsTable.city, pattern),
    ilike(leadsTable.state, pattern),
    ilike(leadsTable.zip, pattern),
    ilike(leadsTable.homeAddress, pattern),
    ilike(leadsTable.homeCity, pattern),
    ilike(leadsTable.homeState, pattern),
    ilike(leadsTable.homeZip, pattern),
    ilike(leadsTable.dba, pattern),
    ilike(leadsTable.industry, pattern),
    ilike(leadsTable.bankName, pattern),
  );

  const leads = await db.select({
    id: leadsTable.id,
    businessName: leadsTable.businessName,
    dba: leadsTable.dba,
    ownerName: leadsTable.ownerName,
    email: leadsTable.email,
    phone: leadsTable.phone,
    status: leadsTable.status,
    ssn: leadsTable.ssn,
    ein: leadsTable.ein,
    address: leadsTable.address,
    city: leadsTable.city,
    state: leadsTable.state,
    zip: leadsTable.zip,
    homeAddress: leadsTable.homeAddress,
    homeCity: leadsTable.homeCity,
    homeState: leadsTable.homeState,
    homeZip: leadsTable.homeZip,
    industry: leadsTable.industry,
    businessType: leadsTable.businessType,
    creditScore: leadsTable.creditScore,
    riskCategory: leadsTable.riskCategory,
    monthlyRevenue: leadsTable.monthlyRevenue,
    requestedAmount: leadsTable.requestedAmount,
    bankName: leadsTable.bankName,
    hasExistingLoans: leadsTable.hasExistingLoans,
    loanCount: leadsTable.loanCount,
    dob: leadsTable.dob,
    driversLicense: leadsTable.driversLicense,
    dlState: leadsTable.dlState,
    createdAt: leadsTable.createdAt,
    assignedToId: leadsTable.assignedToId,
  }).from(leadsTable)
    .where(leadWhere!)
    .limit(20);

  const results = await Promise.all(leads.map(async (lead) => {
    const deals = await db.select({
      id: dealsTable.id,
      stage: dealsTable.stage,
      amount: dealsTable.amount,
      factorRate: dealsTable.factorRate,
      paybackAmount: dealsTable.paybackAmount,
      term: dealsTable.term,
      fundedDate: dealsTable.fundedDate,
      fundingSource: dealsTable.fundingSource,
      funderName: dealsTable.funderName,
      paymentFrequency: dealsTable.paymentFrequency,
      paymentAmount: dealsTable.paymentAmount,
      totalPayments: dealsTable.totalPayments,
      paymentsCompleted: dealsTable.paymentsCompleted,
      defaultStatus: dealsTable.defaultStatus,
      defaultedAt: dealsTable.defaultedAt,
      defaultNotes: dealsTable.defaultNotes,
      defaultAmount: dealsTable.defaultAmount,
      repName: usersTable.fullName,
      createdAt: dealsTable.createdAt,
    }).from(dealsTable)
      .leftJoin(usersTable, eq(dealsTable.repId, usersTable.id))
      .where(eq(dealsTable.leadId, lead.id))
      .orderBy(dealsTable.createdAt);

    let assignedRepName: string | null = null;
    if (lead.assignedToId) {
      const [rep] = await db.select({ fullName: usersTable.fullName }).from(usersTable).where(eq(usersTable.id, lead.assignedToId));
      assignedRepName = rep?.fullName || null;
    }

    const decryptedLead = decryptLeadFields(lead);
    const hasFunded = deals.some(d => d.stage === "funded");
    const hasDefaulted = deals.some(d => d.defaultStatus === "defaulted");
    const totalFunded = deals.filter(d => d.stage === "funded").reduce((s, d) => s + (d.amount || 0), 0);

    return {
      ...decryptedLead,
      ssn: decryptedLead.ssn ? `***-**-${decryptedLead.ssn.replace(/[^\d]/g, "").slice(-4)}` : null,
      assignedRepName,
      deals,
      hasFunded,
      hasDefaulted,
      totalFunded,
      dealCount: deals.length,
    };
  }));

  res.json({ results });
});

export default router;
