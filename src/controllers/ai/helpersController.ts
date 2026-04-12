import { eq, count, sum, desc, gte, sql, ilike, or, and } from "drizzle-orm";
import {
  db,
  leadsTable,
  dealsTable,
  callsTable,
  commissionsTable,
  activitiesTable,
  usersTable,
  documentsTable,
  bankStatementAnalysesTable,
} from "../../configs/database";
import { anthropic } from "../../integrations/anthropic";
import { acquireAiSlot, releaseAiSlot } from "../analysis/coreController";

export const AI_MODEL = "claude-sonnet-4-5";

export async function callClaude(system: string, userContent: string, options: { maxTokens?: number; jsonMode?: boolean } = {}): Promise<string> {
  const { maxTokens = 8192, jsonMode = false } = options;
  const systemPrompt = jsonMode
    ? system + "\n\nIMPORTANT: Return ONLY valid JSON. No markdown formatting, no code blocks, no explanatory text outside the JSON."
    : system;

  await acquireAiSlot();
  try {
    const response = await anthropic.messages.create({
      model: AI_MODEL,
      max_tokens: maxTokens,
      system: systemPrompt,
      messages: [{ role: "user", content: userContent }],
    });

    const block = response.content[0];
    return block.type === "text" ? block.text : "";
  } finally {
    releaseAiSlot();
  }
}

export async function callClaudeWithHistory(system: string, messages: Array<{ role: "user" | "assistant"; content: string }>, options: { maxTokens?: number } = {}): Promise<string> {
  const { maxTokens = 8192 } = options;

  await acquireAiSlot();
  try {
    const response = await anthropic.messages.create({
      model: AI_MODEL,
      max_tokens: maxTokens,
      system,
      messages,
    });

    const block = response.content[0];
    return block.type === "text" ? block.text : "";
  } finally {
    releaseAiSlot();
  }
}

export async function getBusinessSnapshot() {
  const now = new Date();
  const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
  const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);

  const [
    totalLeads,
    todayLeads,
    recentLeads,
    weekLeads,
    leadsByStatus,
    totalDeals,
    dealsByStage,
    fundedDeals,
    totalFundedAmount,
    recentCalls,
    commissionStats,
    topReps,
    riskDistribution,
    recentActivity,
    scrubbingStats,
    documentStats,
    bankStatementAnalysisCount,
  ] = await Promise.all([
    db.select({ count: count() }).from(leadsTable),
    db.select({ count: count() }).from(leadsTable).where(gte(leadsTable.createdAt, todayStart)),
    db.select({ count: count() }).from(leadsTable).where(gte(leadsTable.createdAt, thirtyDaysAgo)),
    db.select({ count: count() }).from(leadsTable).where(gte(leadsTable.createdAt, sevenDaysAgo)),
    db.select({ status: leadsTable.status, count: count() }).from(leadsTable).groupBy(leadsTable.status),
    db.select({ count: count() }).from(dealsTable),
    db.select({ stage: dealsTable.stage, count: count(), total: sum(dealsTable.amount) }).from(dealsTable).groupBy(dealsTable.stage),
    db.select({ count: count(), total: sum(dealsTable.amount) }).from(dealsTable).where(eq(dealsTable.stage, "funded")),
    db.select({ total: sum(dealsTable.amount) }).from(dealsTable).where(eq(dealsTable.stage, "funded")),
    db.select({ count: count() }).from(callsTable).where(gte(callsTable.createdAt, sevenDaysAgo)),
    db.select({ total: sum(commissionsTable.amount), count: count() }).from(commissionsTable),
    db.select({
      repId: leadsTable.assignedToId,
      repName: usersTable.fullName,
      leadCount: count(),
    }).from(leadsTable)
      .leftJoin(usersTable, eq(leadsTable.assignedToId, usersTable.id))
      .where(sql`${leadsTable.assignedToId} IS NOT NULL`)
      .groupBy(leadsTable.assignedToId, usersTable.fullName)
      .orderBy(desc(count()))
      .limit(5),
    db.select({ risk: leadsTable.riskCategory, count: count() }).from(leadsTable)
      .where(sql`${leadsTable.riskCategory} IS NOT NULL`)
      .groupBy(leadsTable.riskCategory),
    db.select({
      type: activitiesTable.type,
      description: activitiesTable.description,
      createdAt: activitiesTable.createdAt,
    }).from(activitiesTable).orderBy(desc(activitiesTable.createdAt)).limit(10),
    db.select({ status: leadsTable.bankStatementsStatus, count: count() })
      .from(leadsTable)
      .where(sql`${leadsTable.bankStatementsStatus} IS NOT NULL AND ${leadsTable.bankStatementsStatus} != 'none'`)
      .groupBy(leadsTable.bankStatementsStatus),
    db.select({ type: documentsTable.type, count: count() })
      .from(documentsTable)
      .groupBy(documentsTable.type),
    db.select({ count: count() }).from(bankStatementAnalysesTable),
  ]);

  const scrubbedCount = scrubbingStats.find(s => s.status === "scrubbed")?.count || 0;
  const reviewCount = scrubbingStats.find(s => s.status === "scrubbing_review" || s.status === "review")?.count || 0;
  const analyzedCount = scrubbingStats.find(s => s.status === "analyzed")?.count || 0;

  const docsByType: Record<string, number> = {};
  for (const d of documentStats) {
    docsByType[d.type || "unknown"] = Number(d.count);
  }

  return {
    totalLeads: totalLeads[0]?.count || 0,
    leadsToday: todayLeads[0]?.count || 0,
    leadsThisWeek: weekLeads[0]?.count || 0,
    leadsLast30Days: recentLeads[0]?.count || 0,
    leadsByStatus,
    totalDeals: totalDeals[0]?.count || 0,
    dealsByStage,
    fundedDeals: { count: fundedDeals[0]?.count || 0, total: fundedDeals[0]?.total || 0 },
    totalFundedAmount: totalFundedAmount[0]?.total || 0,
    callsThisWeek: recentCalls[0]?.count || 0,
    commissions: { total: commissionStats[0]?.total || 0, count: commissionStats[0]?.count || 0 },
    topReps,
    riskDistribution,
    scrubbing: {
      totalAnalyses: bankStatementAnalysisCount[0]?.count || 0,
      scrubbed: scrubbedCount,
      needsReview: reviewCount,
      analyzed: analyzedCount,
    },
    documents: docsByType,
    recentActivity: recentActivity.map(a => ({
      type: a.type,
      description: a.description,
      when: a.createdAt,
    })),
  };
}

export async function searchLeadByName(query: string) {
  const searchTerms = query.trim().split(/\s+/).filter(t => t.length >= 2);
  if (searchTerms.length === 0) return [];

  const conditions = searchTerms.map(term => {
    const like = `%${term}%`;
    return or(
      ilike(leadsTable.businessName, like),
      ilike(leadsTable.ownerName, like),
      ilike(leadsTable.dba, like),
    );
  });

  const leads = await db.select({
    id: leadsTable.id,
    businessName: leadsTable.businessName,
    ownerName: leadsTable.ownerName,
    phone: leadsTable.phone,
    status: leadsTable.status,
  }).from(leadsTable)
    .where(and(...conditions.filter(Boolean) as any))
    .orderBy(desc(leadsTable.createdAt))
    .limit(5);

  return leads;
}

export async function getLeadContext(leadId: number) {
  const lead = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId)).limit(1);
  if (!lead[0]) return null;

  const [deals, calls, docs, analyses] = await Promise.all([
    db.select().from(dealsTable).where(eq(dealsTable.leadId, leadId)),
    db.select().from(callsTable).where(eq(callsTable.leadId, leadId)).orderBy(desc(callsTable.createdAt)).limit(10),
    db.select().from(documentsTable).where(eq(documentsTable.leadId, leadId)),
    db.select().from(bankStatementAnalysesTable).where(eq(bankStatementAnalysesTable.leadId, leadId)).orderBy(desc(bankStatementAnalysesTable.createdAt)),
  ]);

  const l = lead[0];

  const bankStatements = docs.filter(d => d.type === "bank_statement");
  const voidedChecks = docs.filter(d => d.type === "voided_check");
  const photoIds = docs.filter(d => d.type === "photo_id");

  const monthsFromAnalyses: string[] = [];
  for (const a of analyses) {
    const revs = a.monthlyRevenues as any[];
    if (revs && Array.isArray(revs)) {
      for (const r of revs) {
        if (r.month && !monthsFromAnalyses.includes(r.month)) {
          monthsFromAnalyses.push(r.month);
        }
      }
    }
  }
  monthsFromAnalyses.sort().reverse();

  const allLoans: any[] = [];
  for (const a of analyses) {
    const loans = a.loanDetails as any[];
    if (loans && Array.isArray(loans)) {
      allLoans.push(...loans);
    }
  }

  let totalGrossRevenue = 0;
  for (const a of analyses) {
    totalGrossRevenue += Number(a.grossRevenue) || 0;
  }

  return {
    id: l.id,
    businessName: l.businessName,
    dba: l.dba,
    ownerName: l.ownerName,
    email: l.email,
    phone: l.phone,
    status: l.status,
    source: l.source,
    state: l.state,
    industry: l.industry,
    businessType: l.businessType,
    yearsInBusiness: l.yearsInBusiness,
    creditScore: l.creditScore,
    requestedAmount: l.requestedAmount,
    monthlyRevenue: l.monthlyRevenue,
    riskCategory: l.riskCategory,
    estimatedApproval: l.estimatedApproval,
    hasExistingLoans: l.hasExistingLoans,
    loanCount: l.loanCount,
    existingLoanDetails: allLoans.length > 0 ? allLoans : l.loanDetails,
    avgDailyBalance: l.avgDailyBalance,
    revenueTrend: l.revenueTrend,
    grossRevenue: totalGrossRevenue || l.grossRevenue,
    ownershipPercentage: l.ownershipPct,
    assignedTo: l.assignedToId,
    createdAt: l.createdAt,
    documents: {
      bankStatements: {
        count: bankStatements.length,
        fileNames: bankStatements.map(d => d.name),
      },
      voidedCheck: {
        has: voidedChecks.length > 0,
        count: voidedChecks.length,
      },
      photoId: {
        has: photoIds.length > 0,
        count: photoIds.length,
      },
      totalDocuments: docs.length,
      allTypes: [...new Set(docs.map(d => d.type))],
    },
    bankStatementAnalysis: {
      isScrubbed: analyses.length > 0,
      analysisCount: analyses.length,
      monthsCovered: monthsFromAnalyses,
      riskScore: analyses[0]?.riskScore || null,
      hasLoans: analyses.some(a => a.hasLoans),
      hasOnDeck: analyses.some(a => a.hasOnDeck),
      loanDetails: allLoans,
      monthlyRevenues: analyses.flatMap(a => {
        const revs = a.monthlyRevenues as any[];
        return Array.isArray(revs) ? revs : [];
      }),
      riskFactors: analyses.flatMap(a => {
        const rf = a.riskFactors as string[];
        return Array.isArray(rf) ? rf : [];
      }),
      nsfCount: analyses.reduce((s, a) => s + (a.nsfCount || 0), 0),
    },
    deals: deals.map(d => ({ stage: d.stage, amount: d.amount, factorRate: d.factorRate, funderId: d.funderId, createdAt: d.createdAt })),
    callHistory: calls.map(c => ({ outcome: c.outcome, notes: c.notes, date: c.createdAt })),
  };
}

export const ADMIN_SYSTEM_PROMPT = `You are Bridge Capital AI — the intelligent assistant powering Bridge Capital's funding platform. You are an expert in merchant cash advance (MCA), business lending, underwriting, and sales strategy.

Your personality: Confident, sharp, data-driven. You speak like a senior funding advisor — concise but thorough. Use numbers and specifics whenever possible. Format with markdown for readability.

BUSINESS DATA ACCESS:
You have REAL-TIME access to the entire business database. You can answer any question about:
- **Lead counts**: total leads, leads today, this week, this month, by status (new, contacted, funded, etc.)
- **Scrubbing/Analysis**: how many leads have been scrubbed, need review, how many bank statement analyses exist
- **Funded deals**: how many deals funded, total funded amount, deals by stage
- **Documents**: how many bank statements, voided checks, photo IDs are in the system
- **Risk breakdown**: how many A1, A2, B1, B2, C rated leads
- **Team performance**: top reps by lead count, call activity
- **Commissions**: total commissions earned
- **Recent activity**: latest system activities

LEAD-SPECIFIC ACCESS:
When a user asks about a specific lead (by name or ID), you have access to:
- Full contact info and business details
- **Document completeness**: which documents they have (bank statements, voided check, photo ID) with file names
- **Bank statement months**: exactly which months of bank statements are on file
- **Scrub/analysis results**: risk score, revenue data, loan details, NSF counts, risk factors
- **Deal history**: any submitted or funded deals
- **Call history**: recent calls and outcomes

HOW TO GUIDE USERS:
- If they ask where to find something, tell them which page/section to go to (e.g. "Go to Scrubbing page", "Open the lead and check the Info tab")
- If they need to upload documents, tell them to go to the lead page and scroll to Upload Documents
- If they need to scrub, explain they can either use the Scrubbing page or the Scrub button on the lead's Info tab
- Explain what risk scores mean (A1 = best, C = highest risk)
- Help them understand the pipeline flow: Import → Scrubbing → Underwriting → Submit → Funded

IMPORTANT RULES:
- Always use the actual data provided. Never make up numbers.
- When asked about document completeness, be specific: "They have 3 bank statements (Jan, Feb, Mar 2026), a voided check, but no photo ID"
- When asked "how many" type questions, give exact counts from the snapshot data
- Keep responses focused and actionable. Use bullet points and headers for complex answers.`;

export const REP_SYSTEM_PROMPT = `You are Bridge Capital AI — a personal sales coach and mentor for merchant cash advance (MCA) sales reps.

Your personality: Supportive, motivating, and practical. You speak like a seasoned sales coach — encouraging but direct. Format with markdown for readability.

STRICT RULES — YOU MUST FOLLOW THESE:
- You are ONLY a personal sales coach and guide. You help reps improve their sales skills, handle objections, craft pitches, and stay motivated.
- You MUST NOT reveal any company data, deal counts, pipeline numbers, revenue figures, commission amounts, lead counts, conversion rates, or any business metrics — even if you have access to such data internally.
- You MUST NOT answer questions about how many deals were funded, how the company is performing, what other reps are doing, or any sensitive business information.
- If the user asks for company data, metrics, deal counts, pipeline info, or any sensitive information, politely redirect them: "I'm here to help you sharpen your sales skills and close more deals! For company metrics and reports, check out the Analytics or Reports pages. What can I help you with on the sales side?"
- You MUST NOT share information about other employees, their performance, or any internal company details.

Your capabilities:
- Coach on MCA sales techniques, cold calling, and closing strategies
- Help craft call scripts and talking points for merchant outreach
- Teach objection handling (pricing, competitor comparisons, timing concerns)
- Provide tips on building rapport with business owners
- Offer guidance on time management and daily sales routines
- Motivate and encourage the rep to stay focused and hit their goals
- Explain MCA industry concepts (factor rates, terms, qualifications)
- Suggest best practices for follow-ups and pipeline management techniques (general, not company-specific data)
- Guide them through the system: where to find leads, how to upload documents, how to submit to funders

Keep responses encouraging, practical, and focused on helping the rep become a better salesperson.`;
