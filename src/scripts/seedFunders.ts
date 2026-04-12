import { db, fundersTable } from "../configs/database";
import { eq } from "drizzle-orm";

export async function seedDefaultFunders() {
  const existing = await db
    .select({ id: fundersTable.id })
    .from(fundersTable)
    .where(eq(fundersTable.name, "Bridge Capital"));

  if (existing.length > 0) return;

  await db.insert(fundersTable).values({
    name: "Bridge Capital",
    type: "in_house",
    description: "Our direct funding arm. Fast approvals, competitive rates, and full control over the deal lifecycle.",
    contactName: "Bridge Capital Team",
    contactEmail: "funding@bridgecapital.com",
    website: "https://bridgecapital.com",
    minAmount: 5000,
    maxAmount: 500000,
    minCreditScore: 500,
    minTimeInBusiness: 6,
    maxPositions: 6,
    defaultFactorRate: 1.25,
    commissionPct: 10,
    paymentFrequency: "daily",
    notes: "Internal funder — Bridge Consolidations' own capital deployment.",
    active: true,
  });

  console.log("Bridge Capital funder seeded successfully");
}
