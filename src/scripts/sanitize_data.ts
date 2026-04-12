import { db, leadsTable, bankStatementAnalysesTable, dealsTable } from '../configs/database';
import { eq, sql } from 'drizzle-orm';

async function sanitize() {
  console.log('--- Starting Data Sanitization ---');

  // Leads cleanup
  const leadsUpdated = await db.update(leadsTable)
    .set({ 
      grossRevenue: sql`CASE WHEN gross_revenue = 'NaN' THEN '0' ELSE gross_revenue END`,
      monthlyRevenue: sql`CASE WHEN monthly_revenue = 'NaN' THEN '0' ELSE monthly_revenue END`,
      requestedAmount: sql`CASE WHEN requested_amount = 'NaN' THEN '0' ELSE requested_amount END`,
      avgDailyBalance: sql`CASE WHEN avg_daily_balance = 'NaN' THEN '0' ELSE avg_daily_balance END`,
      creditScore: sql`CASE WHEN credit_score = 'NaN' THEN '0' ELSE credit_score END`
    })
    .returning();
  console.log(`Cleaned ${leadsUpdated.length} leads.`);

  // Deals cleanup
  const dealsUpdated = await db.update(dealsTable)
    .set({ 
      amount: sql`CASE WHEN amount::text = 'NaN' THEN 0 ELSE amount END`,
      factorRate: sql`CASE WHEN factor_rate::text = 'NaN' THEN 0 ELSE factor_rate END`,
      paybackAmount: sql`CASE WHEN payback_amount::text = 'NaN' THEN 0 ELSE payback_amount END`,
      commission: sql`CASE WHEN commission::text = 'NaN' THEN 0 ELSE commission END`,
      paymentAmount: sql`CASE WHEN payment_amount::text = 'NaN' THEN 0 ELSE payment_amount END`
    })
    .returning();
  console.log(`Cleaned ${dealsUpdated.length} deals.`);

  // Bank Statement Analyses cleanup
  const analysesUpdated = await db.update(bankStatementAnalysesTable)
    .set({ 
      grossRevenue: sql`CASE WHEN gross_revenue = 'NaN' THEN '0' ELSE gross_revenue END`,
      avgDailyBalance: sql`CASE WHEN avg_daily_balance = 'NaN' THEN '0' ELSE avg_daily_balance END`
    })
    .returning();
  console.log(`Cleaned ${analysesUpdated.length} analyses.`);

  console.log('--- Sanitization Complete ---');
  process.exit(0);
}

sanitize().catch(err => {
  console.error('Sanitization failed:', err);
  process.exit(1);
});
