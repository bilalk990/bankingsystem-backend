import { db } from './src/db';
import { sql } from 'drizzle-orm';
async function main() {
  const results = await db.execute(sql`
    SELECT a.result
    FROM analyses a
    JOIN leads l ON a.lead_id = l.id
    WHERE l.business_name ILIKE '%atlas%mountain%'
    ORDER BY a.created_at DESC
    LIMIT 1
  `);
  if (results.rows.length > 0) {
    const result = results.rows[0].result as any;
    const loans = result.loanDetails || [];
    console.log('Loan details:');
    for (const l of loans) {
      console.log(JSON.stringify({ lender: l.lender, amount: l.amount, frequency: l.frequency, occurrences: l.occurrences, fundedAmount: l.fundedAmount, fundedDate: l.fundedDate, possiblyPaidOff: l.possiblyPaidOff }));
    }
  }
  process.exit(0);
}
main();
