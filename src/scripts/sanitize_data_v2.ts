import { db } from '../configs/database';
import { sql } from 'drizzle-orm';

async function sanitize() {
  console.log('--- Starting Robust Data Sanitization ---');

  const tablesToClean = [
    { name: 'leads', columns: ['gross_revenue', 'monthly_revenue', 'requested_amount', 'avg_daily_balance'] },
    { name: 'bank_statement_analyses', columns: ['gross_revenue', 'avg_daily_balance'] },
    { name: 'deals', columns: ['amount', 'factor_rate', 'payback_amount', 'commission', 'payment_amount', 'default_amount'] }
  ];

  for (const table of tablesToClean) {
    for (const col of table.columns) {
      console.log(`Cleaning ${table.name}.${col}...`);
      try {
        await db.execute(sql.raw(`UPDATE ${table.name} SET ${col} = '0' WHERE ${col}::text = 'NaN' OR ${col} IS NULL`));
      } catch (err: any) {
        console.warn(`Could not clean ${table.name}.${col}: ${err.message}`);
      }
    }
  }

  console.log('--- Sanitization Complete ---');
  process.exit(0);
}

sanitize().catch(err => {
  console.error('Sanitization failed:', err);
  process.exit(1);
});
