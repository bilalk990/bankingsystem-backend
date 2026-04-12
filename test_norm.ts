import { db, documentsTable } from './src/configs/database';
import { eq } from 'drizzle-orm';

async function main() {
  const docs = await db.select({ cachedRawText: documentsTable.cachedRawText })
    .from(documentsTable)
    .where(eq(documentsTable.leadId, 100659));
  
  const text = docs[0].cachedRawText || '';
  const lines = text.split('\n');
  
  // Show deposit area with line numbers
  for (let i = 55; i < 100; i++) {
    console.log(`${i}: ${JSON.stringify(lines[i])}`);
  }
  
  // Show some Zelle lines
  console.log('\n=== ZELLE LINES ===');
  lines.forEach((l: string, i: number) => {
    if (/Zelle/i.test(l)) console.log(`${i}: ${JSON.stringify(l)}`);
  });
  
  // Show deposit lines
  console.log('\n=== DEPOSIT LINES ===');
  lines.forEach((l: string, i: number) => {
    if (/^12\/(09|15|17|18|22|30|31).*\d+\.\d{2}$/.test(l.trim())) console.log(`${i}: ${JSON.stringify(l)}`);
  });
  
  process.exit(0);
}
main();
