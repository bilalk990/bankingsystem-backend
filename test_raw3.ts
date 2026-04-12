import { db, documentsTable } from './src/configs/database';
import { eq } from 'drizzle-orm';

async function main() {
  const docs = await db.select({ id: documentsTable.id, name: documentsTable.name, cachedRawText: documentsTable.cachedRawText })
    .from(documentsTable)
    .where(eq(documentsTable.leadId, 100659));
  
  for (const d of docs) {
    if (!d.name?.includes("9531001")) continue;
    const text = d.cachedRawText || '';
    const lines = text.split("\n");
    for (const line of lines) {
      const trimmed = line.trim();
      if (/^\d{2}\/\d{2}/.test(trimmed)) {
        console.log(`RAW: [${trimmed}]`);
      }
    }
    break;
  }
  process.exit(0);
}
main();
