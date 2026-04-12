import { db, documentsTable } from './src/configs/database';
import { eq } from 'drizzle-orm';
import { extractTextFromDocument } from './src/controllers/analysis/coreController';

async function main() {
  const docs = await db.select({ id: documentsTable.id, name: documentsTable.name, url: documentsTable.url, storageKey: documentsTable.storageKey })
    .from(documentsTable)
    .where(eq(documentsTable.leadId, 100659));
  
  for (const d of docs) {
    if (!d.name?.includes("9531001")) continue;
    console.log("Re-extracting:", d.name);
    console.log("URL:", d.url);
    const text = await extractTextFromDocument(d.url!, d.storageKey);
    const lines = text.split("\n");
    console.log("Total lines:", lines.length);
    for (const line of lines) {
      const trimmed = line.trim();
      if (/^\d{2}\/\d{2}/.test(trimmed)) {
        console.log("  LINE:", trimmed.slice(0, 120));
      }
    }
    break;
  }
  process.exit(0);
}
main();
