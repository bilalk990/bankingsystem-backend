import { db, documentsTable } from './src/configs/database';
import { eq } from 'drizzle-orm';

async function main() {
  const docs = await db.select({ cachedRawText: documentsTable.cachedRawText }).from(documentsTable).where(eq(documentsTable.leadId, 100659));
  for (const d of docs) {
    const text = d.cachedRawText || '';
    const lines = text.split('\n');
    for (const line of lines) {
      const trimmed = line.trim();
      if (/Card/i.test(trimmed) && /\d+\.\d{2}$/.test(trimmed) && /^\d{2}\/\d{2}/.test(trimmed)) {
        const idx = trimmed.lastIndexOf('Card');
        const around = trimmed.slice(idx, idx + 20);
        const codes: number[] = [];
        for (let i = 0; i < around.length; i++) codes.push(around.charCodeAt(i));
        console.log('AROUND:', JSON.stringify(around), 'CODES:', codes.join(','));
        break;
      }
    }
    break;
  }
  process.exit(0);
}
main();
