import { db, documentsTable } from './src/configs/database';
import { eq } from 'drizzle-orm';
import { Client } from "@replit/object-storage";
import fs from "fs";

async function main() {
  const client = new Client();
  const docs = await db.select({ id: documentsTable.id, name: documentsTable.name, storageKey: documentsTable.storageKey })
    .from(documentsTable)
    .where(eq(documentsTable.leadId, 100659));
  
  for (const d of docs) {
    if (d.name?.includes("9531001") && d.storageKey) {
      console.log("Downloading:", d.storageKey);
      const { ok, value } = await client.downloadAsBytes(d.storageKey);
      if (ok && value) {
        fs.writeFileSync("/tmp/chase_test.pdf", Buffer.from(value));
        console.log("Written to /tmp/chase_test.pdf, size:", value.length);
      } else {
        console.log("Download failed");
      }
      break;
    }
  }
  process.exit(0);
}
main();
