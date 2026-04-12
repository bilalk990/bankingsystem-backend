import "dotenv/config";
import { db, usersTable } from "./src/configs/database.ts";
import { eq } from "drizzle-orm";
import bcrypt from "bcryptjs";

async function updatePassword() {
  const email = "admin@cashadvance.com";
  const newPassword = "5555";
  const hashedPassword = await bcrypt.hash(newPassword, 10);

  await db.update(usersTable)
    .set({ password: hashedPassword })
    .where(eq(usersTable.email, email));

  console.log(`Password for ${email} updated to 5555`);
  process.exit(0);
}

updatePassword().catch(console.error);
