import "dotenv/config";
import { db, usersTable } from "./src/configs/database.ts";

async function checkUsers() {
  const users = await db.select().from(usersTable);
  console.log("Users in DB:", users.map(u => ({ id: u.id, email: u.email, role: u.role, fullName: u.fullName })));
  process.exit(0);
}

checkUsers().catch(console.error);
