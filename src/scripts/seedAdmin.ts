import { pool } from "../configs/database";
import bcrypt from "bcryptjs";

const ADMIN_EMAIL = "admin@cashadvance.com";
const ADMIN_PASSWORD = "5555";

export async function resetAdminPassword() {
  try {
    const dbInfo = await pool.query("SELECT current_database(), current_user, (SELECT count(*) FROM users) as user_count");
    console.log("[SEED] DB info:", JSON.stringify(dbInfo.rows[0]));
    
    const existing = await pool.query("SELECT id, email FROM users WHERE email = $1", [ADMIN_EMAIL]);
    console.log("[SEED] Found admin users:", existing.rows.length, existing.rows.map((r: any) => `${r.id}:${r.email}`));
    
    const hashed = await bcrypt.hash(ADMIN_PASSWORD, 10);
    if (existing.rows.length === 0) {
      const insertResult = await pool.query(
        `INSERT INTO users (email, password, full_name, role, active, failed_login_attempts, must_change_password, password_changed_at)
         VALUES ($1, $2, 'Admin Owner', 'super_admin', true, 0, false, NOW())
         RETURNING id`,
        [ADMIN_EMAIL, hashed]
      );
      console.log("[SEED] Created admin user with id:", insertResult.rows[0]?.id);
    } else {
      const updateResult = await pool.query(
        `UPDATE users SET password = $1, role = 'super_admin', failed_login_attempts = 0, locked_until = NULL, password_changed_at = COALESCE(password_changed_at, NOW()), must_change_password = false WHERE email = $2`,
        [hashed, ADMIN_EMAIL]
      );
      console.log("[SEED] Updated admin password, rows affected:", updateResult.rowCount);
    }
    const fixedManagers = await pool.query(
      `UPDATE users SET role = 'manager' WHERE email = 'manager@bc.com' AND role = 'admin' RETURNING id, email`
    );
    if (fixedManagers.rowCount && fixedManagers.rowCount > 0) {
      console.log("[SEED] Fixed manager role for:", fixedManagers.rows.map((r: any) => r.email));
    }
  } catch (e: any) {
    console.error("[SEED] Failed to reset admin password:", e.message, e.stack);
  }
}
