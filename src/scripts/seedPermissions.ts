import { db, rolePermissionsTable, PERMISSIONS } from "../configs/database";
import { and, eq } from "drizzle-orm";

const DEFAULT_ADMIN_PERMISSIONS: Record<string, boolean> = {
  dashboard: true,
  leads: true,
  "leads.create": true,
  "leads.delete": true,
  "leads.bulk_actions": true,
  pipeline: true,
  "pipeline.move_deals": true,
  analytics: false,
  commissions: true,
  reports: true,
  templates: true,
  lead_scoring: false,
  forecast: false,
  calculator: true,
  goals: false,
  activity: false,
  portfolio: false,
  ai_learning: true,
  funders: true,
  isos: false,
  import: true,
  users: false,
  role_management: false,
  webhooks: false,
  ai_chat: true,
  ai_underwriting: true,
  ai_scripts: true,
};

const DEFAULT_MANAGER_PERMISSIONS: Record<string, boolean> = {
  dashboard: true,
  leads: true,
  "leads.create": true,
  "leads.delete": true,
  "leads.bulk_actions": true,
  pipeline: true,
  "pipeline.move_deals": true,
  analytics: true,
  commissions: true,
  reports: true,
  templates: true,
  lead_scoring: true,
  forecast: true,
  calculator: true,
  goals: true,
  activity: true,
  portfolio: true,
  ai_learning: true,
  funders: true,
  isos: true,
  import: true,
  users: false,
  role_management: false,
  webhooks: true,
  ai_chat: true,
  ai_underwriting: true,
  ai_scripts: true,
};

const DEFAULT_REP_PERMISSIONS: Record<string, boolean> = {
  dashboard: true,
  leads: true,
  "leads.create": true,
  "leads.delete": false,
  "leads.bulk_actions": false,
  pipeline: true,
  "pipeline.move_deals": true,
  analytics: false,
  commissions: true,
  reports: false,
  templates: true,
  lead_scoring: false,
  forecast: false,
  calculator: true,
  goals: false,
  activity: false,
  portfolio: false,
  ai_learning: false,
  funders: true,
  isos: false,
  import: false,
  users: false,
  role_management: false,
  webhooks: false,
  ai_chat: true,
  ai_underwriting: false,
  ai_scripts: true,
};

export async function seedRolePermissions() {
  const roles: Record<string, Record<string, boolean>> = {
    admin: DEFAULT_ADMIN_PERMISSIONS,
    manager: DEFAULT_MANAGER_PERMISSIONS,
    rep: DEFAULT_REP_PERMISSIONS,
  };

  for (const [role, defaults] of Object.entries(roles)) {
    for (const permission of PERMISSIONS) {
      const enabled = defaults[permission] ?? false;
      const existing = await db
        .select()
        .from(rolePermissionsTable)
        .where(
          and(
            eq(rolePermissionsTable.role, role),
            eq(rolePermissionsTable.permission, permission)
          )
        );

      if (existing.length === 0) {
        await db.insert(rolePermissionsTable).values({
          role,
          permission,
          enabled,
        });
      }
    }
  }

  console.log("Role permissions seeded successfully");
}
