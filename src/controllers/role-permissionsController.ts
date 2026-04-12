import { Router, type IRouter } from "express";
import { db, rolePermissionsTable, PERMISSIONS } from "../configs/database";
import { eq, and } from "drizzle-orm";
import { requireAuth, requireSuperAdmin, requirePermission } from "../middlewares/authMiddleware";

const router: IRouter = Router();

router.get("/role-permissions", requireAuth, requirePermission("role_management"), async (_req, res) => {
  try {
    const permissions = await db.select().from(rolePermissionsTable);
    
    const grouped: Record<string, Record<string, boolean>> = {};
    for (const p of permissions) {
      if (!grouped[p.role]) grouped[p.role] = {};
      grouped[p.role][p.permission] = p.enabled;
    }

    res.json({
      permissions: grouped,
      availablePermissions: PERMISSIONS,
      roles: ["admin", "manager", "rep"],
    });
  } catch (e: any) {
    res.status(500).json({ error: "Failed to fetch permissions" });
  }
});

router.put("/role-permissions", requireAuth, requirePermission("role_management"), async (req, res) => {
  try {
    const { role, permission, enabled } = req.body;
    if (!role || !permission || typeof enabled !== "boolean") {
      res.status(400).json({ error: "role, permission, and enabled required" });
      return;
    }

    if (role === "super_admin") {
      res.status(400).json({ error: "Cannot modify super_admin permissions" });
      return;
    }

    const existing = await db.select().from(rolePermissionsTable)
      .where(and(eq(rolePermissionsTable.role, role), eq(rolePermissionsTable.permission, permission)));
    
    if (existing.length > 0) {
      await db.update(rolePermissionsTable)
        .set({ enabled, updatedAt: new Date() })
        .where(and(eq(rolePermissionsTable.role, role), eq(rolePermissionsTable.permission, permission)));
    } else {
      await db.insert(rolePermissionsTable).values({ role, permission, enabled });
    }

    res.json({ success: true });
  } catch (e: any) {
    res.status(500).json({ error: "Failed to update permission" });
  }
});

router.get("/role-permissions/:role", requireAuth, async (req, res) => {
  try {
    const user = (req as any).user;
    const role = req.params.role;
    
    if (user.role !== "super_admin" && user.role !== role) {
      res.status(403).json({ error: "Not authorized" });
      return;
    }

    const permissions = await db.select().from(rolePermissionsTable)
      .where(eq(rolePermissionsTable.role, role));
    
    const permMap: Record<string, boolean> = {};
    for (const p of permissions) {
      permMap[p.permission] = p.enabled;
    }

    res.json(permMap);
  } catch (e: any) {
    res.status(500).json({ error: "Failed to fetch role permissions" });
  }
});

export default router;
