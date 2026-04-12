import { Router, type IRouter } from "express";
import { eq, and, sql, desc, isNull, or, inArray, count } from "drizzle-orm";
import {
  db,
  phoneNumbersTable,
  usersTable,
  leadsTable,
} from "../configs/database";
import { requireAuth, requireAdmin } from "../middlewares/authMiddleware";

const router: IRouter = Router();

const AREA_CODE_STATE_MAP: Record<string, string[]> = {
  "AL": ["205","251","256","334","938"],
  "AK": ["907"],
  "AZ": ["480","520","602","623","928"],
  "AR": ["479","501","870"],
  "CA": ["209","213","310","323","341","369","408","415","424","442","510","530","559","562","619","626","628","650","657","661","669","707","714","747","760","805","818","831","858","909","916","925","949","951"],
  "CO": ["303","719","720","970"],
  "CT": ["203","475","860","959"],
  "DE": ["302"],
  "FL": ["239","305","321","352","386","407","561","727","754","772","786","813","850","863","904","941","954"],
  "GA": ["229","404","470","478","678","706","762","770","912"],
  "HI": ["808"],
  "ID": ["208","986"],
  "IL": ["217","224","309","312","331","618","630","708","773","779","815","847","872"],
  "IN": ["219","260","317","463","574","765","812","930"],
  "IA": ["319","515","563","641","712"],
  "KS": ["316","620","785","913"],
  "KY": ["270","364","502","606","859"],
  "LA": ["225","318","337","504","985"],
  "ME": ["207"],
  "MD": ["240","301","410","443","667"],
  "MA": ["339","351","413","508","617","774","781","857","978"],
  "MI": ["231","248","269","313","517","586","616","734","810","906","947","989"],
  "MN": ["218","320","507","612","651","763","952"],
  "MS": ["228","601","662","769"],
  "MO": ["314","417","573","636","660","816"],
  "MT": ["406"],
  "NE": ["308","402","531"],
  "NV": ["702","725","775"],
  "NH": ["603"],
  "NJ": ["201","551","609","732","848","856","862","908","973"],
  "NM": ["505","575"],
  "NY": ["212","315","332","347","516","518","585","607","631","646","680","716","718","838","845","914","917","929","934"],
  "NC": ["252","336","704","743","828","910","919","980","984"],
  "ND": ["701"],
  "OH": ["216","220","234","330","380","419","440","513","567","614","740","937"],
  "OK": ["405","539","580","918"],
  "OR": ["458","503","541","971"],
  "PA": ["215","267","272","412","484","570","610","717","724","814","878"],
  "RI": ["401"],
  "SC": ["803","843","854","864"],
  "SD": ["605"],
  "TN": ["423","615","629","731","865","901","931"],
  "TX": ["210","214","254","281","325","346","361","409","430","432","469","512","682","713","737","806","817","830","832","903","915","936","940","956","972","979"],
  "UT": ["385","435","801"],
  "VT": ["802"],
  "VA": ["276","434","540","571","703","757","804"],
  "WA": ["206","253","360","425","509","564"],
  "WV": ["304","681"],
  "WI": ["262","414","534","608","715","920"],
  "WY": ["307"],
  "DC": ["202"],
};

function getStateFromAreaCode(areaCode: string): string | null {
  for (const [state, codes] of Object.entries(AREA_CODE_STATE_MAP)) {
    if (codes.includes(areaCode)) return state;
  }
  return null;
}

function extractAreaCode(phone: string): string | null {
  const digits = phone.replace(/\D/g, "");
  if (digits.length === 10) return digits.substring(0, 3);
  if (digits.length === 11 && digits[0] === "1") return digits.substring(1, 4);
  return null;
}

router.get("/phone-numbers", requireAuth, async (req, res): Promise<void> => {
  try {
    const user = (req as any).user;
    const isAdminRole = user.role === "admin" || user.role === "super_admin";

    let numbers;
    if (isAdminRole) {
      numbers = await db.select({
        id: phoneNumbersTable.id,
        number: phoneNumbersTable.number,
        friendlyName: phoneNumbersTable.friendlyName,
        areaCode: phoneNumbersTable.areaCode,
        state: phoneNumbersTable.state,
        region: phoneNumbersTable.region,
        provider: phoneNumbersTable.provider,
        status: phoneNumbersTable.status,
        assignedToId: phoneNumbersTable.assignedToId,
        isPrimary: phoneNumbersTable.isPrimary,
        capabilities: phoneNumbersTable.capabilities,
        monthlyFee: phoneNumbersTable.monthlyFee,
        createdAt: phoneNumbersTable.createdAt,
        lastUsedAt: phoneNumbersTable.lastUsedAt,
        assignedToName: usersTable.fullName,
      })
        .from(phoneNumbersTable)
        .leftJoin(usersTable, eq(phoneNumbersTable.assignedToId, usersTable.id))
        .orderBy(desc(phoneNumbersTable.createdAt));
    } else {
      numbers = await db.select({
        id: phoneNumbersTable.id,
        number: phoneNumbersTable.number,
        friendlyName: phoneNumbersTable.friendlyName,
        areaCode: phoneNumbersTable.areaCode,
        state: phoneNumbersTable.state,
        region: phoneNumbersTable.region,
        provider: phoneNumbersTable.provider,
        status: phoneNumbersTable.status,
        assignedToId: phoneNumbersTable.assignedToId,
        isPrimary: phoneNumbersTable.isPrimary,
        capabilities: phoneNumbersTable.capabilities,
        monthlyFee: phoneNumbersTable.monthlyFee,
        createdAt: phoneNumbersTable.createdAt,
        lastUsedAt: phoneNumbersTable.lastUsedAt,
        assignedToName: usersTable.fullName,
      })
        .from(phoneNumbersTable)
        .leftJoin(usersTable, eq(phoneNumbersTable.assignedToId, usersTable.id))
        .where(eq(phoneNumbersTable.assignedToId, user.id))
        .orderBy(desc(phoneNumbersTable.createdAt));
    }

    const [totalCount] = await db.select({ count: count() }).from(phoneNumbersTable);
    const [assignedCount] = await db.select({ count: count() }).from(phoneNumbersTable).where(sql`${phoneNumbersTable.assignedToId} IS NOT NULL`);
    const [availableCount] = await db.select({ count: count() }).from(phoneNumbersTable).where(eq(phoneNumbersTable.status, "available"));

    res.json({
      numbers,
      stats: {
        total: Number(totalCount?.count ?? 0),
        assigned: Number(assignedCount?.count ?? 0),
        available: Number(availableCount?.count ?? 0),
      },
    });
  } catch (e: any) {
    console.error("Phone numbers error:", e);
    res.status(500).json({ error: "Failed to fetch phone numbers" });
  }
});

router.post("/phone-numbers", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const { number, friendlyName, provider, capabilities, monthlyFee, state, region } = req.body;
    if (!number) { res.status(400).json({ error: "Phone number is required" }); return; }

    const digits = number.replace(/\D/g, "");
    const areaCode = extractAreaCode(number);
    if (!areaCode) { res.status(400).json({ error: "Invalid phone number format" }); return; }

    const derivedState = state || getStateFromAreaCode(areaCode);

    const [existing] = await db.select({ id: phoneNumbersTable.id }).from(phoneNumbersTable).where(eq(phoneNumbersTable.number, digits));
    if (existing) { res.status(409).json({ error: "Number already exists in pool" }); return; }

    const [created] = await db.insert(phoneNumbersTable).values({
      number: digits.length === 11 ? digits : `1${digits}`,
      friendlyName: friendlyName || null,
      areaCode,
      state: derivedState,
      region: region || null,
      provider: provider || "twilio",
      status: "available",
      capabilities: capabilities || "voice,sms",
      monthlyFee: monthlyFee || null,
    }).returning();

    res.json(created);
  } catch (e: any) {
    console.error("Create phone number error:", e);
    res.status(500).json({ error: "Failed to add phone number" });
  }
});

router.post("/phone-numbers/bulk", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const { numbers } = req.body;
    if (!Array.isArray(numbers) || numbers.length === 0) {
      res.status(400).json({ error: "Array of numbers is required" }); return;
    }

    const results = { added: 0, skipped: 0, errors: [] as string[] };

    for (const num of numbers) {
      const raw = typeof num === "string" ? num : num.number;
      if (!raw) continue;

      const areaCode = extractAreaCode(raw);
      if (!areaCode) { results.errors.push(`Invalid: ${raw}`); results.skipped++; continue; }

      const digits = raw.replace(/\D/g, "");
      const fullNumber = digits.length === 11 ? digits : `1${digits}`;

      const [existing] = await db.select({ id: phoneNumbersTable.id }).from(phoneNumbersTable).where(eq(phoneNumbersTable.number, fullNumber));
      if (existing) { results.skipped++; continue; }

      await db.insert(phoneNumbersTable).values({
        number: fullNumber,
        friendlyName: typeof num === "object" ? num.friendlyName : null,
        areaCode,
        state: getStateFromAreaCode(areaCode),
        provider: typeof num === "object" ? num.provider || "twilio" : "twilio",
        status: "available",
        capabilities: "voice,sms",
      });
      results.added++;
    }

    res.json(results);
  } catch (e: any) {
    console.error("Bulk add error:", e);
    res.status(500).json({ error: "Bulk add failed" });
  }
});

router.patch("/phone-numbers/:id/assign", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const id = parseInt(req.params.id);
    const { userId, isPrimary } = req.body;

    if (userId) {
      const [user] = await db.select({ id: usersTable.id }).from(usersTable).where(eq(usersTable.id, userId));
      if (!user) { res.status(404).json({ error: "User not found" }); return; }

      if (isPrimary) {
        await db.update(phoneNumbersTable)
          .set({ isPrimary: false })
          .where(eq(phoneNumbersTable.assignedToId, userId));
      }
    }

    const [updated] = await db.update(phoneNumbersTable)
      .set({
        assignedToId: userId || null,
        status: userId ? "assigned" : "available",
        isPrimary: isPrimary ?? false,
      })
      .where(eq(phoneNumbersTable.id, id))
      .returning();

    if (!updated) { res.status(404).json({ error: "Number not found" }); return; }

    res.json(updated);
  } catch (e: any) {
    console.error("Assign error:", e);
    res.status(500).json({ error: "Failed to assign number" });
  }
});

router.patch("/phone-numbers/:id/unassign", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const id = parseInt(req.params.id);
    const [updated] = await db.update(phoneNumbersTable)
      .set({ assignedToId: null, status: "available", isPrimary: false })
      .where(eq(phoneNumbersTable.id, id))
      .returning();

    if (!updated) { res.status(404).json({ error: "Number not found" }); return; }
    res.json(updated);
  } catch (e: any) {
    res.status(500).json({ error: "Failed to unassign number" });
  }
});

router.delete("/phone-numbers/:id", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const id = parseInt(req.params.id);
    const [deleted] = await db.delete(phoneNumbersTable).where(eq(phoneNumbersTable.id, id)).returning();
    if (!deleted) { res.status(404).json({ error: "Number not found" }); return; }
    res.json({ success: true });
  } catch (e: any) {
    res.status(500).json({ error: "Failed to delete number" });
  }
});

router.get("/phone-numbers/best-match/:leadId", requireAuth, async (req, res): Promise<void> => {
  try {
    const leadId = parseInt(req.params.leadId);
    const user = (req as any).user;

    const [lead] = await db.select({
      phone: leadsTable.phone,
      state: leadsTable.state,
      zip: leadsTable.zip,
      assignedToId: leadsTable.assignedToId,
    }).from(leadsTable).where(eq(leadsTable.id, leadId));

    if (!lead) { res.status(404).json({ error: "Lead not found" }); return; }

    const leadAreaCode = extractAreaCode(lead.phone);
    const leadState = lead.state?.toUpperCase() || (leadAreaCode ? getStateFromAreaCode(leadAreaCode) : null);

    const callerUserId = lead.assignedToId || user.id;

    const allNumbers = await db.select()
      .from(phoneNumbersTable)
      .where(
        or(
          eq(phoneNumbersTable.assignedToId, callerUserId),
          eq(phoneNumbersTable.status, "available"),
        )
      );

    if (allNumbers.length === 0) {
      res.json({ bestMatch: null, reason: "No numbers available" }); return;
    }

    let bestMatch = null;
    let reason = "";

    const exactAreaCodeMatch = allNumbers.find(n => n.areaCode === leadAreaCode && n.assignedToId === callerUserId);
    if (exactAreaCodeMatch) {
      bestMatch = exactAreaCodeMatch;
      reason = "Exact area code match (assigned to rep)";
    }

    if (!bestMatch) {
      const poolAreaCodeMatch = allNumbers.find(n => n.areaCode === leadAreaCode && n.status === "available");
      if (poolAreaCodeMatch) {
        bestMatch = poolAreaCodeMatch;
        reason = "Exact area code match (from pool)";
      }
    }

    if (!bestMatch && leadState) {
      const stateMatch = allNumbers.find(n => n.state?.toUpperCase() === leadState && n.assignedToId === callerUserId);
      if (stateMatch) {
        bestMatch = stateMatch;
        reason = "Same state match (assigned to rep)";
      }
    }

    if (!bestMatch && leadState) {
      const poolStateMatch = allNumbers.find(n => n.state?.toUpperCase() === leadState && n.status === "available");
      if (poolStateMatch) {
        bestMatch = poolStateMatch;
        reason = "Same state match (from pool)";
      }
    }

    if (!bestMatch) {
      const primaryNumber = allNumbers.find(n => n.assignedToId === callerUserId && n.isPrimary);
      if (primaryNumber) {
        bestMatch = primaryNumber;
        reason = "Rep's primary number (no local match)";
      }
    }

    if (!bestMatch) {
      const anyAssigned = allNumbers.find(n => n.assignedToId === callerUserId);
      if (anyAssigned) {
        bestMatch = anyAssigned;
        reason = "Rep's assigned number (no local match)";
      }
    }

    if (!bestMatch) {
      bestMatch = allNumbers[0];
      reason = "Pool fallback (no better match)";
    }

    res.json({
      bestMatch: {
        id: bestMatch.id,
        number: bestMatch.number,
        friendlyName: bestMatch.friendlyName,
        areaCode: bestMatch.areaCode,
        state: bestMatch.state,
      },
      reason,
      leadAreaCode,
      leadState,
    });
  } catch (e: any) {
    console.error("Best match error:", e);
    res.status(500).json({ error: "Failed to find best match" });
  }
});

router.get("/phone-numbers/rep/:userId", requireAuth, async (req, res): Promise<void> => {
  try {
    const userId = parseInt(req.params.userId);
    const numbers = await db.select()
      .from(phoneNumbersTable)
      .where(eq(phoneNumbersTable.assignedToId, userId))
      .orderBy(desc(phoneNumbersTable.isPrimary));

    res.json({ numbers });
  } catch (e: any) {
    res.status(500).json({ error: "Failed to fetch rep numbers" });
  }
});

router.get("/phone-numbers/stats", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const stateGroups = await db.execute(sql`
      SELECT state, COUNT(*) as total,
        COUNT(CASE WHEN assigned_to_id IS NOT NULL THEN 1 END) as assigned,
        COUNT(CASE WHEN status = 'available' THEN 1 END) as available
      FROM phone_numbers
      WHERE state IS NOT NULL
      GROUP BY state
      ORDER BY state
    `);

    const repGroups = await db.execute(sql`
      SELECT pn.assigned_to_id, u.full_name, COUNT(*) as number_count,
        COUNT(CASE WHEN pn.is_primary THEN 1 END) as primary_count
      FROM phone_numbers pn
      INNER JOIN users u ON u.id = pn.assigned_to_id
      WHERE pn.assigned_to_id IS NOT NULL
      GROUP BY pn.assigned_to_id, u.full_name
      ORDER BY u.full_name
    `);

    res.json({
      byState: (stateGroups as any).rows || stateGroups,
      byRep: (repGroups as any).rows || repGroups,
    });
  } catch (e: any) {
    res.status(500).json({ error: "Failed to fetch stats" });
  }
});

export default router;
