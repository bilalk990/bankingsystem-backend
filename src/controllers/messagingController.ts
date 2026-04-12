import { Router, type IRouter } from "express";
import { eq, and, sql, desc, asc, count, isNull, or, inArray } from "drizzle-orm";
import {
  db,
  leadsTable,
  leadMessagesTable,
  activitiesTable,
  usersTable,
  notificationsTable,
  dealsTable,
} from "../configs/database";
import { requireAuth } from "../middlewares/authMiddleware";

const router: IRouter = Router();

router.post("/leads/:id/messages", requireAuth, async (req, res): Promise<void> => {
  try {
    const leadId = parseInt(req.params.id);
    if (isNaN(leadId)) { res.status(400).json({ error: "Invalid lead ID" }); return; }

    const user = (req as any).user;
    const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!lead) { res.status(404).json({ error: "Lead not found" }); return; }
    if (user.role === "rep" && lead.assignedToId !== user.id) {
      res.status(403).json({ error: "Not authorized" }); return;
    }

    const { content, messageType, aiGenerated } = req.body;
    if (!content || !content.trim()) {
      res.status(400).json({ error: "Message content is required" }); return;
    }

    const [message] = await db.insert(leadMessagesTable).values({
      leadId,
      source: "crm",
      direction: "outbound",
      content: content.trim(),
      senderName: user.fullName || user.username,
      userId: user.id,
      messageType: messageType || "sms",
      aiGenerated: aiGenerated || false,
      isRead: true,
    }).returning();

    await db.update(leadsTable).set({
      lastContactedAt: new Date(),
    }).where(eq(leadsTable.id, leadId));

    await db.insert(activitiesTable).values({
      type: "message_sent",
      description: `${messageType === "email" ? "📧" : "💬"} ${aiGenerated ? "AI-suggested " : ""}${messageType || "SMS"} sent to ${lead.ownerName} at ${lead.phone}${aiGenerated ? " (AI-generated)" : ""}`,
      leadId,
      userId: user.id,
    });

    res.json(message);
  } catch (e: any) {
    console.error("Send message error:", e);
    res.status(500).json({ error: "Failed to send message" });
  }
});

router.get("/messages/conversations", requireAuth, async (req, res): Promise<void> => {
  try {
    const user = (req as any).user;
    const isAdminRole = user.role === "admin" || user.role === "super_admin";

    let leadFilter;
    if (!isAdminRole) {
      leadFilter = eq(leadsTable.assignedToId, user.id);
    }

    const leadsWithMessages = await db.execute(sql`
      SELECT DISTINCT lm.lead_id
      FROM lead_messages lm
      INNER JOIN leads l ON l.id = lm.lead_id
      ${leadFilter ? sql`WHERE l.assigned_to_id = ${user.id}` : sql``}
    `);
    const leadIds = ((leadsWithMessages as any).rows || leadsWithMessages).map((r: any) => r.lead_id);

    if (leadIds.length === 0) {
      res.json({ conversations: [] }); return;
    }

    const conversations = [];
    for (const lid of leadIds) {
      const [lead] = await db.select({
        id: leadsTable.id,
        businessName: leadsTable.businessName,
        ownerName: leadsTable.ownerName,
        phone: leadsTable.phone,
        status: leadsTable.status,
        requestedAmount: leadsTable.requestedAmount,
        monthlyRevenue: leadsTable.monthlyRevenue,
        industry: leadsTable.industry,
        assignedToId: leadsTable.assignedToId,
      }).from(leadsTable).where(eq(leadsTable.id, lid));

      if (!lead) continue;

      const [lastMsg] = await db.select({
        id: leadMessagesTable.id,
        content: leadMessagesTable.content,
        direction: leadMessagesTable.direction,
        senderName: leadMessagesTable.senderName,
        createdAt: leadMessagesTable.createdAt,
        messageType: leadMessagesTable.messageType,
      }).from(leadMessagesTable)
        .where(eq(leadMessagesTable.leadId, lid))
        .orderBy(desc(leadMessagesTable.createdAt))
        .limit(1);

      const [unread] = await db.select({ count: count() })
        .from(leadMessagesTable)
        .where(and(
          eq(leadMessagesTable.leadId, lid),
          eq(leadMessagesTable.direction, "inbound"),
          eq(leadMessagesTable.isRead, false),
        ));

      const [fundedDeal] = await db.select({
        amount: dealsTable.amount,
        stage: dealsTable.stage,
      }).from(dealsTable)
        .where(eq(dealsTable.leadId, lid))
        .orderBy(desc(dealsTable.createdAt))
        .limit(1);

      const assignedUser = lead.assignedToId
        ? (await db.select({ fullName: usersTable.fullName }).from(usersTable).where(eq(usersTable.id, lead.assignedToId)))[0]
        : null;

      conversations.push({
        leadId: lead.id,
        businessName: lead.businessName,
        ownerName: lead.ownerName,
        phone: lead.phone,
        status: lead.status,
        requestedAmount: lead.requestedAmount,
        monthlyRevenue: lead.monthlyRevenue,
        industry: lead.industry,
        assignedToName: assignedUser?.fullName || null,
        dealAmount: fundedDeal?.amount || null,
        dealStage: fundedDeal?.stage || null,
        lastMessage: lastMsg || null,
        unreadCount: Number(unread?.count ?? 0),
      });
    }

    conversations.sort((a, b) => {
      const aTime = a.lastMessage ? new Date(a.lastMessage.createdAt).getTime() : 0;
      const bTime = b.lastMessage ? new Date(b.lastMessage.createdAt).getTime() : 0;
      return bTime - aTime;
    });

    res.json({ conversations });
  } catch (e: any) {
    console.error("Conversations error:", e);
    res.status(500).json({ error: "Failed to fetch conversations" });
  }
});

router.get("/leads/:id/messages", requireAuth, async (req, res): Promise<void> => {
  try {
    const leadId = parseInt(req.params.id);
    if (isNaN(leadId)) { res.status(400).json({ error: "Invalid lead ID" }); return; }

    const user = (req as any).user;
    if (user.role === "rep") {
      const [lead] = await db.select({ assignedToId: leadsTable.assignedToId }).from(leadsTable).where(eq(leadsTable.id, leadId));
      if (!lead || lead.assignedToId !== user.id) {
        res.status(403).json({ error: "Not authorized" }); return;
      }
    }

    const messages = await db.select({
      id: leadMessagesTable.id,
      leadId: leadMessagesTable.leadId,
      source: leadMessagesTable.source,
      direction: leadMessagesTable.direction,
      content: leadMessagesTable.content,
      senderName: leadMessagesTable.senderName,
      messageType: leadMessagesTable.messageType,
      aiGenerated: leadMessagesTable.aiGenerated,
      isRead: leadMessagesTable.isRead,
      userId: leadMessagesTable.userId,
      createdAt: leadMessagesTable.createdAt,
    }).from(leadMessagesTable)
      .where(eq(leadMessagesTable.leadId, leadId))
      .orderBy(asc(leadMessagesTable.createdAt));

    await db.update(leadMessagesTable)
      .set({ isRead: true })
      .where(and(
        eq(leadMessagesTable.leadId, leadId),
        eq(leadMessagesTable.direction, "inbound"),
        eq(leadMessagesTable.isRead, false),
      ));

    res.json({ messages });
  } catch (e: any) {
    console.error("Lead messages error:", e);
    res.status(500).json({ error: "Failed to fetch messages" });
  }
});

router.get("/messages/inbox", requireAuth, async (req, res): Promise<void> => {
  try {
    const user = (req as any).user;
    const isAdminRole = user.role === "admin" || user.role === "super_admin";

    const conditions = [
      eq(leadMessagesTable.direction, "inbound"),
    ];

    if (!isAdminRole) {
      const userLeads = await db.select({ id: leadsTable.id }).from(leadsTable)
        .where(eq(leadsTable.assignedToId, user.id));
      const leadIds = userLeads.map(l => l.id);
      if (leadIds.length === 0) {
        res.json({ messages: [], unreadCount: 0 }); return;
      }
      conditions.push(inArray(leadMessagesTable.leadId, leadIds));
    }

    const messages = await db.select({
      id: leadMessagesTable.id,
      leadId: leadMessagesTable.leadId,
      content: leadMessagesTable.content,
      senderName: leadMessagesTable.senderName,
      source: leadMessagesTable.source,
      isRead: leadMessagesTable.isRead,
      isHotTrigger: leadMessagesTable.isHotTrigger,
      createdAt: leadMessagesTable.createdAt,
      businessName: leadsTable.businessName,
      ownerName: leadsTable.ownerName,
      phone: leadsTable.phone,
      leadStatus: leadsTable.status,
      isHot: leadsTable.isHot,
      assignedToId: leadsTable.assignedToId,
    }).from(leadMessagesTable)
      .innerJoin(leadsTable, eq(leadMessagesTable.leadId, leadsTable.id))
      .where(and(...conditions))
      .orderBy(desc(leadMessagesTable.createdAt))
      .limit(100);

    const [unreadResult] = await db.select({
      count: count(),
    }).from(leadMessagesTable)
      .innerJoin(leadsTable, eq(leadMessagesTable.leadId, leadsTable.id))
      .where(and(
        ...conditions,
        eq(leadMessagesTable.isRead, false),
      ));

    res.json({
      messages,
      unreadCount: unreadResult?.count || 0,
    });
  } catch (e: any) {
    console.error("Inbox error:", e);
    res.status(500).json({ error: "Failed to fetch inbox" });
  }
});

router.patch("/messages/:id/read", requireAuth, async (req, res): Promise<void> => {
  try {
    const id = parseInt(req.params.id);
    if (isNaN(id)) { res.status(400).json({ error: "Invalid message ID" }); return; }

    const user = (req as any).user;
    const [msg] = await db.select({
      id: leadMessagesTable.id,
      leadId: leadMessagesTable.leadId,
    }).from(leadMessagesTable).where(eq(leadMessagesTable.id, id));
    if (!msg) { res.status(404).json({ error: "Message not found" }); return; }

    if (user.role === "rep") {
      const [lead] = await db.select({ assignedToId: leadsTable.assignedToId }).from(leadsTable).where(eq(leadsTable.id, msg.leadId));
      if (!lead || lead.assignedToId !== user.id) {
        res.status(403).json({ error: "Not authorized" }); return;
      }
    }

    const [updated] = await db.update(leadMessagesTable)
      .set({ isRead: true })
      .where(eq(leadMessagesTable.id, id))
      .returning();

    res.json(updated);
  } catch (e: any) {
    res.status(500).json({ error: "Failed to mark as read" });
  }
});

router.patch("/leads/:id/messages/read-all", requireAuth, async (req, res): Promise<void> => {
  try {
    const leadId = parseInt(req.params.id);
    if (isNaN(leadId)) { res.status(400).json({ error: "Invalid lead ID" }); return; }

    const user = (req as any).user;
    if (user.role === "rep") {
      const [lead] = await db.select({ assignedToId: leadsTable.assignedToId }).from(leadsTable).where(eq(leadsTable.id, leadId));
      if (!lead || lead.assignedToId !== user.id) {
        res.status(403).json({ error: "Not authorized" }); return;
      }
    }

    await db.update(leadMessagesTable)
      .set({ isRead: true })
      .where(and(
        eq(leadMessagesTable.leadId, leadId),
        eq(leadMessagesTable.direction, "inbound"),
        eq(leadMessagesTable.isRead, false),
      ));

    res.json({ success: true });
  } catch (e: any) {
    res.status(500).json({ error: "Failed to mark messages as read" });
  }
});

router.get("/messages/stats", requireAuth, async (req, res): Promise<void> => {
  try {
    const user = (req as any).user;
    const isAdminRole = user.role === "admin" || user.role === "super_admin";

    let leadCondition;
    if (!isAdminRole) {
      leadCondition = eq(leadsTable.assignedToId, user.id);
    }

    const baseJoin = db.select({
      leadId: leadMessagesTable.leadId,
      direction: leadMessagesTable.direction,
      isRead: leadMessagesTable.isRead,
    }).from(leadMessagesTable)
      .innerJoin(leadsTable, eq(leadMessagesTable.leadId, leadsTable.id));

    const allMsgs = leadCondition
      ? await baseJoin.where(leadCondition)
      : await baseJoin;

    const unreadInbound = allMsgs.filter(m => m.direction === "inbound" && !m.isRead).length;
    const totalInbound = allMsgs.filter(m => m.direction === "inbound").length;
    const totalOutbound = allMsgs.filter(m => m.direction === "outbound").length;
    const leadsWithReplies = new Set(allMsgs.filter(m => m.direction === "inbound").map(m => m.leadId)).size;

    res.json({
      unreadInbound,
      totalInbound,
      totalOutbound,
      leadsWithReplies,
    });
  } catch (e: any) {
    res.status(500).json({ error: "Failed to fetch message stats" });
  }
});

export default router;
