import { Router, type IRouter } from "express";
import { requireAuth } from "../../middlewares/authMiddleware";
import { callClaude, callClaudeWithHistory, getBusinessSnapshot, getLeadContext, searchLeadByName, ADMIN_SYSTEM_PROMPT, REP_SYSTEM_PROMPT } from "./helpersController";

const router: IRouter = Router();

async function resolveLeadFromMessage(message: string): Promise<{ id?: number; searchName?: string } | null> {
  const idMatch = message.match(/lead\s*#?\s*(\d{3,})/i) || message.match(/merchant\s*#?\s*(\d{3,})/i) || message.match(/(?:^|\s)#(\d{3,})/i);
  if (idMatch) return { id: parseInt(idMatch[1]) };

  const extraction = await callClaude(
    `You extract business/merchant/person names from user messages in a cash advance CRM context.
If the user is asking about a specific business, merchant, company, or person by name, extract that name.
Return ONLY a JSON object: {"name": "extracted name"} or {"name": null} if no specific entity is mentioned.
Do NOT extract generic words like "leads", "merchant", "business" without a proper name.
Examples:
- "does pro taint llc have a voided check" -> {"name": "pro taint llc"}
- "what docs does palmira bros have" -> {"name": "palmira bros"}
- "tell me about willie d powells" -> {"name": "willie d powells"}
- "how many leads came in today" -> {"name": null}
- "show me the pipeline" -> {"name": null}
- "what's the status on ABC trucking" -> {"name": "ABC trucking"}`,
    message,
    { maxTokens: 100, jsonMode: true }
  );

  try {
    const parsed = JSON.parse(extraction);
    if (parsed.name && typeof parsed.name === "string" && parsed.name.trim().length >= 2) {
      return { searchName: parsed.name.trim() };
    }
  } catch {}

  return null;
}

router.post("/ai/chat", requireAuth, async (req, res) => {
  try {
    const { message, context, conversationHistory } = req.body;
    if (!message || typeof message !== "string") return res.status(400).json({ error: "Message required" });
    const trimmedMessage = message.slice(0, 2000);

    const user = (req as any).user;
    const isAdminUser = user.role === "admin" || user.role === "super_admin";

    let systemContent: string;

    if (isAdminUser) {
      const [snapshot, leadRef] = await Promise.all([
        getBusinessSnapshot(),
        context?.leadId ? Promise.resolve({ id: context.leadId }) : resolveLeadFromMessage(trimmedMessage),
      ]);

      let leadContext = "";

      if (leadRef?.id) {
        const leadData = await getLeadContext(leadRef.id);
        if (leadData) {
          const label = context?.leadId ? "CURRENT LEAD CONTEXT" : "LEAD CONTEXT";
          leadContext = `\n\n${label} (ID #${leadData.id} — "${leadData.businessName}"):\n${JSON.stringify(leadData, null, 2)}`;
        }
      } else if (leadRef?.searchName) {
        const matches = await searchLeadByName(leadRef.searchName);
        if (matches.length === 1) {
          const leadData = await getLeadContext(matches[0].id);
          if (leadData) {
            leadContext = `\n\nLEAD CONTEXT (searched "${leadRef.searchName}" → matched "${leadData.businessName}" ID #${leadData.id}):\n${JSON.stringify(leadData, null, 2)}\n\nIMPORTANT: Confirm the business name to the user so they know you found the right one. Example: "For **${leadData.businessName}** (Lead #${leadData.id})..."`;
          }
        } else if (matches.length > 1) {
          leadContext = `\n\nLEAD SEARCH for "${leadRef.searchName}" returned ${matches.length} matches:\n${JSON.stringify(matches.map(m => ({ id: m.id, name: m.businessName, owner: m.ownerName, phone: m.phone, status: m.status })), null, 2)}\n\nIMPORTANT: Show the user all matches and ask them to clarify which one they mean. List them with their ID and owner name.`;
        } else {
          leadContext = `\n\nLEAD SEARCH for "${leadRef.searchName}" returned NO results. Let the user know you couldn't find a lead with that name and suggest they double-check the spelling or try a different name.`;
        }
      }

      systemContent = `${ADMIN_SYSTEM_PROMPT}\n\nCURRENT BUSINESS SNAPSHOT:\n${JSON.stringify(snapshot, null, 2)}${leadContext}\n\nCurrent user: ${user.fullName} (${user.role}). Today: ${new Date().toLocaleDateString()}.`;
    } else {
      systemContent = `${REP_SYSTEM_PROMPT}\n\nCurrent user: ${user.fullName} (Sales Rep). Today: ${new Date().toLocaleDateString()}.`;
    }

    const messages: Array<{ role: "user" | "assistant"; content: string }> = [];

    if (Array.isArray(conversationHistory)) {
      for (const msg of conversationHistory.slice(-8)) {
        if (msg.role === "user" || msg.role === "assistant") {
          messages.push({ role: msg.role, content: String(msg.content).slice(0, 2000) });
        }
      }
    }

    messages.push({ role: "user", content: trimmedMessage });

    const reply = await callClaudeWithHistory(systemContent, messages);

    res.json({ reply: reply || "I couldn't generate a response. Please try again." });
  } catch (e: any) {
    console.error("AI chat error:", e);
    res.status(500).json({ error: "AI assistant is temporarily unavailable. Please try again." });
  }
});

export default router;
