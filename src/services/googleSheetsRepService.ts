import { getUncachableGoogleSheetClient } from "./googleSheetsService";

function extractSheetId(url: string): string | null {
  const match = url.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/);
  return match ? match[1] : null;
}

export async function appendToRepSheet(
  sheetUrl: string,
  tabName: string | null,
  leadData: {
    businessName: string;
    ownerName: string;
    phone: string;
    email?: string | null;
    approvalAmount: number;
    term: number;
    factorRate: number;
    paybackAmount: number;
    dailyPayment: number;
    riskCategory?: string | null;
    grossRevenue?: number | null;
    monthlyRevenue?: number | null;
    industry?: string | null;
    state?: string | null;
    notes?: string | null;
  }
): Promise<{ success: boolean; error?: string }> {
  try {
    const spreadsheetId = extractSheetId(sheetUrl);
    if (!spreadsheetId) return { success: false, error: "Invalid sheet URL" };

    const sheets = await getUncachableGoogleSheetClient();
    const range = tabName ? `${tabName}!A:A` : "Sheet1!A:A";

    const now = new Date();
    const dateStr = now.toLocaleDateString("en-US", { month: "2-digit", day: "2-digit", year: "numeric" });
    const timeStr = now.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" });

    const row = [
      dateStr,
      timeStr,
      leadData.businessName,
      leadData.ownerName,
      leadData.phone,
      leadData.email || "",
      leadData.state || "",
      leadData.industry || "",
      leadData.riskCategory || "",
      `$${leadData.grossRevenue?.toLocaleString() || "0"}`,
      `$${leadData.approvalAmount.toLocaleString()}`,
      leadData.term.toString(),
      leadData.factorRate.toFixed(2),
      `$${leadData.paybackAmount.toLocaleString()}`,
      `$${leadData.dailyPayment.toLocaleString()}`,
      leadData.notes || "",
    ];

    const appendRange = tabName ? `${tabName}!A:P` : "Sheet1!A:P";

    await sheets.spreadsheets.values.append({
      spreadsheetId,
      range: appendRange,
      valueInputOption: "USER_ENTERED",
      insertDataOption: "INSERT_ROWS",
      requestBody: { values: [row] },
    });

    return { success: true };
  } catch (e: any) {
    console.error("Failed to append to rep sheet:", e.message);
    return { success: false, error: e.message };
  }
}
