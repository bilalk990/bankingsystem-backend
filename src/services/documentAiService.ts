import { extractFullAccountNumber } from "./accountExtractor";

export interface ParsedTransaction {
  date: string;
  description: string;
  amount: number;
  type: "debit" | "credit";
  balance?: number;
}

export interface ParsedStatementData {
  transactions: ParsedTransaction[];
  accountNumber?: string;
  accountHolder?: string;
  bankName?: string;
  statementPeriod?: { start: string; end: string };
  beginningBalance?: number;
  endingBalance?: number;
  totalDeposits?: number;
  totalWithdrawals?: number;
}

const BANK_IDENTIFIERS: { pattern: RegExp; name: string }[] = [
  { pattern: /\bINWOOD\s+NATIONAL\s+BANK\b|\bInwood\s+National\b|\bINWOOD\s+BANK\b/i, name: "Inwood National Bank" },
  { pattern: /\bQuaint\s*Oak\b/i, name: "Quaint Oak Bank" },
  { pattern: /\bJPMorgan\s+Chase\b|\bChase\s+Bank\b|\bCHASE\b/i, name: "Chase" },
  { pattern: /\bTD\s+Bank\b|\bTD\s+Checking\b/i, name: "TD Bank" },
  { pattern: /\bBank\s+of\s+America\b|\bBofA\b|\bBANK\s+OF\s+AMERICA/i, name: "Bank of America" },
  { pattern: /\bWells\s+Fargo\b/i, name: "Wells Fargo" },
  { pattern: /\bPNC\s+Bank\b|\bPNC\b/i, name: "PNC" },
  { pattern: /\bCitizens\s+Bank\b|\bCitizens\b/i, name: "Citizens" },
  { pattern: /\bSynovus\b/i, name: "Synovus" },
  { pattern: /\bUS\s+Bank\b|\bU\.S\.\s+Bank\b/i, name: "US Bank" },
  { pattern: /\bRegions\s+Bank\b|\bRegions\b/i, name: "Regions" },
  { pattern: /\bKeyBank\b|\bKey\s+Bank\b/i, name: "KeyBank" },
  { pattern: /\bCapital\s+One\b/i, name: "Capital One" },
  { pattern: /\bM&T\s+Bank\b/i, name: "M&T Bank" },
  { pattern: /\bHuntington\b/i, name: "Huntington" },
  { pattern: /\bFifth\s+Third\b/i, name: "Fifth Third" },
  { pattern: /\bBMO\b|\bBMO\s+Harris\b/i, name: "BMO" },
  { pattern: /\bTruist\b/i, name: "Truist" },
  { pattern: /\bNavy\s*Federal\b|\bNFCU\b|navyfederal\.org/i, name: "Navy Federal Credit Union" },
];

const DATE_PATTERNS = [
  /(\d{1,2}\/\d{1,2}\/\d{4})/,
  /(\d{1,2}\/\d{1,2}\/\d{2})/,
  /(\d{1,2}\/\d{1,2})/,
  /(\d{1,2}-\d{1,2}-\d{4})/,
  /(\d{1,2}-\d{1,2}-\d{2})/,
];

function extractBankName(rawText: string): string | undefined {
  for (const { pattern, name } of BANK_IDENTIFIERS) {
    if (pattern.test(rawText)) return name;
  }
  return undefined;
}

function extractAccountNumber(rawText: string): string | undefined {
  return extractFullAccountNumber(rawText);
}

function extractStatementPeriod(rawText: string): { start: string; end: string } | undefined {
  const patterns = [
    /(?:Statement\s+Period|Period)[:\s]*(\w+\s+\d{1,2},?\s+\d{4})\s*(?:through|thru|to|-|–)\s*(\w+\s+\d{1,2},?\s+\d{4})/i,
    /(?:Statement\s+Period|Period)[:\s]*(\d{1,2}\/\d{1,2}\/\d{2,4})\s*(?:through|thru|to|-|–)\s*(\d{1,2}\/\d{1,2}\/\d{2,4})/i,
    /(?:Statement\s+Dates?)\s+(\d{1,2}\/\d{1,2}\/\d{2,4})\s+(?:through|thru|to|-|–)\s+(\d{1,2}\/\d{1,2}\/\d{2,4})/i,
    /(?:From|Beginning)[:\s]*(\d{1,2}\/\d{1,2}\/\d{2,4})\s*(?:through|thru|to|-|–)\s*(\d{1,2}\/\d{1,2}\/\d{2,4})/i,
    /(\w+\s+\d{1,2},?\s+\d{4})\s*(?:through|thru|to|-|–)\s*(\w+\s+\d{1,2},?\s+\d{4})/i,
  ];
  for (const p of patterns) {
    const m = rawText.match(p);
    if (m) return { start: m[1].trim(), end: m[2].trim() };
  }
  const endingMatch = rawText.match(/Statement\s+Ending\s+(\d{1,2})\/(\d{1,2})\/(\d{2,4})/i);
  if (endingMatch) {
    const mo = endingMatch[1];
    const day = endingMatch[2];
    const yr = endingMatch[3];
    return { start: `${mo}/01/${yr}`, end: `${mo}/${day}/${yr}` };
  }
  return undefined;
}

function extractBalances(rawText: string): { beginning?: number; ending?: number } {
  const result: { beginning?: number; ending?: number } = {};
  const begPatterns = [
    /(?:beginning|opening|starting|previous)\s+balance[:\s]*\$?([\d,]+\.\d{2})/i,
    /(?:Balance\s+(?:Forward|Brought))[:\s]*\$?([\d,]+\.\d{2})/i,
  ];
  const endPatterns = [
    /(?:ending|closing|new)\s+balance[:\s]*\$?([\d,]+\.\d{2})/i,
    /(?:Balance\s+at\s+End)[:\s]*\$?([\d,]+\.\d{2})/i,
  ];
  for (const p of begPatterns) {
    const m = rawText.match(p);
    if (m) { result.beginning = parseFloat(m[1].replace(/,/g, "")); break; }
  }
  for (const p of endPatterns) {
    const m = rawText.match(p);
    if (m) { result.ending = parseFloat(m[1].replace(/,/g, "")); break; }
  }
  return result;
}

function parseAmount(text: string): number {
  const cleaned = text.replace(/[$,\s]/g, "").replace(/\(([^)]+)\)/, "-$1");
  const val = parseFloat(cleaned);
  return isNaN(val) ? 0 : val;
}

const TRANSACTION_LINE_PATTERNS = [
  /^(\d{1,2}\/\d{1,2}(?:\/\d{2,4})?)\s+(.+?)\s+(-?\$?[\d,]+\.\d{2})\s*$/,
  /^(\d{1,2}\/\d{1,2}(?:\/\d{2,4})?)\s+(.+?)\s+(-?\$?[\d,]+\.\d{2})\s+(-?\$?[\d,]+\.\d{2})\s*$/,
  /^(\d{1,2}\/\d{1,2}(?:\/\d{2,4})?)\s+(.+?)\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s*$/,
  /^(\d{1,2}-\d{1,2}(?:-\d{2,4})?)\s+(.+?)\s+([\d,]+\.\d{2})-?\s+([\d,]+\.\d{2})\s*$/,
  /^(\d{1,2}-\d{1,2}(?:-\d{2,4})?)\s+(.+?)\s+(-?\$?[\d,]+\.\d{2})\s*$/,
];

type SectionType = "deposits" | "withdrawals" | "transactions" | "unknown";

function detectSectionType(headerLine: string): SectionType {
  const lower = headerLine.toLowerCase();
  if (/electronic\s+withdraw|atm\s+&?\s*debit|checks?\s+paid|withdraw.*debit|debit.*withdraw|other\s+(?:debit|charge|subtraction)/i.test(lower)) return "withdrawals";
  if (/deposit|credit|addition/i.test(lower)) return "deposits";
  if (/withdraw|debit|check|subtraction|electronic\s+payment|other\s+deduction/i.test(lower)) return "withdrawals";
  if (/transaction/i.test(lower)) return "transactions";
  return "unknown";
}

const MONTH_NAME_MAP: Record<string, string> = {
  jan: "01", feb: "02", mar: "03", apr: "04", may: "05", jun: "06",
  jul: "07", aug: "08", sep: "09", oct: "10", nov: "11", dec: "12",
};

function isTransactionDate(text: string): boolean {
  const t = text.trim();
  if (/^\d{1,2}[\/\-]\d{1,2}(?:[\/\-]\d{2,4})?$/.test(t)) return true;
  if (/^(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}$/i.test(t)) return true;
  return false;
}

function normalizeMonthNameDate(dateStr: string): string {
  const m = dateStr.match(/^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})$/i);
  if (m) {
    const mo = MONTH_NAME_MAP[m[1].toLowerCase().slice(0, 3)];
    return `${mo}/${m[2].padStart(2, "0")}`;
  }
  return dateStr;
}

function parseChaseAmountFromEnd(text: string): { desc: string; amount: number } | null {
  let trimmed = text.replace(/\s*$/, "");

  trimmed = trimmed.replace(/\s+\d{1,2}$/, "");

  const withDollar = trimmed.match(/^(.*?)\$(\d{1,3}(?:,\d{3})*\.\d{2})$/);
  if (withDollar) {
    const amt = parseFloat(withDollar[2].replace(/,/g, ""));
    if (amt > 0 && amt < 10_000_000) {
      return { desc: withDollar[1].trim(), amount: amt };
    }
  }

  if (!/\.\d{2}$/.test(trimmed)) return null;

  const spaceAmtMatch = trimmed.match(/^(.*\S)\s{2,}(\d{1,3}(?:,\d{3})*\.\d{2})$/);
  if (spaceAmtMatch) {
    const amt = parseFloat(spaceAmtMatch[2].replace(/,/g, ""));
    if (amt > 0 && amt < 10_000_000) {
      let desc = spaceAmtMatch[1].trim();
      if (/\d{3,}$/.test(desc)) desc = desc.replace(/\d+$/, "").trim();
      return { desc, amount: amt };
    }
  }

  const singleSpaceAmt = trimmed.match(/^(.*\S)\s+(\d{1,3}(?:,\d{3})*\.\d{2})$/);
  if (singleSpaceAmt) {
    const amt = parseFloat(singleSpaceAmt[2].replace(/,/g, ""));
    if (amt > 0 && amt < 10_000_000) {
      let desc = singleSpaceAmt[1].trim();
      if (/\d{3,}$/.test(desc)) desc = desc.replace(/\d+$/, "").trim();
      return { desc, amount: amt };
    }
  }

  const cardMatch = trimmed.match(/^(.*Card\s+\d{4})(\d{1,3}(?:,\d{3})*\.\d{2})$/);
  if (cardMatch) {
    const amt = parseFloat(cardMatch[2].replace(/,/g, ""));
    if (amt > 0 && amt < 100_000) {
      return { desc: cardMatch[1].trim(), amount: amt };
    }
  }

  const letterThenAmt = trimmed.match(/^(.*[A-Za-z)])(\d{1,3}(?:,\d{3})*\.\d{2})$/);
  if (letterThenAmt) {
    const amt = parseFloat(letterThenAmt[2].replace(/,/g, ""));
    if (amt > 0 && amt < 10_000_000) {
      return { desc: letterThenAmt[1].trim(), amount: amt };
    }
  }

  return null;
}

function parseChaseTransactions(rawText: string): ParsedTransaction[] {
  const transactions: ParsedTransaction[] = [];
  const lines = rawText.split("\n");
  let currentSection: SectionType = "unknown";

  const sectionHeaderPattern = /(?:DEPOSITS?\s+AND\s+(?:OTHER\s+)?(?:CREDITS?|ADDITIONS?)|ELECTRONIC\s+(?:WITHDRAWALS?|PAYMENTS?)|CHECKS?\s+PAID|ATM\s+&\s*DEBIT\s+CARD\s+(?:WITHDRAWALS?|PURCHASES?)|ATM\s+&\s*DEBIT\s+CARD\s+SUMMARY)/i;
  const skipPattern = /^(?:DATEDESCRIPTIONAMOUNT|Date\s*Description|SUBTOTAL|TOTAL\s|BALANCE\s+FORWARD|continued|page\s*\d|Pageof|CHECKING\s+SUMMARY|How\s+to\s+Avoid|SM$|INSTANCESAMOUNT|CUSTOMER\s+SERVICE|For\s+complete|If\s+you\s+meet|Here'?s\s+the\s+business|You\s+can\s+also|Maintain\s+a\s+linked|Meet\s+Chase|Congratulations|®|\s*$)/i;
  const summaryPattern = /^Total\s*(?:Deposits|Withdrawals|ATM|Card|Electronic)/i;
  const tableDataStart = /^--- TABLE DATA ---$/i;
  const structuredDataStart = /^\[STRUCTURED_PARSED_DATA\]/i;
  const stopParsing = /^(?:IN\s+CASE\s+OF\s+ERRORS|SERVICE\s+CHARGE\s+SUMMARY)/i;
  let inTableData = false;

  let pendingAchDate: string | null = null;
  let pendingAchDesc: string = "";
  let pendingAchSection: SectionType = "unknown";

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    if (structuredDataStart.test(line)) break;
    if (tableDataStart.test(line)) { inTableData = true; continue; }
    if (inTableData) {
      if (/^\*start\*/.test(line)) { inTableData = false; }
      else { continue; }
    }
    if (stopParsing.test(line)) { currentSection = "unknown"; continue; }

    if (/^\*start\*/.test(line)) {
      if (/deposit/i.test(line)) currentSection = "deposits";
      else if (/withdrawal|electronic/i.test(line)) currentSection = "withdrawals";
      else if (/atm.*debit.*summary/i.test(line)) currentSection = "unknown";
      continue;
    }
    if (/^\*end\*/.test(line)) {
      const embeddedTxn = line.match(/(\d{2}\/\d{2})\s+(.+?)\s+(\d{1,3}(?:,\d{3})*\.\d{2})\s*$/);
      if (embeddedTxn) {
        const eDate = embeddedTxn[1];
        const eDesc = embeddedTxn[2].trim();
        const eAmt = parseFloat(embeddedTxn[3].replace(/,/g, ""));
        if (eAmt > 0 && eAmt < 10_000_000) {
          const eType = currentSection === "deposits" ? "credit" : "debit";
          transactions.push({ date: eDate, description: eDesc, amount: eAmt, type: eType });
        }
      }
      continue;
    }

    if (sectionHeaderPattern.test(line) && !/^\d{2}\/\d{2}/.test(line)) {
      const detected = detectSectionType(line);
      if (detected !== "unknown") currentSection = detected;
      continue;
    }

    if (skipPattern.test(line)) continue;
    if (summaryPattern.test(line)) continue;

    if (pendingAchDate && !/^\d{2}\/\d{2}/.test(line)) {
      const amtOnly = line.match(/^(\d{1,3}(?:,\d{3})*\.\d{2})\s*$/);
      if (amtOnly) {
        const amount = parseFloat(amtOnly[1].replace(/,/g, ""));
        if (amount > 0 && amount < 10_000_000) {
          const type = pendingAchSection === "deposits" ? "credit" : "debit";
          transactions.push({ date: pendingAchDate, description: pendingAchDesc.trim(), amount, type });
        }
        pendingAchDate = null;
        pendingAchDesc = "";
        continue;
      }
      if (/^(?:Descr:|Sec:|Trace|Ind\s|Name:|Branch|\d{3}-\d{3}-\d{4}|Account=)/i.test(line)) {
        pendingAchDesc += " " + line;
        continue;
      }
      pendingAchDate = null;
      pendingAchDesc = "";
    }

    const dateMatch = line.match(/^(\d{2}\/\d{2})(.*)/);
    if (!dateMatch) continue;

    const date = dateMatch[1];
    const rest = dateMatch[2];

    if (!rest || rest.trim().length === 0) continue;

    const achStart = rest.match(/^(.*)Orig\s+CO\s+Name:\s*(.+)/i);
    if (achStart) {
      pendingAchDate = date;
      pendingAchDesc = achStart[2].trim();
      pendingAchSection = currentSection;
      const amtInline = parseChaseAmountFromEnd(rest);
      if (amtInline && amtInline.amount > 0) {
        const type = currentSection === "deposits" ? "credit" : "debit";
        transactions.push({ date, description: pendingAchDesc, amount: amtInline.amount, type });
        pendingAchDate = null;
        pendingAchDesc = "";
      }
      continue;
    }

    const bookTransfer = rest.match(/^(.*)Book\s+Transfer/i);
    if (bookTransfer) {
      const btAmtInline = parseChaseAmountFromEnd(rest);
      if (btAmtInline && btAmtInline.amount > 0) {
        const type = currentSection === "deposits" ? "credit" : "debit";
        const btDesc = "Book Transfer " + rest.slice(rest.indexOf("Book Transfer") + 13).trim();
        const btDescClean = btDesc.replace(/\s+\d{1,3}(?:,\d{3})*\.\d{2}.*$/, "").trim();
        transactions.push({ date, description: btDescClean, amount: btAmtInline.amount, type });
      } else {
        pendingAchDate = date;
        pendingAchDesc = "Book Transfer " + rest.slice(rest.indexOf("Book Transfer") + 13).trim();
        pendingAchSection = currentSection;
      }
      continue;
    }

    const parsed = parseChaseAmountFromEnd(rest);
    if (parsed && parsed.desc.length >= 2 && parsed.amount > 0) {
      let type: "debit" | "credit" = "debit";
      if (currentSection === "deposits") {
        type = "credit";
      } else if (currentSection === "withdrawals") {
        type = "debit";
      } else {
        type = classifyTransactionByDescription(parsed.desc);
      }
      transactions.push({ date, description: parsed.desc, amount: parsed.amount, type });
    }
  }

  if (transactions.length > 0) {
    console.log(`[TxnParser-Chase] Parsed ${transactions.length} transactions (${transactions.filter(t => t.type === "credit").length} credits, ${transactions.filter(t => t.type === "debit").length} debits)`);
  }

  return transactions;
}

function normalizeTransactionLines(rawText: string): string {
  const lines = rawText.split("\n");
  const result: string[] = [];
  for (const rawLine of lines) {
    let line = rawLine;
    const dateNoSpace = line.match(/^(\d{2}\/\d{2}(?:\/\d{2,4})?)([A-Za-z])/);
    if (dateNoSpace) {
      line = dateNoSpace[1] + " " + line.slice(dateNoSpace[1].length);
    }
    if (/^\d{2}\/\d{2}\s/.test(line)) {
      line = line.replace(/(Card\s*\d{4})\s*(\$?[\d,]+\.\d{2})\s*$/, "$1 $2");
      if (!/\s+\$?[\d,]+\.\d{2}\s*$/.test(line)) {
        const dollarAmt = line.match(/^(.+?)(\$\d{1,3}(?:,\d{3})*\.\d{2})\s*$/);
        if (dollarAmt) {
          line = dollarAmt[1].trimEnd() + " " + dollarAmt[2];
        } else {
          const bareAmt = line.match(/^(.+?[A-Za-z)\s])\s*(\d{1,3}(?:,\d{3})*\.\d{2})\s*$/);
          if (bareAmt) {
            line = bareAmt[1].trimEnd() + " " + bareAmt[2];
          }
        }
      }
    }
    result.push(line);
  }
  return result.join("\n");
}

function parseInwoodTransactions(rawText: string): ParsedTransaction[] {
  const transactions: ParsedTransaction[] = [];
  const lines = rawText.split("\n");
  let currentSection: "deposits" | "debits" | "unknown" = "unknown";
  const txnPattern = /^\s*(\d{1,2}\/\d{1,2})\s+(.+?)\s+([\d,]+\.\d{2})-?\s*$/;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const trimmed = line.trim();

    if (/\.+\s*DEPOSITS?\s+AND\s+OTHER\s+(?:ADDITIONS?|CREDITS?)\s*\.+/i.test(trimmed) ||
        /DEPOSITS?\s+AND\s+OTHER\s+(?:ADDITIONS?|CREDITS?)/i.test(trimmed)) {
      currentSection = "deposits";
      continue;
    }
    if (/\.+\s*CHECKS?\s+AND\s+OTHER\s+(?:DEBITS?|WITHDRAWALS?)\s*\.+/i.test(trimmed) ||
        /CHECKS?\s+AND\s+OTHER\s+(?:DEBITS?|WITHDRAWALS?)/i.test(trimmed)) {
      currentSection = "debits";
      continue;
    }
    if (/DAILY\s+BALANCE/i.test(trimmed) || /SERVICE\s+CHARGE\s+SUMMARY/i.test(trimmed) ||
        /CHECKS?\s+IN\s+(?:NUMBER|CHECK)\s+ORDER/i.test(trimmed) || /\.+\s*CHECKS?\s+IN\s/i.test(trimmed)) {
      currentSection = "unknown";
      continue;
    }
    if (currentSection === "unknown") continue;

    if (/^\s*Date\s+Description/i.test(trimmed)) continue;
    if (/^\s*(?:Subtotal|Total|Page\s+\d)/i.test(trimmed)) continue;
    if (/^\s*(?:Account\s+Number|Enclosure|Date\s+\d{1,2}\/\d{1,2}\/\d{2,4}\s+Page)/i.test(trimmed)) continue;
    if (/^\s*(?:Return\s+Service|P\s+O\s+BOX|\d{5}[\s-]\d{4})/i.test(trimmed)) continue;

    const match = trimmed.match(txnPattern);
    if (match) {
      const dateStr = match[1];
      const description = match[2].trim();
      const amountStr = match[3];
      const amount = parseFloat(amountStr.replace(/,/g, ""));
      if (amount <= 0 || amount > 100_000_000) continue;
      if (/^(?:page|continued|subtotal|total\b|balance|enclosure)/i.test(description)) continue;

      const hasTrailingMinus = line.trimEnd().endsWith("-");
      let type: "debit" | "credit";
      if (hasTrailingMinus) {
        type = "debit";
      } else if (currentSection === "deposits") {
        type = "credit";
      } else if (currentSection === "debits") {
        type = "debit";
      } else {
        type = hasTrailingMinus ? "debit" : "credit";
      }

      transactions.push({ date: dateStr, description, amount, type });
    }
  }

  const credits = transactions.filter(t => t.type === "credit").length;
  const debits = transactions.filter(t => t.type === "debit").length;
  console.log(`[TxnParser-Inwood] Parsed ${transactions.length} transactions (${credits} credits, ${debits} debits)`);
  return transactions;
}

function parseNavyFederalTransactions(rawText: string): ParsedTransaction[] {
  const transactions: ParsedTransaction[] = [];
  const lines = rawText.split("\n");
  let inTransactionSection = false;
  let pendingAmount: number | null = null;
  let pendingType: "debit" | "credit" = "debit";
  let pendingBalance: number | null = null;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    if (/Date\s+Transaction\s+Detail/i.test(line)) {
      inTransactionSection = true;
      continue;
    }
    if (!inTransactionSection) continue;
    if (/^\s*Page\s+\d/i.test(line) || /Statement\s+Period/i.test(line)) { continue; }
    if (/Summary\s+of\s+your\s+deposit/i.test(line)) break;

    const amountOnlyMatch = line.match(/^([\d,]+\.\d{2})(-?)\s*(?:([\d,]+\.\d{2})\s*)?$/);
    if (amountOnlyMatch) {
      const amt = parseFloat(amountOnlyMatch[1].replace(/,/g, ""));
      const isDebit = amountOnlyMatch[2] === "-";
      const secondAmt = amountOnlyMatch[3] ? parseFloat(amountOnlyMatch[3].replace(/,/g, "")) : null;
      if (amt > 0 && amt < 100_000_000) {
        if (pendingAmount !== null && !isDebit) {
          pendingBalance = amt;
          if (secondAmt) pendingBalance = secondAmt;
        } else {
          pendingAmount = amt;
          pendingType = isDebit ? "debit" : "credit";
          pendingBalance = secondAmt;
        }
      }
      continue;
    }

    const txnMatch = line.match(/^(\d{1,2}-\d{1,2})\s+(.+?)\s+([\d,]+\.\d{2})(-?)\s+([\d,]+\.\d{2})\s*$/);
    if (txnMatch) {
      const date = txnMatch[1];
      const desc = txnMatch[2].trim();
      const amt = parseFloat(txnMatch[3].replace(/,/g, ""));
      const isDebit = txnMatch[4] === "-";
      const balance = parseFloat(txnMatch[5].replace(/,/g, ""));
      if (amt > 0 && amt < 100_000_000 && desc.length >= 2 && !/^Beginning\s+Balance/i.test(desc)) {
        const type = isDebit ? "debit" : "credit";
        transactions.push({ date, description: desc, amount: amt, type, balance });
      }
      pendingAmount = null;
      pendingBalance = null;
      continue;
    }

    const dateDescBalance = line.match(/^(\d{1,2}-\d{1,2})\s+(.+?)\s+([\d,]+\.\d{2})\s*$/);
    if (dateDescBalance) {
      const date = dateDescBalance[1];
      const desc = dateDescBalance[2].trim();
      const balanceVal = parseFloat(dateDescBalance[3].replace(/,/g, ""));
      if (/^Beginning\s+Balance/i.test(desc)) {
        pendingAmount = null;
        pendingBalance = null;
        continue;
      }
      if (pendingAmount !== null) {
        transactions.push({ date, description: desc, amount: pendingAmount, type: pendingType, balance: balanceVal });
        pendingAmount = null;
        pendingBalance = null;
        continue;
      }
      continue;
    }

    const dateDescNoAmt = line.match(/^(\d{1,2}-\d{1,2})\s+(.+?)\s*$/);
    if (dateDescNoAmt && !/^\d{1,2}-\d{1,2}\s+\d/.test(line)) {
      if (pendingAmount !== null) {
        const date = dateDescNoAmt[1];
        const desc = dateDescNoAmt[2].trim();
        if (desc.length >= 2 && !/^Beginning\s+Balance/i.test(desc)) {
          transactions.push({ date, description: desc, amount: pendingAmount, type: pendingType, balance: pendingBalance ?? undefined });
          pendingAmount = null;
          pendingBalance = null;
        }
      }
      continue;
    }
  }

  if (transactions.length > 0) {
    console.log(`[TxnParser-NavyFederal] Parsed ${transactions.length} transactions (${transactions.filter(t => t.type === "credit").length} credits, ${transactions.filter(t => t.type === "debit").length} debits)`);
  }
  return transactions;
}

export function parseTransactionsFromText(rawText: string): ParsedTransaction[] {
  const isNavyFederal = /\bNavy\s*Federal\b|\bNFCU\b|navyfederal\.org/i.test(rawText);
  if (isNavyFederal) {
    return parseNavyFederalTransactions(rawText);
  }
  const isInwood = /\bINWOOD\s+NATIONAL\s+BANK\b|\bInwood\s+National\b|\binwoodbank\.com\b/i.test(rawText);
  if (isInwood) {
    return parseInwoodTransactions(rawText);
  }
  const isChase = /JPMorgan\s+Chase|Chase\s+Bank|CHASE\s+BUSINESS|Chase\s+Business/i.test(rawText);
  if (isChase) {
    return parseChaseTransactions(rawText);
  }
  const transactions: ParsedTransaction[] = [];
  const normalizedText = normalizeTransactionLines(rawText);
  const lines = normalizedText.split("\n");
  let currentSection: SectionType = "unknown";

  const sectionHeaderPattern = /(?:TRANSACTION\s+DETAIL|DEPOSITS?\s+AND\s+(?:OTHER\s+)?(?:CREDITS?|ADDITIONS?)|CHECKS?\s+(?:AND\s+OTHER\s+)?(?:DEBITS?|WITHDRAWALS?)|ELECTRONIC\s+(?:WITHDRAWALS?|PAYMENTS?)|WITHDRAWALS?\s+AND\s+(?:OTHER\s+)?(?:DEBITS?|SUBTRACTIONS?)|CHECKS?\s+PAID|OTHER\s+(?:WITHDRAWALS?|DEBITS?|CHARGES?)|ACH\s+TRANSACTIONS?|WIRE\s+TRANSFERS?|ATM\s+(?:TRANSACTIONS?|WITHDRAWALS?|DEPOSITS?)|TRANSACTION\s+HISTORY|ACCOUNT\s+ACTIVITY|ATM\s+&\s*DEBIT\s+CARD\s+(?:WITHDRAWALS?|PURCHASES?))/i;

  const skipLinePattern = /^(?:Date\s*Description|Date\s+Ref|SUBTOTAL|TOTAL\s|BALANCE\s+FORWARD|continued\s+on|page\s+\d|beginning\s+balance|ending\s+balance|previous\s+balance|closing\s+balance|DATEDESCRIPTIONAMOUNT|\*start\*|\*end\*|CHECKING\s+SUMMARY|How\s+to\s+Avoid|Pageof|SM$|INSTANCESAMOUNT|\s*$)/i;

  const summaryLinePattern = /^Total\s+(?:Deposits|Withdrawals|ATM|Card|Electronic)/i;

  let pendingAchDate: string | null = null;
  let pendingAchDesc: string = "";

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    if (sectionHeaderPattern.test(line) && !/^\d{2}\/\d{2}/.test(line)) {
      const detected = detectSectionType(line);
      if (detected !== "unknown") currentSection = detected;
      continue;
    }

    if (skipLinePattern.test(line)) continue;
    if (summaryLinePattern.test(line)) continue;
    if (/^(?:DAILY\s+(?:BALANCE|ENDING)|IN\s+CASE\s+OF\s+ERRORS)/i.test(line)) {
      currentSection = "unknown";
      continue;
    }

    if (pendingAchDate && !(/^\d{2}\/\d{2}/.test(line))) {
      const amtOnlyMatch = line.match(/^\$?([\d,]+\.\d{2})\s*$/);
      if (amtOnlyMatch) {
        const amount = Math.abs(parseAmount(amtOnlyMatch[1]));
        if (amount > 0 && amount < 100_000_000) {
          const type = currentSection === "deposits" ? "credit" : "debit";
          transactions.push({ date: pendingAchDate, description: pendingAchDesc.trim(), amount, type });
        }
        pendingAchDate = null;
        pendingAchDesc = "";
        continue;
      }
      if (/^(?:Descr:|Sec:|Trace|Ind\s|Name:|Branch|\d{3}-\d{3}-\d{4})/i.test(line)) {
        pendingAchDesc += " " + line;
        continue;
      }
      pendingAchDate = null;
      pendingAchDesc = "";
    }

    let matched = false;
    for (const pattern of TRANSACTION_LINE_PATTERNS) {
      const match = line.match(pattern);
      if (match) {
        const dateStr = match[1];
        if (!isTransactionDate(dateStr)) break;

        const description = match[2].trim();
        if (description.length < 2) break;
        if (/^(?:page|continued|subtotal|total\b|balance)/i.test(description)) break;

        let amount: number;
        let balance: number | undefined;
        let type: "debit" | "credit";

        if (match[4]) {
          amount = Math.abs(parseAmount(match[3]));
          balance = parseAmount(match[4]);
        } else {
          amount = Math.abs(parseAmount(match[3]));
        }

        if (amount === 0 || amount > 100_000_000) break;

        if (currentSection === "deposits") {
          type = "credit";
        } else if (currentSection === "withdrawals") {
          type = "debit";
        } else {
          const rawAmt = match[3];
          if (rawAmt.startsWith("-") || rawAmt.includes("(")) {
            type = "debit";
          } else {
            type = classifyTransactionByDescription(description);
          }
        }

        transactions.push({ date: dateStr, description, amount, type, balance });
        matched = true;

        if (i + 1 < lines.length) {
          const nextLine = lines[i + 1].trim();
          const nextStartsWithDate = /^\d{1,2}[\/\-]\d{1,2}/.test(nextLine) || /^(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\b/i.test(nextLine);
          const isAddressLine = /\b[A-Z]{2}\s+#\d{3,}/.test(nextLine) || /^\d+\s+\w+\s+(?:ST|STREET|AVE|AVENUE|DR|DRIVE|RD|ROAD|LN|LANE|BLVD|HWY|CT|PL|WAY|CIR|PKWY)\b/i.test(nextLine) || /\b\w+\s+[A-Z]{2}\s+\d{5}\b/.test(nextLine);
          if (nextLine && !nextStartsWithDate && !isAddressLine && !sectionHeaderPattern.test(nextLine) && !skipLinePattern.test(nextLine) && !summaryLinePattern.test(nextLine) && nextLine.length >= 3 && nextLine.length <= 80 && !(/^\$?[\d,]+\.\d{2}\s*$/.test(nextLine))) {
            transactions[transactions.length - 1].description += " " + nextLine;
            i++;
          }
        }

        break;
      }
    }

    if (!matched) {
      const achMatch = line.match(/^(\d{2}\/\d{2})\s*(?:Orig\s+CO\s+Name:|Online\s+Transfer\s+|ACH\s+)(.+)/i);
      if (achMatch && isTransactionDate(achMatch[1])) {
        pendingAchDate = achMatch[1];
        pendingAchDesc = achMatch[2].trim();
        continue;
      }
    }

    if (!matched && line.includes("|")) {
      const mdMatch = line.match(
        /^\|?\s*(?:(\d{1,2}\/\d{1,2}(?:\/\d{2,4})?)|((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}))\s*\|\s*(.+?)\s*\|\s*((?:-?\$?[\d,]+\.\d{2})?)\s*\|\s*((?:\$?[\d,]+\.\d{2})?)\s*\|\s*((?:-?\$?[\d,]+\.\d{2})?)\s*\|?\s*$/i
      );
      if (mdMatch) {
        const dateStr = mdMatch[1] || normalizeMonthNameDate(mdMatch[2] || "");
        if (isTransactionDate(mdMatch[1] || mdMatch[2] || "")) {
          const description = (mdMatch[3] || "").replace(/<br\/?>/gi, " ").trim();
          if (description.length >= 2 && !/^(?:page|continued|subtotal|total|balance|---)/i.test(description)) {
            const withdrawalStr = (mdMatch[4] || "").trim();
            const depositStr = (mdMatch[5] || "").trim();
            const balanceStr = (mdMatch[6] || "").trim();

            let amount = 0;
            let type: "debit" | "credit" = "debit";

            if (depositStr && parseAmount(depositStr) > 0) {
              amount = Math.abs(parseAmount(depositStr));
              type = "credit";
            } else if (withdrawalStr && parseAmount(withdrawalStr) !== 0) {
              amount = Math.abs(parseAmount(withdrawalStr));
              type = "debit";
            }

            if (amount > 0 && amount < 100_000_000) {
              const balance = balanceStr ? parseAmount(balanceStr) : undefined;
              transactions.push({ date: dateStr, description, amount, type, balance: balance || undefined });
            }
          }
        }
      }
    }
  }

  if (transactions.length > 0) {
    console.log(`[TxnParser] Parsed ${transactions.length} transactions (${transactions.filter(t => t.type === "credit").length} credits, ${transactions.filter(t => t.type === "debit").length} debits)`);
  }

  return transactions;
}

function classifyTransactionByDescription(desc: string): "debit" | "credit" {
  const creditKeywords = /\b(deposit|credit|transfer\s+in|incoming|payroll|direct\s+dep|mobile\s+deposit|cash\s+deposit|wire\s+in|ach\s+credit)\b/i;
  const debitKeywords = /\b(withdrawal|debit|check\s+#|chk\s+#|payment|purchase|transfer\s+out|wire\s+out|ach\s+debit|pos\s+debit|atm\s+withdrawal|fee|charge)\b/i;
  if (creditKeywords.test(desc)) return "credit";
  if (debitKeywords.test(desc)) return "debit";
  return "debit";
}

export function parseStatementData(rawText: string): ParsedStatementData {
  const bankName = extractBankName(rawText);
  const accountNumber = extractAccountNumber(rawText);
  const statementPeriod = extractStatementPeriod(rawText);
  const balances = extractBalances(rawText);
  const transactions = parseTransactionsFromText(rawText);

  const result: ParsedStatementData = {
    transactions,
    bankName,
    accountNumber,
    statementPeriod,
    beginningBalance: balances.beginning,
    endingBalance: balances.ending,
  };

  const depositTotal = extractDepositTotalFromRawText(rawText);
  if (depositTotal > 0) result.totalDeposits = depositTotal;

  const debitTotal = extractDebitTotalFromRawText(rawText);
  if (debitTotal > 0) result.totalWithdrawals = debitTotal;

  if (!result.totalDeposits && transactions.length > 0) {
    const creditSum = transactions.filter(t => t.type === "credit").reduce((s, t) => s + t.amount, 0);
    if (creditSum > 0) result.totalDeposits = creditSum;
  }
  if (!result.totalWithdrawals && transactions.length > 0) {
    const debitSum = transactions.filter(t => t.type === "debit").reduce((s, t) => s + t.amount, 0);
    if (debitSum > 0) result.totalWithdrawals = debitSum;
  }

  const maskedAcct = accountNumber ? `****${accountNumber.slice(-4)}` : "?";
  const credits = transactions.filter(t => t.type === "credit").length;
  const debits = transactions.filter(t => t.type === "debit").length;
  const creditSum = transactions.filter(t => t.type === "credit").reduce((s, t) => s + t.amount, 0);
  const debitSum = transactions.filter(t => t.type === "debit").reduce((s, t) => s + t.amount, 0);
  console.log(`[Parser] ${bankName || "Unknown bank"}: ${transactions.length} txns (${credits} credits=$${creditSum.toLocaleString()}, ${debits} debits=$${debitSum.toLocaleString()}), account=${maskedAcct}, period=${statementPeriod?.start || "?"}-${statementPeriod?.end || "?"}`);

  if (result.totalDeposits && creditSum > 0) {
    const ratio = creditSum / result.totalDeposits;
    if (ratio < 0.8 || ratio > 1.2) {
      console.log(`[Parser-CrossCheck] ⚠ Parsed credit sum $${creditSum.toLocaleString()} vs statement total deposits $${result.totalDeposits.toLocaleString()} (ratio=${ratio.toFixed(2)}) — may have missed transactions`);
    } else {
      console.log(`[Parser-CrossCheck] ✓ Parsed credits match statement total (ratio=${ratio.toFixed(2)})`);
    }
  }

  return result;
}

export function formatParsedDataForPrompt(result: ParsedStatementData): string {
  const hasData = result.transactions.length > 0 || result.accountNumber || result.beginningBalance !== undefined || result.endingBalance !== undefined || result.totalDeposits;
  if (!hasData) return "";

  let text = "\n--- VERIFIED DATA (machine-parsed from bank statement) ---\n";

  if (result.bankName) text += `Bank: ${result.bankName}\n`;
  if (result.accountNumber) text += `Account Number: ${result.accountNumber}\n`;
  if (result.statementPeriod) {
    text += `Statement Period: ${result.statementPeriod.start} to ${result.statementPeriod.end}\n`;
  }
  if (result.beginningBalance !== undefined) text += `Starting/Beginning Balance: $${result.beginningBalance.toLocaleString("en-US", { minimumFractionDigits: 2 })}\n`;
  if (result.endingBalance !== undefined) text += `Ending Balance: $${result.endingBalance.toLocaleString("en-US", { minimumFractionDigits: 2 })}\n`;

  if (result.totalDeposits || result.totalWithdrawals) {
    text += `\nVerified Summaries:\n`;
    if (result.totalDeposits) {
      text += `  Total Deposits and Additions $${result.totalDeposits.toLocaleString("en-US", { minimumFractionDigits: 2 })}\n`;
    }
    if (result.totalWithdrawals) {
      text += `  Withdrawals: $${result.totalWithdrawals.toLocaleString("en-US", { minimumFractionDigits: 2 })}\n`;
    }
    if (result.beginningBalance !== undefined) {
      text += `  Beginning Balance: $${result.beginningBalance.toLocaleString("en-US", { minimumFractionDigits: 2 })}\n`;
    }
    if (result.endingBalance !== undefined) {
      text += `  Ending Balance: $${result.endingBalance.toLocaleString("en-US", { minimumFractionDigits: 2 })}\n`;
    }
  }

  if (result.transactions.length > 0) {
    const deposits = result.transactions.filter(t => t.type === "credit");
    const debits = result.transactions.filter(t => t.type === "debit");
    text += `\nVerified Transactions: ${result.transactions.length} total (${deposits.length} credits, ${debits.length} debits)\n`;

    const recurring = findRecurringDebits(debits);
    if (recurring.length > 0) {
      text += `\nPotential Recurring Debits (machine-detected):\n`;
      for (const r of recurring) {
        const loanFlag = r.isLikelyLoan ? " ⚠️ LIKELY LOAN — contains lender/funding keyword" : "";
        text += `  "${r.description}" — $${r.amount.toFixed(2)} x ${r.count} times (${r.dates.join(", ")})${loanFlag}\n`;
      }
    }

    const lenderScanPattern = /\b(capital|advance|funding|finance|lending|finserv|ondeck|kabbage|bluevine|fundbox|credibly|libertas|yellowstone|fox|mantis|everest|cfg|mulligan|reliant|clearview|itria|cloudfund|navitas|vox|wynwood|platinum\s*rapid|qfs|jmb|samson|kings|fleetcor|stage\s*adv|7even|cashable|vitalcap|vcg)\b/i;
    const notLoanScan = /\b(insurance|utility|electric|water|phone|internet|rent|payroll|gusto|adp|paychex|tax|irs|stripe|clover|visa|mastercard|amex|discover|equipment\s*financ|truck\s*financ|capital\s*one\s*card)\b/i;
    const lenderDebits = debits.filter(d => lenderScanPattern.test(d.description) && !notLoanScan.test(d.description));
    if (lenderDebits.length > 0) {
      const lenderGroups = new Map<string, { amount: number; count: number; dates: string[] }[]>();
      for (const d of lenderDebits) {
        const descWords = d.description.replace(/\d{6,}/g, "").replace(/\b(ACH|CORP|DEBIT|EXPC|INC|LLC|CUSTOMER|ID|AUTOPAY|PYMT|PMT|SCD|DBCRD)\b/gi, "").replace(/\s+/g, " ").trim();
        const key = descWords.slice(0, 40).toUpperCase();
        if (!lenderGroups.has(key)) lenderGroups.set(key, []);
        lenderGroups.get(key)!.push({ amount: d.amount, count: 1, dates: [d.date] });
      }
      text += `\n⚠️ LENDER-NAME SCAN — Debits containing lender/funding keywords:\n`;
      for (const [name, entries] of lenderGroups) {
        const amounts = entries.map(e => e.amount);
        const uniqueAmounts = [...new Set(amounts.map(a => a.toFixed(2)))];
        text += `  "${name}" — ${entries.length} debit(s), amounts: ${uniqueAmounts.map(a => `$${a}`).join(", ")}, dates: ${entries.map(e => e.dates[0]).join(", ")}\n`;
      }
    }

    text += `\nAll Debit Transactions:\n`;
    for (const t of debits) {
      text += `  ${t.date} | ${t.description} | $${t.amount.toFixed(2)}${t.balance !== undefined ? ` | bal: $${t.balance.toFixed(2)}` : ""}\n`;
    }

    text += `\nAll Credit Transactions:\n`;
    for (const t of deposits) {
      text += `  ${t.date} | ${t.description} | $${t.amount.toFixed(2)}${t.balance !== undefined ? ` | bal: $${t.balance.toFixed(2)}` : ""}\n`;
    }

    if (deposits.length > 0) {
      const creditSum = deposits.reduce((s, t) => s + t.amount, 0);
      text += `\nParsed Credit Transaction Sum: $${creditSum.toLocaleString("en-US", { minimumFractionDigits: 2 })} (${deposits.length} transactions)\n`;
    }
  }

  text += "\n--- END VERIFIED DATA ---\n";
  return text;
}

const UNIVERSAL_SUMMARY_LABELS: { key: string; pattern: RegExp }[] = [
  { key: "beginningBalance", pattern: /(?:beginning|opening|starting|previous|prior)\s+balance/i },
  { key: "deposits",        pattern: /(?:deposits?\s+and\s+(?:other\s+)?(?:additions?|credits?)|total\s+deposits?(?:\s+and\s+additions?)?|credits?\s+this\s+(?:statement\s+)?period|deposits?\s+\/\s*credits?|credits?\s*\(\s*\+\s*\))/i },
  { key: "checksPaid",       pattern: /checks?\s+paid/i },
  { key: "electronicWith",   pattern: /electronic\s+(?:withdrawals?|payments?)/i },
  { key: "fees",             pattern: /(?:(?:service|monthly|maintenance|account)\s+)(?:fees?|charges?)(?:\s+(?:and\s+)?(?:charges?|assessments?))?/i },
  { key: "endingBalance",    pattern: /(?:ending|closing|new)\s+balance/i },
  { key: "otherWith",        pattern: /(?:other|atm|misc(?:ellaneous)?|wire)\s+(?:withdrawals?|charges?|debits?)/i },
  { key: "debits",           pattern: /(?:debits?\s+this\s+(?:statement\s+)?period|(?:withdrawals?|debits?)\s+and\s+(?:other\s+)?(?:subtractions?|charges?|debits?)|total\s+(?:withdrawals?|debits?))/i },
];

const CHASE_SUMMARY_LABELS = UNIVERSAL_SUMMARY_LABELS;

const SUMMARY_SECTION_HEADERS = [
  /CHECKING\s+SUMMARY/i,
  /SAVINGS\s+SUMMARY/i,
  /ACCOUNT\s+SUMMARY/i,
  /STATEMENT\s+SUMMARY/i,
  /ACCOUNT\s+ACTIVITY\s+SUMMARY/i,
  /ACCOUNT\s+OVERVIEW/i,
  /SUMMARY\s+OF\s+ACCOUNT\s+ACTIVITY/i,
  /ACCOUNT\s+BALANCE\s+SUMMARY/i,
];

interface SummaryLabelPosition {
  key: string;
  lineIdx: number;
  charIdx: number;
  inlineAmount?: number;
}

function parseCheckingSummaryTable(blockText: string): { deposits: number; beginningBalance: number } | null {
  const lines = blockText.split("\n");

  const seenKeys = new Set<string>();
  const labelPositions: SummaryLabelPosition[] = [];
  for (let i = 0; i < lines.length; i++) {
    for (const lbl of CHASE_SUMMARY_LABELS) {
      if (seenKeys.has(lbl.key)) continue;
      const m = lines[i].match(lbl.pattern);
      if (m) {
        seenKeys.add(lbl.key);
        labelPositions.push({ key: lbl.key, lineIdx: i, charIdx: lines.slice(0, i).join("\n").length + (m.index || 0) });
      }
    }
  }

  if (!labelPositions.find(l => l.key === "deposits")) return null;
  if (labelPositions.length < 2) return null;

  const lineNums = labelPositions.map(l => l.lineIdx);
  const lineSpan = Math.max(...lineNums) - Math.min(...lineNums);
  if (lineSpan > 15) return null;

  const labelOrder = [...labelPositions].sort((a, b) => a.charIdx - b.charIdx);

  for (const lp of labelOrder) {
    const lineText = lines[lp.lineIdx];
    const amts = lineText.match(/\$?\-?\(?\d[\d,]*\.\d{2}\)?/g);
    if (amts && amts.length > 0) {
      const lastAmt = amts[amts.length - 1];
      lp.inlineAmount = Math.abs(parseFloat(lastAmt.replace(/[$,()]/g, "")));
    }
  }

  const allHaveInline = labelOrder.every(lp => lp.inlineAmount !== undefined && lp.inlineAmount > 0);
  if (allHaveInline) {
    const depositsEntry = labelOrder.find(l => l.key === "deposits");
    const begBalEntry = labelOrder.find(l => l.key === "beginningBalance");
    const endBalEntry = labelOrder.find(l => l.key === "endingBalance");
    const begBal = begBalEntry?.inlineAmount || 0;
    const deposits = depositsEntry!.inlineAmount!;
    if (endBalEntry?.inlineAmount && begBal > 0) {
      const endBal = endBalEntry.inlineAmount;
      let totalDebits = 0;
      for (const lp of labelOrder) {
        if (lp.key === "checksPaid" || lp.key === "electronicWith" || lp.key === "otherWith" || lp.key === "fees" || lp.key === "debits") {
          totalDebits += lp.inlineAmount || 0;
        }
      }
      const expected = begBal + deposits - totalDebits;
      const diff = Math.abs(expected - endBal);
      const tolerance = Math.max(endBal, begBal) * 0.05;
      if (diff > tolerance && diff > 100) {
        // console.log(`[DepositExtract] Account Summary inline balance check WARN: beg=$${begBal.toFixed(2)} + dep=$${deposits.toFixed(2)} - debits=$${totalDebits.toFixed(2)} = $${expected.toFixed(2)} vs ending=$${endBal.toFixed(2)} (diff=$${diff.toFixed(2)})`);
      }
    }
    // console.log(`[DepositExtract] Account Summary (inline amounts): ${labelOrder.map(l => `${l.key}=$${l.inlineAmount?.toFixed(2)}`).join(", ")}`);
    return { deposits, beginningBalance: begBal };
  }

  const endingBalEntry = labelOrder.find(l => l.key === "endingBalance");
  const tableBoundaryLine = endingBalEntry
    ? endingBalEntry.lineIdx
    : Math.max(...labelOrder.map(l => l.lineIdx));
  const firstLabelLine = Math.min(...labelOrder.map(l => l.lineIdx));
  const tableStartChar = lines.slice(0, firstLabelLine).join("\n").length + (firstLabelLine > 0 ? 1 : 0);
  const tableEndChar = lines.slice(0, tableBoundaryLine + 1).join("\n").length + 200;
  const tableRegion = blockText.slice(tableStartChar, Math.min(blockText.length, tableEndChar));

  const labelsInRegion = labelOrder.filter(l => l.lineIdx <= tableBoundaryLine);

  const amountRegex = /\$?\-?\(?\d[\d,]*\.\d{2}\)?/g;
  const allAmounts: number[] = [];
  let am;
  while ((am = amountRegex.exec(tableRegion)) !== null) {
    allAmounts.push(Math.abs(parseFloat(am[0].replace(/[$,()]/g, ""))));
  }

  const depositsIdx = labelsInRegion.findIndex(l => l.key === "deposits");
  if (depositsIdx >= 0 && depositsIdx < allAmounts.length) {
    const depositsLine = lines[labelsInRegion[depositsIdx].lineIdx];
    const isBreakdownHeader = /deposits?\s+and\s+(?:other\s+)?(?:additions?|credits?)/i.test(depositsLine)
      && /(?:checks?|deductions?)/i.test(depositsLine);
    if (isBreakdownHeader) {
      const startLine = labelsInRegion[depositsIdx].lineIdx;
      const subBlock = lines.slice(startLine, startLine + 12).join("\n");
      const totalMatch = subBlock.match(/\bTotal\s+(\d+)\s+\$?([\d,]+\.\d{2})/);
      if (totalMatch) {
        const totalAmt = parseFloat(totalMatch[2].replace(/,/g, ""));
        if (totalAmt > 100 && totalAmt < 100_000_000) {
          // console.log(`[DepositExtract] PNC breakdown header detected — using Total line: ${totalMatch[1]} items, $${totalAmt.toFixed(2)}`);
          return { deposits: totalAmt, beginningBalance: 0 };
        }
      }
    }

    const begBalIdx = labelsInRegion.findIndex(l => l.key === "beginningBalance");
    const begBal = begBalIdx >= 0 && begBalIdx < allAmounts.length ? allAmounts[begBalIdx] : 0;
    const deposits = allAmounts[depositsIdx];

    const endBalIdx = labelsInRegion.findIndex(l => l.key === "endingBalance");
    if (endBalIdx >= 0 && endBalIdx < allAmounts.length && begBal > 0) {
      const endBal = allAmounts[endBalIdx];
      let totalDebits = 0;
      for (let i = 0; i < labelsInRegion.length && i < allAmounts.length; i++) {
        const k = labelsInRegion[i].key;
        if (k === "checksPaid" || k === "electronicWith" || k === "otherWith" || k === "fees" || k === "debits") {
          totalDebits += allAmounts[i];
        }
      }
      const expected = begBal + deposits - totalDebits;
      const diff = Math.abs(expected - endBal);
      const tolerance = Math.max(endBal, begBal) * 0.05;
      if (diff > tolerance && diff > 100) {
        // console.log(`[DepositExtract] Account Summary balance check WARN: beg=$${begBal.toFixed(2)} + dep=$${deposits.toFixed(2)} - debits=$${totalDebits.toFixed(2)} = $${expected.toFixed(2)} vs ending=$${endBal.toFixed(2)} (diff=$${diff.toFixed(2)})`);
      }
    }

    // console.log(`[DepositExtract] Account Summary (positional): labels=[${labelsInRegion.map(l => l.key).join(",")}], amounts=[${allAmounts.map(a => `$${a.toFixed(2)}`).join(",")}], depositsIdx=${depositsIdx} → $${deposits.toFixed(2)}`);
    return {
      deposits,
      beginningBalance: begBal,
    };
  }

  return null;
}

function extractColumnLayoutDepositFromText(rawText: string): number {
  for (const headerPattern of SUMMARY_SECTION_HEADERS) {
    const headerMatch = rawText.match(headerPattern);
    if (headerMatch) {
      const summaryStart = headerMatch.index!;
      const summaryBlock = rawText.slice(summaryStart, summaryStart + 1500);
      const parsed = parseCheckingSummaryTable(summaryBlock);
      if (parsed && parsed.deposits > 100 && parsed.deposits < 100_000_000) {
        return parsed.deposits;
      }
    }
  }

  const depositRegex = /deposits?\s+and\s+(?:other\s+)?(?:additions?|credits?)/gi;
  let depositMatch;
  let best = 0;
  while ((depositMatch = depositRegex.exec(rawText)) !== null) {
    const nearby = rawText.slice(depositMatch.index, depositMatch.index + 400);
    const hasColumnLayout = /(?:Deposits\s+and\s+\w+)\n(?:(?:Checks|Electronic|ATM|Fees|Ending|Other|Service|Misc|Wire|Beginning|CHECKING|Chase|INSTANCES|AMOUNT)\b[^\n]*\n)+/i.test(nearby)
      || /INSTANCES\s*\n\s*AMOUNT/i.test(nearby);
    if (hasColumnLayout) {
      const amountRegex = /\$?([\d,]+\.\d{2})/g;
      const allAmounts: number[] = [];
      let m;
      while ((m = amountRegex.exec(nearby)) !== null) {
        const amt = parseFloat(m[1].replace(/,/g, ""));
        if (amt > 0) allAmounts.push(amt);
      }
      if (allAmounts.length >= 2 && allAmounts[1] > 100 && allAmounts[1] < 100_000_000 && allAmounts[1] > best) {
        // console.log(`[DepositExtract] Column layout: skipping first $${allAmounts[0].toFixed(2)} (beg bal), using $${allAmounts[1].toFixed(2)} (deposits)`);
        best = allAmounts[1];
      } else if (allAmounts.length === 1 && allAmounts[0] > 1000 && allAmounts[0] < 100_000_000 && allAmounts[0] > best) {
        best = allAmounts[0];
      }
    }
  }
  if (best > 0) return best;

  return 0;
}

function extractCreditThisPeriod(rawText: string): number {
  const labelPatterns = [
    /(\d+)\s+credit\(?s?\)?\s+this\s+period/gi,
    /credit\(?s?\)?\s+this\s+period/gi,
    /credit\s*\(\s*s\s*\)\s*this\s*period/gi,
    /credits?\s+this\s+(?:statement\s+)?period/gi,
  ];
  let best = 0;
  for (const labelRegex of labelPatterns) {
    let labelMatch;
    while ((labelMatch = labelRegex.exec(rawText)) !== null) {
      const forwardRange = 500;
      const backwardRange = 200;
      const startBack = Math.max(0, labelMatch.index - backwardRange);
      const endFwd = Math.min(rawText.length, labelMatch.index + labelMatch[0].length + forwardRange);
      const nearby = rawText.slice(labelMatch.index, endFwd);
      const behind = rawText.slice(startBack, labelMatch.index);
      const amountRegex = /\$?([\d,]+\.\d{2})/g;
      let am;
      while ((am = amountRegex.exec(nearby)) !== null) {
        const amt = parseFloat(am[1].replace(/,/g, ""));
        if (amt > 100 && amt < 100_000_000 && amt > best) {
          // console.log(`[DepositExtract] Credit(s) This Period proximity (fwd): $${amt.toFixed(2)}`);
          best = amt;
          break;
        }
      }
      if (best === 0) {
        const behindAmounts: number[] = [];
        const behindRegex = /\$?([\d,]+\.\d{2})/g;
        let bm;
        while ((bm = behindRegex.exec(behind)) !== null) {
          const amt = parseFloat(bm[1].replace(/,/g, ""));
          if (amt > 100 && amt < 100_000_000) behindAmounts.push(amt);
        }
        if (behindAmounts.length > 0) {
          const lastBehind = behindAmounts[behindAmounts.length - 1];
          if (lastBehind > best) {
            // console.log(`[DepositExtract] Credit(s) This Period proximity (behind): $${lastBehind.toFixed(2)}`);
            best = lastBehind;
          }
        }
      }
      if (best > 0) break;
    }
    if (best > 0) break;
  }
  if (best === 0) {
    const hasCredit = /credit/i.test(rawText);
    const hasPeriod = /this\s+period/i.test(rawText);
    if (hasCredit && hasPeriod) {
      // console.log(`[DepositExtract] WARN: rawText contains 'credit' and 'this period' but no Credit(s) This Period regex matched`);
    }
  }
  return best;
}

function extractAccountSummaryCredits(rawText: string): number {
  const creditMatch = rawText.match(/credit\(?s?\)?\s+this\s+(?:statement\s+)?period/i);
  const beginMatch = rawText.match(/(?:beginning|opening|starting|previous)\s+balance/i);
  if (!creditMatch || !beginMatch) return 0;

  const creditPos = creditMatch.index!;
  const beginPos = beginMatch.index!;

  const debitMatch = rawText.match(/debit\(?s?\)?\s+this\s+(?:statement\s+)?period/i);
  const endMatch = rawText.match(/(?:ending|closing)\s+balance/i);

  const descLabels = [
    { label: "beginning", pos: beginPos },
    { label: "credit", pos: creditPos },
  ];
  if (debitMatch) descLabels.push({ label: "debit", pos: debitMatch.index! });
  if (endMatch) descLabels.push({ label: "ending", pos: endMatch.index! });
  descLabels.sort((a, b) => a.pos - b.pos);

  const creditOrder = descLabels.findIndex(d => d.label === "credit");
  if (creditOrder < 0) return 0;

  const endingEntry = descLabels.find(d => d.label === "ending");
  const lastDescPos = descLabels[descLabels.length - 1].pos;
  const boundaryEnd = endingEntry ? endingEntry.pos + 500 : lastDescPos + 1000;
  const searchArea = rawText.slice(lastDescPos, Math.min(rawText.length, boundaryEnd));
  const amountRegex = /\$?([\d,]+\.\d{2})/g;
  const amounts: number[] = [];
  let am;
  while ((am = amountRegex.exec(searchArea)) !== null) {
    const val = parseFloat(am[1].replace(/,/g, ""));
    if (val > 0) amounts.push(val);
  }

  if (amounts.length > creditOrder && amounts[creditOrder] > 100) {
    const begIdx = descLabels.findIndex(d => d.label === "beginning");
    const endIdx = descLabels.findIndex(d => d.label === "ending");
    const debitIdx = descLabels.findIndex(d => d.label === "debit");
    if (begIdx >= 0 && endIdx >= 0 && debitIdx >= 0 && begIdx < amounts.length && endIdx < amounts.length && debitIdx < amounts.length) {
      const bv = amounts[begIdx];
      const ev = amounts[endIdx];
      const dv = amounts[debitIdx];
      const cv = amounts[creditOrder];
      const expected = bv + cv - dv;
      const diff = Math.abs(expected - ev);
      if (diff > 1.0 && diff / Math.max(ev, 1) > 0.01) {
        // console.log(`[DepositExtract] Account Summary balance check WARN: beg=$${bv.toFixed(2)} + credit=$${cv.toFixed(2)} - debit=$${dv.toFixed(2)} = $${expected.toFixed(2)} vs ending=$${ev.toFixed(2)} (diff=$${diff.toFixed(2)})`);
      }
    }
    // console.log(`[DepositExtract] Account Summary column-format: credit is description #${creditOrder}, amount=$${amounts[creditOrder].toFixed(2)}`);
    return amounts[creditOrder];
  }
  return 0;
}

function extractDebitTotalFromRawText(rawText: string): number {
  const patterns = [
    /(\d+)\s+debit\(?s?\)?\s+this\s+period\s+\$?([\d,]+\.?\d*)/gi,
    /total\s+(?:withdrawals?|debits?)\s+(?:and\s+(?:other\s+)?(?:fees?|charges?)?\s*)?\$?([\d,]+\.?\d*)/gi,
    /(?:withdrawals?|debits?)\s+(?:and\s+other\s+)?(?:subtractions?|charges?)?\s+\$?([\d,]+\.?\d*)/gi,
    /(?:checks?\s+paid\s+and\s+other\s+)?(?:withdrawals?\s+and\s+(?:other\s+)?(?:debits?|subtractions?))\s+\$?([\d,]+\.?\d*)/gi,
    /withdrawals?\s*\/\s*debits?\s+\$?([\d,]+\.\d{2})/gi,
    /(?:^|\n)\s*(?:total\s+)?(?:withdrawals?|debits?)\s+\$?([\d,]+\.\d{2})/gim,
    /(?:checks?\s+and\s+other\s+)?debits?\s*\(-\)\s*\$?\s*([\d,]+\.\d{2})/gi,
  ];
  for (const pat of patterns) {
    const regex = new RegExp(pat.source, pat.flags);
    let m;
    while ((m = regex.exec(rawText)) !== null) {
      const amtStr = m[m.length - 1];
      const amt = parseFloat(amtStr.replace(/,/g, ""));
      if (amt > 100 && amt < 100_000_000) {
        // console.log(`[DepositExtract] Found debit total from raw text: $${amt.toFixed(2)}`);
        return amt;
      }
    }
  }

  const debitIdx = rawText.search(/debit\(?s?\)?\s+this\s+period/i);
  if (debitIdx >= 0) {
    const nearby = rawText.slice(debitIdx, debitIdx + 500);
    const amtMatch = nearby.match(/\$?([\d,]+\.\d{2})/);
    if (amtMatch) {
      const amt = parseFloat(amtMatch[1].replace(/,/g, ""));
      if (amt > 100 && amt < 100_000_000) {
        // console.log(`[DepositExtract] Found debit total proximity: $${amt.toFixed(2)}`);
        return amt;
      }
    }
    const behind = rawText.slice(Math.max(0, debitIdx - 300), debitIdx);
    const behindAmounts: number[] = [];
    const behindRegex = /\$?([\d,]+\.\d{2})/g;
    let bm;
    while ((bm = behindRegex.exec(behind)) !== null) {
      const amt = parseFloat(bm[1].replace(/,/g, ""));
      if (amt > 100 && amt < 100_000_000) behindAmounts.push(amt);
    }
    if (behindAmounts.length > 0) {
      const lastBehind = behindAmounts[behindAmounts.length - 1];
      // console.log(`[DepositExtract] Found debit total proximity (behind): $${lastBehind.toFixed(2)}`);
      return lastBehind;
    }
  }

  return 0;
}

export function computeDepositsFromBalance(rawText: string, beginBal: number, endBal: number): number {
  const debitTotal = extractDebitTotalFromRawText(rawText);
  if (debitTotal <= 0) {
    // console.log(`[DepositExtract] Balance equation: could not find debit total from raw text`);
    if (endBal > beginBal) {
      const minDeposits = endBal - beginBal;
      // console.log(`[DepositExtract] Balance floor: ending > beginning, min deposits = $${minDeposits.toFixed(2)}`);
      return minDeposits;
    }
    return 0;
  }
  const deposits = endBal - beginBal + debitTotal;
  if (deposits > 0 && deposits < 100_000_000) {
    // console.log(`[DepositExtract] Balance equation: end($${endBal.toFixed(2)}) - begin($${beginBal.toFixed(2)}) + debits($${debitTotal.toFixed(2)}) = deposits($${deposits.toFixed(2)})`);
    return deposits;
  }
  return 0;
}

function parseMoneyFromLine(line: string): number | null {
  const m = line.match(/^[\s]*[-]?\$?\(?([\d,]+\.\d{2})\)?(?:\s*(?:CR|DR))?[\s]*$/i);
  if (!m) return null;
  const v = parseFloat(m[1].replace(/,/g, ""));
  return v > 0 ? v : null;
}

function extractChaseAccountSummary(rawText: string): number {
  const labelMatch = rawText.match(/deposits?\s+and\s+other\s+credits?[\s:]*(?:\n|(?=[\$\d]))/i);
  if (!labelMatch) {
    const altMatch = rawText.match(/deposits?\s+and\s+other\s+credits?\s+([\d,]+\.\d{2})/i);
    if (altMatch) {
      const v = parseFloat(altMatch[1].replace(/,/g, ""));
      if (v > 100 && v < 100_000_000) {
        // console.log(`[DepositExtract] BoA Account Summary (direct): $${v.toFixed(2)}`);
        return v;
      }
    }
    return 0;
  }

  const inlineAmt = labelMatch[0].match(/\$?([\d,]+\.\d{2})/);
  if (inlineAmt) {
    const v = parseFloat(inlineAmt[1].replace(/,/g, ""));
    if (v > 100 && v < 100_000_000) {
      // console.log(`[DepositExtract] Chase Account Summary (inline): $${v.toFixed(2)}`);
      return v;
    }
  }
  
  const afterLabel = rawText.slice(labelMatch.index!, labelMatch.index! + labelMatch[0].length + 200);
  const sameLineAmt = afterLabel.match(/credits?\s+([\d,]+\.\d{2})/i);
  if (sameLineAmt) {
    const v = parseFloat(sameLineAmt[1].replace(/,/g, ""));
    if (v > 100 && v < 100_000_000) {
      // console.log(`[DepositExtract] Account Summary (same-line no $): $${v.toFixed(2)}`);
      return v;
    }
  }

  const pos = labelMatch.index!;
  const nearby = rawText.slice(pos, pos + 800);
  const lines = nearby.split("\n").map(l => l.trim()).filter(Boolean);
  const descLabels: string[] = [];
  const amountValues: number[] = [];

  for (const line of lines) {
    const amt = parseMoneyFromLine(line);
    if (amt !== null) {
      amountValues.push(amt);
    } else if (/^#\s*of\s/i.test(line) || /^\d+$/.test(line)) {
      continue;
    } else if (/^[A-Za-z]/.test(line)) {
      descLabels.push(line.toLowerCase());
    }
  }

  const depositIdx = descLabels.findIndex(l => /deposits?\s+and\s+other\s+credits?/i.test(l));
  if (depositIdx >= 0 && depositIdx < amountValues.length) {
    const amt = amountValues[depositIdx];
    if (amt > 100 && amt < 100_000_000) {
      // console.log(`[DepositExtract] Chase Account Summary: label="${descLabels[depositIdx]}" → $${amt.toFixed(2)}`);
      return amt;
    }
  }

  const creditsCount = /# of deposits\/credits:\s*(\d+)/i.exec(nearby);
  if (creditsCount && amountValues.length >= 2) {
    const begBal = rawText.match(/(?:beginning|opening|previous)\s+balance[:\s]*\$?([\d,]+\.\d{2})/i);
    const bv = begBal ? parseFloat(begBal[1].replace(/,/g, "")) : -1;
    const endBal = rawText.match(/(?:ending|closing)\s+balance[:\s]*\$?([\d,]+\.\d{2})/i);
    const ev = endBal ? parseFloat(endBal[1].replace(/,/g, "")) : -1;
    for (const amt of amountValues) {
      if (bv >= 0 && Math.abs(amt - bv) < 0.01) continue;
      if (ev >= 0 && Math.abs(amt - ev) < 0.01) continue;
      if (amt > 100 && amt < 100_000_000) {
        // console.log(`[DepositExtract] Chase Account Summary (credits count found): $${amt.toFixed(2)}`);
        return amt;
      }
    }
  }

  return 0;
}

function extractMultiCategoryDeposits(rawText: string): number {
  const summaryMatch = rawText.match(/(?:ACCOUNT\s*SUMMARY|(?:SMALL\s+)?BUSINESS\s+(?:CHECKING\s+)?(?:ACCOUNT\s+)?SUMMARY|CHECKING\s+SUMMARY)/i);
  if (!summaryMatch) return 0;
  const rawBlock = rawText.slice(summaryMatch.index!, summaryMatch.index! + 1500);
  const block = rawBlock.replace(/\|/g, ' ').replace(/[-]{3,}/g, ' ');

  const isTdBank = /BeginningBalance|ElectronicDeposits|EndingBalance/i.test(block);
  const isBalanceForward = /Balance\s+Forward/i.test(block);

  const mainDepositMatch = block.match(/(?:^|\n)\s*(?:\+\s*)?(?:\d+\s+)?Deposits?\s+(?:\d+\s+)?\$?\s*([\d,]+\.\d{2})\+?/im);
  const electronicMatch = block.match(/(?:^|\n)\s*(?:\+\s*)?(?:\d+\s+)?Electronic\s*(?:Deposits?|Additions?)\s+(?:\d+\s+)?\$?\s*([\d,]+\.\d{2})\+?/im);
  const automaticMatch = block.match(/(?:^|\n)\s*(?:\+\s*)?(?:\d+\s+)?Automatic\s+(?:Deposits?|Additions?)\s+(?:\d+\s+)?\$?\s*([\d,]+\.\d{2})\+?/im);
  const directMatch = block.match(/(?:^|\n)\s*(?:\+\s*)?(?:\d+\s+)?Direct\s+(?:Deposits?|Additions?)\s+(?:\d+\s+)?\$?\s*([\d,]+\.\d{2})\+?/im);
  const mobileMatch = block.match(/(?:^|\n)\s*(?:\+\s*)?(?:\d+\s+)?Mobile\s+(?:Deposits?|Additions?)\s+(?:\d+\s+)?\$?\s*([\d,]+\.\d{2})\+?/im);
  const wireMatch = block.match(/(?:^|\n)\s*(?:\+\s*)?(?:\d+\s+)?(?:Wire|Incoming)\s+(?:Deposits?|Additions?)\s+(?:\d+\s+)?\$?\s*([\d,]+\.\d{2})\+?/im);
  const miscDepositMatch = block.match(/(?:^|\n)\s*(?:\+\s*)?(?:\d+\s+)?(?:Misc(?:ellaneous)?|Other)\s+(?:Deposits?|Additions?)\s+(?:\d+\s+)?\$?\s*([\d,]+\.\d{2})\+?/im);
  const otherCreditsMatch = block.match(/(?:^|\n)\s*(?:\+\s*)?(?:\d+\s+)?Other\s+Credits?\s+(?:\d+\s+)?\$?\s*([\d,]+\.\d{2})\+?/im);
  const customerMatch = block.match(/(?:^|\n)\s*(?:\+\s*)?(?:\d+\s+)?Customer\s+(?:Deposits?|Additions?)\s+(?:\d+\s+)?\$?\s*([\d,]+\.\d{2})\+?/im);
  const achMatch = block.match(/(?:^|\n)\s*(?:\+\s*)?(?:\d+\s+)?ACH\s+(?:Deposits?|Credits?|Additions?)\s+(?:\d+\s+)?\$?\s*([\d,]+\.\d{2})\+?/im);
  const tellerMatch = block.match(/(?:^|\n)\s*(?:\+\s*)?(?:\d+\s+)?Teller\s+(?:Deposits?|Additions?)\s+(?:\d+\s+)?\$?\s*([\d,]+\.\d{2})\+?/im);
  const atmDepositMatch = block.match(/(?:^|\n)\s*(?:\+\s*)?(?:\d+\s+)?ATM\s+(?:Deposits?(?:\s+and\s+Additions?)?|Additions?)\s+(?:\d+\s+)?\$?\s*([\d,]+\.\d{2})\+?/im);

  const mainAmt = mainDepositMatch ? parseFloat(mainDepositMatch[1].replace(/,/g, "")) : 0;
  const electronicAmt = electronicMatch ? parseFloat(electronicMatch[1].replace(/,/g, "")) : 0;
  const automaticAmt = automaticMatch ? parseFloat(automaticMatch[1].replace(/,/g, "")) : 0;
  const directAmt = directMatch ? parseFloat(directMatch[1].replace(/,/g, "")) : 0;
  const mobileAmt = mobileMatch ? parseFloat(mobileMatch[1].replace(/,/g, "")) : 0;
  const wireAmt = wireMatch ? parseFloat(wireMatch[1].replace(/,/g, "")) : 0;
  const miscAmt = miscDepositMatch ? parseFloat(miscDepositMatch[1].replace(/,/g, "")) : 0;
  const otherCreditsAmt = otherCreditsMatch ? parseFloat(otherCreditsMatch[1].replace(/,/g, "")) : 0;
  const customerAmt = customerMatch ? parseFloat(customerMatch[1].replace(/,/g, "")) : 0;
  const achAmt = achMatch ? parseFloat(achMatch[1].replace(/,/g, "")) : 0;
  const tellerAmt = tellerMatch ? parseFloat(tellerMatch[1].replace(/,/g, "")) : 0;
  const atmAmt = atmDepositMatch ? parseFloat(atmDepositMatch[1].replace(/,/g, "")) : 0;

  const extraCategories = electronicAmt + automaticAmt + directAmt + mobileAmt + wireAmt + miscAmt + otherCreditsAmt + customerAmt + achAmt + tellerAmt + atmAmt;

  if (extraCategories > 0 && mainAmt > 0) {
    const allSum = mainAmt + extraCategories;
    if (mainAmt >= extraCategories) {
      return mainAmt;
    }
    return allSum;
  }

  if (extraCategories > 0 && mainAmt === 0) {
    const catCount = [electronicAmt, automaticAmt, directAmt, mobileAmt, wireAmt, miscAmt, otherCreditsAmt, customerAmt, achAmt, tellerAmt, atmAmt].filter(a => a > 0).length;
    if (catCount >= 2) return extraCategories;
  }

  if (isTdBank && (mainAmt > 0 || electronicAmt > 0)) {
    const total = mainAmt + electronicAmt;
    return total;
  }

  if (isBalanceForward && mainAmt > 0 && extraCategories === 0) {
    return mainAmt;
  }

  return 0;
}

function extractCreditUnionSummaryDeposits(rawText: string): number {
  const otherCreditsMatch = rawText.match(/\d+\s+other\s+credits?\s+(?:for:?\s*)?\$?([\d,]+\.\d{2})/i);
  const atmDepositsMatch = rawText.match(/\d+\s+atm\s*\/?\s*debit\s+deposits?:?\s*\$?([\d,]+\.\d{2})/i);
  const electronicCreditsMatch = rawText.match(/\d+\s+electronic\s+credits?:?\s*\$?([\d,]+\.\d{2})/i);
  const directDepositsMatch = rawText.match(/\d+\s+direct\s+deposits?:?\s*\$?([\d,]+\.\d{2})/i);

  let total = 0;
  const parts: string[] = [];
  if (otherCreditsMatch) {
    const v = parseFloat(otherCreditsMatch[1].replace(/,/g, ""));
    if (v > 0) { total += v; parts.push(`OtherCredits=$${v.toFixed(2)}`); }
  }
  if (atmDepositsMatch) {
    const v = parseFloat(atmDepositsMatch[1].replace(/,/g, ""));
    if (v > 0) { total += v; parts.push(`ATMDeposits=$${v.toFixed(2)}`); }
  }
  if (electronicCreditsMatch) {
    const v = parseFloat(electronicCreditsMatch[1].replace(/,/g, ""));
    if (v > 0) { total += v; parts.push(`ElectronicCredits=$${v.toFixed(2)}`); }
  }
  if (directDepositsMatch) {
    const v = parseFloat(directDepositsMatch[1].replace(/,/g, ""));
    if (v > 0) { total += v; parts.push(`DirectDeposits=$${v.toFixed(2)}`); }
  }
  if (total > 0 && parts.length > 0) {
    // console.log(`[DepositExtract] Credit Union summary: ${parts.join(" + ")} = $${total.toFixed(2)}`);
  }
  return total;
}

function extractMultiCategoryDirect(rawText: string): number {
  const cleanText = rawText.replace(/\|/g, ' ').replace(/[-]{3,}/g, ' ');
  const summaryIdx = rawText.search(/(?:ACCOUNT\s*SUMMARY|CHECKING\s+SUMMARY|BUSINESS\s+(?:CHECKING\s+)?SUMMARY)/i);
  if (summaryIdx >= 0) {
    const snippet = rawText.slice(summaryIdx, summaryIdx + 600).replace(/\n/g, '\\n');
    // console.log(`[DepositExtract] DirectMultiCat summary snippet: "${snippet.substring(0, 400)}"`);
  } else {
    const hasDeposit = rawText.match(/deposits?/gi);
    // console.log(`[DepositExtract] DirectMultiCat: no summary header found. "deposits" mentions: ${hasDeposit?.length || 0}`);
  }
  const CATS = [
    { label: "deposits", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?(?:Total\s+)?Deposits?\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im, exclusive: true },
    { label: "automatic", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?Automatic\s+(?:Deposits?|Additions?)\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "electronic", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?Electronic\s+(?:Deposits?|Credits?|Additions?)\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "direct", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?Direct\s+(?:Deposits?|Additions?)\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "mobile", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?Mobile\s+(?:Deposits?|Additions?)\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "wire", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?(?:Wire|Incoming)\s+(?:Deposits?|Credits?|Transfers?|Additions?)\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "misc", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?(?:Misc(?:ellaneous)?|Other)\s+(?:Deposits?|Additions?)\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "otherCredits", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?Other\s+Credits?\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "ach", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?ACH\s+(?:Deposits?|Credits?|Additions?)\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "customer", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?Customer\s+(?:Deposits?|Additions?)\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "teller", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?Teller\s+(?:Deposits?|Additions?)\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "counterCredits", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?Counter\s+Credits?\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "merchantDeposits", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?Merchant\s+(?:Deposits?|Additions?)\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "lockboxDeposits", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?(?:Lockbox|Lock\s*Box)\s+(?:Deposits?|Additions?)\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "atmDeposits", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?ATM\s+(?:Deposits?(?:\s+and\s+Additions?)?|Additions?)\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "branchDeposits", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?(?:Branch|In-?\s*Branch)\s+(?:Deposits?|Additions?)\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "cashDeposits", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?Cash\s+(?:Deposits?|Additions?)\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "checkDeposits", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?Check\s+(?:Deposits?|Additions?)\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "creditMemos", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?Credit\s+Memo(?:s|randum)?\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "interestEarned", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?Interest\s+(?:Earned|Paid|Credit)\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "returnedItems", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?(?:Returned|Return)\s+(?:Items?|Deposits?)\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "creditAdjustments", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?Credit\s+Adjustments?\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "remittance", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?(?:Remittance|Remit)\s+(?:Credits?|Deposits?)\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
    { label: "posSales", pattern: /(?:^|\n)\s*(?:\+\s*)?(?:\d{1,3}\s+)?(?:POS|Point\s+of\s+Sale)\s+(?:Credits?|Deposits?|Sales?)\s+(?:\d{1,3}\s+)?\$?\s*([\d,]+\.\d{2})\s*\+?/im },
  ];
  let total = 0;
  let count = 0;
  const parts: string[] = [];
  const matched: { label: string; amt: number }[] = [];
  const DEPOSIT_PREFIX_WORDS = /\b(?:customer|other|electronic|direct|mobile|wire|incoming|automatic|misc|miscellaneous|ach|teller|counter|merchant|lockbox|lock\s*box|branch|atm|cash|check|return|returned|credit|remit|pos|point)\b/i;
  for (const cat of CATS) {
    const m = cleanText.match(cat.pattern);
    if (m) {
      const amt = parseFloat(m[1].replace(/,/g, ""));
      if (amt > 0 && amt < 100_000_000) {
        if ((cat as any).exclusive) {
          const matchedLine = m[0].trim();
          const beforeDeposit = matchedLine.replace(/deposits?\s.*$/i, "").trim();
          const lastWords = beforeDeposit.replace(/[\d\s+$.,]+/g, " ").trim();
          if (DEPOSIT_PREFIX_WORDS.test(lastWords)) continue;
        }
        matched.push({ label: cat.label, amt });
      }
    }
  }
  const depositsEntry = matched.find(m => m.label === "deposits");
  const subCategories = matched.filter(m => m.label !== "deposits");
  if (depositsEntry && subCategories.length >= 1) {
    const subSum = subCategories.reduce((s, m) => s + m.amt, 0);
    if (depositsEntry.amt >= subSum) {
      for (const m of [depositsEntry]) { total += m.amt; count++; parts.push(`${m.label}=$${m.amt.toFixed(2)} (parent total)`); }
    } else {
      for (const m of matched) { total += m.amt; count++; parts.push(`${m.label}=$${m.amt.toFixed(2)}`); }
    }
  } else {
    for (const m of matched) { total += m.amt; count++; parts.push(`${m.label}=$${m.amt.toFixed(2)}`); }
  }
  if (count >= 2) {
    return total;
  }

  const universalResult = universalDepositCategoryScan(cleanText);
  if (universalResult > 0) return universalResult;

  return 0;
}

function universalDepositCategoryScan(text: string): number {
  const summaryHeaders = [...text.matchAll(/(?:ACCOUNT\s*SUMMARY|CHECKING\s+SUMMARY|BUSINESS\s+(?:CHECKING\s+)?(?:ACCOUNT\s+)?SUMMARY|STATEMENT\s+SUMMARY|ACTIVITY\s+SUMMARY|BALANCE\s+SUMMARY)/gi)];
  if (summaryHeaders.length === 0) return 0;

  const creditLabels: Array<{ label: string; amount: number; lineIdx: number }> = [];

  const DEBIT_KEYWORDS = /\b(withdraw|debit|check\s+paid|fee|charge|payment|deduction|disbursement|draft)\b/i;
  const CREDIT_KEYWORDS = /\b(deposit|credit|addition|receipt|incoming|transfer\s+in|wire\s+in|ach\s+in|remit|merchant|lockbox|teller|mobile|branch|cash|check\s+dep|electronic|direct|counter|pos|sale)\b/i;
  const SKIP_KEYWORDS = /\b(beginning|opening|ending|closing|average|low|daily|available|minimum|statement\s+period|number\s+of|account\s+number|previous)\b/i;
  const TOTAL_KEYWORDS = /\b(total\s+deposit|total\s+credit|total\s+addition|total\s+receipt|deposits?\s+and\s+(?:other\s+)?(?:additions?|credits?))\b/i;
  const DATE_PREFIX = /^\d{1,2}\/\d{1,2}(?:\/\d{2,4})?\s+/;
  const CREDIT_PERIOD_SUMMARY = /\d+\s+credit\(?s?\)?\s+this\s+period/i;

  for (const headerMatch of summaryHeaders) {
    const startIdx = headerMatch.index!;
    const block = text.slice(startIdx, Math.min(text.length, startIdx + 1500));
    const blockLines = block.split("\n");

    let foundCreditPeriodSummary = false;
    const SECTION_STOP = /^(?:Electronic\s+(?:Credits?|Debits?)|Deposits?|Checks?\s+(?:Paid|Cleared)|Other\s+(?:Credits?|Debits?)|Daily\s+(?:Ledger|Balance)|Transaction\s+Detail|Date\s+Description\s+Amount)\s*$/i;

    for (let i = 0; i < blockLines.length; i++) {
      const line = blockLines[i].trim();
      if (!line || SKIP_KEYWORDS.test(line)) continue;
      if (i > 2 && SECTION_STOP.test(line)) break;

      const amountMatch = line.match(/\$?\s*([\d,]+\.\d{2})\s*[\+\-]?\s*$/);
      if (!amountMatch) continue;

      const amt = parseFloat(amountMatch[1].replace(/,/g, ""));
      if (amt <= 0 || amt >= 100_000_000) continue;

      const labelPart = line.slice(0, line.indexOf(amountMatch[0])).trim().replace(/[\.\s]+$/, "");
      if (!labelPart || labelPart.length < 3) continue;

      if (DATE_PREFIX.test(labelPart)) continue;

      if (DEBIT_KEYWORDS.test(labelPart)) continue;

      if (CREDIT_KEYWORDS.test(labelPart) && !TOTAL_KEYWORDS.test(labelPart)) {
        if (CREDIT_PERIOD_SUMMARY.test(labelPart)) {
          foundCreditPeriodSummary = true;
        }
        const isDupe = creditLabels.some(c => c.label.toLowerCase() === labelPart.toLowerCase() || Math.abs(c.amount - amt) < 0.01);
        if (!isDupe) {
          creditLabels.push({ label: labelPart, amount: amt, lineIdx: i });
        }
      }
    }

    if (foundCreditPeriodSummary) {
      const summaryEntry = creditLabels.find(c => CREDIT_PERIOD_SUMMARY.test(c.label));
      if (summaryEntry) {
        return summaryEntry.amount;
      }
    }
  }

  if (creditLabels.length >= 2) {
    const total = creditLabels.reduce((s, c) => s + c.amount, 0);
    const sumOfComponents = total;

    const totalRowMatch = text.match(/(?:total\s+(?:deposits?|credits?|additions?)(?:\s+and\s+(?:other\s+)?(?:credits?|additions?))?)\s*\$?\s*([\d,]+\.\d{2})/i);
    if (totalRowMatch) {
      const totalRowAmt = parseFloat(totalRowMatch[1].replace(/,/g, ""));
      if (totalRowAmt > 0 && Math.abs(totalRowAmt - sumOfComponents) / Math.max(totalRowAmt, sumOfComponents) < 0.05) {
        // console.log(`[DepositExtract] UNIVERSAL scan: components ($${sumOfComponents.toFixed(2)}) ≈ total row ($${totalRowAmt.toFixed(2)}). Using total row.`);
        return totalRowAmt;
      }
      if (totalRowAmt > sumOfComponents * 0.9 && totalRowAmt < sumOfComponents * 1.1) {
        // console.log(`[DepositExtract] UNIVERSAL scan: total row ($${totalRowAmt.toFixed(2)}) close to components ($${sumOfComponents.toFixed(2)}). Using total row.`);
        return totalRowAmt;
      }
    }

    const parts = creditLabels.map(c => `${c.label}=$${c.amount.toFixed(2)}`);
    // console.log(`[DepositExtract] UNIVERSAL multi-category scan: ${parts.join(" + ")} = $${total.toFixed(2)}`);
    return total;
  }

  return 0;
}

function extractIonBankAccountSummary(rawText: string): number {
  const headerMatch = rawText.match(/Beginning\s+Balance\s+Deposits\s+Interest\s+Paid/i);
  if (!headerMatch) return 0;

  const afterHeaders = rawText.slice(headerMatch.index! + headerMatch[0].length, headerMatch.index! + headerMatch[0].length + 600);
  const amounts: number[] = [];
  const amtRegex = /(\d[\d,]*\.\d{2})/g;
  let m;
  while ((m = amtRegex.exec(afterHeaders)) !== null) {
    amounts.push(parseFloat(m[1].replace(/,/g, "")));
    if (amounts.length >= 6) break;
  }

  if (amounts.length >= 2) {
    const deposits = amounts[1];
    if (deposits >= 0 && deposits < 100_000_000) {
      // console.log(`[DepositExtract] Ion Bank Account Summary table: BegBal=$${amounts[0].toFixed(2)}, Deposits=$${deposits.toFixed(2)}`);
      return deposits;
    }
  }
  return 0;
}

function extractNavyFederalSummaryDeposits(rawText: string): number {
  const isNavyFederal = /\bNavy\s*Federal\b|\bNFCU\b|navyfederal\.org/i.test(rawText);
  if (!isNavyFederal) return 0;

  const summaryMatch = rawText.match(
    /Summary\s+of\s+your\s+deposit\s+accounts[\s\S]*?Totals?\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})/i
  );
  if (summaryMatch) {
    const depositsCredits = parseFloat(summaryMatch[2].replace(/,/g, ""));
    if (depositsCredits > 0 && depositsCredits < 100_000_000) {
      console.log(`[DepositExtract] Navy Federal summary table Totals row: Deposits/Credits=$${depositsCredits.toFixed(2)}`);
      return depositsCredits;
    }
  }

  const checkingMatch = rawText.match(
    /Business\s+Checking[\s\S]{0,100}?\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})/i
  );
  if (checkingMatch) {
    const depositsCredits = parseFloat(checkingMatch[2].replace(/,/g, ""));
    if (depositsCredits > 0 && depositsCredits < 100_000_000) {
      console.log(`[DepositExtract] Navy Federal Business Checking: Deposits/Credits=$${depositsCredits.toFixed(2)}`);
      return depositsCredits;
    }
  }

  return 0;
}

export function extractDepositTotalFromRawText(rawText: string): number {
  const navyFederalAmt = extractNavyFederalSummaryDeposits(rawText);
  if (navyFederalAmt > 0) return navyFederalAmt;

  const ionBankAmt = extractIonBankAccountSummary(rawText);
  if (ionBankAmt > 0) {
    // console.log(`[DepositExtract] Ion Bank Account Summary authoritative: $${ionBankAmt.toFixed(2)}`);
    return ionBankAmt;
  }

  const nDepositsMatch = rawText.match(/(\d+)\s+deposits?\s*\/\s*credits?\s+\$?([\d,.\s]+\.\d{2})/i);
  if (nDepositsMatch) {
    let amtStr = nDepositsMatch[2].trim();
    const dots = (amtStr.match(/\./g) || []).length;
    if (dots > 1) {
      const lastDot = amtStr.lastIndexOf('.');
      amtStr = amtStr.slice(0, lastDot).replace(/\./g, '') + amtStr.slice(lastDot);
    }
    const amt = parseFloat(amtStr.replace(/[,\s]/g, ""));
    if (amt > 100 && amt < 100_000_000) {
      // console.log(`[DepositExtract] N Deposits/Credits format: ${nDepositsMatch[1]} deposits, $${amt.toFixed(2)}`);
      return amt;
    }
  }

  const multiCategoryAmt = extractMultiCategoryDeposits(rawText);
  const directMultiCat = extractMultiCategoryDirect(rawText);
  const bestMultiCat = Math.max(multiCategoryAmt, directMultiCat);
  if (bestMultiCat > 0) {
    // console.log(`[DepositExtract] Multi-category total wins early: $${bestMultiCat.toFixed(2)} (header-based=$${multiCategoryAmt.toFixed(2)}, direct=$${directMultiCat.toFixed(2)})`);
    return bestMultiCat;
  }

  const columnLayoutAmt = extractColumnLayoutDepositFromText(rawText);
  const creditThisPeriodAmt = extractCreditThisPeriod(rawText);
  const accountSummaryAmt = extractAccountSummaryCredits(rawText);
  const chaseSummaryAmt = extractChaseAccountSummary(rawText);
  const creditUnionAmt = extractCreditUnionSummaryDeposits(rawText);

  const patterns = [
    /deposit\s+amount\s*[=:–\-\*►→]+\s*\$?\s*([\d,]+\.?\d*)/gi,
    /(\d+)\s+deposits?\s+and\s+other\s+credits?\s+\$?([\d,]+\.?\d*)/gi,
    /(\d+)\s+credit\(?s?\)?\s+this\s+period\s+\$?([\d,]+\.?\d*)/gi,
    /deposits?\s+and\s+(?:other\s+)?(?:additions?|credits?)\s+(\d+)\s+\$?([\d,]+\.\d{2})/gi,
    /deposits?\s+and\s+(?:other\s+)?(?:additions?|credits?)\s+\$?([\d,]+\.\d{2})/gi,
    /deposits?\s*\/\s*credits?\s+\$?([\d,]+\.\d{2})/gi,
    /total\s+deposits?\s+(?:and\s+(?:other\s+)?(?:credits?|additions?)?\s*)?\$?([\d,]+\.?\d*)/gi,
    /total\s+deposits?\s+and\s+additions\s+\$?([\d,]+\.?\d*)/gi,
    /total\s+additions?\s+\$?([\d,]+\.?\d*)/gi,
    /total\s+(?:other\s+)?deposits?\s+(?:&|and)\s+(?:other\s+)?credits?\s+\$?([\d,]+\.?\d*)/gi,
    /(?:^|\n)\s*Deposits\s+\$?([\d,]+\.\d{2})/gi,
    /(?:^|\n)\s*Deposits\s+\d+\s+\$?([\d,]+\.\d{2})/gi,
    /credits?\s*\(\s*\+\s*\)\s*\$?\s*([\d,]+\.\d{2})/gi,
    /total\s+credits?\s+\$?\s*([\d,]+\.\d{2})/gi,
    /regular\s+deposits?\s+\$?\s*([\d,]+\.\d{2})/gi,
  ];

  let priorityAmt = 0;
  for (const pat of patterns) {
    const regex = new RegExp(pat.source, pat.flags);
    let m;
    while ((m = regex.exec(rawText)) !== null) {
      const amtStr = m[m.length - 1];
      const amt = parseFloat(amtStr.replace(/,/g, ""));
      if (amt > 100 && amt < 100_000_000 && amt > priorityAmt) priorityAmt = amt;
    }
    if (priorityAmt > 0) break;
  }

  const bestSpecial = Math.max(columnLayoutAmt, creditThisPeriodAmt, accountSummaryAmt, chaseSummaryAmt, multiCategoryAmt, creditUnionAmt);
  if (bestSpecial > 0 && bestSpecial > priorityAmt) {
    // console.log(`[DepositExtract] Special extraction $${bestSpecial.toFixed(2)} wins over inline $${priorityAmt.toFixed(2)}`);
    return bestSpecial;
  }
  if (priorityAmt > 0) return priorityAmt;
  if (bestSpecial > 0) return bestSpecial;

  const depositIdx = rawText.search(/deposits?\s+and\s+(?:other\s+)?(?:additions?|credits?)/i);
  if (depositIdx >= 0) {
    const nearby = rawText.slice(depositIdx, depositIdx + 400);
    const isTransactionList = /\bDate\b[\s\n]+\bDescription\b/i.test(nearby)
      || /\n\s*\d{1,2}\/\d{1,2}\s+[A-Z]/m.test(nearby);
    if (!isTransactionList) {
      let begBalToExclude = 0;
      for (const headerPattern of SUMMARY_SECTION_HEADERS) {
        const csMatch = rawText.match(headerPattern);
        if (csMatch) {
          const parsed = parseCheckingSummaryTable(rawText.slice(csMatch.index!, csMatch.index! + 1500));
          if (parsed) { begBalToExclude = parsed.beginningBalance; break; }
        }
      }
      if (begBalToExclude === 0) {
        const begBalMatch = rawText.match(/(?:beginning|opening|starting|previous)\s+balance[^\n]*?\$?([\d,]+\.\d{2})/i);
        if (begBalMatch) begBalToExclude = parseFloat(begBalMatch[1].replace(/,/g, ""));
      }

      // console.log(`[DepositExtract] Found "Deposits and..." summary at pos ${depositIdx}, nearby text: "${nearby.replace(/\n/g, "\\n").slice(0, 250)}"`);

      const totalLineMatch = nearby.match(/\bTotal\s+(\d+)\s+\$?([\d,]+\.\d{2})/);
      if (totalLineMatch) {
        const totalAmt = parseFloat(totalLineMatch[2].replace(/,/g, ""));
        if (totalAmt > 100 && totalAmt < 100_000_000) {
          // console.log(`[DepositExtract] PNC-style Total line found: ${totalLineMatch[1]} items, $${totalAmt.toFixed(2)}`);
          return totalAmt;
        }
      }

      const amountMatches = nearby.match(/\$?([\d,]+\.\d{2})/g);
      if (amountMatches) {
        for (const amtStr of amountMatches) {
          const amt = parseFloat(amtStr.replace(/[$,]/g, ""));
          if (amt > 100 && amt < 100_000_000) {
            if (begBalToExclude > 0 && Math.abs(amt - begBalToExclude) < 0.01) {
              // console.log(`[DepositExtract] Proximity: skipping $${amt.toFixed(2)} (matches Beginning Balance)`);
              continue;
            }
            // console.log(`[DepositExtract] Proximity match found: $${amt.toFixed(2)}`);
            return amt;
          }
        }
      }
    } else {
      // console.log(`[DepositExtract] Skipping "Deposits and..." at pos ${depositIdx} — detected transaction listing, not summary total`);
    }
  } else {
    const depositIdx2 = rawText.search(/total\s+deposits\b/i);
    if (depositIdx2 >= 0) {
      const nearby = rawText.slice(depositIdx2, depositIdx2 + 200);
      const m = nearby.match(/\$?([\d,]+\.\d{2})/);
      if (m) {
        const amt = parseFloat(m[1].replace(/,/g, ""));
        if (amt > 100 && amt < 100_000_000) {
          // console.log(`[DepositExtract] Found via "Total Deposits": $${amt.toFixed(2)}`);
          return amt;
        }
      }
    }
  }

  return 0;
}

function findRecurringDebits(debits: ParsedTransaction[]): { description: string; amount: number; count: number; dates: string[]; isLikelyLoan: boolean }[] {
  const lenderKeywords = /\b(capital|advance|funding|finance|lending|finserv|ondeck|kabbage|bluevine|fundbox|credibly|libertas|yellowstone|pearl|fora|kalamata|capytal|capitalize|fox|mantis|everest|cfg|byline|mulligan|reliant|clearview|itria|cloudfund|navitas|ascentium|tvt|greenbox|biz2credit|lendio|fundation|breakout|headway|behalf|payability|newtek|smartbiz|vox|wynwood|platinum\s*rapid|qfs|jmb|unique\s*funding|samson|kings|fleetcor|stage\s*adv|7even|cashable|vitalcap|vital\s*cap|vcg)\b/i;
  const notLoanKeywords = /\b(insurance|utility|electric|gas\s+co|water|phone|internet|rent|payroll|gusto|adp|paychex|tax|irs|stripe|clover|visa|mastercard|amex|discover|equipment\s*financ|truck\s*financ)\b/i;

  const groups = new Map<string, { amount: number; dates: string[]; descriptions: string[] }>();

  for (const d of debits) {
    const key = d.amount.toFixed(2);
    if (!groups.has(key)) groups.set(key, { amount: d.amount, dates: [], descriptions: [] });
    const g = groups.get(key)!;
    g.dates.push(d.date);
    g.descriptions.push(d.description);
  }

  const recurring: { description: string; amount: number; count: number; dates: string[]; isLikelyLoan: boolean }[] = [];
  for (const [, g] of groups) {
    if (g.dates.length >= 3) {
      const desc = g.descriptions[0] || "Unknown";
      const allDescs = g.descriptions.join(" ");
      const isLikelyLoan = lenderKeywords.test(allDescs) && !notLoanKeywords.test(allDescs);
      recurring.push({ description: desc, amount: g.amount, count: g.dates.length, dates: g.dates, isLikelyLoan });
    }
  }

  return recurring.sort((a, b) => b.count - a.count);
}
