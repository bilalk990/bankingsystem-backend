import { identifyBank, BankTemplate } from "./bankTemplates";
import type { ParsedTransaction } from "./documentAiService";

export interface ExtractedTransaction {
  date: string;
  description: string;
  amount: number;
  type: "debit" | "credit";
  balance?: number;
  rawLine: string;
}

export interface RecurringPayment {
  lender: string;
  shortName: string;
  amount: number;
  frequency: "daily" | "weekly" | "monthly";
  occurrences: number;
  dates: string[];
  confidence: "high" | "medium";
  accountsFoundIn?: string[];
  fundedAmount?: number;
  fundedDate?: string;
}

export interface FundingDeposit {
  lender: string;
  shortName: string;
  amount: number;
  date: string;
}

export interface TransactionEngineResult {
  bankName: string | null;
  templateUsed: boolean;
  transactions: ExtractedTransaction[];
  credits: ExtractedTransaction[];
  debits: ExtractedTransaction[];
  totalCredits: number;
  totalDebits: number;
  recurringPayments: RecurringPayment[];
  fundingDeposits: FundingDeposit[];
  accountNumber?: string;
  statementMonth?: string;
}

const LENDER_PATTERNS: { pattern: RegExp; shortName: string }[] = [
  { pattern: /ondeck|on\s*deck|odk\s*capital/i, shortName: "ondeck" },
  { pattern: /forward\s*fin|forwardfinusa/i, shortName: "forward" },
  { pattern: /fundbox/i, shortName: "fundbox" },
  { pattern: /bluevine/i, shortName: "bluevine" },
  { pattern: /kabbage/i, shortName: "kabbage" },
  { pattern: /kapitus|kap\s*servic|servicing\s*by\s*kap/i, shortName: "kapitus" },
  { pattern: /gmfunding|gm\s*funding/i, shortName: "gmfunding" },
  { pattern: /sq\s*advance|square\s*capital|sq\s*capital|sq\s*loan/i, shortName: "square" },
  { pattern: /\bw\s+funding\b/i, shortName: "wfunding" },
  { pattern: /can\s*capital/i, shortName: "cancapital" },
  { pattern: /rapid\s*financ/i, shortName: "rapid" },
  { pattern: /credibly/i, shortName: "credibly" },
  { pattern: /libertas/i, shortName: "libertas" },
  { pattern: /yellowstone/i, shortName: "yellowstone" },
  { pattern: /pearl\s*capital/i, shortName: "pearl" },
  { pattern: /fora\s*financial/i, shortName: "fora" },
  { pattern: /kalamata/i, shortName: "kalamata" },
  { pattern: /national\s*funding/i, shortName: "national" },
  { pattern: /fox\s*fund/i, shortName: "fox" },
  { pattern: /mantis/i, shortName: "mantis" },
  { pattern: /everest\s*business/i, shortName: "everest" },
  { pattern: /cfg\s*merchant|cfgms/i, shortName: "cfg" },
  { pattern: /mulligan/i, shortName: "mulligan" },
  { pattern: /clearview/i, shortName: "clearview" },
  { pattern: /itria/i, shortName: "itria" },
  { pattern: /cloudfund/i, shortName: "cloudfund" },
  { pattern: /navitas/i, shortName: "navitas" },
  { pattern: /vox\s*fund/i, shortName: "vox" },
  { pattern: /wynwood/i, shortName: "wynwood" },
  { pattern: /platinum\s*rapid/i, shortName: "platinum" },
  { pattern: /qfs/i, shortName: "qfs" },
  { pattern: /jmb\s*capital/i, shortName: "jmb" },
  { pattern: /unique\s*fund/i, shortName: "unique" },
  { pattern: /samson/i, shortName: "samson" },
  { pattern: /kings\s*capital/i, shortName: "kings" },
  { pattern: /stage\s*adv|stage\s*fund/i, shortName: "stage" },
  { pattern: /7even/i, shortName: "7even" },
  { pattern: /cashable/i, shortName: "cashable" },
  { pattern: /vitalcap|vital\s*capital/i, shortName: "vitalcap" },
  { pattern: /vcg\s*(?:capital|fund)/i, shortName: "vcg" },
  { pattern: /zen\s*fund/i, shortName: "zen" },
  { pattern: /ace\s*fund/i, shortName: "ace" },
  { pattern: /aspire\s*fund/i, shortName: "aspire" },
  { pattern: /breeze\s*advance/i, shortName: "breeze" },
  { pattern: /canfield/i, shortName: "canfield" },
  { pattern: /clara\s*capital/i, shortName: "clara" },
  { pattern: /compass\s*fund/i, shortName: "compass" },
  { pattern: /daytona/i, shortName: "daytona" },
  { pattern: /diamond\s*advance/i, shortName: "diamond" },
  { pattern: /elevate\s*fund/i, shortName: "elevate" },
  { pattern: /epic\s*advance/i, shortName: "epic" },
  { pattern: /expansion\s*capital/i, shortName: "expansion" },
  { pattern: /family\s*fund/i, shortName: "family" },
  { pattern: /fenix\s*capital/i, shortName: "fenix" },
  { pattern: /figure\s*lending/i, shortName: "figure" },
  { pattern: /fresh\s*fund/i, shortName: "fresh" },
  { pattern: /funding\s*metrics/i, shortName: "metrics" },
  { pattern: /giggle\s*financ/i, shortName: "giggle" },
  { pattern: /gotorro/i, shortName: "gotorro" },
  { pattern: /highland/i, shortName: "highland" },
  { pattern: /hightower/i, shortName: "hightower" },
  { pattern: /honor\s*capital/i, shortName: "honor" },
  { pattern: /idea\s*247|idea\s*financial/i, shortName: "idea" },
  { pattern: /ifund/i, shortName: "ifund" },
  { pattern: /immediate\s*(?:advance|capital)/i, shortName: "immediate" },
  { pattern: /iou\s*central/i, shortName: "iou" },
  { pattern: /lcf/i, shortName: "lcf" },
  { pattern: /legend\s*advance/i, shortName: "legend" },
  { pattern: /lendbuzz/i, shortName: "lendbuzz" },
  { pattern: /lendistry/i, shortName: "lendistry" },
  { pattern: /lg\s*funding/i, shortName: "lg" },
  { pattern: /liberty\s*fund/i, shortName: "liberty" },
  { pattern: /litefund/i, shortName: "litefund" },
  { pattern: /millstone/i, shortName: "millstone" },
  { pattern: /mr\s*advance/i, shortName: "mradvance" },
  { pattern: /newport\s*business/i, shortName: "newport" },
  { pattern: /nitro\s*advance/i, shortName: "nitro" },
  { pattern: /oak\s*capital/i, shortName: "oak" },
  { pattern: /ocean\s*advance/i, shortName: "ocean" },
  { pattern: /olympus\s*(?:business|capital)/i, shortName: "olympus" },
  { pattern: /one\s*river/i, shortName: "oneriver" },
  { pattern: /orange\s*advance/i, shortName: "orange" },
  { pattern: /overton/i, shortName: "overton" },
  { pattern: /parkside/i, shortName: "parkside" },
  { pattern: /path\s*2\s*capital/i, shortName: "path2" },
  { pattern: /power\s*fund/i, shortName: "power" },
  { pattern: /premium\s*merchant/i, shortName: "premium" },
  { pattern: /prosperum/i, shortName: "prosperum" },
  { pattern: /prosperity\s*fund/i, shortName: "prosperity" },
  { pattern: /\bram\s*(?:payment|capital)/i, shortName: "ram" },
  { pattern: /readycap/i, shortName: "readycap" },
  { pattern: /reboost/i, shortName: "reboost" },
  { pattern: /redwood\s*business/i, shortName: "redwood" },
  { pattern: /reliance\s*financial/i, shortName: "reliance" },
  { pattern: /retro\s*advance/i, shortName: "retro" },
  { pattern: /revenued/i, shortName: "revenued" },
  { pattern: /rocket\s*capital/i, shortName: "rocket" },
  { pattern: /specialty\s*capital/i, shortName: "specialty" },
  { pattern: /stellar\s*advance/i, shortName: "stellar" },
  { pattern: /suncoast/i, shortName: "suncoast" },
  { pattern: /swift\s*fund/i, shortName: "swift" },
  { pattern: /tbf\s*group/i, shortName: "tbf" },
  { pattern: /the\s*fundworks|thefundworks/i, shortName: "fundworks" },
  { pattern: /triton/i, shortName: "triton" },
  { pattern: /trupath/i, shortName: "trupath" },
  { pattern: /ufce/i, shortName: "ufce" },
  { pattern: /ufs\b/i, shortName: "ufs" },
  { pattern: /upfunding/i, shortName: "upfunding" },
  { pattern: /vader/i, shortName: "vader" },
  { pattern: /wave\s*advance/i, shortName: "wave" },
  { pattern: /webfunder/i, shortName: "webfunder" },
  { pattern: /westwood/i, shortName: "westwood" },
  { pattern: /wide\s*merchant/i, shortName: "wide" },
  { pattern: /pipe\s*capital/i, shortName: "pipe" },
  { pattern: /ssmb/i, shortName: "ssmb" },
  { pattern: /coast\s*fund/i, shortName: "coast" },
  { pattern: /fintegra/i, shortName: "fintegra" },
  { pattern: /altfunding|alt\s*funding/i, shortName: "alt" },
  { pattern: /funding\s*futures/i, shortName: "futures" },
  { pattern: /mako\s*fund/i, shortName: "mako" },
  { pattern: /main\s*street\s*group/i, shortName: "mainstreet" },
  { pattern: /integra\s*fund/i, shortName: "integra" },
  { pattern: /reliant/i, shortName: "reliant" },
  { pattern: /headway/i, shortName: "headway" },
  { pattern: /behalf/i, shortName: "behalf" },
  { pattern: /breakout/i, shortName: "breakout" },
  { pattern: /greenbox/i, shortName: "greenbox" },
  { pattern: /world\s*business/i, shortName: "world" },
  { pattern: /tvt\s*capital/i, shortName: "tvt" },
  { pattern: /united\s*capital/i, shortName: "united" },
  { pattern: /bretton/i, shortName: "bretton" },
  { pattern: /fleetcor/i, shortName: "fleetcor" },
];

const NOT_LOAN_DESCRIPTIONS = /\b(insurance|aflac|aflak|payroll|adp\b|gusto\b|paychex|irs\b|rent\b|lease\b|utility|electric|gas\s+co|water\b|phone\b|internet|stripe\b|clover\b|visa\b|mastercard|amex\b|discover\b|zelle\b|venmo\b|cash\s*app|apple\s+(?:pay|cash)|google\s+pay|samsung\s+pay|atm\s*withdraw|booster\s+fuel|child\s+support|texas\s+sdu|equipment\s*financ|truck\s*financ|truck\s*leas|dakota\s*fin|chase\s*card|payment\s+to\s+chase|credit\s*card|card\s+ending\s+in|american\s+express|amex\s+card|fortiva|credit\s+one|progressive|prog\s+direct|t-?mobile|spectrum|duke\s*energy|verizon|carolina\s+health|pos\s+payment|ib\s+transfer|internet\s+xfr|kwik\s+trip|home\s+depot|chick-?fil-?a|wawa\b|fleet\s+farm|meijer|chico'?s|walmart|target\b|costco|sam'?s\s+club|walgreens|cvs\b|dunkin|starbucks|bill\s*pay|online\s*(?:pay|banking|bill)|online\s*transfer\s*to|bnk\s*of\s*amer|bank\s*of\s*america|bkofamerica|robinhood|coinbase|webull|e\s*trade|etrade|schwab|fidelity|td\s*ameritrade|td\s*auto\s*finance|ally\s*invest|acorns|sofi\s*invest|carmax\s*auto|allstate|geico|state\s*farm|fleetsmarts|clicklease|lease\s*services|self\s+financial|intuit\s+financ|isuzu\s+financ|styku|next\s+insur|pmnt\s+sent|payment\s+to\b|ascentium\s*capital|leasechg|lease\s+pymt|loan\s+pymt|orig\s+co\s+name.*(?:visa|bk\s+of\s+amer|ins\b)|autopay\s+to|transfer\s+to\s+(?:sav|chk|checking|savings))\b/i;

const ADDRESS_PATTERN = /\b[A-Z]{2}\s+#\d{3,}/;

function matchLender(description: string): { shortName: string; fullMatch: string } | null {
  for (const lp of LENDER_PATTERNS) {
    const m = description.match(lp.pattern);
    if (m) return { shortName: lp.shortName, fullMatch: m[0] };
  }
  return null;
}

export function normalizeLenderName(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "")
    .replace(/(?:llc|inc|corp|ltd|co|funding|capital|financ(?:e|ial|ing)?|advance|servic(?:e|es|ing)|payment(?:s)?|group)$/g, "")
    .replace(/(?:llc|inc|corp|ltd|co)$/g, "")
    .trim();
}

export function areSameLender(name1: string, name2: string): boolean {
  const n1 = normalizeLenderName(name1);
  const n2 = normalizeLenderName(name2);
  if (n1 === n2) return true;
  if (n1.length >= 3 && n2.length >= 3) {
    if (n1.includes(n2) || n2.includes(n1)) return true;
  }
  const m1 = matchLender(name1);
  const m2 = matchLender(name2);
  if (m1 && m2 && m1.shortName === m2.shortName) return true;
  return false;
}

function computeFrequencyFromDates(dates: string[]): "daily" | "weekly" | "monthly" {
  if (dates.length < 2) return "weekly";

  const parsed: number[] = [];
  for (const d of dates) {
    const parts = d.split(/[\/-]/);
    let ts: number;
    if (parts.length >= 3 && parts[0].length === 4) {
      ts = new Date(parseInt(parts[0]), parseInt(parts[1]) - 1, parseInt(parts[2])).getTime();
    } else if (parts.length >= 2) {
      const month = parseInt(parts[0]);
      const day = parseInt(parts[1]);
      const year = parts[2] ? (parts[2].length === 2 ? 2000 + parseInt(parts[2]) : parseInt(parts[2])) : new Date().getFullYear();
      ts = new Date(year, month - 1, day).getTime();
    } else {
      continue;
    }
    if (!isNaN(ts)) parsed.push(ts);
  }

  if (parsed.length < 2) return "weekly";

  parsed.sort((a, b) => a - b);
  const gaps: number[] = [];
  for (let i = 1; i < parsed.length; i++) {
    const daysBetween = (parsed[i] - parsed[i - 1]) / (1000 * 60 * 60 * 24);
    if (daysBetween > 0 && daysBetween < 60) gaps.push(daysBetween);
  }

  if (gaps.length === 0) return "weekly";

  const sortedGaps = [...gaps].sort((a, b) => a - b);
  const medianGap = sortedGaps[Math.floor(sortedGaps.length / 2)];

  const dailyGaps = gaps.filter(g => g >= 0.5 && g <= 3);
  const weeklyGaps = gaps.filter(g => g >= 5 && g <= 10);
  const monthlyGaps = gaps.filter(g => g >= 25 && g <= 35);

  if (medianGap <= 2 && dailyGaps.length >= 5 && dailyGaps.length >= gaps.length * 0.6) return "daily";

  if (medianGap >= 5 && medianGap <= 10 && weeklyGaps.length >= gaps.length * 0.5) return "weekly";

  if (medianGap >= 25 && medianGap <= 35 && monthlyGaps.length >= gaps.length * 0.5) return "monthly";

  if (medianGap <= 3 && dailyGaps.length >= 3 && dailyGaps.length >= gaps.length * 0.5) return "daily";
  if (medianGap <= 10) return "weekly";
  if (medianGap >= 20) return "monthly";
  return "weekly";
}

export function extractFromParsedTransactions(
  parsedTxns: ParsedTransaction[],
  bankName: string | null,
  rawText: string
): TransactionEngineResult {
  const transactions: ExtractedTransaction[] = parsedTxns.map(t => ({
    date: t.date,
    description: t.description,
    amount: t.amount,
    type: t.type,
    balance: t.balance,
    rawLine: `${t.date} ${t.description} ${t.amount}`,
  }));

  const credits = transactions.filter(t => t.type === "credit");
  const debits = transactions.filter(t => t.type === "debit");
  const totalCredits = credits.reduce((s, t) => s + t.amount, 0);
  const totalDebits = debits.reduce((s, t) => s + t.amount, 0);

  const rawTextLenderDebits = scanRawTextForLenderDebits(rawText, debits);
  const allDebits = [...debits, ...rawTextLenderDebits];

  const recurringPayments = findRecurringLenderPayments(allDebits);
  const fundingDeposits = findFundingDeposits(credits);

  console.log(`[TxnEngine-Parsed] ${bankName || "Unknown"}: ${transactions.length} txns (${credits.length} credits=$${Math.round(totalCredits).toLocaleString()}, ${allDebits.length} debits=$${Math.round(totalDebits).toLocaleString()}), ${recurringPayments.length} recurring, ${fundingDeposits.length} funding`);

  return {
    bankName,
    templateUsed: true,
    transactions,
    credits,
    debits,
    totalCredits,
    totalDebits,
    recurringPayments,
    fundingDeposits,
    accountNumber: parseAccountNumber(rawText),
  };
}

function normalizeTransactionLine(line: string): string {
  const dateNoSpace = line.match(/^(\d{1,2}\/\d{1,2}(?:\/\d{2,4})?)([A-Za-z])/);
  if (dateNoSpace) {
    line = dateNoSpace[1] + " " + line.slice(dateNoSpace[1].length);
  }
  if (/^\d{1,2}\/\d{1,2}(?:\/\d{2,4})?\s/.test(line)) {
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
  return line;
}

const isAddressLine = (line: string): boolean =>
  /\b[A-Z]{2}\s+#\d{3,}/.test(line) ||
  /^\d+\s+\w+\s+(?:ST|STREET|AVE|AVENUE|DR|DRIVE|RD|ROAD|LN|LANE|BLVD|HWY|CT|PL|WAY|CIR|PKWY)\b/i.test(line) ||
  /\b\w+\s+[A-Z]{2}\s+\d{5}\b/.test(line);

export function extractAllTransactions(rawText: string): TransactionEngineResult {
  const template = identifyBank(rawText);
  const bankName = template?.name || null;
  const transactions: ExtractedTransaction[] = [];

  const lines = rawText.split("\n");
  let currentSection: "deposits" | "withdrawals" | "unknown" = "unknown";

  const genericDepositHeader = /^(?:DEPOSITS?\s+AND\s+(?:OTHER\s+)?(?:CREDITS?|ADDITIONS?)|CREDITS?\s+AND\s+DEPOSITS?|DEPOSITS?(?:\s+DETAIL)?)\s*$/i;
  const genericWithdrawalHeader = /^(?:WITHDRAWALS?\s+AND\s+(?:OTHER\s+)?(?:DEBITS?|SUBTRACTIONS?)|CHECKS?\s+(?:AND\s+OTHER\s+)?(?:DEBITS?|WITHDRAWALS?)|ELECTRONIC\s+(?:WITHDRAWALS?|PAYMENTS?)|CHECKS?\s+PAID|OTHER\s+(?:WITHDRAWALS?|DEBITS?|DEDUCTIONS?|CHARGES?)|ACH\s+(?:DEBITS?|PAYMENTS?)|DAILY\s+CARD\s+WITHDRAWALS?|ATM\s+(?:WITHDRAWALS?|TRANSACTIONS?))\s*$/i;

  const creditKeywords = /\b(deposit|credit|transfer\s+in|transfer\s+from|incoming|payroll|direct\s+dep|mobile\s+deposit|cash\s+deposit|wire\s+in|ach\s+credit|refund|interest\s+paid)\b/i;
  const debitKeywords = /\b(withdrawal|debit|check\s+#|chk\s+#|payment|purchase|transfer\s+out|transfer\s+to|wire\s+out|ach\s+debit|pos\s+debit|atm\s+withdrawal|fee|charge)\b/i;

  const txnPatterns = template?.transactionLinePatterns || [
    /^(\d{1,2}\/\d{1,2}(?:\/\d{2,4})?)\s+(.+?)\s+(-?\$?[\d,]+\.\d{2})\s*$/,
    /^(\d{1,2}\/\d{1,2}(?:\/\d{2,4})?)\s+(.+?)\s+(-?\$?[\d,]+\.\d{2})\s+(-?\$?[\d,]+\.\d{2})\s*$/,
    /^(\d{1,2}\/\d{1,2}(?:\/\d{2,4})?)\s+(.+?)\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s*$/,
    /^(\d{1,2}-\d{1,2}(?:-\d{2,4})?)\s+(.+?)\s+([\d,]+\.\d{2})-?\s+([\d,]+\.\d{2})\s*$/,
    /^(\d{1,2}-\d{1,2}(?:-\d{2,4})?)\s+(.+?)\s+(-?\$?[\d,]+\.\d{2})\s*$/,
    /^(\w{3}\s+\d{1,2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s+(\d[\d,]*\.\d{2})\s*$/,
  ];

  const skipLine = /^(?:Date\s+Description|Date\s+Ref|SUBTOTAL|TOTAL|BALANCE\s+FORWARD|continued\s+on|page\s+\d|beginning\s+balance|ending\s+balance|previous\s+balance|closing\s+balance|daily\s+(?:ending\s+)?balance|DAILY\s+BALANCE\s+DETAIL|\s*$)/i;
  const tableDataStartLine = /^--- TABLE DATA ---$/i;
  const structuredDataLine = /^\[STRUCTURED_PARSED_DATA\]/i;
  const dailyBalanceSection = /DAILY\s+(?:ENDING\s+)?BALANCE/i;
  let inDailyBalance = false;
  let inTableData = false;

  const startsWithDate = /^\d{1,2}[\/\-]\d{1,2}/;

  const sectionHeaderPattern = /(?:TRANSACTION\s+DETAIL|DEPOSITS?\s+AND\s+(?:OTHER\s+)?(?:CREDITS?|ADDITIONS?)|CHECKS?\s+(?:AND\s+OTHER\s+)?(?:DEBITS?|WITHDRAWALS?)|ELECTRONIC\s+(?:WITHDRAWALS?|PAYMENTS?)|WITHDRAWALS?\s+AND\s+(?:OTHER\s+)?(?:DEBITS?|SUBTRACTIONS?)|CHECKS?\s+PAID|OTHER\s+(?:WITHDRAWALS?|DEBITS?|CHARGES?)|ACH\s+TRANSACTIONS?)/i;
  const summaryLinePattern = /^Total\s+(?:Deposits|Withdrawals|ATM|Card|Electronic)/i;

  for (let i = 0; i < lines.length; i++) {
    let line = normalizeTransactionLine(lines[i].trim());
    if (!line) continue;
    if (structuredDataLine.test(line)) break;
    if (tableDataStartLine.test(line)) { inTableData = true; continue; }
    if (inTableData) {
      if (/^\*start\*/.test(line)) { inTableData = false; }
      else { continue; }
    }
    if (/^\*start\*/.test(line) || /^\*end\*/.test(line)) continue;

    const isDateLine = startsWithDate.test(line);

    if (!isDateLine && dailyBalanceSection.test(line)) {
      inDailyBalance = true;
      continue;
    }

    if (!isDateLine) {
      if (template) {
        if (template.sectionHeaders.deposits.test(line)) { currentSection = "deposits"; inDailyBalance = false; continue; }
        if (template.sectionHeaders.withdrawals.test(line)) { currentSection = "withdrawals"; inDailyBalance = false; continue; }
        if (template.sectionHeaders.checks?.test(line)) { currentSection = "withdrawals"; inDailyBalance = false; continue; }
      } else {
        if (genericDepositHeader.test(line)) { currentSection = "deposits"; inDailyBalance = false; continue; }
        if (genericWithdrawalHeader.test(line)) { currentSection = "withdrawals"; inDailyBalance = false; continue; }
      }
    }

    if (inDailyBalance) continue;
    if (skipLine.test(line)) continue;

    let txnMatched = false;
    for (const pattern of txnPatterns) {
      const match = line.match(pattern);
      if (!match) continue;

      const dateStr = match[1];
      let description = match[2].trim();
      if (description.length < 2) break;
      if (/^(?:page|continued|subtotal|total|balance|daily\s+(?:ending\s+)?balance)/i.test(description)) break;

      let amount: number;
      let balance: number | undefined;

      if (match[4]) {
        amount = Math.abs(parseFloat(match[3].replace(/[$,]/g, "")));
        balance = parseFloat(match[4].replace(/[$,]/g, ""));
      } else {
        amount = Math.abs(parseFloat(match[3].replace(/[$,]/g, "")));
      }

      if (isNaN(amount) || amount === 0 || amount > 100_000_000) break;

      if (i + 1 < lines.length) {
        const nextLine = lines[i + 1].trim();
        const nextStartsWithDate = /^\d{1,2}[\/\-]\d{1,2}/.test(nextLine) || /^(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\b/i.test(nextLine);
        if (nextLine && !nextStartsWithDate && !isAddressLine(nextLine) && !sectionHeaderPattern.test(nextLine) && !skipLine.test(nextLine) && !summaryLinePattern.test(nextLine) && nextLine.length >= 3 && nextLine.length <= 80 && !(/^\$?[\d,]+\.\d{2}\s*$/.test(nextLine))) {
          description += " " + nextLine;
          i++;
        }
      }

      let type: "debit" | "credit";
      if (currentSection === "deposits") {
        type = "credit";
      } else if (currentSection === "withdrawals") {
        type = "debit";
      } else {
        const rawAmt = match[3];
        if (rawAmt.startsWith("-") || rawAmt.includes("(")) {
          type = "debit";
        } else if (balance !== undefined) {
          const prevTxn = transactions.length > 0 ? transactions[transactions.length - 1] : null;
          const prevBalance = prevTxn?.balance;
          if (prevBalance !== undefined) {
            const balDelta = balance - prevBalance;
            if (Math.abs(balDelta - amount) < 0.02) {
              type = "credit";
            } else if (Math.abs(balDelta + amount) < 0.02) {
              type = "debit";
            } else if (creditKeywords.test(description)) {
              type = "credit";
            } else if (debitKeywords.test(description)) {
              type = "debit";
            } else {
              type = "debit";
            }
          } else if (creditKeywords.test(description)) {
            type = "credit";
          } else if (debitKeywords.test(description)) {
            type = "debit";
          } else {
            type = "debit";
          }
        } else if (creditKeywords.test(description)) {
          type = "credit";
        } else if (debitKeywords.test(description)) {
          type = "debit";
        } else {
          type = "debit";
        }
      }

      transactions.push({ date: dateStr, description, amount, type, balance, rawLine: line });
      txnMatched = true;
      break;
    }

    if (!txnMatched && line.includes("|")) {
      const MONTH_NAMES: Record<string, string> = {
        jan: "01", feb: "02", mar: "03", apr: "04", may: "05", jun: "06",
        jul: "07", aug: "08", sep: "09", oct: "10", nov: "11", dec: "12",
      };
      const mdMatch = line.match(
        /^\|?\s*(?:(\d{1,2}\/\d{1,2}(?:\/\d{2,4})?)|((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}))\s*\|\s*(.+?)\s*\|\s*((?:-?\$?[\d,]+\.\d{2})?)\s*\|\s*((?:\$?[\d,]+\.\d{2})?)\s*\|\s*((?:-?\$?[\d,]+\.\d{2})?)\s*\|?\s*$/i
      );
      if (mdMatch) {
        let dateStr = mdMatch[1] || "";
        if (!dateStr && mdMatch[2]) {
          const dm = mdMatch[2].match(/^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})$/i);
          if (dm) dateStr = `${MONTH_NAMES[dm[1].toLowerCase().slice(0, 3)]}/${dm[2].padStart(2, "0")}`;
        }
        if (dateStr) {
          const description = (mdMatch[3] || "").replace(/<br\/?>/gi, " ").trim();
          if (description.length >= 2 && !/^(?:page|continued|subtotal|total|balance|---)/i.test(description)) {
            const withdrawalStr = (mdMatch[4] || "").trim();
            const depositStr = (mdMatch[5] || "").trim();
            const balanceStr = (mdMatch[6] || "").trim();
            let amount = 0;
            let type: "debit" | "credit" = "debit";
            if (depositStr) {
              const dAmt = Math.abs(parseFloat(depositStr.replace(/[$,]/g, "")));
              if (dAmt > 0) { amount = dAmt; type = "credit"; }
            }
            if (amount === 0 && withdrawalStr) {
              const wAmt = Math.abs(parseFloat(withdrawalStr.replace(/[$,]/g, "")));
              if (wAmt > 0) { amount = wAmt; type = "debit"; }
            }
            if (amount > 0 && amount < 100_000_000) {
              const balance = balanceStr ? parseFloat(balanceStr.replace(/[$,]/g, "")) : undefined;
              transactions.push({ date: dateStr, description, amount, type, balance: balance && !isNaN(balance) ? balance : undefined, rawLine: line });
            }
          }
        }
      }
    }
  }

  const credits = transactions.filter(t => t.type === "credit");
  const debits = transactions.filter(t => t.type === "debit");
  const totalCredits = credits.reduce((s, t) => s + t.amount, 0);
  const totalDebits = debits.reduce((s, t) => s + t.amount, 0);

  const rawTextLenderDebits = scanRawTextForLenderDebits(rawText, debits);
  const allDebits = [...debits, ...rawTextLenderDebits];

  const recurringPayments = findRecurringLenderPayments(allDebits);
  const fundingDeposits = findFundingDeposits(credits);

  if (rawTextLenderDebits.length > 0) {
    // console.log(`[TxnEngine] Raw text scan found ${rawTextLenderDebits.length} additional lender debit(s) not caught by line parser`);
  }
  // console.log(`[TxnEngine] ${bankName || "Unknown"} (template: ${!!template}): ${transactions.length} txns (${credits.length} credits=$${Math.round(totalCredits).toLocaleString()}, ${allDebits.length} debits=$${Math.round(totalDebits).toLocaleString()}), ${recurringPayments.length} recurring lender payments, ${fundingDeposits.length} funding deposits`);
  for (const rp of recurringPayments) {
    // console.log(`[TxnEngine-Recurring]   "${rp.lender}" (${rp.shortName}) $${rp.amount} ${rp.frequency} occ=${rp.occurrences} conf=${rp.confidence}`);
  }
  for (const fd of fundingDeposits) {
    // console.log(`[TxnEngine-Funding]   "${fd.lender}" (${fd.shortName}) $${fd.amount} date=${fd.date}`);
  }

  return {
    bankName,
    templateUsed: !!template,
    transactions,
    credits,
    debits,
    totalCredits,
    totalDebits,
    recurringPayments,
    fundingDeposits,
    accountNumber: parseAccountNumber(rawText),
  };
}

function scanRawTextForLenderDebits(rawText: string, existingDebits: ExtractedTransaction[]): ExtractedTransaction[] {
  const results: ExtractedTransaction[] = [];
  const foundLenderAmounts = new Set<string>();

  const lines = rawText.split("\n");
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim().replace(/^[<>]\s*/, "");
    if (!line) continue;

    const lender = matchLender(line);
    if (!lender) continue;
    if (NOT_LOAN_DESCRIPTIONS.test(line)) continue;

    const allAmts = [...line.matchAll(/\$?([\d,]+\.\d{2})/g)];
    let amountStr: string | null = null;
    if (allAmts.length > 0) {
      const lenderIdx = line.toLowerCase().indexOf(lender.fullMatch.toLowerCase());
      if (lenderIdx >= 0 && allAmts.length > 1) {
        const afterLender = allAmts.filter(m => (m.index ?? 0) > lenderIdx);
        if (afterLender.length >= 3) {
          amountStr = afterLender[afterLender.length - 2][0];
        } else if (afterLender.length === 2) {
          amountStr = afterLender[0][0];
        } else if (afterLender.length === 1) {
          amountStr = afterLender[0][0];
        } else {
          amountStr = allAmts[allAmts.length > 1 ? allAmts.length - 2 : 0][0];
        }
      } else if (allAmts.length >= 2) {
        amountStr = allAmts[allAmts.length - 2][0];
      } else {
        amountStr = allAmts[0][0];
      }
    }
    if (!amountStr) {
      if (i + 1 < lines.length) {
        const nextLine = lines[i + 1].trim();
        if (nextLine && !matchLender(nextLine)) {
          const nextAmt = nextLine.match(/\$?([\d,]+\.\d{2})/);
          if (nextAmt) { amountStr = nextAmt[0]; }
        }
      }
      if (!amountStr) continue;
    }

    const amount = parseFloat(amountStr.replace(/[$,]/g, ""));
    if (isNaN(amount) || amount < 50 || amount > 100000) continue;

    const dedupKey = `${lender.shortName}:${amount}`;
    if (foundLenderAmounts.has(dedupKey)) continue;

    const dateMatch = line.match(/(\d{1,2}\/\d{1,2}(?:\/\d{2,4})?)/);
    let date = dateMatch ? dateMatch[1] : "";

    const alreadyFound = existingDebits.some(d => {
      const dLender = matchLender(d.description);
      return dLender && dLender.shortName === lender.shortName && Math.abs(d.amount - amount) < 0.02;
    });
    if (alreadyFound) continue;

    foundLenderAmounts.add(dedupKey);
    const descClean = line.replace(/\$?[\d,]+\.\d{2}/g, "").replace(/\s+/g, " ").trim().slice(0, 80);
    // console.log(`[TxnEngine-RawScan] Found lender "${lender.shortName}" ($${amount}) in raw text: "${line.slice(0, 60)}..."`);
    results.push({
      date,
      description: descClean || lender.fullMatch,
      amount,
      type: "debit",
      rawLine: line,
    });
  }
  return results;
}

function findRecurringLenderPayments(debits: ExtractedTransaction[]): RecurringPayment[] {
  const lenderGroups = new Map<string, { lender: string; amounts: Map<number, { count: number; dates: string[] }> }>();

  for (const d of debits) {
    if (NOT_LOAN_DESCRIPTIONS.test(d.description)) continue;

    const lender = matchLender(d.description);
    if (!lender) continue;

    if (!lenderGroups.has(lender.shortName)) {
      lenderGroups.set(lender.shortName, { lender: lender.fullMatch, amounts: new Map() });
    }

    const group = lenderGroups.get(lender.shortName)!;
    const roundedAmt = Math.round(d.amount * 100) / 100;

    if (!group.amounts.has(roundedAmt)) {
      group.amounts.set(roundedAmt, { count: 0, dates: [] });
    }
    const entry = group.amounts.get(roundedAmt)!;
    entry.count++;
    entry.dates.push(d.date);
  }

  const results: RecurringPayment[] = [];

  for (const [shortName, group] of lenderGroups) {
    const sortedAmounts = [...group.amounts.entries()].sort((a, b) => b[1].count - a[1].count);
    const isGuaranteedLender = LENDER_PATTERNS.some(lp => lp.shortName === shortName);
    const minOcc = isGuaranteedLender ? 2 : 3;

    const allAmounts = sortedAmounts.map(([a]) => a);
    const totalTxns = sortedAmounts.reduce((s, [, d]) => s + d.count, 0);
    if (allAmounts.length >= 3 && totalTxns >= 3) {
      const uniqueAmounts = new Set(allAmounts.map(a => Math.round(a * 100)));
      const maxRepeating = Math.max(...sortedAmounts.map(([, d]) => d.count));
      if (uniqueAmounts.size >= totalTxns * 0.6 && maxRepeating < 3) {
        continue;
      }
    }

    const usedAmounts = new Set<number>();
    const repeatingAmounts = sortedAmounts.filter(([_, d]) => d.count >= 2);
    const multipleExactAmounts = repeatingAmounts.length >= 2;

    for (const [seedAmount, seedData] of sortedAmounts) {
      if (usedAmounts.has(seedAmount)) continue;

      let clusterOcc = 0;
      let clusterDates: string[] = [];
      let clusterBestAmount = seedAmount;
      let clusterBestCount = seedData.count;

      for (const [amount, data] of group.amounts) {
        if (usedAmounts.has(amount)) continue;
        const ratio = Math.max(amount, seedAmount) / Math.min(amount, seedAmount);
        const mergeThreshold = (multipleExactAmounts && data.count >= 2 && amount !== seedAmount) ? 1.02 : 1.20;
        if (ratio <= mergeThreshold) {
          clusterOcc += data.count;
          clusterDates.push(...data.dates);
          usedAmounts.add(amount);
          if (data.count > clusterBestCount) {
            clusterBestAmount = amount;
            clusterBestCount = data.count;
          }
        }
      }

      if (clusterOcc < minOcc) continue;

      if (clusterBestAmount < 25) continue;

      const monthCounts = new Map<string, number>();
      for (const d of clusterDates) {
        const parts = d.split(/[\/-]/);
        let monthKey: string;
        if (parts.length >= 3 && parts[0].length === 4) {
          monthKey = parts[0] + "-" + parts[1];
        } else if (parts.length >= 2) {
          monthKey = parts[0];
        } else {
          monthKey = d;
        }
        monthCounts.set(monthKey, (monthCounts.get(monthKey) || 0) + 1);
      }
      const bestMonthCount = Math.max(...monthCounts.values(), 0);

      const minMonthCount = isGuaranteedLender ? 2 : 3;
      if (bestMonthCount < minMonthCount) {
        continue;
      }

      const frequency = computeFrequencyFromDates(clusterDates);

      results.push({
        lender: group.lender,
        shortName,
        amount: clusterBestAmount,
        frequency,
        occurrences: clusterOcc,
        dates: clusterDates.sort(),
        confidence: clusterOcc >= 5 ? "high" : "medium",
      });
    }
  }

  const amountGroups = new Map<number, ExtractedTransaction[]>();
  for (const d of debits) {
    if (NOT_LOAN_DESCRIPTIONS.test(d.description)) continue;
    if (ADDRESS_PATTERN.test(d.description)) continue;
    if (matchLender(d.description)) continue;

    const rounded = Math.round(d.amount * 100) / 100;
    if (rounded < 50 || rounded > 50000) continue;
    if (!amountGroups.has(rounded)) amountGroups.set(rounded, []);
    amountGroups.get(rounded)!.push(d);
  }

  for (const [amount, txns] of amountGroups) {
    if (txns.length < 3) continue;

    const descriptions = txns.map(t => t.description);
    const commonDesc = findCommonDescription(descriptions);
    if (!commonDesc || commonDesc.length < 3) continue;

    if (NOT_LOAN_DESCRIPTIONS.test(commonDesc)) continue;

    const hasLenderKeyword = /\b(capital|advance|funding|finance|lending|financial|finserv|loan|mca)\b/i.test(commonDesc);
    if (!hasLenderKeyword && txns.length < 5) continue;

    const alreadyCovered = results.some(r => {
      const rNorm = r.lender.toLowerCase().replace(/[^a-z0-9]/g, "");
      const cNorm = commonDesc.toLowerCase().replace(/[^a-z0-9]/g, "");
      return rNorm.includes(cNorm) || cNorm.includes(rNorm);
    });
    if (alreadyCovered) continue;

    const txnDates = txns.map(t => t.date).sort();
    const frequency = computeFrequencyFromDates(txnDates);

    results.push({
      lender: commonDesc,
      shortName: commonDesc.toLowerCase().replace(/[^a-z]/g, "").slice(0, 10),
      amount,
      frequency,
      occurrences: txns.length,
      dates: txnDates,
      confidence: "medium",
    });
  }

  return results;
}

function findFundingDeposits(credits: ExtractedTransaction[]): FundingDeposit[] {
  const results: FundingDeposit[] = [];

  for (const c of credits) {
    if (c.amount < 2500) continue;

    const lender = matchLender(c.description);
    if (!lender) continue;

    results.push({
      lender: lender.fullMatch,
      shortName: lender.shortName,
      amount: c.amount,
      date: c.date,
    });
  }

  return results;
}

export function deduplicateLoansAcrossAccounts(
  allRecurring: RecurringPayment[],
  allFunding: FundingDeposit[]
): { recurring: RecurringPayment[]; funding: FundingDeposit[] } {
  const groups = new Map<string, RecurringPayment[]>();

  for (const r of allRecurring) {
    let foundGroup = false;
    for (const [key, members] of groups) {
      if (areSameLender(r.lender, members[0].lender) || r.shortName === members[0].shortName) {
        members.push(r);
        foundGroup = true;
        break;
      }
    }
    if (!foundGroup) {
      groups.set(r.shortName + "_" + groups.size, [r]);
    }
  }

  const deduped: RecurringPayment[] = [];
  for (const [, members] of groups) {
    if (members.length === 1) {
      deduped.push(members[0]);
      continue;
    }

    const amountGroups = new Map<number, RecurringPayment[]>();
    for (const m of members) {
      let merged = false;
      for (const [amt, group] of amountGroups) {
        const ratio = Math.max(amt, m.amount) / Math.min(amt, m.amount);
        if (ratio <= 1.05) {
          group.push(m);
          merged = true;
          break;
        }
      }
      if (!merged) {
        amountGroups.set(m.amount, [m]);
      }
    }

    for (const [, group] of amountGroups) {
      const allDates = group.flatMap(g => g.dates);
      const allAccounts = group.flatMap(g => g.accountsFoundIn || []);
      const totalOcc = group.reduce((s, g) => s + g.occurrences, 0);
      const best = group.reduce((a, b) => b.occurrences > a.occurrences ? b : a, group[0]);

      const uniqueDates = [...new Set(allDates)].sort();
      const frequency = computeFrequencyFromDates(uniqueDates);

      deduped.push({
        ...best,
        occurrences: totalOcc,
        dates: uniqueDates,
        frequency,
        accountsFoundIn: [...new Set(allAccounts)],
        confidence: totalOcc >= 5 ? "high" : "medium",
      });
    }
  }

  const fundingGroups = new Map<string, FundingDeposit>();
  for (const f of allFunding) {
    const key = normalizeLenderName(f.lender);
    if (!fundingGroups.has(key) || f.amount > fundingGroups.get(key)!.amount) {
      fundingGroups.set(key, f);
    }
  }

  for (const r of deduped) {
    const rKey = normalizeLenderName(r.lender);
    for (const [fKey, f] of fundingGroups) {
      if (areSameLender(r.lender, f.lender)) {
        r.fundedAmount = f.amount;
        r.fundedDate = f.date;
        break;
      }
    }
  }

  return { recurring: deduped, funding: [...fundingGroups.values()] };
}

export function consolidateSameLenderEntries(recurring: RecurringPayment[]): RecurringPayment[] {
  const result: RecurringPayment[] = [];
  const processed = new Set<number>();

  for (let i = 0; i < recurring.length; i++) {
    if (processed.has(i)) continue;
    const r = recurring[i];
    const sameGroup: RecurringPayment[] = [r];
    processed.add(i);

    for (let j = i + 1; j < recurring.length; j++) {
      if (processed.has(j)) continue;
      if (areSameLender(r.lender, recurring[j].lender) || r.shortName === recurring[j].shortName) {
        sameGroup.push(recurring[j]);
        processed.add(j);
      }
    }

    if (sameGroup.length === 1) {
      result.push(r);
      continue;
    }

    const allDates = sameGroup.flatMap(g => g.dates);
    const uniqueDates = [...new Set(allDates)].sort();

    const amountClusters = new Map<number, { entries: RecurringPayment[]; dates: string[] }>();
    for (const entry of sameGroup) {
      let merged = false;
      for (const [amt, cluster] of amountClusters) {
        const ratio = Math.max(amt, entry.amount) / Math.min(amt, entry.amount);
        if (ratio <= 1.05) {
          cluster.entries.push(entry);
          cluster.dates.push(...entry.dates);
          merged = true;
          break;
        }
      }
      if (!merged) {
        amountClusters.set(entry.amount, { entries: [entry], dates: [...entry.dates] });
      }
    }

    if (amountClusters.size === 1) {
      const best = sameGroup.reduce((a, b) => b.occurrences > a.occurrences ? b : a, sameGroup[0]);
      const totalOcc = sameGroup.reduce((s, g) => s + g.occurrences, 0);
      const frequency = computeFrequencyFromDates(uniqueDates);
      result.push({
        ...best,
        occurrences: totalOcc,
        dates: uniqueDates,
        frequency,
        confidence: totalOcc >= 5 ? "high" : "medium",
      });
    } else {
      const sortedClusters = [...amountClusters.entries()].sort((a, b) =>
        b[1].entries.reduce((s, e) => s + e.occurrences, 0) -
        a[1].entries.reduce((s, e) => s + e.occurrences, 0)
      );

      const hasDateOverlap = (dates1: string[], dates2: string[]) => {
        const set1 = new Set(dates1);
        return dates2.some(d => set1.has(d));
      };

      if (sortedClusters.length === 2) {
        const [, cluster1] = sortedClusters[0];
        const [, cluster2] = sortedClusters[1];
        if (hasDateOverlap(cluster1.dates, cluster2.dates)) {
          const best = cluster1.entries.reduce((a, b) => b.occurrences > a.occurrences ? b : a, cluster1.entries[0]);
          const totalOcc = [...cluster1.entries, ...cluster2.entries].reduce((s, e) => s + e.occurrences, 0);
          const allDates = [...new Set([...cluster1.dates, ...cluster2.dates])].sort();
          result.push({
            ...best,
            occurrences: totalOcc,
            dates: allDates,
            frequency: computeFrequencyFromDates(allDates),
            confidence: totalOcc >= 5 ? "high" : "medium",
          });
        } else {
          for (const [, cluster] of sortedClusters) {
            const best = cluster.entries.reduce((a, b) => b.occurrences > a.occurrences ? b : a, cluster.entries[0]);
            const totalOcc = cluster.entries.reduce((s, e) => s + e.occurrences, 0);
            const cDates = [...new Set(cluster.dates)].sort();
            result.push({
              ...best,
              occurrences: totalOcc,
              dates: cDates,
              frequency: computeFrequencyFromDates(cDates),
              confidence: totalOcc >= 5 ? "high" : "medium",
            });
          }
        }
      } else {
        const allEntries = sortedClusters.flatMap(([, c]) => c.entries);
        const best = allEntries.reduce((a, b) => b.occurrences > a.occurrences ? b : a, allEntries[0]);
        const totalOcc = allEntries.reduce((s, e) => s + e.occurrences, 0);
        const allClusterDates = sortedClusters.flatMap(([, c]) => c.dates);
        const allDates = [...new Set(allClusterDates)].sort();
        result.push({
          ...best,
          occurrences: totalOcc,
          dates: allDates,
          frequency: computeFrequencyFromDates(allDates),
          confidence: totalOcc >= 5 ? "high" : "medium",
        });
      }
    }
  }

  return result;
}

function findCommonDescription(descs: string[]): string {
  if (descs.length === 0) return "";

  const clean = (d: string) => d.replace(/\d{6,}/g, "").replace(/\b(ACH|CORP|DEBIT|EXPC|INC|LLC|CUSTOMER|ID|AUTOPAY|PYMT|PMT|SCD|DBCRD|PAYMENT|TRANSFER|DEPOSIT|WITHDRAWAL|PURCHASE|POS|ATM|CREDIT|CHECK|WIRE|PRENOTE|RECURRING|ONETIME|XXXXXX?\d{4}|x{3,}\d+)\b/gi, "").replace(/\s+/g, " ").trim();

  const cleaned = descs.map(clean);
  if (cleaned.length === 0) return "";

  const wordCounts = new Map<string, number>();
  for (const desc of cleaned) {
    const words = desc.split(/\s+/);
    const seen = new Set<string>();
    for (const w of words) {
      const key = w.toUpperCase();
      if (key.length < 2 || seen.has(key)) continue;
      seen.add(key);
      wordCounts.set(key, (wordCounts.get(key) || 0) + 1);
    }
  }

  const threshold = cleaned.length * 0.6;
  const commonWords = [...wordCounts.entries()]
    .filter(([_, count]) => count >= threshold)
    .map(([word]) => word);

  if (commonWords.length === 0) return "";
  return commonWords.join(" ");
}

function parseAccountNumber(text: string): string | undefined {
  const patterns = [
    /account\s+(?:number|ending\s+in|#)\s*:?\s*\*+?(\d{4})\b/i,
    /primary\s+account\s*:?\s*\*+?(\d{4})\b/i,
    /checking\s+(?:account\s+)?#?\s*\*+?(\d{4})\b/i,
    /account\s+suffix\s*:?\s*(\d{4})\b/i,
    /\b(\d{4})\s+ending\s+account\b/i
  ];
  for (const p of patterns) {
    const m = text.match(p);
    if (m) return m[1];
  }
  return undefined;
}
