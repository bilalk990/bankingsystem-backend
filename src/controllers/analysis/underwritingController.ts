import { Router, type IRouter } from "express";
import { eq, and, sql, count, inArray, isNotNull, not } from "drizzle-orm";
import { db, leadsTable, documentsTable, bankStatementAnalysesTable, underwritingConfirmationsTable, dealsTable, notificationsTable, usersTable, appSettingsTable, uploadBatchesTable } from "../../configs/database";
import { requireAuth, requireAdmin } from "../../middlewares/authMiddleware";
import { maskSsn, decryptLeadFields } from "../../utils/encryption";
import { logSecurityEvent } from "../../utils/security";
import { backgroundJobs, parseAIResponse, analyzeSingleLead, cancelScrubbing, resetScrubCancel, runConcurrentBatch, costTracker, type BackgroundJob, KNOWN_LENDER_SHORTNAMES, normalizeLenderKey, saveLenderRule, invalidateVerdictCache } from "./coreController";
import { extractAccountLast4, extractAccountFromFilename } from "../../services/accountExtractor";
import { appendToRepSheet } from "../../services/googleSheetsRepService";
import { getUncachableGoogleSheetClient } from "../../services/googleSheetsService";

const router: IRouter = Router();

interface RevEntry { sortKey: number; monthNum: string; fullMonth: string; rev: number; account?: string; }
interface LoanEntry { lender: string; amount: number; frequency: string; fundedAmount?: number; fundedDate?: string; account?: string; occurrences?: number; possiblyPaidOff?: boolean; lastSeenMonth?: string; lastSeenLabel?: string; }
const parseNumeric = (val: any): number => {
  if (val === null || val === undefined) return 0;
  if (typeof val === 'number') return isNaN(val) ? 0 : val;
  const cleaned = String(val).replace(/[^0-9.-]/g, '');
  const parsed = parseFloat(cleaned);
  return isNaN(parsed) ? 0 : parsed;
};

const MONTH_NAMES: Record<string, string> = { january:"1",february:"2",march:"3",april:"4",may:"5",june:"6",july:"7",august:"8",september:"9",october:"10",november:"11",december:"12",
  jan:"1",feb:"2",mar:"3",apr:"4",jun:"6",jul:"7",aug:"8",sep:"9",oct:"10",nov:"11",dec:"12" };

function shortK(val: number): string {
  if (!val || val === 0) return "0";
  const abs = Math.abs(val);
  if (abs >= 950000) {
    const millions = abs / 1000000;
    const rounded = Math.round(millions * 10) / 10;
    return rounded % 1 === 0 ? `${rounded}M` : `${rounded}M`;
  }
  const thousands = abs / 1000;
  const frac = thousands - Math.floor(thousands);
  const rounded = frac >= 0.8 ? Math.ceil(thousands) : Math.floor(thousands);
  return `${rounded}k`;
}

function loanAmtStr(val: number, isFunded = false): string {
  if (!val || val === 0) return "0";
  const abs = Math.abs(val);
  if (isFunded) {
    if (abs >= 950000) return `${(abs / 1000000).toFixed(1)}M`;
    if (abs >= 1000) {
      const thousands = abs / 1000;
      const frac = thousands - Math.floor(thousands);
      const rounded = frac >= 0.8 ? Math.ceil(thousands) : Math.floor(thousands);
      return `${rounded}k`;
    }
    return String(Math.round(abs));
  }
  return String(Math.round(abs));
}

const LENDER_SHORT_NAMES: Record<string, string> = {
  "ondeck capital": "ondeck", "ondeck": "ondeck", "on deck capital": "ondeck",
  "forward financing": "forward", "forwardfinusa": "forward", "forwardfin": "forward", "forwardfinance": "forward", "forwardfinance3": "forward",
  "business to business forward financin": "forward", "business to business forward financing": "forward", "forward financin": "forward", "forward fin": "forward",
  "fundbox inc.": "fundbox", "fundbox inc": "fundbox", "fundbox": "fundbox",
  "cfgmerchant solutions": "cfg", "cfg merchant solutions": "cfg", "cfg merchant solutions llc": "cfg", "cfgms": "cfg", "cfg": "cfg",
  "fox funding group": "fox", "fox funding": "fox",
  "mulligan funding": "mulligan", "pearl capital": "pearl",
  "itria ventures": "itria", "itria ventures (itria ven hc 1)": "itria",
  "maison capital": "masion", "kapitus": "kapitus", "kapitus servicin": "kapitus", "kap servicing": "kapitus", "kap servic": "kapitus",
  "webbank/intuit": "intuit", "intuit financing": "intuit", "intuit financing (qbc payments)": "intuit",
  "intuit financing qbc (intuit)": "intuit", "intuit payments inc (ach)": "intuit",
  "credibly": "credibly", "kalamata capital": "kalamata", "kalamata capital group llc": "kalamata",
  "square capital": "square", "square capital (sq advance)": "square", "sq advance": "square", "daily sq advance": "square", "sq capital": "square", "sq loan": "square", "square": "square",
  "shopify capital": "shopify", "bluevine capital": "bluevine", "bluevine": "bluevine",
  "breakout capital": "breakout", "iou central": "iou",
  "national funding": "national", "headway capital": "headway",
  "sba eid loan": "sba", "sba eidl loan": "sba", "sba loan": "sba", "sba loan payment": "sba",
  "lendistry": "lendistry", "lendingclub bank": "lendingclub",
  "essentia funding": "essentia", "w funding": "wfunding", "daily w funding": "wfunding", "daily funding": "wfunding",
  "fundation group": "fondation", "fundation funding": "fondation",
  "mint funding": "mint", "spartan capital": "spartan",
  "sbfs llc": "sbfs", "selene finance": "selene",
  "efinancialtree": "efinancial", "fincor financing": "fincor",
  "lg funding llc": "lg", "top choice financial llc": "topchoice", "top choice": "topchoice",
  "loot financial s / loot financial services": "loot", "loot": "loot",
  "revenued llc": "revenued", "revenued": "revenued",
  "celtic bank": "celtic", "mca servicing company": "mca",
  "libertas funding": "libertas", "libertas": "libertas",
  "lendingservices": "lendingserv",
  "fundomate techno": "fundomate", "the fundworks": "fundworks", "the fundworks financial": "fundworks",
  "capremium": "capremium", "capital premium": "capremium", "capital premium finance": "capremium", "capital premium financing": "capremium", "cap premium": "capremium", "fleetcor": "fleetcor",
  "acv capital": "acv", "acvcapital": "acv",
  "bizfund": "bizfund", "bizfund.com llc": "bizfund",
  "kanmon": "kanmon", "valon": "valon",
  "rival funding": "rival", "overton funding": "overton",
  "dsc investors us (likely mca)": "dsc", "dsc investors us": "dsc",
  "ebf holdings llc": "ebf", "ebf holdings (ebf debit)": "ebf", "ebf holdings": "ebf",
  "cobalt funding / cobalt fund": "cobalt",
  "honor capital eti": "honor",
  "fora financial": "fora", "forafinancial s6": "fora",
  "american express loan": "amexloan", "american express (loan products)": "amexloan",
  "nextgear payment": "nextgear", "nextgear funding": "nextgear",
  "idea 247 inc": "idea247",
  "beacon funding": "beacon",
  "pirs capital llc": "pirs",
  "uptown fund": "uptown",
  "lux financial llc (choice bank)": "lux",
  "app funding beta llc": "appfund",
  "enfin financial (enfin-m3 & enfin corp)": "enfin",
  "legend advance funding": "legend",
  "carlton capital group": "carlton",
  "fintapp payment": "fintapp", "fintap": "fintap",
  "ramp statement": "ramp",
  "specialty capital 2": "specialty", "specialty cap 2": "specialty",
  "qfs capital llc": "qfs",
  "vitalcap fund": "vitalcap", "vitalcap": "vitalcap", "vital capital": "vitalcap", "vitalcap fund preauthpmt": "vitalcap",
  "vcg": "vcg", "vcg capital": "vcg", "vcg funding": "vcg",
  "vox funding": "vox", "vox capital": "vox",
  "zen funding": "zen", "zen funding source": "zen", "zen funding source llc": "zen",
  "fintegra": "fintegra", "fintegra llc": "fintegra", "fintegra funding": "fintegra",
  "altfunding": "alt", "alt funding": "alt", "altfunding llc": "alt",
  "forward fin llc": "forward", "forward fin llc ff": "forward",
  "funding futures": "futures", "funding futures llc": "futures",
  "olympus business capital": "olympus", "olympus capital": "olympus", "olympus business": "olympus",
  "mako funding": "mako", "mako capital": "mako",
  "gmfunding": "gmfunding", "gm funding": "gmfunding", "gmfunding daily": "gmfunding", "gm funding daily": "gmfunding",
  "fratello funding": "fratello", "fratello capital": "fratello", "fratello": "fratello",
  "ascentra": "ascentra", "ascentra funding": "ascentra", "ascentra capital": "ascentra",
  "luminar funding": "luminar", "luminar capital": "luminar", "luminar": "luminar",
  "kif funding": "kif", "kif": "kif",
  "greenbridge capital": "greenbridge", "greenbridgecap": "greenbridge", "greenbridge": "greenbridge",
  "arbitrage funding": "arbitrage", "arbitrage": "arbitrage",
  "jrg capital": "jrg", "jrg": "jrg",
  "aurum funding": "aurum", "aurum capital": "aurum", "aurum": "aurum",
  "pdm capital": "pdm", "pdm funding": "pdm", "pdm": "pdm",
  "pfg capital": "pfg", "pfg funding": "pfg", "pfg": "pfg",
  "stash capital": "stashcap", "stashcap": "stashcap",
  "merchant advance": "merchadv", "merchadv": "merchadv",
  "lily funding": "lily", "lily": "lily",
  "mckenzie capital": "mckenzie", "mckenzie": "mckenzie",
  "purple tree": "purpletree", "purpletree capital": "purpletree", "purpletree": "purpletree",
  "lexio funding": "lexio", "lexio capital": "lexio", "lexio": "lexio",
  "global funding": "global", "global capital": "global",
  "advance syndicate": "advsyn", "advsyn": "advsyn", "advancesyndicate": "advsyn",
  "monetaria": "monetaria", "monetaria funding": "monetaria",
  "trustify": "trustify", "trustify capital": "trustify",
  "bluetie": "bluetie", "bluetie funding": "bluetie",
  "seamless funding": "seamless", "seamless capital": "seamless",
  "liquidbee": "liquidbee", "liquidbee capital": "liquidbee",
  "belltower": "belltower", "belltower capital": "belltower",
  "palisade": "palisade", "palisade capital": "palisade", "palisad": "palisade",
  "marlin": "marlin", "marlin funding": "marlin", "marlin capital": "marlin",
  "xuper": "xuper", "xuper funding": "xuper",
  "fundfi": "fundfi", "fund fi": "fundfi",
  "slim funding": "slim", "slim capital": "slim",
  "steady funding": "steady", "steady capital": "steady",
  "newco capital": "newco", "newco": "newco",
  "secure account": "secure", "secure funding": "secure", "secure capital": "secure",
};

const LENDER_CANONICAL_NAMES: Record<string, string> = {
  wfunding: "W Funding",
  square: "SQ Advance",
  ondeck: "OnDeck",
  kabbage: "Kabbage",
  bluevine: "BlueVine",
  credibly: "Credibly",
  libertas: "Libertas",
  kapitus: "Kapitus",
  fundbox: "Fundbox",
  itria: "Itria Ventures",
  vitalcap: "VitalCap",
  revenued: "Revenued",
  gmfunding: "GM Funding",
};

const BANK_PREFIX_WORDS = new Set(["corp", "ach", "achpmt", "pmt", "payment", "debit", "credit", "web", "preauth", "preauthpmt", "preauthorized", "recurring", "expc", "check", "draft", "online", "transfer", "wire", "autopay", "daily", "weekly", "monthly", "external", "withdrawal"]);

function stripACHPrefix(name: string): string {
  return name
    .replace(/^business\s+to\s+business\s*/i, "")
    .replace(/^(?:ach|corp)\s+(?:debit|credit|payment|pmt)\s*[-–—]?\s*/i, "")
    .trim();
}

function shortLenderName(lender: string): string {
  const stripped = stripACHPrefix(lender);
  const lower = stripped.toLowerCase().trim();
  if (LENDER_SHORT_NAMES[lower]) return LENDER_SHORT_NAMES[lower];
  for (const [key, short] of Object.entries(LENDER_SHORT_NAMES)) {
    if (lower.includes(key)) return short;
  }
  const words = lower.replace(/[^a-z0-9\s]/g, "").split(/\s+/).filter(w => w.length > 1);
  const meaningful = words.filter(w => !BANK_PREFIX_WORDS.has(w) && !/^\d+$/.test(w));
  return (meaningful[0] || words[0])?.slice(0, 10) || "unknown";
}

function inferYearFromAnalysis(analysis: any): number {
  const statementMonth = analysis.statementMonth || "";
  const ymdMatch = statementMonth.match(/(\d{4})/);
  if (ymdMatch) return parseInt(ymdMatch[1]);
  for (const mr of ((analysis.monthlyRevenues as any[]) || [])) {
    const m = (mr.month || "").match(/(\d{4})/);
    if (m) return parseInt(m[1]);
  }
  const dateRange = analysis.statementPeriod || analysis.dateRange || "";
  const ym = dateRange.match(/(\d{4})/);
  if (ym) return parseInt(ym[1]);
  return 0;
}

function parseRevEntries(analyses: any[]): RevEntry[] {
  const entries: RevEntry[] = [];
  const seen = new Map<string, number>();
  for (const a of analyses) {
    const inferredYear = inferYearFromAnalysis(a);
    for (const mr of ((a.monthlyRevenues as any[]) || [])) {
      let m = (mr.month || "").toLowerCase().trim();
      let monthNum = "?";
      let sortKey = 0;
      let fullMonth = m;
      const ymdMatch = m.match(/^(\d{4})[\-\/](\d{1,2})/);
      if (ymdMatch) {
        const yr = parseInt(ymdMatch[1]);
        const mo = parseInt(ymdMatch[2]);
        monthNum = String(mo);
        sortKey = yr * 100 + mo;
        fullMonth = `${yr}-${String(mo).padStart(2, "0")}`;
      } else {
        const parts = m.split(/[\s\/\-]+/);
        monthNum = MONTH_NAMES[parts[0]] || parts[0].replace(/[^0-9]/g, "") || "?";
        const mo = parseInt(monthNum) || 0;
        let yr = 0;
        for (let pi = 1; pi < parts.length; pi++) {
          const yrCandidate = parseInt(parts[pi]);
          if (yrCandidate >= 2020 && yrCandidate <= 2030) { yr = yrCandidate; break; }
          if (yrCandidate >= 20 && yrCandidate <= 30) { yr = 2000 + yrCandidate; break; }
        }
        if (yr === 0 && inferredYear > 0 && mo > 0) {
          yr = inferredYear;
          console.log(`[ParseRevEntries] Month "${m}" has no year — inferred ${yr} from analysis metadata`);
        }
        if (yr > 0 && mo > 0) {
          sortKey = yr * 100 + mo;
          fullMonth = `${yr}-${String(mo).padStart(2, "0")}`;
        } else {
          sortKey = mo;
          fullMonth = monthNum;
          console.warn(`[ParseRevEntries] WARNING: Month "${m}" has no year and could not be inferred — chronological sort may be wrong`);
        }
      }
      const acct = mr.account || "";
      const rev = mr.revenue || 0;
      const dupeKey = `${fullMonth}|${acct}`;
      const existingIdx = seen.get(dupeKey);
      if (existingIdx !== undefined) {
        const existingRev = entries[existingIdx].rev;
        const ratio = Math.max(rev, existingRev) / Math.min(rev, existingRev);
        if (ratio <= 1.05) {
          if (rev > existingRev) {
            entries[existingIdx] = { sortKey, monthNum, fullMonth, rev, account: mr.account || undefined };
          }
        } else {
          const altKey = `${fullMonth}|${acct}|${entries.length}`;
          seen.set(altKey, entries.length);
          entries.push({ sortKey, monthNum, fullMonth, rev, account: mr.account || undefined });
        }
      } else {
        seen.set(dupeKey, entries.length);
        entries.push({ sortKey, monthNum, fullMonth, rev, account: mr.account || undefined });
      }
    }
  }
  entries.sort((a, b) => b.sortKey - a.sortKey || b.rev - a.rev);
  return entries;
}

function parseMonthSortKey(monthStr: string): number {
  const moMap: Record<string, number> = {
    jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6,
    jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12,
    january: 1, february: 2, march: 3, april: 4, june: 6,
    july: 7, august: 8, september: 9, october: 10, november: 11, december: 12,
  };
  const m = monthStr.match(/^(\d{4})-(\d{2})$/);
  if (m) return parseInt(m[1]) * 100 + parseInt(m[2]);
  const w = monthStr.match(/(\w+)\s+(\d{4})/i);
  if (w) {
    const mo = moMap[w[1].toLowerCase()];
    if (mo) return parseInt(w[2]) * 100 + mo;
  }
  const s = monthStr.match(/^([A-Za-z]{3})(\d{2,4})$/);
  if (s) {
    const mo = moMap[s[1].toLowerCase()];
    const yr = s[2].length === 2 ? 2000 + parseInt(s[2]) : parseInt(s[2]);
    if (mo) return yr * 100 + mo;
  }
  return 0;
}

function getAnalysisMonth(analysis: any): string {
  const revs = (analysis.monthlyRevenues as any[]) || [];
  let bestMonth = "";
  let bestKey = 0;
  for (const r of revs) {
    const m = r.month || "";
    const key = parseMonthSortKey(m);
    if (key > bestKey) { bestKey = key; bestMonth = m; }
  }
  if (!bestMonth && analysis.statementMonth) {
    bestMonth = analysis.statementMonth;
  }
  return bestMonth;
}

function monthLabel(monthStr: string): string {
  const moNames: Record<number, string> = {
    1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
    7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December",
  };
  const m = monthStr.match(/^(\d{4})-(\d{2})$/);
  if (m) return `${moNames[parseInt(m[2])] || m[2]} ${m[1]}`;
  return monthStr;
}

function buildActiveLoansFromMostRecent(analyses: any[]): { activeLoans: any[]; totalLoanPayments: number } {
  if (analyses.length === 0) return { activeLoans: [], totalLoanPayments: 0 };

  const analysesWithMonth = analyses.map(a => {
    const month = getAnalysisMonth(a);
    return { analysis: a, month, monthKey: parseMonthSortKey(month) };
  });

  const withMonth = analysesWithMonth.filter(a => a.monthKey > 0);
  if (withMonth.length === 0) {
    // console.log(`[ActiveLoans] No month data found in ${analyses.length} analyses — using all as active`);
    const fallbackLoans: any[] = [];
    for (const a of analyses) {
      for (const loan of ((a.loanDetails as any[]) || [])) {
        const lenderName = (loan.lender || "").trim();
        if (!lenderName || lenderName.length < 3) continue;
        if (!loan.amount || loan.amount <= 0) continue;
        if (/^(withdrawal|withdraw)$/i.test(lenderName)) continue;
        if (!KNOWN_LENDER_SAFELIST.test(lenderName) && NOT_LOAN_ANYWHERE.test(lenderName)) continue;
        const occ = loan.occurrences || 0;
        if (occ < 2) continue;
        if (loan.amount > 5000 && (loan.frequency === "daily" || !loan.frequency) && occ < 10) continue;
        const lenderShort = shortLenderName(lenderName);
        const existing = fallbackLoans.find(l => {
          if (shortLenderName(l.lender || "") !== lenderShort) return false;
          if (l._analysisId === a.id) return false;
          return Math.abs((l.amount || 0) - (loan.amount || 0)) / Math.max(l.amount || 1, loan.amount || 1) < 0.35;
        });
        if (existing) {
          existing.occurrences = (existing.occurrences || 0) + (loan.occurrences || 0);
          if (!existing.fundedAmount && (loan.fundedAmount || loan.funded_amount)) existing.fundedAmount = loan.fundedAmount || loan.funded_amount;
          if (!existing.fundedDate && (loan.fundedDate || loan.funded_date)) existing.fundedDate = loan.fundedDate || loan.funded_date;
        } else {
          fallbackLoans.push({ ...loan, occurrences: loan.occurrences || 0, fundedAmount: loan.fundedAmount || loan.funded_amount || null, fundedDate: loan.fundedDate || loan.funded_date || null, _analysisId: a.id });
        }
      }
    }
    let totalLP = 0;
    for (const loan of fallbackLoans) {
      if (loan.amount && (loan.frequency === "daily" || loan.frequency === "weekly")) {
        let m = typeof loan.amount === "string" ? parseFloat(loan.amount) : (Number(loan.amount) || 0);
        if (loan.frequency === "daily") m *= 22;
        else if (loan.frequency === "weekly") m *= 4;
        totalLP += m;
      }
    }
    return { activeLoans: fallbackLoans, totalLoanPayments: totalLP };
  }

  withMonth.sort((a, b) => b.monthKey - a.monthKey);
  const mostRecentKey = withMonth[0].monthKey;
  const mostRecentMonth = withMonth[0].month;
  const recentYr = Math.floor(mostRecentKey / 100);
  const recentMo = mostRecentKey % 100;
  const cutoffKey = recentMo <= 2 ? (recentYr - 1) * 100 + (recentMo + 10) : mostRecentKey - 2;
  // console.log(`[ActiveLoans] Most recent statement month: ${mostRecentMonth} (key=${mostRecentKey}), cutoff=${cutoffKey}, ${withMonth.length} analyses with month data`);
  const mostRecentAnalyses = withMonth.filter(a => a.monthKey >= cutoffKey);
  const olderAnalyses = withMonth.filter(a => a.monthKey < cutoffKey);

  const activeLoans: any[] = [];

  for (const { analysis } of mostRecentAnalyses) {
    for (const loan of ((analysis.loanDetails as any[]) || [])) {
      const lenderName = (loan.lender || "").trim();
      if (!lenderName || lenderName.length < 3) continue;
      if (!loan.amount || loan.amount <= 0) continue;
      if (/^(withdrawal|withdraw)$/i.test(lenderName)) continue;
      if (!KNOWN_LENDER_SAFELIST.test(lenderName) && NOT_LOAN_ANYWHERE.test(lenderName)) continue;
      const occ = loan.occurrences || 0;
      if (occ < 2) continue;
      if (loan.amount > 5000 && (loan.frequency === "daily" || !loan.frequency) && occ < 10) continue;
      const lenderShort = shortLenderName(lenderName);
      const loanAcct = (loan.account || "").replace(/\D/g, "").slice(-4);
      const existing = activeLoans.find(l => {
        if (shortLenderName(l.lender || "") !== lenderShort) return false;
        if (l._analysisId === analysis.id) return false;
        return Math.abs((l.amount || 0) - (loan.amount || 0)) / Math.max(l.amount || 1, loan.amount || 1) < 0.35;
      });
      if (existing) {
        existing.occurrences = (existing.occurrences || 0) + (loan.occurrences || 0);
        if (!existing.fundedAmount && (loan.fundedAmount || loan.funded_amount)) {
          existing.fundedAmount = loan.fundedAmount || loan.funded_amount;
        }
        if (!existing.fundedDate && (loan.fundedDate || loan.funded_date)) {
          existing.fundedDate = loan.fundedDate || loan.funded_date;
        }
      } else {
        activeLoans.push({
          ...loan,
          occurrences: loan.occurrences || 0,
          fundedAmount: loan.fundedAmount || loan.funded_amount || null,
          fundedDate: loan.fundedDate || loan.funded_date || null,
          _analysisId: analysis.id,
        });
      }
    }
  }

  for (const activeLoan of activeLoans) {
    const activeShort = shortLenderName(activeLoan.lender || "");
    for (const { analysis, month } of olderAnalyses) {
      for (const olderLoan of ((analysis.loanDetails as any[]) || [])) {
        const olderShort = shortLenderName((olderLoan.lender || "").trim());
        if (olderShort !== activeShort) continue;
        const olderFunded = olderLoan.fundedAmount || olderLoan.funded_amount;
        const olderDate = olderLoan.fundedDate || olderLoan.funded_date;
        if (olderFunded && (!activeLoan.fundedAmount || olderFunded > activeLoan.fundedAmount)) {
          activeLoan.fundedAmount = olderFunded;
          activeLoan.fundedDate = olderDate || month;
          // console.log(`[ActiveLoans] "${activeLoan.lender}": found funding $${olderFunded} from ${month}`);
        } else if (!activeLoan.fundedDate && olderDate) {
          activeLoan.fundedDate = olderDate;
        }
        if (!activeLoan.fundedDate && month) {
          const isEarliestOccurrence = !olderAnalyses.some(o =>
            o.monthKey < parseMonthSortKey(month) &&
            ((o.analysis.loanDetails as any[]) || []).some(l => shortLenderName((l.lender || "").trim()) === activeShort)
          );
          if (isEarliestOccurrence && !activeLoan.fundedDate) {
            activeLoan.fundedDate = month;
          }
        }
      }
    }
  }

  let totalLoanPayments = 0;
  for (const loan of activeLoans) {
    if (loan.amount && (loan.frequency === "daily" || loan.frequency === "weekly" || loan.frequency === "monthly")) {
      let m = loan.amount;
      if (loan.frequency === "daily") m *= 22;
      else if (loan.frequency === "weekly") m *= 4;
      else if (loan.frequency === "monthly") m *= 1;
      totalLoanPayments += m;
    }
  }

  return { activeLoans, totalLoanPayments };
}

function fixLoanFrequencies(allLoans: any[], numMonths: number) {
  const n = Math.max(numMonths, 1);
  for (let i = allLoans.length - 1; i >= 0; i--) {
    const loan = allLoans[i];
    if (loan.frequency === "biweekly") {
      loan.frequency = "weekly";
    }
    const occ = loan.occurrences || 0;
    if (occ < 2) {
      console.log(`[FreqFix] Removing "${loan.lender}": only ${occ} occurrence(s) — not a recurring loan`);
      allLoans.splice(i, 1);
      continue;
    }
    const perMonth = occ / n;
    const lenderName = (loan.lender || "").trim();
    const isKnown = KNOWN_LENDER_SAFELIST.test(lenderName);
    const amount = loan.amount || 0;

    if (loan.frequency === "daily") {
      if (perMonth >= 15 && perMonth <= 30) {
      } else if (perMonth >= 10 && perMonth < 15) {
      } else if (perMonth >= 3 && perMonth < 10) {
        console.log(`[FreqFix] "${lenderName}": ${perMonth.toFixed(1)}/mo — too few for daily, setting to weekly`);
        loan.frequency = "weekly";
      } else if (perMonth >= 1 && perMonth < 3) {
        if (isKnown) {
          console.log(`[FreqFix] "${lenderName}": ${perMonth.toFixed(1)}/mo — known lender, setting to monthly`);
          loan.frequency = "monthly";
        } else {
          console.log(`[FreqFix] Removing "${lenderName}": ${perMonth.toFixed(1)}/mo — not enough for recurring`);
          allLoans.splice(i, 1);
          continue;
        }
      } else if (perMonth < 1) {
        if (!isKnown) { allLoans.splice(i, 1); continue; }
        loan.frequency = "monthly";
      }
    }

    if (loan.frequency === "weekly") {
      if (perMonth >= 15 && amount <= 3000) {
        console.log(`[FreqFix] "${lenderName}": ${perMonth.toFixed(1)}/mo, $${amount} — upgrading weekly to daily`);
        loan.frequency = "daily";
      } else if (perMonth < 2 && !isKnown) {
        allLoans.splice(i, 1);
        continue;
      } else if (perMonth >= 1 && perMonth < 2 && isKnown) {
        loan.frequency = "monthly";
      }
    }

    if (loan.frequency === "monthly") {
      if (perMonth >= 15) {
        loan.frequency = "daily";
      } else if (perMonth >= 3) {
        loan.frequency = "weekly";
      }
    }

    if (amount > 15000 && loan.frequency === "weekly" && perMonth < 4) {
      console.log(`[FreqFix] Removing "${lenderName}": $${amount} weekly with ${perMonth.toFixed(1)}/mo — likely not an MCA`);
      allLoans.splice(i, 1);
      continue;
    }
  }
}

const NOT_LOAN_KEYWORDS = /^(amex\b|visa\b|mastercard|discover\b|chase\s+card|citi\s+card|barclays|rent\b|lease\b|utilit|electric|gas\s+co|water\b|phone\b|internet|payroll|adp\b|gusto\b|paychex|irs\b|state\s+tax|stripe\s+proc|clover\b|paypal\s+merch|subscript|advertis|google\s+ads|facebook|meta\s+ads|accounting|child\s+support|texas\s+sdu|sdu\b|intuit\s+payroll|us\s+bank\s+loan|usbank\s+loan|wells\s+fargo\s+(mort|loan|auto)|chase\s+(mort|loan|auto)|bank\s+of\s+america\s+(mort|loan|auto)|driveway\b|carvana\b|ally\s+(auto|financial)|sba\b|sba\s+eid|sba\s+loan|sba\s+eidl|m&t\b|m\s*&\s*t\s+equip|sumitomo|mitsui|vfs\b|paccar|priority\s+first|volvo\s+fin|daimler|peterbilt|kenworth|freightliner|navistar|caterpillar\s+fin|john\s+deere|kubota\s+cred|komatsu|case\s+cred|bobcat|toyota\s+motor\s+cred|de\s+lage\s+landen|dll\b|pnc\s+equip|wells\s+fargo\s+equip|bank\s+of\s+the\s+west|tcf\s+equip|equip.*finance|equip.*leasing|truck.*finance|truck.*leasing|commercial\s+vehicle|fleet.*finance|zelle\b|venmo\b|cash\s*app|cashapp|apple\s+pay|google\s+pay|samsung\s+pay|money\s+transfer|payment\s+sent|booster\s+fuel|authorized\s+on|recurring\s+payment|capital\s+one\s+online|on$|to$|^on\b|^to\b)/i;
const NOT_LOAN_ANYWHERE = /\binsurance\b|\baflac\b|\baflak\b|\bdakota\s*fin|\batm\s*withdraw|\bintuit\b|\bpaychex\b|\bgusto\b|\badp\b|\bquickbooks\b|\bsquare\s*payroll\b|\bverizon\b|\bcomcast\b|\bspectrum\b|\bxfinity\b|\bbill\s*pay\b|\bonline\s*(?:pay|banking|bill)\b|\bbnk\s*of\s*amer\b|\bbank\s*of\s*america\b|\brobinhood\b|\bcoinbase\b|\bwebull\b|\be\s*trade\b|\betrade\b|\bschwab\b|\bfidelity\b|\btd\s*ameritrade\b|\bally\s*invest\b|\bacorns\b|\bsofi\s*invest\b|\bcarmax\s*auto\b|\ballstate\b|\bgeico\b|\bstate\s*farm\b|\bfleetsmarts\b|\bclicklease\b|\blease\s*services\b/i;
const KNOWN_LENDER_SAFELIST = /\b(ondeck|on\s*deck|odk\s*capital|kabbage|paypal\s*working|square\s*capital|bluevine|fundbox|can\s*capital|rapid\s*financ|credibly|libertas|yellowstone|pearl\s*capital|forward\s*fin|forwardfinusa|fora\s*financial|kalamata|national\s*funding|fox\s*fund|mantis|everest\s*business|cfg\s*merchant|cfgms|mulligan|reliant|clearview|itria|cloudfund|navitas|ascentium|tvt\s*capital|greenbox|world\s*business|biz2credit|lendio|fundation|celtic\s*bank|webbank|breakout|headway|behalf|payability|newtek|smartbiz|vox\s*fund|wynwood|platinum\s*rapid|green\s*capital|qfs|jmb\s*capital|unique\s*fund|samson|kings\s*capital|stage\s*adv|7even|cashable|vitalcap|vital\s*capital|vcg|zen\s*funding|ace\s*funding|acg\s*llc|app\s*funding|aspire\s*fund|biz\s*capital|breeze\s*advance|bretton|canfield|capybara|clara\s*capital|compass\s*fund|credit\s*key|daytona\s*fund|diamond\s*advance|eg\s*capital|elevate\s*fund|epic\s*advance|expansion\s*capital|family\s*fund|fast\s*business\s*cash|fdm|fenix\s*capital|figure\s*lending|forever\s*fund|fresh\s*fund|funding\s*metrics|giggle\s*financ|gotorro|highland\s*hill|hightower\s*capital|honor\s*capital|i\s*got\s*funded|gotfunded|idea\s*247|idea\s*financial|ifund\s*expert|immediate\s*advance|immediate\s*capital|iou\s*central|kapitus|gmfunding|gm\s*funding|sq\s*advance|w\s*funding|lcf\s*group|legend\s*advance|lendbuzz|lendistry|lendr|lg\s*funding|liberty\s*fund|litefund|millstone|mr\s*advance|newco\s*capital|newport\s*business|nitro\s*advance|oak\s*capital|ocean\s*advance|olympus\s*business|olympus\s*capital|one\s*river|orange\s*advance|overton\s*fund|parkside\s*fund|path\s*2\s*capital|power\s*fund|premium\s*merchant|prosperum|ram\s*payment|readycap|reboost|redwood\s*business|reliance\s*financial|retro\s*advance|revenued|rocket\s*capital|samsonservic|secure\s*account|servicing\s*by\s*kap|kap\s*servic|simply\s*fund|snap\s*financ|spartan\s*capital|specialty\s*capital|stellar\s*advance|suncoast\s*fund|swift\s*fund|tbf\s*group|the\s*fundworks|thefundworks|thoro\s*corp|triton|trupath|ufce|ufs|upfunding|vader|wave\s*advance|webfunder|westwood\s*fund|wide\s*merchant|zen\s*fund|pipe\s*capital|ssmb\s*financial|coast\s*fund|fintegra|altfunding|alt\s*funding|funding\s*futures|mako\s*fund|main\s*street\s*group|integra\s*fund|1\s*dc\s*fund|1st\s*alliance|fratello|ascentra|luminar|kif\s*fund|greenbridge|arbitrage|jrg\s*capital|aurum|pdm|pfg|stashcap|stash\s*cap|merchadv|merchant\s*adv|lily\s*fund|mckenzie|purpletree|purple\s*tree|lexio|global\s*fund|advsyn|advance\s*syndicate|monetaria|trustify|bluetie|seamless\s*fund|liquidbee|belltower|palisade|marlin|xuper|fundfi|slim\s*fund|steady\s*fund)\b/i;

function parseLoanEntries(allLoans: any[]): LoanEntry[] {
  const seen = new Map<string, LoanEntry & { _analysisId?: number }>();
  for (const l of allLoans) {
    const rawName = (l.lender || l.name || "").trim();
    const name = stripACHPrefix(rawName) || rawName;
    if (!name || name.length < 3 || name.toLowerCase() === "unknown") continue;
    const knownLender = KNOWN_LENDER_SAFELIST.test(name);
    if (!knownLender && NOT_LOAN_KEYWORDS.test(name)) continue;
    if (!knownLender && NOT_LOAN_ANYWHERE.test(name)) continue;
    if (/^(on|to|at|in|of|for|the|payment|debit|credit|transfer|check|misc|authorized|withdrawal|withdraw):?\s*$/i.test(name)) continue;
    const shortName = shortLenderName(name);
    const amount = l.amount || 0;
    const occ = l.occurrences || 0;
    const lFundedAmt = l.fundedAmount || l.funded_amount || 0;
    const lFundedDate = l.fundedDate || l.funded_date || "";

    // 1. Try to find by short name and metadata (Funded Date/Amount)
    let bestMatch: (LoanEntry & { _analysisId?: number }) | null = null;

    for (const [key, entry] of seen) {
      const entryShort = shortLenderName(entry.lender || "");
      if (entryShort !== shortName) continue;

      // Rule A: Metadata match (Same funding info = Same loan)
      const eFundedAmt = entry.fundedAmount || 0;
      const eFundedDate = entry.fundedDate || "";
      if (lFundedAmt > 0 && eFundedAmt === lFundedAmt && lFundedDate && eFundedDate === lFundedDate) {
        bestMatch = entry;
        break;
      }

      // Rule B: Similarity match (Same frequency, similar payment)
      if (entry.frequency === (l.frequency || "unknown")) {
        const ratio = Math.max(amount, entry.amount) / Math.min(amount, entry.amount > 0 ? entry.amount : 1);
        if (ratio <= 1.45) { // Increased to 45% for high-fluctuation daily payments
          bestMatch = entry;
          break;
        }
      }
    }

    if (bestMatch) {
      bestMatch.occurrences = (bestMatch.occurrences || 0) + occ;
      if (!bestMatch.fundedAmount && lFundedAmt) bestMatch.fundedAmount = lFundedAmt;
      if (!bestMatch.fundedDate && lFundedDate) bestMatch.fundedDate = lFundedDate;
      // Use the higher payment amount to be conservative for underwriting
      if (amount > bestMatch.amount) bestMatch.amount = amount;
      
      const existingLower = (bestMatch.lender || "").toLowerCase().trim();
      const newLower = name.toLowerCase().trim();
      const newKnown = !!LENDER_SHORT_NAMES[newLower] || Object.keys(LENDER_SHORT_NAMES).some(k => newLower.includes(k) && k.length > 3);
      const existingKnown = !!LENDER_SHORT_NAMES[existingLower] || Object.keys(LENDER_SHORT_NAMES).some(k => existingLower.includes(k) && k.length > 3);
      if (newKnown && !existingKnown) bestMatch.lender = name;
    } else {
      const key = `${shortName}|${Math.round(amount)}|${lFundedAmt}|${lFundedDate}`;
      seen.set(key, { 
        lender: name, 
        amount, 
        frequency: l.frequency || "unknown", 
        fundedAmount: lFundedAmt || undefined, 
        fundedDate: lFundedDate || undefined, 
        account: l.account || undefined, 
        occurrences: occ, 
        possiblyPaidOff: l.possiblyPaidOff || false, 
        lastSeenMonth: l.lastSeenMonth || undefined, 
        lastSeenLabel: l.lastSeenLabel || undefined, 
        _analysisId: l._analysisId 
      });
    }
  }
  for (const entry of seen.values()) {
    const short = shortLenderName(entry.lender || "");
    if (LENDER_CANONICAL_NAMES[short]) {
      entry.lender = LENDER_CANONICAL_NAMES[short];
    }
  }

  const amountGroups = new Map<number, string[]>();
  for (const [key, entry] of seen) {
    const rounded = Math.round(entry.amount);
    if (!amountGroups.has(rounded)) amountGroups.set(rounded, []);
    amountGroups.get(rounded)!.push(key);
  }
  for (const [amount, keys] of amountGroups) {
    if (keys.length < 2) continue;
    const entries = keys.map(k => ({ key: k, entry: seen.get(k)! }));
    const withDirectLookup = entries.filter(e => {
      const lower = stripACHPrefix(e.entry.lender || "").toLowerCase().trim();
      return !!LENDER_SHORT_NAMES[lower] || Object.keys(LENDER_SHORT_NAMES).some(k => k.length > 5 && lower === k);
    });
    if (withDirectLookup.length >= 1 && withDirectLookup.length < entries.length) {
      for (const e of entries) {
        if (withDirectLookup.includes(e)) continue;
        const eLower = (e.entry.lender || "").toLowerCase();
        const isDescriptivePrefix = /^business\s+to\s+business/i.test(e.entry.lender || "") || eLower.length > 40;
        if (isDescriptivePrefix) {
          console.log(`[LoanDedup] Removing "${e.entry.lender}" $${amount} — same amount as "${withDirectLookup[0].entry.lender}", likely misattributed ACH description`);
          seen.delete(e.key);
        }
      }
    }
  }

  const MIN_DAILY_OCC = 5;
  const MIN_WEEKLY_OCC = 2;
  return [...seen.values()].filter(l => {
    if (l.amount <= 0) return false;
    const f = (l.frequency || "").toLowerCase();
    if (f === "bi-weekly" || f === "biweekly" || f === "bi weekly" || f === "semi-weekly") {
      l.frequency = "weekly";
    }
    if (l.frequency !== "daily" && l.frequency !== "weekly" && l.frequency !== "monthly") return false;
    const occ = l.occurrences || 0;
    const hasRecentFunding = !!(l.fundedAmount && l.fundedAmount > 0);
    if (l.frequency === "daily" && occ < MIN_DAILY_OCC) {
      if (hasRecentFunding && occ >= 1) {
      } else {
        return false;
      }
    }
    if (l.frequency === "weekly" && occ < MIN_WEEKLY_OCC) {
      if (hasRecentFunding && occ >= 1) {
      } else {
        return false;
      }
    }
    if (l.frequency === "monthly" && occ < 2) {
      if (hasRecentFunding && occ >= 1) {
      } else {
        return false;
      }
    }
    return true;
  });
}

function formatScrubData(revEntries: RevEntry[], loanEntries: LoanEntry[], format: string): string {
  if (revEntries.length === 0 && loanEntries.length === 0) return "no stmnts";

  const uniqueMonths = new Set<number>();
  for (const e of revEntries) uniqueMonths.add(e.sortKey);
  const sortedMonthKeys = [...uniqueMonths].sort((a, b) => b - a);

  const mostRecentKey = sortedMonthKeys[0];
  const mostRecentEntries = revEntries.filter(e => e.sortKey === mostRecentKey);
  mostRecentEntries.sort((a, b) => b.rev - a.rev);

  const parts: string[] = [];
  for (const e of mostRecentEntries) {
    parts.push(`${e.monthNum}-${shortK(e.rev)}`);
  }

  // Deduplicate and format loans
  const uniqueLoans = new Map<string, string>();
  for (const l of loanEntries) {
    if (l.possiblyPaidOff) continue;
    const short = shortLenderName(l.lender);
    
    let loanStr = "";
    if (l.fundedAmount && l.fundedDate) {
      const dm = l.fundedDate.match(/^(\d{4})-(\d{2})-?(\d{2})?/);
      if (dm) {
        const fMo = parseInt(dm[2]);
        const fDay = dm[3] ? parseInt(dm[3]) : 0;
        const fundedStr = loanAmtStr(l.fundedAmount, true);
        const datePrefix = fDay ? `${fMo}/${fDay}` : `${fMo}`;
        loanStr = `${datePrefix}-${short}-${fundedStr}`;
      } else {
        loanStr = `${short} ${loanAmtStr(l.amount)}`;
      }
    } else {
      loanStr = `${short} ${loanAmtStr(l.amount)}`;
      if (l.fundedAmount) {
        loanStr += `/f${loanAmtStr(l.fundedAmount, true)}`;
      }
    }

    // Only keep one entry per lender (prioritize funded info if multiple exist somehow)
    if (!uniqueLoans.has(short) || loanStr.includes("-")) {
      uniqueLoans.set(short, loanStr);
    }
  }

  parts.push(...uniqueLoans.values());

  const olderKeys = sortedMonthKeys.slice(1, 3);
  for (const key of olderKeys) {
    const entries = revEntries.filter(e => e.sortKey === key);
    entries.sort((a, b) => b.rev - a.rev);
    for (const e of entries) {
      parts.push(`${e.monthNum}-${shortK(e.rev)}`);
    }
  }

  const result = parts.length > 0 ? parts.join(" ") : "no stmnts";
  return result;
}

async function smartAutoAssign(approvalAmount: number, riskCategory: string | null): Promise<{ repId: number; repName: string } | null> {
  const reps = await db.select().from(usersTable)
    .where(and(eq(usersTable.role, "rep"), eq(usersTable.active, true), eq(usersTable.autoAssignEnabled, true)));

  if (reps.length === 0) return null;

  const risk = (riskCategory || "B1").toUpperCase();
  const isLowRisk = risk.startsWith("A");
  const isHighRisk = risk.startsWith("C") || risk === "B2";

  let eligible = reps.filter(r => {
    const pref = r.riskPreference || "any";
    if (pref === "low" && isHighRisk) return false;
    if (pref === "high" && isLowRisk) return false;

    if (r.minDealAmount != null) {
      const min = parseFloat(String(r.minDealAmount));
      if (!isNaN(min) && approvalAmount < min) return false;
    }
    if (r.maxDealAmount != null) {
      const max = parseFloat(String(r.maxDealAmount));
      if (!isNaN(max) && approvalAmount > max) return false;
    }

    return true;
  });

  if (eligible.length === 0) {
    eligible = reps.filter(r => {
      const pref = r.riskPreference || "any";
      if (pref === "low" && isHighRisk) return false;
      if (pref === "high" && isLowRisk) return false;
      return true;
    });
  }

  if (eligible.length === 0) {
    eligible = reps.filter(r => {
      const pref = r.riskPreference || "any";
      return pref === "any";
    });
  }

  if (eligible.length === 0) eligible = reps;

  const tierOrder: Record<string, number> = { top: 0, mid: 1, standard: 2 };
  eligible.sort((a, b) => (tierOrder[a.repTier || "standard"] || 2) - (tierOrder[b.repTier || "standard"] || 2));

  if (approvalAmount >= 300000) {
    const topReps = eligible.filter(r => (r.repTier || "standard") === "top");
    if (topReps.length > 0) eligible = topReps;
  }

  const repCounts = await Promise.all(eligible.map(async (rep) => {
    const [c] = await db.select({ count: count() }).from(leadsTable)
      .where(and(eq(leadsTable.assignedToId, rep.id), sql`${leadsTable.status} NOT IN ('funded', 'not_interested', 'dead')`));
    return { ...rep, activeLeads: Number(c.count) };
  }));

  repCounts.sort((a, b) => a.activeLeads - b.activeLeads);

  const chosen = repCounts[0];
  return { repId: chosen.id, repName: chosen.fullName };
}

router.get("/underwriting/review-queue", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    const leadsWithAnalysis = await db.select({
      leadId: leadsTable.id, businessName: leadsTable.businessName, ownerName: leadsTable.ownerName,
      phone: leadsTable.phone, status: leadsTable.status, riskCategory: leadsTable.riskCategory,
      hasExistingLoans: leadsTable.hasExistingLoans, loanCount: leadsTable.loanCount,
      avgDailyBalance: leadsTable.avgDailyBalance, revenueTrend: leadsTable.revenueTrend,
      grossRevenue: leadsTable.grossRevenue, monthlyRevenue: leadsTable.monthlyRevenue,
      requestedAmount: leadsTable.requestedAmount, assignedToId: leadsTable.assignedToId,
      estimatedApproval: leadsTable.estimatedApproval,
      createdAt: leadsTable.createdAt,
    })
    .from(leadsTable)
    .innerJoin(bankStatementAnalysesTable, eq(bankStatementAnalysesTable.leadId, leadsTable.id))
    .where(sql`${leadsTable.status} = 'underwriting'`)
    .orderBy(sql`${bankStatementAnalysesTable.createdAt} DESC`);

    const uniqueLeads = Array.from(new Map(leadsWithAnalysis.map(l => [l.leadId, l])).values());

    const enriched = await Promise.all(uniqueLeads.map(async (lead) => {
      const pendingConfirmations = await db.select({ count: sql<number>`count(*)` })
        .from(underwritingConfirmationsTable)
        .where(and(eq(underwritingConfirmationsTable.leadId, lead.leadId), eq(underwritingConfirmationsTable.status, "pending")));
      return { ...lead, pendingFindings: Number(pendingConfirmations[0]?.count || 0) };
    }));

    res.json(enriched);
  } catch (e: any) { res.status(500).json({ error: e.message }); }
});

router.post("/underwriting/approve-and-distribute", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const { leadIds, repId } = req.body;
    if (!leadIds || !Array.isArray(leadIds) || leadIds.length === 0) {
      res.status(400).json({ error: "leadIds array is required" }); return;
    }

    let targetRepId = repId;
    if (targetRepId) {
      const [targetRep] = await db.select().from(usersTable)
        .where(and(eq(usersTable.id, targetRepId), eq(usersTable.active, true)));
      if (!targetRep) { res.status(400).json({ error: "Target rep not found or inactive" }); return; }
    }

    if (!targetRepId) {
      const reps = await db.select().from(usersTable)
        .where(and(eq(usersTable.role, "rep"), eq(usersTable.active, true)));
      if (reps.length === 0) { res.status(400).json({ error: "No active sales reps found" }); return; }

      const repLeadCounts = await Promise.all(reps.map(async (rep) => {
        const count = await db.select({ count: sql<number>`count(*)` }).from(leadsTable)
          .where(eq(leadsTable.assignedToId, rep.id));
        return { repId: rep.id, count: Number(count[0]?.count || 0) };
      }));
      repLeadCounts.sort((a, b) => a.count - b.count);
      targetRepId = repLeadCounts[0].repId;
    }

    const uniqueLeadIds = [...new Set(leadIds.map(Number).filter(Boolean))];
    let assigned = 0;
    const leadNames: string[] = [];
    for (const leadId of uniqueLeadIds) {
      const [lead] = await db.select({ businessName: leadsTable.businessName }).from(leadsTable).where(eq(leadsTable.id, leadId));
      if (!lead) continue;
      leadNames.push(lead.businessName);
      await db.update(leadsTable).set({ assignedToId: targetRepId, status: "contacted" }).where(eq(leadsTable.id, leadId));
      assigned++;
    }

    if (assigned === 0) { res.status(400).json({ error: "No valid leads found to assign" }); return; }

    const [rep] = await db.select().from(usersTable).where(eq(usersTable.id, targetRepId));

    await db.insert(notificationsTable).values({
      userId: targetRepId, type: "leads_assigned",
      title: `${assigned} New Lead${assigned > 1 ? "s" : ""} Assigned!`,
      message: assigned <= 3 ? `You got: ${leadNames.join(", ")}. Time to close!` : `${assigned} hot leads just dropped in your queue. Let's get it!`,
      link: "/leads",
    });

    res.json({ assigned, repId: targetRepId, repName: rep?.fullName || "Unknown" });
  } catch (e: any) { res.status(500).json({ error: e.message }); }
});

router.get("/underwriting/review/:leadId", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const leadId = parseInt(String(req.params.leadId), 10);
    const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!lead) { res.status(404).json({ error: "Lead not found" }); return; }

    const documents = await db.select().from(documentsTable).where(eq(documentsTable.leadId, leadId));
    const bankStatements = documents.filter(d => d.type === "bank_statement" || d.classifiedType === "bank_statement");
    const analyses = await db.select().from(bankStatementAnalysesTable)
      .where(eq(bankStatementAnalysesTable.leadId, leadId)).orderBy(sql`${bankStatementAnalysesTable.createdAt} DESC`);
    const confirmations = await db.select().from(underwritingConfirmationsTable)
      .where(eq(underwritingConfirmationsTable.leadId, leadId)).orderBy(sql`${underwritingConfirmationsTable.createdAt} ASC`);
    const existingDeals = await db.select().from(dealsTable).where(eq(dealsTable.leadId, leadId));
    const activeReps = await db.select({ id: usersTable.id, fullName: usersTable.fullName, email: usersTable.email, role: usersTable.role })
      .from(usersTable).where(and(eq(usersTable.active, true), sql`${usersTable.role} IN ('rep', 'admin', 'manager', 'super_admin')`));

    const decrypted = decryptLeadFields(lead);
    const user = (req as any).user;
    logSecurityEvent("pii_data_accessed", "info",
      `User ${user.email} viewed underwriting review for lead #${leadId}`, {
        userId: user.id, req,
        metadata: { leadId, businessName: lead.businessName, accessType: "underwriting_review" },
      }).catch(() => {});
    const accountNumbers = new Set<string>();
    if (decrypted.accountNumber) accountNumbers.add(decrypted.accountNumber);
    for (const doc of bankStatements) {
      const acctMatch = (doc.name?.toLowerCase() || "").match(/acct?\s*#?\s*(\d{4,})/i);
      if (acctMatch) accountNumbers.add(acctMatch[1]);
    }

    let totalDeposits = 0, totalLoanPayments = 0;
    const allMonthlyRevenues: any[] = [], allNegativeDays: any[] = [];
    let totalNsfCount = 0;

    for (const a of analyses) {
      if (a.grossRevenue) totalDeposits += parseNumeric(a.grossRevenue);
      for (const loan of ((a.loanDetails as any[]) || [])) {
        if (loan.amount && (loan.frequency === "daily" || loan.frequency === "weekly" || loan.frequency === "monthly")) {
          let m = parseNumeric(loan.amount);
          if (loan.frequency === "daily") m *= 22;
          else if (loan.frequency === "weekly") m *= 4;
          else if (loan.frequency === "monthly") m *= 1;
          totalLoanPayments += m;
        }
      }
      let acctLast4 = "";
      if (a.aiRawAnalysis) {
        try {
          const raw = parseAIResponse(a.aiRawAnalysis);
          if (raw.accountNumber) acctLast4 = String(raw.accountNumber).slice(-4);
        } catch {}
      }
      for (const mr of ((a.monthlyRevenues as any[]) || [])) {
        const mrAcctFallback = (mr.account || "").replace(/[\s-]/g, "").slice(-4);
        allMonthlyRevenues.push({ ...mr, acctLast4: acctLast4 || mrAcctFallback || undefined });
      }
      allNegativeDays.push(...((a.negativeDays as any[]) || []));
      totalNsfCount += (a.nsfCount as number) || 0;
    }

    const trueGross = totalDeposits - totalLoanPayments;
    const holdbackPct = totalDeposits > 0 ? Math.round((totalLoanPayments / totalDeposits) * 100) : 0;

    const allTransactions: any[] = [];
    const latestAnalysis = analyses[0];
    if (latestAnalysis?.aiRawAnalysis) {
      try {
        const raw = parseAIResponse(latestAnalysis.aiRawAnalysis);
        if (Array.isArray(raw.notableTransactions)) allTransactions.push(...raw.notableTransactions);
      } catch {}
    }

    res.json({
      lead: {
        id: lead.id, businessName: lead.businessName, dba: lead.dba, ownerName: lead.ownerName,
        email: lead.email, phone: lead.phone, status: lead.status, creditScore: lead.creditScore,
        ssn: maskSsn(lead.ssn), requestedAmount: lead.requestedAmount,
        monthlyRevenue: lead.monthlyRevenue, industry: lead.industry, businessType: lead.businessType,
        yearsInBusiness: lead.yearsInBusiness, state: lead.state, city: lead.city,
        riskCategory: lead.riskCategory, avgDailyBalance: lead.avgDailyBalance,
        revenueTrend: lead.revenueTrend, grossRevenue: lead.grossRevenue,
        hasExistingLoans: lead.hasExistingLoans, loanCount: lead.loanCount,
        hasOnDeck: lead.hasOnDeck, assignedToId: lead.assignedToId,
        estimatedApproval: lead.estimatedApproval,
        bankStatementsStatus: lead.bankStatementsStatus, ein: decrypted.ein, dob: decrypted.dob,
      },
      bankStatements: bankStatements.map(bs => ({ id: bs.id, name: bs.name, url: bs.url, type: bs.type, classifiedType: bs.classifiedType, createdAt: bs.createdAt })),
      analyses, confirmations, existingDeals,
      accountNumbers: Array.from(accountNumbers),
      financialSummary: {
        totalDeposits, totalLoanPayments, trueGross, holdbackPct,
        monthlyRevenues: allMonthlyRevenues, avgDailyBalance: lead.avgDailyBalance || 0,
        negativeDays: allNegativeDays, negativeDayCount: allNegativeDays.length, nsfCount: totalNsfCount,
        estimatedApproval: lead.estimatedApproval || 0,
        hasNegativeBalance: allNegativeDays.length > 0,
        lowestBalance: allNegativeDays.length > 0 ? Math.min(...allNegativeDays.map((d: any) => d.endingBalance || 0)) : null,
      },
      notableTransactions: allTransactions,
      activeReps: activeReps.map(r => ({ id: r.id, fullName: r.fullName, email: r.email, role: r.role })),
    });
  } catch (e: any) { console.error("Underwriting review error:", e); res.status(500).json({ error: e.message }); }
});

router.post("/underwriting/approve", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const { leadId, approvalAmount, term, factorRate, repId, notes, fundingSource, funderName } = req.body;
    const user = (req as any).user;
    if (!leadId || !approvalAmount || !term) { res.status(400).json({ error: "leadId, approvalAmount, and term are required" }); return; }

    const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!lead) { res.status(404).json({ error: "Lead not found" }); return; }

    const rate = factorRate || 1.49;
    const paybackAmount = Math.round(approvalAmount * rate * 100) / 100;
    const dailyPayment = Math.round((paybackAmount / term) * 100) / 100;

    let targetRepId = repId;
    let autoAssigned = false;
    let assignedRepName = "";

    if (targetRepId) {
      const [rep] = await db.select().from(usersTable).where(and(eq(usersTable.id, targetRepId), eq(usersTable.active, true)));
      if (!rep) { res.status(400).json({ error: "Rep not found or inactive" }); return; }
      assignedRepName = rep.fullName;
    } else {
      const assignment = await smartAutoAssign(approvalAmount, lead.riskCategory);
      if (assignment) {
        targetRepId = assignment.repId;
        assignedRepName = assignment.repName;
        autoAssigned = true;
      }
    }

    let deal = null;
    if (targetRepId) {
      const isInHouse = !fundingSource || fundingSource === "in_house";
      const [createdDeal] = await db.insert(dealsTable).values({
        leadId, repId: targetRepId, stage: "approved", amount: String(approvalAmount),
        factorRate: String(rate), paybackAmount: String(paybackAmount), term, paymentFrequency: "daily", paymentAmount: String(dailyPayment),
        totalPayments: term, fundingSource: isInHouse ? "in_house" : "out_house",
        funderName: isInHouse ? null : (funderName || null),
        notes: notes || `Approved by ${user.fullName} - ${approvalAmount} over ${term} days at ${rate} factor rate`,
      }).returning();
      deal = createdDeal;

      await db.insert(notificationsTable).values({
        userId: targetRepId, type: "deal_approved", title: "New Approved Deal!",
        message: `${lead.businessName} approved for $${approvalAmount.toLocaleString()} - ${term} days at ${rate} factor.${autoAssigned ? " (Auto-assigned)" : ""}`,
        link: "/deals",
      });

      const [assignedRep] = await db.select().from(usersTable).where(eq(usersTable.id, targetRepId));
      if (assignedRep?.googleSheetUrl) {
        appendToRepSheet(assignedRep.googleSheetUrl, assignedRep.googleSheetTab, {
          businessName: lead.businessName, ownerName: lead.ownerName, phone: lead.phone,
          email: lead.email, approvalAmount, term, factorRate: rate, paybackAmount, dailyPayment,
          riskCategory: lead.riskCategory, grossRevenue: parseNumeric(lead.grossRevenue),
          monthlyRevenue: parseNumeric(lead.monthlyRevenue), industry: lead.industry, state: lead.state,
          notes: notes || null,
        }).catch(err => console.error("Sheet append failed:", err));
      }
    }

    await db.update(leadsTable).set({
      status: "approved", estimatedApproval: approvalAmount,
      ...(targetRepId ? { assignedToId: targetRepId } : {}),
      notes: lead.notes ? `${lead.notes}\n\nAPPROVED: $${approvalAmount} / ${term} days / ${rate} factor rate`
        : `APPROVED: $${approvalAmount} / ${term} days / ${rate} factor rate`,
    }).where(eq(leadsTable.id, leadId));

    res.json({
      deal, paybackAmount, dailyPayment, assignedToRep: !!targetRepId,
      autoAssigned, assignedRepName,
      message: targetRepId
        ? `Deal approved and ${autoAssigned ? "auto-" : ""}assigned to ${assignedRepName}`
        : "Lead approved — no eligible reps found for auto-assignment",
    });
  } catch (e: any) { console.error("Underwriting approve error:", e); res.status(500).json({ error: e.message }); }
});

router.get("/underwriting/funder-names", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const results = await db.selectDistinct({ funderName: dealsTable.funderName }).from(dealsTable)
      .where(and(eq(dealsTable.fundingSource, "out_house"), sql`${dealsTable.funderName} IS NOT NULL AND ${dealsTable.funderName} != ''`));
    res.json(results.map(r => r.funderName).filter(Boolean));
  } catch (e: any) { res.status(500).json({ error: e.message }); }
});

router.post("/underwriting/deny", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const { leadId, reason } = req.body;
    if (!leadId) { res.status(400).json({ error: "leadId is required" }); return; }
    const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!lead) { res.status(404).json({ error: "Lead not found" }); return; }

    await db.update(leadsTable).set({
      status: "dead", notes: lead.notes ? `${lead.notes}\n\nDENIED: ${reason || "No reason provided"}` : `DENIED: ${reason || "No reason provided"}`,
    }).where(eq(leadsTable.id, leadId));

    if (lead.assignedToId) {
      await db.insert(notificationsTable).values({
        userId: lead.assignedToId, type: "lead_denied", title: "Lead Denied",
        message: `${lead.businessName} was denied underwriting.${reason ? ` Reason: ${reason}` : ""}`, link: "/leads",
      });
    }
    res.json({ success: true });
  } catch (e: any) { res.status(500).json({ error: e.message }); }
});

router.post("/underwriting/mark-deposit", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const { leadId, analysisId, depositDescription, depositAmount, depositDate, markAs, adminNotes } = req.body;
    const user = (req as any).user;
    if (!leadId || !analysisId || !markAs) { res.status(400).json({ error: "leadId, analysisId, and markAs are required" }); return; }
    if (!["loan", "authentic"].includes(markAs)) { res.status(400).json({ error: "markAs must be 'loan' or 'authentic'" }); return; }

    const maxIndex = await db.select({ max: sql<number>`COALESCE(MAX(${underwritingConfirmationsTable.findingIndex}), -1)` })
      .from(underwritingConfirmationsTable).where(eq(underwritingConfirmationsTable.analysisId, analysisId));

    const [lead] = await db.select({ businessName: leadsTable.businessName }).from(leadsTable).where(eq(leadsTable.id, leadId));
    const [confirmation] = await db.insert(underwritingConfirmationsTable).values({
      analysisId, leadId, leadBusinessName: lead?.businessName || null,
      findingType: markAs === "loan" ? "loan" : "authentic_deposit",
      findingIndex: (Number(maxIndex[0]?.max) || 0) + 1,
      originalValue: { description: depositDescription || "Manual mark", amount: depositAmount || 0, date: depositDate || null, markedAs: markAs },
      status: "confirmed", adminLabel: markAs, adminNotes: adminNotes || `Manually marked as ${markAs} by manager`,
      confirmedById: user.id, confirmedAt: new Date(),
    }).returning();
    res.json({ confirmation });
  } catch (e: any) { res.status(500).json({ error: e.message }); }
});

router.post("/underwriting/correct-deposit", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const { leadId, analysisId, month, account, correctedAmount } = req.body;
    if (!leadId || !analysisId || !month || correctedAmount === undefined) {
      res.status(400).json({ error: "leadId, analysisId, month, and correctedAmount are required" }); return;
    }

    if (typeof correctedAmount !== "number" || !isFinite(correctedAmount) || correctedAmount <= 0) {
      res.status(400).json({ error: "correctedAmount must be a positive number" }); return;
    }

    const [analysis] = await db.select().from(bankStatementAnalysesTable)
      .where(and(eq(bankStatementAnalysesTable.id, analysisId), eq(bankStatementAnalysesTable.leadId, leadId)));
    if (!analysis) { res.status(404).json({ error: "Analysis not found for this lead" }); return; }

    const revenues = (analysis.monthlyRevenues as any[]) || [];
    let found = false;
    for (const rev of revenues) {
      const revMonth = (rev.month || "").toLowerCase().trim();
      const targetMonth = month.toLowerCase().trim();
      const revAcct = (rev.account || "").replace(/\D/g, "").slice(-4);
      const targetAcct = (account || "").replace(/\D/g, "").slice(-4);

      if (revMonth === targetMonth && (!targetAcct || !revAcct || revAcct === targetAcct)) {
        rev.revenue = correctedAmount;
        rev.rev = correctedAmount;
        rev.needsReview = false;
        rev.reviewResolved = true;
        rev.correctedBy = (req as any).user?.fullName || "admin";
        rev.correctedAt = new Date().toISOString();
        found = true;
        break;
      }
    }

    if (!found) { res.status(404).json({ error: "Monthly revenue entry not found" }); return; }

    await db.update(bankStatementAnalysesTable)
      .set({ monthlyRevenues: revenues })
      .where(eq(bankStatementAnalysesTable.id, analysisId));

    const allAnalyses = await db.select().from(bankStatementAnalysesTable).where(eq(bankStatementAnalysesTable.leadId, leadId));
    let totalDeposits = 0;
    let totalMonths = 0;
    for (const a of allAnalyses) {
      for (const mr of ((a.monthlyRevenues as any[]) || [])) {
        totalDeposits += parseNumeric(mr.revenue || mr.rev);
        totalMonths++;
      }
    }
    const avgMonthly = totalMonths > 0 ? totalDeposits / totalMonths : 0;
    await db.update(leadsTable).set({ grossRevenue: String(parseNumeric(totalDeposits)), monthlyRevenue: String(parseNumeric(avgMonthly)) }).where(eq(leadsTable.id, leadId));

    console.log(`[DepositCorrection] Lead ${leadId}: ${month} corrected to $${correctedAmount} by ${(req as any).user?.fullName}`);
    res.json({ success: true });
  } catch (e: any) { res.status(500).json({ error: e.message }); }
});

router.get("/underwriting/missing-statements", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    const leads = await db.select({
      id: leadsTable.id, businessName: leadsTable.businessName, ownerName: leadsTable.ownerName,
      phone: leadsTable.phone, email: leadsTable.email, status: leadsTable.status,
      bankStatementsStatus: leadsTable.bankStatementsStatus, assignedToId: leadsTable.assignedToId,
      createdAt: leadsTable.createdAt, requestedAmount: leadsTable.requestedAmount,
    }).from(leadsTable)
    .where(sql`${leadsTable.status} IN ('new', 'contacted', 'underwriting') AND (${leadsTable.bankStatementsStatus} IS NULL OR ${leadsTable.bankStatementsStatus} IN ('none', 'partial', 'incomplete'))`)
    .orderBy(sql`${leadsTable.createdAt} DESC`);

    const docCounts = await db.select({ leadId: documentsTable.leadId, count: sql<number>`count(*)` })
      .from(documentsTable)
      .where(sql`${documentsTable.type} = 'bank_statement' OR ${documentsTable.classifiedType} = 'bank_statement'`)
      .groupBy(documentsTable.leadId);

    const countMap = new Map(docCounts.map(d => [d.leadId, Number(d.count)]));
    const missing = leads.filter(l => (countMap.get(l.id) || 0) < 2).map(l => ({ ...l, bankStatementCount: countMap.get(l.id) || 0 }));

    const reps = await db.select({ id: usersTable.id, name: usersTable.fullName })
      .from(usersTable).where(and(eq(usersTable.active, true), sql`${usersTable.role} IN ('rep', 'admin', 'super_admin')`));
    res.json({ leads: missing, reps });
  } catch (e: any) { res.status(500).json({ error: e.message }); }
});

router.post("/underwriting/assign-for-statements", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const { leadId, repId } = req.body;
    if (!leadId || !repId) { res.status(400).json({ error: "leadId and repId required" }); return; }
    const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!lead) { res.status(404).json({ error: "Lead not found" }); return; }

    await db.update(leadsTable).set({ assignedToId: repId, bankStatementsStatus: "requested" }).where(eq(leadsTable.id, leadId));
    await db.insert(notificationsTable).values({
      userId: repId, type: "statements_needed", title: "Bank Statements Needed",
      message: `Call ${lead.businessName} (${lead.ownerName}) and get their latest bank statements. Phone: ${lead.phone}`,
      link: `/leads/${leadId}`,
    });
    res.json({ success: true });
  } catch (e: any) { res.status(500).json({ error: e.message }); }
});

router.get("/underwriting/stats", requireAuth, async (req, res): Promise<void> => {
  const [total, confirmed, rejected, relabeled, pending] = await Promise.all([
    db.select({ count: sql<number>`count(*)` }).from(underwritingConfirmationsTable),
    db.select({ count: sql<number>`count(*)` }).from(underwritingConfirmationsTable).where(eq(underwritingConfirmationsTable.status, "confirmed")),
    db.select({ count: sql<number>`count(*)` }).from(underwritingConfirmationsTable).where(eq(underwritingConfirmationsTable.status, "rejected")),
    db.select({ count: sql<number>`count(*)` }).from(underwritingConfirmationsTable).where(eq(underwritingConfirmationsTable.status, "relabeled")),
    db.select({ count: sql<number>`count(*)` }).from(underwritingConfirmationsTable).where(eq(underwritingConfirmationsTable.status, "pending")),
  ]);

  const t = Number(total[0]?.count), p = Number(pending[0]?.count), c = Number(confirmed[0]?.count);
  const accuracy = (t - p) > 0 ? Math.round((c / (t - p)) * 100) : 0;

  res.json({
    totalFindings: t, confirmed: c, rejected: Number(rejected[0]?.count),
    relabeled: Number(relabeled[0]?.count), pending: p, accuracy,
    learningDataPoints: c + Number(rejected[0]?.count) + Number(relabeled[0]?.count),
  });
});

router.post("/underwriting/ai-chat", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const { leadId, message, conversationHistory, selectedText } = req.body;
    const user = (req as any).user;
    if (!leadId || !message) { res.status(400).json({ error: "leadId and message required" }); return; }

    const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!lead) { res.status(404).json({ error: "Lead not found" }); return; }

    const analyses = await db.select().from(bankStatementAnalysesTable)
      .where(eq(bankStatementAnalysesTable.leadId, leadId))
      .orderBy(sql`${bankStatementAnalysesTable.createdAt} DESC`);

    const confirmations = await db.select().from(underwritingConfirmationsTable)
      .where(eq(underwritingConfirmationsTable.leadId, leadId));

    const documents = await db.select().from(documentsTable)
      .where(eq(documentsTable.leadId, leadId));
    const bankStatements = documents.filter(d => d.type === "bank_statement" || d.classifiedType === "bank_statement");

    const { extractTextFromDocument, getLearningContext } = await import("./coreController");
    const learningContext = await getLearningContext();

    let statementTexts = "";
    for (const bs of bankStatements.slice(0, 5)) {
      if (bs.url) {
        const text = await extractTextFromDocument(bs.url, bs.storageKey);
        if (text && !text.startsWith("[")) {
          statementTexts += `\n--- Statement: ${bs.name} ---\n${text.slice(0, 8000)}\n`;
        }
      }
    }

    let analysisContext = "";
    if (analyses.length > 0) {
      const latest = analyses[0];
      analysisContext = `\nAI ANALYSIS RESULTS:\n`;
      analysisContext += `Risk Score: ${latest.riskScore}\n`;
      analysisContext += `Gross Revenue: $${latest.grossRevenue}\n`;
      analysisContext += `Avg Daily Balance: $${latest.avgDailyBalance}\n`;
      analysisContext += `NSF Count: ${latest.nsfCount}\n`;
      analysisContext += `Revenue Trend: ${latest.revenueTrend}\n`;
      if (latest.loanDetails) analysisContext += `Detected Loans: ${JSON.stringify(latest.loanDetails)}\n`;
      if (latest.monthlyRevenues) analysisContext += `Monthly Revenues: ${JSON.stringify(latest.monthlyRevenues)}\n`;
      if (latest.negativeDays) analysisContext += `Negative Days: ${JSON.stringify(latest.negativeDays)}\n`;
      if (latest.aiRawAnalysis) {
        try {
          const raw = parseAIResponse(latest.aiRawAnalysis);
          if (raw.notableTransactions) analysisContext += `Notable Transactions: ${JSON.stringify(raw.notableTransactions)}\n`;
          if (raw.summary) analysisContext += `Summary: ${raw.summary}\n`;
          if (raw.verificationNotes) analysisContext += `Verification Notes: ${raw.verificationNotes}\n`;
        } catch {}
      }
    }

    let confirmationContext = "";
    if (confirmations.length > 0) {
      confirmationContext = `\nMANAGER CONFIRMATIONS/CORRECTIONS:\n`;
      for (const c of confirmations) {
        confirmationContext += `- Finding: ${c.findingType}, Status: ${c.status}`;
        if (c.adminLabel) confirmationContext += `, Label: ${c.adminLabel}`;
        if (c.adminNotes) confirmationContext += `, Notes: ${c.adminNotes}`;
        confirmationContext += `\n`;
      }
    }

    const systemPrompt = `You are an expert cash advance underwriting AI assistant working alongside a manager named ${user.fullName}.

You are currently helping review the bank statements for:
Business: ${lead.businessName}
Owner: ${lead.ownerName}
${lead.creditScore ? `Credit Score: ${lead.creditScore}` : ""}
${lead.monthlyRevenue ? `Reported Monthly Revenue: $${lead.monthlyRevenue}` : ""}
${lead.requestedAmount ? `Requested Amount: $${lead.requestedAmount}` : ""}
${lead.industry ? `Industry: ${lead.industry}` : ""}
${lead.state ? `State: ${lead.state}` : ""}
${analysisContext}
${confirmationContext}
${learningContext}

BANK STATEMENT DATA:
${statementTexts || "[No statement text extracted yet]"}

YOUR ROLE:
- You are the manager's AI co-pilot during underwriting
- Help identify and explain transactions, loans, revenue patterns, and risk factors
- When the manager highlights text from the statement, analyze it and explain what it means
- Be specific about dollar amounts, dates, and transaction descriptions
- If the manager corrects you or teaches you something, acknowledge it and learn
- Provide your reasoning clearly so the manager can verify
- Flag anything suspicious or noteworthy
- Help calculate figures like true gross revenue, holdback percentages, etc.
- Be conversational and helpful, not robotic
- If asked about a specific transaction, look it up in the statement data
- When the manager asks "what do you see" or similar, give a clear summary of key findings

FORMAT: Use markdown for readability. Bold key numbers and findings. Use bullet points for lists.`;

    const { callClaudeWithHistory } = await import("../ai/helpersController");

    const messages: Array<{ role: "user" | "assistant"; content: string }> = [];
    if (Array.isArray(conversationHistory)) {
      for (const msg of conversationHistory.slice(-12)) {
          messages.push({ role: msg.role as "user" | "assistant", content: String(msg.content || "").slice(0, 4000) });
      }
    }

    let userMessage = String(message || "");
    if (selectedText) {
      userMessage = `[The manager highlighted this text from the bank statement: "${selectedText}"]\n\n${userMessage}`;
    }
    messages.push({ role: "user", content: userMessage.slice(0, 4000) });

    const reply = await callClaudeWithHistory(systemPrompt, messages, { maxTokens: 4096 });

    res.json({ reply: reply || "I couldn't generate a response. Please try again." });
  } catch (e: any) {
    console.error("Underwriting AI chat error:", e);
    res.status(500).json({ error: "AI assistant is temporarily unavailable." });
  }
});

router.get("/underwriting/statement-text/:leadId", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const leadId = parseInt(String(req.params.leadId), 10);

    const analyses = await db.select().from(bankStatementAnalysesTable)
      .where(eq(bankStatementAnalysesTable.leadId, leadId))
      .orderBy(bankStatementAnalysesTable.createdAt);

    const storedText = analyses
      .map(a => a.extractedStatementText)
      .filter(Boolean)
      .join("\n");

    if (storedText) {
      const sections = storedText.split(/---\s+(.+?)\s+---/).filter(Boolean);
      const results = [];
      for (let i = 0; i < sections.length; i += 2) {
        const name = sections[i]?.trim() || `Statement ${Math.floor(i / 2) + 1}`;
        let text = sections[i + 1]?.trim() || "";
        text = text.replace(/^\[PDF:\s*\d+\s*pages?\]\s*/i, "").trim();
        const hasText = !!text && text.length > 20 && !text.startsWith("[");
        results.push({
          id: i / 2,
          name,
          url: "",
          text,
          hasText,
        });
      }
      if (results.length > 0) {
        res.json(results);
        return;
      }
    }

    const documents = await db.select().from(documentsTable)
      .where(eq(documentsTable.leadId, leadId));
    const bankStatements = documents.filter(d => d.type === "bank_statement" || d.classifiedType === "bank_statement");

    const { extractTextFromDocument } = await import("./coreController");

    const results = [];
    for (const bs of bankStatements) {
      let text = "";
      if (bs.url) {
        try {
          text = await extractTextFromDocument(bs.url, bs.storageKey);
        } catch {
          text = "";
        }
      }
      const cleaned = (text || "").replace(/^\[PDF:\s*\d+\s*pages?\]\s*/i, "").trim();
      results.push({
        id: bs.id,
        name: bs.name,
        url: bs.url,
        text: cleaned,
        hasText: !!cleaned && cleaned.length > 20,
      });
    }

    res.json(results);
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

router.get("/scrubbing/queue", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    const cleanupResult = await db.execute(sql`
      UPDATE leads SET status = 'new', sheet_writeback_status = NULL,
        bank_statements_status = NULL, risk_category = NULL, gross_revenue = NULL,
        avg_daily_balance = NULL, revenue_trend = NULL, has_existing_loans = NULL,
        loan_count = NULL, has_on_deck = NULL, estimated_approval = NULL
      WHERE status IN ('scrubbing_review', 'scrubbed', 'scrubbing')
      AND NOT EXISTS (SELECT 1 FROM bank_statement_analyses bsa WHERE bsa.lead_id = leads.id)
      AND NOT EXISTS (SELECT 1 FROM documents d WHERE d.lead_id = leads.id AND d.type = 'bank_statement')
    `);
    const cleanedCount = (cleanupResult as any)?.rowCount || 0;
    if (cleanedCount > 0) console.log(`[Queue] Auto-cleaned ${cleanedCount} stale leads (no DB records) back to new`);


    const leadsWithAnalysis = await db.execute(sql`
      SELECT DISTINCT ON (l.id)
        l.id as lead_id, l.business_name, l.owner_name, l.phone, l.status,
        l.risk_category, l.gross_revenue, l.monthly_revenue,
        l.has_existing_loans, l.loan_count, l.avg_daily_balance,
        l.revenue_trend, l.estimated_approval, l.assigned_to_id,
        l.created_at, l.sheet_writeback_status,
        bsa.id as analysis_id, bsa.has_loans, bsa.gross_revenue as bsa_gross,
        bsa.risk_score, bsa.nsf_count, bsa.created_at as analyzed_at,
        (SELECT COUNT(*)::int FROM underwriting_confirmations uc WHERE uc.lead_id = l.id AND uc.status = 'pending') as pending_count,
        (SELECT COUNT(*)::int FROM underwriting_confirmations uc WHERE uc.lead_id = l.id) as total_findings,
        (SELECT COUNT(*)::int FROM underwriting_confirmations uc WHERE uc.lead_id = l.id AND uc.status IN ('confirmed', 'rejected', 'relabeled')) as reviewed_count
      FROM leads l
      INNER JOIN bank_statement_analyses bsa ON bsa.lead_id = l.id
      ORDER BY l.id, bsa.created_at DESC
    `);

    const rows = ((leadsWithAnalysis as any).rows || leadsWithAnalysis);

    const needsReview = rows.filter((r: any) => r.status === "scrubbing_review");
    const scrubbed = rows.filter((r: any) => r.status === "scrubbed" && (!r.sheet_writeback_status || r.sheet_writeback_status === "written"));
    const sheetFailed = rows.filter((r: any) => r.status === "scrubbed" && r.sheet_writeback_status && r.sheet_writeback_status !== "written");

    const pendingLeadsResult = await db.execute(sql`
      SELECT l.id as lead_id, l.business_name, l.owner_name, l.phone, l.status,
        (SELECT COUNT(*)::int FROM documents d WHERE d.lead_id = l.id AND d.type = 'bank_statement') as doc_count
      FROM leads l
      WHERE EXISTS (SELECT 1 FROM documents d WHERE d.lead_id = l.id AND d.type = 'bank_statement')
      AND NOT EXISTS (SELECT 1 FROM bank_statement_analyses bsa WHERE bsa.lead_id = l.id)
      ORDER BY l.created_at DESC
    `);
    const pendingRows = ((pendingLeadsResult as any).rows || pendingLeadsResult);
    const pendingLeads = pendingRows.map((r: any) => ({
      leadId: r.lead_id, businessName: r.business_name, ownerName: r.owner_name,
      phone: r.phone, status: r.status, docCount: r.doc_count,
    }));

    const batches = await db.select().from(uploadBatchesTable)
      .where(eq(uploadBatchesTable.status, "completed"));
    const unmatchedItems: any[] = [];
    for (const batch of batches) {
      const folders = (batch.unmatchedFolders as string[]) || [];
      if (folders.length > 0) {
        for (const folderName of folders) {
          unmatchedItems.push({
            batchId: batch.id,
            fileName: batch.fileName,
            folderName,
            sourceTier: batch.sourceTier,
            importedAt: batch.createdAt,
            extractDir: batch.extractDir,
          });
        }
      }
    }

    const mapRow = (r: any) => ({
      leadId: r.lead_id, businessName: r.business_name, ownerName: r.owner_name,
      phone: r.phone, status: r.status, riskCategory: r.risk_category,
      grossRevenue: parseNumeric(r.gross_revenue || r.bsa_gross),
      monthlyRevenue: parseNumeric(r.monthly_revenue),
      hasLoans: r.has_existing_loans, loanCount: r.loan_count || 0,
      avgDailyBalance: parseNumeric(r.avg_daily_balance),
      riskScore: r.risk_score, nsfCount: r.nsf_count || 0,
      pendingFindings: r.pending_count, totalFindings: r.total_findings,
      reviewedFindings: r.reviewed_count, analyzedAt: r.analyzed_at,
      assignedToId: r.assigned_to_id,
      sheetWritebackStatus: r.sheet_writeback_status,
    });

    res.json({
      needsReview: needsReview.map(mapRow),
      scrubbed: scrubbed.map(mapRow),
      sheetFailed: sheetFailed.map(mapRow),
      unmatchedStatements: unmatchedItems,
      pendingLeads,
      stats: {
        totalAnalyzed: rows.length,
        needsReview: needsReview.length,
        scrubbed: scrubbed.length,
        sheetFailed: sheetFailed.length,
        unmatchedStatements: unmatchedItems.length,
        pendingLeads: pendingLeads.length,
      },
    });
  } catch (e: any) {
    console.error("Scrubbing queue error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.post("/scrubbing/cancel", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    cancelScrubbing();
    for (const [id, job] of backgroundJobs) {
      if (job.status === "running") {
        job.status = "completed";
        job.completedAt = Date.now();
        job.currentLead = "Cancelled by user";
      }
    }
    console.log("[Scrub] Scrubbing cancelled by user");
    res.json({ success: true, message: "Scrubbing cancelled" });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

router.post("/scrubbing/rescrub/:leadId", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const leadId = parseInt(String(req.params.leadId), 10);
    const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!lead) { res.status(404).json({ error: "Lead not found" }); return; }

    const rStart = Date.now();
    console.log(`[Re-Scrub] Starting re-scrub for lead ${leadId} "${lead.businessName}"`);
    await db.delete(bankStatementAnalysesTable).where(eq(bankStatementAnalysesTable.leadId, leadId));
    await db.update(leadsTable).set({ status: "scrubbing", sheetWritebackStatus: null }).where(eq(leadsTable.id, leadId));

    res.json({ success: true, message: `Re-scrub started for "${lead.businessName}"` });

    try {
      const result = await analyzeSingleLead(leadId);
      const autoResult = await tryAutoApprove(leadId);
      if (!autoResult.autoApproved) {
        await db.update(leadsTable).set({ status: "scrubbing_review" }).where(eq(leadsTable.id, leadId));
      }
      const elapsed = ((Date.now() - rStart) / 1000).toFixed(1);
      console.log(`[Re-Scrub] Completed for lead ${leadId}, risk=${result.analysis.riskScore} in ${elapsed}s`);
    } catch (e: any) {
      console.error(`[Re-Scrub] FAILED lead ${leadId}:`, e.message);
      await db.update(leadsTable).set({ status: "scrubbing_review" }).where(eq(leadsTable.id, leadId));
    }
  } catch (e: any) {
    console.error(`[Re-Scrub] Error:`, e.message);
    res.status(500).json({ success: false, error: e.message });
  }
});

router.post("/scrubbing/retry-writeback/:leadId", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const leadId = parseInt(String(req.params.leadId), 10);
    let [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!lead) { res.status(404).json({ error: "Lead not found" }); return; }
    lead = decryptLeadFields(lead);

    const analyses = await db.select().from(bankStatementAnalysesTable)
      .where(eq(bankStatementAnalysesTable.leadId, leadId))
      .orderBy(sql`${bankStatementAnalysesTable.createdAt} DESC`);

    const allLoans: any[] = [];
    for (const a of analyses) {
      const loanDetailsForAnalysis = (a.loanDetails as any[]) || [];
      for (const loan of loanDetailsForAnalysis) allLoans.push({ ...loan, _analysisId: a.id });
      if (a.aiRawAnalysis) {
        try {
          const raw = parseAIResponse(a.aiRawAnalysis);
          if (Array.isArray(raw.recurringPulls)) {
            const existingLenderKeys = new Set(loanDetailsForAnalysis.map((l: any) => shortLenderName((l.lender || l.name || "").trim())));
            for (const p of raw.recurringPulls) {
              const pullLender = p.likelyLender || p.likely_lender || "Unknown";
              const pullKey = shortLenderName(pullLender);
              if (existingLenderKeys.has(pullKey)) continue;
              allLoans.push({ lender: pullLender, amount: p.amount || 0, frequency: p.frequency || "daily", _analysisId: a.id });
            }
          }
        } catch {}
      }
    }
    fixLoanFrequencies(allLoans, analyses.length);

    const [settingsRow] = await db.select().from(appSettingsTable).where(eq(appSettingsTable.key, "scrub_writeback"));
    if (!settingsRow) { res.json({ success: false, error: "No writeback settings configured" }); return; }
    const config = JSON.parse(settingsRow.value);
    const idUrlMatch = (config.spreadsheetId || "").match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/);
    if (idUrlMatch) config.spreadsheetId = idUrlMatch[1];
    if (!config.enabled || !config.spreadsheetId) { res.json({ success: false, error: "Writeback is disabled or no spreadsheet configured" }); return; }

    const revEntries = parseRevEntries(analyses);
    const loanEntries = parseLoanEntries(allLoans);
    const scrubData = formatScrubData(revEntries, loanEntries, config.scrubFormat || "A");

    const sheets = await getUncachableGoogleSheetClient();
    const sheetName = config.sheetName || "Sheet1";
    const headerRes = await sheets.spreadsheets.values.get({ spreadsheetId: config.spreadsheetId, range: `${sheetName}!1:1` });
    const headers = (headerRes.data.values?.[0] || []).map((h: string) => h.toLowerCase().trim());
    if (headers.length === 0) { res.json({ success: false, error: "Sheet has no headers" }); return; }

    const lastCol = String.fromCharCode(65 + Math.min(headers.length - 1, 25));
    const allDataRes = await sheets.spreadsheets.values.get({ spreadsheetId: config.spreadsheetId, range: `${sheetName}!A:${lastCol}` });
    const allRows = allDataRes.data.values || [];

    const ssnCol = headers.findIndex((h: string) => /ssn|social/i.test(h));
    const bizCol = headers.findIndex((h: string) => /business.*name|company|biz/i.test(h));
    const phoneCol = headers.findIndex((h: string) => /phone|tel/i.test(h));

    const cleanPhone = (p: string) => (p || "").replace(/\D/g, "").slice(-10);
    const leadSsn = (lead.ssn || "").replace(/\D/g, "");
    const leadPhone = cleanPhone(lead.phone || "");
    const leadBiz = (lead.businessName || "").toLowerCase().trim();

    let matchedRowIdx = -1;
    for (let r = 1; r < allRows.length; r++) {
      const row = allRows[r];
      if (leadSsn && leadSsn.length >= 9 && ssnCol >= 0 && row[ssnCol] && (row[ssnCol] || "").replace(/\D/g, "") === leadSsn) { matchedRowIdx = r; break; }
      if (leadPhone && leadPhone.length === 10 && phoneCol >= 0 && row[phoneCol] && cleanPhone(row[phoneCol] || "") === leadPhone) { matchedRowIdx = r; break; }
      if (leadBiz && leadBiz.length >= 4 && bizCol >= 0 && row[bizCol] && (row[bizCol] || "").toLowerCase().trim() === leadBiz) { matchedRowIdx = r; break; }
    }

    if (matchedRowIdx < 0) {
      await db.update(leadsTable).set({ sheetWritebackStatus: "no_match" }).where(eq(leadsTable.id, leadId));
      res.json({ success: false, error: "Still no matching row found in Google Sheet" });
      return;
    }

    const writeCol = config.writeColumn || "A";
    const cellRef = `${sheetName}!${writeCol}${matchedRowIdx + 1}`;
    await sheets.spreadsheets.values.update({
      spreadsheetId: config.spreadsheetId, range: cellRef,
      valueInputOption: "RAW", requestBody: { values: [[scrubData]] },
    });
    await db.update(leadsTable).set({ sheetWritebackStatus: "written" }).where(eq(leadsTable.id, leadId));
    console.log(`[Scrub Retry] Wrote to sheet row ${matchedRowIdx + 1} for lead #${leadId}`);
    res.json({ success: true, message: `Scrub data written to row ${matchedRowIdx + 1}` });
  } catch (e: any) {
    console.error("[Scrub Retry] Error:", e.message);
    res.json({ success: false, error: e.message });
  }
});

router.post("/scrubbing/retry-all-writeback", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    const sheetFailedLeads = await db.select({ id: leadsTable.id, businessName: leadsTable.businessName })
      .from(leadsTable)
      .where(
        and(
          eq(leadsTable.status, "scrubbed"),
          isNotNull(leadsTable.sheetWritebackStatus),
          not(eq(leadsTable.sheetWritebackStatus, "written"))
        )
      );

    if (sheetFailedLeads.length === 0) {
      res.json({ success: true, message: "No sheet issues to retry", results: [] });
      return;
    }

    const [settingsRow] = await db.select().from(appSettingsTable).where(eq(appSettingsTable.key, "scrub_writeback"));
    if (!settingsRow) { res.json({ success: false, error: "No writeback settings configured" }); return; }
    const config = JSON.parse(settingsRow.value);
    const idUrlMatch = (config.spreadsheetId || "").match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/);
    if (idUrlMatch) config.spreadsheetId = idUrlMatch[1];
    if (!config.enabled || !config.spreadsheetId) { res.json({ success: false, error: "Writeback disabled or no spreadsheet" }); return; }

    const sheets = await getUncachableGoogleSheetClient();
    const sheetName = config.sheetName || "Sheet1";
    const headerRes = await sheets.spreadsheets.values.get({ spreadsheetId: config.spreadsheetId, range: `${sheetName}!1:1` });
    const headers = (headerRes.data.values?.[0] || []).map((h: string) => h.toLowerCase().trim());
    if (headers.length === 0) { res.json({ success: false, error: "Sheet has no headers" }); return; }

    const lastCol = String.fromCharCode(65 + Math.min(headers.length - 1, 25));
    const allDataRes = await sheets.spreadsheets.values.get({ spreadsheetId: config.spreadsheetId, range: `${sheetName}!A:${lastCol}` });
    const allRows = allDataRes.data.values || [];

    const ssnCol = headers.findIndex((h: string) => /ssn|social/i.test(h));
    const bizCol = headers.findIndex((h: string) => /business.*name|company|biz/i.test(h));
    const phoneCol = headers.findIndex((h: string) => /phone|tel/i.test(h));
    const cleanPhone = (p: string) => (p || "").replace(/\D/g, "").slice(-10);

    const results: { leadId: number; business: string; success: boolean; error?: string }[] = [];

    for (const lead of sheetFailedLeads) {
      try {
        let [fullLead] = await db.select().from(leadsTable).where(eq(leadsTable.id, lead.id));
        if (!fullLead) { results.push({ leadId: lead.id, business: lead.businessName || "", success: false, error: "Lead not found" }); continue; }
        fullLead = decryptLeadFields(fullLead);

        const analyses = await db.select().from(bankStatementAnalysesTable)
          .where(eq(bankStatementAnalysesTable.leadId, lead.id))
          .orderBy(sql`${bankStatementAnalysesTable.createdAt} DESC`);

        const allLoans: any[] = [];
        for (const a of analyses) {
          const loanDetailsForAnalysis = (a.loanDetails as any[]) || [];
          for (const loan of loanDetailsForAnalysis) allLoans.push({ ...loan, _analysisId: a.id });
          if (a.aiRawAnalysis) {
            try {
              const raw = parseAIResponse(a.aiRawAnalysis);
              if (Array.isArray(raw.recurringPulls)) {
                const existingLenderKeys = new Set(loanDetailsForAnalysis.map((l: any) => shortLenderName((l.lender || l.name || "").trim())));
                for (const p of raw.recurringPulls) {
                  const pullLender = p.likelyLender || p.likely_lender || "Unknown";
                  const pullKey = shortLenderName(pullLender);
                  if (existingLenderKeys.has(pullKey)) continue;
                  allLoans.push({ lender: pullLender, amount: p.amount || 0, frequency: p.frequency || "daily", _analysisId: a.id });
                }
              }
            } catch {}
          }
        }
        fixLoanFrequencies(allLoans, analyses.length);

        const revEntries = parseRevEntries(analyses);
        const loanEntries = parseLoanEntries(allLoans);
        const scrubData = formatScrubData(revEntries, loanEntries, config.scrubFormat || "A");

        const leadSsn = (fullLead.ssn || "").replace(/\D/g, "");
        const leadPhone = cleanPhone(fullLead.phone || "");
        const leadBiz = (fullLead.businessName || "").toLowerCase().trim();

        let matchedRowIdx = -1;
        for (let r = 1; r < allRows.length; r++) {
          const row = allRows[r];
          if (leadSsn && leadSsn.length >= 9 && ssnCol >= 0 && row[ssnCol] && (row[ssnCol] || "").replace(/\D/g, "") === leadSsn) { matchedRowIdx = r; break; }
          if (leadPhone && leadPhone.length === 10 && phoneCol >= 0 && row[phoneCol] && cleanPhone(row[phoneCol] || "") === leadPhone) { matchedRowIdx = r; break; }
          if (leadBiz && leadBiz.length >= 4 && bizCol >= 0 && row[bizCol] && (row[bizCol] || "").toLowerCase().trim() === leadBiz) { matchedRowIdx = r; break; }
        }

        if (matchedRowIdx < 0) {
          await db.update(leadsTable).set({ sheetWritebackStatus: "no_match" }).where(eq(leadsTable.id, lead.id));
          results.push({ leadId: lead.id, business: lead.businessName || "", success: false, error: "No matching row in sheet" });
          continue;
        }

        const writeCol = config.writeColumn || "A";
        const cellRef = `${sheetName}!${writeCol}${matchedRowIdx + 1}`;
        await sheets.spreadsheets.values.update({
          spreadsheetId: config.spreadsheetId, range: cellRef,
          valueInputOption: "RAW", requestBody: { values: [[scrubData]] },
        });
        await db.update(leadsTable).set({ sheetWritebackStatus: "written" }).where(eq(leadsTable.id, lead.id));
        results.push({ leadId: lead.id, business: lead.businessName || "", success: true });
        console.log(`[Scrub Retry All] Wrote lead #${lead.id} to row ${matchedRowIdx + 1}`);
      } catch (e: any) {
        results.push({ leadId: lead.id, business: lead.businessName || "", success: false, error: e.message });
      }
    }

    const succeeded = results.filter(r => r.success).length;
    const failed = results.filter(r => !r.success).length;
    res.json({ success: true, message: `Retried ${results.length}: ${succeeded} succeeded, ${failed} failed`, results });
  } catch (e: any) {
    console.error("[Scrub Retry All] Error:", e.message);
    res.status(500).json({ success: false, error: e.message });
  }
});

router.post("/scrubbing/refresh-all-previews", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    const reviewLeads = await db.select({ id: leadsTable.id, businessName: leadsTable.businessName })
      .from(leadsTable)
      .where(eq(leadsTable.status, "scrubbing_review"));

    if (reviewLeads.length === 0) {
      res.json({ success: true, message: "No leads in scrubbing_review", updated: 0 });
      return;
    }

    const [settingsRow] = await db.select().from(appSettingsTable).where(eq(appSettingsTable.key, "scrub_writeback"));
    if (!settingsRow) { res.json({ success: false, error: "No writeback settings configured" }); return; }
    const config = JSON.parse(settingsRow.value);
    const idUrlMatch = (config.spreadsheetId || "").match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/);
    if (idUrlMatch) config.spreadsheetId = idUrlMatch[1];
    if (!config.enabled || !config.spreadsheetId) { res.json({ success: false, error: "Writeback disabled or no spreadsheet" }); return; }

    const sheets = await getUncachableGoogleSheetClient();
    const sheetName = config.sheetName || "Sheet1";
    const headerRes = await sheets.spreadsheets.values.get({ spreadsheetId: config.spreadsheetId, range: `${sheetName}!1:1` });
    const headers = (headerRes.data.values?.[0] || []).map((h: string) => h.toLowerCase().trim());
    if (headers.length === 0) { res.json({ success: false, error: "Sheet has no headers" }); return; }

    const lastCol = String.fromCharCode(65 + Math.min(headers.length - 1, 25));
    const allDataRes = await sheets.spreadsheets.values.get({ spreadsheetId: config.spreadsheetId, range: `${sheetName}!A:${lastCol}` });
    const allRows = allDataRes.data.values || [];

    const ssnCol = headers.findIndex((h: string) => /ssn|social/i.test(h));
    const bizCol = headers.findIndex((h: string) => /business.*name|company|biz/i.test(h));
    const phoneCol = headers.findIndex((h: string) => /phone|tel/i.test(h));
    const cleanPhone = (p: string) => (p || "").replace(/\D/g, "").slice(-10);

    let updated = 0;
    let skipped = 0;
    let noMatch = 0;
    const errors: string[] = [];

    console.log(`[Preview Refresh] Starting refresh for ${reviewLeads.length} leads in scrubbing_review`);

    for (const lead of reviewLeads) {
      try {
        let [fullLead] = await db.select().from(leadsTable).where(eq(leadsTable.id, lead.id));
        if (!fullLead) { skipped++; continue; }
        fullLead = decryptLeadFields(fullLead);

        const analyses = await db.select().from(bankStatementAnalysesTable)
          .where(eq(bankStatementAnalysesTable.leadId, lead.id))
          .orderBy(sql`${bankStatementAnalysesTable.createdAt} DESC`);

        if (analyses.length === 0) { skipped++; continue; }

        const allLoans: any[] = [];
        for (const a of analyses) {
          const loanDetailsForAnalysis = (a.loanDetails as any[]) || [];
          for (const loan of loanDetailsForAnalysis) allLoans.push({ ...loan, _analysisId: a.id });
          if (a.aiRawAnalysis) {
            try {
              const raw = parseAIResponse(a.aiRawAnalysis);
              if (Array.isArray(raw.recurringPulls)) {
                const existingLenderKeys = new Set(loanDetailsForAnalysis.map((l: any) => shortLenderName((l.lender || l.name || "").trim())));
                for (const p of raw.recurringPulls) {
                  const pullLender = p.likelyLender || p.likely_lender || "Unknown";
                  const pullKey = shortLenderName(pullLender);
                  if (existingLenderKeys.has(pullKey)) continue;
                  allLoans.push({ lender: pullLender, amount: p.amount || 0, frequency: p.frequency || "daily", _analysisId: a.id });
                }
              }
            } catch {}
          }
        }
        fixLoanFrequencies(allLoans, analyses.length);

        const revEntries = parseRevEntries(analyses);
        const loanEntries = parseLoanEntries(allLoans);
        const scrubData = formatScrubData(revEntries, loanEntries, config.scrubFormat || "A");

        const leadSsn = (fullLead.ssn || "").replace(/\D/g, "");
        const leadPhone = cleanPhone(fullLead.phone || "");
        const leadBiz = (fullLead.businessName || "").toLowerCase().trim();

        let matchedRowIdx = -1;
        for (let r = 1; r < allRows.length; r++) {
          const row = allRows[r];
          if (leadSsn && leadSsn.length >= 9 && ssnCol >= 0 && row[ssnCol] && (row[ssnCol] || "").replace(/\D/g, "") === leadSsn) { matchedRowIdx = r; break; }
          if (leadPhone && leadPhone.length === 10 && phoneCol >= 0 && row[phoneCol] && cleanPhone(row[phoneCol] || "") === leadPhone) { matchedRowIdx = r; break; }
          if (leadBiz && leadBiz.length >= 4 && bizCol >= 0 && row[bizCol] && (row[bizCol] || "").toLowerCase().trim() === leadBiz) { matchedRowIdx = r; break; }
        }

        if (matchedRowIdx < 0) {
          noMatch++;
          continue;
        }

        const writeCol = config.writeColumn || "A";
        const cellRef = `${sheetName}!${writeCol}${matchedRowIdx + 1}`;
        await sheets.spreadsheets.values.update({
          spreadsheetId: config.spreadsheetId, range: cellRef,
          valueInputOption: "RAW", requestBody: { values: [[scrubData]] },
        });
        updated++;
        console.log(`[Preview Refresh] Lead #${lead.id} (${lead.businessName}): wrote "${scrubData}" to row ${matchedRowIdx + 1}`);
      } catch (e: any) {
        errors.push(`Lead #${lead.id}: ${e.message}`);
      }
    }

    console.log(`[Preview Refresh] Done — ${updated} updated, ${skipped} skipped, ${noMatch} no sheet match, ${errors.length} errors`);
    res.json({ success: true, message: `Refreshed ${updated} of ${reviewLeads.length} leads`, updated, skipped, noMatch, errors: errors.slice(0, 10) });
  } catch (e: any) {
    console.error("[Preview Refresh] Error:", e.message);
    res.status(500).json({ success: false, error: e.message });
  }
});

router.post("/scrubbing/retry-all-match", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    const batches = await db.select().from(uploadBatchesTable).where(eq(uploadBatchesTable.status, "completed"));
    const unmatchedItems: { batchId: number; folderName: string }[] = [];
    for (const batch of batches) {
      const folders = (batch.unmatchedFolders as string[]) || [];
      for (const f of folders) unmatchedItems.push({ batchId: batch.id, folderName: f });
    }

    if (unmatchedItems.length === 0) {
      res.json({ success: true, message: "No unmatched items to retry", results: [] });
      return;
    }

    const allLeads = await db.select({ id: leadsTable.id, businessName: leadsTable.businessName, dba: leadsTable.dba, ownerName: leadsTable.ownerName })
      .from(leadsTable);

    const cleanFn = (name: string) => name
      .replace(/[-_]+/g, " ")
      .replace(/[\s]+\d{10,}$/g, "")
      .replace(/\b\d{10,}\b/g, "")
      .replace(/\b(llc|inc|corp|ltd|co|company|enterprises?|group|holdings?|solutions?|services?|international|global|usa|bank\s*statement?s?|statement?s?)\b/gi, "")
      .replace(/\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}/g, "").replace(/\s+/g, " ").trim();

    const fuzzyMatch = (folderClean: string, leadClean: string): boolean => {
      if (!folderClean || !leadClean) return false;
      if (folderClean.includes(leadClean) || leadClean.includes(folderClean)) return true;
      const fWords = folderClean.split(" ").filter(w => w.length > 1);
      const lWords = leadClean.split(" ").filter(w => w.length > 1);
      if (fWords.length < 2 || lWords.length < 2) return false;
      const longer = fWords.length >= lWords.length ? fWords : lWords;
      const shorter = fWords.length >= lWords.length ? lWords : fWords;
      const matchCount = shorter.filter(w => longer.some(lw => lw.includes(w) || w.includes(lw))).length;
      return matchCount >= Math.max(2, shorter.length * 0.6);
    };

    const fsModule = await import("fs");
    const pathModule = await import("path");
    const results: { folderName: string; success: boolean; matchedTo?: string; error?: string }[] = [];

    for (const item of unmatchedItems) {
      const cleaned = cleanFn(item.folderName);
      if (!cleaned || cleaned.length < 2) {
        results.push({ folderName: item.folderName, success: false, error: "Name too short" });
        continue;
      }

      const searchLower = cleaned.toLowerCase();
      let matchedLead: { id: number; businessName: string } | null = null;

      for (const l of allLeads) {
        const bizClean = cleanFn(l.businessName || "").toLowerCase();
        const dbaClean = cleanFn(l.dba || "").toLowerCase();
        if (fuzzyMatch(searchLower, bizClean)) { matchedLead = { id: l.id, businessName: l.businessName || "" }; break; }
        if (fuzzyMatch(searchLower, dbaClean)) { matchedLead = { id: l.id, businessName: l.businessName || "" }; break; }
      }

      if (!matchedLead) {
        results.push({ folderName: item.folderName, success: false, error: "No lead matched" });
        continue;
      }

      const [batch] = await db.select().from(uploadBatchesTable).where(eq(uploadBatchesTable.id, item.batchId));
      if (!batch) { results.push({ folderName: item.folderName, success: false, error: "Batch not found" }); continue; }

      let filesAttached = 0;
      const extractDir = batch.extractDir as string | null;
      if (extractDir) {
        const folderPath = pathModule.default.join(extractDir, item.folderName);
        if (fsModule.default.existsSync(folderPath)) {
          const files = fsModule.default.readdirSync(folderPath).filter((f: string) => /\.(pdf|png|jpg|jpeg)$/i.test(f));
          for (const file of files) {
            const relUrl = `/uploads/extracted/${pathModule.default.basename(extractDir)}/${item.folderName}/${file}`;
            await db.insert(documentsTable).values({
              leadId: matchedLead.id,
              name: file,
              url: relUrl,
              type: "bank_statement",
            }).onConflictDoNothing();
            filesAttached++;
          }
        }
      }

      const currentUnmatched = (batch.unmatchedFolders as string[]) || [];
      const updated = currentUnmatched.filter(f => f !== item.folderName);
      await db.update(uploadBatchesTable).set({ unmatchedFolders: updated }).where(eq(uploadBatchesTable.id, item.batchId));
      results.push({ folderName: item.folderName, success: true, matchedTo: matchedLead.businessName });
      console.log(`[Retry All Match] Matched "${item.folderName}" to lead "${matchedLead.businessName}", ${filesAttached} files attached`);
    }

    const succeeded = results.filter(r => r.success).length;
    const failed = results.filter(r => !r.success).length;
    res.json({ success: true, message: `Retried ${results.length}: ${succeeded} matched, ${failed} unmatched`, results });
  } catch (e: any) {
    console.error("[Retry All Match] Error:", e.message);
    res.status(500).json({ success: false, error: e.message });
  }
});

router.post("/scrubbing/retry-match", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const { batchId, folderName } = req.body;
    if (!batchId || !folderName) { res.status(400).json({ error: "batchId and folderName required" }); return; }

    const [batch] = await db.select().from(uploadBatchesTable).where(eq(uploadBatchesTable.id, batchId));
    if (!batch) { res.status(404).json({ error: "Upload batch not found" }); return; }

    const allLeads = await db.select({ id: leadsTable.id, businessName: leadsTable.businessName, dba: leadsTable.dba, ownerName: leadsTable.ownerName })
      .from(leadsTable);

    const cleanFn = (name: string) => name
      .replace(/[-_]+/g, " ")
      .replace(/[\s]+\d{10,}$/g, "")
      .replace(/\b\d{10,}\b/g, "")
      .replace(/\b(llc|inc|corp|ltd|co|company|enterprises?|group|holdings?|solutions?|services?|international|global|usa|bank\s*statement?s?|statement?s?)\b/gi, "")
      .replace(/\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}/g, "").replace(/\s+/g, " ").trim();

    const cleaned = cleanFn(folderName);
    if (!cleaned || cleaned.length < 2) { res.json({ success: false, error: "Folder name too short to match" }); return; }

    const searchLower = cleaned.toLowerCase();
    let matchedLead: { id: number; businessName: string } | null = null;

    for (const l of allLeads) {
      const bn = (l.businessName || "").toLowerCase();
      const dba = (l.dba || "").toLowerCase();
      const on = (l.ownerName || "").toLowerCase();
      if (bn.includes(searchLower) || dba.includes(searchLower) || on.includes(searchLower) || searchLower.includes(bn) || searchLower.includes(on)) {
        matchedLead = { id: l.id, businessName: l.businessName };
        break;
      }
    }

    if (!matchedLead) {
      const words = searchLower.split(" ").filter(w => w.length > 3);
      for (const word of words) {
        for (const l of allLeads) {
          const bn = (l.businessName || "").toLowerCase();
          const dba = (l.dba || "").toLowerCase();
          const on = (l.ownerName || "").toLowerCase();
          if (bn.includes(word) || dba.includes(word) || on.includes(word)) {
            matchedLead = { id: l.id, businessName: l.businessName };
            break;
          }
        }
        if (matchedLead) break;
      }
    }

    if (!matchedLead) {
      res.json({ success: false, error: `Still no matching lead found for "${folderName}"` });
      return;
    }

    const currentFolders = (batch.unmatchedFolders as string[]) || [];
    if (!currentFolders.includes(folderName)) {
      res.json({ success: false, error: "This folder has already been resolved" });
      return;
    }

    let filesAttached = 0;
    const fs = await import("fs");
    const path = await import("path");
    if (batch.extractDir && fs.existsSync(batch.extractDir)) {
      const resolvedBase = fs.realpathSync(batch.extractDir);
      const safeFolder = folderName.replace(/[<>:"/\\|?*]/g, "_");
      const folderPath = path.resolve(resolvedBase, safeFolder);
      if (!folderPath.startsWith(resolvedBase)) {
        res.json({ success: false, error: "Invalid folder name" });
        return;
      }
      if (fs.existsSync(folderPath)) {
        const files = fs.readdirSync(folderPath).filter((f: string) => !f.startsWith(".") && !f.startsWith("__"));
        for (const file of files) {
          const filePath = path.join(folderPath, file);
          if (!fs.statSync(filePath).isFile()) continue;
          const docUrl = `/uploads/extracted/${path.basename(resolvedBase)}/${safeFolder}/${file}`;
          await db.insert(documentsTable).values({
            leadId: matchedLead.id, type: "bank_statement", name: file, url: docUrl,
          });
          filesAttached++;
        }
      }
    }

    const updatedFolders = currentFolders.filter(f => f !== folderName);
    await db.update(uploadBatchesTable).set({
      unmatchedFolders: updatedFolders,
      matchedFolders: (batch.matchedFolders || 0) + 1,
    }).where(eq(uploadBatchesTable.id, batchId));

    if (filesAttached > 0) {
      await db.update(leadsTable).set({ bankStatementsStatus: "uploaded" }).where(eq(leadsTable.id, matchedLead.id));
    }

    console.log(`[Retry Match] Matched "${folderName}" → lead #${matchedLead.id} (${matchedLead.businessName}), ${filesAttached} files attached`);
    res.json({
      success: true,
      matchedLeadId: matchedLead.id,
      matchedBusinessName: matchedLead.businessName,
      filesAttached,
      message: `Matched to "${matchedLead.businessName}" — ${filesAttached} files attached. Run AI scrubbing from the lead's scrubbing page to analyze.`,
    });
  } catch (e: any) {
    console.error("[Retry Match] Error:", e.message);
    res.json({ success: false, error: e.message });
  }
});

router.post("/scrubbing/dismiss-unmatched", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const { batchId, folderName } = req.body;
    if (!batchId || !folderName) { res.status(400).json({ error: "batchId and folderName required" }); return; }

    const [batch] = await db.select().from(uploadBatchesTable).where(eq(uploadBatchesTable.id, batchId));
    if (!batch) { res.status(404).json({ error: "Batch not found" }); return; }

    const currentFolders = (batch.unmatchedFolders as string[]) || [];
    const updatedFolders = currentFolders.filter(f => f !== folderName);
    await db.update(uploadBatchesTable).set({ unmatchedFolders: updatedFolders as any[] }).where(eq(uploadBatchesTable.id, batchId));

    res.json({ success: true });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

router.get("/scrubbing/detail/:leadId", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const leadId = parseInt(String(req.params.leadId), 10);
    const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!lead) { res.status(404).json({ error: "Lead not found" }); return; }

    const analyses = await db.select().from(bankStatementAnalysesTable)
      .where(eq(bankStatementAnalysesTable.leadId, leadId))
      .orderBy(sql`${bankStatementAnalysesTable.createdAt} DESC`);

    const confirmations = await db.select().from(underwritingConfirmationsTable)
      .where(eq(underwritingConfirmationsTable.leadId, leadId))
      .orderBy(sql`${underwritingConfirmationsTable.createdAt} ASC`);

    const bankStatements = await db.select().from(documentsTable)
      .where(eq(documentsTable.leadId, leadId));

    let totalDeposits = 0;
    const allMonthlyRevenues: any[] = [];
    let totalNsfCount = 0;
    const allNegativeDays: any[] = [];
    const allTransactions: any[] = [];
    const allRecurringPulls: any[] = [];

    const { activeLoans: allLoans } = buildActiveLoansFromMostRecent(analyses);
    fixLoanFrequencies(allLoans, 1);

    let totalLoanPayments = 0;
    for (const loan of allLoans) {
      if (loan.amount && (loan.frequency === "daily" || loan.frequency === "weekly" || loan.frequency === "monthly")) {
        let m = parseNumeric(loan.amount);
        if (loan.frequency === "daily") m *= 22;
        else if (loan.frequency === "weekly") m *= 4;
        else if (loan.frequency === "monthly") m *= 1;
        totalLoanPayments += m;
      }
    }

    for (const a of analyses) {
      if (a.grossRevenue) totalDeposits += parseNumeric(a.grossRevenue);
      let scrubAcctLast4 = "";
      let scrubBankName = "";

      if (a.aiRawAnalysis) {
        try {
          const rawPre = parseAIResponse(a.aiRawAnalysis);
          if (rawPre.accountNumber) {
            const cleaned = String(rawPre.accountNumber).replace(/\D/g, "");
            if (cleaned.length >= 4) scrubAcctLast4 = cleaned.slice(-4);
          }
          if (rawPre.bankName) {
            scrubBankName = String(rawPre.bankName).trim();
          }
        } catch {}
      }

      for (const mr of ((a.monthlyRevenues as any[]) || [])) {
        const rawAcct = mr.account ? String(mr.account).replace(/\D/g, "") : "";
        const mrAcct = rawAcct.length >= 4 ? rawAcct.slice(-4) : (rawAcct || scrubAcctLast4);
        const mrBank = mr.bankName || scrubBankName || undefined;
        const revMonth = (mr.month || "").toLowerCase().trim();
        const revAmt = mr.revenue || 0;
        const dupeKey = `${revMonth}|${mrAcct || ""}`;
        const existingRev = allMonthlyRevenues.find((r: any) => {
          const rMonth = (r.month || "").toLowerCase().trim();
          return `${rMonth}|${r.acctLast4 || ""}` === dupeKey;
        });
        if (existingRev) {
          const incomingFlagged = mr.needsReview && !mr.reviewResolved;
          const existingFlagged = existingRev.needsReview && !existingRev.reviewResolved;
          if (incomingFlagged || (!existingFlagged && revAmt > (existingRev.revenue || 0))) {
            Object.assign(existingRev, { ...mr, acctLast4: mrAcct || undefined, bankName: mrBank, analysisId: a.id });
          }
        } else {
          allMonthlyRevenues.push({ ...mr, acctLast4: mrAcct || undefined, bankName: mrBank, analysisId: a.id });
        }
      }
      allNegativeDays.push(...((a.negativeDays as any[]) || []));
      totalNsfCount += (a.nsfCount as number) || 0;

      if (a.aiRawAnalysis) {
        try {
          const raw = parseAIResponse(a.aiRawAnalysis);
          if (Array.isArray(raw.notableTransactions)) allTransactions.push(...raw.notableTransactions);
          if (Array.isArray(raw.recurringPulls)) {
            for (const p of raw.recurringPulls) {
              const pullAmt = Math.round(p.amount || 0);
              const pullLenderRaw = (p.likelyLender || p.likely_lender || "Unknown");
              const pullLender = pullLenderRaw.toLowerCase().replace(/[^a-z0-9]/g, "");
              const pullShort = shortLenderName(pullLenderRaw);
              const existingPull = allRecurringPulls.find((ep: any) => {
                const epLender = (ep.likelyLender || "").toLowerCase().replace(/[^a-z0-9]/g, "");
                const epShort = shortLenderName(ep.likelyLender || "");
                return (Math.round(ep.amount) === pullAmt && epLender === pullLender) ||
                       (epShort === pullShort && epShort !== "unknown");
              });
              const similarPull = !existingPull ? allRecurringPulls.find((ep: any) => {
                const epShort = shortLenderName(ep.likelyLender || "");
                if (epShort !== pullShort && (ep.likelyLender || "").toLowerCase().replace(/[^a-z0-9]/g, "") !== pullLender) return false;
                const epAmt = ep.amount || 0;
                const curAmt = p.amount || 0;
                if (epAmt === 0 || curAmt === 0) return false;
                return Math.max(epAmt, curAmt) / Math.min(epAmt, curAmt) <= 1.35;
              }) : null;
              if (existingPull || similarPull) {
                const target = existingPull || similarPull!;
                if (!existingPull && (p.amount || 0) > (target.amount || 0)) {
                  console.log(`[PullMerge] "${target.likelyLender}": payment changed $${target.amount} → $${p.amount}`);
                  target.amount = p.amount;
                }
                target.occurrences = (target.occurrences || 0) + (p.occurrences || 0);
                if (p.dateRange || p.date_range) {
                  const newRange = p.dateRange || p.date_range || "";
                  if (newRange && target.dateRange) {
                    const allDates = `${target.dateRange}, ${newRange}`.split(/[,\s]+to\s+|,\s*/).filter(Boolean).sort();
                    target.dateRange = `${allDates[0]} to ${allDates[allDates.length - 1]}`;
                  } else if (newRange) {
                    target.dateRange = newRange;
                  }
                }
              } else {
                allRecurringPulls.push({
                  amount: p.amount || 0,
                  frequency: p.frequency || "daily",
                  occurrences: p.occurrences || 0,
                  dateRange: p.dateRange || p.date_range || "",
                  likelyLender: p.likelyLender || p.likely_lender || "Unknown",
                  monthlyTotal: p.monthlyTotal || p.monthly_total || 0,
                  confidence: p.confidence || "medium",
                });
              }
            }
          }
        } catch {}
      }
    }

    if (lead.loanCount !== allLoans.length) {
      await db.update(leadsTable).set({
        loanCount: allLoans.length,
        hasExistingLoans: allLoans.length > 0,
        loanDetails: allLoans,
      }).where(eq(leadsTable.id, leadId));
    }

    const loanLenderShortKeys = new Set(allLoans.map((l: any) => shortLenderName((l.lender || l.name || "").trim())));
    const filteredRecurringPulls = allRecurringPulls.filter((p: any) => {
      const pullShort = shortLenderName(p.likelyLender || "");
      return !loanLenderShortKeys.has(pullShort) || pullShort === "unknown";
    });

    const totalRecurringPullCost = filteredRecurringPulls.reduce((sum: number, p: any) => sum + (p.monthlyTotal || 0), 0);

    const monthTotalsMap = new Map<string, number>();
    for (const mr of allMonthlyRevenues) {
      const month = (mr.month || "").toLowerCase().trim();
      if (!month) continue;
      const existing = monthTotalsMap.get(month) || 0;
      monthTotalsMap.set(month, existing + (mr.revenue || 0));
    }
    if (monthTotalsMap.size > 1) {
      const vals = [...monthTotalsMap.values()].filter(v => v > 0).sort((a, b) => a - b);
      if (vals.length > 1) {
        const median = vals[Math.floor(vals.length / 2)];
        if (median > 0) {
          for (const [month, amt] of monthTotalsMap) {
            if (amt > median * 5 && amt > 100000) {
              console.log(`[ScrubDetail-Outlier] Month ${month}: $${amt} is ${(amt / median).toFixed(1)}x median ($${median}), capping`);
              monthTotalsMap.set(month, median);
            }
          }
        }
      }
    }
    const sortedMonthEntries = [...monthTotalsMap.entries()].sort((a, b) => b[0].localeCompare(a[0]));
    const recent2Months = sortedMonthEntries.slice(0, 2);
    if (recent2Months.length > 0) {
      totalDeposits = Math.round(recent2Months.reduce((s, [, v]) => s + v, 0) / recent2Months.length);
    }

    const trueGross = totalDeposits - totalLoanPayments;
    const holdbackPct = totalDeposits > 0 ? Math.round((totalLoanPayments / totalDeposits) * 100) : 0;

    const checklist = [
      { item: "Existing Loans & Advances", description: "Scanning for recurring payments to lenders (Capital Funding, OnDeck, etc.)", found: allLoans.length, status: analyses.length > 0 ? "done" : "pending" },
      { item: "Daily/Weekly Same-Amount Pulls", description: "Same exact dollar amount pulled every day or every week = MCA debt indicator", found: allRecurringPulls.length, warn: allRecurringPulls.length > 0, status: analyses.length > 0 ? "done" : "pending" },
      { item: "Gross Deposits", description: "Total deposits across all statements", value: totalDeposits, status: analyses.length > 0 ? "done" : "pending" },
      { item: "True Gross (Deposits - Loans)", description: "Real revenue after subtracting loan payments", value: trueGross, status: analyses.length > 0 ? "done" : "pending" },
      { item: "Negative Balance Days", description: "Days account balance went below $0", found: allNegativeDays.length, status: analyses.length > 0 ? "done" : "pending" },
      { item: "NSF / Returned Items", description: "Non-sufficient funds and bounced payments", found: totalNsfCount, status: analyses.length > 0 ? "done" : "pending" },
      { item: "Revenue Trend", description: "Is revenue growing, stable, or declining?", value: lead.revenueTrend || "unknown", status: analyses.length > 0 ? "done" : "pending" },
      { item: "Risk Score", description: "AI risk classification from A1 (best) to C (worst)", value: lead.riskCategory || analyses[0]?.riskScore || "unknown", status: analyses.length > 0 ? "done" : "pending" },
    ];

    const rejectedLenders = new Set<string>();
    for (const c of confirmations) {
      if (c.status === "rejected") {
        const val = c.originalValue as any;
        const lender = (val?.lender || "").toLowerCase().replace(/[^a-z0-9]/g, "");
        if (lender) rejectedLenders.add(lender);
      }
    }
    try {
      const { getLenderVerdicts } = await import("./coreController");
      const verdicts = await getLenderVerdicts();
      for (const [lender, verdict] of verdicts) {
        if (verdict.verdict === "rejected") rejectedLenders.add(lender.toLowerCase().replace(/[^a-z0-9]/g, ""));
      }
    } catch {}

    const rejectedShortNames = new Set<string>();
    for (const rl of rejectedLenders) {
      rejectedShortNames.add(shortLenderName(rl));
    }

    const confirmedLoans = allLoans.filter(loan => {
      const lenderNorm = (loan.lender || "").toLowerCase().replace(/[^a-z0-9]/g, "");
      if (rejectedLenders.has(lenderNorm)) return false;
      const lenderShort = shortLenderName(loan.lender || "");
      if (rejectedShortNames.has(lenderShort) && lenderShort !== "unknown") return false;
      for (const rl of rejectedLenders) {
        if (lenderNorm.includes(rl) || rl.includes(lenderNorm)) return false;
      }
      if ((loan.occurrences || 0) < 2) return false;
      return true;
    });

    const pullsAsLoans = allRecurringPulls
      .filter((p: any) => {
        const lenderNorm = (p.likelyLender || "").toLowerCase().replace(/[^a-z0-9]/g, "");
        if (rejectedLenders.has(lenderNorm)) return false;
        const alreadyInLoans = confirmedLoans.some((l: any) => {
          const ln = (l.lender || "").toLowerCase().replace(/[^a-z0-9]/g, "");
          return ln === lenderNorm || ln.includes(lenderNorm) || lenderNorm.includes(ln);
        });
        return !alreadyInLoans;
      })
      .map((p: any) => ({
        lender: p.likelyLender || "Unknown",
        amount: p.amount || 0,
        frequency: p.frequency || "daily",
        occurrences: p.occurrences || 0,
        fundedAmount: null,
        fundedDate: null,
        account: "",
      }));

    const allLoansForPreview = [...confirmedLoans, ...pullsAsLoans];

    const revEntries = parseRevEntries(analyses);
    const loanEntries = parseLoanEntries(allLoansForPreview);
    let scrubPreviewFormat = "A";
    try {
      const [settingsRow] = await db.select().from(appSettingsTable).where(eq(appSettingsTable.key, "scrub_writeback"));
      if (settingsRow) {
        const config = JSON.parse(settingsRow.value);
        scrubPreviewFormat = config.scrubFormat || "A";
      }
    } catch {}
    const scrubPreview = formatScrubData(revEntries, loanEntries, scrubPreviewFormat);

    const pendingFindings = confirmations.filter(c => c.status === "pending");
    const reviewedFindings = confirmations.filter(c => c.status !== "pending");

    const { getLearningContext } = await import("./coreController");
    const aiStats = await db.execute(sql`
      SELECT
        COUNT(*)::int as total,
        COUNT(CASE WHEN status = 'confirmed' THEN 1 END)::int as confirmed,
        COUNT(CASE WHEN status = 'rejected' THEN 1 END)::int as rejected,
        COUNT(CASE WHEN status = 'relabeled' THEN 1 END)::int as relabeled
      FROM underwriting_confirmations
      WHERE status != 'pending'
    `);
    const statsRow = ((aiStats as any).rows || aiStats)[0] || {};
    const aiAccuracy = statsRow.total > 0 ? Math.round((statsRow.confirmed / statsRow.total) * 100) : 0;

    const monthAbbrs = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    const docLabels: Record<number, { label: string; month: string; acct: string; bankName: string; isDuplicate: boolean }> = {};
    const seenMonthAcct = new Set<string>();

    function extractFromText(txt: string): { month: string; acct: string } {
      let month = "", acct = "";
      const mn: Record<string, number> = { jan:1,feb:2,mar:3,apr:4,may:5,jun:6,jul:7,aug:8,sep:9,oct:10,nov:11,dec:12,january:1,february:2,march:3,april:4,june:6,july:7,august:8,september:9,october:10,november:11,december:12 };
      const cleaned = txt.replace(/^\[PDF:\s*\d+\s*pages?\]\s*/i, "").trim();
      const header = cleaned.slice(0, 4000);

      function monthFromWord(w: string): number { return mn[w.toLowerCase().slice(0, 3)] || 0; }
      function yrShort(y: string): string { return y.length === 4 ? y.slice(2) : y; }

      function pickStatementMonth(startMo: number, startYr: string, endMo: number, endDay: number, endYr: string, startDay?: number): string {
        if (startMo === endMo) {
          return `${monthAbbrs[endMo]}${yrShort(endYr)}`;
        }
        const startYrFull = startYr.length === 2 ? 2000 + parseInt(startYr) : parseInt(startYr);
        const daysInStartMo = new Date(startYrFull, startMo, 0).getDate();
        const sDay = startDay || 1;
        const daysInStart = daysInStartMo - sDay + 1;
        const daysInEnd = endDay;
        // console.log(`[TabLabel] pickStatementMonth: ${startMo}/${sDay} → ${endMo}/${endDay}, daysInStart=${daysInStart}, daysInEnd=${daysInEnd}`);
        if (daysInEnd <= daysInStart) {
          return `${monthAbbrs[startMo]}${yrShort(startYr)}`;
        }
        return `${monthAbbrs[endMo]}${yrShort(endYr)}`;
      }

      const beginEndMatch = header.match(/beginning\s+balance\s+(\d{1,2})\/(\d{1,2})\/(\d{2,4})/i);
      const endBalMatch = header.match(/ending\s+balance\s+(\d{1,2})\/(\d{1,2})\/(\d{2,4})/i);
      if (beginEndMatch && endBalMatch) {
        const startMo = parseInt(beginEndMatch[1]);
        const startDayVal = parseInt(beginEndMatch[2]);
        const startYr = beginEndMatch[3];
        const endMo = parseInt(endBalMatch[1]);
        const endDay = parseInt(endBalMatch[2]);
        const endYr = endBalMatch[3];
        if (startMo >= 1 && startMo <= 12 && endMo >= 1 && endMo <= 12) {
          month = pickStatementMonth(startMo, startYr, endMo, endDay, endYr, startDayVal);
        }
      }

      const dateRangePatterns: { pat: RegExp; type: "num" | "word6" | "word5" }[] = [
        { pat: /(\d{1,2})\/(\d{1,2})\/(\d{2,4})\s*(?:[-–—]|thru|through|to)\s*(\d{1,2})\/(\d{1,2})\/(\d{2,4})/i, type: "num" },
        { pat: /(\w{3,9})\s+(\d{1,2})\s*,?\s*(\d{4})\s*(?:[-–—]|thru|through|to)\s*(\w{3,9})\s+(\d{1,2})\s*,?\s*(\d{4})/i, type: "word6" },
        { pat: /(\w{3,9})\s+(\d{1,2})\s*[-–—]\s*(\w{3,9})\s+(\d{1,2})\s*,?\s*(\d{4})/i, type: "word5" },
        { pat: /(\w{3,9})(\d{1,2})(\d{4})\s*[-–—]\s*(\w{3,9})(\d{1,2})(\d{4})/i, type: "word6" },
        { pat: /(\d{1,2})\/(\d{1,2})\/(\d{2,4})\s*\n\s*(?:through|thru|to)\s*\n?\s*(\d{1,2})\/(\d{1,2})\/(\d{2,4})/i, type: "num" },
        { pat: /(\w{3,9})\s+(\d{1,2})\s*,?\s*(\d{4})\s*\n\s*(?:through|thru|to)\s*\n?\s*(\w{3,9})\s+(\d{1,2})\s*,?\s*(\d{4})/i, type: "word6" },
        { pat: /(?:Beginning|From)\s+(\w{3,9})\s+(\d{1,2})\s*,?\s*(\d{4})\s*\n?\s*(?:through|thru|to)\s*\n?\s*(\w{3,9})\s+(\d{1,2})\s*,?\s*(\d{4})/i, type: "word6" },
      ];

      if (!month) for (const {pat, type} of dateRangePatterns) {
        const m = header.match(pat);
        if (!m) continue;
        if (type === "num") {
          const startMo = parseInt(m[1]);
          const sDay = parseInt(m[2]);
          const startYr = m[3];
          const endMo = parseInt(m[4]);
          const endDay = parseInt(m[5]);
          const endYr = m[6];
          if (endMo >= 1 && endMo <= 12 && startMo >= 1 && startMo <= 12) {
            month = pickStatementMonth(startMo, startYr, endMo, endDay, endYr, sDay);
            break;
          }
        } else if (type === "word6") {
          const startMo = monthFromWord(m[1]);
          const sDay = parseInt(m[2]);
          const startYr = m[3];
          const endMo = monthFromWord(m[4]);
          const endDay = parseInt(m[5]);
          const endYr = m[6];
          if (endMo) {
            if (startMo) {
              month = pickStatementMonth(startMo, startYr, endMo, endDay, endYr, sDay);
            } else {
              month = `${monthAbbrs[endMo]}${yrShort(endYr)}`;
            }
            break;
          }
        } else if (type === "word5") {
          const startMo = monthFromWord(m[1]);
          const sDay = parseInt(m[2]);
          const startYr = m[5];
          const endMo = monthFromWord(m[3]);
          const endDay = parseInt(m[4]);
          const endYr = m[5];
          if (endMo) {
            if (startMo) {
              month = pickStatementMonth(startMo, startYr, endMo, endDay, endYr, sDay);
            } else {
              month = `${monthAbbrs[endMo]}${yrShort(endYr)}`;
            }
            break;
          }
        }
      }

      if (!month) {
        const endingPatterns: { pat: RegExp; type: "num" | "word" }[] = [
          { pat: /statement\s+ending\s*[:\s]*(\d{1,2})\/(\d{1,2})\/(\d{2,4})/i, type: "num" },
          { pat: /statement\s+ending\s*[:\s]*(\w{3,9})\s+(\d{1,2})\s*,?\s*(\d{4})/i, type: "word" },
          { pat: /(?:period\s*ending|ending\s*(?:balance\s+on|date)|statement\s*(?:date|period|ending)|closing\s*date|ending\s+balance\s+on)[:\s]*(\d{1,2})\/(\d{1,2})\/(\d{2,4})/i, type: "num" },
          { pat: /(?:period\s*ending|ending\s*(?:balance\s+on|date)|statement\s*(?:date|period|ending)|closing\s*date|ending\s+balance\s+on)[:\s]*(\w{3,9})\s+(\d{1,2})\s*,?\s*(\d{4})/i, type: "word" },
          { pat: /ending\s+balance\s+on\s+(\w{3,9})\s+(\d{1,2})\s*,?\s*(\d{4})/i, type: "word" },
        ];
        for (const {pat, type} of endingPatterns) {
          const m = header.match(pat);
          if (!m) continue;
          if (type === "num") {
            const mo = parseInt(m[1]);
            if (mo >= 1 && mo <= 12) { month = `${monthAbbrs[mo]}${yrShort(m[3])}`; break; }
          } else {
            const mo = monthFromWord(m[1]);
            if (mo) { month = `${monthAbbrs[mo]}${yrShort(m[3])}`; break; }
          }
        }
      }

      if (!month) {
        const anyDate = header.match(/(\w{3,9})\s+\d{1,2}\s*,\s*(\d{4})/g);
        if (anyDate && anyDate.length >= 1) {
          const last = anyDate[anyDate.length > 1 ? 1 : 0];
          const parts = last?.match(/(\w{3,9})\s+\d{1,2}\s*,\s*(\d{4})/);
          if (parts) {
            const mo = monthFromWord(parts[1]);
            if (mo) month = `${monthAbbrs[mo]}${yrShort(parts[2])}`;
          }
        }
      }

      if (!month) {
        const bodySlice = cleaned.slice(0, 15000);
        const txnDates = bodySlice.match(/\b(\d{1,2})\/(\d{1,2})\/(\d{2,4})\b/g);
        if (txnDates && txnDates.length >= 3) {
          const monthCounts: Record<string, number> = {};
          for (const dt of txnDates) {
            const p = dt.match(/(\d{1,2})\/(\d{1,2})\/(\d{2,4})/);
            if (p) {
              const mo = parseInt(p[1]);
              if (mo >= 1 && mo <= 12) {
                const key = `${monthAbbrs[mo]}${yrShort(p[3])}`;
                monthCounts[key] = (monthCounts[key] || 0) + 1;
              }
            }
          }
          let bestMonth = "", bestCount = 0;
          for (const [k, v] of Object.entries(monthCounts)) {
            if (v > bestCount) { bestMonth = k; bestCount = v; }
          }
          if (bestMonth && bestCount >= 3) month = bestMonth;
        }
      }
      acct = extractAccountLast4(cleaned) || "";
      return { month, acct };
    }

    const storedTextMap = new Map<string, string>();
    for (const a of analyses) {
      if (a.extractedStatementText) {
        const fullText = String(a.extractedStatementText);
        const stmtRegex = /={5,}\s*STATEMENT\s+\d+:\s*"([^"]+)"\s*={5,}\s*\n([\s\S]*?)(?=={5,}\s*(?:END\s+STATEMENT|STATEMENT\s+\d)|$)/gi;
        let stmtMatch;
        while ((stmtMatch = stmtRegex.exec(fullText)) !== null) {
          const docName = stmtMatch[1].trim().toLowerCase();
          const docText = stmtMatch[2].trim();
          if (docName && docText) {
            storedTextMap.set(docName, docText);
          }
        }
        if (storedTextMap.size === 0) {
          const dashParts = fullText.split(/^---\s+(.+?)\s+---$/m).filter(Boolean);
          for (let i = 0; i < dashParts.length - 1; i += 2) {
            const name = dashParts[i]?.trim();
            const text = dashParts[i + 1]?.trim() || "";
            if (name && text) {
              storedTextMap.set(name.toLowerCase(), text);
            }
          }
        }
      }
    }

    let aiAcctGlobal = "";
    let aiBankGlobal = "";
    const aiMonthAccts = new Map<string, Array<{ acct: string; bankName: string }>>();
    for (const a of analyses) {
      if (a.aiRawAnalysis) {
        try {
          const raw = parseAIResponse(a.aiRawAnalysis);
          if (raw.accountNumber && !aiAcctGlobal) aiAcctGlobal = String(raw.accountNumber).replace(/\D/g, "").slice(-4);
          if (raw.bankName && !aiBankGlobal) aiBankGlobal = String(raw.bankName).trim();
          if (Array.isArray(raw.monthlyRevenues)) {
            for (const mr of raw.monthlyRevenues) {
              if (!aiAcctGlobal && mr.account) aiAcctGlobal = String(mr.account).replace(/\D/g, "").slice(-4);
              if (mr.month) {
                const pts = String(mr.month).split("-");
                if (pts.length === 2) {
                  const moNum = parseInt(pts[1]);
                  const yrStr = pts[0].slice(2);
                  if (moNum >= 1 && moNum <= 12) {
                    const key = `${monthAbbrs[moNum]}${yrStr}`;
                    const acctVal = mr.account ? String(mr.account).replace(/\D/g, "").slice(-4) : (aiAcctGlobal || "");
                    const bnk = mr.bankName ? String(mr.bankName).trim() : "";
                    if (!aiMonthAccts.has(key)) aiMonthAccts.set(key, []);
                    const arr = aiMonthAccts.get(key)!;
                    if (acctVal && !arr.some(e => e.acct === acctVal)) {
                      arr.push({ acct: acctVal, bankName: bnk });
                    } else if (!acctVal && arr.length === 0) {
                      arr.push({ acct: "", bankName: bnk });
                    }
                  }
                }
              }
            }
          }
        } catch {}
      }
    }

    const allAiMonths = new Set<string>();
    for (const key of aiMonthAccts.keys()) allAiMonths.add(key);

    function monthToSortKey(m: string): number {
      const moMap: Record<string, number> = { Jan:1,Feb:2,Mar:3,Apr:4,May:5,Jun:6,Jul:7,Aug:8,Sep:9,Oct:10,Nov:11,Dec:12 };
      const moStr = m.slice(0, 3);
      const yrStr = m.slice(3);
      const yr = yrStr.length === 2 ? 2000 + parseInt(yrStr) : parseInt(yrStr);
      return yr * 100 + (moMap[moStr] || 0);
    }

    function adjacentMonth(m: string, offset: number): string {
      const moMap: Record<string, number> = { Jan:1,Feb:2,Mar:3,Apr:4,May:5,Jun:6,Jul:7,Aug:8,Sep:9,Oct:10,Nov:11,Dec:12 };
      const moArr = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
      const moStr = m.slice(0, 3);
      const yrStr = m.slice(3);
      const yr = yrStr.length === 2 ? 2000 + parseInt(yrStr) : parseInt(yrStr);
      const mo = moMap[moStr] || 1;
      let newMo = mo + offset;
      let newYr = yr;
      while (newMo > 12) { newMo -= 12; newYr++; }
      while (newMo < 1) { newMo += 12; newYr--; }
      return `${moArr[newMo]}${String(newYr).slice(2)}`;
    }

    interface DocInfo { id: number; name: string; month: string; acct: string; bankName: string; fromText: boolean; confidence: "high" | "medium" | "low" }
    const docInfos: DocInfo[] = [];
    for (const bs of bankStatements) {
      if (bs.type !== "bank_statement") continue;
      let storedText = storedTextMap.get(bs.name.toLowerCase());

      if (!storedText) {
        const bsLower = bs.name.toLowerCase().replace(/\.[^.]+$/, "");
        for (const [key, val] of storedTextMap) {
          const keyClean = key.replace(/\.[^.]+$/, "");
          if (keyClean === bsLower || key.includes(bsLower) || bsLower.includes(keyClean)) {
            storedText = val;
            break;
          }
        }
      }

      if (!storedText && storedTextMap.size === 1) {
        storedText = storedTextMap.values().next().value;
      }

      if (!storedText) {
        for (const a of analyses) {
          if (a.extractedStatementText) {
            const fullText = String(a.extractedStatementText);
            if (fullText.length > 200 && !fullText.includes("=====") && !fullText.includes("---")) {
              storedText = fullText;
              break;
            }
          }
        }
      }

      if (storedText) {
        const headerCheck = storedText.slice(0, 2000);
        if (/asset\s+report/i.test(headerCheck) && !/(?:statement\s+period|account\s+summary|beginning\s+balance|ending\s+balance)/i.test(headerCheck)) {
          // console.log(`[TabLabel] Skipping "${bs.name}": detected as Asset Report`);
          continue;
        }
      }

      let month = "";
      let acct = "";
      let bankName = "";
      let fromText = false;
      let confidence: "high" | "medium" | "low" = "low";

      if (storedText) {
        const info = extractFromText(storedText);
        month = info.month;
        acct = info.acct;
        if (acct) fromText = true;
        if (month) confidence = "medium";
        // if (month || acct) console.log(`[TabLabel] Text match for "${bs.name}" → month=${month}, acct=${acct}`);
      }

      if (!month || !acct) {
        const fnMatch = bs.name.match(/[_\-]([A-Za-z]{3})(\d{2})[_\-](\d{4})/);
        if (fnMatch) {
          const fnMoWord = fnMatch[1].charAt(0).toUpperCase() + fnMatch[1].slice(1).toLowerCase();
          const moMap: Record<string, boolean> = { Jan:true,Feb:true,Mar:true,Apr:true,May:true,Jun:true,Jul:true,Aug:true,Sep:true,Oct:true,Nov:true,Dec:true };
          if (moMap[fnMoWord]) {
            if (!month) { month = `${fnMoWord}${fnMatch[2]}`; if (!confidence || confidence === "low") confidence = "medium"; }
            if (!acct) acct = fnMatch[3];
          }
        }
      }

      if (!acct) {
        const fnAcct = extractAccountFromFilename(bs.name);
        if (fnAcct) {
          acct = fnAcct;
        }
      }

      if (!bankName && aiBankGlobal) bankName = aiBankGlobal;
      docInfos.push({ id: bs.id, name: bs.name, month, acct, bankName, fromText, confidence });
    }

    if (allAiMonths.size > 0) {
      const aiMonthsList = Array.from(allAiMonths).sort((a, b) => monthToSortKey(a) - monthToSortKey(b));
      const assignedAiMonths = new Set<string>();

      for (const d of docInfos) {
        if (d.month && allAiMonths.has(d.month)) {
          d.confidence = "high";
          assignedAiMonths.add(d.month);
        }
      }

      for (const d of docInfos) {
        if (d.month && d.confidence !== "high") {
          const adj = [adjacentMonth(d.month, -1), adjacentMonth(d.month, 1)];
          for (const candidate of adj) {
            if (allAiMonths.has(candidate) && !assignedAiMonths.has(candidate)) {
              // console.log(`[TabLabel] Correcting ${d.name}: text said ${d.month} but AI has ${candidate} unassigned — using AI month`);
              d.month = candidate;
              d.confidence = "high";
              assignedAiMonths.add(candidate);
              break;
            }
          }
        }
      }

      const docsWithoutMonth = docInfos.filter(d => !d.month);
      if (docsWithoutMonth.length > 0) {
        const unassigned = aiMonthsList.filter(m => !assignedAiMonths.has(m));
        if (unassigned.length > 0) {
          const mnShort: Record<string, string[]> = {
            Jan: ["jan","january","01"], Feb: ["feb","february","02"], Mar: ["mar","march","03"],
            Apr: ["apr","april","04"], May: ["may","05"], Jun: ["jun","june","06"],
            Jul: ["jul","july","07"], Aug: ["aug","august","08"], Sep: ["sep","september","09"],
            Oct: ["oct","october","10"], Nov: ["nov","november","11"], Dec: ["dec","december","12"]
          };
          const sorted = [...unassigned].sort((a, b) => monthToSortKey(b) - monthToSortKey(a));
          const usedDocs = new Set<number>();

          for (const aiMonth of sorted) {
            const moAbbr = aiMonth.slice(0, 3);
            const yrSuffix = aiMonth.slice(3);
            const keywords = mnShort[moAbbr] || [moAbbr.toLowerCase()];
            const moNum = (Object.keys(mnShort).indexOf(moAbbr) + 1).toString();
            const moNumPad = moNum.padStart(2, "0");
            let bestDoc = -1;
            for (let i = 0; i < docsWithoutMonth.length; i++) {
              if (usedDocs.has(i)) continue;
              const fn = docsWithoutMonth[i].name.toLowerCase();
              const numericPattern = new RegExp(`(?:^|[^\\d])(?:${moNumPad}|${moNum})${yrSuffix}(?:[^\\d]|$)`);
              if (numericPattern.test(fn)) { bestDoc = i; break; }
              if (keywords.filter(kw => kw.length > 2).some(kw => fn.includes(kw))) { bestDoc = i; break; }
            }
            if (bestDoc === -1) {
              for (let i = 0; i < docsWithoutMonth.length; i++) {
                if (!usedDocs.has(i)) { bestDoc = i; break; }
              }
            }
            if (bestDoc >= 0) {
              // console.log(`[TabLabel] Assigning AI month ${aiMonth} to ${docsWithoutMonth[bestDoc].name}`);
              docsWithoutMonth[bestDoc].month = aiMonth;
              docsWithoutMonth[bestDoc].confidence = "high";
              assignedAiMonths.add(aiMonth);
              usedDocs.add(bestDoc);
            }
          }
        }
      }
    }

    const usedMonthAccts = new Set<string>();
    for (const d of docInfos) {
      if (d.acct && d.month) usedMonthAccts.add(`${d.month}|${d.acct}`);
    }

    const allUniqueAccts = new Set<string>();
    const allUniqueBanks = new Set<string>();
    for (const entries of aiMonthAccts.values()) {
      for (const e of entries) {
        if (e.acct) allUniqueAccts.add(e.acct);
        if (e.bankName) allUniqueBanks.add(e.bankName.toLowerCase());
      }
    }
    const isSingleBankLead = allUniqueAccts.size <= 1 && allUniqueBanks.size <= 1;
    for (const d of docInfos) {
      if (!d.acct && d.month && aiMonthAccts.has(d.month)) {
        const candidates = aiMonthAccts.get(d.month)!;
        const unused = candidates.find(c => !usedMonthAccts.has(`${d.month}|${c.acct}`));
        const pick = unused || candidates[0];
        d.acct = pick.acct;
        if (!d.bankName && pick.bankName) d.bankName = pick.bankName;
        usedMonthAccts.add(`${d.month}|${d.acct}`);
      }
      if (!d.acct && aiAcctGlobal && isSingleBankLead) d.acct = aiAcctGlobal;
    }

    function getDocText(d: DocInfo): string {
      let txt = storedTextMap.get(d.name.toLowerCase()) || "";
      if (!txt) {
        const dLower = d.name.toLowerCase().replace(/\.[^.]+$/, "");
        for (const [key, val] of storedTextMap) {
          const keyClean = key.replace(/\.[^.]+$/, "");
          if (keyClean === dLower || key.includes(dLower) || dLower.includes(keyClean)) { txt = val; break; }
        }
      }
      return txt;
    }

    function getTxnMonthCounts(txt: string): Record<string, number> {
      const body = txt.slice(0, 15000);
      const txnDates = body.match(/\b(\d{1,2})\/(\d{1,2})\/(\d{2,4})\b/g);
      const counts: Record<string, number> = {};
      if (!txnDates) return counts;
      for (const dt of txnDates) {
        const p = dt.match(/(\d{1,2})\/(\d{1,2})\/(\d{2,4})/);
        if (p) {
          const mo = parseInt(p[1]);
          if (mo >= 1 && mo <= 12) {
            const key2 = `${monthAbbrs[mo]}${p[3].length === 4 ? p[3].slice(2) : p[3]}`;
            counts[key2] = (counts[key2] || 0) + 1;
          }
        }
      }
      return counts;
    }

    const monthAcctGroups = new Map<string, DocInfo[]>();
    for (const d of docInfos) {
      if (d.month && d.acct) {
        const key = `${d.month}|${d.acct}`;
        if (!monthAcctGroups.has(key)) monthAcctGroups.set(key, []);
        monthAcctGroups.get(key)!.push(d);
      }
    }

    const allAssignedMonths = new Set(docInfos.map(d => d.month).filter(Boolean));

    for (const [_key, group] of monthAcctGroups) {
      if (group.length <= 1) continue;
      // console.log(`[TabLabel] Duplicate group detected: ${group.map(d => `"${d.name}"→${d.month}`).join(", ")}`);

      const docMonthData: Array<{ doc: DocInfo; counts: Record<string, number>; bestMonth: string; bestCount: number }> = [];
      for (const d of group) {
        const txt = getDocText(d);
        if (!txt) continue;
        const counts = getTxnMonthCounts(txt);
        let bestMonth = "", bestCount = 0;
        for (const [k, v] of Object.entries(counts)) {
          if (v > bestCount) { bestMonth = k; bestCount = v; }
        }
        docMonthData.push({ doc: d, counts, bestMonth, bestCount });
      }

      if (docMonthData.length < 2) continue;

      const distinctBestMonths = new Set(docMonthData.map(dm => dm.bestMonth).filter(Boolean));

      if (distinctBestMonths.size >= 2) {
        for (const dm of docMonthData) {
          if (dm.bestMonth && dm.bestCount >= 3 && dm.bestMonth !== dm.doc.month) {
            // console.log(`[TabLabel] Duplicate resolution: "${dm.doc.name}" relabeled from ${dm.doc.month} → ${dm.bestMonth} (${dm.bestCount} txn dates)`);
            dm.doc.month = dm.bestMonth;
          }
        }
      } else if (distinctBestMonths.size === 1) {
        const sharedBest = docMonthData[0].bestMonth;
        if (!sharedBest) continue;
        let bestHolder = docMonthData[0];
        for (const dm of docMonthData) {
          if (dm.bestCount > bestHolder.bestCount) bestHolder = dm;
        }

        if (bestHolder.doc.month !== sharedBest) {
          // console.log(`[TabLabel] Duplicate resolution: "${bestHolder.doc.name}" relabeled from ${bestHolder.doc.month} → ${sharedBest} (majority holder, ${bestHolder.bestCount} txn dates)`);
          bestHolder.doc.month = sharedBest;
        }

        for (const dm of docMonthData) {
          if (dm === bestHolder) continue;
          const sorted = Object.entries(dm.counts)
            .filter(([k]) => k !== sharedBest)
            .sort((a, b) => b[1] - a[1]);
          if (sorted.length > 0 && sorted[0][1] >= 2) {
            const altMonth = sorted[0][0];
            if (!allAssignedMonths.has(altMonth) || allAssignedMonths.has(dm.doc.month)) {
              // console.log(`[TabLabel] Duplicate resolution: "${dm.doc.name}" relabeled from ${dm.doc.month} → ${altMonth} (2nd-best txn month, ${sorted[0][1]} dates)`);
              dm.doc.month = altMonth;
              allAssignedMonths.add(altMonth);
            }
          } else {
            const candidatePrev = adjacentMonth(sharedBest, -1);
            const candidateNext = adjacentMonth(sharedBest, 1);
            const picked = !allAssignedMonths.has(candidatePrev) ? candidatePrev
              : !allAssignedMonths.has(candidateNext) ? candidateNext : null;
            if (picked) {
              // console.log(`[TabLabel] Duplicate resolution: "${dm.doc.name}" relabeled from ${dm.doc.month} → ${picked} (adjacent month fallback)`);
              dm.doc.month = picked;
              allAssignedMonths.add(picked);
            }
          }
        }
      }
    }

    for (const d of docInfos) {
      if (d.month || d.acct) {
        const label = d.month && d.acct ? `${d.month} ${d.acct}` : d.month || d.acct;
        const dupeKey = `${d.month}|${d.acct}`;
        const isDuplicate = dupeKey !== "|" && d.month !== "" && d.acct !== "" && seenMonthAcct.has(dupeKey);
        if (dupeKey !== "|") seenMonthAcct.add(dupeKey);
        docLabels[d.id] = { label, month: d.month, acct: d.acct, bankName: d.bankName, isDuplicate };
      }
    }

    res.json({
      lead: {
        id: lead.id, businessName: lead.businessName, ownerName: lead.ownerName,
        phone: lead.phone, email: lead.email, status: lead.status,
        riskCategory: lead.riskCategory, grossRevenue: lead.grossRevenue,
        monthlyRevenue: lead.monthlyRevenue, avgDailyBalance: lead.avgDailyBalance,
        revenueTrend: lead.revenueTrend, hasExistingLoans: lead.hasExistingLoans,
        loanCount: lead.loanCount, estimatedApproval: lead.estimatedApproval,
        industry: lead.industry, state: lead.state, city: lead.city,
      },
      financials: {
        grossDeposits: totalDeposits,
        totalLoanPayments,
        trueGross,
        holdbackPct,
        avgDailyBalance: lead.avgDailyBalance || 0,
        nsfCount: totalNsfCount,
        negativeDayCount: allNegativeDays.length,
        monthlyRevenues: allMonthlyRevenues,
      },
      checklist,
      loans: allLoans.map((l: any) => {
        const short = shortLenderName(l.lender || "");
        if (LENDER_CANONICAL_NAMES[short]) l.lender = LENDER_CANONICAL_NAMES[short];
        return l;
      }),
      recurringPulls: filteredRecurringPulls,
      totalRecurringPullCost,
      findings: {
        pending: pendingFindings,
        reviewed: reviewedFindings,
        all: confirmations,
      },
      notableTransactions: allTransactions.slice(0, 20),
      aiPerformance: {
        accuracy: aiAccuracy,
        totalReviewed: statsRow.total || 0,
        confirmed: statsRow.confirmed || 0,
        rejected: statsRow.rejected || 0,
        relabeled: statsRow.relabeled || 0,
      },
      statementsAnalyzed: analyses.length,
      statementsTotal: bankStatements.filter(d => d.type === "bank_statement" || d.classifiedType === "bank_statement").length,
      scrubPreview,
      docLabels,
    });
  } catch (e: any) {
    console.error("Scrubbing detail error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.get("/settings/scrub-writeback", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    const [row] = await db.select().from(appSettingsTable).where(eq(appSettingsTable.key, "scrub_writeback"));
    if (!row) { res.json({ enabled: false, spreadsheetId: "", sheetName: "", scrubFormat: "A" }); return; }
    res.json(JSON.parse(row.value));
  } catch (e: any) { res.status(500).json({ error: e.message }); }
});

router.post("/settings/scrub-writeback", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const { enabled, spreadsheetId: rawId, sheetName, writeColumn, scrubFormat, autoApproveConfident } = req.body;
    const col = (writeColumn || "A").replace(/[^A-Z]/gi, "").toUpperCase().slice(0, 2) || "A";
    const sName = (sheetName || "Sheet1").replace(/[!'"]/g, "").slice(0, 100) || "Sheet1";
    let resolvedId = (rawId || "").trim();
    const urlMatch = resolvedId.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/);
    if (urlMatch) resolvedId = urlMatch[1];
    const validFormats = ["A", "B", "C", "D", "E"];
    const fmt = validFormats.includes((scrubFormat || "").toUpperCase()) ? (scrubFormat as string).toUpperCase() : "A";
    const config = { enabled: !!enabled, spreadsheetId: resolvedId, sheetName: sName, writeColumn: col, scrubFormat: fmt, autoApproveConfident: !!autoApproveConfident };
    await db.insert(appSettingsTable).values({ key: "scrub_writeback", value: JSON.stringify(config) })
      .onConflictDoUpdate({ target: appSettingsTable.key, set: { value: JSON.stringify(config), updatedAt: new Date() } });
    res.json(config);
  } catch (e: any) { res.status(500).json({ error: e.message }); }
});

router.post("/settings/scrub-writeback/test", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    let config: any;
    if (req.body && req.body.spreadsheetId) {
      config = { ...req.body };
    } else {
      const [settingsRow] = await db.select().from(appSettingsTable).where(eq(appSettingsTable.key, "scrub_writeback"));
      if (!settingsRow) { res.json({ success: false, error: "No scrub writeback settings configured. Save settings first." }); return; }
      config = JSON.parse(settingsRow.value);
    }
    if (!config.enabled) { res.json({ success: false, error: "Scrub writeback is disabled" }); return; }
    const idUrlMatch = (config.spreadsheetId || "").match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/);
    if (idUrlMatch) config.spreadsheetId = idUrlMatch[1];
    if (!config.spreadsheetId) { res.json({ success: false, error: "No spreadsheet ID configured" }); return; }

    const sheets = await getUncachableGoogleSheetClient();
    const sheetName = config.sheetName || "Sheet1";

    const headerRes = await sheets.spreadsheets.values.get({
      spreadsheetId: config.spreadsheetId,
      range: `${sheetName}!1:1`,
    });
    const headers = (headerRes.data.values?.[0] || []).map((h: string) => h.toLowerCase().trim());
    if (headers.length === 0) { res.json({ success: false, error: "Sheet has no headers in row 1" }); return; }

    const ssnCol = headers.findIndex((h: string) => /ssn|social/i.test(h));
    const bizCol = headers.findIndex((h: string) => /business.*name|company|biz/i.test(h));
    const phoneCol = headers.findIndex((h: string) => /phone|tel/i.test(h));

    const lastCol = String.fromCharCode(65 + Math.min(headers.length - 1, 25));
    const allDataRes = await sheets.spreadsheets.values.get({
      spreadsheetId: config.spreadsheetId,
      range: `${sheetName}!A:${lastCol}`,
    });
    const rowCount = (allDataRes.data.values || []).length - 1;

    const writeCol = config.writeColumn || "A";
    const writeColIdx = writeCol.charCodeAt(0) - 65;
    const writeColHeader = headers[writeColIdx] || `(column ${writeCol})`;

    const scrubLeads = await db.select().from(leadsTable)
      .where(eq(leadsTable.status, "scrubbing_review"))
      .limit(5);
    const decrypted = scrubLeads.map(l => decryptLeadFields(l));

    let matchCount = 0;
    const allRows = allDataRes.data.values || [];
    const cleanPhone = (p: string) => (p || "").replace(/\D/g, "").slice(-10);
    for (const lead of decrypted) {
      const leadSsn = (lead.ssn || "").replace(/\D/g, "");
      const leadPhone = cleanPhone(lead.phone || "");
      const leadBiz = (lead.businessName || "").toLowerCase().trim();
      for (let r = 1; r < allRows.length; r++) {
        const row = allRows[r];
        if (leadSsn && leadSsn.length >= 9 && ssnCol >= 0 && row[ssnCol] && (row[ssnCol] || "").replace(/\D/g, "") === leadSsn) { matchCount++; break; }
        if (leadPhone && leadPhone.length === 10 && phoneCol >= 0 && row[phoneCol] && cleanPhone(row[phoneCol] || "") === leadPhone) { matchCount++; break; }
        if (leadBiz && leadBiz.length >= 4 && bizCol >= 0 && row[bizCol] && (row[bizCol] || "").toLowerCase().trim() === leadBiz) { matchCount++; break; }
      }
    }

    let writeAccess = false;
    let writeError = "";
    try {
      const testCell = `${sheetName}!${writeCol}1`;
      const currentVal = await sheets.spreadsheets.values.get({ spreadsheetId: config.spreadsheetId, range: testCell });
      const origVal = currentVal.data.values?.[0]?.[0] || "";
      await sheets.spreadsheets.values.update({
        spreadsheetId: config.spreadsheetId, range: testCell,
        valueInputOption: "RAW", requestBody: { values: [[origVal]] },
      });
      writeAccess = true;
    } catch (we: any) {
      writeError = we.message || "No write access";
    }

    res.json({
      success: true,
      sheetName,
      headerCount: headers.length,
      rowCount,
      writeColumn: writeCol,
      writeColumnHeader: writeColHeader,
      writeAccess,
      writeError: writeError || undefined,
      matchColumns: {
        ssn: ssnCol >= 0 ? headers[ssnCol] : null,
        businessName: bizCol >= 0 ? headers[bizCol] : null,
        phone: phoneCol >= 0 ? headers[phoneCol] : null,
      },
      sampleLeadsChecked: decrypted.length,
      matchesFound: matchCount,
      message: `Connected! Found ${rowCount} rows. ${writeAccess ? "Write access: YES." : "Write access: NO — " + writeError} Match columns: ${[ssnCol >= 0 ? "SSN" : null, bizCol >= 0 ? "Business" : null, phoneCol >= 0 ? "Phone" : null].filter(Boolean).join(", ") || "NONE"}.${decrypted.length > 0 ? ` ${matchCount}/${decrypted.length} sample leads found in sheet.` : ""}`
    });
  } catch (e: any) {
    console.error("[Scrub Writeback Test]", e.message);
    res.json({ success: false, error: e.message });
  }
});

async function tryAutoApprove(leadId: number): Promise<{ autoApproved: boolean; reason: string }> {
  try {
    const [settingsRow] = await db.select().from(appSettingsTable).where(eq(appSettingsTable.key, "scrub_writeback"));
    if (!settingsRow) return { autoApproved: false, reason: "no_settings" };
    const config = JSON.parse(settingsRow.value);
    if (!config.autoApproveConfident) return { autoApproved: false, reason: "auto_approve_off" };
    if (!config.enabled || !config.spreadsheetId) return { autoApproved: false, reason: "writeback_disabled" };

    const analyses = await db.select().from(bankStatementAnalysesTable)
      .where(eq(bankStatementAnalysesTable.leadId, leadId));
    if (analyses.length === 0) return { autoApproved: false, reason: "no_analyses" };

    const totalDocs = await db.select({ count: sql<number>`count(*)` }).from(documentsTable)
      .where(and(eq(documentsTable.leadId, leadId), eq(documentsTable.type, "bank_statement")));
    const docCount = Number(totalDocs[0]?.count || 0);

    const analyzedDocIds = new Set(analyses.map(a => a.documentId).filter(Boolean));

    if (docCount > 0 && analyzedDocIds.size < docCount) {
      console.log(`[AutoApprove] Lead #${leadId}: only ${analyzedDocIds.size}/${docCount} bank statement docs analyzed — files may have failed, sending to manual review`);
      return { autoApproved: false, reason: "incomplete_analysis" };
    }

    const uniqueMonths = new Set(analyses.map(a => a.statementMonth).filter(Boolean));
    if (docCount >= 3 && uniqueMonths.size <= 1) {
      console.log(`[AutoApprove] Lead #${leadId}: ${docCount} docs uploaded but only ${uniqueMonths.size} month(s) analyzed — likely missing data, sending to manual review`);
      return { autoApproved: false, reason: "insufficient_months" };
    }

    let hasReviewFlags = false;
    for (const a of analyses) {
      const revs = (a.monthlyRevenues as any[]) || [];
      for (const rev of revs) {
        if (rev.needsReview) {
          hasReviewFlags = true;
          break;
        }
      }
      if (hasReviewFlags) break;
    }
    if (hasReviewFlags) {
      console.log(`[AutoApprove] Lead #${leadId}: has review flags in monthly revenues, sending to manual review`);
      return { autoApproved: false, reason: "needs_review" };
    }

    let hasRevenue = false;
    for (const a of analyses) {
      if (a.grossRevenue && parseNumeric(a.grossRevenue) > 0) { hasRevenue = true; break; }
    }
    if (!hasRevenue) {
      console.log(`[AutoApprove] Lead #${leadId}: no revenue detected, sending to manual review`);
      return { autoApproved: false, reason: "no_revenue" };
    }

    const pendingConfirmations = await db.select().from(underwritingConfirmationsTable)
      .where(and(eq(underwritingConfirmationsTable.leadId, leadId), eq(underwritingConfirmationsTable.status, "pending")));
    if (pendingConfirmations.length > 0) {
      let remainingPending = 0;
      for (const pc of pendingConfirmations) {
        const val = pc.originalValue as any;
        const lenderName = (val?.lender || val?.likelyLender || "").trim();
        if (lenderName && KNOWN_LENDER_SAFELIST.test(lenderName)) {
          await db.update(underwritingConfirmationsTable)
            .set({ status: "confirmed", adminNotes: "Auto-confirmed: recognized known lender (auto-approve)", confirmedAt: new Date() })
            .where(eq(underwritingConfirmationsTable.id, pc.id));
          console.log(`[AutoApprove] Lead #${leadId}: auto-confirmed known lender "${lenderName}"`);
        } else {
          remainingPending++;
        }
      }
      if (remainingPending > 0) {
        console.log(`[AutoApprove] Lead #${leadId}: ${remainingPending} pending loan confirmations (unknown lenders), sending to manual review`);
        return { autoApproved: false, reason: "pending_confirmations" };
      }
    }

    console.log(`[AutoApprove] Lead #${leadId}: 100% confident, auto-approving and writing to sheet`);
    let [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!lead) return { autoApproved: false, reason: "lead_not_found" };
    lead = decryptLeadFields(lead);

    await db.update(leadsTable).set({ status: "scrubbed", sheetWritebackStatus: null }).where(eq(leadsTable.id, leadId));

    const { activeLoans: allLoans } = buildActiveLoansFromMostRecent(analyses);
    fixLoanFrequencies(allLoans, 1);

    try {
      const { getLenderVerdicts } = await import("./coreController");
      const verdicts = await getLenderVerdicts();
      const rejectedLenders = new Set<string>();
      for (const [lender, verdict] of verdicts) {
        if (verdict.verdict === "rejected") rejectedLenders.add(lender.toLowerCase().replace(/[^a-z0-9]/g, ""));
      }
      const filteredLoans = allLoans.filter(loan => {
        const norm = (loan.lender || "").toLowerCase().replace(/[^a-z0-9]/g, "");
        return !rejectedLenders.has(norm);
      });

      const revEntries = parseRevEntries(analyses);
      const loanEntries = parseLoanEntries(filteredLoans);
      const scrubData = formatScrubData(revEntries, loanEntries, config.scrubFormat || "A");

      const sheets = await getUncachableGoogleSheetClient();
      const sheetName = config.sheetName || "Sheet1";
      const headerRes = await sheets.spreadsheets.values.get({ spreadsheetId: config.spreadsheetId, range: `${sheetName}!1:1` });
      const headers = (headerRes.data.values?.[0] || []).map((h: string) => h.toLowerCase().trim());
      if (headers.length === 0) throw new Error("no_headers");

      const lastCol = String.fromCharCode(65 + Math.min(headers.length - 1, 25));
      const allDataRes = await sheets.spreadsheets.values.get({ spreadsheetId: config.spreadsheetId, range: `${sheetName}!A:${lastCol}` });
      const allRows = allDataRes.data.values || [];
      const ssnCol = headers.findIndex((h: string) => /ssn|social/i.test(h));
      const bizCol = headers.findIndex((h: string) => /business.*name|company|biz/i.test(h));
      const phoneCol = headers.findIndex((h: string) => /phone|tel/i.test(h));
      const cleanPhone = (p: string) => (p || "").replace(/\D/g, "").slice(-10);

      let matchedRowIdx = -1;
      const leadSsn = (lead.ssn || "").replace(/\D/g, "");
      const leadPhone = cleanPhone(lead.phone || "");
      const leadBiz = (lead.businessName || "").toLowerCase().trim();

      for (let r = 1; r < allRows.length; r++) {
        const row = allRows[r];
        if (leadSsn && leadSsn.length >= 9 && ssnCol >= 0 && row[ssnCol]) {
          if ((row[ssnCol] || "").replace(/\D/g, "") === leadSsn) { matchedRowIdx = r; break; }
        }
        if (leadPhone && leadPhone.length === 10 && phoneCol >= 0 && row[phoneCol]) {
          if (cleanPhone(row[phoneCol] || "") === leadPhone) { matchedRowIdx = r; break; }
        }
        if (leadBiz && leadBiz.length >= 4 && bizCol >= 0 && row[bizCol]) {
          if ((row[bizCol] || "").toLowerCase().trim() === leadBiz) { matchedRowIdx = r; break; }
        }
      }

      if (matchedRowIdx >= 0) {
        const writeCol = config.writeColumn || "A";
        await sheets.spreadsheets.values.update({
          spreadsheetId: config.spreadsheetId,
          range: `${sheetName}!${writeCol}${matchedRowIdx + 1}`,
          valueInputOption: "RAW",
          requestBody: { values: [[scrubData]] },
        });
        await db.update(leadsTable).set({ sheetWritebackStatus: "written" }).where(eq(leadsTable.id, leadId));
        console.log(`[AutoApprove] Lead #${leadId}: wrote to sheet row ${matchedRowIdx + 1}, data: ${scrubData}`);
      } else {
        await db.update(leadsTable).set({ sheetWritebackStatus: "no_match" }).where(eq(leadsTable.id, leadId));
        console.log(`[AutoApprove] Lead #${leadId}: auto-approved but no sheet match found`);
      }
    } catch (sheetErr: any) {
      console.error(`[AutoApprove] Sheet write error for lead #${leadId}:`, sheetErr.message);
      await db.update(leadsTable).set({ sheetWritebackStatus: "error" }).where(eq(leadsTable.id, leadId));
    }
    return { autoApproved: true, reason: "auto_approved" };
  } catch (e: any) {
    console.error(`[AutoApprove] Error for lead #${leadId}:`, e.message);
    return { autoApproved: false, reason: `error: ${e.message}` };
  }
}

export { tryAutoApprove };

router.post("/scrubbing/approve/:leadId", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const leadId = parseInt(String(req.params.leadId), 10);
    let [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
    if (!lead) { res.status(404).json({ error: "Lead not found" }); return; }
    lead = decryptLeadFields(lead);

    await db.update(leadsTable).set({ status: "scrubbed", sheetWritebackStatus: null }).where(eq(leadsTable.id, leadId));

    const analyses = await db.select().from(bankStatementAnalysesTable)
      .where(eq(bankStatementAnalysesTable.leadId, leadId))
      .orderBy(sql`${bankStatementAnalysesTable.createdAt} DESC`);

    const { activeLoans: allLoans } = buildActiveLoansFromMostRecent(analyses);
    fixLoanFrequencies(allLoans, 1);

    const user = (req as any).user;

    await db.update(underwritingConfirmationsTable)
      .set({ status: "confirmed", adminNotes: "Auto-confirmed via scrub approval", confirmedAt: new Date(), confirmedById: user?.id })
      .where(and(eq(underwritingConfirmationsTable.leadId, leadId), eq(underwritingConfirmationsTable.status, "pending")));

    const confirmations = await db.select().from(underwritingConfirmationsTable)
      .where(eq(underwritingConfirmationsTable.leadId, leadId));
    const rejectedLenders = new Set<string>();
    for (const c of confirmations) {
      if (c.status === "rejected") {
        const val = c.originalValue as any;
        const lender = (val?.lender || "").toLowerCase().replace(/[^a-z0-9]/g, "");
        if (lender) rejectedLenders.add(lender);
      }
    }
    try {
      const { getLenderVerdicts } = await import("./coreController");
      const verdicts = await getLenderVerdicts();
      for (const [lender, verdict] of verdicts) {
        if (verdict.verdict === "rejected") rejectedLenders.add(lender.toLowerCase().replace(/[^a-z0-9]/g, ""));
      }
    } catch {}
    const filteredLoans = allLoans.filter(loan => {
      const lenderNorm = (loan.lender || "").toLowerCase().replace(/[^a-z0-9]/g, "");
      return !rejectedLenders.has(lenderNorm);
    });

    const SKIP_LEARN = /^(unknown|n\/a|none|other|unidentified|pending)$/i;
    const seenLenders = new Set<string>();
    for (const loan of filteredLoans) {
      const lenderName = (loan.lender || "").trim();
      if (!lenderName || SKIP_LEARN.test(lenderName)) continue;
      const norm = normalizeLenderKey(lenderName);
      if (seenLenders.has(norm)) continue;
      seenLenders.add(norm);
      await saveLenderRule(lenderName, "confirmed", "Learned from manual scrub approval", user?.id, { skipIfRejected: true });
    }
    if (seenLenders.size > 0) {
      invalidateVerdictCache();
      console.log(`[Scrub Approve] Lead #${leadId}: learned ${seenLenders.size} lender(s) as confirmed: ${[...seenLenders].join(", ")}`);
    }

    let sheetWritten = false;
    let writebackAttempted = false;
    let writebackReason = "";
    let writebackError = "";
    let scrubData = "no stmnts";
    try {
      const [settingsRow] = await db.select().from(appSettingsTable).where(eq(appSettingsTable.key, "scrub_writeback"));
      console.log(`[Scrub Approve] Lead #${leadId}, settings found: ${!!settingsRow}`);
      if (settingsRow) {
        const config = JSON.parse(settingsRow.value);
        const idUrlMatch = (config.spreadsheetId || "").match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/);
        if (idUrlMatch) config.spreadsheetId = idUrlMatch[1];
        const revEntries = parseRevEntries(analyses);
        const loanEntries = parseLoanEntries(filteredLoans);
        scrubData = formatScrubData(revEntries, loanEntries, config.scrubFormat || "A");
        console.log(`[Scrub Approve] Config: enabled=${config.enabled}, sheetId=${config.spreadsheetId || "empty"}`);
        if (!config.enabled) { writebackReason = "disabled"; }
        else if (!config.spreadsheetId) { writebackReason = "no_spreadsheet"; }
        if (config.enabled && config.spreadsheetId) {
          writebackAttempted = true;
          const sheets = await getUncachableGoogleSheetClient();
          const sheetName = config.sheetName || "Sheet1";

          const headerRes = await sheets.spreadsheets.values.get({
            spreadsheetId: config.spreadsheetId,
            range: `${sheetName}!1:1`,
          });
          const headers = (headerRes.data.values?.[0] || []).map((h: string) => h.toLowerCase().trim());
          if (headers.length === 0) { console.log("[Scrub Approve] Sheet has no headers, skipping writeback"); throw new Error("skip"); }

          const lastCol = String.fromCharCode(65 + Math.min(headers.length - 1, 25));
          const allDataRes = await sheets.spreadsheets.values.get({
            spreadsheetId: config.spreadsheetId,
            range: `${sheetName}!A:${lastCol}`,
          });
          const allRows = allDataRes.data.values || [];

          const ssnCol = headers.findIndex((h: string) => /ssn|social/i.test(h));
          const bizCol = headers.findIndex((h: string) => /business.*name|company|biz/i.test(h));
          const phoneCol = headers.findIndex((h: string) => /phone|tel/i.test(h));
          const ownerCol = headers.findIndex((h: string) => /owner|contact.*name|first.*name/i.test(h));
          console.log(`[Scrub Approve] Sheet: ${headers.length} headers, ${allRows.length - 1} rows. Match cols: ssn=${ssnCol}, biz=${bizCol}, phone=${phoneCol}`);

          const cleanPhone = (p: string) => (p || "").replace(/\D/g, "").slice(-10);

          let matchedRowIdx = -1;
          const leadSsn = (lead.ssn || "").replace(/\D/g, "");
          const leadPhone = cleanPhone(lead.phone || "");
          const leadBiz = (lead.businessName || "").toLowerCase().trim();
          console.log(`[Scrub Approve] Lead #${leadId}: has ssn=${leadSsn.length >= 9}, phone=${leadPhone.length === 10}, bizLen=${leadBiz.length}`);

          for (let r = 1; r < allRows.length; r++) {
            const row = allRows[r];
            if (leadSsn && leadSsn.length >= 9 && ssnCol >= 0 && row[ssnCol]) {
              const sheetSsn = (row[ssnCol] || "").replace(/\D/g, "");
              if (sheetSsn.length >= 9 && sheetSsn === leadSsn) { matchedRowIdx = r; break; }
            }
            if (leadPhone && leadPhone.length === 10 && phoneCol >= 0 && row[phoneCol]) {
              const sheetPhone = cleanPhone(row[phoneCol] || "");
              if (sheetPhone.length === 10 && sheetPhone === leadPhone) { matchedRowIdx = r; break; }
            }
            if (leadBiz && leadBiz.length >= 4 && bizCol >= 0 && row[bizCol]) {
              const sheetBiz = (row[bizCol] || "").toLowerCase().trim();
              if (sheetBiz && sheetBiz === leadBiz) { matchedRowIdx = r; break; }
            }
          }

          if (matchedRowIdx >= 0) {
            const writeCol = config.writeColumn || "A";
            const cellRef = `${sheetName}!${writeCol}${matchedRowIdx + 1}`;
            await sheets.spreadsheets.values.update({
              spreadsheetId: config.spreadsheetId,
              range: cellRef,
              valueInputOption: "RAW",
              requestBody: { values: [[scrubData]] },
            });
            sheetWritten = true;
            writebackReason = "written";
            console.log(`[Scrub Approve] Wrote to sheet row ${matchedRowIdx + 1} col ${writeCol} for lead #${leadId}`);
          } else {
            writebackReason = "no_match";
            console.log(`[Scrub Approve] No matching row found in sheet for lead #${leadId}`);
          }
        }
      }
    } catch (e: any) {
      if (e.message !== "skip") {
        writebackReason = "error";
        writebackError = e.message || "Unknown error";
      }
      console.error(`[Scrub Approve] Sheet write error for lead #${leadId}:`, e.message, e.response?.data || "");
    }

    if (writebackAttempted) {
      await db.update(leadsTable).set({ sheetWritebackStatus: writebackReason }).where(eq(leadsTable.id, leadId));
    }

    res.json({ success: true, status: "scrubbed", sheetWritten, writebackAttempted, writebackReason, writebackError, scrubData });
  } catch (e: any) {
    console.error("Scrubbing approve error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.post("/admin/wipe-analyses", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  if (user.role !== "super_admin") {
    res.status(403).json({ error: "Super admin only" });
    return;
  }
  try {
    backgroundJobs.clear();
    const [{ cnt }] = await db.select({ cnt: count() }).from(bankStatementAnalysesTable);
    await db.execute(sql`DELETE FROM bank_statement_analyses`);

    await db.update(leadsTable)
      .set({ status: "new", bankStatementsStatus: null, sheetWritebackStatus: null })
      .where(inArray(leadsTable.status, ["scrubbing_review", "scrubbed"]));

    res.json({ success: true, deletedAnalyses: Number(cnt), message: "Analyses wiped — documents preserved. Visit Import page to trigger re-analysis." });
  } catch (e: any) {
    console.error("Wipe analyses error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.post("/admin/wipe-statements", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  if (user.role !== "super_admin") {
    res.status(403).json({ error: "Super admin only" });
    return;
  }
  try {
    const affectedLeadsResult = await db.select({ leadId: documentsTable.leadId }).from(documentsTable).where(eq(documentsTable.type, "bank_statement"));
    const leadIds = [...new Set(affectedLeadsResult.map(r => r.leadId))];

    await db.execute(sql`DELETE FROM bank_statement_analyses`);
    await db.execute(sql`DELETE FROM documents WHERE type = 'bank_statement'`);
    await db.execute(sql`DELETE FROM upload_batches`);

    if (leadIds.length > 0) {
      await db.update(leadsTable)
        .set({ status: "new", bankStatementsStatus: null, sheetWritebackStatus: null })
        .where(and(
          inArray(leadsTable.id, leadIds),
          inArray(leadsTable.status, ["scrubbing_review", "scrubbed"])
        ));
    }

    res.json({ success: true, clearedDocuments: true, resetLeads: leadIds.length });
  } catch (e: any) {
    console.error("Wipe statements error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.post("/admin/wipe-leads", requireAuth, async (req, res): Promise<void> => {
  const user = (req as any).user;
  if (user.role !== "super_admin" && user.role !== "admin") {
    res.status(403).json({ error: "Admin access required" });
    return;
  }
  try {
    backgroundJobs.clear();

    await db.execute(sql`UPDATE underwriting_confirmations SET analysis_id = NULL, lead_id = NULL WHERE analysis_id IS NOT NULL OR lead_id IS NOT NULL`);
    await db.execute(sql`TRUNCATE TABLE bank_statement_analyses CASCADE`);
    await db.execute(sql`TRUNCATE TABLE documents CASCADE`);
    await db.execute(sql`TRUNCATE TABLE deals CASCADE`);
    await db.execute(sql`TRUNCATE TABLE notifications CASCADE`);
    await db.execute(sql`TRUNCATE TABLE activities CASCADE`);
    await db.execute(sql`TRUNCATE TABLE submissions CASCADE`);
    await db.execute(sql`TRUNCATE TABLE leads CASCADE`);
    await db.execute(sql`TRUNCATE TABLE import_batches CASCADE`);
    await db.execute(sql`TRUNCATE TABLE upload_batches CASCADE`);
    const [{ cnt }] = await db.select({ cnt: count() }).from(leadsTable);
    res.json({ success: true, remaining: Number(cnt) });
  } catch (e: any) {
    console.error("Wipe error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.post("/admin/reset-lead-scrub/:leadId", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const leadId = parseInt(String(req.params.leadId), 10);
    if (isNaN(leadId)) { res.status(400).json({ error: "Invalid lead ID" }); return; }

    const delAnalyses = await db.delete(bankStatementAnalysesTable).where(eq(bankStatementAnalysesTable.leadId, leadId)).returning();
    const delDocs = await db.delete(documentsTable).where(and(eq(documentsTable.leadId, leadId), eq(documentsTable.type, "bank_statement"))).returning();

    await db.update(leadsTable).set({
      bankStatementsStatus: null, riskCategory: null, grossRevenue: null,
      avgDailyBalance: null, revenueTrend: null, hasExistingLoans: null,
      loanCount: null, hasOnDeck: null, estimatedApproval: null,
      status: "new", sheetWritebackStatus: null,
    }).where(eq(leadsTable.id, leadId));

    res.json({ success: true, deletedAnalyses: delAnalyses.length, deletedDocs: delDocs.length });
  } catch (e: any) {
    console.error("Reset lead scrub error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.post("/admin/fix-scrub-status", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    let totalFixed = 0;
    const fixedLeads: any[] = [];

    const scrubbedOrReview = await db.select({ id: leadsTable.id, businessName: leadsTable.businessName, status: leadsTable.status })
      .from(leadsTable)
      .where(inArray(leadsTable.status, ["scrubbed", "scrubbing_review", "scrubbing"]));

    for (const lead of scrubbedOrReview) {
      const docs = await db.select({ count: sql<number>`count(*)` }).from(documentsTable)
        .where(and(eq(documentsTable.leadId, lead.id), eq(documentsTable.type, "bank_statement")));
      const docCount = Number(docs[0]?.count || 0);
      const analyses = await db.select({ count: sql<number>`count(*)` }).from(bankStatementAnalysesTable)
        .where(eq(bankStatementAnalysesTable.leadId, lead.id));
      const analysisCount = Number(analyses[0]?.count || 0);

      if (docCount === 0 && analysisCount === 0) {
        await db.update(leadsTable).set({
          status: "new", bankStatementsStatus: null, riskCategory: null,
          grossRevenue: null, avgDailyBalance: null, revenueTrend: null,
          hasExistingLoans: null, loanCount: null, hasOnDeck: null,
          estimatedApproval: null, sheetWritebackStatus: null,
        }).where(eq(leadsTable.id, lead.id));
        await db.delete(underwritingConfirmationsTable).where(eq(underwritingConfirmationsTable.leadId, lead.id));
        fixedLeads.push({ id: lead.id, name: lead.businessName, was: lead.status, now: "new", reason: "no_docs" });
        totalFixed++;
      }
    }

    const leadsWithAnalyses = await db.select({ leadId: bankStatementAnalysesTable.leadId })
      .from(bankStatementAnalysesTable)
      .groupBy(bankStatementAnalysesTable.leadId);
    const leadIds = leadsWithAnalyses.map(r => r.leadId).filter(Boolean);

    if (leadIds.length > 0) {
      const stuckLeads = await db.select({ id: leadsTable.id, businessName: leadsTable.businessName })
        .from(leadsTable)
        .where(and(inArray(leadsTable.id, leadIds as number[]), eq(leadsTable.status, "new")));
      if (stuckLeads.length > 0) {
        const stuckIds = stuckLeads.map(l => l.id);
        await db.update(leadsTable).set({ status: "scrubbing_review" }).where(inArray(leadsTable.id, stuckIds));
        for (const l of stuckLeads) fixedLeads.push({ id: l.id, name: l.businessName, was: "new", now: "scrubbing_review", reason: "has_analyses" });
        totalFixed += stuckIds.length;
      }
    }

    console.log(`[Fix] Fixed ${totalFixed} leads:`, fixedLeads.map(l => `${l.id}:${l.name}(${l.reason})`).join(", "));
    res.json({ success: true, updated: totalFixed, leads: fixedLeads });
  } catch (e: any) {
    console.error("Fix scrub status error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.post("/admin/rescrub-all", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const running = [...backgroundJobs.values()].find(j => j.status === "running");
    if (running) {
      res.status(409).json({ error: "A scrub job is already running. Cancel it first or wait." });
      return;
    }

    const { includeCompleted = true, onlyStatus } = req.body || {};

    let statusFilters: string[];
    if (onlyStatus) {
      statusFilters = Array.isArray(onlyStatus) ? onlyStatus : [onlyStatus];
    } else {
      statusFilters = ["scrubbing", "scrubbing_review"];
      if (includeCompleted) statusFilters.push("approved", "declined", "offer_sent", "funded");
    }

    const leadsWithDocs = await db.selectDistinct({ leadId: documentsTable.leadId })
      .from(documentsTable)
      .innerJoin(leadsTable, eq(documentsTable.leadId, leadsTable.id))
      .where(and(
        eq(documentsTable.type, "bank_statement"),
        inArray(leadsTable.status, statusFilters)
      ));

    const allLeadIds = leadsWithDocs.map(r => r.leadId).filter((id): id is number => id !== null);
    if (allLeadIds.length === 0) {
      res.json({ success: true, total: 0, message: `No leads with bank statements found in status: ${statusFilters.join(", ")}` });
      return;
    }

    for (const leadId of allLeadIds) {
      await db.delete(bankStatementAnalysesTable).where(eq(bankStatementAnalysesTable.leadId, leadId));
      await db.update(documentsTable).set({ cachedRawText: null }).where(eq(documentsTable.leadId, leadId));
    }

    console.log(`[ReScrub-All] Starting re-scrub of ${allLeadIds.length} leads (deleted old analyses, cleared cached text)`);

    const jobId = `rescrub_all_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    resetScrubCancel();
    const job: BackgroundJob = {
      id: jobId, status: "running", total: allLeadIds.length,
      processed: 0, currentLead: "Starting re-scrub...", results: [], startedAt: Date.now(),
    };
    backgroundJobs.set(jobId, job);

    res.json({ success: true, jobId, total: allLeadIds.length, message: `Re-scrubbing ${allLeadIds.length} leads in background` });

    runConcurrentBatch(allLeadIds, async (leadId: number) => {
      let leadName = "Unknown";
      try {
        const [lead] = await db.select().from(leadsTable).where(eq(leadsTable.id, leadId));
        if (!lead) { job.results.push({ leadId, businessName: "Unknown", status: "error", error: "Lead not found" }); job.processed++; return; }
        leadName = lead.businessName || "Unknown";
        job.currentLead = leadName;

        const { analysis } = await analyzeSingleLead(leadId);
        const isEmpty = !analysis.grossRevenue && !analysis.hasExistingLoans;
        if (isEmpty) {
          console.log(`[ReScrub-All] Empty analysis for "${leadName}" — resetting to new`);
          await db.update(leadsTable).set({ status: "new" }).where(eq(leadsTable.id, leadId));
          job.results.push({ leadId, businessName: leadName, status: "empty", riskScore: analysis.riskScore });
        } else {
          const autoResult = await tryAutoApprove(leadId);
          if (!autoResult.autoApproved) {
            await db.update(leadsTable).set({ status: "scrubbing_review" }).where(eq(leadsTable.id, leadId));
          }
          job.results.push({ leadId, businessName: leadName, status: autoResult.autoApproved ? "auto_approved" : "success", riskScore: analysis.riskScore });
        }
      } catch (err: any) {
        console.error(`[ReScrub-All] Error for "${leadName}" (#${leadId}):`, err.message);
        job.results.push({ leadId, businessName: leadName, status: "error", error: err.message });
      } finally {
        job.processed++;
      }
    }).then(() => {
      job.status = "completed";
      job.currentLead = "Done";
      const successes = job.results.filter(r => r.status === "success").length;
      const empties = job.results.filter(r => r.status === "empty").length;
      const errors = job.results.filter(r => r.status === "error").length;
      console.log(`[ReScrub-All] Complete: ${successes} success, ${empties} empty, ${errors} errors out of ${job.total}`);
    }).catch(err => {
      job.status = "error";
      console.error("[ReScrub-All] Fatal error:", err.message);
    });
  } catch (e: any) {
    console.error("Re-scrub all error:", e);
    res.status(500).json({ error: e.message });
  }
});

router.get("/admin/ai-costs", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    res.json(costTracker.getSummary());
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

router.get("/admin/ai-costs/lead/:leadId", requireAuth, requireAdmin, async (req, res): Promise<void> => {
  try {
    const leadId = parseInt(String(req.params.leadId), 10);
    res.json(costTracker.getLeadCost(leadId));
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

router.post("/admin/retry-auto-approve", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    const needsReviewLeads = await db.select({ id: leadsTable.id, businessName: leadsTable.businessName })
      .from(leadsTable)
      .where(eq(leadsTable.status, "scrubbing_review"));
    if (needsReviewLeads.length === 0) {
      res.json({ success: true, approved: 0, message: "No leads in Needs Review" });
      return;
    }
    let approved = 0;
    const results: any[] = [];
    for (const lead of needsReviewLeads) {
      const result = await tryAutoApprove(lead.id);
      if (result.autoApproved) approved++;
      results.push({ leadId: lead.id, businessName: lead.businessName, ...result });
    }
    console.log(`[RetryAutoApprove] Processed ${needsReviewLeads.length} leads, auto-approved ${approved}`);
    res.json({ success: true, total: needsReviewLeads.length, approved, results });
  } catch (e: any) {
    console.error("[RetryAutoApprove] Error:", e.message);
    res.status(500).json({ error: e.message });
  }
});

router.get("/scrubbing/lender-crossref", requireAuth, requireAdmin, async (_req, res): Promise<void> => {
  try {
    const [settingsRow] = await db.select().from(appSettingsTable).where(eq(appSettingsTable.key, "scrub_writeback"));
    if (!settingsRow) { res.json({ error: "Scrub writeback not configured" }); return; }
    const config = JSON.parse(settingsRow.value);
    if (!config.enabled || !config.spreadsheetId) { res.json({ error: "Scrub writeback disabled or no sheet configured" }); return; }

    let spreadsheetId = (config.spreadsheetId || "").trim();
    const urlMatch = spreadsheetId.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/);
    if (urlMatch) spreadsheetId = urlMatch[1];

    const sheets = await getUncachableGoogleSheetClient();
    const sheetName = `'${(config.sheetName || "Sheet1").replace(/'/g, "''")}'`;

    const colLetterToIdx = (col: string): number => {
      let idx = 0;
      for (let i = 0; i < col.length; i++) {
        idx = idx * 26 + (col.charCodeAt(i) - 64);
      }
      return idx - 1;
    };

    const writeCol = (config.writeColumn || "A").toUpperCase();
    const writeColIdx = colLetterToIdx(writeCol);
    const maxColLetter = writeColIdx >= 25 ? "AZ" : "Z";

    const allDataRes = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: `${sheetName}!A:${maxColLetter}`,
    });
    const allRows = allDataRes.data.values || [];
    if (allRows.length < 2) { res.json({ unknownLenders: [], sheetLenders: [], totalRowsScanned: 0 }); return; }

    const headers = (allRows[0] || []).map((h: string) => (h || "").toLowerCase().trim());
    const manualColIdx = headers.findIndex((h: string) => /manual|scrub.*manual|notes/i.test(h));

    const allShortNamesFromSheet = new Set<string>();
    const lenderMentions = new Map<string, { count: number; examples: string[]; source: string }>();

    const monthRevPattern = /^\d{1,2}[\-]\d+[kKmM]$/;
    const pureNumPattern = /^[\d,\.\+\-]+[kKmM]?$/;

    for (let r = 1; r < allRows.length; r++) {
      const row = allRows[r];
      const colsToScan = [];
      if (manualColIdx >= 0 && row[manualColIdx]) colsToScan.push({ text: row[manualColIdx], src: "manual (col B)" });
      if (row[writeColIdx]) colsToScan.push({ text: row[writeColIdx], src: "writeback (col " + config.writeColumn + ")" });

      for (const { text, src } of colsToScan) {
        const tokens = String(text).split(/\s+/);
        for (const token of tokens) {
          if (monthRevPattern.test(token)) continue;
          if (pureNumPattern.test(token)) continue;
          const m = token.match(/^([a-zA-Z][a-zA-Z0-9]*)/);
          if (!m) continue;
          const name = m[1].toLowerCase();
          if (name.length < 3) continue;
          if (/^(the|and|for|inc|llc|corp|ltd|dba|gross|net|total|avg|min|max|new|old|per|day|week|year|month|none|null|true|false|all|any|has|not|bad|good|yes|low|high|top|mid|end|clean|best|last|first|next|same|other|also|just|only|more|most|less|some|full|open|free|late|near|sure|own|real|able|much|many|each|both|even|well|back|long|made|over|after|still|into|just|such|very|honest|reliable|micro|garden|merit|blade|merk)$/.test(name)) continue;
          allShortNamesFromSheet.add(name);
          const bizName = row[headers.findIndex((h: string) => /business.*name|company|biz/i.test(h))] || "";
          if (!lenderMentions.has(name)) lenderMentions.set(name, { count: 0, examples: [], source: src });
          const entry = lenderMentions.get(name)!;
          entry.count++;
          if (entry.examples.length < 3) entry.examples.push(`Row ${r + 1}: "${String(text).slice(0, 60)}" (${bizName})`);
        }
      }
    }

    const allKnownShortNames = new Set<string>();
    for (const s of KNOWN_LENDER_SHORTNAMES) allKnownShortNames.add(s);
    for (const key of Object.keys(LENDER_SHORT_NAMES)) {
      const short = LENDER_SHORT_NAMES[key];
      allKnownShortNames.add(short);
      const normalized = normalizeLenderKey(key);
      if (normalized.length >= 3) allKnownShortNames.add(normalized);
    }
    const knownSafelistNames = [
      "ondeck","kabbage","bluevine","fundbox","cancapital","rapid","credibly","libertas",
      "yellowstone","pearl","forward","fora","kalamata","national","fox","mantis","everest",
      "cfg","cfgms","mulligan","clearview","itria","cloudfund","navitas","vox","wynwood",
      "platinum","qfs","jmb","unique","samson","kings","stage","7even","cashable","vitalcap",
      "vcg","zen","ace","aspire","breeze","canfield","clara","compass","daytona","diamond",
      "elevate","epic","expansion","family","fenix","figure","fresh","metrics","giggle",
      "gotorro","highland","hightower","honor","idea","ifund","immediate","iou","lcf",
      "legend","lendbuzz","lendistry","lg","liberty","litefund","millstone","mradvance",
      "newport","nitro","oak","ocean","olympus","oneriver","orange","overton","parkside",
      "path2","power","premium","prosperum","prosperity","readycap","reboost","redwood",
      "reliance","retro","revenued","rocket","specialty","stellar","suncoast","swift","tbf",
      "fundworks","triton","trupath","ufce","ufs","upfunding","vader","wave","webfunder",
      "westwood","wide","pipe","ssmb","coast","fintegra","alt","futures","mako","mainstreet",
      "integra","reliant","headway","behalf","breakout","greenbox","world","tvt","united",
      "bretton","fleetcor","kapitus","gmfunding","sq","wfunding","snap","spartan","simply",
      "tucker","thoro","lendingserv","mca","celtic","webbank","cross","newtek","smartbiz",
      "payability","biz2credit","lendio","fundation","shopify","link","rival","dsc","ebf",
      "cobalt","honor","beacon","pirs","uptown","lux","appfund","enfin","carlton","fintapp",
      "ramp","topchoice","loot","selene","efinancial","fincor","fundomate","capremium",
      "acv","kanmon","valon","byzflex","byzwash","byzfund","capybara","sbfs","elixir",
      "travelers","msm","intuit","masion","amexloan","nextgear","idea247",
      "fdm","dlp","gotfunded","advsyn","fratello","ascentra","luminar",
      "kif","greenbridge","arbitrage","jrg","aurum","pdm","pfg",
      "stashcap","merchadv","lily","mckenzie","purpletree","lexio","global",
      "monetaria","trustify","bluetie","seamless","liquidbee","belltower",
      "palisade","marlin","xuper","ghkapital","fundfi","newco","slim","steady","secure"
    ];
    for (const n of knownSafelistNames) allKnownShortNames.add(n);

    const dbLenders = await db.execute(sql`
      SELECT DISTINCT jsonb_array_elements(loan_details)->>'lender' as lender_name
      FROM bank_statement_analyses
      WHERE jsonb_array_length(loan_details) > 0
    `);
    const dbLenderNames = new Set<string>();
    for (const row of dbLenders.rows) {
      const name = (row.lender_name as string || "").trim();
      if (name) {
        dbLenderNames.add(name);
        const short = shortLenderName(name);
        allKnownShortNames.add(short);
        allKnownShortNames.add(normalizeLenderKey(name));
      }
    }

    const editDist1 = (a: string, b: string): boolean => {
      if (Math.abs(a.length - b.length) > 1) return false;
      if (a === b) return true;
      let diffs = 0;
      if (a.length === b.length) {
        for (let i = 0; i < a.length; i++) { if (a[i] !== b[i]) diffs++; if (diffs > 1) return false; }
        return diffs === 1;
      }
      const [shorter, longer] = a.length < b.length ? [a, b] : [b, a];
      let si = 0;
      for (let li = 0; li < longer.length; li++) {
        if (shorter[si] !== longer[li]) { diffs++; if (diffs > 1) return false; } else { si++; }
      }
      return true;
    };

    const unknownLenders: { name: string; count: number; examples: string[]; source: string }[] = [];
    const knownLendersFound: { name: string; count: number }[] = [];

    for (const [name, info] of lenderMentions) {
      const isKnown = allKnownShortNames.has(name) ||
        [...allKnownShortNames].some(k => k.length >= 4 && (name.includes(k) || k.includes(name)));
      const isTypo = !isKnown && [...allKnownShortNames].some(k => k.length >= 4 && name.length >= 4 && editDist1(name, k));
      if (isKnown || isTypo) {
        knownLendersFound.push({ name, count: info.count });
      } else {
        unknownLenders.push({ name, count: info.count, examples: info.examples, source: info.source });
      }
    }

    unknownLenders.sort((a, b) => b.count - a.count);
    knownLendersFound.sort((a, b) => b.count - a.count);

    const stmtTexts = await db.execute(sql`
      SELECT bsa.extracted_statement_text, l.business_name
      FROM bank_statement_analyses bsa
      JOIN leads l ON l.id = bsa.lead_id
      WHERE bsa.extracted_statement_text IS NOT NULL
        AND length(bsa.extracted_statement_text) > 500
      LIMIT 200
    `);

    const unknownFromStatements: { pattern: string; context: string; business: string }[] = [];
    const lenderKeywordRegex = /\b([A-Z][A-Z0-9]{2,}(?:\s+[A-Z][A-Z0-9]+)*)\s+(?:ADVANCE|FUNDING|CAPITAL|FINANCIAL|LENDING|FUND|FINSERV|FINANC)\b/g;

    for (const row of stmtTexts.rows) {
      const text = row.extracted_statement_text as string || "";
      const biz = row.business_name as string || "";
      let match;
      while ((match = lenderKeywordRegex.exec(text)) !== null) {
        const fullMatch = match[0].trim();
        const normalized = normalizeLenderKey(fullMatch);
        const isKnown = allKnownShortNames.has(normalized) ||
          [...allKnownShortNames].some(k => k.length >= 4 && (normalized.includes(k) || k.includes(normalized)));
        if (!isKnown && fullMatch.length > 4 && fullMatch.length < 40) {
          unknownFromStatements.push({
            pattern: fullMatch,
            context: text.slice(Math.max(0, match.index - 40), match.index + fullMatch.length + 40).replace(/\n/g, " "),
            business: biz,
          });
        }
      }
    }

    res.json({
      unknownLenders,
      unknownFromStatements: unknownFromStatements.slice(0, 50),
      knownLendersFound,
      totalRowsScanned: allRows.length - 1,
      totalSheetLenderTokens: allShortNamesFromSheet.size,
      totalKnownPatterns: allKnownShortNames.size,
      totalDbLenders: dbLenderNames.size,
    });
  } catch (e: any) {
    console.error("[Lender CrossRef]", e.message);
    res.status(500).json({ error: e.message });
  }
});

export default router;
