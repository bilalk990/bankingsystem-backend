const MULTI_GROUP_ACCOUNT_PATTERNS = [
  /(?:Account\s*(?:Number|No\.?|#))[:\s]*[-\s]*(\d{1,4}[\s-]+\d{3,4}[\s-]+\d{3,4}[\s-]+\d{3,4})/i,
  /(?:Acct\.?\s*(?:No\.?|#|Number))[:\s]*[-\s]*(\d{1,4}[\s-]+\d{3,4}[\s-]+\d{3,4}[\s-]+\d{3,4})/i,
  /(?:Account\s*(?:Number|No\.?|#))[:\s]*[-\s]*(\d{4}[\s-]+\d{3,4}[\s-]+\d{3,4})/i,
  /(?:Acct\.?\s*(?:No\.?|#|Number))[:\s]*[-\s]*(\d{4}[\s-]+\d{3,4}[\s-]+\d{3,4})/i,
  /(?:Account\s*(?:Number|No\.?|#))[:\s]*[-\s]*(\d{4,5}[\s-]+\d{4,})/i,
  /(?:Acct\.?\s*(?:No\.?|#|Number))[:\s]*[-\s]*(\d{4,5}[\s-]+\d{4,})/i,
];

const MASKED_CHARS = /[xX.*●•✱◆■□○]+/;

const ACCOUNT_PATTERNS = [
  /(?:Account\s*(?:Number|No\.?|#))[:\s]*[-\s]*(\d{4,})/i,
  /(?:Acct\.?\s*(?:No\.?|#|Number))[:\s]*[-\s]*(\d{4,})/i,

  /(?:Account\s*(?:Number|No\.?|#))[:\s]*[-\s]*(?:[xX.*●•✱◆■□○]+[\s-]*)(\d{3,4})/i,
  /(?:Account\s*No\.?)[:\s]*[-\s]*(?:[xX.*●•✱◆■□○]+[\s-]*)(\d{3,4})/i,
  /(?:Acct\.?\s*(?:No\.?|#|Number))[:\s]*[-\s]*(?:[xX.*●•✱◆■□○]+[\s-]*)(\d{3,4})/i,

  /(?:Account\s*(?:Number|No\.?|#))[:\s]*[-\s]*(?:[xX.*●•✱◆■□○]+\s+[xX.*●•✱◆■□○]+\s+)(\d{3,4})/i,

  /(?:Business\s+(?:Simple|Account|Plus|Enhanced|Advantage|Interest|Premium|Classic)?\s*Checking|Primary\s+Checking|Checking\s+Account)\s*[-–—#:]*\s*(?:[xX.*●•✱◆■□○]+[\s-]*)(\d{3,4})/i,
  /(?:Business\s+(?:Simple|Account|Plus|Enhanced|Advantage|Interest|Premium|Classic)?\s*Checking|Primary\s+Checking|Checking\s+Account)\s*#?\s*(\d{3,})/i,

  /(?:Savings?\s+Account|Money\s*Market|Share\s*Draft|Share\s*Account)\s*[-–—#:]*\s*(?:[xX.*●•✱◆■□○]+[\s-]*)(\d{3,4})/i,
  /(?:Savings?\s+Account|Money\s*Market|Share\s*Draft|Share\s*Account)\s*#?\s*(\d{3,})/i,

  /(?:Checking|Savings|Money\s*Market|Share\s*Draft)\s*[-–—]\s*(?:[xX.*●•✱◆■□○]+[\s-]*)(\d{3,4})/i,
  /(?:Checking|Savings|Money\s*Market|Share\s*Draft)\s+(?:[xX.*●•✱◆■□○]+[\s-]*)(\d{3,4})/i,

  /(?:account\s+(?:ending|ends)\s+in)\s*[:\s]*(\d{4})/i,
  /(?:account\s+(?:\S+\s+)?ending\s+in)\s*[:\s]*(\d{4})/i,
  /(?:last\s+(?:four|4)\s+digits?)[:\s]*(\d{4})/i,

  /(?:member\s*(?:number|no\.?|#))[:\s]*[-\s]*(\d{4,})/i,
  /(?:member\s*(?:number|no\.?|#))[:\s]*[-\s]*(?:[xX.*●•✱◆■□○]+[\s-]*)(\d{4})/i,

  /(?:customer\s*(?:number|no\.?|#))[:\s]*[-\s]*(\d{4,})/i,
  /(?:customer\s*(?:number|no\.?|#))[:\s]*[-\s]*(?:[xX.*●•✱◆■□○]+[\s-]*)(\d{4})/i,

  /(?:Spending\s+Account|Basic\s+Business|Business\s+Basic)\s*[-–—#:]*\s*(?:[xX.*●•✱◆■□○]+[\s-]*)(\d{4})/i,
  /(?:Spending\s+Account|Basic\s+Business|Business\s+Basic)\s*#?\s*(\d{4,})/i,

  /(?:Business\s+Basic\s+Checking|Business\s+(?:Essential|Fundamental|Standard|Growth|Performance|Exceptional|Value|Cash\s+Pro)\s+Checking)\s+(\d{8,})/i,
  /(?:Business\s+Basic\s+Checking|Business\s+(?:Essential|Fundamental|Standard|Growth|Performance|Exceptional|Value|Cash\s+Pro)\s+Checking)\s*[-–—#:]*\s*(?:[xX.*●•✱◆■□○]+[\s-]*)(\d{4})/i,

  /(?:Statement\s*(?:Number|No\.?))[:\s]*[-\s]*(\d{4,})/i,

  /(?:Account\s*No\.?)[:\s]*[-\s]*(\d{5,})/i,
  /(?:Account|Acct|Checking|Savings)\s*(?:No\.?|#|Number)?[:\s]*(\d{4,5}-\d{4})\s*\(\d\)/i,
];

export function extractBusinessNameFromText(text: string): string | null {
  const slice = text.slice(0, 3000);
  // Look for common patterns where the account holder name appears
  const patterns = [
    /(?:Account\s+for|Statement\s+for|Customer\s+Name)[:\s]*\n?([A-Z0-9\s,&.]{3,60})/i,
    /(?:^|\n)([A-Z0-9\s,&.]{3,60})\s*\n(?:[0-9]{1,5}\s+[A-Z0-9\s,.]{3,60})/i, // Name followed by address line
    /([A-Z0-9\s,&.]{3,60})\n\s*(?:PO\s+BOX|P\.O\.\s+BOX)/i,
  ];

  for (const pat of patterns) {
    const m = slice.match(pat);
    if (m && m[1]) {
      const name = m[1].trim();
      if (name.length > 3 && !/Bank|Union|Statement|Page|Account/i.test(name)) {
        return name;
      }
    }
  }
  
  // Fallback: take the first non-empty lines that don't look like bank headers
  const lines = slice.split('\n').map(l => l.trim()).filter(l => l.length > 5);
  for (let i = 0; i < Math.min(lines.length, 10); i++) {
    const line = lines[i];
    if (/^[A-Z0-9\s,&.]{5,}$/.test(line) && !/Bank|National|Federal|Credit|Statement|Ending|Beginning|Balance|Page|Account|Date|Thru/i.test(line)) {
      return line;
    }
  }

  return null;
}

export function fuzzyBusinessMatch(name1: string, name2: string): number {
  const n1 = name1.toLowerCase().replace(/[^a-z0-9]/g, "");
  const n2 = name2.toLowerCase().replace(/[^a-z0-9]/g, "");
  if (n1 === n2) return 100;
  if (n1.includes(n2) || n2.includes(n1)) return 85;

  let matches = 0;
  const words1 = name1.toLowerCase().split(/\s+/).filter(w => w.length > 2);
  const words2 = name2.toLowerCase().split(/\s+/).filter(w => w.length > 2);
  for (const w1 of words1) {
    for (const w2 of words2) {
      if (w1 === w2) matches++;
    }
  }
  if (words1.length > 0 && words2.length > 0) {
    return Math.round((matches / Math.max(words1.length, words2.length)) * 100);
  }
  return 0;
}

const PHONE_PATTERN = /\d{3}[-.)\s]\d{3}[-.]\d{4}/;
const BANK_ID_PATTERN = /[A-Z]{2}\d{3}\|[A-Z]{2}\d{3}\|\d+/;
const ROUTING_PATTERN = /(?:routing|transit|aba)\s*(?:number|no\.?|#)?[:\s]*\d/i;

function stripMarkdown(text: string): string {
  return text.replace(/\*\*/g, "");
}

function matchAccount(slice: string, patterns: RegExp[]): { cleaned: string; raw: string } | null {
  const normalized = stripMarkdown(slice);
  for (const pat of patterns) {
    const m = normalized.match(pat);
    if (m) {
      const startCtx = Math.max(0, m.index! - 30);
      const endCtx = Math.min(normalized.length, m.index! + m[0].length + 20);
      const context = normalized.substring(startCtx, endCtx);
      if (PHONE_PATTERN.test(context)) continue;
      if (BANK_ID_PATTERN.test(context)) continue;
      const preContext = normalized.substring(startCtx, m.index!);
      if (ROUTING_PATTERN.test(preContext)) continue;
      const cleaned = m[1].replace(/[\s-]/g, "");
      if (cleaned.length < 3) continue;
      return { cleaned, raw: m[1] };
    }
  }
  return null;
}

export function extractAccountLast4(text: string, searchLimit = 8000): string | null {
  const slice = text.slice(0, searchLimit);
  const multi = matchAccount(slice, MULTI_GROUP_ACCOUNT_PATTERNS);
  if (multi) return multi.cleaned.slice(-4);
  const single = matchAccount(slice, ACCOUNT_PATTERNS);
  if (single) return single.cleaned.slice(-4);
  return null;
}

export function extractFullAccountNumber(text: string, searchLimit = 8000): string | undefined {
  const slice = text.slice(0, searchLimit);
  const multi = matchAccount(slice, MULTI_GROUP_ACCOUNT_PATTERNS);
  if (multi) return multi.cleaned;
  const single = matchAccount(slice, ACCOUNT_PATTERNS);
  if (single) return single.cleaned;
  return undefined;
}

export function extractAllAccountLast4s(text: string, searchLimit = 0): string[] {
  const raw = searchLimit > 0 ? text.slice(0, searchLimit) : text;
  const slice = stripMarkdown(raw);
  const found = new Set<string>();

  const allPatterns = [...MULTI_GROUP_ACCOUNT_PATTERNS, ...ACCOUNT_PATTERNS];
  for (const pat of allPatterns) {
    const globalPat = new RegExp(pat.source, pat.flags.includes("g") ? pat.flags : pat.flags + "g");
    let m;
    while ((m = globalPat.exec(slice)) !== null) {
      const startCtx = Math.max(0, m.index - 30);
      const endCtx = Math.min(slice.length, m.index + m[0].length + 20);
      const context = slice.substring(startCtx, endCtx);
      if (PHONE_PATTERN.test(context)) continue;
      if (BANK_ID_PATTERN.test(context)) continue;
      const preContext = slice.substring(startCtx, m.index);
      if (ROUTING_PATTERN.test(preContext)) continue;
      const cleaned = m[1].replace(/[\s-]/g, "");
      if (cleaned.length >= 3) {
        found.add(cleaned.slice(-4));
      }
    }
  }
  return Array.from(found);
}

export function extractAccountFromFilename(filename: string): string | null {
  const base = filename.replace(/\.[^.]+$/, "");
  const bankAcctMatch = base.match(/bank\s*(\d{4,})/i);
  if (bankAcctMatch) {
    const digits = bankAcctMatch[1].slice(-4);
    if (!isYearLike(digits)) return digits;
  }
  const acctMatch = base.match(/(?:acct|account|chk|checking|savings)[-_]?(\d{4,})/i);
  if (acctMatch) {
    const digits = acctMatch[1].slice(-4);
    if (!isYearLike(digits)) return digits;
  }
  const stmtAcctMatch = base.match(/(?:statement|stmt|stmts?)s?(\d{4,})$/i);
  if (stmtAcctMatch) {
    const digits = stmtAcctMatch[1].slice(-4);
    if (!isYearLike(digits)) return digits;
  }
  const trailingMatch = base.match(/[a-zA-Z](\d{4,})$/);
  if (trailingMatch) {
    const allDigits = trailingMatch[1];
    const datePrefix = allDigits.match(/^(20\d{6})/);
    if (datePrefix) {
      const afterDate = allDigits.slice(8);
      if (afterDate.length >= 4) {
        const last4 = afterDate.slice(-4);
        if (!isYearLike(last4)) return last4;
      }
    } else {
      const last4 = allDigits.slice(-4);
      if (!isYearLike(last4)) return last4;
    }
  }
  return null;
}

function isYearLike(s: string): boolean {
  const n = parseInt(s, 10);
  return n >= 1990 && n <= 2099;
}
