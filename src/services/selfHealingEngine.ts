import { ParsedTransaction, ParsedStatementData } from "./documentAiService";

export interface HealingResult {
  isCorrected: boolean;
  originalValue: number;
  correctedValue: number;
  reason: string;
}

export interface StatementValidationResult {
  isValid: boolean;
  delta: number;
  anomalies: string[];
  autoCorrections: Array<{
    date: string;
    description: string;
    originalAmount: number;
    newAmount: number;
    reason: string;
  }>;
}

/**
 * Validates the core balance calculus of a statement:
 * Beginning Balance + Deposits - Withdrawals = Ending Balance
 */
export function validateStatementCalculus(data: ParsedStatementData): { isValid: boolean; delta: number } {
  const beg = data.beginningBalance ?? 0;
  const end = data.endingBalance ?? 0;
  const dep = data.totalDeposits ?? 0;
  const wit = data.totalWithdrawals ?? 0;

  if (beg === 0 && end === 0 && dep === 0 && wit === 0) return { isValid: true, delta: 0 };

  const expected = Math.round((beg + dep - wit) * 100) / 100;
  const actual = Math.round(end * 100) / 100;
  const delta = Math.round((actual - expected) * 100) / 100;

  return {
    isValid: Math.abs(delta) < 0.05,
    delta
  };
}

/**
 * Heuristically detects and repairs merged column anomalies (e.g., Chase Platinum count+amount)
 */
export function repairTransactionAnomalies(
  txns: ParsedTransaction[],
  targetTotal: number,
  type: "credit" | "debit"
): { correctedTransactions: ParsedTransaction[]; corrections: any[] } {
  const currentTotal = txns.filter(t => t.type === type).reduce((s, t) => s + t.amount, 0);
  const diff = Math.abs(currentTotal - targetTotal);

  if (diff < 0.05) return { correctedTransactions: txns, corrections: [] };

  const corrections: any[] = [];
  const resultTxns = [...txns];

  // Strategy A: Structural Outlier Detection (Leading Digit Removal)
  // If stripping the 1st digit from a few high-value txns makes the total match, it's a high-confidence fix
  const candidates = resultTxns.filter(t => t.type === type && t.amount > 100);
  
  // We look for a subset of transactions that, when stripped, reduce the total by exactly 'diff'
  // For simplicity, we check each high-value transaction individually first
  for (let i = 0; i < resultTxns.length; i++) {
    const t = resultTxns[i];
    if (t.type !== type) continue;

    const amtStr = t.amount.toFixed(2).replace(".", ""); // Treat as integer cents initially
    if (amtStr.length < 5) continue; // Must be at least $10.00

    const firstDigit = parseInt(amtStr[0]);
    if (firstDigit === 0) continue;

    const strippedAmt = parseFloat(t.amount.toString().substring(1));
    const reduction = t.amount - strippedAmt;

    // Check if this reduction matches the error delta (or a significant portion of it)
    if (Math.abs(reduction - diff) < 0.05) {
      corrections.push({
        date: t.date,
        description: t.description,
        originalAmount: t.amount,
        newAmount: strippedAmt,
        reason: `Auto-corrected merged column (stripped leading digit '${firstDigit}')`
      });
      resultTxns[i] = { ...t, amount: strippedAmt };
      return { correctedTransactions: resultTxns, corrections };
    }
  }

  // Strategy B: Multiple Transactions with Same Count
  // If multiple transactions share the same leading digit and stripping them all fixes the total
  const possibleCounts = [1, 2, 3, 4, 5, 6, 7, 8, 9];
  for (const count of possibleCounts) {
    let totalReduction = 0;
    const batchCorrections = [];
    const tempTxns = [...resultTxns];

    for (let i = 0; i < tempTxns.length; i++) {
      const t = tempTxns[i];
      if (t.type !== type) continue;
      
      const s = t.amount.toString();
      if (s.startsWith(count.toString()) && s.length >= 6) { // e.g. 610003.20
         const stripped = parseFloat(s.substring(1));
         totalReduction += (t.amount - stripped);
         batchCorrections.push({ idx: i, original: t.amount, new: stripped, desc: t.description });
      }
    }

    if (Math.abs(totalReduction - diff) < 0.05 && batchCorrections.length > 0) {
      batchCorrections.forEach(c => {
        const t = tempTxns[c.idx];
        resultTxns[c.idx] = { ...t, amount: c.new };
        corrections.push({
          date: t.date,
          description: t.description,
          originalAmount: c.original,
          newAmount: c.new,
          reason: `Batch auto-corrected merged columns (count=${count})`
        });
      });
      return { correctedTransactions: resultTxns, corrections };
    }
  }

  return { correctedTransactions: txns, corrections: [] };
}
