/**
 * Utility for robust financial rounding to 2 decimal places.
 * Avoids basic floating point issues by using an epsilon or scaling.
 */
export function roundToTwo(num: number | string | null | undefined): number {
  if (num === null || num === undefined) return 0;
  const val = typeof num === "string" ? parseFloat(num) : num;
  if (isNaN(val)) return 0;
  
  // Standard financial rounding to 2 decimals
  return Math.round((val + Number.EPSILON) * 100) / 100;
}

/**
 * Ensures a value is a number, handling strings from Drizzle numeric fields.
 */
export function toNum(val: any): number {
  if (val === null || val === undefined) return 0;
  const n = typeof val === "string" ? parseFloat(val) : val;
  return isNaN(n) ? 0 : n;
}
