import type { Request, Response, NextFunction } from "express";
import { logSecurityEvent } from "./security";

const SCRIPT_PATTERNS = [
  /<script[\s>]/i,
  /javascript\s*:/i,
  /on(load|error|click|mouseover|focus|blur|submit|change|input|keydown|keyup|keypress)\s*=/i,
  /<iframe[\s>]/i,
  /<object[\s>]/i,
  /<embed[\s>]/i,
  /<form[\s>]/i,
  /eval\s*\(/i,
  /document\.(cookie|write|location)/i,
  /window\.(location|open)/i,
  /\.\s*innerHTML\s*=/i,
  /<svg[\s>].*?on\w+\s*=/i,
  /data\s*:\s*text\/html/i,
  /vbscript\s*:/i,
  /expression\s*\(/i,
];

const SQL_INJECTION_PATTERNS = [
  /('\s*(OR|AND)\s+')/i,
  /;\s*(DROP|DELETE|UPDATE|INSERT|ALTER|EXEC|EXECUTE)\s+/i,
  /UNION\s+(ALL\s+)?SELECT/i,
  /--\s*$/m,
  /\/\*[\s\S]*?\*\//,
];

function containsMaliciousContent(value: string): { detected: boolean; type: string; pattern: string } {
  for (const pattern of SCRIPT_PATTERNS) {
    if (pattern.test(value)) {
      return { detected: true, type: "xss", pattern: pattern.source };
    }
  }
  for (const pattern of SQL_INJECTION_PATTERNS) {
    if (pattern.test(value)) {
      return { detected: true, type: "sql_injection", pattern: pattern.source };
    }
  }
  return { detected: false, type: "", pattern: "" };
}

function stripDangerousTags(value: string): string {
  return value
    .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, "")
    .replace(/<\/?(?:script|iframe|object|embed|form|link|meta|style)\b[^>]*>/gi, "")
    .replace(/on(load|error|click|mouseover|focus|blur|submit|change|input|keydown|keyup|keypress)\s*=\s*["'][^"']*["']/gi, "")
    .replace(/javascript\s*:/gi, "")
    .replace(/vbscript\s*:/gi, "")
    .replace(/data\s*:\s*text\/html/gi, "");
}

function sanitizeValue(value: any): any {
  if (typeof value === "string") {
    return stripDangerousTags(value);
  }
  if (Array.isArray(value)) {
    return value.map(sanitizeValue);
  }
  if (value && typeof value === "object") {
    return sanitizeObject(value);
  }
  return value;
}

function sanitizeObject(obj: Record<string, any>): Record<string, any> {
  const result: Record<string, any> = {};
  for (const [key, value] of Object.entries(obj)) {
    result[key] = sanitizeValue(value);
  }
  return result;
}

function scanForThreats(obj: any, path: string = ""): { detected: boolean; type: string; pattern: string; field: string; value: string } | null {
  if (typeof obj === "string") {
    const check = containsMaliciousContent(obj);
    if (check.detected) {
      return { ...check, field: path, value: obj.substring(0, 200) };
    }
  }
  if (Array.isArray(obj)) {
    for (let i = 0; i < obj.length; i++) {
      const threat = scanForThreats(obj[i], `${path}[${i}]`);
      if (threat) return threat;
    }
  }
  if (obj && typeof obj === "object" && !Buffer.isBuffer(obj)) {
    for (const [key, value] of Object.entries(obj)) {
      const threat = scanForThreats(value, path ? `${path}.${key}` : key);
      if (threat) return threat;
    }
  }
  return null;
}

export function inputSanitizer(req: Request, res: Response, next: NextFunction): void {
  if (req.body && typeof req.body === "object") {
    const threat = scanForThreats(req.body);
    if (threat) {
      const userId = req.cookies?.userId ? parseInt(req.cookies.userId) : undefined;
      logSecurityEvent(
        threat.type === "xss" ? "suspicious_ip" : "suspicious_ip",
        "critical",
        `${threat.type.toUpperCase()} attempt detected in field "${threat.field}" from IP ${req.headers["x-forwarded-for"]?.toString().split(",")[0]?.trim() || req.ip}`,
        {
          userId: isNaN(userId as number) ? undefined : userId,
          req,
          metadata: {
            type: threat.type,
            field: threat.field,
            pattern: threat.pattern,
            snippet: threat.value,
            method: req.method,
            url: req.originalUrl,
          },
        }
      ).catch(() => {});
    }
    req.body = sanitizeObject(req.body);
  }

  if (req.query && typeof req.query === "object") {
    for (const [key, value] of Object.entries(req.query)) {
      if (typeof value === "string") {
        const check = containsMaliciousContent(value);
        if (check.detected) {
          logSecurityEvent(
            "suspicious_ip",
            "critical",
            `${check.type.toUpperCase()} attempt in query param "${key}" from IP ${req.headers["x-forwarded-for"]?.toString().split(",")[0]?.trim() || req.ip}`,
            {
              req,
              metadata: { type: check.type, field: `query.${key}`, snippet: value.substring(0, 200), url: req.originalUrl },
            }
          ).catch(() => {});
        }
        if (key !== "__proto__" && key !== "constructor" && key !== "prototype") {
          (req.query as any)[key] = stripDangerousTags(value);
        }
      }
    }
  }

  next();
}
