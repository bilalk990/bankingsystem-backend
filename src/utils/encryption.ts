import crypto from "crypto";

const ALGORITHM = "aes-256-gcm";
const IV_LENGTH = 16;
const TAG_LENGTH = 16;
const PREFIX = "ENC$";

function getKey(): Buffer {
  const key = process.env.ENCRYPTION_KEY;
  if (!key) {
    throw new Error("ENCRYPTION_KEY environment variable is required");
  }
  return crypto.createHash("sha256").update(key).digest();
}

export function encrypt(plaintext: string): string {
  if (!plaintext) return plaintext;
  if (plaintext.startsWith(PREFIX)) return plaintext;

  const key = getKey();
  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv(ALGORITHM, key, iv);

  let encrypted = cipher.update(plaintext, "utf8", "base64");
  encrypted += cipher.final("base64");
  const tag = cipher.getAuthTag();

  return `${PREFIX}${iv.toString("base64")}$${encrypted}$${tag.toString("base64")}`;
}

export function decrypt(ciphertext: string): string {
  if (!ciphertext) return ciphertext;
  if (!ciphertext.startsWith(PREFIX)) return ciphertext;

  const key = getKey();
  const parts = ciphertext.slice(PREFIX.length).split("$");
  if (parts.length !== 3) {
    throw new Error("Invalid encrypted data format");
  }

  const iv = Buffer.from(parts[0], "base64");
  const encrypted = parts[1];
  const tag = Buffer.from(parts[2], "base64");

  const decipher = crypto.createDecipheriv(ALGORITHM, key, iv, { authTagLength: TAG_LENGTH });
  decipher.setAuthTag(tag);

  let decrypted = decipher.update(encrypted, "base64", "utf8");
  decrypted += decipher.final("utf8");
  return decrypted;
}

export function hmacHash(value: string): string {
  if (!value) return "";
  const key = getKey();
  return crypto.createHmac("sha256", key).update(value).digest("hex");
}

export function isEncrypted(value: string | null | undefined): boolean {
  return !!value && value.startsWith(PREFIX);
}

const SENSITIVE_FIELDS = [
  "ssn", "dob", "taxId", "ein",
  "driversLicense", "dlState", "dlExpiry",
  "accountNumber", "routingNumber",
] as const;

export function encryptLeadFields<T extends Record<string, any>>(data: T): T & { ssnHash?: string } {
  const result = { ...data } as any;

  for (const field of SENSITIVE_FIELDS) {
    if (result[field] && typeof result[field] === "string" && !isEncrypted(result[field])) {
      result[field] = encrypt(result[field]);
    }
  }

  if (data.ssn && typeof data.ssn === "string" && !isEncrypted(data.ssn)) {
    const normalizedSsn = data.ssn.replace(/[^\d]/g, "");
    if (normalizedSsn.length > 0) {
      result.ssnHash = hmacHash(normalizedSsn);
    }
  }

  return result;
}

export function decryptLeadFields<T extends Record<string, any>>(data: T): T {
  if (!data) return data;
  const result = { ...data } as any;

  for (const field of SENSITIVE_FIELDS) {
    if (result[field] && typeof result[field] === "string" && isEncrypted(result[field])) {
      try {
        result[field] = decrypt(result[field]);
      } catch {
        result[field] = null;
      }
    }
  }

  return result;
}

export function maskSsn(ssn: string | null | undefined): string | null {
  if (!ssn) return null;
  const plain = isEncrypted(ssn) ? decrypt(ssn) : ssn;
  return `***-**-${plain.replace(/[^\d]/g, "").slice(-4)}`;
}

export function normalizeSsnForHash(ssn: string): string {
  return ssn.replace(/[^\d]/g, "");
}
