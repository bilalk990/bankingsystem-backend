import { Storage } from "@google-cloud/storage";
import fs from "fs";
import path from "path";

const REPLIT_SIDECAR_ENDPOINT = "http://127.0.0.1:1106";

const storage = new Storage({
  credentials: {
    audience: "replit",
    subject_token_type: "access_token",
    token_url: `${REPLIT_SIDECAR_ENDPOINT}/token`,
    type: "external_account",
    credential_source: {
      url: `${REPLIT_SIDECAR_ENDPOINT}/credential`,
      format: {
        type: "json",
        subject_token_field_name: "access_token",
      },
    },
    universe_domain: "googleapis.com",
  },
  projectId: "",
});

function getBucket() {
  const bucketId = process.env.DEFAULT_OBJECT_STORAGE_BUCKET_ID;
  if (!bucketId) throw new Error("Object storage not configured");
  return storage.bucket(bucketId);
}

export async function uploadFileToStorage(localPath: string, storagePath: string): Promise<string> {
  const bucket = getBucket();
  const objectKey = `bank-statements/${storagePath}`;
  await bucket.upload(localPath, { destination: objectKey });
  return objectKey;
}

export async function uploadBufferToStorage(buffer: Buffer, storagePath: string, contentType?: string): Promise<string> {
  const bucket = getBucket();
  const objectKey = `bank-statements/${storagePath}`;
  const file = bucket.file(objectKey);
  await file.save(buffer, { contentType: contentType || "application/octet-stream" });
  return objectKey;
}

export async function getFileFromStorage(objectKey: string): Promise<{ buffer: Buffer; contentType: string }> {
  const bucket = getBucket();
  const file = bucket.file(objectKey);
  const [exists] = await file.exists();
  if (!exists) throw new Error("File not found in storage");
  const [buffer] = await file.download();
  const [metadata] = await file.getMetadata();
  return {
    buffer: Buffer.from(buffer),
    contentType: (metadata.contentType as string) || "application/octet-stream",
  };
}

export async function fileExistsInStorage(objectKey: string): Promise<boolean> {
  try {
    const bucket = getBucket();
    const file = bucket.file(objectKey);
    const [exists] = await file.exists();
    return exists;
  } catch {
    return false;
  }
}

export async function deleteFileFromStorage(objectKey: string): Promise<void> {
  const bucket = getBucket();
  const file = bucket.file(objectKey);
  const [exists] = await file.exists();
  if (exists) await file.delete();
}

export function isStorageConfigured(): boolean {
  return !!process.env.DEFAULT_OBJECT_STORAGE_BUCKET_ID;
}

export async function uploadLocalFileAndGetKey(localFilePath: string, relativePath: string): Promise<string | null> {
  if (!isStorageConfigured()) return null;
  let resolvedPath = localFilePath;
  if (!fs.existsSync(resolvedPath)) {
    const dir = path.dirname(localFilePath);
    const base = path.basename(localFilePath);
    const withToplevel = path.join(dir, "__toplevel", base);
    if (fs.existsSync(withToplevel)) {
      resolvedPath = withToplevel;
    } else {
      return null;
    }
  }
  try {
    const ext = path.extname(localFilePath).toLowerCase();
    const mimeTypes: Record<string, string> = {
      ".pdf": "application/pdf",
      ".png": "image/png",
      ".jpg": "image/jpeg",
      ".jpeg": "image/jpeg",
    };
    const buffer = fs.readFileSync(resolvedPath);
    return await uploadBufferToStorage(buffer, relativePath, mimeTypes[ext] || "application/octet-stream");
  } catch (e: any) {
    console.error(`[Storage] Failed to upload ${relativePath}:`, e.message);
    return null;
  }
}
