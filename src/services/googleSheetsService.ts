// Google Sheets Integration (Replit Connector: google-sheet)
import { google } from "googleapis";

let connectionSettings: any;
let lastFetchedAt = 0;

async function getAccessToken() {
  const now = Date.now();
  const tokenValid = connectionSettings?.settings?.expires_at && new Date(connectionSettings.settings.expires_at).getTime() > now + 60000;
  const recentlyFetched = now - lastFetchedAt < 30000;
  if (tokenValid && recentlyFetched) {
    return connectionSettings.settings.access_token;
  }

  const hostname = process.env.REPLIT_CONNECTORS_HOSTNAME;
  const xReplitToken = process.env.REPL_IDENTITY
    ? "repl " + process.env.REPL_IDENTITY
    : process.env.WEB_REPL_RENEWAL
    ? "depl " + process.env.WEB_REPL_RENEWAL
    : null;

  if (!xReplitToken) {
    throw new Error("X-Replit-Token not found for repl/depl");
  }

  connectionSettings = await fetch(
    "https://" + hostname + "/api/v2/connection?include_secrets=true&connector_names=google-sheet",
    {
      headers: {
        Accept: "application/json",
        "X-Replit-Token": xReplitToken,
      },
    }
  ).then(res => res.json()).then(data => data.items?.[0]);
  lastFetchedAt = Date.now();

  const accessToken = connectionSettings?.settings?.access_token || connectionSettings.settings?.oauth?.credentials?.access_token;

  if (!connectionSettings || !accessToken) {
    throw new Error("Google Sheets not connected");
  }
  return accessToken;
}

export async function getUncachableGoogleSheetClient() {
  const accessToken = await getAccessToken();
  const oauth2Client = new google.auth.OAuth2();
  oauth2Client.setCredentials({ access_token: accessToken });
  return google.sheets({ version: "v4", auth: oauth2Client });
}

export async function isGoogleSheetsConnected(): Promise<boolean> {
  try {
    await getAccessToken();
    return true;
  } catch {
    return false;
  }
}
