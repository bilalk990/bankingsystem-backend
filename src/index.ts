import app from "./app";
import { seedRolePermissions } from "./scripts/seedPermissions";
import { seedDefaultFunders } from "./scripts/seedFunders";
import { resetAdminPassword } from "./scripts/seedAdmin";
import { oneTimeDataWipe } from "./scripts/oneTimeCleanup";
import { resumeIncompleteScrubbing } from "./controllers/importController";

process.on("unhandledRejection", (reason: any) => {
  const msg = reason?.message || String(reason);
  if (msg.includes("FormatError") || msg.includes("Illegal character") || msg.includes("Invalid PDF")) {
    console.warn(`[PDF] Suppressed unhandled PDF parsing error: ${msg.slice(0, 200)}`);
    return;
  }
  console.error("[UNHANDLED REJECTION]", reason);
});

process.on("uncaughtException", (err: Error) => {
  const msg = err.message || "";
  if (msg.includes("FormatError") || msg.includes("Illegal character") || msg.includes("Invalid PDF")) {
    console.warn(`[PDF] Suppressed uncaught PDF parsing error: ${msg.slice(0, 200)}`);
    return;
  }
  console.error("[UNCAUGHT EXCEPTION]", err);
  process.exit(1);
});

const rawPort = process.env["PORT"];

if (!rawPort) {
  throw new Error(
    "PORT environment variable is required but was not provided.",
  );
}

const port = Number(rawPort);

if (Number.isNaN(port) || port <= 0) {
  throw new Error(`Invalid PORT value: "${rawPort}"`);
}

Promise.all([seedRolePermissions(), seedDefaultFunders(), resetAdminPassword(), oneTimeDataWipe()]).then(() => {
  const server = app.listen(port, () => {
    console.log(`Server listening on port ${port}`);
  });
  server.timeout = 30 * 60 * 1000;
  server.keepAliveTimeout = 30 * 60 * 1000;
  server.headersTimeout = 31 * 60 * 1000;

  console.log("[Resume-Scrub] Auto-resume disabled — use 'Start Scrubbing' button to resume incomplete leads");
}).catch((err) => {
  console.error("Failed to seed:", err);
  const server = app.listen(port, () => {
    console.log(`Server listening on port ${port}`);
  });
  server.timeout = 30 * 60 * 1000;
  server.keepAliveTimeout = 30 * 60 * 1000;
  server.headersTimeout = 31 * 60 * 1000;
});
