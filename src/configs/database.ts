import "dotenv/config";
import { drizzle } from "drizzle-orm/node-postgres";
import pg from "pg";
import * as schema from "../models";

const { Pool } = pg;

if (!process.env.DATABASE_URL) {
  throw new Error(
    "DATABASE_URL must be set. Did you forget to provision a database?",
  );
}

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 25,
  idleTimeoutMillis: 60000,
  connectionTimeoutMillis: 60000,
  statement_timeout: 600000,
  allowExitOnIdle: true,
  ssl: process.env.NODE_ENV === "production" ? {
    rejectUnauthorized: false
  } : false
});

pool.on("error", (err) => {
  console.error("[DB Pool] Idle client error:", err.message);
});

export const db = drizzle(pool, { schema });

export * from "../models";
