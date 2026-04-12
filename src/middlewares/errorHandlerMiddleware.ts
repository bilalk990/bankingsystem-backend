import type { Request, Response, NextFunction } from "express";

export function errorHandler(err: any, _req: Request, res: Response, _next: NextFunction) {
  console.error("[API ERROR]", err.message, err.stack?.split("\n").slice(0, 3).join("\n"));
  res.status(500).json({ error: "Internal server error" });
}
