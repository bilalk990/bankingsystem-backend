import rateLimit from "express-rate-limit";

export const loginLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
  message: { error: "Too many login attempts. Please try again in 15 minutes." },
  standardHeaders: true,
  legacyHeaders: false,
  validate: false,
  keyGenerator: (req) => req.ip || "unknown",
});

export const apiLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 1000,
  message: { error: "Too many requests. Please slow down and try again in a minute." },
  standardHeaders: true,
  legacyHeaders: false,
  validate: false,
  keyGenerator: (req) => req.cookies?.userId || req.ip || "unknown",
  skip: (req) => {
    const p = req.originalUrl || req.path;
    return p.includes("/import/chunk-upload") ||
           p.includes("/import/chunk-complete") ||
           p.includes("/import/zip-job") ||
           p.includes("/import/bank-statements");
  },
});
