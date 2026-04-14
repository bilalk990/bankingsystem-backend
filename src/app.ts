import express, { type Express } from "express";
import cors from "cors";
import cookieParser from "cookie-parser";
import helmet from "helmet";
import path from "path";
import router from "./routes";
import { inputSanitizer } from "./middlewares";
import { loginLimiter, apiLimiter } from "./middlewares";
import { errorHandler } from "./middlewares";

const app: Express = express();
app.set("strict routing", false);
app.set("case sensitive routing", false);

const isProduction = process.env.NODE_ENV === "production";

// app.use(helmet({
//   contentSecurityPolicy: {
//     directives: {
//       defaultSrc: ["'self'"],
//       scriptSrc: ["'self'", "'unsafe-inline'", "'unsafe-eval'"],
//       styleSrc: ["'self'", "'unsafe-inline'", "https://fonts.googleapis.com"],
//       fontSrc: ["'self'", "https://fonts.gstatic.com"],
//       imgSrc: ["'self'", "data:", "blob:", "https:"],
//       connectSrc: ["'self'", "https:", "wss:"],
//       frameSrc: ["'self'"],
//       objectSrc: ["'none'"],
//       baseUri: ["'self'"],
//       formAction: ["'self'"],
//     },
//   },
//   crossOriginEmbedderPolicy: false,
//   crossOriginResourcePolicy: { policy: "cross-origin" },
// }));

const isDev = process.env.NODE_ENV !== "production";
const envOrigin = process.env.CORS_ORIGIN;
const allowedOrigins: string[] = [
  "https://bridge-cap.company",
  "https://www.bridge-cap.company",
  "http://localhost:3003",
  "http://localhost:3001",
  "http://localhost:3002",
  "http://localhost:3000",
];

if (envOrigin) {
  envOrigin.split(",").forEach(o => allowedOrigins.push(o.trim()));
}

// Support for other platforms if needed
if (process.env.REPLIT_DEV_DOMAIN) allowedOrigins.push(`https://${process.env.REPLIT_DEV_DOMAIN}`);

app.use(cors({
  credentials: true,
  origin: (origin, callback) => {
    // Allow requests with no origin (like mobile apps or curl)
    if (!origin) return callback(null, true);
    
    // In dev mode, be more permissive if needed
    if (process.env.NODE_ENV !== "production") return callback(null, true);

    if (allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      console.log('[CORS] Blocked origin:', origin);
      callback(null, false);
    }
  },
}));
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true, limit: "10mb" }));
app.use(cookieParser());
app.use("/api", inputSanitizer);

app.set("trust proxy", 1);

// app.use("/api/auth/login", loginLimiter);
// app.use("/api", apiLimiter);

app.use("/uploads", express.static(path.join(process.cwd(), "uploads")));
app.use("/static", express.static(path.join(process.cwd(), "src/static")));

app.use("/api", router);

app.use("/api", errorHandler);

export default app;
