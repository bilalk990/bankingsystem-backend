import { Router, type IRouter } from "express";
import analysisRoutes from "./routesRoutes";
import underwritingRoutes from "./underwritingRoutes";

const router: IRouter = Router();

router.use(analysisRoutes);
router.use(underwritingRoutes);

export default router;
