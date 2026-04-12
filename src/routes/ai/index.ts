import { Router, type IRouter } from "express";
import chatRoutes from "./chatRoutes";
import insightsRoutes from "./insightsRoutes";
import scriptsRoutes from "./scriptsRoutes";
import dashboardRoutes from "./dashboardRoutes";
import renewalsRoutes from "./renewalsRoutes";
import killerModeRoutes from "./killer-modeRoutes";

const router: IRouter = Router();

router.use(chatRoutes);
router.use(insightsRoutes);
router.use(scriptsRoutes);
router.use(dashboardRoutes);
router.use(renewalsRoutes);
router.use(killerModeRoutes);

export default router;
