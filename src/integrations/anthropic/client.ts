import Anthropic from "@anthropic-ai/sdk";

const useIntegration = !!(process.env.AI_INTEGRATIONS_ANTHROPIC_BASE_URL && process.env.AI_INTEGRATIONS_ANTHROPIC_API_KEY);
const useOwnKey = !useIntegration && !!process.env.ANTHROPIC_API_KEY;

if (!useIntegration && !useOwnKey) {
  throw new Error(
    "No Anthropic credentials available. Set AI_INTEGRATIONS_ANTHROPIC_BASE_URL + AI_INTEGRATIONS_ANTHROPIC_API_KEY, or ANTHROPIC_API_KEY.",
  );
}

export const anthropic = useIntegration
  ? new Anthropic({
      apiKey: process.env.AI_INTEGRATIONS_ANTHROPIC_API_KEY,
      baseURL: process.env.AI_INTEGRATIONS_ANTHROPIC_BASE_URL,
    })
  : new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
