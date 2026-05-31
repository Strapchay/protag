import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";

const AION_GATEWAY_PROVIDER = "aion-gateway";

function env(name: string): string {
  return process.env[name] ?? "";
}

function enabled(value: string): boolean {
  return value.toLowerCase() === "true" || value === "1";
}

export default function (pi: ExtensionAPI) {
  const globalKey = "__aionGatewayProviderRegistered";
  const globalState = globalThis as any;
  if (globalState[globalKey]) {
    return;
  }
  globalState[globalKey] = true;

  if (!enabled(env("AION_INFERENCE_GATEWAY_ENABLED"))) {
    return;
  }

  const baseUrl = env("AION_INFERENCE_GATEWAY_URL");
  const targetProvider = env("AION_TARGET_PROVIDER");
  const targetModel = env("AION_TARGET_MODEL");
  if (!baseUrl || !targetProvider || !targetModel) {
    return;
  }

  const config: any = {
    baseUrl,
    apiKey: "AION_INFERENCE_GATEWAY_KEY",
    headers: {
      "X-Aion-Gateway-Key": env("AION_INFERENCE_GATEWAY_KEY"),
      "X-Aion-Agent-ID": env("AION_AGENT_ID"),
      "X-Aion-Domain-ID": env("AION_DOMAIN_ID"),
      "X-Aion-Target-Provider": targetProvider,
      "X-Aion-Target-Profile": env("AION_TARGET_PROFILE"),
    },
    models: [buildGatewayModel(targetModel)],
  };

  const api = env("AION_TARGET_API");
  if (api) {
    config.api = api;
  }

  pi.registerProvider(AION_GATEWAY_PROVIDER, config);
  notifyGatewayLoaded(baseUrl, config.headers);
}

function buildGatewayModel(modelId: string) {
  const contextWindow = parseInt(env("AION_TARGET_CONTEXT_WINDOW") || "128000", 10);
  const maxTokens = parseInt(env("AION_TARGET_MAX_TOKENS") || "4096", 10);
  const api = env("AION_TARGET_API");

  const model: any = {
    id: modelId,
    name: env("AION_TARGET_PROFILE") || modelId,
    reasoning: false,
    input: ["text"],
    contextWindow,
    maxTokens,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  };

  if (api === "openai-completions") {
    model.compat = {
      supportsReasoningEffort: false,
      supportsDeveloperRole: false,
      maxTokensField: "max_tokens",
    };
  }

  return model;
}

function notifyGatewayLoaded(baseUrl: string, headers: Record<string, string>) {
  const url = `${baseUrl.replace(/\/+$/, "")}/aion/gateway/extension-loaded`;
  const body = JSON.stringify({
    agent_id: headers["X-Aion-Agent-ID"],
    domain_id: headers["X-Aion-Domain-ID"],
    provider: headers["X-Aion-Target-Provider"],
    profile: headers["X-Aion-Target-Profile"],
  });

  fetch(url, {
    method: "POST",
    headers: {
      ...headers,
      "Content-Type": "application/json",
    },
    body,
  }).catch(() => {
    // The gateway log is best-effort observability. A failed ping must not
    // prevent Pi from starting or using its provider configuration.
  });
}
