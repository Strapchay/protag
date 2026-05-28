import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";

function env(name: string): string {
  return process.env[name] ?? "";
}

function enabled(value: string): boolean {
  return value.toLowerCase() === "true" || value === "1";
}

export default function (pi: ExtensionAPI) {
  if (!enabled(env("AION_INFERENCE_GATEWAY_ENABLED"))) {
    return;
  }

  const provider = env("AION_TARGET_PROVIDER");
  const baseUrl = env("AION_INFERENCE_GATEWAY_URL");
  if (!provider || !baseUrl) {
    return;
  }

  const config: any = {
    baseUrl,
    headers: {
      "X-Aion-Gateway-Key": env("AION_INFERENCE_GATEWAY_KEY"),
      "X-Aion-Agent-ID": env("AION_AGENT_ID"),
      "X-Aion-Domain-ID": env("AION_DOMAIN_ID"),
      "X-Aion-Target-Provider": provider,
      "X-Aion-Target-Profile": env("AION_TARGET_PROFILE"),
    },
  };

  const api = env("AION_TARGET_API");
  if (api) {
    config.api = api;
  }

  pi.registerProvider(provider, config);
}
