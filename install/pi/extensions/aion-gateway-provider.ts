import * as http from "node:http";
import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import {
  calculateCost,
  createAssistantMessageEventStream,
  parseStreamingJson,
  type AssistantMessage,
  type AssistantMessageEventStream,
  type Context,
  type Model,
  type SimpleStreamOptions,
} from "@mariozechner/pi-ai";

const AION_GATEWAY_PROVIDER = "aion-gateway";
const AION_UNIX_API = "aion-unix-openai";

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

  const targetModel = env("AION_TARGET_MODEL");
  if (!targetModel) {
    return;
  }

  const socketPath = env("AION_INFERENCE_SOCKET");
  if (socketPath) {
    pi.registerProvider(AION_GATEWAY_PROVIDER, {
      baseUrl: "http://unix",
      apiKey: "AION_AGENT_CAPABILITY",
      api: AION_UNIX_API,
      models: [buildGatewayModel(targetModel, AION_UNIX_API)],
      streamSimple: streamAionUnix,
    } as any);
    notifyUnixGatewayLoaded(socketPath);
    return;
  }

  registerTCPGateway(pi, targetModel);
}

function registerTCPGateway(pi: ExtensionAPI, targetModel: string) {
  const baseUrl = env("AION_INFERENCE_GATEWAY_URL");
  const targetProvider = env("AION_TARGET_PROVIDER");
  if (!baseUrl || !targetProvider) {
    return;
  }
  const headers = {
    "X-Aion-Gateway-Key": env("AION_INFERENCE_GATEWAY_KEY"),
    "X-Aion-Agent-ID": env("AION_AGENT_ID"),
    "X-Aion-Domain-ID": env("AION_DOMAIN_ID"),
    "X-Aion-Target-Provider": targetProvider,
    "X-Aion-Target-Profile": env("AION_TARGET_PROFILE"),
  };
  const api = env("AION_TARGET_API") || "openai-completions";
  pi.registerProvider(AION_GATEWAY_PROVIDER, {
    baseUrl,
    apiKey: "AION_INFERENCE_GATEWAY_KEY",
    api,
    headers,
    models: [buildGatewayModel(targetModel, api)],
  } as any);
  notifyTCPGatewayLoaded(baseUrl, headers);
}

function buildGatewayModel(modelId: string, api: string) {
  const contextWindow = parseInt(env("AION_TARGET_CONTEXT_WINDOW") || "128000", 10);
  const maxTokens = parseInt(env("AION_TARGET_MAX_TOKENS") || "4096", 10);
  return {
    id: modelId,
    name: env("AION_TARGET_PROFILE") || modelId,
    api,
    reasoning: false,
    input: ["text", "image"],
    contextWindow,
    maxTokens,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    compat: {
      supportsReasoningEffort: false,
      supportsDeveloperRole: false,
      maxTokensField: "max_tokens",
    },
  };
}

function streamAionUnix(
  model: Model<any>,
  context: Context,
  options?: SimpleStreamOptions,
): AssistantMessageEventStream {
  if (env("AION_TARGET_API") === "anthropic-messages") {
    return streamAionUnixAnthropic(model, context, options);
  }
  return streamAionUnixOpenAI(model, context, options);
}

function streamAionUnixOpenAI(
  model: Model<any>,
  context: Context,
  options?: SimpleStreamOptions,
): AssistantMessageEventStream {
  const stream = createAssistantMessageEventStream();
  const output: AssistantMessage = {
    role: "assistant",
    content: [],
    api: model.api,
    provider: model.provider,
    model: model.id,
    usage: emptyUsage(),
    stopReason: "stop",
    timestamp: Date.now(),
  };

  void (async () => {
    try {
      let payload: any = buildOpenAIRequest(model, context, options);
      const replacement = await options?.onPayload?.(payload, model);
      if (replacement !== undefined) {
        payload = replacement;
      }
      const response = await requestUnix(
        env("AION_INFERENCE_SOCKET"),
        "/v1/chat/completions",
        payload,
        options?.signal,
        options?.timeoutMs,
      );
      await options?.onResponse?.(
        { status: response.statusCode ?? 0, headers: responseHeaders(response) },
        model,
      );
      if ((response.statusCode ?? 500) >= 400) {
        throw new Error(await readHTTPError(response));
      }

      stream.push({ type: "start", partial: output });
      await consumeOpenAIStream(response, model, output, stream);
      if (options?.signal?.aborted) {
        throw new Error("Request was aborted");
      }
      if (output.stopReason === "error") {
        throw new Error(output.errorMessage || "Provider returned an error stop reason");
      }
      stream.push({ type: "done", reason: output.stopReason as any, message: output });
      stream.end();
    } catch (error) {
      output.stopReason = options?.signal?.aborted ? "aborted" : "error";
      output.errorMessage = error instanceof Error ? error.message : String(error);
      stream.push({ type: "error", reason: output.stopReason, error: output });
      stream.end();
    }
  })();

  return stream;
}

function streamAionUnixAnthropic(
  model: Model<any>,
  context: Context,
  options?: SimpleStreamOptions,
): AssistantMessageEventStream {
  const stream = createAssistantMessageEventStream();
  const output: AssistantMessage = {
    role: "assistant",
    content: [],
    api: model.api,
    provider: model.provider,
    model: model.id,
    usage: emptyUsage(),
    stopReason: "stop",
    timestamp: Date.now(),
  };
  void (async () => {
    try {
      let payload: any = buildAnthropicRequest(model, context, options);
      const replacement = await options?.onPayload?.(payload, model);
      if (replacement !== undefined) payload = replacement;
      const response = await requestUnix(
        env("AION_INFERENCE_SOCKET"),
        "/v1/messages",
        payload,
        options?.signal,
        options?.timeoutMs,
      );
      await options?.onResponse?.(
        { status: response.statusCode ?? 0, headers: responseHeaders(response) },
        model,
      );
      if ((response.statusCode ?? 500) >= 400) {
        throw new Error(await readHTTPError(response));
      }
      stream.push({ type: "start", partial: output });
      await consumeAnthropicStream(response, model, output, stream);
      if (options?.signal?.aborted) throw new Error("Request was aborted");
      if (output.stopReason === "error") {
        throw new Error(output.errorMessage || "Provider returned an error stop reason");
      }
      stream.push({ type: "done", reason: output.stopReason as any, message: output });
      stream.end();
    } catch (error) {
      output.stopReason = options?.signal?.aborted ? "aborted" : "error";
      output.errorMessage = error instanceof Error ? error.message : String(error);
      stream.push({ type: "error", reason: output.stopReason, error: output });
      stream.end();
    }
  })();
  return stream;
}

function buildAnthropicRequest(model: Model<any>, context: Context, options?: SimpleStreamOptions) {
  const payload: any = {
    model: model.id,
    messages: convertAnthropicMessages(context),
    max_tokens: options?.maxTokens || model.maxTokens || 4096,
    stream: true,
  };
  if (context.systemPrompt) payload.system = context.systemPrompt;
  if (options?.temperature !== undefined) payload.temperature = options.temperature;
  if (context.tools?.length) {
    payload.tools = context.tools.map((tool) => ({
      name: tool.name,
      description: tool.description,
      input_schema: tool.parameters,
    }));
  }
  return payload;
}

function convertAnthropicMessages(context: Context): any[] {
  const messages: any[] = [];
  for (const message of context.messages) {
    if (message.role === "user") {
      const content =
        typeof message.content === "string"
          ? [{ type: "text", text: message.content }]
          : message.content.map((block) =>
              block.type === "text"
                ? { type: "text", text: block.text }
                : {
                    type: "image",
                    source: { type: "base64", media_type: block.mimeType, data: block.data },
                  },
            );
      messages.push({ role: "user", content });
      continue;
    }
    if (message.role === "assistant") {
      const content = message.content.flatMap((block: any) => {
        if (block.type === "text" && block.text) return [{ type: "text", text: block.text }];
        if (block.type === "toolCall") {
          return [{ type: "tool_use", id: block.id, name: block.name, input: block.arguments }];
        }
        return [];
      });
      if (content.length) messages.push({ role: "assistant", content });
      continue;
    }
    const content = message.content.map((block: any) =>
      block.type === "text"
        ? { type: "text", text: block.text }
        : {
            type: "image",
            source: { type: "base64", media_type: block.mimeType, data: block.data },
          },
    );
    messages.push({
      role: "user",
      content: [{ type: "tool_result", tool_use_id: message.toolCallId, content, is_error: message.isError }],
    });
  }
  return mergeAdjacentAnthropicMessages(messages);
}

function mergeAdjacentAnthropicMessages(messages: any[]): any[] {
  const merged: any[] = [];
  for (const message of messages) {
    const previous = merged[merged.length - 1];
    if (previous?.role === message.role && Array.isArray(previous.content) && Array.isArray(message.content)) {
      previous.content.push(...message.content);
    } else {
      merged.push(message);
    }
  }
  return merged;
}

async function consumeAnthropicStream(
  response: http.IncomingMessage,
  model: Model<any>,
  output: AssistantMessage,
  stream: AssistantMessageEventStream,
) {
  const blocks = new Map<number, any>();
  const partialArgs = new Map<number, string>();
  const finishBlock = (index: number) => {
    const block = blocks.get(index);
    if (!block) return;
    const contentIndex = output.content.indexOf(block);
    if (block.type === "text") {
      stream.push({ type: "text_end", contentIndex, content: block.text, partial: output });
    } else if (block.type === "thinking") {
      stream.push({ type: "thinking_end", contentIndex, content: block.thinking, partial: output });
    } else if (block.type === "toolCall") {
      block.arguments = parseStreamingJson(partialArgs.get(index) || "{}");
      stream.push({ type: "toolcall_end", contentIndex, toolCall: block, partial: output });
    }
    blocks.delete(index);
    partialArgs.delete(index);
  };

  for await (const data of readSSE(response)) {
    if (!data) continue;
    const event = JSON.parse(data);
    if (event.type === "error") {
      throw new Error(event.error?.message || "Anthropic stream returned an error");
    }
    if (event.type === "message_start") {
      output.responseId ||= event.message?.id;
      if (event.message?.model && event.message.model !== model.id) output.responseModel ||= event.message.model;
      updateAnthropicUsage(output, model, event.message?.usage);
      continue;
    }
    if (event.type === "content_block_start") {
      const source = event.content_block ?? {};
      let block: any;
      let eventType: "text_start" | "thinking_start" | "toolcall_start";
      if (source.type === "tool_use") {
        block = { type: "toolCall", id: source.id || "", name: source.name || "", arguments: source.input || {} };
        const hasInput = source.input && Object.keys(source.input).length > 0;
        partialArgs.set(event.index, hasInput ? JSON.stringify(source.input) : "");
        eventType = "toolcall_start";
      } else if (source.type === "thinking") {
        block = { type: "thinking", thinking: source.thinking || "", thinkingSignature: source.signature };
        eventType = "thinking_start";
      } else {
        block = { type: "text", text: source.text || "" };
        eventType = "text_start";
      }
      blocks.set(event.index, block);
      output.content.push(block);
      stream.push({ type: eventType, contentIndex: output.content.length - 1, partial: output } as any);
      continue;
    }
    if (event.type === "content_block_delta") {
      const block = blocks.get(event.index);
      if (!block) continue;
      const contentIndex = output.content.indexOf(block);
      const delta = event.delta ?? {};
      if (delta.type === "text_delta" && block.type === "text") {
        block.text += delta.text || "";
        stream.push({ type: "text_delta", contentIndex, delta: delta.text || "", partial: output });
      } else if (delta.type === "thinking_delta" && block.type === "thinking") {
        block.thinking += delta.thinking || "";
        stream.push({ type: "thinking_delta", contentIndex, delta: delta.thinking || "", partial: output });
      } else if (delta.type === "signature_delta" && block.type === "thinking") {
        block.thinkingSignature = (block.thinkingSignature || "") + (delta.signature || "");
      } else if (delta.type === "input_json_delta" && block.type === "toolCall") {
        const next = (partialArgs.get(event.index) || "") + (delta.partial_json || "");
        partialArgs.set(event.index, next);
        block.arguments = parseStreamingJson(next);
        stream.push({ type: "toolcall_delta", contentIndex, delta: delta.partial_json || "", partial: output });
      }
      continue;
    }
    if (event.type === "content_block_stop") {
      finishBlock(event.index);
      continue;
    }
    if (event.type === "message_delta") {
      if (event.delta?.stop_reason) output.stopReason = mapAnthropicStopReason(event.delta.stop_reason) as any;
      updateAnthropicUsage(output, model, event.usage);
    }
  }
  for (const index of [...blocks.keys()]) finishBlock(index);
}

function updateAnthropicUsage(output: AssistantMessage, model: Model<any>, raw: any) {
  if (!raw) return;
  if (raw.input_tokens !== undefined) output.usage.input = raw.input_tokens;
  if (raw.output_tokens !== undefined) output.usage.output = raw.output_tokens;
  if (raw.cache_read_input_tokens !== undefined) output.usage.cacheRead = raw.cache_read_input_tokens;
  if (raw.cache_creation_input_tokens !== undefined) output.usage.cacheWrite = raw.cache_creation_input_tokens;
  output.usage.totalTokens = output.usage.input + output.usage.output + output.usage.cacheRead + output.usage.cacheWrite;
  calculateCost(model, output.usage);
}

function mapAnthropicStopReason(reason: string) {
  if (reason === "max_tokens") return "length";
  if (reason === "tool_use") return "toolUse";
  if (reason === "end_turn" || reason === "stop_sequence") return "stop";
  return "error";
}

function buildOpenAIRequest(model: Model<any>, context: Context, options?: SimpleStreamOptions) {
  const payload: any = {
    model: model.id,
    messages: convertMessages(context),
    stream: true,
    stream_options: { include_usage: true },
  };
  if (options?.maxTokens) {
    payload.max_tokens = options.maxTokens;
  }
  if (options?.temperature !== undefined) {
    payload.temperature = options.temperature;
  }
  if (context.tools?.length) {
    payload.tools = context.tools.map((tool) => ({
      type: "function",
      function: {
        name: tool.name,
        description: tool.description,
        parameters: tool.parameters,
        strict: false,
      },
    }));
  }
  return payload;
}

function convertMessages(context: Context): any[] {
  const messages: any[] = [];
  if (context.systemPrompt) {
    messages.push({ role: "system", content: context.systemPrompt });
  }
  for (const message of context.messages) {
    if (message.role === "user") {
      if (typeof message.content === "string") {
        messages.push({ role: "user", content: message.content });
      } else {
        const content = message.content.map((block) =>
          block.type === "text"
            ? { type: "text", text: block.text }
            : {
                type: "image_url",
                image_url: { url: `data:${block.mimeType};base64,${block.data}` },
              },
        );
        if (content.length) {
          messages.push({ role: "user", content });
        }
      }
      continue;
    }
    if (message.role === "assistant") {
      const text = message.content
        .filter((block) => block.type === "text")
        .map((block: any) => block.text)
        .join("");
      const toolCalls = message.content
        .filter((block) => block.type === "toolCall")
        .map((block: any) => ({
          id: block.id,
          type: "function",
          function: { name: block.name, arguments: JSON.stringify(block.arguments) },
        }));
      if (text || toolCalls.length) {
        messages.push({
          role: "assistant",
          content: text || null,
          ...(toolCalls.length ? { tool_calls: toolCalls } : {}),
        });
      }
      continue;
    }
    const content = message.content
      .filter((block) => block.type === "text")
      .map((block: any) => block.text)
      .join("\n");
    messages.push({
      role: "tool",
      tool_call_id: message.toolCallId,
      content: content || "(tool completed without text output)",
    });
  }
  return messages;
}

async function consumeOpenAIStream(
  response: http.IncomingMessage,
  model: Model<any>,
  output: AssistantMessage,
  stream: AssistantMessageEventStream,
) {
  let currentBlock: any = null;
  const finishCurrentBlock = () => {
    if (!currentBlock) return;
    const contentIndex = output.content.indexOf(currentBlock);
    if (contentIndex < 0) return;
    if (currentBlock.type === "text") {
      stream.push({ type: "text_end", contentIndex, content: currentBlock.text, partial: output });
    } else if (currentBlock.type === "thinking") {
      stream.push({
        type: "thinking_end",
        contentIndex,
        content: currentBlock.thinking,
        partial: output,
      });
    } else if (currentBlock.type === "toolCall") {
      currentBlock.arguments = parseStreamingJson(currentBlock.partialArgs);
      delete currentBlock.partialArgs;
      delete currentBlock.streamIndex;
      stream.push({
        type: "toolcall_end",
        contentIndex,
        toolCall: currentBlock,
        partial: output,
      });
    }
    currentBlock = null;
  };

  for await (const data of readSSE(response)) {
    if (!data || data === "[DONE]") continue;
    const chunk = JSON.parse(data);
    output.responseId ||= chunk.id;
    if (chunk.model && chunk.model !== model.id) {
      output.responseModel ||= chunk.model;
    }
    if (chunk.usage) {
      output.usage = parseUsage(chunk.usage, model);
    }
    const choice = Array.isArray(chunk.choices) ? chunk.choices[0] : undefined;
    if (!choice) continue;
    if (choice.finish_reason) {
      output.stopReason = mapStopReason(choice.finish_reason) as any;
    }
    const delta = choice.delta ?? {};
    const reasoning = delta.reasoning_content || delta.reasoning || delta.reasoning_text;
    if (reasoning) {
      if (!currentBlock || currentBlock.type !== "thinking") {
        finishCurrentBlock();
        currentBlock = { type: "thinking", thinking: "" };
        output.content.push(currentBlock);
        stream.push({
          type: "thinking_start",
          contentIndex: output.content.length - 1,
          partial: output,
        });
      }
      currentBlock.thinking += reasoning;
      stream.push({
        type: "thinking_delta",
        contentIndex: output.content.indexOf(currentBlock),
        delta: reasoning,
        partial: output,
      });
    }
    if (delta.content) {
      if (!currentBlock || currentBlock.type !== "text") {
        finishCurrentBlock();
        currentBlock = { type: "text", text: "" };
        output.content.push(currentBlock);
        stream.push({ type: "text_start", contentIndex: output.content.length - 1, partial: output });
      }
      currentBlock.text += delta.content;
      stream.push({
        type: "text_delta",
        contentIndex: output.content.indexOf(currentBlock),
        delta: delta.content,
        partial: output,
      });
    }
    for (const toolCall of delta.tool_calls ?? []) {
      const streamIndex = typeof toolCall.index === "number" ? toolCall.index : undefined;
      if (
        !currentBlock ||
        currentBlock.type !== "toolCall" ||
        (streamIndex !== undefined && currentBlock.streamIndex !== streamIndex)
      ) {
        finishCurrentBlock();
        currentBlock = {
          type: "toolCall",
          id: toolCall.id || "",
          name: toolCall.function?.name || "",
          arguments: {},
          partialArgs: "",
          streamIndex,
        };
        output.content.push(currentBlock);
        stream.push({
          type: "toolcall_start",
          contentIndex: output.content.length - 1,
          partial: output,
        });
      }
      currentBlock.id ||= toolCall.id || "";
      currentBlock.name ||= toolCall.function?.name || "";
      const argumentDelta = toolCall.function?.arguments || "";
      currentBlock.partialArgs += argumentDelta;
      currentBlock.arguments = parseStreamingJson(currentBlock.partialArgs);
      stream.push({
        type: "toolcall_delta",
        contentIndex: output.content.indexOf(currentBlock),
        delta: argumentDelta,
        partial: output,
      });
    }
  }
  finishCurrentBlock();
}

function requestUnix(
  socketPath: string,
  path: string,
  payload: unknown,
  signal?: AbortSignal,
  timeoutMs?: number,
): Promise<http.IncomingMessage> {
  return new Promise((resolve, reject) => {
    if (!socketPath) {
      reject(new Error("AION_INFERENCE_SOCKET is not configured"));
      return;
    }
    const body = JSON.stringify(payload);
    let response: http.IncomingMessage | undefined;
    let timer: NodeJS.Timeout | undefined;
    const request = http.request(
      {
        socketPath,
        path,
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(body).toString(),
          "X-Aion-Agent-Capability": env("AION_AGENT_CAPABILITY"),
        },
      },
      (incoming) => {
        response = incoming;
        incoming.once("close", cleanup);
        resolve(incoming);
      },
    );
    const cleanup = () => {
      signal?.removeEventListener("abort", abort);
      if (timer) clearTimeout(timer);
    };
    const abort = () => {
      const error = new Error("Request was aborted");
      response?.destroy(error);
      request.destroy(error);
    };
    if (signal?.aborted) {
      abort();
    } else {
      signal?.addEventListener("abort", abort, { once: true });
    }
    if (timeoutMs !== undefined && timeoutMs > 0) {
      timer = setTimeout(() => {
        const error = new Error(`Request timed out after ${timeoutMs}ms`);
        response?.destroy(error);
        request.destroy(error);
      }, timeoutMs);
    }
    request.once("error", (error) => {
      cleanup();
      reject(error);
    });
    request.end(body);
  });
}

async function* readSSE(response: http.IncomingMessage): AsyncGenerator<string> {
  const decoder = new TextDecoder();
  let buffer = "";
  for await (const chunk of response) {
    buffer += decoder.decode(chunk as Buffer, { stream: true }).replace(/\r\n/g, "\n");
    let boundary = buffer.indexOf("\n\n");
    while (boundary >= 0) {
      const event = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);
      const data = event
        .split("\n")
        .filter((line) => line.startsWith("data:"))
        .map((line) => line.slice(5).trimStart())
        .join("\n");
      if (data) yield data;
      boundary = buffer.indexOf("\n\n");
    }
  }
  buffer += decoder.decode();
  const data = buffer
    .split("\n")
    .filter((line) => line.startsWith("data:"))
    .map((line) => line.slice(5).trimStart())
    .join("\n");
  if (data) yield data;
}

function parseUsage(raw: any, model: Model<any>) {
  const cached = raw.prompt_tokens_details?.cached_tokens ?? raw.prompt_cache_hit_tokens ?? 0;
  const cacheWrite = raw.prompt_tokens_details?.cache_write_tokens ?? 0;
  const cacheRead = cacheWrite > 0 ? Math.max(0, cached - cacheWrite) : cached;
  const input = Math.max(0, (raw.prompt_tokens ?? 0) - cacheRead - cacheWrite);
  const output = raw.completion_tokens ?? 0;
  const usage = {
    input,
    output,
    cacheRead,
    cacheWrite,
    totalTokens: input + output + cacheRead + cacheWrite,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  };
  calculateCost(model, usage);
  return usage;
}

function emptyUsage() {
  return {
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: 0,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  };
}

function mapStopReason(reason: string) {
  if (reason === "length") return "length";
  if (reason === "tool_calls" || reason === "function_call") return "toolUse";
  if (reason === "stop" || reason === "end") return "stop";
  return "error";
}

function responseHeaders(response: http.IncomingMessage): Record<string, string> {
  const headers: Record<string, string> = {};
  for (const [key, value] of Object.entries(response.headers)) {
    if (Array.isArray(value)) headers[key] = value.join(", ");
    else if (value !== undefined) headers[key] = value;
  }
  return headers;
}

async function readHTTPError(response: http.IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of response) chunks.push(Buffer.from(chunk as Buffer));
  const body = Buffer.concat(chunks).toString("utf8").trim();
  return `Aion inference gateway returned HTTP ${response.statusCode ?? 500}${body ? `: ${body}` : ""}`;
}

function notifyUnixGatewayLoaded(socketPath: string) {
  const body = JSON.stringify({ transport: "unix" });
  const request = http.request({
    socketPath,
    path: "/aion/gateway/extension-loaded",
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Content-Length": Buffer.byteLength(body).toString(),
      "X-Aion-Agent-Capability": env("AION_AGENT_CAPABILITY"),
    },
  });
  request.on("error", () => {});
  request.end(body);
}

function notifyTCPGatewayLoaded(baseUrl: string, headers: Record<string, string>) {
  const url = `${baseUrl.replace(/\/+$/, "")}/aion/gateway/extension-loaded`;
  const body = JSON.stringify({
    agent_id: headers["X-Aion-Agent-ID"],
    domain_id: headers["X-Aion-Domain-ID"],
    provider: headers["X-Aion-Target-Provider"],
    profile: headers["X-Aion-Target-Profile"],
  });
  fetch(url, { method: "POST", headers: { ...headers, "Content-Type": "application/json" }, body }).catch(
    () => {},
  );
}
