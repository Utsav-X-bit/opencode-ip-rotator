export interface Env {
  RELAY_TOKEN?: string;
}

const UPSTREAM_ORIGIN = "https://opencode.ai";
const DEFAULT_USER_AGENT = "opencode/1.18.31 ai-sdk/provider-utils/4.0.40 runtime/bun/1.3.14";

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // 1. CORS Preflight
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": "*",
        },
      });
    }

    const url = new URL(request.url);

    // 2. Health check endpoint
    if (url.pathname === "/" && request.method === "GET") {
      return new Response(JSON.stringify({ status: "healthy", service: "opencode-zen-gateway" }), {
        headers: { "content-type": "application/json" },
      });
    }

    // 3. Strict authentication verification (prevents open proxy classification)
    if (env.RELAY_TOKEN) {
      const authHeader = request.headers.get("x-relay-token") || request.headers.get("authorization");
      const token = authHeader?.replace(/^Bearer\s+/i, "")?.trim();
      const urlToken = url.searchParams.get("token");
      if (token !== env.RELAY_TOKEN && authHeader !== env.RELAY_TOKEN && urlToken !== env.RELAY_TOKEN) {
        return new Response(JSON.stringify({ error: "Unauthorized access" }), {
          status: 401,
          headers: { "content-type": "application/json" },
        });
      }
    }

    // 4. Resolve target path (supports both pi-bansos relay headers and direct path mapping)
    const relayTarget = request.headers.get("x-relay-target");
    const relayPath = request.headers.get("x-relay-path");

    let targetUrl: string;
    if (relayTarget) {
      targetUrl = `${relayTarget.replace(/\/$/, "")}${relayPath || "/"}`;
    } else {
      let upstreamPath = url.pathname;
      if (upstreamPath.startsWith("/v1/")) {
        upstreamPath = `/zen${upstreamPath}`;
      } else if (!upstreamPath.startsWith("/zen/")) {
        upstreamPath = `/zen/v1${upstreamPath}`;
      }
      const searchParams = new URLSearchParams(url.search);
      searchParams.delete("token");
      const qs = searchParams.toString();
      targetUrl = `${UPSTREAM_ORIGIN}${upstreamPath}${qs ? `?${qs}` : ""}`;
    }
    // 5. Clean and forward headers
    const headers = new Headers(request.headers);
    headers.delete("host");
    headers.delete("content-length");
    headers.delete("x-relay-token");
    headers.delete("x-relay-target");
    headers.delete("x-relay-path");
    // Never forward the relay credential upstream: if the client authenticated
    // via Authorization: Bearer <RELAY_TOKEN>, strip it (upstream uses its own auth).
    const presented = request.headers.get("x-relay-token") || request.headers.get("authorization");
    if (presented && env.RELAY_TOKEN) {
      const bare = presented.replace(/^Bearer\s+/i, "").trim();
      if (bare === env.RELAY_TOKEN || presented === env.RELAY_TOKEN) {
        headers.delete("authorization");
      }
    }
    headers.delete("cf-connecting-ip");
    headers.delete("cf-ipcountry");
    headers.delete("cf-ray");
    headers.delete("cf-visitor");

    // Enforce authentic OpenCode User-Agent
    const ua = headers.get("user-agent") || "";
    if (!ua || ua.includes("urllib") || !ua.startsWith("opencode/")) {
      headers.set("user-agent", DEFAULT_USER_AGENT);
    }

    try {
      const response = await fetch(targetUrl, {
        method: request.method,
        headers,
        body: request.method !== "GET" && request.method !== "HEAD" ? request.body : undefined,
      });

      const responseHeaders = new Headers(response.headers);
      responseHeaders.set("Access-Control-Allow-Origin", "*");

      return new Response(response.body, {
        status: response.status,
        headers: responseHeaders,
      });
    } catch (err: unknown) {
      return new Response(JSON.stringify({ error: "Gateway forward error", details: String(err) }), {
        status: 502,
        headers: { "content-type": "application/json" },
      });
    }
  },
};
