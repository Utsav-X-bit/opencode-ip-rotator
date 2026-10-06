/**
 * Deno Deploy Edge Relay for OpenCode Zen
 *
 * Runs on Deno Deploy (Google Cloud Edge infrastructure worldwide).
 * 100,000 requests/day free forever.
 */

const UPSTREAM_DEFAULT = "https://opencode.ai";
const port = Number(Deno.env.get("PORT")) || 8000;

Deno.serve({ port }, async (req: Request) => {
  // CORS Preflight
  if (req.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "*",
      },
    });
  }

  const url = new URL(req.url);

  // Health check
  if (url.pathname === "/" && req.method === "GET" && !req.headers.has("x-relay-target")) {
    return new Response(JSON.stringify({ status: "healthy", service: "opencode-deno-relay" }), {
      headers: { "content-type": "application/json" },
    });
  }

  // Support both pi-bansos relay header and path-based forwarding
  const relayTarget = req.headers.get("x-relay-target");
  const relayPath = req.headers.get("x-relay-path");

  let targetUrl: string;
  if (relayTarget) {
    targetUrl = `${relayTarget.replace(/\/$/, "")}${relayPath || "/"}`;
  } else if (url.pathname.startsWith("/zen/")) {
    targetUrl = `${UPSTREAM_DEFAULT}${url.pathname}${url.search}`;
  } else {
    targetUrl = `${UPSTREAM_DEFAULT}/zen${url.pathname}${url.search}`;
  }

  // Forward headers (strip hop-by-hop and length headers)
  const headers = new Headers(req.headers);
  headers.delete("host");
  headers.delete("x-relay-target");
  headers.delete("x-relay-path");
  headers.delete("content-length");
  const ua = headers.get("user-agent") || "";
  if (!ua || ua.includes("urllib")) {
    headers.set("user-agent", "opencode/1.18.31 ai-sdk/provider-utils/4.0.40 runtime/bun/1.3.14");
  }
  try {
    const upstream = await fetch(targetUrl, {
      method: req.method,
      headers,
      body: req.method !== "GET" && req.method !== "HEAD" ? req.body : undefined,
    });

    const responseHeaders = new Headers(upstream.headers);
    responseHeaders.set("Access-Control-Allow-Origin", "*");

    return new Response(upstream.body, {
      status: upstream.status,
      headers: responseHeaders,
    });
  } catch (error) {
    return new Response(JSON.stringify({ error: "Upstream connection failed", details: String(error) }), {
      status: 502,
      headers: { "content-type": "application/json" },
    });
  }
});
