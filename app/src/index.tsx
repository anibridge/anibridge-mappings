import { swaggerUI } from "@hono/swagger-ui";
import { OpenAPIHono, createRoute, z } from "@hono/zod-openapi";
import { getMappings } from "./server/mappings";

type Bindings = { ASSETS: { fetch: (request: Request) => Promise<Response> } };

const app = new OpenAPIHono<{ Bindings: Bindings }>({ strict: false });

const PROVENANCE_URL =
  "https://github.com/anibridge/anibridge-mappings/releases/latest/download/provenance.json";

app.get("/data/provenance.json", async (c) => {
  try {
    const upstream = await fetch(PROVENANCE_URL, {
      headers: { Accept: "application/json" },
    });
    const headers = new Headers(upstream.headers);
    headers.set("Access-Control-Allow-Origin", "*");
    headers.set("Cache-Control", "public, max-age=3600");
    headers.set("Vary", "Origin");
    return new Response(await upstream.arrayBuffer(), {
      status: upstream.status,
      headers,
    });
  } catch (error) {
    console.error("Failed to proxy provenance.json", error);
    return c.json({ error: "Failed to fetch provenance data." }, 502, {
      "Access-Control-Allow-Origin": "*",
    });
  }
});

const normalizeText = (value: string | undefined) =>
  (value ?? "").trim().toLowerCase();

const toNumber = (value: string | undefined, fallback: number) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const mappingsQuerySchema = z.object({
  provider: z.string().optional(),
  id: z.string().optional(),
  scope: z.string().optional(),
  limit: z.string().optional(),
  offset: z.string().optional(),
});

const mappingsResponseSchema = z.object({
  pagination: z.object({
    limit: z.number(),
    offset: z.number(),
    total: z.number(),
    returned: z.number(),
  }),
  data: z.record(
    z.string(),
    z.record(z.string(), z.record(z.string(), z.string())),
  ),
});

const mappingsRoute = createRoute({
  method: "get",
  path: "/api/v3/mappings",
  request: { query: mappingsQuerySchema },
  responses: {
    200: {
      description: "Search mappings.json entries by source",
      content: { "application/json": { schema: mappingsResponseSchema } },
    },
  },
});

type SourceIndex = Map<string, Map<string, Map<string, string>>>;
let sourceIndexPromise: Promise<SourceIndex> | null = null;

const getSourceIndex = async () => {
  if (!sourceIndexPromise) {
    sourceIndexPromise = (async () => {
      const mappings = await getMappings();
      const index: SourceIndex = new Map();

      for (const source of Object.keys(mappings)) {
        if (source.startsWith("$")) continue;
        const [provider, id, ...scopeParts] = source.split(":");
        if (!provider || !id) continue;
        const scope = scopeParts.join(":");
        const providerKey = normalizeText(provider);
        const idKey = normalizeText(id);
        const scopeKey = normalizeText(scope);

        let idMap = index.get(providerKey);
        if (!idMap) {
          idMap = new Map();
          index.set(providerKey, idMap);
        }
        let scopeMap = idMap.get(idKey);
        if (!scopeMap) {
          scopeMap = new Map();
          idMap.set(idKey, scopeMap);
        }
        if (!scopeMap.has(scopeKey)) {
          scopeMap.set(scopeKey, source);
        }
      }

      return index;
    })().catch((err) => {
      sourceIndexPromise = null;
      throw err;
    });
  }

  return sourceIndexPromise;
};

app.openapi(mappingsRoute, async (c) => {
  const query = c.req.query();
  const providerQuery = normalizeText(query.provider);
  const idQuery = normalizeText(query.id);
  const scopeQuery = normalizeText(query.scope);
  const limit = Math.max(1, Math.min(toNumber(query.limit, 50), 1000));
  const offset = Math.max(0, toNumber(query.offset, 0));

  const mappings = await getMappings();
  const index = await getSourceIndex();
  const response: Record<string, Record<string, Record<string, string>>> = {};
  let total = 0;
  let added = 0;

  const addEntry = (
    source: string,
    target: string,
    key: string,
    value: string,
  ) => {
    if (total >= offset && added < limit) {
      const sourceBucket = (response[source] ??= {});
      const targetBucket = (sourceBucket[target] ??= {});
      targetBucket[key] = value;
      added += 1;
    }
    total += 1;
  };

  const forEachSource = (callback: (source: string) => void) => {
    if (providerQuery) {
      const idMap = index.get(providerQuery);
      if (!idMap) return;
      if (idQuery) {
        const scopeMap = idMap.get(idQuery);
        if (!scopeMap) return;
        if (scopeQuery) {
          const source = scopeMap.get(scopeQuery);
          if (source) callback(source);
          return;
        }
        for (const source of scopeMap.values()) callback(source);
        return;
      }
      for (const scopeMap of idMap.values()) {
        for (const source of scopeMap.values()) callback(source);
      }
      return;
    }
    for (const idMap of index.values()) {
      for (const scopeMap of idMap.values()) {
        for (const source of scopeMap.values()) callback(source);
      }
    }
  };

  forEachSource((source) => {
    if (source.startsWith("$")) return;
    const targets = mappings[source];
    if (!targets) return;
    for (const [target, entries] of Object.entries(targets)) {
      if (target.startsWith("$")) continue;
      for (const [key, value] of Object.entries(entries)) {
        if (key.startsWith("$")) continue;
        addEntry(source, target, key, value);
        if (added >= limit && total >= offset + limit) return;
      }
    }
  });

  return c.json({
    pagination: { limit, offset, total, returned: added },
    data: response,
  });
});

app.doc31("/openapi.json", {
  openapi: "3.1.0",
  info: { version: "3.0.0", title: "AniBridge Mappings API" },
});

app.get(
  "/docs",
  swaggerUI({
    url: "/openapi.json",
    title: "AniBridge Mappings API",
    version: "3.0.0",
  }),
);

app.get("*", (c) => c.env.ASSETS.fetch(c.req.raw));

export default app;
