import { OpenAPIHono, createRoute, z } from "@hono/zod-openapi";
import { getMappings } from "./mappings";
import {
  filterMappings,
  getProvenance,
  paginateMappings,
  summarizeProvenance,
} from "./provenance";
import type { MappingFilters, PresenceFilter, SortOrder } from "./provenance";

const toNumber = (value: string | undefined, fallback: number) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const toPresence = (value: string | undefined): PresenceFilter => {
  if (value === "present" || value === "missing") return value;
  return "all";
};

const toSortOrder = (value: string | undefined): SortOrder => {
  if (value === "present" || value === "missing" || value === "timeline") {
    return value;
  }
  return "default";
};

const buildFilters = (query: Record<string, string | undefined>) => {
  const perPage = toNumber(query.perPage ?? query.maxItems, 50);
  const page = toNumber(query.page, 1);
  const filters: MappingFilters = {
    source: query.source ?? "",
    target: query.target ?? "",
    actor: query.actor ?? "",
    reason: query.reason ?? "",
    range: query.range ?? "",
    stage: query.stage ?? "all",
    present: toPresence(query.present),
    sort: toSortOrder(query.sort),
    page,
    perPage,
  };
  return filters;
};

const normalizeText = (value: string | undefined) =>
  (value ?? "").trim().toLowerCase();

type MappingEntry = {
  source: string;
  target: string;
  key: string;
  value: string;
};

export const api = new OpenAPIHono({ strict: false });

api.onError((err, c) => {
  console.error("API error", err);
  return c.json({ error: "Internal Server Error" }, 500);
});

const errorSchema = z.object({ error: z.string() });
const dictRangeSchema = z.object({
  s: z.string().optional(),
  t: z.string().optional(),
});
const dictSchema = z.object({
  descriptors: z.array(z.string()),
  actions: z.array(z.string()),
  stages: z.array(z.string()),
  actors: z.array(z.string()),
  reasons: z.array(z.string()),
  ranges: z.array(dictRangeSchema),
});
const mappingEventSchema = z.object({
  a: z.number(),
  s: z.number(),
  ac: z.number(),
  rs: z.number(),
  r: z.number(),
  e: z.boolean(),
});
const mappingSchema = z.object({
  s: z.number(),
  t: z.number(),
  p: z.boolean().optional(),
  n: z.number().optional(),
  ev: z.array(mappingEventSchema).optional(),
});
const provenanceSchema = z.object({
  dict: dictSchema,
  mappings: z.array(mappingSchema),
  $meta: z.object({}).catchall(z.unknown()).optional(),
});
const summarySchema = z.object({
  generated_on: z.string().nullable(),
  mappings: z.number(),
  present_mappings: z.number(),
  missing_mappings: z.number(),
});

const mappingsSearchQuerySchema = z.object({
  source: z.string().optional(),
  target: z.string().optional(),
  key: z.string().optional(),
  value: z.string().optional(),
  limit: z.string().optional(),
  offset: z.string().optional(),
});

const mappingsSearchResponseSchema = z.object({
  query: z.object({
    source: z.string(),
    target: z.string(),
    key: z.string(),
    value: z.string(),
    limit: z.number(),
    offset: z.number(),
  }),
  total: z.number(),
  items: z.array(
    z.object({
      source: z.string(),
      target: z.string(),
      key: z.string(),
      value: z.string(),
    }),
  ),
});

const healthRoute = createRoute({
  method: "get",
  path: "/health",
  responses: {
    200: {
      description: "Health check",
      content: {
        "application/json": {
          schema: z.object({ ok: z.boolean(), timestamp: z.string() }),
        },
      },
    },
  },
});

api.openapi(healthRoute, (c) =>
  c.json({ ok: true, timestamp: new Date().toISOString() }),
);

const provenanceRoute = createRoute({
  method: "get",
  path: "/provenance",
  responses: {
    200: {
      description: "Full provenance payload",
      content: { "application/json": { schema: provenanceSchema } },
    },
  },
});
api.openapi(provenanceRoute, async (c) => {
  const payload = await getProvenance();
  return c.json(payload);
});

const summaryRoute = createRoute({
  method: "get",
  path: "/provenance/summary",
  responses: {
    200: {
      description: "Summary counts",
      content: { "application/json": { schema: summarySchema } },
    },
  },
});
api.openapi(summaryRoute, async (c) => {
  const payload = await getProvenance();
  return c.json(summarizeProvenance(payload));
});

const mappingsSearchRoute = createRoute({
  method: "get",
  path: "/mappings",
  request: { query: mappingsSearchQuerySchema },
  responses: {
    200: {
      description: "Search mappings.json entries",
      content: { "application/json": { schema: mappingsSearchResponseSchema } },
    },
  },
});
api.openapi(mappingsSearchRoute, async (c) => {
  const query = c.req.query();
  const sourceQuery = normalizeText(query.source);
  const targetQuery = normalizeText(query.target);
  const keyQuery = normalizeText(query.key);
  const valueQuery = normalizeText(query.value);
  const limit = Math.max(1, Math.min(toNumber(query.limit, 50), 1000));
  const offset = Math.max(0, toNumber(query.offset, 0));

  const mappings = await getMappings();
  let total = 0;
  const items: MappingEntry[] = [];

  for (const [source, targets] of Object.entries(mappings)) {
    if (sourceQuery && !source.toLowerCase().includes(sourceQuery)) continue;
    for (const [target, entries] of Object.entries(targets)) {
      if (targetQuery && !target.toLowerCase().includes(targetQuery)) {
        continue;
      }
      for (const [key, value] of Object.entries(entries)) {
        if (keyQuery && !key.toLowerCase().includes(keyQuery)) continue;
        if (valueQuery && !value.toLowerCase().includes(valueQuery)) continue;
        if (total >= offset && items.length < limit) {
          items.push({ source, target, key, value });
        }
        total += 1;
      }
    }
  }

  return c.json({
    query: {
      source: sourceQuery,
      target: targetQuery,
      key: keyQuery,
      value: valueQuery,
      limit,
      offset,
    },
    total,
    items,
  });
});

const filtersSchema = z.object({
  source: z.string().default(""),
  target: z.string().default(""),
  actor: z.string().default(""),
  reason: z.string().default(""),
  range: z.string().default(""),
  stage: z.string().default("all"),
  present: z.enum(["all", "present", "missing"]).default("all"),
  sort: z
    .enum(["default", "present", "missing", "timeline"])
    .default("default"),
  page: z.number().default(1),
  perPage: z.number().default(50),
});

const mappingsQuerySchema = z.object({
  source: z.string().optional(),
  target: z.string().optional(),
  actor: z.string().optional(),
  reason: z.string().optional(),
  range: z.string().optional(),
  stage: z.string().optional(),
  present: z.enum(["all", "present", "missing"]).optional(),
  sort: z.enum(["default", "present", "missing", "timeline"]).optional(),
  page: z.string().optional(),
  perPage: z.string().optional(),
  maxItems: z.string().optional(),
});

const mappingsRoute = createRoute({
  method: "get",
  path: "/provenance/mappings",
  request: { query: mappingsQuerySchema },
  responses: {
    200: {
      description: "Filtered mappings",
      content: {
        "application/json": {
          schema: z.object({
            filters: filtersSchema,
            page: z.number(),
            perPage: z.number(),
            pages: z.number(),
            total: z.number(),
            items: z.array(mappingSchema.extend({ id: z.number() })),
          }),
        },
      },
    },
  },
});

api.openapi(mappingsRoute, async (c) => {
  const query = c.req.query();
  const filters = buildFilters(query);
  const payload = await getProvenance();
  const filtered = filterMappings(payload, filters);
  const paged = paginateMappings(filtered, filters);
  const items = paged.items.map(({ index, mapping }) => ({
    id: index,
    ...mapping,
  }));

  return c.json({
    filters,
    page: paged.page,
    perPage: paged.perPage,
    pages: paged.pages,
    total: paged.total,
    items,
  });
});

const mappingRoute = createRoute({
  method: "get",
  path: "/provenance/mappings/{index}",
  request: { params: z.object({ index: z.string() }) },
  responses: {
    200: {
      description: "Mapping by index",
      content: {
        "application/json": {
          schema: z.object({
            id: z.number(),
            mapping: mappingSchema,
            source: z.string(),
            target: z.string(),
          }),
        },
      },
    },
    400: {
      description: "Invalid index",
      content: { "application/json": { schema: errorSchema } },
    },
    404: {
      description: "Mapping not found",
      content: { "application/json": { schema: errorSchema } },
    },
  },
});

api.openapi(mappingRoute, async (c) => {
  const index = Number(c.req.param("index"));
  if (!Number.isFinite(index) || index < 0) {
    return c.json({ error: "Invalid mapping index." }, 400);
  }
  const payload = await getProvenance();
  const mapping = payload.mappings[index];
  if (!mapping) {
    return c.json({ error: "Mapping not found." }, 404);
  }

  return c.json(
    {
      id: index,
      mapping,
      source: payload.dict.descriptors?.[mapping.s] ?? "",
      target: payload.dict.descriptors?.[mapping.t] ?? "",
    },
    200,
  );
});
