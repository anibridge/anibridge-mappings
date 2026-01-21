import { swaggerUI } from "@hono/swagger-ui";
import { OpenAPIHono } from "@hono/zod-openapi";
import { renderer } from "./renderer";
import { App } from "./components/App";
import { api } from "./server/api";
import {
  filterMappings,
  getProvenance,
  paginateMappings,
  summarizeProvenance,
} from "./server/provenance";
import type {
  MappingFilters,
  PresenceFilter,
  SortOrder,
} from "./server/provenance";
import { getMappings } from "./server/mappings";

const app = new OpenAPIHono();

app.route("/api/v3", api);

app.doc("/openapi.json", {
  openapi: "3.0.0",
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

app.use(renderer);

app.get("/mappings.json", async (c) => {
  const payload = await getMappings();
  return c.json(payload, 200);
});
app.get("/provenance.json", async (c) => {
  const payload = await getProvenance();
  return c.json(payload, 200);
});

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

const buildQuery = (
  filters: MappingFilters,
  overrides: Partial<MappingFilters>,
) => {
  const merged = { ...filters, ...overrides };
  const params = new URLSearchParams({
    source: merged.source,
    target: merged.target,
    actor: merged.actor,
    reason: merged.reason,
    range: merged.range,
    stage: merged.stage,
    present: merged.present,
    sort: merged.sort,
    page: String(merged.page),
    perPage: String(merged.perPage),
  });
  return params.toString();
};

app.get("/", async (c) => {
  const query = c.req.query();
  const filters = buildFilters(query);
  const payload = await getProvenance();
  const filtered = filterMappings(payload, filters);
  const paged = paginateMappings(filtered, filters);
  const items = paged.items.map(({ index, mapping }) => ({
    id: index,
    ...mapping,
  }));
  const summary = summarizeProvenance(payload);
  const pageInfo = `Page ${paged.page} of ${paged.pages} • ${paged.total.toLocaleString()} total matches`;
  const matchSummary = `${paged.total.toLocaleString()} mapping(s) match your filters.`;

  const prevHref =
    paged.page > 1
      ? `/?${buildQuery(filters, { page: paged.page - 1 })}`
      : null;
  const nextHref =
    paged.page < paged.pages
      ? `/?${buildQuery(filters, { page: paged.page + 1 })}`
      : null;

  return c.render(
    <App
      dict={payload.dict}
      filters={{ ...filters, page: paged.page, perPage: paged.perPage }}
      items={items}
      pagination={{
        page: paged.page,
        pages: paged.pages,
        perPage: paged.perPage,
        total: paged.total,
        prevHref,
        nextHref,
      }}
      summary={summary}
      meta={payload.$meta ?? {}}
      pageInfo={pageInfo}
      matchSummary={matchSummary}
    />,
  );
});

export default app;
