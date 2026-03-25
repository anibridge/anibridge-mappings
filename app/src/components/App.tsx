import { diffLines } from "diff";
import type { Change } from "diff";
import { useEffect, useMemo, useState } from "hono/jsx/dom";
import type {
  Dict,
  Mapping,
  MappingFilters,
  PresenceFilter,
  ProvenancePayload,
} from "../shared/provenance";
import {
  filterMappings,
  getDictValue,
  getProvenance,
  getRange,
  paginateMappings,
  summarizeProvenance,
} from "../shared/provenance";

type MappingWithId = Mapping & { id: number };
type SortColumn = "default" | "state" | "source" | "target" | "steps";
type SortDirection = "asc" | "desc";
type MappingViewFormat = "json" | "yaml";
type DiffLine = { key: string; type: "add" | "remove" | "same"; text: string };
type TimelineSlide = {
  index: number;
  action: string;
  stage: string;
  actor: string;
  reason: string;
  range: string;
  effect: "active" | "inactive" | "skipped";
  diff: DiffLine[];
};

const DEFAULT_FILTERS: MappingFilters = {
  source: "",
  target: "",
  actor: "",
  reason: "",
  range: "",
  stage: "all",
  present: "present",
  sort: "default",
  page: 1,
  perPage: 50,
};

const EXTERNAL_SITES = {
  anidb: {
    label: "AniDB",
    buildUrl: (id: string) => `https://anidb.net/anime/${id}`,
  },
  anilist: {
    label: "AniList",
    buildUrl: (id: string) => `https://anilist.co/anime/${id}`,
  },
  mal: {
    label: "MAL",
    buildUrl: (id: string) => `https://myanimelist.net/anime/${id}`,
  },
  imdb_movie: {
    label: "IMDB",
    buildUrl: (id: string) => `https://www.imdb.com/title/${id}`,
  },
  imdb_show: {
    label: "IMDB",
    buildUrl: (id: string) => `https://www.imdb.com/title/${id}`,
  },
  tmdb_movie: {
    label: "TMDB",
    buildUrl: (id: string) => `https://www.themoviedb.org/movie/${id}`,
  },
  tmdb_show: {
    label: "TMDB",
    buildUrl: (id: string) => `https://www.themoviedb.org/tv/${id}`,
  },
  tvdb_movie: {
    label: "TVDB",
    buildUrl: (id: string) => `https://www.thetvdb.com/dereferrer/movie/${id}`,
  },
  tvdb_show: {
    label: "TVDB",
    buildUrl: (id: string) => `https://www.thetvdb.com/dereferrer/series/${id}`,
  },
} as const;

type ExternalSiteKey = keyof typeof EXTERNAL_SITES;

const descriptorToExternal = (descriptor?: string | null) => {
  if (!descriptor) return null;
  const [provider, entryId] = descriptor.split(":");
  if (!provider || !entryId) return null;
  const site = EXTERNAL_SITES[provider as ExternalSiteKey];
  if (!site) return null;
  return { label: site.label, url: site.buildUrl(entryId) };
};

const toDiffLines = (prevJson: string, nextJson: string) => {
  const parts: Change[] = diffLines(prevJson, nextJson);
  const rows: DiffLine[] = [];

  parts.forEach((part, partIndex) => {
    const type: DiffLine["type"] = part.added
      ? "add"
      : part.removed
        ? "remove"
        : "same";
    const split = part.value.split("\n");
    split.forEach((line, lineIndex) => {
      if (lineIndex === split.length - 1 && line === "") return;
      rows.push({ key: `${partIndex}-${lineIndex}`, type, text: line });
    });
  });

  return rows;
};

const buildTimelineSlides = (dict: Dict, mapping: Mapping): TimelineSlide[] => {
  const events = mapping.ev ?? [];
  if (!events.length) return [];

  const activeRanges = new Map<string, string>();
  const sourceDescriptor = getDictValue(dict, "descriptors", mapping.s) || "-";
  const targetDescriptor = getDictValue(dict, "descriptors", mapping.t) || "-";
  let previousJson = JSON.stringify(
    { [sourceDescriptor]: { [targetDescriptor]: {} } },
    null,
    2,
  );

  return events.map((event, index) => {
    const action = getDictValue(dict, "actions", event.a) || "-";
    const stage = getDictValue(dict, "stages", event.s) || "-";
    const actor = getDictValue(dict, "actors", event.ac) || "-";
    const reason = getDictValue(dict, "reasons", event.rs) || "-";
    const range = getRange(dict, event.r);
    const sourceRange = range.source_range || "-";
    const targetRange = range.target_range || "-";

    if (event.e) {
      if (action === "add") {
        activeRanges.set(sourceRange, targetRange);
      }
      if (action === "remove") {
        const currentTarget = activeRanges.get(sourceRange);
        if (currentTarget === targetRange) {
          activeRanges.delete(sourceRange);
        }
      }
    }

    const orderedRanges = Object.fromEntries(
      [...activeRanges.entries()]
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([source, target]) => [source, target]),
    );

    const currentJson = JSON.stringify(
      { [sourceDescriptor]: { [targetDescriptor]: orderedRanges } },
      null,
      2,
    );
    const diff = toDiffLines(previousJson, currentJson);
    previousJson = currentJson;

    const effect: TimelineSlide["effect"] = event.e
      ? action === "remove"
        ? "inactive"
        : "active"
      : "skipped";

    return {
      index,
      action,
      stage,
      actor,
      reason,
      range: `${sourceRange} -> ${targetRange}`,
      effect,
      diff,
    };
  });
};

const stepLabel = (step: number, total: number) => `Step ${step} / ${total}`;

const MAPPING_QUERY_PARAM = "mapping";
const MAPPING_VIEW_FORMAT_STORAGE_KEY = "anibridge:mapping-view-format";

const getSelectedIdFromUrl = (): number | null => {
  if (typeof window === "undefined") return null;
  const value = new URLSearchParams(window.location.search).get(
    MAPPING_QUERY_PARAM,
  );
  if (!value) return null;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
};

const setSelectedIdInUrl = (selectedId: number | null, replace = false) => {
  if (typeof window === "undefined") return;
  const url = new URL(window.location.href);
  if (selectedId === null) {
    url.searchParams.delete(MAPPING_QUERY_PARAM);
  } else {
    url.searchParams.set(MAPPING_QUERY_PARAM, String(selectedId));
  }

  const nextUrl = `${url.pathname}${url.search}${url.hash}`;
  if (replace) {
    window.history.replaceState(null, "", nextUrl);
    return;
  }
  window.history.pushState(null, "", nextUrl);
};

const getMappingViewFormatFromStorage = (): MappingViewFormat => {
  if (typeof window === "undefined") return "json";
  const stored = window.localStorage.getItem(MAPPING_VIEW_FORMAT_STORAGE_KEY);
  return stored === "yaml" ? "yaml" : "json";
};

const formatYamlValue = (
  value: string | Record<string, unknown>,
  depth = 0,
): string => {
  const indent = "  ".repeat(depth);
  if (typeof value === "string") {
    return JSON.stringify(value);
  }

  const entries = Object.entries(value);
  if (!entries.length) return "{}";

  return entries
    .map(([key, child]) => {
      const normalizedChild = child as string | Record<string, unknown>;
      if (
        typeof normalizedChild === "object" &&
        normalizedChild !== null &&
        Object.keys(normalizedChild).length
      ) {
        return `${indent}${JSON.stringify(key)}:\n${formatYamlValue(normalizedChild, depth + 1)}`;
      }
      if (typeof normalizedChild === "object" && normalizedChild !== null) {
        return `${indent}${JSON.stringify(key)}: {}`;
      }
      return `${indent}${JSON.stringify(key)}: ${formatYamlValue(normalizedChild, depth + 1)}`;
    })
    .join("\n");
};

const buildFinalMappingView = (dict: Dict, mapping: Mapping) => {
  const sourceDescriptor = getDictValue(dict, "descriptors", mapping.s) || "-";
  const targetDescriptor = getDictValue(dict, "descriptors", mapping.t) || "-";
  const activeRanges = new Map<string, string>();

  for (const event of mapping.ev ?? []) {
    if (!event.e) continue;
    const action = getDictValue(dict, "actions", event.a) || "";
    const range = getRange(dict, event.r);
    const sourceRange = range.source_range || "-";
    const targetRange = range.target_range || "-";

    if (action === "add") {
      activeRanges.set(sourceRange, targetRange);
    }
    if (action === "remove" && activeRanges.get(sourceRange) === targetRange) {
      activeRanges.delete(sourceRange);
    }
  }

  const orderedRanges = Object.fromEntries(
    [...activeRanges.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([source, target]) => [source, target]),
  );

  return { [sourceDescriptor]: { [targetDescriptor]: orderedRanges } };
};

export const App = () => {
  const [payload, setPayload] = useState<ProvenancePayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filters, setFilters] = useState<MappingFilters>(DEFAULT_FILTERS);
  const [sortColumn, setSortColumn] = useState<SortColumn>("default");
  const [sortDirection, setSortDirection] = useState<SortDirection>("asc");
  const [selectedId, setSelectedId] = useState<number | null>(() =>
    getSelectedIdFromUrl(),
  );
  const [timelineOpen, setTimelineOpen] = useState(false);
  const [timelineStep, setTimelineStep] = useState(0);
  const [mappingViewFormat, setMappingViewFormat] = useState<MappingViewFormat>(
    () => getMappingViewFormatFromStorage(),
  );

  useEffect(() => {
    let active = true;
    setLoading(true);
    getProvenance()
      .then((data) => {
        if (!active) return;
        setPayload(data);
        setError(null);
      })
      .catch((err) => {
        if (!active) return;
        setError(err instanceof Error ? err.message : "Failed to load data.");
      })
      .finally(() => {
        if (!active) return;
        setLoading(false);
      });

    return () => {
      active = false;
    };
  }, []);

  const summary = useMemo(
    () => (payload ? summarizeProvenance(payload) : null),
    [payload],
  );

  const filtered = useMemo(() => {
    if (!payload) return [];
    const items = filterMappings(payload, filters);
    if (sortColumn === "default") {
      return items;
    }

    const direction = sortDirection === "asc" ? 1 : -1;
    return items.slice().sort((a, b) => {
      let compare = 0;
      if (sortColumn === "state") {
        compare = Number(Boolean(a.mapping.p)) - Number(Boolean(b.mapping.p));
      }
      if (sortColumn === "source") {
        const aSource =
          getDictValue(payload.dict, "descriptors", a.mapping.s) || "";
        const bSource =
          getDictValue(payload.dict, "descriptors", b.mapping.s) || "";
        compare = aSource.localeCompare(bSource);
      }
      if (sortColumn === "target") {
        const aTarget =
          getDictValue(payload.dict, "descriptors", a.mapping.t) || "";
        const bTarget =
          getDictValue(payload.dict, "descriptors", b.mapping.t) || "";
        compare = aTarget.localeCompare(bTarget);
      }
      if (sortColumn === "steps") {
        const aSteps = a.mapping.n ?? a.mapping.ev?.length ?? 0;
        const bSteps = b.mapping.n ?? b.mapping.ev?.length ?? 0;
        compare = aSteps - bSteps;
      }

      if (compare === 0) {
        compare = a.index - b.index;
      }
      return compare * direction;
    });
  }, [payload, filters, sortColumn, sortDirection]);

  const paged = useMemo(() => {
    if (!payload) {
      return {
        page: 1,
        perPage: filters.perPage,
        pages: 1,
        total: 0,
        items: [],
      };
    }
    return paginateMappings(filtered, filters);
  }, [payload, filtered, filters]);

  const rows: MappingWithId[] = useMemo(
    () => paged.items.map(({ index, mapping }) => ({ id: index, ...mapping })),
    [paged.items],
  );

  useEffect(() => {
    if (!rows.length) {
      setSelectedId(null);
      setTimelineOpen(false);
      return;
    }

    setSelectedId((prev) => {
      if (prev === null) return rows[0]?.id ?? null;
      return rows.some((item) => item.id === prev)
        ? prev
        : (rows[0]?.id ?? null);
    });
  }, [rows]);

  useEffect(() => {
    const onPopState = () => {
      setSelectedId(getSelectedIdFromUrl());
    };

    window.addEventListener("popstate", onPopState);
    return () => {
      window.removeEventListener("popstate", onPopState);
    };
  }, []);

  useEffect(() => {
    setSelectedIdInUrl(selectedId, true);
  }, [selectedId]);

  useEffect(() => {
    setTimelineOpen(false);
  }, [selectedId]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    window.localStorage.setItem(
      MAPPING_VIEW_FORMAT_STORAGE_KEY,
      mappingViewFormat,
    );
  }, [mappingViewFormat]);

  const selected = useMemo(
    () => rows.find((item) => item.id === selectedId) ?? null,
    [rows, selectedId],
  );

  const selectedSource = selected
    ? getDictValue(payload?.dict as Dict, "descriptors", selected.s)
    : "";
  const selectedTarget = selected
    ? getDictValue(payload?.dict as Dict, "descriptors", selected.t)
    : "";
  const selectedSourceExternal = descriptorToExternal(selectedSource);
  const selectedTargetExternal = descriptorToExternal(selectedTarget);

  const finalMappingView = useMemo(() => {
    if (!payload || !selected) return "";
    const mappingObject = buildFinalMappingView(payload.dict, selected);
    if (mappingViewFormat === "yaml") {
      return formatYamlValue(mappingObject);
    }
    return JSON.stringify(mappingObject, null, 2);
  }, [payload, selected, mappingViewFormat]);

  const timelineSlides = useMemo(() => {
    if (!timelineOpen || !payload || !selected) return [] as TimelineSlide[];
    return buildTimelineSlides(payload.dict, selected);
  }, [timelineOpen, payload, selected]);

  useEffect(() => {
    if (!timelineSlides.length) {
      setTimelineStep(0);
      return;
    }
    setTimelineStep(timelineSlides.length - 1);
  }, [timelineSlides.length, timelineOpen, selectedId]);

  const timelineCurrent = timelineSlides[timelineStep] ?? null;

  const updateFilter = (key: keyof MappingFilters, value: string) => {
    setFilters((prev) => ({ ...prev, [key]: value, page: 1 }));
  };

  const updateSort = (column: SortColumn) => {
    if (sortColumn === column) {
      setSortDirection((prev) => (prev === "asc" ? "desc" : "asc"));
      return;
    }
    setSortColumn(column);
    setSortDirection(column === "steps" ? "desc" : "asc");
  };

  const sortLabel = (column: SortColumn) => {
    if (sortColumn !== column) return "";
    return sortDirection === "asc" ? " ▲" : " ▼";
  };

  if (loading) {
    return (
      <div class="flex h-screen items-center justify-center bg-slate-100 p-3 text-sm text-slate-700 dark:bg-slate-900 dark:text-slate-300">
        Loading mappings...
      </div>
    );
  }

  if (error || !payload || !summary) {
    return (
      <div class="h-screen bg-slate-100 p-3 text-sm text-rose-700 dark:bg-slate-900 dark:text-rose-300">
        {error ?? "Unable to load data."}
      </div>
    );
  }

  return (
    <div class="h-screen bg-slate-100 p-2.5 text-[13px] leading-[1.35] text-slate-800 dark:bg-slate-900 dark:text-slate-100">
      <div class="mx-auto grid h-full max-w-[1460px] grid-rows-[auto_auto_1fr] gap-2 font-['Segoe_UI',Tahoma,'Trebuchet_MS',sans-serif]">
        <header class="border border-slate-300 bg-slate-50 px-3 py-2 dark:border-slate-600 dark:bg-slate-800">
          <h1 class="m-0 mb-1 text-[15px] font-bold tracking-[0.02em]">
            AniBridge Mappings
          </h1>
          <div class="flex flex-wrap gap-3 text-slate-600 dark:text-slate-300">
            <span>Generated: {summary.generated_on ?? "unknown"}</span>
            <span>Total: {summary.mappings.toLocaleString()}</span>
            <span>Present: {summary.present_mappings.toLocaleString()}</span>
            <span>Missing: {summary.missing_mappings.toLocaleString()}</span>
          </div>
        </header>

        <section class="grid grid-cols-2 gap-1.5 border border-slate-300 bg-slate-100 p-2 md:grid-cols-2 xl:grid-cols-4 dark:border-slate-600 dark:bg-slate-800">
          <input
            value={filters.source}
            onInput={(event) =>
              updateFilter("source", (event.target as HTMLInputElement).value)
            }
            placeholder="source descriptor"
            class="w-full border border-slate-400 bg-white px-2 py-1 text-xs outline-none focus:border-sky-700 dark:border-slate-500 dark:bg-slate-800 dark:text-slate-100 dark:focus:border-sky-300"
          />
          <input
            value={filters.target}
            onInput={(event) =>
              updateFilter("target", (event.target as HTMLInputElement).value)
            }
            placeholder="target descriptor"
            class="w-full border border-slate-400 bg-white px-2 py-1 text-xs outline-none focus:border-sky-700 dark:border-slate-500 dark:bg-slate-800 dark:text-slate-100 dark:focus:border-sky-300"
          />
          <select
            value={filters.stage}
            onChange={(event) =>
              updateFilter("stage", (event.target as HTMLSelectElement).value)
            }
            class="w-full border border-slate-400 bg-white px-2 py-1 text-xs outline-none focus:border-sky-700 dark:border-slate-500 dark:bg-slate-800 dark:text-slate-100 dark:focus:border-sky-300"
          >
            <option value="all">all stages</option>
            {payload.dict.stages.map((stage) => (
              <option key={stage} value={stage}>
                {stage}
              </option>
            ))}
          </select>
          <select
            value={filters.present}
            onChange={(event) =>
              updateFilter(
                "present",
                (event.target as HTMLSelectElement).value as PresenceFilter,
              )
            }
            class="w-full border border-slate-400 bg-white px-2 py-1 text-xs outline-none focus:border-sky-700 dark:border-slate-500 dark:bg-slate-800 dark:text-slate-100 dark:focus:border-sky-300"
          >
            <option value="all">all</option>
            <option value="present">present</option>
            <option value="missing">missing</option>
          </select>
        </section>

        <section class="grid min-h-0 grid-cols-1 gap-2 xl:grid-cols-[48%_52%]">
          <aside class="flex min-h-0 flex-col border border-slate-300 bg-slate-50 dark:border-slate-600 dark:bg-slate-800">
            <div class="grid grid-cols-[88px_minmax(0,1fr)_minmax(0,1fr)_64px] border-b border-slate-300 bg-slate-200 px-2 py-1 text-[11px] uppercase tracking-[0.03em] text-slate-600 dark:border-slate-600 dark:bg-slate-700 dark:text-slate-300">
              <button
                type="button"
                class="text-left"
                onClick={() => updateSort("state")}
              >
                {`STATE${sortLabel("state")}`}
              </button>
              <button
                type="button"
                class="text-left"
                onClick={() => updateSort("source")}
              >
                {`SOURCE${sortLabel("source")}`}
              </button>
              <button
                type="button"
                class="text-left"
                onClick={() => updateSort("target")}
              >
                {`TARGET${sortLabel("target")}`}
              </button>
              <button
                type="button"
                class="text-right"
                onClick={() => updateSort("steps")}
              >
                {`STEPS${sortLabel("steps")}`}
              </button>
            </div>

            <div class="min-h-0 flex-1 overflow-auto">
              {rows.map((mapping) => {
                const source =
                  getDictValue(payload.dict, "descriptors", mapping.s) || "-";
                const target =
                  getDictValue(payload.dict, "descriptors", mapping.t) || "-";
                const steps = mapping.n ?? mapping.ev?.length ?? 0;
                const active = selectedId === mapping.id;

                return (
                  <button
                    type="button"
                    key={mapping.id}
                    class={`grid w-full grid-cols-[88px_minmax(0,1fr)_minmax(0,1fr)_64px] border-b border-slate-200 px-2 py-1 text-left text-[12px] ${
                      active
                        ? "bg-sky-100 dark:bg-sky-800/35"
                        : "bg-white hover:bg-slate-100 dark:bg-slate-800 dark:hover:bg-slate-700"
                    }`}
                    onClick={() => {
                      setSelectedIdInUrl(mapping.id);
                      setSelectedId(mapping.id);
                    }}
                  >
                    <span class="truncate">
                      {mapping.p ? "present" : "missing"}
                    </span>
                    <span class="truncate">{source}</span>
                    <span class="truncate">{target}</span>
                    <span class="text-right">{steps}</span>
                  </button>
                );
              })}
            </div>

            <div class="flex items-center justify-between gap-2 border-t border-slate-300 bg-slate-100 px-2 py-1.5 text-xs dark:border-slate-600 dark:bg-slate-700 dark:text-slate-200">
              <button
                type="button"
                disabled={paged.page <= 1}
                class="border border-slate-400 bg-white px-2 py-1 disabled:opacity-50 dark:border-slate-500 dark:bg-slate-800"
                onClick={() =>
                  setFilters((prev) => ({
                    ...prev,
                    page: Math.max(1, prev.page - 1),
                  }))
                }
              >
                Prev
              </button>
              <span>
                Page {paged.page} / {paged.pages}
              </span>
              <button
                type="button"
                disabled={paged.page >= paged.pages}
                class="border border-slate-400 bg-white px-2 py-1 disabled:opacity-50 dark:border-slate-500 dark:bg-slate-800"
                onClick={() =>
                  setFilters((prev) => ({
                    ...prev,
                    page: Math.min(paged.pages, prev.page + 1),
                  }))
                }
              >
                Next
              </button>
            </div>
          </aside>

          <main class="min-h-0 overflow-auto border border-slate-300 bg-slate-50 p-2 dark:border-slate-600 dark:bg-slate-800">
            {selected ? (
              <>
                <div class="grid gap-2 border border-slate-300 bg-slate-100 p-2 md:grid-cols-3 dark:border-slate-600 dark:bg-slate-700">
                  <div>
                    <div class="text-[11px] uppercase text-slate-600 dark:text-slate-300">
                      Source
                    </div>
                    {selectedSourceExternal ? (
                      <a
                        class="mt-0.5 inline-flex items-center gap-1 break-words font-mono text-xs text-sky-700 hover:underline dark:text-sky-300"
                        href={selectedSourceExternal.url}
                        target="_blank"
                        rel="noopener noreferrer"
                      >
                        {selectedSource || "-"}
                        <svg
                          aria-hidden="true"
                          viewBox="0 0 24 24"
                          class="h-3 w-3 shrink-0"
                          fill="none"
                          stroke="currentColor"
                          stroke-width="2"
                          stroke-linecap="round"
                          stroke-linejoin="round"
                        >
                          <path d="M7 17 17 7" />
                          <path d="M9 7h8v8" />
                        </svg>
                      </a>
                    ) : (
                      <div class="mt-0.5 break-words font-mono text-xs">
                        {selectedSource || "-"}
                      </div>
                    )}
                  </div>
                  <div>
                    <div class="text-[11px] uppercase text-slate-600 dark:text-slate-300">
                      Target
                    </div>
                    {selectedTargetExternal ? (
                      <a
                        class="mt-0.5 inline-flex items-center gap-1 break-words font-mono text-xs text-sky-700 hover:underline dark:text-sky-300"
                        href={selectedTargetExternal.url}
                        target="_blank"
                        rel="noopener noreferrer"
                      >
                        {selectedTarget || "-"}
                        <svg
                          aria-hidden="true"
                          viewBox="0 0 24 24"
                          class="h-3 w-3 shrink-0"
                          fill="none"
                          stroke="currentColor"
                          stroke-width="2"
                          stroke-linecap="round"
                          stroke-linejoin="round"
                        >
                          <path d="M7 17 17 7" />
                          <path d="M9 7h8v8" />
                        </svg>
                      </a>
                    ) : (
                      <div class="mt-0.5 break-words font-mono text-xs">
                        {selectedTarget || "-"}
                      </div>
                    )}
                  </div>
                  <div>
                    <div class="text-[11px] uppercase text-slate-600 dark:text-slate-300">
                      State
                    </div>
                    <div class="mt-0.5 font-mono text-xs">
                      {selected.p ? "present" : "missing"}
                    </div>
                  </div>
                </div>

                <section class="mt-2 border border-slate-300 bg-white dark:border-slate-600 dark:bg-slate-800">
                  <div class="border-b border-slate-300 bg-slate-100 px-2 py-1.5 dark:border-slate-600 dark:bg-slate-700">
                    <div class="mb-1 flex items-center justify-between gap-2">
                      <div class="text-xs font-semibold text-slate-700 dark:text-slate-100">
                        Mapping
                      </div>
                      <div class="inline-flex items-center gap-1 text-xs">
                        <button
                          type="button"
                          class={`border px-2 py-0.5 ${
                            mappingViewFormat === "json"
                              ? "border-sky-700 bg-sky-50 text-sky-800 dark:border-sky-300 dark:bg-sky-950/30 dark:text-sky-200"
                              : "border-slate-400 bg-white text-slate-700 dark:border-slate-500 dark:bg-slate-800 dark:text-slate-300"
                          }`}
                          onClick={() => setMappingViewFormat("json")}
                        >
                          JSON
                        </button>
                        <button
                          type="button"
                          class={`border px-2 py-0.5 ${
                            mappingViewFormat === "yaml"
                              ? "border-sky-700 bg-sky-50 text-sky-800 dark:border-sky-300 dark:bg-sky-950/30 dark:text-sky-200"
                              : "border-slate-400 bg-white text-slate-700 dark:border-slate-500 dark:bg-slate-800 dark:text-slate-300"
                          }`}
                          onClick={() => setMappingViewFormat("yaml")}
                        >
                          YAML
                        </button>
                      </div>
                    </div>
                    <pre class="max-h-[260px] overflow-auto border border-slate-300 bg-white p-2 font-mono text-xs dark:border-slate-600 dark:bg-slate-800">
                      {finalMappingView}
                    </pre>
                  </div>
                </section>

                <details
                  open={timelineOpen}
                  onToggle={(event: MouseEvent) =>
                    setTimelineOpen(
                      (event.currentTarget as HTMLDetailsElement).open,
                    )
                  }
                  class="mt-2 border border-slate-300 bg-white dark:border-slate-600 dark:bg-slate-800"
                >
                  <summary class="cursor-pointer border-b border-slate-300 bg-slate-100 px-2 py-1.5 text-xs font-semibold dark:border-slate-600 dark:bg-slate-700 dark:text-slate-100">
                    Timeline ({selected.n ?? selected.ev?.length ?? 0} steps)
                  </summary>
                  {!timelineOpen ? (
                    <p class="m-0 px-2 py-2 text-xs text-slate-600 dark:text-slate-300">
                      Expand to load timeline.
                    </p>
                  ) : null}

                  {timelineOpen && timelineSlides.length ? (
                    <>
                      <div class="flex items-center justify-between gap-2 border-b border-slate-300 bg-slate-100 px-2 py-1.5 text-xs dark:border-slate-600 dark:bg-slate-700 dark:text-slate-100">
                        <button
                          type="button"
                          disabled={timelineStep <= 0}
                          class="border border-slate-400 bg-white px-2 py-1 disabled:opacity-50 dark:border-slate-500 dark:bg-slate-800"
                          onClick={() =>
                            setTimelineStep((prev) => Math.max(0, prev - 1))
                          }
                        >
                          Older
                        </button>
                        <span>
                          {stepLabel(timelineStep + 1, timelineSlides.length)}
                        </span>
                        <button
                          type="button"
                          disabled={timelineStep >= timelineSlides.length - 1}
                          class="border border-slate-400 bg-white px-2 py-1 disabled:opacity-50 dark:border-slate-500 dark:bg-slate-800"
                          onClick={() =>
                            setTimelineStep((prev) =>
                              Math.min(timelineSlides.length - 1, prev + 1),
                            )
                          }
                        >
                          Newer
                        </button>
                      </div>

                      {timelineCurrent ? (
                        <div class="p-2">
                          <div class="mb-2 grid gap-1 text-xs text-slate-600 md:grid-cols-2 dark:text-slate-300">
                            <span>Action: {timelineCurrent.action}</span>
                            <span>Stage: {timelineCurrent.stage}</span>
                            <span>Actor: {timelineCurrent.actor}</span>
                            <span>Reason: {timelineCurrent.reason}</span>
                            <span>Range: {timelineCurrent.range}</span>
                            <span>Effect: {timelineCurrent.effect}</span>
                          </div>

                          <pre class="max-h-[360px] overflow-auto border border-slate-300 bg-white p-2 font-mono text-xs dark:border-slate-600 dark:bg-slate-800">
                            {timelineCurrent.diff.map((line) => (
                              <div
                                key={line.key}
                                class={
                                  line.type === "add"
                                    ? "bg-emerald-50 text-emerald-800 dark:bg-emerald-900/20 dark:text-emerald-200"
                                    : line.type === "remove"
                                      ? "bg-rose-50 text-rose-800 dark:bg-rose-900/20 dark:text-rose-200"
                                      : "text-slate-700 dark:text-slate-300"
                                }
                              >
                                {line.type === "add"
                                  ? "+ "
                                  : line.type === "remove"
                                    ? "- "
                                    : "  "}
                                {line.text || " "}
                              </div>
                            ))}
                          </pre>
                        </div>
                      ) : null}
                    </>
                  ) : null}
                </details>
              </>
            ) : (
              <div class="p-2 text-sm text-slate-600 dark:text-slate-300">
                No mapping selected.
              </div>
            )}
          </main>
        </section>
      </div>
    </div>
  );
};
