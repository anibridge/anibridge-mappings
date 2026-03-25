import { useEffect, useMemo, useState } from "hono/jsx/dom";
import type { MappingFilters, ProvenancePayload } from "../utils/provenance";
import {
  Dict,
  filterMappings,
  getDictValue,
  getProvenance,
  paginateMappings,
  summarizeProvenance,
} from "../utils/provenance";
import { FiltersBar } from "./filters-bar";
import { MappingDetails } from "./mapping-details";
import { MappingsTable } from "./mappings-table";
import type {
  MappingViewFormat,
  MappingWithId,
  SortColumn,
  SortDirection,
} from "./ui-types";
import {
  buildFinalMappingObject,
  buildTimelineSlides,
  descriptorToExternal,
  formatMappingView,
} from "../utils/mapping-presentation";
import {
  buildDescriptorMappingKey,
  getSelectedMappingKeyFromUrl,
  setSelectedMappingKeyInUrl,
} from "../utils/url-state";

const MAPPING_VIEW_FORMAT_STORAGE_KEY = "anibridge:mapping-view-format";

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

const getMappingViewFormatFromStorage = (): MappingViewFormat => {
  if (typeof window === "undefined") return "json";
  const stored = window.localStorage.getItem(MAPPING_VIEW_FORMAT_STORAGE_KEY);
  return stored === "yaml" ? "yaml" : "json";
};

const mappingDescriptorKey = (
  dict: Dict,
  mapping: { s: number; t: number },
) => {
  const sourceDescriptor = getDictValue(dict, "descriptors", mapping.s) || "";
  const targetDescriptor = getDictValue(dict, "descriptors", mapping.t) || "";
  return buildDescriptorMappingKey(sourceDescriptor, targetDescriptor);
};

export const App = () => {
  const [payload, setPayload] = useState<ProvenancePayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filters, setFilters] = useState<MappingFilters>(DEFAULT_FILTERS);
  const [sortColumn, setSortColumn] = useState<SortColumn>("default");
  const [sortDirection, setSortDirection] = useState<SortDirection>("asc");
  const [selectedKey, setSelectedKey] = useState<string | null>(() =>
    getSelectedMappingKeyFromUrl(),
  );
  const [timelineOpen, setTimelineOpen] = useState(false);
  const [timelineStep, setTimelineStep] = useState(0);
  const [mappingViewFormat, setMappingViewFormat] = useState<MappingViewFormat>(
    getMappingViewFormatFromStorage,
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
    if (sortColumn === "default") return items;

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
      if (compare === 0) compare = a.index - b.index;
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
    () =>
      paged.items.map(({ index, mapping }) => ({
        id: index,
        key: payload ? mappingDescriptorKey(payload.dict, mapping) : "",
        ...mapping,
      })),
    [paged.items, payload],
  );

  useEffect(() => {
    if (!payload) {
      return;
    }

    if (!filtered.length) {
      setSelectedKey(null);
      setTimelineOpen(false);
      return;
    }

    setSelectedKey((prev) => {
      const firstKey = mappingDescriptorKey(payload.dict, filtered[0].mapping);
      if (prev === null) return firstKey;
      return filtered.some(
        ({ mapping }) => mappingDescriptorKey(payload.dict, mapping) === prev,
      )
        ? prev
        : firstKey;
    });
  }, [payload, filtered]);

  useEffect(() => {
    const onPopState = () => setSelectedKey(getSelectedMappingKeyFromUrl());
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  useEffect(() => {
    setSelectedMappingKeyInUrl(selectedKey, true);
  }, [selectedKey]);

  useEffect(() => {
    setTimelineOpen(false);
  }, [selectedKey]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    window.localStorage.setItem(
      MAPPING_VIEW_FORMAT_STORAGE_KEY,
      mappingViewFormat,
    );
  }, [mappingViewFormat]);

  const selectedEntry = useMemo(
    () =>
      payload
        ? (filtered.find(
            ({ mapping }) =>
              mappingDescriptorKey(payload.dict, mapping) === selectedKey,
          ) ?? null)
        : null,
    [payload, filtered, selectedKey],
  );

  const selected = selectedEntry?.mapping ?? null;

  const selectedSource =
    payload && selected
      ? getDictValue(payload.dict, "descriptors", selected.s)
      : "";
  const selectedTarget =
    payload && selected
      ? getDictValue(payload.dict, "descriptors", selected.t)
      : "";

  const selectedSourceExternal = descriptorToExternal(selectedSource);
  const selectedTargetExternal = descriptorToExternal(selectedTarget);

  const finalMappingView = useMemo(() => {
    if (!payload || !selected) return "";
    const mappingObject = buildFinalMappingObject(payload.dict, selected);
    return formatMappingView(mappingObject, mappingViewFormat);
  }, [payload, selected, mappingViewFormat]);

  const timelineSlides = useMemo(() => {
    if (!timelineOpen || !payload || !selected) return [];
    return buildTimelineSlides(payload.dict, selected);
  }, [timelineOpen, payload, selected]);

  useEffect(() => {
    if (!timelineSlides.length) {
      setTimelineStep(0);
      return;
    }
    setTimelineStep(timelineSlides.length - 1);
  }, [timelineSlides.length, timelineOpen, selectedKey]);

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

        <FiltersBar
          payload={payload}
          filters={filters}
          onFilterChange={updateFilter}
        />

        <section class="grid min-h-0 grid-cols-1 gap-2 xl:grid-cols-[48%_52%]">
          <MappingsTable
            dict={payload.dict}
            rows={rows}
            selectedKey={selectedKey}
            sortLabel={sortLabel}
            onSort={updateSort}
            onSelect={(mappingKey) => {
              setSelectedMappingKeyInUrl(mappingKey);
              setSelectedKey(mappingKey);
            }}
            paged={{ page: paged.page, pages: paged.pages }}
            setFilters={(updater) => setFilters(updater)}
          />

          {selected ? (
            <MappingDetails
              dict={payload.dict}
              selected={selected}
              selectedSource={selectedSource}
              selectedTarget={selectedTarget}
              selectedSourceExternal={selectedSourceExternal}
              selectedTargetExternal={selectedTargetExternal}
              mappingViewFormat={mappingViewFormat}
              onMappingViewFormatChange={setMappingViewFormat}
              finalMappingView={finalMappingView}
              timelineOpen={timelineOpen}
              onTimelineToggle={setTimelineOpen}
              timelineSlides={timelineSlides}
              timelineStep={timelineStep}
              setTimelineStep={setTimelineStep}
            />
          ) : (
            <main class="min-h-0 overflow-auto border border-slate-300 bg-slate-50 p-2 text-sm text-slate-600 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-300">
              No mapping selected.
            </main>
          )}
        </section>
      </div>
    </div>
  );
};
