import { diffLines } from "diff";
import type { Change } from "diff";
import type {
  Dict,
  Mapping,
  MappingEvent,
  MappingFilters,
} from "../server/provenance";
import { getDictValue, getRange } from "../server/provenance";

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
type MappingWithId = Mapping & { id: number };

type Pagination = {
  page: number;
  pages: number;
  perPage: number;
  total: number;
  prevHref: string | null;
  nextHref: string | null;
};

type Summary = {
  generated_on: string | null;
  mappings: number;
  present_mappings: number;
  missing_mappings: number;
};

type AppProps = {
  dict: Dict;
  filters: MappingFilters;
  items: MappingWithId[];
  pagination: Pagination;
  summary: Summary;
  meta: Record<string, unknown>;
  pageInfo: string;
  matchSummary: string;
};

const descriptorToExternal = (descriptor?: string | null) => {
  if (!descriptor) return null;
  const [provider, entryId] = descriptor.split(":");
  if (!provider || !entryId) return null;
  const site = EXTERNAL_SITES[provider as ExternalSiteKey];
  if (!site) return null;
  return { label: site.label, url: site.buildUrl(entryId) };
};

const ExternalLinkBadge = ({
  descriptor,
  roleLabel,
}: {
  descriptor?: string | null;
  roleLabel: string;
}) => {
  const external = descriptorToExternal(descriptor);
  if (!external) return null;
  return (
    <a
      class="inline-flex items-center gap-1.5 rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 text-[11px] font-semibold uppercase tracking-wide text-slate-600 shadow-sm transition hover:border-slate-300 hover:text-slate-900 dark:border-slate-700 dark:bg-slate-900/60 dark:text-slate-300 dark:hover:text-white"
      href={external.url}
      target="_blank"
      rel="noopener noreferrer"
      title={`${external.label} ${roleLabel}`}
      aria-label={`Open ${roleLabel} on ${external.label}`}
    >
      <svg aria-hidden="true" viewBox="0 0 20 20" class="h-3.5 w-3.5">
        <path
          fill="currentColor"
          d="M11 3h6v6h-2V6.41l-7.29 7.3-1.42-1.42 7.3-7.29H11V3z"
        />
        <path fill="currentColor" d="M5 5h4v2H7v6h6v-2h2v4H5V5z" />
      </svg>
      <span>{external.label}</span>
    </a>
  );
};

const renderRange = (dict: Dict, event: MappingEvent) => {
  const range = getRange(dict, event.r);
  const source = range.source_range || "-";
  const target = range.target_range || "-";
  return `${source} → ${target}`;
};

type DiffLine = { key: string; type: "add" | "remove" | "same"; text: string };

const renderDiffLines = (prevJson: string, nextJson: string) => {
  const parts: Change[] = diffLines(prevJson, nextJson);
  const lines: DiffLine[] = [];
  parts.forEach((part: Change, partIndex: number) => {
    const type: DiffLine["type"] = part.added
      ? "add"
      : part.removed
        ? "remove"
        : "same";
    const split = part.value.split("\n");
    split.forEach((line: string, lineIndex: number) => {
      if (lineIndex === split.length - 1 && line === "") return;
      lines.push({ key: `${partIndex}-${lineIndex}`, type, text: line });
    });
  });
  return lines;
};

const buildSnapshot = (
  dict: Dict,
  event: MappingEvent,
  activeRanges: Map<string, Set<string>>,
) => {
  const action = getDictValue(dict, "actions", event.a);
  const range = getRange(dict, event.r);
  const sourceRange = range.source_range || "-";
  const targetRange = range.target_range || "-";
  if (event.e) {
    if (action === "add") {
      const set = activeRanges.get(sourceRange) ?? new Set<string>();
      set.add(targetRange);
      activeRanges.set(sourceRange, set);
    }
    if (action === "remove") {
      const set = activeRanges.get(sourceRange);
      if (set) {
        set.delete(targetRange);
        if (!set.size) activeRanges.delete(sourceRange);
      }
    }
  }
  const orderedRanges = Object.fromEntries(
    [...activeRanges.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([key, values]) => [
        key,
        [...values].sort((a, b) => a.localeCompare(b)).join(", "),
      ]),
  );
  const effect = event.e
    ? action === "remove"
      ? "inactive"
      : "active"
    : "skipped";
  return {
    action: action || "-",
    stage: getDictValue(dict, "stages", event.s) || "-",
    actor: getDictValue(dict, "actors", event.ac) || "-",
    reason: getDictValue(dict, "reasons", event.rs) || "-",
    range: `${sourceRange} → ${targetRange}`,
    effect,
    orderedRanges,
  };
};

const MappingTimelineCarousel = ({
  dict,
  mapping,
  mappingId,
}: {
  dict: Dict;
  mapping: Mapping;
  mappingId: number;
}) => {
  const events = mapping.ev ?? [];
  if (!events.length) {
    return (
      <div class="rounded-xl border border-slate-200/70 bg-white/70 px-3 py-2 text-xs text-slate-500 dark:border-slate-800 dark:bg-slate-950/40 dark:text-slate-400">
        No timeline events recorded for this mapping.
      </div>
    );
  }

  const activeRanges = new Map<string, Set<string>>();
  const sourceDescriptor = getDictValue(dict, "descriptors", mapping.s) || "-";
  const targetDescriptor = getDictValue(dict, "descriptors", mapping.t) || "-";
  let previousJson = JSON.stringify(
    { [sourceDescriptor]: { [targetDescriptor]: {} } },
    null,
    2,
  );
  const slides = events.map((event, index) => {
    const snapshot = buildSnapshot(dict, event, activeRanges);
    const snapshotJson = JSON.stringify(
      { [sourceDescriptor]: { [targetDescriptor]: snapshot.orderedRanges } },
      null,
      2,
    );
    const diff = renderDiffLines(previousJson, snapshotJson);
    previousJson = snapshotJson;
    return { index, stepNumber: index + 1, snapshot, diff };
  });
  const displaySlides = [...slides].reverse();

  return (
    <details
      class="rounded-2xl border border-slate-200/70 bg-white/70 px-3 py-3 text-xs text-slate-600 shadow-sm dark:border-slate-800 dark:bg-slate-950/40 dark:text-slate-300"
      data-carousel
      data-carousel-id={`mapping-${mappingId}`}
    >
      <summary class="flex cursor-pointer list-none flex-wrap items-center justify-between gap-2 text-[11px] font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400">
        <span>Timeline</span>
        <span>{mapping.n ?? events.length} steps</span>
      </summary>
      <div class="mt-3 flex items-center justify-between gap-2">
        <button
          type="button"
          data-carousel-prev
          class="rounded-lg border border-slate-200 px-2 py-1 text-xs font-semibold text-slate-700 hover:border-slate-300 dark:border-slate-700 dark:text-slate-200"
        >
          Prev
        </button>
        <div class="text-[11px] uppercase tracking-wide text-slate-500 dark:text-slate-400">
          Use buttons or swipe
        </div>
        <button
          type="button"
          data-carousel-next
          class="rounded-lg border border-slate-200 px-2 py-1 text-xs font-semibold text-slate-700 hover:border-slate-300 dark:border-slate-700 dark:text-slate-200"
        >
          Next
        </button>
      </div>
      <div class="mt-3 h-80 overflow-hidden" data-carousel-viewport>
        <div
          class="flex h-full gap-4 overflow-x-auto overflow-y-hidden scroll-smooth pb-4 snap-x snap-mandatory"
          data-carousel-track
        ></div>
      </div>
      <template data-carousel-template>
        {displaySlides.map((slide, displayIndex) => {
          const { index, snapshot, diff, stepNumber } = slide;
          return (
            <section
              id={`mapping-${mappingId}-slide-${index}`}
              key={`mapping-${mappingId}-slide-${index}`}
              data-carousel-slide={String(index)}
              class="flex h-full w-[88%] min-w-[260px] flex-shrink-0 snap-center flex-col gap-3 overflow-y-auto rounded-xl border border-slate-200 bg-white p-3 shadow-sm dark:border-slate-800 dark:bg-slate-900/70 sm:w-[520px]"
            >
              <div class="flex items-center justify-between gap-2 text-[11px] uppercase tracking-wide text-slate-500 dark:text-slate-400">
                <span>
                  Step {stepNumber} of {slides.length}
                </span>
                <span>{snapshot.action}</span>
              </div>
              <div class="grid gap-2 text-xs text-slate-600 dark:text-slate-300">
                <div>
                  <p>Stage: {snapshot.stage}</p>
                  <p>Actor: {snapshot.actor}</p>
                  <p>Reason: {snapshot.reason}</p>
                </div>
                <div class="rounded-lg border border-slate-200/70 bg-slate-50/70 px-2 py-1 font-mono text-[11px] text-slate-700 dark:border-slate-800 dark:bg-slate-950/40 dark:text-slate-200">
                  Range: {snapshot.range} • {snapshot.effect}
                </div>
              </div>
              <div class="rounded-lg border border-slate-200/70 bg-slate-50/70 px-2 py-2 font-mono text-[11px] text-slate-800 dark:border-slate-800 dark:bg-slate-950/40 dark:text-slate-100">
                <div class="mb-1 text-[10px] uppercase tracking-wide text-slate-500 dark:text-slate-400">
                  Active ranges diff
                </div>
                <pre class="m-0 space-y-0">
                  {diff.map((line) => (
                    <div
                      key={line.key}
                      class={
                        line.type === "add"
                          ? "bg-emerald-500/10 text-emerald-700 dark:text-emerald-200"
                          : line.type === "remove"
                            ? "bg-rose-500/10 text-rose-700 dark:text-rose-200"
                            : "text-slate-700 dark:text-slate-200"
                      }
                    >
                      <span class="select-none pr-2 opacity-60">
                        {line.type === "add"
                          ? "+"
                          : line.type === "remove"
                            ? "-"
                            : " "}
                      </span>
                      <span class="whitespace-pre-wrap">
                        {line.text || " "}
                      </span>
                    </div>
                  ))}
                </pre>
              </div>
            </section>
          );
        })}
      </template>
    </details>
  );
};

const MappingCard = ({
  dict,
  mapping,
  mappingId,
}: {
  dict: Dict;
  mapping: Mapping;
  mappingId: number;
}) => {
  const source = getDictValue(dict, "descriptors", mapping.s);
  const target = getDictValue(dict, "descriptors", mapping.t);
  const isPresent = Boolean(mapping.p);
  const stageSet = new Set(
    (mapping.ev || []).map((event) => getDictValue(dict, "stages", event.s)),
  );

  const latestRanges = new Set<number>();
  (mapping.ev || []).forEach((event) => {
    if (!event.e) return;
    const action = getDictValue(dict, "actions", event.a);
    if (action === "add") latestRanges.add(event.r);
    if (action === "remove") latestRanges.delete(event.r);
  });

  return (
    <article class="min-w-0 space-y-4 rounded-2xl border border-slate-200/80 bg-white p-4 shadow-sm transition hover:border-slate-300 hover:shadow-md dark:border-slate-800 dark:bg-slate-900/70">
      <header class="flex flex-wrap items-start justify-between gap-3">
        <div class="min-w-0 w-full">
          <div class="flex w-full flex-wrap items-center justify-between gap-3">
            <div class="flex flex-wrap items-center gap-2">
              <span
                class={
                  "inline-flex items-center rounded-full border px-2 py-0.5 text-[11px] font-semibold uppercase tracking-wide " +
                  (isPresent
                    ? "border-emerald-300 bg-emerald-50 text-emerald-700 dark:border-emerald-500/60 dark:bg-emerald-500/10 dark:text-emerald-300"
                    : "border-rose-300 bg-rose-50 text-rose-700 dark:border-rose-500/60 dark:bg-rose-500/10 dark:text-rose-300")
                }
              >
                {isPresent ? "present" : "missing"}
              </span>
              {[...stageSet].map((stage) =>
                stage ? (
                  <span
                    key={stage}
                    class="inline-flex items-center rounded-full border border-slate-200 px-2 py-0.5 text-[11px] font-semibold uppercase tracking-wide text-slate-600 dark:border-slate-700 dark:text-slate-300"
                  >
                    {stage}
                  </span>
                ) : null,
              )}
            </div>
            <div class="flex items-center gap-2">
              <ExternalLinkBadge descriptor={source} roleLabel="source" />
              <ExternalLinkBadge descriptor={target} roleLabel="target" />
            </div>
          </div>
        </div>
      </header>
      <div class="grid gap-3 rounded-xl border border-slate-200/70 bg-slate-50/70 p-3 text-sm dark:border-slate-800 dark:bg-slate-900/60 sm:grid-cols-2">
        <div>
          <div class="text-[11px] font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400">
            Source
          </div>
          <div class="mt-2 break-words rounded-lg border border-slate-200/70 bg-white px-3 py-2 font-mono text-sm leading-relaxed text-slate-900 shadow-sm dark:border-slate-700/60 dark:bg-slate-950/60 dark:text-slate-100">
            {source || "-"}
          </div>
        </div>
        <div>
          <div class="text-[11px] font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400">
            Target
          </div>
          <div class="mt-2 break-words rounded-lg border border-slate-200/70 bg-white px-3 py-2 font-mono text-sm leading-relaxed text-slate-900 shadow-sm dark:border-slate-700/60 dark:bg-slate-950/60 dark:text-slate-100">
            {target || "-"}
          </div>
        </div>
      </div>
      <MappingTimelineCarousel
        dict={dict}
        mapping={mapping}
        mappingId={mappingId}
      />
    </article>
  );
};

export const App = ({
  dict,
  filters,
  items,
  pagination,
  summary,
  meta,
  pageInfo,
  matchSummary,
}: AppProps) => (
  <>
    <header class="mx-auto flex w-full max-w-6xl flex-col gap-4 px-6 pb-2 pt-8 sm:flex-row sm:items-start sm:justify-between">
      <div>
        <h1 class="text-2xl font-semibold">AniBridge Mappings</h1>
        <p class="text-sm text-slate-500 dark:text-slate-400">
          Explore AniBridge mappings with provenance tracking.
        </p>
      </div>
      <div class="text-sm text-slate-500 dark:text-slate-400">
        <div>
          Generated:{" "}
          {summary.generated_on
            ? new Date(summary.generated_on).toLocaleString()
            : "unknown"}
        </div>
        <div>
          {summary.mappings.toLocaleString()} mappings •{" "}
          {summary.present_mappings.toLocaleString()} present •{" "}
          {summary.missing_mappings.toLocaleString()} missing
        </div>
        {meta?.source ? <div>Source: {String(meta.source)}</div> : null}
      </div>
    </header>

    <section className="sticky top-0 z-10 border-y border-slate-200 bg-white/95 backdrop-blur dark:border-slate-800 dark:bg-slate-950/90">
      <div className="mx-auto w-full max-w-6xl px-6 py-4">
        <form className="flex w-full flex-col gap-3" method="get">
          <div class="grid w-full gap-3 sm:grid-cols-2 lg:grid-cols-6">
            <label
              class="flex flex-col gap-1 text-xs font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400"
              htmlFor="query-source"
            >
              Source descriptor
              <input
                id="query-source"
                name="source"
                type="search"
                placeholder="provider:id[:scope]"
                autoComplete="off"
                value={filters.source}
                class="rounded-lg border border-slate-200 bg-transparent px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none dark:border-slate-800 dark:text-slate-100"
              />
            </label>
            <label
              class="flex flex-col gap-1 text-xs font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400"
              htmlFor="query-target"
            >
              Target descriptor
              <input
                id="query-target"
                name="target"
                type="search"
                placeholder="provider:id[:scope]"
                autoComplete="off"
                value={filters.target}
                class="rounded-lg border border-slate-200 bg-transparent px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none dark:border-slate-800 dark:text-slate-100"
              />
            </label>
            <label
              class="flex flex-col gap-1 text-xs font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400"
              htmlFor="query-range"
            >
              Range search
              <input
                id="query-range"
                name="range"
                type="search"
                placeholder="1-12, 5|2"
                autoComplete="off"
                value={filters.range}
                class="rounded-lg border border-slate-200 bg-transparent px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none dark:border-slate-800 dark:text-slate-100"
              />
            </label>
            <label
              class="flex flex-col gap-1 text-xs font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400"
              htmlFor="query-actor"
            >
              Actor
              <select
                id="query-actor"
                name="actor"
                class="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none dark:border-slate-800 dark:bg-slate-900 dark:text-slate-100"
              >
                <option value="">All</option>
                {dict.actors.map((actor) => (
                  <option
                    value={actor}
                    selected={filters.actor === actor}
                    key={`actor-${actor}`}
                  >
                    {actor}
                  </option>
                ))}
              </select>
            </label>
            <label
              class="flex flex-col gap-1 text-xs font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400"
              htmlFor="query-reason"
            >
              Reason
              <select
                id="query-reason"
                name="reason"
                class="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none dark:border-slate-800 dark:bg-slate-900 dark:text-slate-100"
              >
                <option value="">All</option>
                {dict.reasons.map((reason) => (
                  <option
                    value={reason}
                    selected={filters.reason === reason}
                    key={`reason-${reason}`}
                  >
                    {reason}
                  </option>
                ))}
              </select>
            </label>
            <label
              class="flex flex-col gap-1 text-xs font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400"
              htmlFor="stage"
            >
              Stage
              <select
                id="stage"
                name="stage"
                class="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none dark:border-slate-800 dark:bg-slate-900 dark:text-slate-100"
              >
                <option value="all">All</option>
                {dict.stages.map((stage) => (
                  <option
                    value={stage}
                    selected={filters.stage === stage}
                    key={`stage-${stage}`}
                  >
                    {stage}
                  </option>
                ))}
              </select>
            </label>
            <label
              class="flex flex-col gap-1 text-xs font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400"
              htmlFor="present"
            >
              Presence
              <select
                id="present"
                name="present"
                class="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none dark:border-slate-800 dark:bg-slate-900 dark:text-slate-100"
              >
                <option value="all" selected={filters.present === "all"}>
                  All
                </option>
                <option
                  value="present"
                  selected={filters.present === "present"}
                >
                  Present only
                </option>
                <option
                  value="missing"
                  selected={filters.present === "missing"}
                >
                  Missing only
                </option>
              </select>
            </label>
            <label
              class="flex flex-col gap-1 text-xs font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400"
              htmlFor="sort"
            >
              Sort
              <select
                id="sort"
                name="sort"
                class="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none dark:border-slate-800 dark:bg-slate-900 dark:text-slate-100"
              >
                <option value="default" selected={filters.sort === "default"}>
                  Default
                </option>
                <option value="present" selected={filters.sort === "present"}>
                  Present first
                </option>
                <option value="missing" selected={filters.sort === "missing"}>
                  Missing first
                </option>
                <option value="timeline" selected={filters.sort === "timeline"}>
                  Most timeline steps
                </option>
              </select>
            </label>
            <label
              class="flex flex-col gap-1 text-xs font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400"
              htmlFor="per-page"
            >
              Max items
              <select
                id="per-page"
                name="perPage"
                class="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none dark:border-slate-800 dark:bg-slate-900 dark:text-slate-100"
              >
                {[50, 100, 250, 500, 1000].map((value) => (
                  <option
                    value={String(value)}
                    selected={filters.perPage === value}
                    key={`per-page-${value}`}
                  >
                    {value}
                  </option>
                ))}
              </select>
            </label>
            <div class="flex items-end gap-2 lg:col-span-2">
              <button
                class="inline-flex items-center justify-center rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-slate-800 dark:bg-slate-100 dark:text-slate-900"
                type="submit"
              >
                Apply filters
              </button>
              <a
                href="/"
                class="inline-flex items-center justify-center rounded-lg border border-slate-200 px-4 py-2 text-sm font-semibold text-slate-700 shadow-sm hover:border-slate-300 dark:border-slate-700 dark:text-slate-200"
              >
                Clear filters
              </a>
            </div>
          </div>
          <div class="flex flex-wrap items-center justify-between gap-3 rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm text-slate-600 shadow-sm dark:border-slate-800 dark:bg-slate-900/70 dark:text-slate-300">
            <div>{pageInfo}</div>
            <div class="flex items-center gap-2">
              <a
                class={`rounded-lg border border-slate-200 px-3 py-1 text-sm font-semibold text-slate-700 hover:border-slate-300 dark:border-slate-700 dark:text-slate-200 ${
                  pagination.prevHref ? "" : "pointer-events-none opacity-50"
                }`}
                href={pagination.prevHref ?? "#"}
              >
                Prev
              </a>
              <label class="flex items-center gap-2 text-xs uppercase tracking-wide">
                <span>Page</span>
                <input
                  name="page"
                  type="number"
                  min="1"
                  max={String(pagination.pages)}
                  step="1"
                  value={String(filters.page)}
                  class="w-20 rounded-lg border border-slate-200 bg-transparent px-2 py-1 text-sm text-slate-900 dark:border-slate-800 dark:text-slate-100"
                />
              </label>
              <a
                class={`rounded-lg border border-slate-200 px-3 py-1 text-sm font-semibold text-slate-700 hover:border-slate-300 dark:border-slate-700 dark:text-slate-200 ${
                  pagination.nextHref ? "" : "pointer-events-none opacity-50"
                }`}
                href={pagination.nextHref ?? "#"}
              >
                Next
              </a>
            </div>
          </div>
        </form>
      </div>
    </section>

    <main class="mx-auto w-full max-w-6xl px-6 pb-12 pt-6">
      <div class="mb-4 flex flex-wrap items-center justify-between gap-3 text-sm text-slate-500 dark:text-slate-400">
        <div>{matchSummary}</div>
        <div>Showing {items.length.toLocaleString()} items.</div>
      </div>
      <div class="grid gap-4">
        {items.length ? (
          items.map((mapping) => (
            <MappingCard
              key={`mapping-${mapping.id}`}
              dict={dict}
              mapping={mapping}
              mappingId={mapping.id}
            />
          ))
        ) : (
          <div class="rounded-xl border border-slate-200 bg-white px-4 py-6 text-sm text-slate-500 dark:border-slate-800 dark:bg-slate-900/70 dark:text-slate-400">
            No mappings match the current filters.
          </div>
        )}
      </div>
    </main>
    <script
      dangerouslySetInnerHTML={{
        __html: `
(() => {
  const getActiveIndex = (track) => {
    const slides = Array.from(track.querySelectorAll('[data-carousel-slide]'));
    if (!slides.length) return 0;
    const trackRect = track.getBoundingClientRect();
    const center = trackRect.left + trackRect.width / 2;
    let bestIndex = 0;
    let bestDistance = Infinity;
    slides.forEach((slide, index) => {
      const rect = slide.getBoundingClientRect();
      const slideCenter = rect.left + rect.width / 2;
      const distance = Math.abs(slideCenter - center);
      if (distance < bestDistance) {
        bestDistance = distance;
        bestIndex = index;
      }
    });
    return bestIndex;
  };

  const scrollToIndex = (track, index) => {
    const slides = Array.from(track.querySelectorAll('[data-carousel-slide]'));
    if (!slides.length) return;
    const safeIndex = Math.max(0, Math.min(index, slides.length - 1));
    const target = slides[safeIndex];
    if (!target) return;
    target.scrollIntoView({ behavior: 'smooth', inline: 'center', block: 'nearest' });
  };

  const mountCarousel = (details) => {
    if (details.dataset.rendered === 'true') return;
    const template = details.querySelector('[data-carousel-template]');
    const track = details.querySelector('[data-carousel-track]');
    if (!template || !track) return;
    track.appendChild(template.content.cloneNode(true));
    details.dataset.rendered = 'true';
  };

  const carousels = document.querySelectorAll('[data-carousel]');
  carousels.forEach((details) => {
    details.addEventListener('toggle', () => {
      if (details.open) {
        mountCarousel(details);
      }
    });

    details.addEventListener('click', (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      const isPrev = target.closest('[data-carousel-prev]');
      const isNext = target.closest('[data-carousel-next]');
      if (!isPrev && !isNext) return;
      event.preventDefault();
      const track = details.querySelector('[data-carousel-track]');
      if (!track) return;
      const current = getActiveIndex(track);
      const nextIndex = isNext ? current + 1 : current - 1;
      scrollToIndex(track, nextIndex);
    });
  });
})();
        `,
      }}
    />
  </>
);
