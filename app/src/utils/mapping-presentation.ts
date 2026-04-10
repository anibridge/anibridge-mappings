import { diffLines } from "diff";
import type { Change } from "diff";
import type { Dict, Mapping } from "./provenance";
import { getDictValue, getRange } from "./provenance";
import type { DiffLine, ExternalLink, MappingViewFormat, TimelineSlide } from "../components/ui-types";

/** Parse "N" or "N-M" into [start, end]. Returns null for non-numeric keys. */
const parseBounds = (s: string): [number, number] | null => {
  const m = /^(\d+)(?:-(\d+))?$/.exec(s.trim());
  if (!m) return null;
  const lo = Number(m[1]);
  const hi = m[2] !== undefined ? Number(m[2]) : lo;
  return [lo, hi];
};

const fmtRange = (lo: number, hi: number): string =>
  lo === hi ? `${lo}` : `${lo}-${hi}`;

const collapseRanges = (
  ranges: Record<string, string>,
): Record<string, string> => {
  type NumRun = { ss: number; se: number; value: string };
  const numRuns: NumRun[] = [];
  let pass1Other: Record<string, string> = {};

  for (const [src, tgt] of Object.entries(ranges)) {
    const sb = parseBounds(src);
    if (!sb) {
      pass1Other[src] = tgt;
      continue;
    }
    numRuns.push({ ss: sb[0], se: sb[1], value: tgt });
  }

  let pass1: Record<string, string>;
  if (!numRuns.length) {
    pass1 = ranges;
  } else {
    numRuns.sort((a, b) => a.ss - b.ss || a.se - b.se);
    pass1 = {};
    let run = numRuns[0];
    for (let i = 1; i < numRuns.length; i++) {
      const cur = numRuns[i];
      if (cur.value === run.value && cur.ss === run.se + 1) {
        run = { ...run, se: cur.se };
      } else {
        pass1[fmtRange(run.ss, run.se)] = run.value;
        run = cur;
      }
    }
    pass1[fmtRange(run.ss, run.se)] = run.value;
    Object.assign(pass1, pass1Other);
  }

  type LinRun = { ss: number; se: number; ts: number; te: number };
  const linRuns: LinRun[] = [];
  const passthrough: Record<string, string> = {};

  for (const [src, tgt] of Object.entries(pass1)) {
    const sb = parseBounds(src);
    const tb = parseBounds(tgt);
    if (!sb || !tb) {
      passthrough[src] = tgt;
      continue;
    }
    linRuns.push({ ss: sb[0], se: sb[1], ts: tb[0], te: tb[1] });
  }

  if (!linRuns.length) return pass1;

  linRuns.sort((a, b) => a.ss - b.ss || a.se - b.se || a.ts - b.ts);
  const merged: Record<string, string> = {};
  let cur = linRuns[0];
  let offset = cur.ts - cur.ss;

  for (let i = 1; i < linRuns.length; i++) {
    const next = linRuns[i];
    const nextOffset = next.ts - next.ss;
    if (
      next.ss === cur.se + 1 &&
      next.ts === cur.te + 1 &&
      nextOffset === offset
    ) {
      cur = { ...cur, se: next.se, te: next.te };
    } else {
      merged[fmtRange(cur.ss, cur.se)] = fmtRange(cur.ts, cur.te);
      cur = next;
      offset = nextOffset;
    }
  }
  merged[fmtRange(cur.ss, cur.se)] = fmtRange(cur.ts, cur.te);
  Object.assign(merged, passthrough);
  return merged;
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

export const descriptorToExternal = (descriptor?: string | null): ExternalLink | null => {
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

export const buildTimelineSlides = (dict: Dict, mapping: Mapping): TimelineSlide[] => {
  const events = mapping.ev ?? [];
  if (!events.length) return [];

  const activeRanges = new Set<string>();
  const sourceDescriptor = getDictValue(dict, "descriptors", mapping.s) || "-";
  const targetDescriptor = getDictValue(dict, "descriptors", mapping.t) || "-";
  let previousJson = JSON.stringify({ [sourceDescriptor]: { [targetDescriptor]: {} } }, null, 2);

  return events.map((event, index) => {
    const action = getDictValue(dict, "actions", event.a) || "-";
    const stage = getDictValue(dict, "stages", event.s) || "-";
    const actor = getDictValue(dict, "actors", event.ac) || "-";
    const reason = getDictValue(dict, "reasons", event.rs) || "-";
    const range = getRange(dict, event.r);
    const sourceRange = range.source_range || "-";
    const targetRange = range.target_range || "-";

    if (event.e) {
      const pairKey = `${sourceRange}\0${targetRange}`;
      if (action === "add") {
        activeRanges.add(pairKey);
      }
      if (action === "remove") {
        activeRanges.delete(pairKey);
      }
    }

    const orderedRanges = collapseRanges(Object.fromEntries(
      [...activeRanges]
        .map(k => k.split('\0') as [string, string])
        .sort(([a], [b]) => a.localeCompare(b)),
    ));

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

export const stepLabel = (step: number, total: number) => `Step ${step} / ${total}`;

export const buildFinalMappingObject = (dict: Dict, mapping: Mapping) => {
  const sourceDescriptor = getDictValue(dict, "descriptors", mapping.s) || "-";
  const targetDescriptor = getDictValue(dict, "descriptors", mapping.t) || "-";
  const activeRanges = new Set<string>();

  for (const event of mapping.ev ?? []) {
    if (!event.e) continue;
    const action = getDictValue(dict, "actions", event.a) || "";
    const range = getRange(dict, event.r);
    const sourceRange = range.source_range || "-";
    const targetRange = range.target_range || "-";
    const pairKey = `${sourceRange}\0${targetRange}`;

    if (action === "add") {
      activeRanges.add(pairKey);
    }
    if (action === "remove") {
      activeRanges.delete(pairKey);
    }
  }

  const orderedRanges = collapseRanges(Object.fromEntries(
    [...activeRanges]
      .map(k => k.split('\0') as [string, string])
      .sort(([a], [b]) => a.localeCompare(b)),
  ));

  return { [sourceDescriptor]: { [targetDescriptor]: orderedRanges } };
};

const formatYamlValue = (value: string | Record<string, unknown>, depth = 0): string => {
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

export const formatMappingView = (
  mappingObject: Record<string, unknown>,
  format: MappingViewFormat,
): string => {
  if (format === "yaml") {
    return formatYamlValue(mappingObject);
  }
  return JSON.stringify(mappingObject, null, 2);
};
