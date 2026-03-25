import { diffLines } from "diff";
import type { Change } from "diff";
import type { Dict, Mapping } from "./provenance";
import { getDictValue, getRange } from "./provenance";
import type { DiffLine, ExternalLink, MappingViewFormat, TimelineSlide } from "../components/ui-types";

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

  const activeRanges = new Map<string, string>();
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
