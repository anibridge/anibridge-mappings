import { strFromU8, unzipSync } from "fflate";

export type DictRange = { s?: string; t?: string };
export type Dict = {
  descriptors: string[];
  actions: string[];
  stages: string[];
  actors: string[];
  reasons: string[];
  ranges: DictRange[];
};

export type MappingEvent = {
  a: number;
  s: number;
  ac: number;
  rs: number;
  r: number;
  e: boolean;
};

export type Mapping = {
  s: number;
  t: number;
  p?: boolean;
  n?: number;
  ev?: MappingEvent[];
};

export type ProvenancePayload = {
  dict: Dict;
  mappings: Mapping[];
  $meta?: Record<string, unknown>;
};

const PROVENANCE_URL = "/data/provenance.zip";

let provenancePromise: Promise<ProvenancePayload> | null = null;

type ArchiveManifest = {
  format?: string;
  schema_version?: string;
  generated_on?: string;
  summary?: {
    descriptors?: number;
    mappings?: number;
    present_mappings?: number;
    missing_mappings?: number;
    events?: number;
    files?: number;
  };
};

type ArchiveIndexItem = {
  descriptor: string;
  file: string;
};

type ArchiveEvent = {
  action: string;
  stage: string;
  actor?: string;
  reason?: string;
  source_range: string;
  target_range: string;
  effective: boolean;
};

type ArchiveMapping = {
  target_descriptor: string;
  events: ArchiveEvent[];
};

type ArchiveDescriptorDoc = {
  descriptor: string;
  mappings: ArchiveMapping[];
};

const parseJson = <T>(buffer: Uint8Array, label: string): T => {
  try {
    return JSON.parse(strFromU8(buffer)) as T;
  } catch (error) {
    throw new Error(
      `Failed to parse ${label}: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
};

const indexOfValue = (
  value: string | undefined,
  values: string[],
  map: Map<string, number>,
) => {
  if (!value) return -1;
  const existing = map.get(value);
  if (existing !== undefined) return existing;
  const index = values.length;
  values.push(value);
  map.set(value, index);
  return index;
};

const parseProvenanceZip = (content: ArrayBuffer): ProvenancePayload => {
  const files = unzipSync(new Uint8Array(content));
  const manifestBytes = files["manifest.json"];
  const indexBytes = files["descriptor-index.json"];

  if (!manifestBytes) {
    throw new Error("provenance.zip is missing manifest.json");
  }
  if (!indexBytes) {
    throw new Error("provenance.zip is missing descriptor-index.json");
  }

  const manifest = parseJson<ArchiveManifest>(manifestBytes, "manifest.json");
  const index = parseJson<ArchiveIndexItem[]>(
    indexBytes,
    "descriptor-index.json",
  );

  const descriptors: string[] = [];
  const descriptorIndex = new Map<string, number>();
  const actions: string[] = [];
  const actionIndex = new Map<string, number>();
  const stages: string[] = [];
  const stageIndex = new Map<string, number>();
  const actors: string[] = [];
  const actorIndex = new Map<string, number>();
  const reasons: string[] = [];
  const reasonIndex = new Map<string, number>();
  const ranges: DictRange[] = [];
  const rangeIndex = new Map<string, number>();

  const mappings: Mapping[] = [];
  const seenPairs = new Set<string>();

  for (const item of index) {
    const descriptor = item.descriptor;
    const descriptorFile = files[item.file];
    if (!descriptorFile) {
      throw new Error(`Missing descriptor file in provenance.zip: ${item.file}`);
    }

    const descriptorDoc = parseJson<ArchiveDescriptorDoc>(descriptorFile, item.file);
    for (const mapping of descriptorDoc.mappings || []) {
      const targetDescriptor = mapping.target_descriptor;
      const [left, right] = [descriptor, targetDescriptor].sort((a, b) =>
        a.localeCompare(b),
      );
      const pairKey = `${left}\u0000${right}`;
      if (seenPairs.has(pairKey)) {
        continue;
      }
      seenPairs.add(pairKey);

      const sourceIdx = indexOfValue(left, descriptors, descriptorIndex);
      const targetIdx = indexOfValue(right, descriptors, descriptorIndex);

      const flip = descriptor !== left;
      const currentRanges = new Set<number>();
      const compactEvents: MappingEvent[] = [];

      for (const event of mapping.events || []) {
        const sourceRange = flip ? event.target_range : event.source_range;
        const targetRange = flip ? event.source_range : event.target_range;
        const rangeKey = `${sourceRange}\u0000${targetRange}`;
        let rangeIdx = rangeIndex.get(rangeKey);
        if (rangeIdx === undefined) {
          rangeIdx = ranges.length;
          ranges.push({ s: sourceRange, t: targetRange });
          rangeIndex.set(rangeKey, rangeIdx);
        }

        if (event.effective) {
          if (event.action === "add") currentRanges.add(rangeIdx);
          if (event.action === "remove") currentRanges.delete(rangeIdx);
        }

        compactEvents.push({
          a: indexOfValue(event.action, actions, actionIndex),
          s: indexOfValue(event.stage, stages, stageIndex),
          ac: indexOfValue(event.actor, actors, actorIndex),
          rs: indexOfValue(event.reason, reasons, reasonIndex),
          r: rangeIdx,
          e: Boolean(event.effective),
        });
      }

      mappings.push({
        s: sourceIdx,
        t: targetIdx,
        p: currentRanges.size > 0,
        n: compactEvents.length,
        ev: compactEvents,
      });
    }
  }

  return {
    $meta: {
      format: manifest.format ?? "anibridge.provenance.v1",
      schema_version: manifest.schema_version,
      generated_on: manifest.generated_on,
      archive_summary: manifest.summary,
      projected_mappings: mappings.length,
    },
    dict: {
      descriptors,
      actions,
      stages,
      actors,
      reasons,
      ranges,
    },
    mappings,
  };
};

export const getProvenance = async (): Promise<ProvenancePayload> => {
  if (!provenancePromise) {
    provenancePromise = (async () => {
      const res = await fetch(PROVENANCE_URL, {
        headers: { Accept: "application/zip" },
      });
      if (!res.ok) {
        throw new Error(
          `Failed to fetch provenance: ${res.status} ${res.statusText}`,
        );
      }
      return parseProvenanceZip(await res.arrayBuffer());
    })().catch((err) => {
      provenancePromise = null;
      throw err;
    });
  }

  return provenancePromise;
};

export type PresenceFilter = "all" | "present" | "missing";
export type SortOrder = "default" | "present" | "missing" | "timeline";

export type MappingFilters = {
  source: string;
  target: string;
  actor: string;
  reason: string;
  range: string;
  stage: string;
  present: PresenceFilter;
  sort: SortOrder;
  page: number;
  perPage: number;
};

type RangeToken = {
  raw: string;
  source: string;
  target: string;
  pair: boolean;
};

type DictListKey = Exclude<keyof Dict, "ranges">;

export const getDictValue = (dict: Dict, key: DictListKey, index?: number) => {
  if (index === undefined || index === null || index < 0) return "";
  const value = dict[key];
  if (!Array.isArray(value)) return "";
  return value[index] ?? "";
};

export const getRange = (dict: Dict, index?: number) => {
  if (index === undefined || index === null || index < 0) {
    return { source_range: "", target_range: "" };
  }
  const range = dict.ranges?.[index];
  if (!range) return { source_range: "", target_range: "" };
  return { source_range: range.s ?? "", target_range: range.t ?? "" };
};

const toText = (value: unknown) => (value ?? "").toString();
const normalize = (value: unknown) => toText(value).trim().toLowerCase();

const parseRangeTokens = (input: string) => {
  const query = normalize(input);
  if (!query) return [] as RangeToken[];
  return query
    .split(",")
    .map((token) => token.trim())
    .filter(Boolean)
    .map((token) => {
      if (token.includes("|")) {
        const [source, target] = token.split("|").map((part) => part.trim());
        return { raw: token, source, target, pair: true };
      }
      return { raw: token, source: "", target: "", pair: false };
    });
};

const mappingLabel = (mapping: Mapping, dict: Dict) => {
  const source = getDictValue(dict, "descriptors", mapping.s);
  const target = getDictValue(dict, "descriptors", mapping.t);
  if (source && target) return `${source} → ${target}`;
  if (source) return source;
  if (target) return target;
  return "(untitled mapping)";
};

export const mappingMatches = (
  mapping: Mapping,
  filters: MappingFilters,
  dict: Dict,
) => {
  const isPresent = Boolean(mapping.p);
  if (filters.present === "present" && !isPresent) return false;
  if (filters.present === "missing" && isPresent) return false;

  if (filters.stage !== "all") {
    const hasStage = (mapping.ev || []).some(
      (event) => getDictValue(dict, "stages", event.s) === filters.stage,
    );
    if (!hasStage) return false;
  }

  const sourceQuery = normalize(filters.source);
  if (
    sourceQuery &&
    !normalize(getDictValue(dict, "descriptors", mapping.s)).includes(sourceQuery)
  ) {
    return false;
  }

  const targetQuery = normalize(filters.target);
  if (
    targetQuery &&
    !normalize(getDictValue(dict, "descriptors", mapping.t)).includes(targetQuery)
  ) {
    return false;
  }

  const rangeTokens = parseRangeTokens(filters.range);
  if (rangeTokens.length) {
    const ranges = (mapping.ev || []).map((event) => getRange(dict, event.r));
    const matchesToken = (token: RangeToken) =>
      ranges.some((range) => {
        const sourceValue = normalize(range.source_range);
        const targetValue = normalize(range.target_range);
        if (token.pair) {
          return (
            sourceValue.includes(token.source) &&
            targetValue.includes(token.target)
          );
        }
        return normalize(`${range.source_range} ${range.target_range}`).includes(
          token.raw,
        );
      });
    if (!rangeTokens.every(matchesToken)) return false;
  }

  const actorQuery = normalize(filters.actor);
  if (actorQuery) {
    const match = (mapping.ev || []).some((event) =>
      normalize(getDictValue(dict, "actors", event.ac)).includes(actorQuery),
    );
    if (!match) return false;
  }

  const reasonQuery = normalize(filters.reason);
  if (reasonQuery) {
    const match = (mapping.ev || []).some((event) =>
      normalize(getDictValue(dict, "reasons", event.rs)).includes(reasonQuery),
    );
    if (!match) return false;
  }

  return true;
};

export type IndexedMapping = { index: number; mapping: Mapping };

export const filterMappings = (
  payload: ProvenancePayload,
  filters: MappingFilters,
) => {
  const dict = payload.dict;
  let filtered = payload.mappings
    .map((mapping, index) => ({ mapping, index }))
    .filter(({ mapping }) => mappingMatches(mapping, filters, dict));

  if (filters.sort === "present") {
    filtered = filtered
      .slice()
      .sort(
        (a, b) => Number(Boolean(b.mapping.p)) - Number(Boolean(a.mapping.p)),
      );
  }
  if (filters.sort === "missing") {
    filtered = filtered
      .slice()
      .sort(
        (a, b) => Number(Boolean(a.mapping.p)) - Number(Boolean(b.mapping.p)),
      );
  }
  if (filters.sort === "timeline") {
    filtered = filtered
      .slice()
      .sort(
        (a, b) =>
          (b.mapping.n ?? b.mapping.ev?.length ?? 0) -
          (a.mapping.n ?? a.mapping.ev?.length ?? 0) ||
          mappingLabel(a.mapping, dict).localeCompare(mappingLabel(b.mapping, dict)),
      );
  }

  return filtered;
};

export const paginateMappings = (
  mappings: IndexedMapping[],
  filters: MappingFilters,
) => {
  const perPage = Math.max(1, Math.min(filters.perPage, 1000));
  const total = mappings.length;
  const pages = Math.max(1, Math.ceil(total / perPage));
  const page = Math.min(Math.max(filters.page, 1), pages);
  const start = (page - 1) * perPage;
  const items = mappings.slice(start, start + perPage);

  return { page, perPage, pages, total, items };
};

export const summarizeProvenance = (payload: ProvenancePayload) => {
  const meta = payload.$meta ?? {};
  const present = payload.mappings.filter((mapping) => mapping.p).length;
  const generatedOn =
    typeof meta.generated_on === "string" ? meta.generated_on : null;
  return {
    generated_on: generatedOn,
    mappings: payload.mappings.length,
    present_mappings: present,
    missing_mappings: payload.mappings.length - present,
  };
};
