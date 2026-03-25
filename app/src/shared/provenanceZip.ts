import { strFromU8, unzipSync } from "fflate";

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

type DictRange = { s?: string; t?: string };

type LegacyEvent = {
  a: number;
  s: number;
  ac: number;
  rs: number;
  r: number;
  e: boolean;
};

type LegacyMapping = {
  s: number;
  t: number;
  p?: boolean;
  n?: number;
  ev?: LegacyEvent[];
};

export type LegacyProvenancePayload = {
  dict: {
    descriptors: string[];
    actions: string[];
    stages: string[];
    actors: string[];
    reasons: string[];
    ranges: DictRange[];
  };
  mappings: LegacyMapping[];
  $meta?: Record<string, unknown>;
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

export const parseProvenanceZip = (
  content: ArrayBuffer,
): LegacyProvenancePayload => {
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

  const mappings: LegacyMapping[] = [];
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
      const compactEvents: LegacyEvent[] = [];

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
