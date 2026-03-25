import type { Mapping } from "../utils/provenance";

export type MappingWithId = Mapping & { id: number; key: string };

export type SortColumn = "default" | "state" | "source" | "target" | "steps";
export type SortDirection = "asc" | "desc";
export type MappingViewFormat = "json" | "yaml";

export type DiffLine = { key: string; type: "add" | "remove" | "same"; text: string };

export type TimelineSlide = {
  index: number;
  action: string;
  stage: string;
  actor: string;
  reason: string;
  range: string;
  effect: "active" | "inactive" | "skipped";
  diff: DiffLine[];
};

export type ExternalLink = {
  label: string;
  url: string;
};
