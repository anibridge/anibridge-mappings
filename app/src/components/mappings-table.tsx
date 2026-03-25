import type { Dict, MappingFilters } from "../utils/provenance";
import { getDictValue } from "../utils/provenance";
import type { MappingWithId, SortColumn } from "./ui-types";

type MappingsTableProps = {
  dict: Dict;
  rows: MappingWithId[];
  selectedKey: string | null;
  sortLabel: (column: SortColumn) => string;
  onSort: (column: SortColumn) => void;
  onSelect: (mappingKey: string) => void;
  paged: { page: number; pages: number };
  setFilters: (updater: (prev: MappingFilters) => MappingFilters) => void;
};

export const MappingsTable = ({
  dict,
  rows,
  selectedKey,
  sortLabel,
  onSort,
  onSelect,
  paged,
  setFilters,
}: MappingsTableProps) => {
  return (
    <aside class="flex min-h-0 flex-col border border-slate-300 bg-slate-50 dark:border-slate-600 dark:bg-slate-800">
      <div class="grid grid-cols-[88px_minmax(0,1fr)_minmax(0,1fr)_64px] border-b border-slate-300 bg-slate-200 px-2 py-1 text-[11px] uppercase tracking-[0.03em] text-slate-600 dark:border-slate-600 dark:bg-slate-700 dark:text-slate-300">
        <button type="button" class="text-left" onClick={() => onSort("state")}>
          {`STATE${sortLabel("state")}`}
        </button>
        <button
          type="button"
          class="text-left"
          onClick={() => onSort("source")}
        >
          {`SOURCE${sortLabel("source")}`}
        </button>
        <button
          type="button"
          class="text-left"
          onClick={() => onSort("target")}
        >
          {`TARGET${sortLabel("target")}`}
        </button>
        <button
          type="button"
          class="text-right"
          onClick={() => onSort("steps")}
        >
          {`STEPS${sortLabel("steps")}`}
        </button>
      </div>

      <div class="min-h-0 flex-1 overflow-auto">
        {rows.map((mapping) => {
          const source = getDictValue(dict, "descriptors", mapping.s) || "-";
          const target = getDictValue(dict, "descriptors", mapping.t) || "-";
          const steps = mapping.n ?? mapping.ev?.length ?? 0;
          const active = selectedKey === mapping.key;

          return (
            <button
              type="button"
              key={mapping.id}
              class={`grid w-full grid-cols-[88px_minmax(0,1fr)_minmax(0,1fr)_64px] border-b border-slate-200 px-2 py-1 text-left text-[12px] ${
                active
                  ? "bg-sky-100 dark:bg-sky-800/35"
                  : "bg-white hover:bg-slate-100 dark:bg-slate-800 dark:hover:bg-slate-700"
              }`}
              onClick={() => onSelect(mapping.key)}
            >
              <span class="truncate">{mapping.p ? "present" : "missing"}</span>
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
  );
};
