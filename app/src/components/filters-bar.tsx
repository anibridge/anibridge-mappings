import type {
  PresenceFilter,
  ProvenancePayload,
  MappingFilters,
} from "../utils/provenance";

type FiltersBarProps = {
  payload: ProvenancePayload;
  filters: MappingFilters;
  onFilterChange: (key: keyof MappingFilters, value: string) => void;
};

export const FiltersBar = ({
  payload,
  filters,
  onFilterChange,
}: FiltersBarProps) => {
  return (
    <section class="grid grid-cols-2 gap-1.5 border border-slate-300 bg-slate-100 p-2 md:grid-cols-2 xl:grid-cols-4 dark:border-slate-600 dark:bg-slate-800">
      <input
        value={filters.source}
        onInput={(event) =>
          onFilterChange("source", (event.target as HTMLInputElement).value)
        }
        placeholder="source descriptor"
        class="w-full border border-slate-400 bg-white px-2 py-1 text-xs outline-none focus:border-sky-700 dark:border-slate-500 dark:bg-slate-800 dark:text-slate-100 dark:focus:border-sky-300"
      />
      <input
        value={filters.target}
        onInput={(event) =>
          onFilterChange("target", (event.target as HTMLInputElement).value)
        }
        placeholder="target descriptor"
        class="w-full border border-slate-400 bg-white px-2 py-1 text-xs outline-none focus:border-sky-700 dark:border-slate-500 dark:bg-slate-800 dark:text-slate-100 dark:focus:border-sky-300"
      />
      <select
        value={filters.stage}
        onChange={(event) =>
          onFilterChange("stage", (event.target as HTMLSelectElement).value)
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
          onFilterChange(
            "present",
            (event.target as HTMLSelectElement).value as PresenceFilter,
          )
        }
        class="w-full border border-slate-400 bg-white px-2 py-1 text-xs outline-none focus:border-sky-700 dark:border-slate-500 dark:bg-slate-800 dark:text-slate-100 dark:focus:border-sky-300"
      >
        <option value="present">present</option>
        <option value="missing">missing</option>
        <option value="all">all</option>
      </select>
    </section>
  );
};
