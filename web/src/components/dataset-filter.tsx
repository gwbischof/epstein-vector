"use client";

import { Database } from "lucide-react";

interface DatasetFilterProps {
  dataset: number | null;
  onChange: (d: number | null) => void;
}

const DATASETS = [null, 4, 8, 9, 10, 11, 12] as const;

export function DatasetFilter({ dataset, onChange }: DatasetFilterProps) {
  return (
    <div className="glass rounded-xl px-3 py-1.5 flex items-center gap-2">
      <Database className="w-3.5 h-3.5 text-slate-500 shrink-0" />
      <select
        value={dataset ?? "all"}
        onChange={(e) => onChange(e.target.value === "all" ? null : Number(e.target.value))}
        className="bg-transparent text-xs font-medium uppercase tracking-wider text-slate-300 focus:outline-none cursor-pointer appearance-none pr-4"
        style={{
          backgroundImage: `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' fill='none' viewBox='0 0 24 24' stroke='%2394a3b8' stroke-width='2'%3E%3Cpath stroke-linecap='round' stroke-linejoin='round' d='m6 9 6 6 6-6'/%3E%3C/svg%3E")`,
          backgroundRepeat: "no-repeat",
          backgroundPosition: "right 0 center",
        }}
      >
        {DATASETS.map((d) => (
          <option key={d ?? "all"} value={d ?? "all"} className="bg-slate-900 text-slate-200">
            {d === null ? "All Datasets" : `Dataset ${d}`}
          </option>
        ))}
      </select>
    </div>
  );
}
