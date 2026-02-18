"use client";

import { Search, CornerDownLeft } from "lucide-react";

interface SearchBarProps {
  query: string;
  onQueryChange: (q: string) => void;
  onSearch: () => void;
  loading: boolean;
}

export function SearchBar({ query, onQueryChange, onSearch, loading }: SearchBarProps) {
  return (
    <div className="relative w-full group">
      {/* Outer glow ring */}
      <div className="absolute -inset-[1px] rounded-2xl bg-gradient-to-r from-cyan-500/20 via-violet-500/10 to-cyan-500/20 opacity-0 group-focus-within:opacity-100 transition-opacity duration-500 blur-sm" />

      <div className="relative glass-strong rounded-2xl flex items-center gap-3 px-5 py-4 group-focus-within:glow-cyan-strong transition-shadow duration-500">
        {/* Search icon / spinner */}
        {loading ? (
          <div className="w-5 h-5 border-2 border-cyan-400/30 border-t-cyan-400 rounded-full spin-slow shrink-0" />
        ) : (
          <Search className="w-5 h-5 text-slate-500 group-focus-within:text-cyan-400 transition-colors duration-300 shrink-0" />
        )}

        <input
          type="text"
          value={query}
          onChange={(e) => onQueryChange(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              onSearch();
            }
          }}
          placeholder="Search DOJ documents..."
          className="flex-1 bg-transparent text-base text-slate-100 placeholder:text-slate-500 focus:outline-none font-sans"
          autoFocus
        />

        {/* Enter hint */}
        {query.trim() && (
          <button
            onClick={onSearch}
            className="shrink-0 flex items-center gap-1.5 text-xs text-slate-500 hover:text-cyan-400 transition-colors px-2 py-1 rounded-lg bg-slate-800/50"
          >
            <span className="hidden sm:inline">Search</span>
            <CornerDownLeft className="w-3 h-3" />
          </button>
        )}
      </div>
    </div>
  );
}
