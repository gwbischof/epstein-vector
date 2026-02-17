"use client";

import { motion } from "framer-motion";
import { Brain, Type } from "lucide-react";
import type { SearchMode } from "@/lib/types";

interface SearchModeToggleProps {
  mode: SearchMode;
  onChange: (mode: SearchMode) => void;
}

export function SearchModeToggle({ mode, onChange }: SearchModeToggleProps) {
  return (
    <div className="glass rounded-xl p-1 flex relative">
      {/* Sliding indicator */}
      <motion.div
        className="absolute inset-y-1 rounded-lg bg-cyan-500/15 border border-cyan-500/25"
        layout
        transition={{ type: "spring", stiffness: 400, damping: 35 }}
        style={{
          left: mode === "semantic" ? "4px" : "50%",
          width: "calc(50% - 4px)",
        }}
      />

      <button
        onClick={() => onChange("semantic")}
        className={`relative z-10 flex items-center gap-2 px-4 py-2 rounded-lg text-xs font-medium uppercase tracking-wider transition-colors ${
          mode === "semantic" ? "text-cyan-400" : "text-slate-500 hover:text-slate-300"
        }`}
      >
        <Brain className="w-3.5 h-3.5" />
        Semantic
      </button>

      <button
        onClick={() => onChange("keyword")}
        className={`relative z-10 flex items-center gap-2 px-4 py-2 rounded-lg text-xs font-medium uppercase tracking-wider transition-colors ${
          mode === "keyword" ? "text-cyan-400" : "text-slate-500 hover:text-slate-300"
        }`}
      >
        <Type className="w-3.5 h-3.5" />
        Keyword
      </button>
    </div>
  );
}
