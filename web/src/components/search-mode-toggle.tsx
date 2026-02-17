"use client";

import { motion } from "framer-motion";
import { Brain, Type, Zap } from "lucide-react";
import type { SearchMode } from "@/lib/types";

const MODES: { value: SearchMode; label: string; icon: typeof Brain }[] = [
  { value: "semantic", label: "Semantic", icon: Brain },
  { value: "keyword", label: "Keyword", icon: Type },
  { value: "fuzzy", label: "Fuzzy", icon: Zap },
];

interface SearchModeToggleProps {
  mode: SearchMode;
  onChange: (mode: SearchMode) => void;
}

export function SearchModeToggle({ mode, onChange }: SearchModeToggleProps) {
  const activeIndex = MODES.findIndex((m) => m.value === mode);

  return (
    <div className="glass rounded-xl p-1 flex relative">
      {/* Sliding indicator */}
      <motion.div
        className="absolute inset-y-1 rounded-lg bg-cyan-500/15 border border-cyan-500/25"
        layout
        transition={{ type: "spring", stiffness: 400, damping: 35 }}
        style={{
          left: `calc(${activeIndex} * 33.333% + 4px)`,
          width: "calc(33.333% - 4px)",
        }}
      />

      {MODES.map((m) => {
        const Icon = m.icon;
        return (
          <button
            key={m.value}
            onClick={() => onChange(m.value)}
            className={`relative z-10 flex items-center gap-2 px-4 py-2 rounded-lg text-xs font-medium uppercase tracking-wider transition-colors ${
              mode === m.value ? "text-cyan-400" : "text-slate-500 hover:text-slate-300"
            }`}
          >
            <Icon className="w-3.5 h-3.5" />
            {m.label}
          </button>
        );
      })}
    </div>
  );
}
