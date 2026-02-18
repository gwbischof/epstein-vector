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
  return (
    <div className="glass rounded-xl p-1 flex">
      {MODES.map((m) => {
        const Icon = m.icon;
        const isActive = mode === m.value;
        return (
          <button
            key={m.value}
            onClick={() => onChange(m.value)}
            className={`relative flex items-center justify-center gap-2 px-4 py-2 rounded-lg text-xs font-medium uppercase tracking-wider transition-colors ${
              isActive ? "text-cyan-400" : "text-slate-500 hover:text-slate-300"
            }`}
          >
            {isActive && (
              <motion.div
                layoutId="mode-indicator"
                className="absolute inset-0 rounded-lg bg-cyan-500/15 border border-cyan-500/25"
                transition={{ type: "spring", stiffness: 400, damping: 35 }}
              />
            )}
            <Icon className="relative z-10 w-3.5 h-3.5" />
            <span className="relative z-10">{m.label}</span>
          </button>
        );
      })}
    </div>
  );
}
