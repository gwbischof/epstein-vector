"use client";

import { EyeOff, Eye } from "lucide-react";

interface ExcludeExactToggleProps {
  enabled: boolean;
  onChange: (enabled: boolean) => void;
}

export function ExcludeExactToggle({ enabled, onChange }: ExcludeExactToggleProps) {
  const Icon = enabled ? EyeOff : Eye;

  return (
    <div className="glass rounded-xl p-1">
      <button
        onClick={() => onChange(!enabled)}
        className={`flex items-center gap-2 px-3 py-2 rounded-lg text-xs font-medium uppercase tracking-wider transition-colors ${
          enabled
            ? "text-cyan-400 bg-cyan-500/15"
            : "text-slate-500 hover:text-slate-300"
        }`}
      >
        <Icon className="w-3.5 h-3.5" />
        Hide Exact
      </button>
    </div>
  );
}
