"use client";

import { motion } from "framer-motion";
import { EyeOff, Eye } from "lucide-react";

interface ExcludeExactToggleProps {
  enabled: boolean;
  onChange: (enabled: boolean) => void;
}

export function ExcludeExactToggle({ enabled, onChange }: ExcludeExactToggleProps) {
  const Icon = enabled ? EyeOff : Eye;

  return (
    <button
      onClick={() => onChange(!enabled)}
      className={`glass rounded-xl px-3 py-2 flex items-center gap-2 transition-colors ${
        enabled
          ? "border border-cyan-500/25 bg-cyan-500/10"
          : "border border-transparent hover:border-slate-700/50"
      }`}
    >
      <motion.div
        initial={false}
        animate={{ rotate: enabled ? 0 : 0, scale: enabled ? 1.1 : 1 }}
        transition={{ type: "spring", stiffness: 400, damping: 25 }}
      >
        <Icon className={`w-3.5 h-3.5 ${enabled ? "text-cyan-400" : "text-slate-500"}`} />
      </motion.div>
      <span
        className={`text-xs font-medium uppercase tracking-wider ${
          enabled ? "text-cyan-400" : "text-slate-500"
        }`}
      >
        Hide Exact
      </span>
    </button>
  );
}
