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
    <div className="glass rounded-xl p-1 relative">
      {/* Active background indicator */}
      {enabled && (
        <motion.div
          className="absolute inset-1 rounded-lg bg-cyan-500/15 border border-cyan-500/25"
          layoutId="exclude-exact-bg"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ type: "spring", stiffness: 400, damping: 35 }}
        />
      )}
      <button
        onClick={() => onChange(!enabled)}
        className={`relative z-10 flex items-center gap-2 px-3 py-2 rounded-lg text-xs font-medium uppercase tracking-wider transition-colors ${
          enabled ? "text-cyan-400" : "text-slate-500 hover:text-slate-300"
        }`}
      >
        <motion.div
          initial={false}
          animate={{ scale: enabled ? 1.1 : 1 }}
          transition={{ type: "spring", stiffness: 400, damping: 25 }}
        >
          <Icon className="w-3.5 h-3.5" />
        </motion.div>
        Hide Exact
      </button>
    </div>
  );
}
