"use client";

import { useState, useEffect } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Lock, Check, ChevronDown, Eye, EyeOff } from "lucide-react";

export function ApiKeyInput() {
  const [key, setKey] = useState("");
  const [saved, setSaved] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const [showKey, setShowKey] = useState(false);

  useEffect(() => {
    const stored = localStorage.getItem("epstein-api-key");
    if (stored) {
      setKey(stored);
      setSaved(true);
    }
  }, []);

  const save = () => {
    if (!key.trim()) return;
    localStorage.setItem("epstein-api-key", key.trim());
    setSaved(true);
    setExpanded(false);
  };

  const clear = () => {
    localStorage.removeItem("epstein-api-key");
    setKey("");
    setSaved(false);
    setExpanded(false);
  };

  // Collapsed state — small pill indicator
  if (saved && !expanded) {
    return (
      <motion.button
        initial={{ opacity: 0, y: -10 }}
        animate={{ opacity: 1, y: 0 }}
        onClick={() => setExpanded(true)}
        className="glass glass-hover rounded-full px-4 py-2 flex items-center gap-2 text-xs text-cyan-400 cursor-pointer group"
      >
        <Check className="w-3 h-3" />
        <span className="tracking-wide uppercase font-medium">API Key Set</span>
        <ChevronDown className="w-3 h-3 opacity-40 group-hover:opacity-100 transition-opacity" />
      </motion.button>
    );
  }

  return (
    <AnimatePresence mode="wait">
      <motion.div
        key="api-key-panel"
        initial={{ opacity: 0, y: -10, scale: 0.97 }}
        animate={{ opacity: 1, y: 0, scale: 1 }}
        exit={{ opacity: 0, y: -10, scale: 0.97 }}
        transition={{ duration: 0.25 }}
        className="glass rounded-xl p-4 w-80"
      >
        <div className="flex items-center gap-2 mb-3">
          <Lock className="w-3.5 h-3.5 text-cyan-400" />
          <span className="text-xs uppercase tracking-wider text-slate-400 font-medium">
            API Authentication
          </span>
        </div>

        <div className="relative">
          <input
            type={showKey ? "text" : "password"}
            value={key}
            onChange={(e) => setKey(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && save()}
            placeholder="Enter API key..."
            className="w-full bg-slate-950/60 border border-slate-700/50 rounded-lg px-3 py-2.5 pr-10 text-sm text-slate-200 font-mono placeholder:text-slate-600 focus:outline-none focus:border-cyan-500/40 focus:ring-1 focus:ring-cyan-500/20 transition-all"
          />
          <button
            type="button"
            onClick={() => setShowKey(!showKey)}
            className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-500 hover:text-slate-300 transition-colors"
          >
            {showKey ? <EyeOff className="w-3.5 h-3.5" /> : <Eye className="w-3.5 h-3.5" />}
          </button>
        </div>

        <div className="flex gap-2 mt-3">
          <button
            onClick={save}
            disabled={!key.trim()}
            className="flex-1 bg-cyan-500/15 hover:bg-cyan-500/25 border border-cyan-500/20 hover:border-cyan-500/40 text-cyan-400 rounded-lg px-3 py-2 text-xs font-medium uppercase tracking-wider transition-all disabled:opacity-30 disabled:cursor-not-allowed"
          >
            Save Key
          </button>
          {saved && (
            <button
              onClick={clear}
              className="bg-red-500/10 hover:bg-red-500/20 border border-red-500/20 text-red-400/70 rounded-lg px-3 py-2 text-xs font-medium uppercase tracking-wider transition-all"
            >
              Clear
            </button>
          )}
          {saved && (
            <button
              onClick={() => setExpanded(false)}
              className="bg-slate-700/30 hover:bg-slate-700/50 border border-slate-600/20 text-slate-400 rounded-lg px-3 py-2 text-xs font-medium transition-all"
            >
              Cancel
            </button>
          )}
        </div>
      </motion.div>
    </AnimatePresence>
  );
}
