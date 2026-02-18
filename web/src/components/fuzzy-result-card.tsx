"use client";

import { useMemo } from "react";
import { motion } from "framer-motion";
import { FileText, ExternalLink, Layers, Sparkles, Percent } from "lucide-react";
import type { FuzzyResult } from "@/lib/types";
import { eftaUrl, highlightFuzzyMatch, cleanText } from "@/lib/utils";

interface FuzzyResultCardProps {
  result: FuzzyResult;
  index: number;
  query: string;
  onFindSimilar?: (eftaId: string, chunkIndex: number) => void;
}

function similarityColor(sim: number): string {
  if (sim >= 0.6) return "bg-emerald-400";
  if (sim >= 0.4) return "bg-cyan-400";
  return "bg-amber-400";
}

export function FuzzyResultCard({ result, index, query, onFindSimilar }: FuzzyResultCardProps) {
  const pct = Math.round(result.similarity * 100);

  const cleaned = useMemo(() => cleanText(result.text), [result.text]);

  const highlight = useMemo(
    () => highlightFuzzyMatch(query, cleaned),
    [query, cleaned],
  );

  return (
    <motion.div
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.35, delay: index * 0.04 }}
      className="glass glass-hover rounded-xl p-4 relative overflow-hidden group"
    >
      {/* Top row: EFTA ID + meta */}
      <div className="flex items-start justify-between gap-3 mb-3">
        <div className="flex items-center gap-2 min-w-0">
          <FileText className="w-4 h-4 text-cyan-400/60 shrink-0" />
          <a
            href={eftaUrl(result.efta_id, result.dataset)}
            target="_blank"
            rel="noopener noreferrer"
            className="font-mono text-sm text-cyan-400 hover:text-cyan-300 transition-colors truncate flex items-center gap-1"
          >
            {result.efta_id}
            <ExternalLink className="w-3 h-3 opacity-0 group-hover:opacity-60 transition-opacity shrink-0" />
          </a>
        </div>

        <div className="flex items-center gap-2 shrink-0">
          {result.dataset != null && (
            <span className="text-[10px] font-mono uppercase tracking-wider text-violet-400/80 bg-violet-500/10 px-2 py-0.5 rounded-md border border-violet-500/15">
              DS {result.dataset}
            </span>
          )}
        </div>
      </div>

      {/* Meta row: similarity bar + chunk info */}
      <div className="flex items-center gap-4 mb-3 text-[11px] text-slate-500">
        <span className="flex items-center gap-1.5">
          <Percent className="w-3 h-3" />
          <span>{pct}% match</span>
          <div className="w-16 h-1.5 rounded-full bg-slate-700/50 overflow-hidden">
            <div
              className={`h-full rounded-full ${similarityColor(result.similarity)}`}
              style={{ width: `${pct}%` }}
            />
          </div>
        </span>
        <span className="flex items-center gap-1">
          <Layers className="w-3 h-3" />
          Chunk {result.chunk_index + 1}/{result.total_chunks}
        </span>
      </div>

      {/* Text snippet with fuzzy match highlighted */}
      {highlight ? (
        <p className="text-sm text-slate-300/90 leading-relaxed whitespace-pre-line">
          <span className="text-slate-400/70">{highlight.before} </span>
          <span className="text-cyan-300 bg-cyan-500/15 rounded px-0.5">{highlight.match}</span>
          <span className="text-slate-400/70"> {highlight.after}</span>
        </p>
      ) : (
        <p className="text-sm text-slate-300/90 leading-relaxed whitespace-pre-line line-clamp-4">
          {cleaned}
        </p>
      )}

      {/* Footer actions */}
      <div className="mt-3 pt-3 border-t border-slate-700/30 flex items-center gap-4">
        <a
          href={eftaUrl(result.efta_id, result.dataset)}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1.5 text-xs text-slate-500 hover:text-cyan-400 transition-colors"
        >
          <ExternalLink className="w-3 h-3" />
          View on DOJ EFTA
        </a>
        {onFindSimilar && (
          <button
            onClick={() => onFindSimilar(result.efta_id, result.chunk_index)}
            className="inline-flex items-center gap-1.5 text-xs text-slate-500 hover:text-violet-400 transition-colors"
          >
            <Sparkles className="w-3 h-3" />
            More like this
          </button>
        )}
      </div>
    </motion.div>
  );
}
