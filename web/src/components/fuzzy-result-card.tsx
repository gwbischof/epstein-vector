"use client";

import { motion } from "framer-motion";
import { FileText, ExternalLink, BookOpen, Sparkles, Percent } from "lucide-react";
import type { FuzzyResult } from "@/lib/types";
import { eftaUrl } from "@/lib/utils";

interface FuzzyResultCardProps {
  result: FuzzyResult;
  index: number;
  onFindSimilar?: (eftaId: string, chunkIndex: number) => void;
}

function similarityColor(sim: number): string {
  if (sim >= 0.6) return "bg-emerald-400";
  if (sim >= 0.4) return "bg-cyan-400";
  return "bg-amber-400";
}

export function FuzzyResultCard({ result, index, onFindSimilar }: FuzzyResultCardProps) {
  const pct = Math.round(result.similarity * 100);

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

      {/* Meta row: similarity bar + word count */}
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
          <BookOpen className="w-3 h-3" />
          {result.word_count.toLocaleString()} words
        </span>
      </div>

      {/* Headline with bold highlights rendered */}
      <div
        className="text-sm text-slate-300/90 leading-relaxed [&_b]:text-cyan-400 [&_b]:font-semibold"
        dangerouslySetInnerHTML={{ __html: result.headline }}
      />

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
            onClick={() => onFindSimilar(result.efta_id, 0)}
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
