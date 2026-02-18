"use client";

import { useMemo, useState } from "react";
import { motion } from "framer-motion";
import { FileText, ChevronDown, ChevronUp, ExternalLink, Layers, Sparkles } from "lucide-react";
import type { VectorResult } from "@/lib/types";
import { eftaUrl, highlightExactTerms, cleanText } from "@/lib/utils";

function scoreColor(score: number): string {
  if (score >= 0.5) return "bg-emerald-500";
  if (score >= 0.3) return "bg-amber-500";
  return "bg-orange-500";
}

function scoreLabel(score: number): string {
  if (score >= 0.5) return "text-emerald-400";
  if (score >= 0.3) return "text-amber-400";
  return "text-orange-400";
}

interface VectorResultCardProps {
  result: VectorResult;
  index: number;
  query: string;
  onFindSimilar?: (eftaId: string, chunkIndex: number) => void;
}

export function VectorResultCard({ result, index, query, onFindSimilar }: VectorResultCardProps) {
  const [expanded, setExpanded] = useState(false);
  const cleaned = cleanText(result.text);
  const preview = cleaned.length > 300 && !expanded ? cleaned.slice(0, 300) + "..." : cleaned;

  const segments = useMemo(
    () => highlightExactTerms(query, preview),
    [query, preview],
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

      {/* Score bar + chunk info */}
      <div className="flex items-center gap-3 mb-3">
        <div className="flex items-center gap-2 flex-1 min-w-0">
          <div className="h-1.5 flex-1 bg-slate-800 rounded-full overflow-hidden">
            <motion.div
              className={`h-full rounded-full ${scoreColor(result.score)}`}
              initial={{ width: 0 }}
              animate={{ width: `${Math.min(result.score * 100, 100)}%` }}
              transition={{ duration: 0.6, delay: index * 0.04 + 0.2 }}
            />
          </div>
          <span className={`text-xs font-mono ${scoreLabel(result.score)} shrink-0`}>
            {result.score.toFixed(3)}
          </span>
        </div>

        <div className="flex items-center gap-1 text-[10px] text-slate-500 shrink-0">
          <Layers className="w-3 h-3" />
          {result.chunk_index + 1}/{result.total_chunks}
        </div>
      </div>

      {/* Text preview with exact query terms highlighted */}
      <div className="relative">
        <p className="text-sm text-slate-300/90 leading-relaxed break-words">
          {segments.map((seg, i) =>
            seg.match ? (
              <span key={i} className="text-cyan-300 bg-cyan-500/15 rounded px-0.5">
                {seg.text}
              </span>
            ) : (
              <span key={i}>{seg.text}</span>
            ),
          )}
        </p>

        {result.text.length > 300 && (
          <button
            onClick={() => setExpanded(!expanded)}
            className="mt-2 flex items-center gap-1 text-xs text-cyan-400/70 hover:text-cyan-400 transition-colors"
          >
            {expanded ? (
              <>
                <ChevronUp className="w-3 h-3" /> Show less
              </>
            ) : (
              <>
                <ChevronDown className="w-3 h-3" /> Show more
              </>
            )}
          </button>
        )}
      </div>

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
