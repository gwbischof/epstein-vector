"use client";

import { motion } from "framer-motion";
import { FileText, ExternalLink, Hash, BookOpen } from "lucide-react";
import type { TextResult } from "@/lib/types";
import { eftaUrl } from "@/lib/utils";

interface TextResultCardProps {
  result: TextResult;
  index: number;
}

export function TextResultCard({ result, index }: TextResultCardProps) {
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

      {/* Meta row: word count + rank */}
      <div className="flex items-center gap-4 mb-3 text-[11px] text-slate-500">
        <span className="flex items-center gap-1">
          <BookOpen className="w-3 h-3" />
          {result.word_count.toLocaleString()} words
        </span>
        <span className="flex items-center gap-1">
          <Hash className="w-3 h-3" />
          Rank {result.rank.toFixed(4)}
        </span>
      </div>

      {/* Headline with bold highlights rendered */}
      <div
        className="text-sm text-slate-300/90 leading-relaxed [&_b]:text-cyan-400 [&_b]:font-semibold"
        dangerouslySetInnerHTML={{ __html: result.headline }}
      />

      {/* View on DOJ link */}
      <div className="mt-3 pt-3 border-t border-slate-700/30">
        <a
          href={eftaUrl(result.efta_id, result.dataset)}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1.5 text-xs text-slate-500 hover:text-cyan-400 transition-colors"
        >
          <ExternalLink className="w-3 h-3" />
          View on DOJ EFTA
        </a>
      </div>
    </motion.div>
  );
}
