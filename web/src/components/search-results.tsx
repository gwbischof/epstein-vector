"use client";

import { useEffect, useRef } from "react";
import { AnimatePresence, motion } from "framer-motion";
import { SearchX, Radar } from "lucide-react";
import type { VectorResult, TextResult, SearchMode } from "@/lib/types";
import { VectorResultCard } from "./vector-result-card";
import { TextResultCard } from "./text-result-card";

interface SearchResultsProps {
  mode: SearchMode;
  vectorResults: VectorResult[];
  textResults: TextResult[];
  loading: boolean;
  loadingMore: boolean;
  error: string | null;
  hasSearched: boolean;
  hasMore: boolean;
  onLoadMore: () => void;
  onFindSimilar: (eftaId: string, chunkIndex: number) => void;
}

export function SearchResults({
  mode,
  vectorResults,
  textResults,
  loading,
  loadingMore,
  error,
  hasSearched,
  hasMore,
  onLoadMore,
  onFindSimilar,
}: SearchResultsProps) {
  const sentinelRef = useRef<HTMLDivElement>(null);

  // Intersection observer for infinite scroll
  useEffect(() => {
    const sentinel = sentinelRef.current;
    if (!sentinel) return;

    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting && hasMore && !loading && !loadingMore) {
          onLoadMore();
        }
      },
      { rootMargin: "200px" },
    );

    observer.observe(sentinel);
    return () => observer.disconnect();
  }, [hasMore, loading, loadingMore, onLoadMore]);

  if (error) {
    return (
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        className="glass rounded-xl p-8 text-center"
      >
        <div className="w-10 h-10 rounded-full bg-red-500/10 border border-red-500/20 flex items-center justify-center mx-auto mb-3">
          <SearchX className="w-5 h-5 text-red-400" />
        </div>
        <p className="text-sm text-red-400/90">{error}</p>
      </motion.div>
    );
  }

  if (loading) {
    return (
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        className="flex flex-col items-center justify-center py-16 gap-4"
      >
        <div className="relative w-12 h-12">
          <div className="absolute inset-0 rounded-full border-2 border-cyan-500/20" />
          <div className="absolute inset-0 rounded-full border-2 border-transparent border-t-cyan-400 spin-slow" />
          <Radar className="absolute inset-0 m-auto w-5 h-5 text-cyan-400/50 pulse-dot" />
        </div>
        <p className="text-xs text-slate-500 uppercase tracking-widest">
          {mode === "semantic" ? "Computing embeddings..." : "Searching documents..."}
        </p>
      </motion.div>
    );
  }

  const results = mode === "semantic" ? vectorResults : textResults;

  if (hasSearched && results.length === 0) {
    return (
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        className="glass rounded-xl p-8 text-center"
      >
        <div className="w-10 h-10 rounded-full bg-slate-800 border border-slate-700/50 flex items-center justify-center mx-auto mb-3">
          <SearchX className="w-5 h-5 text-slate-500" />
        </div>
        <p className="text-sm text-slate-400">No documents found</p>
        <p className="text-xs text-slate-600 mt-1">Try a different query or search mode</p>
      </motion.div>
    );
  }

  return (
    <AnimatePresence mode="wait">
      <motion.div
        key={`${mode}-results`}
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        className="grid gap-3"
      >
        {mode === "semantic"
          ? vectorResults.map((r, i) => (
              <VectorResultCard key={`${r.efta_id}-${r.chunk_index}`} result={r} index={i} onFindSimilar={onFindSimilar} />
            ))
          : textResults.map((r, i) => (
              <TextResultCard key={r.efta_id} result={r} index={i} onFindSimilar={onFindSimilar} />
            ))}

        {/* Sentinel for infinite scroll */}
        <div ref={sentinelRef} className="h-1" />

        {/* Loading more indicator */}
        {loadingMore && (
          <div className="flex justify-center py-6">
            <div className="flex items-center gap-3">
              <div className="w-5 h-5 border-2 border-cyan-400/30 border-t-cyan-400 rounded-full spin-slow" />
              <span className="text-xs text-slate-500 uppercase tracking-widest">Loading more...</span>
            </div>
          </div>
        )}

        {/* End of results */}
        {!hasMore && results.length > 0 && (
          <motion.p
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            className="text-center text-[10px] text-slate-600 uppercase tracking-widest py-4"
          >
            {results.length} result{results.length !== 1 ? "s" : ""} &middot; End of results
          </motion.p>
        )}
      </motion.div>
    </AnimatePresence>
  );
}
