"use client";

import { motion, AnimatePresence } from "framer-motion";
import { Sparkles, Brain, Type } from "lucide-react";
import { Starfield } from "@/components/starfield";
import { ApiKeyInput } from "@/components/api-key-input";
import { SearchBar } from "@/components/search-bar";
import { SearchModeToggle } from "@/components/search-mode-toggle";
import { SearchResults } from "@/components/search-results";
import { useSearch } from "@/hooks/use-search";

const EXAMPLE_QUERIES = [
  { label: "flight logs", desc: "plain search" },
  { label: "maxw*", desc: "wildcard" },
  { label: '"wire transfer"', desc: "exact phrase" },
  { label: "Maxwell OR Brunel", desc: "OR" },
  { label: "island -vacation", desc: "NOT" },
  { label: "payments to politicians", desc: "semantic" },
];

export default function SearchPage() {
  const {
    query,
    setQuery,
    mode,
    setMode,
    vectorResults,
    textResults,
    loading,
    loadingMore,
    error,
    hasSearched,
    hasMore,
    totalResults,
    similarTo,
    executeSearch,
    loadMore,
    findSimilar,
  } = useSearch();

  const hasResults = vectorResults.length > 0 || textResults.length > 0 || hasSearched;

  return (
    <div className="relative min-h-screen noise">
      <Starfield />

      {/* Subtle radial gradient overlay */}
      <div
        className="fixed inset-0 z-0 pointer-events-none"
        style={{
          background:
            "radial-gradient(ellipse 80% 60% at 50% 0%, rgba(6,182,212,0.03) 0%, transparent 60%), radial-gradient(ellipse 60% 40% at 80% 100%, rgba(139,92,246,0.02) 0%, transparent 50%)",
        }}
      />

      <div className="relative z-10 min-h-screen flex flex-col">
        {/* Header bar */}
        <header className="flex items-center justify-between px-6 py-4">
          <motion.div
            initial={{ opacity: 0, x: -10 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.5 }}
            className="flex items-center gap-2"
          >
            <div className="w-2 h-2 rounded-full bg-cyan-400 pulse-dot" />
            <span className="text-[10px] uppercase tracking-[0.25em] text-slate-500 font-medium">
              DOJ EFTA Document Search
            </span>
          </motion.div>

          <motion.div
            initial={{ opacity: 0, x: 10 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.5, delay: 0.1 }}
          >
            <ApiKeyInput />
          </motion.div>
        </header>

        {/* Main content area */}
        <main className="flex-1 flex flex-col items-center px-6">
          <AnimatePresence mode="wait">
            {!hasResults ? (
              /* Hero / initial state */
              <motion.div
                key="hero"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0, y: -30 }}
                transition={{ duration: 0.4 }}
                className="flex-1 flex flex-col items-center justify-center max-w-2xl w-full -mt-16"
              >
                {/* Title */}
                <motion.div
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.6, delay: 0.2 }}
                  className="text-center mb-10"
                >
                  <h1 className="text-3xl sm:text-4xl font-bold tracking-tight text-slate-100 mb-3">
                    <span className="text-cyan-400">Epstein</span> Document Search
                  </h1>
                  <p className="text-sm text-slate-500 max-w-md mx-auto leading-relaxed">
                    Semantic and keyword search across the DOJ Epstein File Transfer Archive.
                    Search by meaning or exact terms across all released datasets.
                  </p>
                </motion.div>

                {/* Controls row */}
                <motion.div
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.6, delay: 0.35 }}
                  className="flex items-center justify-center gap-3 mb-4"
                >
                  <SearchModeToggle mode={mode} onChange={setMode} />
                </motion.div>

                {/* Search bar */}
                <motion.div
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.6, delay: 0.45 }}
                  className="w-full mb-10"
                >
                  <SearchBar
                    query={query}
                    onQueryChange={setQuery}
                    onSearch={executeSearch}
                    loading={loading}
                  />
                </motion.div>

                {/* Example query chips */}
                <motion.div
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  transition={{ duration: 0.6, delay: 0.6 }}
                  className="flex flex-col items-center gap-3"
                >
                  <div className="flex items-center gap-1.5 text-[10px] text-slate-600 uppercase tracking-widest">
                    <Sparkles className="w-3 h-3" />
                    Try a search
                  </div>
                  <div className="flex flex-wrap justify-center gap-2">
                    {EXAMPLE_QUERIES.map((eq) => (
                      <button
                        key={eq.label}
                        onClick={() => setQuery(eq.label)}
                        className="glass glass-hover rounded-lg px-3 py-1.5 text-xs text-slate-400 hover:text-cyan-400 transition-colors flex items-center gap-1.5"
                      >
                        <span className="text-slate-300">{eq.label}</span>
                        <span className="text-[9px] text-slate-600 uppercase tracking-wider">({eq.desc})</span>
                      </button>
                    ))}
                  </div>
                </motion.div>

                {/* Search tips */}
                <motion.div
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  transition={{ duration: 0.8, delay: 0.9 }}
                  className="mt-10 w-full grid grid-cols-1 sm:grid-cols-2 gap-3"
                >
                  <div className="glass rounded-xl p-4">
                    <div className="flex items-center gap-2 mb-2.5">
                      <Brain className="w-3.5 h-3.5 text-cyan-400/70" />
                      <span className="text-[10px] uppercase tracking-[0.2em] text-cyan-400/70 font-medium">Semantic</span>
                    </div>
                    <p className="text-[11px] text-slate-500 leading-relaxed">
                      Searches by <span className="text-slate-400">meaning</span>, not exact words. Good for concepts and topics
                      &mdash; <span className="text-slate-400 font-mono text-[10px]">payments to politicians</span> finds
                      documents about donations, wire transfers, and lobbying even if those exact words aren&apos;t used.
                    </p>
                  </div>
                  <div className="glass rounded-xl p-4">
                    <div className="flex items-center gap-2 mb-2.5">
                      <Type className="w-3.5 h-3.5 text-violet-400/70" />
                      <span className="text-[10px] uppercase tracking-[0.2em] text-violet-400/70 font-medium">Keyword</span>
                    </div>
                    <p className="text-[11px] text-slate-500 leading-relaxed">
                      Exact term matching with operators.
                      Use <span className="text-slate-400 font-mono text-[10px]">&quot;quotes&quot;</span> for phrases,
                      {" "}<span className="text-slate-400 font-mono text-[10px]">*</span> for wildcards,
                      {" "}<span className="text-slate-400 font-mono text-[10px]">OR</span> between terms,
                      and <span className="text-slate-400 font-mono text-[10px]">-</span> to exclude.
                    </p>
                  </div>
                </motion.div>
              </motion.div>
            ) : (
              /* Results state */
              <motion.div
                key="results"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                className="w-full max-w-3xl"
              >
                {/* Controls */}
                <div className="flex items-center justify-center gap-3 mb-3 mt-2">
                  <SearchModeToggle mode={mode} onChange={setMode} />
                </div>

                {/* Compact search bar */}
                <motion.div
                  initial={{ opacity: 0, y: -10 }}
                  animate={{ opacity: 1, y: 0 }}
                  className="mb-4"
                >
                  <SearchBar
                    query={query}
                    onQueryChange={setQuery}
                    onSearch={executeSearch}
                    loading={loading}
                  />
                </motion.div>

                {/* Result count */}
                {hasSearched && !loading && (vectorResults.length > 0 || textResults.length > 0) && (
                  <div className="flex items-center gap-2 mb-4">
                    <div className="w-1 h-1 rounded-full bg-cyan-500/50" />
                    <span className="text-[10px] uppercase tracking-widest text-slate-500 font-mono">
                      {totalResults != null
                        ? `${totalResults.toLocaleString()} results`
                        : `${(mode === "semantic" ? vectorResults.length : textResults.length).toLocaleString()} results${hasMore ? "+" : ""}`
                      }
                    </span>
                  </div>
                )}

                {/* Similar-to indicator */}
                {similarTo && (
                  <motion.div
                    initial={{ opacity: 0, y: -5 }}
                    animate={{ opacity: 1, y: 0 }}
                    className="glass rounded-lg px-4 py-2 mb-4 flex items-center gap-2 text-xs"
                  >
                    <Sparkles className="w-3.5 h-3.5 text-violet-400" />
                    <span className="text-slate-400">Showing documents similar to</span>
                    <span className="font-mono text-cyan-400">{similarTo}</span>
                  </motion.div>
                )}

                {/* Results */}
                <SearchResults
                  mode={mode}
                  query={query}
                  vectorResults={vectorResults}
                  textResults={textResults}
                  loading={loading}
                  loadingMore={loadingMore}
                  error={error}
                  hasSearched={hasSearched}
                  hasMore={hasMore}
                  onLoadMore={loadMore}
                  onFindSimilar={findSimilar}
                />
              </motion.div>
            )}
          </AnimatePresence>
        </main>

        {/* Footer */}
        <footer className="px-6 py-4 text-center">
          <p className="text-[10px] text-slate-700 uppercase tracking-widest">
            Epstein File Transfer Archive &middot; DOJ Public Records
          </p>
        </footer>
      </div>
    </div>
  );
}
