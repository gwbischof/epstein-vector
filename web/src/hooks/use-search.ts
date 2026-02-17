"use client";

import { useCallback, useRef, useState } from "react";
import { vectorSearch, textSearch } from "@/lib/api";
import type {
  SearchMode,
  VectorResult,
  TextResult,
} from "@/lib/types";

export function useSearch() {
  const [query, setQuery] = useState("");
  const [mode, setMode] = useState<SearchMode>("semantic");
  const [dataset, setDataset] = useState<number | null>(null);
  const [vectorResults, setVectorResults] = useState<VectorResult[]>([]);
  const [textResults, setTextResults] = useState<TextResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasSearched, setHasSearched] = useState(false);
  const abortRef = useRef<AbortController | null>(null);

  const search = useCallback(
    async (q: string, m: SearchMode, d: number | null) => {
      const apiKey =
        typeof window !== "undefined"
          ? localStorage.getItem("epstein-api-key") ?? ""
          : "";

      if (!q.trim()) return;
      if (!apiKey) {
        setError("Please enter an API key first");
        return;
      }

      // Abort any in-flight request
      abortRef.current?.abort();
      const controller = new AbortController();
      abortRef.current = controller;

      setLoading(true);
      setError(null);
      setHasSearched(true);

      try {
        if (m === "semantic") {
          const res = await vectorSearch(q, apiKey, 20, d, controller.signal);
          setVectorResults(res.results);
          setTextResults([]);
        } else {
          const res = await textSearch(q, apiKey, 20, d, controller.signal);
          setTextResults(res.results);
          setVectorResults([]);
        }
      } catch (err: unknown) {
        if (err instanceof DOMException && err.name === "AbortError") return;
        setError(err instanceof Error ? err.message : "Search failed");
      } finally {
        setLoading(false);
      }
    },
    [],
  );

  const executeSearch = useCallback(() => {
    search(query, mode, dataset);
  }, [search, query, mode, dataset]);

  return {
    query,
    setQuery,
    mode,
    setMode,
    dataset,
    setDataset,
    vectorResults,
    textResults,
    loading,
    error,
    hasSearched,
    executeSearch,
  };
}
