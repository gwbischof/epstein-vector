"use client";

import { useCallback, useRef, useState } from "react";
import { vectorSearch, textSearch, similarSearch } from "@/lib/api";
import type {
  SearchMode,
  VectorResult,
  TextResult,
} from "@/lib/types";

const PAGE_SIZE = 20;

type LastSearch =
  | { kind: "query"; query: string; mode: SearchMode }
  | { kind: "similar"; eftaId: string; chunkIndex: number };

export function useSearch() {
  const [query, setQuery] = useState("");
  const [mode, setMode] = useState<SearchMode>("semantic");
  const [vectorResults, setVectorResults] = useState<VectorResult[]>([]);
  const [textResults, setTextResults] = useState<TextResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasSearched, setHasSearched] = useState(false);
  const [hasMore, setHasMore] = useState(true);
  const [totalResults, setTotalResults] = useState<number | null>(null);
  const [similarTo, setSimilarTo] = useState<string | null>(null); // display label
  const abortRef = useRef<AbortController | null>(null);
  const lastSearchRef = useRef<LastSearch | null>(null);

  const search = useCallback(
    async (q: string, m: SearchMode) => {
      if (!q.trim()) return;

      abortRef.current?.abort();
      const controller = new AbortController();
      abortRef.current = controller;

      setLoading(true);
      setError(null);
      setHasSearched(true);
      setHasMore(true);
      setSimilarTo(null);
      setTotalResults(null);
      lastSearchRef.current = { kind: "query", query: q, mode: m };

      try {
        if (m === "semantic") {
          const res = await vectorSearch(q, PAGE_SIZE, 0, controller.signal);
          setVectorResults(res.results);
          setTextResults([]);
          setHasMore(res.results.length >= PAGE_SIZE);
          setTotalResults(null); // vector search has no total
        } else {
          const res = await textSearch(q, PAGE_SIZE, 0, controller.signal);
          setTextResults(res.results);
          setVectorResults([]);
          setHasMore(res.results.length >= PAGE_SIZE);
          setTotalResults(res.total ?? null);
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

  const findSimilar = useCallback(
    async (eftaId: string, chunkIndex: number) => {
      abortRef.current?.abort();
      const controller = new AbortController();
      abortRef.current = controller;

      setLoading(true);
      setError(null);
      setHasSearched(true);
      setHasMore(true);
      setMode("semantic");
      setQuery("");
      setSimilarTo(eftaId);
      lastSearchRef.current = { kind: "similar", eftaId, chunkIndex };

      try {
        const res = await similarSearch(eftaId, chunkIndex, PAGE_SIZE, 0, controller.signal);
        setVectorResults(res.results);
        setTextResults([]);
        setHasMore(res.results.length >= PAGE_SIZE);
      } catch (err: unknown) {
        if (err instanceof DOMException && err.name === "AbortError") return;
        setError(err instanceof Error ? err.message : "Search failed");
      } finally {
        setLoading(false);
      }
    },
    [],
  );

  const loadMore = useCallback(async () => {
    const last = lastSearchRef.current;
    if (!last || loadingMore || !hasMore) return;

    const controller = new AbortController();
    setLoadingMore(true);

    try {
      if (last.kind === "similar") {
        const offset = vectorResults.length;
        const res = await similarSearch(last.eftaId, last.chunkIndex, PAGE_SIZE, offset, controller.signal);
        setVectorResults((prev) => [...prev, ...res.results]);
        setHasMore(res.results.length >= PAGE_SIZE);
      } else if (last.mode === "semantic") {
        const offset = vectorResults.length;
        const res = await vectorSearch(last.query, PAGE_SIZE, offset, controller.signal);
        setVectorResults((prev) => [...prev, ...res.results]);
        setHasMore(res.results.length >= PAGE_SIZE);
      } else {
        const offset = textResults.length;
        const res = await textSearch(last.query, PAGE_SIZE, offset, controller.signal);
        setTextResults((prev) => [...prev, ...res.results]);
        setHasMore(res.results.length >= PAGE_SIZE);
      }
    } catch (err: unknown) {
      if (err instanceof DOMException && err.name === "AbortError") return;
    } finally {
      setLoadingMore(false);
    }
  }, [loadingMore, hasMore, vectorResults.length, textResults.length]);

  const executeSearch = useCallback(() => {
    search(query, mode);
  }, [search, query, mode]);

  return {
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
  };
}
