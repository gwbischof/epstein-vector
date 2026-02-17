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
  | { kind: "query"; query: string; mode: SearchMode; dataset: number | null }
  | { kind: "similar"; eftaId: string; chunkIndex: number; dataset: number | null };

export function useSearch() {
  const [query, setQuery] = useState("");
  const [mode, setMode] = useState<SearchMode>("semantic");
  const [dataset, setDataset] = useState<number | null>(null);
  const [vectorResults, setVectorResults] = useState<VectorResult[]>([]);
  const [textResults, setTextResults] = useState<TextResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasSearched, setHasSearched] = useState(false);
  const [hasMore, setHasMore] = useState(true);
  const [similarTo, setSimilarTo] = useState<string | null>(null); // display label
  const abortRef = useRef<AbortController | null>(null);
  const lastSearchRef = useRef<LastSearch | null>(null);

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

      abortRef.current?.abort();
      const controller = new AbortController();
      abortRef.current = controller;

      setLoading(true);
      setError(null);
      setHasSearched(true);
      setHasMore(true);
      setSimilarTo(null);
      lastSearchRef.current = { kind: "query", query: q, mode: m, dataset: d };

      try {
        if (m === "semantic") {
          const res = await vectorSearch(q, apiKey, PAGE_SIZE, 0, d, controller.signal);
          setVectorResults(res.results);
          setTextResults([]);
          setHasMore(res.results.length >= PAGE_SIZE);
        } else {
          const res = await textSearch(q, apiKey, PAGE_SIZE, 0, d, controller.signal);
          setTextResults(res.results);
          setVectorResults([]);
          setHasMore(res.results.length >= PAGE_SIZE);
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
      const apiKey =
        typeof window !== "undefined"
          ? localStorage.getItem("epstein-api-key") ?? ""
          : "";
      if (!apiKey) {
        setError("Please enter an API key first");
        return;
      }

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
      lastSearchRef.current = { kind: "similar", eftaId, chunkIndex, dataset };

      try {
        const res = await similarSearch(eftaId, chunkIndex, apiKey, PAGE_SIZE, 0, dataset, controller.signal);
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
    [dataset],
  );

  const loadMore = useCallback(async () => {
    const last = lastSearchRef.current;
    if (!last || loadingMore || !hasMore) return;

    const apiKey =
      typeof window !== "undefined"
        ? localStorage.getItem("epstein-api-key") ?? ""
        : "";
    if (!apiKey) return;

    const controller = new AbortController();
    setLoadingMore(true);

    try {
      if (last.kind === "similar") {
        const offset = vectorResults.length;
        const res = await similarSearch(last.eftaId, last.chunkIndex, apiKey, PAGE_SIZE, offset, last.dataset, controller.signal);
        setVectorResults((prev) => [...prev, ...res.results]);
        setHasMore(res.results.length >= PAGE_SIZE);
      } else if (last.mode === "semantic") {
        const offset = vectorResults.length;
        const res = await vectorSearch(last.query, apiKey, PAGE_SIZE, offset, last.dataset, controller.signal);
        setVectorResults((prev) => [...prev, ...res.results]);
        setHasMore(res.results.length >= PAGE_SIZE);
      } else {
        const offset = textResults.length;
        const res = await textSearch(last.query, apiKey, PAGE_SIZE, offset, last.dataset, controller.signal);
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
    loadingMore,
    error,
    hasSearched,
    hasMore,
    similarTo,
    executeSearch,
    loadMore,
    findSimilar,
  };
}
