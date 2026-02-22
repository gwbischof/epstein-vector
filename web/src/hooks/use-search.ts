"use client";

import { useCallback, useRef, useState } from "react";
import { vectorSearch, textSearch, fuzzySearch, similarSearch } from "@/lib/api";
import type {
  SearchMode,
  VectorResult,
  TextResult,
  FuzzyResult,
} from "@/lib/types";

const PAGE_SIZE = 20;

type LastSearch =
  | { kind: "query"; query: string; mode: SearchMode; excludeExact: boolean }
  | { kind: "similar"; eftaId: string; chunkIndex: number };

export function useSearch() {
  const [query, setQuery] = useState("");
  const [mode, setMode] = useState<SearchMode>("semantic");
  const [vectorResults, setVectorResults] = useState<VectorResult[]>([]);
  const [textResults, setTextResults] = useState<TextResult[]>([]);
  const [fuzzyResults, setFuzzyResults] = useState<FuzzyResult[]>([]);
  const [excludeExact, setExcludeExact] = useState(false);
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
    async (q: string, m: SearchMode, exExact: boolean = false) => {
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
      setTotalResults(null);
      lastSearchRef.current = { kind: "query", query: q, mode: m, excludeExact: exExact };

      try {
        if (m === "semantic") {
          const res = await vectorSearch(q, apiKey, PAGE_SIZE, 0, controller.signal);
          setVectorResults(res.results);
          setTextResults([]);
          setFuzzyResults([]);
          setHasMore(res.results.length >= PAGE_SIZE);
          setTotalResults(null); // vector search has no total
        } else if (m === "fuzzy") {
          const res = await fuzzySearch(q, apiKey, PAGE_SIZE, 0, exExact, controller.signal);
          setFuzzyResults(res.results);
          setVectorResults([]);
          setTextResults([]);
          setHasMore(res.results.length >= PAGE_SIZE);
          setTotalResults(res.total ?? null);
        } else {
          const res = await textSearch(q, apiKey, PAGE_SIZE, 0, controller.signal);
          setTextResults(res.results);
          setVectorResults([]);
          setFuzzyResults([]);
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
      lastSearchRef.current = { kind: "similar", eftaId, chunkIndex };

      try {
        const res = await similarSearch(eftaId, chunkIndex, apiKey, PAGE_SIZE, 0, controller.signal);
        setVectorResults(res.results);
        setTextResults([]);
        setFuzzyResults([]);
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
        const res = await similarSearch(last.eftaId, last.chunkIndex, apiKey, PAGE_SIZE, offset, controller.signal);
        setVectorResults((prev) => [...prev, ...res.results]);
        setHasMore(res.results.length >= PAGE_SIZE);
      } else if (last.mode === "semantic") {
        const offset = vectorResults.length;
        const res = await vectorSearch(last.query, apiKey, PAGE_SIZE, offset, controller.signal);
        setVectorResults((prev) => [...prev, ...res.results]);
        setHasMore(res.results.length >= PAGE_SIZE);
      } else if (last.mode === "fuzzy") {
        const offset = fuzzyResults.length;
        const res = await fuzzySearch(last.query, apiKey, PAGE_SIZE, offset, last.excludeExact, controller.signal);
        setFuzzyResults((prev) => [...prev, ...res.results]);
        setHasMore(res.results.length >= PAGE_SIZE);
      } else {
        const offset = textResults.length;
        const res = await textSearch(last.query, apiKey, PAGE_SIZE, offset, controller.signal);
        setTextResults((prev) => [...prev, ...res.results]);
        setHasMore(res.results.length >= PAGE_SIZE);
      }
    } catch (err: unknown) {
      if (err instanceof DOMException && err.name === "AbortError") return;
    } finally {
      setLoadingMore(false);
    }
  }, [loadingMore, hasMore, vectorResults.length, textResults.length, fuzzyResults.length]);

  const executeSearch = useCallback(() => {
    search(query, mode, excludeExact);
  }, [search, query, mode, excludeExact]);

  return {
    query,
    setQuery,
    mode,
    setMode,
    excludeExact,
    setExcludeExact,
    vectorResults,
    textResults,
    fuzzyResults,
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
