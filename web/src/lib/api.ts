import type {
  VectorSearchResponse,
  TextSearchResponse,
  FuzzySearchResponse,
} from "./types";

const BASE_URL = typeof window !== "undefined" ? window.location.origin : "";

export async function vectorSearch(
  query: string,
  apiKey: string,
  limit: number = 20,
  offset: number = 0,
  signal?: AbortSignal,
): Promise<VectorSearchResponse> {
  const body: Record<string, unknown> = { query, limit, offset };

  const res = await fetch(`${BASE_URL}/vector_search`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": apiKey,
    },
    body: JSON.stringify(body),
    signal,
  });

  if (!res.ok) {
    if (res.status === 401) throw new Error("Invalid API key");
    throw new Error(`Search failed (${res.status})`);
  }

  return res.json();
}

export async function textSearch(
  query: string,
  apiKey: string,
  limit: number = 20,
  offset: number = 0,
  signal?: AbortSignal,
): Promise<TextSearchResponse> {
  const body: Record<string, unknown> = { query, limit, offset };

  const res = await fetch(`${BASE_URL}/text_search`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": apiKey,
    },
    body: JSON.stringify(body),
    signal,
  });

  if (!res.ok) {
    if (res.status === 401) throw new Error("Invalid API key");
    throw new Error(`Search failed (${res.status})`);
  }

  return res.json();
}

export async function fuzzySearch(
  query: string,
  apiKey: string,
  limit: number = 20,
  offset: number = 0,
  excludeExact?: boolean,
  signal?: AbortSignal,
): Promise<FuzzySearchResponse> {
  const body: Record<string, unknown> = { query, limit, offset };
  if (excludeExact) body.exclude_exact = true;

  const res = await fetch(`${BASE_URL}/fuzzy_search`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": apiKey,
    },
    body: JSON.stringify(body),
    signal,
  });

  if (!res.ok) {
    if (res.status === 401) throw new Error("Invalid API key");
    throw new Error(`Search failed (${res.status})`);
  }

  return res.json();
}

export async function similarSearch(
  eftaId: string,
  chunkIndex: number,
  apiKey: string,
  limit: number = 20,
  offset: number = 0,
  signal?: AbortSignal,
): Promise<VectorSearchResponse> {
  const body: Record<string, unknown> = { efta_id: eftaId, chunk_index: chunkIndex, limit, offset };

  const res = await fetch(`${BASE_URL}/similarity_search`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": apiKey,
    },
    body: JSON.stringify(body),
    signal,
  });

  if (!res.ok) {
    if (res.status === 401) throw new Error("Invalid API key");
    throw new Error(`Similar search failed (${res.status})`);
  }

  return res.json();
}
