import type {
  VectorSearchResponse,
  TextSearchResponse,
} from "./types";

const BASE_URL = typeof window !== "undefined" ? window.location.origin : "";

export async function vectorSearch(
  query: string,
  apiKey: string,
  limit: number = 20,
  dataset?: number | null,
  signal?: AbortSignal,
): Promise<VectorSearchResponse> {
  const body: Record<string, unknown> = { query, limit };
  if (dataset != null) body.dataset = dataset;

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
  dataset?: number | null,
  signal?: AbortSignal,
): Promise<TextSearchResponse> {
  const body: Record<string, unknown> = { query, limit };
  if (dataset != null) body.dataset = dataset;

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
