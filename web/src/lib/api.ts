import type {
  VectorSearchResponse,
  TextSearchResponse,
} from "./types";

const BASE_URL = typeof window !== "undefined" ? window.location.origin : "";

export async function vectorSearch(
  query: string,
  limit: number = 20,
  offset: number = 0,
  signal?: AbortSignal,
): Promise<VectorSearchResponse> {
  const body: Record<string, unknown> = { query, limit, offset };

  const res = await fetch(`${BASE_URL}/vector_search`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
    signal,
  });

  if (!res.ok) {
    throw new Error(`Search failed (${res.status})`);
  }

  return res.json();
}

export async function textSearch(
  query: string,
  limit: number = 20,
  offset: number = 0,
  signal?: AbortSignal,
): Promise<TextSearchResponse> {
  const body: Record<string, unknown> = { query, limit, offset };

  const res = await fetch(`${BASE_URL}/text_search`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
    signal,
  });

  if (!res.ok) {
    throw new Error(`Search failed (${res.status})`);
  }

  return res.json();
}

export async function getDocument(
  eftaId: string,
  signal?: AbortSignal,
): Promise<{ text: string }> {
  const res = await fetch(`${BASE_URL}/get_document/${eftaId}`, {
    signal,
  });

  if (!res.ok) {
    if (res.status === 404) throw new Error("Document not found");
    throw new Error(`Failed to fetch document (${res.status})`);
  }

  return res.json();
}

export async function similarSearch(
  eftaId: string,
  chunkIndex: number,
  limit: number = 20,
  offset: number = 0,
  signal?: AbortSignal,
): Promise<VectorSearchResponse> {
  const body: Record<string, unknown> = { efta_id: eftaId, chunk_index: chunkIndex, limit, offset };

  const res = await fetch(`${BASE_URL}/similarity_search`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
    signal,
  });

  if (!res.ok) {
    throw new Error(`Similar search failed (${res.status})`);
  }

  return res.json();
}
