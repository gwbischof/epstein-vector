export type SearchMode = "semantic" | "keyword" | "fuzzy";

export interface VectorResult {
  efta_id: string;
  dataset: number | null;
  chunk_index: number;
  total_chunks: number;
  text: string;
  score: number;
}

export interface VectorSearchResponse {
  query: string;
  results: VectorResult[];
}

export interface TextResult {
  efta_id: string;
  dataset: number | null;
  word_count: number;
  rank: number;
  headline: string;
}

export interface TextSearchResponse {
  query: string;
  results: TextResult[];
}

export interface FuzzyResult {
  efta_id: string;
  dataset: number | null;
  chunk_index: number;
  total_chunks: number;
  text: string;
  similarity: number;
}

export interface FuzzySearchResponse {
  query: string;
  results: FuzzyResult[];
}
