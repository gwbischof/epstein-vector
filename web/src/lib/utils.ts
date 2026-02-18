import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

/**
 * Build DOJ EFTA document URL.
 * Pattern: https://www.justice.gov/epstein/files/DataSet%20{N}/{EFTA_ID}.pdf
 * Falls back to the EFTA search page if no dataset number is available.
 */
export function eftaUrl(eftaId: string, dataset: number | null): string {
  if (dataset != null) {
    return `https://www.justice.gov/epstein/files/DataSet%20${dataset}/${eftaId}.pdf`;
  }
  // Fallback: link to the DOJ search filtered by EFTA ID
  return `https://www.justice.gov/epstein/search?keys=${eftaId}`;
}

/**
 * Clean OCR text: collapse runs of whitespace (from multi-column PDF layouts)
 * into single spaces and trim.
 */
export function cleanText(text: string): string {
  return text.replace(/[^\S\n]+/g, " ").trim();
}

/**
 * Trigram similarity matching (replicates pg_trgm word_similarity behavior).
 * Finds the best-matching substring in `text` for `query` and returns
 * an excerpt with the match wrapped in <b> tags.
 */

function trigrams(s: string): Set<string> {
  const padded = `  ${s.toLowerCase()} `;
  const set = new Set<string>();
  for (let i = 0; i <= padded.length - 3; i++) {
    set.add(padded.slice(i, i + 3));
  }
  return set;
}

function trigramSimilarity(a: string, b: string): number {
  const ta = trigrams(a);
  const tb = trigrams(b);
  if (ta.size === 0 || tb.size === 0) return 0;
  let intersection = 0;
  for (const t of ta) {
    if (tb.has(t)) intersection++;
  }
  return intersection / (ta.size + tb.size - intersection);
}

/**
 * Split text into segments with exact query term matches marked.
 * Returns an array of { text, match } objects for React rendering.
 */
export function highlightExactTerms(
  query: string,
  text: string,
): { text: string; match: boolean }[] {
  const terms = query
    .toLowerCase()
    .split(/\s+/)
    .filter((t) => t.length >= 2);
  if (terms.length === 0) return [{ text, match: false }];

  // Build regex matching any query term as a whole word (case-insensitive)
  const escaped = terms.map((t) => t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  const re = new RegExp(`\\b(${escaped.join("|")})\\b`, "gi");

  const segments: { text: string; match: boolean }[] = [];
  let lastIndex = 0;
  let m: RegExpExecArray | null;

  while ((m = re.exec(text)) !== null) {
    if (m.index > lastIndex) {
      segments.push({ text: text.slice(lastIndex, m.index), match: false });
    }
    segments.push({ text: m[0], match: true });
    lastIndex = re.lastIndex;
  }

  if (lastIndex < text.length) {
    segments.push({ text: text.slice(lastIndex), match: false });
  }

  return segments.length > 0 ? segments : [{ text, match: false }];
}

export function highlightFuzzyMatch(
  query: string,
  text: string,
  contextWords = 25,
): { before: string; match: string; after: string } | null {
  const words = text.split(/\s+/);
  if (words.length === 0) return null;

  const qWordCount = query.split(/\s+/).length;
  let bestScore = 0;
  let bestPos = 0;
  let bestLen = qWordCount;

  const maxWlen = Math.min(qWordCount + 2, words.length);
  for (let wlen = Math.max(1, qWordCount); wlen <= maxWlen; wlen++) {
    for (let i = 0; i <= words.length - wlen; i++) {
      const candidate = words.slice(i, i + wlen).join(" ");
      const score = trigramSimilarity(query, candidate);
      if (score > bestScore) {
        bestScore = score;
        bestPos = i;
        bestLen = wlen;
      }
    }
  }

  if (bestScore < 0.05) return null;

  const start = Math.max(0, bestPos - contextWords);
  const end = Math.min(words.length, bestPos + bestLen + contextWords);

  const prefix = start > 0 ? "..." : "";
  const suffix = end < words.length ? "..." : "";

  return {
    before: prefix + (prefix && " ") + words.slice(start, bestPos).join(" "),
    match: words.slice(bestPos, bestPos + bestLen).join(" "),
    after: words.slice(bestPos + bestLen, end).join(" ") + (suffix && " ") + suffix,
  };
}
