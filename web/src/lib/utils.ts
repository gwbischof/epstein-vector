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

