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
