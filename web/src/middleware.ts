import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

const SECRET_PATH = process.env.SECRET_PATH;
const API_KEY = process.env.API_KEY;

const API_PATHS = [
  "/vector_search",
  "/text_search",
  "/similarity_search",
  "/get_document",
  "/health",
];

function isApiPath(pathname: string): boolean {
  return API_PATHS.some((p) => pathname === p || pathname.startsWith(p + "/"));
}

export function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;

  // API paths: rewrite to backend with API key header
  if (isApiPath(pathname)) {
    const backendUrl = new URL(pathname + request.nextUrl.search, "https://vector.korroni.cloud");
    const headers = new Headers(request.headers);
    if (API_KEY) headers.set("X-API-Key", API_KEY);
    headers.delete("host");
    return NextResponse.rewrite(backendUrl, {
      request: { headers },
    });
  }

  // If SECRET_PATH is not configured, pass everything through (dev mode)
  if (!SECRET_PATH) return NextResponse.next();

  // Secret path → rewrite to /
  if (pathname === `/${SECRET_PATH}`) {
    const url = request.nextUrl.clone();
    url.pathname = "/";
    return NextResponse.rewrite(url);
  }

  // Block direct access to / when secret path is configured
  if (pathname === "/") {
    return new NextResponse("Not Found", { status: 404 });
  }

  return NextResponse.next();
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon\\.ico).*)"],
};
