import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

const SECRET_PATH = process.env.SECRET_PATH;
const API_KEY = process.env.API_KEY;
const BACKEND_URL = "https://vector.korroni.cloud";

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

export async function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;

  // API paths: proxy to backend with API key
  if (isApiPath(pathname)) {
    const url = BACKEND_URL + pathname + request.nextUrl.search;
    const headers: Record<string, string> = {
      "Content-Type": request.headers.get("Content-Type") || "application/json",
    };
    if (API_KEY) headers["X-API-Key"] = API_KEY;

    const res = await fetch(url, {
      method: request.method,
      headers,
      body: request.method !== "GET" && request.method !== "HEAD" ? request.body : undefined,
      // @ts-expect-error -- Node fetch supports duplex for streaming request bodies
      duplex: "half",
    });

    return new NextResponse(res.body, {
      status: res.status,
      headers: {
        "Content-Type": res.headers.get("Content-Type") || "application/json",
      },
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
