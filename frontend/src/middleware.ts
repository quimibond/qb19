import { NextRequest, NextResponse } from "next/server";

/**
 * Simple auth middleware using a shared password.
 * Set AUTH_PASSWORD in .env.local to enable.
 * If not set, the dashboard is open (dev mode).
 */
export function middleware(request: NextRequest) {
  const password = process.env.AUTH_PASSWORD;
  if (!password) {
    return NextResponse.next();
  }

  // Skip auth for API routes (they have their own protection)
  if (request.nextUrl.pathname.startsWith("/api/")) {
    return NextResponse.next();
  }

  // Check for auth cookie
  const authCookie = request.cookies.get("qb_auth")?.value;
  if (authCookie === password) {
    return NextResponse.next();
  }

  // Check if this is a login attempt
  if (request.nextUrl.pathname === "/login") {
    return NextResponse.next();
  }

  // Redirect to login
  const loginUrl = new URL("/login", request.url);
  loginUrl.searchParams.set("next", request.nextUrl.pathname);
  return NextResponse.redirect(loginUrl);
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
};
