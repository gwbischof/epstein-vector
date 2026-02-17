import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Epstein Document Search",
  description: "Semantic and keyword search over DOJ Epstein Library documents",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <head>
        <link
          href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap"
          rel="stylesheet"
        />
      </head>
      <body className="min-h-screen bg-slate-950 text-slate-200 font-sans antialiased">
        {children}
      </body>
    </html>
  );
}
