import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "standalone",
  async rewrites() {
    return [
      {
        source: "/:path(vector_search|text_search|fuzzy_search|similarity_search|health)",
        destination: "https://vector.korroni.cloud/:path",
      },
    ];
  },
};

export default nextConfig;
