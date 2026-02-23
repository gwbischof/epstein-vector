#!/bin/bash
API_KEY="${INGEST_API_KEY:-375314087157b8f449aaa8ce8e5743a494588c48c6c1b538cb1c9e35b5411f60}"
while true; do
  result=$(curl -s -H "X-API-Key: $API_KEY" https://vector.korroni.cloud/ingest/stats)
  docs=$(echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin)['total_docs'])")
  chunked=$(echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin)['chunked_docs'])")
  chunks=$(echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin)['chunks'])")
  echo "$(date '+%H:%M:%S') — docs: $docs | chunked: $chunked | chunks: $chunks"
  sleep 5
done
