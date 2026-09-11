#!/usr/bin/env bash
set -e
TOKEN="${CF_API_TOKEN:-YOUR_CF_API_TOKEN}"
ACCOUNT="${CF_ACCOUNT_ID:-YOUR_CF_ACCOUNT_ID}"
NAME="${WORKER_NAME:-watch-together}"
echo "Uploading worker/worker.js ..."
curl -s -X PUT "https://api.cloudflare.com/client/v4/accounts/$ACCOUNT/workers/scripts/$NAME" \
  -H "Authorization: Bearer $TOKEN" \
  -F "metadata=@worker/metadata.json;type=application/json" \
  -F "script=@worker/worker.js;type=application/javascript+module" | head -c 2000; echo
echo "Subdomain:"
curl -s "https://api.cloudflare.com/client/v4/accounts/$ACCOUNT/workers/subdomain" -H "Authorization: Bearer $TOKEN" | head -c 500; echo
echo "URL: https://$NAME.\$(curl -s https://api.cloudflare.com/client/v4/accounts/$ACCOUNT/workers/subdomain -H \"Authorization: Bearer \$TOKEN\" | grep -o '\"subdomain\":\"[^\"]*\"' | cut -d\\\" -f4).workers.dev/api/health"
echo "If 404: dash.cloudflare.com -> Workers & Pages -> $NAME -> Settings -> Domains & Routes -> Enable workers.dev"
