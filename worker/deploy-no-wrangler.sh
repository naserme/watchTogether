#!/usr/bin/env bash
# Deploy WatchTogether Worker WITHOUT wrangler — via Cloudflare REST API
# نیاز: CF_API_TOKEN + CF_ACCOUNT_ID
# ساخت توکن: https://dash.cloudflare.com/profile/api-tokens -> Create Token -> Edit Cloudflare Workers
#   Permissions: Account -> Workers Scripts:Edit, Account -> Workers KV Storage:Edit (اگر KV), Zone:Read (اختیاری)
#   یا از Account API Token با Workers:Edit استفاده کن

set -e

if [ -z "$CF_API_TOKEN" ] || [ -z "$CF_ACCOUNT_ID" ]; then
  echo "Usage: CF_API_TOKEN=xxx CF_ACCOUNT_ID=xxx bash worker/deploy-no-wrangler.sh"
  echo ""
  echo " CF_ACCOUNT_ID را از داشبورد بگیر: https://dash.cloudflare.com -> Workers & Pages -> Overview -> سمت راست Account ID"
  echo " CF_API_TOKEN  را از https://dash.cloudflare.com/profile/api-tokens بساز"
  exit 1
fi

SCRIPT_NAME="watch-together"
WORKER_FILE="worker/worker.js"

if [ ! -f "$WORKER_FILE" ]; then
  # اگر از داخل worker/ اجرا شد
  WORKER_FILE="worker.js"
fi

echo "==> Uploading $WORKER_FILE as $SCRIPT_NAME ..."
echo "    Account: $CF_ACCOUNT_ID"

# Wrangler این کار را می‌کند — بدون Wrangler با curl:
# PUT /accounts/{id}/workers/scripts/{name}
# multipart/form-data: metadata (json) + script (js) [+ assets اگر داری]

# متادیتا: بایندینگ Durable Object + compatibility_date
METADATA=$(cat <<'JSON'
{
  "main_module": "worker.js",
  "compatibility_date": "2024-12-01",
  "bindings": [
    {"type": "durable_object_namespace", "name": "ROOM", "class_name": "Room"}
  ],
  "migrations": {
    "new_classes": ["Room"],
    "tag": "v1"
  }
}
JSON
)

# نکته: Assets (../client) با این روش خودکار آپلود نمی‌شود.
# دو راه:
#  A) کلاینت را جدا روی Cloudflare Pages دیپلوی کن (پیشنهادی)
#  B) اگر می‌خواهی Worker هم استاتیک سرو کند، از Wrangler Assets استفاده کن — بدون Wrangler باید R2/KV بسازی
# اینجا گزینه A را فرض می‌کنیم: Worker فقط WS+API سرو می‌کند، Pages استاتیک را سرو می‌کند و Route را ست می‌کنی.

curl -s -X PUT \
  "https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/workers/scripts/${SCRIPT_NAME}" \
  -H "Authorization: Bearer ${CF_API_TOKEN}" \
  -F "metadata=${METADATA};type=application/json" \
  -F "script=@${WORKER_FILE};type=application/javascript+module" | jq .

echo ""
echo "==> Done. Check: https://${SCRIPT_NAME}.${CF_ACCOUNT_ID}.workers.dev  (یا دامنه کاستوم از داشبورد)"
echo ""
echo "--- گزینه A (پیشنهادی بدون Wrangler): Pages برای کلاینت ---"
echo " 1) داشبورد -> Workers & Pages -> Create -> Pages -> Upload Assets"
echo "    پوشه client/ را زیپ و آپلود کن، یا به GitHub وصل کن (repo: naserme/watchTogether, build: none, output: client/)"
echo " 2) بعد از Pages deploy، یک Route بساز که /api/* و /ws به Worker برود و بقیه به Pages:"
echo "    داشبورد -> Workers -> $SCRIPT_NAME -> Settings -> Triggers -> Add Route"
echo "    Route: watch.yourdomain.com/api/*  -> $SCRIPT_NAME"
echo "    Route: watch.yourdomain.com/ws     -> $SCRIPT_NAME"
echo "    بقیه درخواست‌ها خودکار به Pages می‌روند."
echo ""
echo "--- اگر می‌خواهی همه‌چیز فقط با Worker و بدون Pages باشد ---"
echo " باید wrangler assets را دستی به KV/R2 تبدیل کنی — توصیه نمی‌شود. از wrangler deploy استفاده کن:"
echo "   npx wrangler deploy --config worker/wrangler.toml"
