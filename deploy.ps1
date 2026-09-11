# Deploy without wrangler — uses Cloudflare API directly
# Usage: .\deploy.ps1  (edit token/account below or set env vars)
param(
  [string]$Token = $env:CF_API_TOKEN,
  [string]$AccountId = $env:CF_ACCOUNT_ID,
  [string]$WorkerName = "watch-together"
)
if (-not $Token) { $Token = "YOUR_CF_API_TOKEN" }
if (-not $AccountId) { $AccountId = "YOUR_CF_ACCOUNT_ID" }

$ErrorActionPreference="Stop"
Push-Location $PSScriptRoot

# 1) PUT worker script + DO binding (metadata.json has new_sqlite_classes for free plan)
Write-Host "Uploading worker/worker.js ..." -ForegroundColor Cyan
$meta = Get-Content worker/metadata.json -Raw
# curl form upload
$boundary = [Guid]::NewGuid().ToString()
$body = @"
--$boundary
Content-Disposition: form-data; name="metadata"
Content-Type: application/json

$meta
--$boundary
Content-Disposition: form-data; name="script"; filename="worker.js"
Content-Type: application/javascript+module

$(Get-Content worker/worker.js -Raw)
--$boundary--
"@
$headers = @{ Authorization = "Bearer $Token" }
try {
  $res = Invoke-RestMethod -Method Put -Uri "https://api.cloudflare.com/client/v4/accounts/$AccountId/workers/scripts/$WorkerName" -Headers $headers -ContentType "multipart/form-data; boundary=$boundary" -Body $body
  $res | ConvertTo-Json -Depth 5 | Write-Host
} catch {
  Write-Host $_.Exception.Message -ForegroundColor Red
  if ($_.ErrorDetails) { Write-Host $_.ErrorDetails.Message }
}

# 2) Check subdomain
Write-Host "`nSubdomain:" -ForegroundColor Cyan
try {
  $sd = Invoke-RestMethod -Headers $headers -Uri "https://api.cloudflare.com/client/v4/accounts/$AccountId/workers/subdomain"
  Write-Host ($sd.result.subdomain + ".workers.dev")
  Write-Host "URL: https://$WorkerName.$($sd.result.subdomain).workers.dev/api/health"
} catch { Write-Host $_.Exception.Message -ForegroundColor Yellow }

Pop-Location
Write-Host "`nIf you get 404 on workers.dev: open dash.cloudflare.com -> Workers & Pages -> $WorkerName -> Settings -> Domains & Routes -> Enable workers.dev" -ForegroundColor Yellow
