# WatchTogether — تماشای همزمان (Metastream / WatchParty / TwoSeven clone)

**فوق سبک · ۲ نفره · سینک هر ثانیه · سافت‌ساب · پراکسی v2ray · UI خیره‌کننده**

لینک مستقیم mp4/webm/m3u8 بده → اتاق `/watch/<id>` بساز → لینک دعوت را بفرست → دو نفر همزمان با سینک هر ثانیه ببینید.

## قابلیت‌ها

- **سینک دموکراتیک:** هر دو نفر play/pause/seek می‌کنند و برای همه اعمال می‌شود. heartbeat هر ۱ ثانیه + اصلاح drift > ۰.۸s
- **صدا و زیرنویس KMPlayer-like:** فیلم را لود کن → ترک‌های **داخلی MKV/MP4** (دوبله فارسی/انگلیسی، زیرنویس‌های سافت سابِ خودِ فایل) خودکار شناسایی می‌شوند — از دکمه‌های 💬/🎧 روی تایم‌لاین یا پنل کناری انتخاب کن. فایل SRT/VTT خارجی هم به همان لیست اضافه می‌شود. (مرورگر: `video.audioTracks` و `video.textTracks` — کروم MKV را گاهی ناقص می‌دهد، روی **Firefox** یا نسخه **MP4** کامل‌تر است)
- **سافت‌ساب خارجی:** افزودن چند SRT/VTT به لیست، سایز `::cue`, تاخیر ±0.5s (فقط برای خارجی)
- **تامنیل تایم‌لاین:** هاور روی seek → فریم دقیق همان لحظه (canvas 160×90)
- **پراکسی v2ray:** تیک «پراکسی» + مودال VPN (bulk import هر خط یک کانفیگ: vless/vmess/trojan/ss/http/socks5) — ping تکی/همه + بهترین خودکار، فعال per-user per-room
- **بهینه برای ایران:** سرور فقط سیگنال sync را relay می‌کند، ویدیو p2p از CDN → کمترین پهنای باند. Range-passthrough برای seek سریع
- **UI:** لندینگ و اتاق با انیمیشن، کنترل سفارشی (play چپ، ولوم/فول‌اسکرین راست)، بدون کنترل native پلیر
- **مصرف:** ~۲۰MB RAM، تک هسته — بدون فریم‌ورک، فقط `ws` + `undici` (برای http/socks proxy)

## نصب و اجرا

### کلون

```bash
# https
git clone https://github.com/naserme/watchTogether.git
cd watchTogether

# ssh
git clone git@github.com:naserme/watchTogether.git

# gh cli
gh repo clone naserme/watchTogether
```

### 1) لوکال (اشتراک با IP:Port روی همین سیستم)

```bash
npm install
node server/server.js
# کنسول IP های LAN را چاپ می‌کند: http://192.168.1.10:3000  و  http://127.0.0.1:3000
# برای اینترنت: پورت 3000 را روی مودم فوروارد کن یا Cloudflare Tunnel بزن
```

### 2) سرور لینوکس (Node — پیشنهادی)

```bash
npm install --production
PORT=3000 HOST=0.0.0.0 node server/server.js
# systemd ( /etc/systemd/system/watchtogether.service ):
#   Environment=PORT=3000
#   ExecStart=/usr/bin/node /opt/watchTogether/server/server.js
# nginx: proxy_pass http://127.0.0.1:3000; + proxy_set_header Upgrade / Connection "upgrade" برای /ws
# docker: docker build -t watch-together . && docker run -p 3000:3000 watch-together
```

### 3) ورکر کلودفلر (Durable Object + Assets)

```bash
cd worker
npm i -g wrangler
wrangler login
wrangler deploy
# آدرس: https://watch-together.<subdomain>.workers.dev/watch/<id>
# تنظیمات در worker/wrangler.toml — Durable Object Room + [assets] directory = ../client
# لوکال ورکر: wrangler dev
```

## v2ray / پراکسی — ویدیوهای یوتیوب و لینک‌های فیلتر

1. **مودال VPN داخل اتاق:** دکمه ◈ VPN → لیست کانفیگ را پیست کن (هر خط یکی) → ایمپورت → پینگ همه / بهترین خودکار → فعال
2. تیک «عبور از پراکسی سرور» را روشن کن → ویدیو از `/api/proxy?url=...&room=&userId=` با `Range` عبور می‌کند (http/socks مستقیم via undici ProxyAgent؛ vless/vmess/trojan/ss نیاز به Xray TUN روی سرور دارد)
3. روی سرور لینوکسی: Xray/V2Ray را در حالت TUN اجرا کن یا `http_proxy` بده — fetch ورکر/سرور خودکار با کانفیگ فعال proxy می‌کند
4. یوتیوب لینک مستقیم mp4 نمی‌دهد؛ یا با yt-dlp لینک مستقیم بگیر یا پراکسی + استخراج لینک استفاده کن

## صدا

پلیر با `video.muted=false, volume=1` شروع می‌کند. اگر مرورگر autoplay را mute کرد، اولین کلیک/key/touch خودکار unmute می‌کند. دکمه 🔊 و اسلایدر ولوم هم هست.

## ساختار

```
server/server.js       — HTTP + WS + /api/proxy + /api/vpn/* (bulk + ping) + Range
client/index.html      — لندینگ
client/watch.html      — پلیر + sync دموکراتیک + sub + thumbnail + VPN modal
worker/worker.js       — Cloudflare Worker + Durable Object Room (سینک با سرور)
worker/wrangler.toml   — Durable Object + ASSETS
Dockerfile / package.json
```

## توسعه آینده

- [ ] **لیست افراد داخل اتاق — presence:** آواتار/نام هر peer، آنلاین/آفلاین، چه کسی paused/seeking است (WS: `peer-join`/`peer-leave` + heartbeat)
- [ ] چت متنی سبک (WS `chat` — UI فعلاً مخفی)
- [ ] تماس صوتی/تصویری (WebRTC — اختیاری، خاموش پیش‌فرض برای سبک ماندن)
- [ ] استخراج لینک مستقیم یوتیوب (yt-dlp / Invidious) روی سرور
- [ ] پشتیبانی کامل MKV demux با ffmpeg.wasm برای وقتی مرورگر audioTracks را نمی‌دهد

## نکات

- زیرنویس هر نفر lokal است — به طرف مقابل ارسال نمی‌شود.
- کیفیت ویدیو را خود لینک تعیین می‌کند؛ برای اینترنت ضعیف لینک کم‌حجم‌تر بده.
- ورکر کلودفلر http/socks را direct fetch می‌کند؛ برای Tunnel کامل، ورکرِ پروکسی جداگانه یا سرور Node را جلو بگذار.

---

ساخته شده با عشق برای **نهال قشنگم** ♥ — © 2026 WatchTogether
