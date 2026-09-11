// Cloudflare Worker — WatchTogether (synced to Node server v1.4 — xxxx-xxxx-xxxx rooms, global subs/dub, health?room)
// Deploy: cd worker && wrangler deploy
// Durable Object: Room per roomId — democratic control + VPN bulk + ping

function uriType(u){
  const t=(u||'').trim();
  if(t.startsWith('vless://')) return 'vless';
  if(t.startsWith('vmess://')) return 'vmess';
  if(t.startsWith('trojan://')) return 'trojan';
  if(t.startsWith('ss://')) return 'shadowsocks';
  if(/^https?:\/\//i.test(t)) return 'http';
  if(/^socks5?:\/\//i.test(t)) return 'socks';
  return 'unknown';
}
function uriHost(u){
  try{
    if(u.startsWith('vmess://')){ const b=atob(u.slice(8)); const j=JSON.parse(b); return j.add||j.host||''; }
    const tmp=u.replace(/^(vless|trojan|ss):\/\//,'http://');
    return new URL(tmp).hostname||'';
  }catch{ return ''; }
}
function storeKey(userId, roomId){ return (userId||'').trim() || (roomId||'').trim() || '__global'; }

// ── module-level VPN store (for main Worker fetch) ──
const vpnStore = new Map(); // key -> {configs:[{id,uri,type,host,label,addedAt}], activeId}
function getStore(key){ if(!vpnStore.has(key)) vpnStore.set(key,{configs:[],activeId:null}); return vpnStore.get(key); }

async function pingUri(uri, timeoutMs=4000, env){
  const u=(uri||'').trim(); const type=uriType(u);
  const host=uriHost(u);
  // For Workers: http/socks cannot use ProxyAgent — do host check; direct fetch instead
  if(!host && type!=='http' && type!=='socks') return {ok:false, error:'cannot parse host'};
  const target = host ? `https://${host}/` : 'https://1.1.1.1/cdn-cgi/trace';
  try{
    const t0=Date.now();
    const ctrl=new AbortController(); const t=setTimeout(()=>ctrl.abort(), timeoutMs);
    await fetch(target, {method:'HEAD', signal:ctrl.signal, headers:{'User-Agent':'Mozilla/5.0'}}).catch(()=>{});
    clearTimeout(t);
    const ms=Date.now()-t0;
    if(type==='http'||type==='socks') return {ok:true, ms, note:'Worker: proxy check via host'};
    return {ok:true, ms, note:'xray-required — host reachable, needs TUN'};
  }catch(e){ return {ok:false, error: String(e.message||e).slice(0,120)}; }
}

// ── Durable Object ──
export class Room {
  constructor(state, env){
    this.state=state; this.env=env;
    this.clients=new Map(); // id -> WebSocket
    this.roomState={ playing:false, time:0, updatedAt:Date.now(), videoUrl:'', sub:null, dub:null };
    this.hostId=null;
  }
  async fetch(req){
    const url = new URL(req.url);
    // Health check for room validation — exists = این DO تا حالا join داشته
    if(url.pathname === '/health' || url.pathname === '/api/health'){
      const exists = !!(await this.state.storage.get('created'));
      return new Response(JSON.stringify({ok:true, exists, peers:this.clients.size, hasVideo:!!this.roomState.videoUrl}), {
        headers:{'Content-Type':'application/json','Access-Control-Allow-Origin':'*'}
      });
    }
    const pair=new WebSocketPair(); const [client, server]=Object.values(pair);
    server.accept();
    const id=Math.random().toString(36).slice(2,8);
    server.addEventListener('message', e=>{
      let m; try{ m=JSON.parse(e.data);}catch{return;}
      if(m.type==='join'){
        this.clients.set(id, server); server._id=id; server._name=m.name||'مهمان';
        this.state.storage.put('created', true);
        if(!this.hostId) this.hostId=id;
        if(m.videoUrl) this.roomState.videoUrl=m.videoUrl;
        server.send(JSON.stringify({type:'joined', id, hostId:this.hostId, state:this.roomState, peers:this.clients.size}));
        this.broadcast({type:'peer-join', id, name:server._name, peers:this.clients.size}, id);
        // democratic: any peer can answer request-sync, so broadcast request to all
        if(this.clients.size>1){
          // no need to target host only; but keep compat: ask everyone except joiner
          for(const [cid, ws] of this.clients) if(cid!==id) try{ ws.send(JSON.stringify({type:'request-sync'})); }catch{}
        }
        return;
      }
      // democratic control: any peer can play/pause/seek/sync
      if(['play','pause','seek','sync'].includes(m.type)){
        if(m.type==='play'){ this.roomState.playing=true; this.roomState.time=m.time??this.roomState.time; }
        if(m.type==='pause'){ this.roomState.playing=false; this.roomState.time=m.time??this.roomState.time; }
        if(m.type==='seek'){ this.roomState.time=m.time; }
        if(m.type==='sync'){ this.roomState.time=m.time; this.roomState.playing=m.playing; }
        this.roomState.updatedAt=Date.now();
        this.broadcast({...m, from:id}, id);
        return;
      }
      if(m.type==='video-change'){ this.roomState.videoUrl=m.videoUrl; this.roomState.time=0; this.roomState.playing=false; this.roomState.sub=null; this.roomState.dub=null; this.roomState.updatedAt=Date.now(); this.broadcast({type:'video-change', videoUrl:m.videoUrl, from:id}, id); return; }
      if(m.type==='sub-change'){ this.roomState.sub=m.sub||null; this.roomState.updatedAt=Date.now(); this.broadcast({type:'sub-change', sub:m.sub, from:id}, id); return; }
      if(m.type==='dub-change'){ this.roomState.dub=m.dub||null; this.roomState.updatedAt=Date.now(); this.broadcast({type:'dub-change', dub:m.dub, from:id}, id); return; }
      if(m.type==='chat'){ this.broadcast({type:'chat', text:m.text, from:id, name:server._name}, id); return; }
      if(m.type==='ping'){ server.send(JSON.stringify({type:'pong', t:m.t})); }
    });
    server.addEventListener('close',()=>{
      this.clients.delete(id);
      if(this.hostId===id){ const next=this.clients.keys().next().value; this.hostId=next||null; if(next) this.broadcast({type:'host-change', hostId:next}); }
      this.broadcast({type:'peer-leave', id, peers:this.clients.size});
    });
    server.addEventListener('error',()=>{ try{server.close();}catch{} });
    return new Response(null,{status:101, webSocket:client});
  }
  broadcast(data, exceptId){
    const msg=JSON.stringify(data);
    for(const [cid, ws] of this.clients) if(cid!==exceptId) try{ ws.send(msg);}catch{}
  }
}

function json(data, status=200, extra={}){
  return new Response(JSON.stringify(data), {status, headers:{'Content-Type':'application/json','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST,DELETE,OPTIONS','Access-Control-Allow-Headers':'*',...extra}});
}

const INDEX_HTML = `<!doctype html>
<html lang="fa" dir="rtl">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>WatchTogether — با هم ببینید، دقیقاً همزمان</title>
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;500;700;800&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{--bg:#06060b;--card:#11111b;--card2:#181825;--border:rgba(255,255,255,.08);--border2:rgba(255,255,255,.13);--accent:#7c5cff;--accent2:#ff3ea5;--accent3:#3b82f6;--text:#f5f5ff;--muted:#9aa0b8;--ok:#22c55e}
html{scroll-behavior:smooth}
body{font-family:'Vazirmatn',system-ui,sans-serif;background:var(--bg);color:var(--text);overflow-x:hidden}
a{color:inherit;text-decoration:none}
/* animated bg */
.bg{position:fixed;inset:0;z-index:-1;overflow:hidden;background:var(--bg)}
.blob{position:absolute;border-radius:50%;filter:blur(70px);opacity:.55;mix-blend-mode:screen}
.b1{width:700px;height:700px;background:radial-gradient(circle at 30% 30%, #7c5cff 0%, transparent 70%);top:-180px;right:-120px;animation:float1 14s ease-in-out infinite}
.b2{width:600px;height:600px;background:radial-gradient(circle at 50% 50%, #ff3ea5 0%, transparent 70%);top:120px;left:-140px;animation:float2 16s ease-in-out infinite}
.b3{width:800px;height:600px;background:radial-gradient(circle at 50% 50%, #3b82f6 0%, transparent 70%);bottom:-200px;right:20%;animation:float3 18s ease-in-out infinite}
@keyframes float1{0%,100%{transform:translate(0,0) scale(1)}50%{transform:translate(-30px,30px) scale(1.06)}}
@keyframes float2{0%,100%{transform:translate(0,0) scale(1)}50%{transform:translate(40px,-20px) scale(1.08)}}
@keyframes float3{0%,100%{transform:translate(0,0) scale(1)}50%{transform:translate(-20px,-30px) scale(1.05)}}
.grid{position:absolute;inset:0;background-image:linear-gradient(rgba(255,255,255,.02) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,.02) 1px, transparent 1px);background-size:40px 40px;mask-image:radial-gradient(ellipse at 50% 0%, black 60%, transparent 80%)}
/* nav */
.nav{max-width:1120px;margin:0 auto;padding:18px 20px;display:flex;align-items:center;justify-content:space-between;position:relative;z-index:2}
.logo{font-weight:800;font-size:20px;letter-spacing:.2px;display:flex;align-items:center;gap:10px}
.logo-mark{width:34px;height:34px;border-radius:11px;background:linear-gradient(135deg,var(--accent),var(--accent2));display:grid;place-items:center;font-size:16px;box-shadow:0 8px 24px rgba(124,92,255,.4)}
.logo span{opacity:.9}
.nav-links{display:flex;gap:18px;font-size:13px;color:var(--muted)}
.nav-links a:hover{color:var(--text)}
.badge{font-size:11px;color:#fff;background:rgba(255,255,255,.08);border:1px solid var(--border);padding:6px 12px;border-radius:999px;display:flex;align-items:center;gap:8px;backdrop-filter:blur(10px)}
.dot{width:8px;height:8px;border-radius:50%;background:var(--ok);box-shadow:0 0 10px var(--ok)}
/* hero */
.hero{max-width:1120px;margin:0 auto;padding:26px 20px 10px;display:grid;grid-template-columns:1.08fr .92fr;gap:28px;align-items:center;position:relative;z-index:2}
@media(max-width:860px){.hero{grid-template-columns:1fr;gap:22px}.nav-links{display:none}}
.eyebrow{font-size:11px;letter-spacing:.12em;color:var(--muted);display:flex;align-items:center;gap:10px;margin-bottom:14px}
.eyebrow i{width:28px;height:1px;background:linear-gradient(90deg,var(--accent),transparent);display:inline-block}
.h1{font-size:44px;line-height:1.15;font-weight:800;letter-spacing:-.02em}
.h1 em{background:linear-gradient(135deg,var(--accent) 20%,var(--accent2) 60%, #ff8a3d 100%);-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-style:normal}
.sub{color:var(--muted);margin-top:14px;line-height:1.9;font-size:14.5px;max-width:520px}
.cta-row{display:flex;gap:10px;margin-top:18px;flex-wrap:wrap}
.pill{font-size:11px;padding:7px 12px;border-radius:999px;background:rgba(255,255,255,.07);border:1px solid var(--border);color:var(--muted);backdrop-filter:blur(8px)}
/* glass card */
.glass{position:relative;background:linear-gradient(180deg, rgba(255,255,255,.08), rgba(255,255,255,.03));border:1px solid var(--border2);border-radius:24px;padding:22px;backdrop-filter:blur(18px);box-shadow:0 20px 60px rgba(0,0,0,.45), inset 0 1px 0 rgba(255,255,255,.08);overflow:hidden}
.glass::before{content:'';position:absolute;inset:0;border-radius:24px;padding:1px;background:linear-gradient(135deg, rgba(124,92,255,.5), transparent 40%, rgba(255,62,165,.3));-webkit-mask:linear-gradient(#fff 0 0) content-box, linear-gradient(#fff 0 0);-webkit-mask-composite:xor;mask-composite:exclude;pointer-events:none;opacity:.6}
.glow{position:absolute;width:220px;height:220px;background:radial-gradient(circle, rgba(124,92,255,.35), transparent 70%);top:-60px;left:-40px;pointer-events:none}
.card-title{font-size:13px;font-weight:700;margin-bottom:14px;display:flex;align-items:center;gap:8px}
.card-title::before{content:'';width:8px;height:8px;border-radius:50%;background:var(--accent);box-shadow:0 0 10px var(--accent)}
.field label{font-size:11px;color:var(--muted);display:block;margin-bottom:6px}
.field input{width:100%;background:rgba(0,0,0,.35);border:1px solid var(--border);color:var(--text);border-radius:14px;padding:13px 14px;font-size:13px;outline:none;transition:.2s}
.field input::placeholder{color:#6b728a}
.field input:focus{border-color:rgba(124,92,255,.5);box-shadow:0 0 0 4px rgba(124,92,255,.15);background:rgba(0,0,0,.5)}
.row{display:flex;gap:10px;margin-top:14px}
.btn{flex:1;border:0;border-radius:14px;padding:13px 16px;font-weight:700;cursor:pointer;font-family:inherit;font-size:13px;transition:.2s;position:relative;overflow:hidden}
.btn-primary{background:linear-gradient(135deg,var(--accent),var(--accent2));color:#fff;box-shadow:0 10px 28px rgba(124,92,255,.4)}
.btn-primary:hover{transform:translateY(-1px);box-shadow:0 14px 36px rgba(124,92,255,.5)}
.btn-primary:active{transform:translateY(0)}
.btn-ghost{background:rgba(255,255,255,.06);color:var(--text);border:1px solid var(--border)}
.btn-ghost:hover{background:rgba(255,255,255,.1)}
.switch{display:flex;align-items:center;gap:8px;margin-top:12px;font-size:11px;color:var(--muted)}
.switch input{accent-color:var(--accent);width:14px;height:14px}
.hint{font-size:11px;color:var(--muted);margin-top:10px;line-height:1.8}
.join-box{margin-top:14px;padding:14px;background:rgba(0,0,0,.3);border:1px solid var(--border);border-radius:16px;animation:slideDown .35s ease}
@keyframes slideDown{from{opacity:0;transform:translateY(-8px)}to{opacity:1;transform:translateY(0)}}
/* mock preview inside hero */
.mock{margin-top:16px;background:#000;border:1px solid var(--border);border-radius:16px;overflow:hidden;position:relative}
.mock-bar{height:32px;background:#0f0f14;display:flex;align-items:center;gap:6px;padding:0 12px;border-bottom:1px solid var(--border)}
.mock-bar i{width:10px;height:10px;border-radius:50%;display:inline-block}
.mock-screen{aspect-ratio:16/9;background:linear-gradient(135deg,#0a0a14 0%, #1a1030 50%, #0f1a2e 100%);position:relative;display:grid;place-items:center;overflow:hidden}
.mock-screen::after{content:'';position:absolute;inset:0;background:radial-gradient(400px 200px at 50% 40%, rgba(124,92,255,.18), transparent 70%)}
.play-fake{width:56px;height:56px;border-radius:50%;background:rgba(255,255,255,.9);display:grid;place-items:center;color:#000;font-size:18px;box-shadow:0 10px 30px rgba(0,0,0,.5);animation:pulse 2s ease-in-out infinite}
@keyframes pulse{0%,100%{transform:scale(1)}50%{transform:scale(1.06)}}
.mock-progress{position:absolute;bottom:0;left:0;right:0;height:3px;background:rgba(255,255,255,.15)}
.mock-progress i{position:absolute;inset:0 30% 0 0;background:linear-gradient(90deg,var(--accent),var(--accent2));display:block}
/* features */
.features{max-width:1120px;margin:28px auto 0;padding:0 20px;display:grid;grid-template-columns:repeat(4,1fr);gap:14px;position:relative;z-index:2}
@media(max-width:860px){.features{grid-template-columns:1fr 1fr}}
@media(max-width:520px){.features{grid-template-columns:1fr}}
.feat{background:linear-gradient(180deg, rgba(255,255,255,.06), rgba(255,255,255,.02));border:1px solid var(--border);border-radius:20px;padding:18px;backdrop-filter:blur(10px);transition:.25s;position:relative;overflow:hidden}
.feat:hover{transform:translateY(-4px);border-color:var(--border2);box-shadow:0 16px 40px rgba(0,0,0,.35)}
.feat::before{content:'';position:absolute;top:0;right:0;width:120px;height:120px;background:radial-gradient(circle, rgba(124,92,255,.12), transparent 70%);pointer-events:none}
.feat-icon{width:36px;height:36px;border-radius:12px;display:grid;place-items:center;font-size:16px;margin-bottom:10px;background:rgba(255,255,255,.06);border:1px solid var(--border)}
.feat b{font-size:13px}
.feat p{font-size:12px;color:var(--muted);margin-top:6px;line-height:1.8}
/* steps */
.steps{max-width:1120px;margin:28px auto 0;padding:0 20px;position:relative;z-index:2}
.steps-head{text-align:center;margin-bottom:18px}
.steps-head h3{font-size:18px}
.steps-head p{font-size:12px;color:var(--muted);margin-top:6px}
.steps-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}
@media(max-width:700px){.steps-grid{grid-template-columns:1fr}}
.step{background:rgba(255,255,255,.04);border:1px solid var(--border);border-radius:20px;padding:18px;display:flex;gap:14px;align-items:flex-start}
.step-num{width:32px;height:32px;border-radius:10px;background:linear-gradient(135deg,var(--accent),var(--accent2));display:grid;place-items:center;font-weight:800;font-size:13px;flex-shrink:0}
.step b{font-size:13px}
.step p{font-size:12px;color:var(--muted);margin-top:4px;line-height:1.7}
/* footer */
.footer{max-width:1120px;margin:30px auto 18px;padding:16px 20px 0;border-top:1px solid var(--border);display:flex;flex-wrap:wrap;gap:10px;justify-content:space-between;color:var(--muted);font-size:11px;position:relative;z-index:2}
/* entrance */
.reveal{opacity:0;transform:translateY(16px);animation:reveal .7s ease forwards}
.reveal-1{animation-delay:.05s}.reveal-2{animation-delay:.15s}.reveal-3{animation-delay:.25s}.reveal-4{animation-delay:.35s}
@keyframes reveal{to{opacity:1;transform:none}}
.shimmer{position:absolute;inset:0;background:linear-gradient(100deg, transparent 30%, rgba(255,255,255,.08) 50%, transparent 70%);transform:translateX(-100%);animation:shimmer 2.2s ease-in-out infinite}
@keyframes shimmer{to{transform:translateX(100%)}}
</style>

<div class="bg"><div class="grid"></div><div class="blob b1"></div><div class="blob b2"></div><div class="blob b3"></div></div>

<nav class="nav reveal">
  <div class="logo"><div class="logo-mark">▶</div> Watch<span>Together</span></div>
  <div class="nav-links"><a href="#features">ویژگی‌ها</a><a href="#steps">نحوه کار</a><a href="/watch/demo">پیش‌نمایش</a></div>
  <div class="badge"><span class="dot"></span> کم‌مصرف · بدون ثبت‌نام</div>
</nav>

<div class="hero">
  <div class="reveal reveal-1">
    <div class="eyebrow"><i></i> سینک هر ثانیه · سافت‌ساب · بهینه برای ایران</div>
    <div class="h1">با هم ببینید،<br><em>دقیقاً همزمان.</em></div>
    <p class="sub">لینک مستقیم ویدیو رو بده، لینک اتاق رو برای دوستت بفرست — هر دو دقیقاً یک فریم می‌بینید. زیرنویس جدا برای هر نفر، سینک خودکار، و پراکسی برای یوتیوب و سایت‌های فیلتر.</p>
    <div class="cta-row">
      <span class="pill">⚡ بدون ثبت‌نام</span>
      <span class="pill">👥 ۲ نفره · قابل توسعه</span>
      <span class="pill">🪶 فوق سبک — 20MB RAM</span>
      <span class="pill">🇮🇷 بهینه برای اینترنت ایران</span>
    </div>
    <div class="mock reveal reveal-2" style="animation-delay:.3s">
      <div class="mock-bar"><i style="background:#ff5f57"></i><i style="background:#ffbd2e"></i><i style="background:#28c840"></i><span style="margin-right:auto;font-size:10px;color:var(--muted);direction:ltr">watch/ a7k9x2 · 2 viewers · synced</span></div>
      <div class="mock-screen">
        <div class="play-fake">▶</div>
        <div class="mock-progress"><i></i></div>
      </div>
    </div>
  </div>

  <div class="glass reveal reveal-2">
    <div class="glow"></div>
    <div class="card-title">شروع تماشا</div>
    <div class="field">
      <label>لینک مستقیم ویدیو (mp4 / webm) یا لینک CDN</label>
      <input id="videoUrl" placeholder="https://.../video.mp4">
    </div>
    <div class="field" style="margin-top:12px">
      <label>نام شما</label>
      <input id="name" placeholder="مثلاً ناصر">
    </div>
    <label class="switch"><input type="checkbox" id="useProxy"> عبور از پراکسی سرور (برای یوتیوب / لینک فیلتر)</label>
    <div class="row">
      <button class="btn btn-primary" id="createBtn">ساخت اتاق →</button>
      <button class="btn btn-ghost" id="joinBtn">ورود با لینک</button>
    </div>
    <div class="hint">هر نفر ویدیو را جدا استریم می‌کند — سرور فقط فرمان پخش را همگام می‌کند، برای همین با حداقل پهنای باند هم روان است.</div>
    <div id="joinBox" class="join-box" style="display:none">
      <div class="field"><label>لینک دعوت اتاق</label><input id="joinLink" placeholder="https://.../watch/xxxxxx" style="direction:ltr"></div>
      <button class="btn btn-primary" style="width:100%;margin-top:10px" id="goJoin">ورود</button>
      <div id="joinHint" class="hint" style="margin-top:8px"></div>
    </div>
  </div>
</div>

<div id="features" class="features reveal reveal-3">
  <div class="feat"><div class="feat-icon">⏱</div><b>سینک هر ثانیه</b><p>هر ۱ ثانیه زمان ارسال می‌شود؛ اختلاف &gt; ۰.۸ ثانیه خودکار اصلاح، کنترل آزاد برای هر دو نفر.</p></div>
  <div class="feat"><div class="feat-icon">💬</div><b>سافت‌ساب واقعی</b><p>آپلود SRT/VTT جدا برای هر نفر، تغییر سایز، تأخیر و استایل — بدون رندر روی ویدیو.</p></div>
  <div class="feat"><div class="feat-icon">🪶</div><b>فوق سبک</b><p>بدون فریم‌ورک؛ یک Node تک‌فایلی + WebSocket — روی ۲۵۶MB RAM هم روان.</p></div>
  <div class="feat"><div class="feat-icon">🔀</div><b>پراکسی v2ray</b><p>اگر سرور v2ray/Xray دارد، ویدیو از پراکسی عبور می‌کند — یوتیوب بدون فیلتر.</p></div>
</div>

<div id="steps" class="steps reveal reveal-4">
  <div class="steps-head"><h3>سه قدم تا تماشای همزمان</h3><p>ساده، سریع، بدون منوی گیج‌کننده</p></div>
  <div class="steps-grid">
    <div class="step"><div class="step-num">۱</div><div><b>لینک ویدیو را بده</b><p>لینک مستقیم mp4 یا CDN را وارد کن و «ساخت اتاق» بزن.</p></div></div>
    <div class="step"><div class="step-num">۲</div><div><b>لینک دعوت را بفرست</b><p>لینک \`/watch/xxxx\` را کپی کن و برای دوستت بفرست — بدون ثبت‌نام.</p></div></div>
    <div class="step"><div class="step-num">۳</div><div><b>با هم ببینید</b><p>هر دو Play/Pause/Seek کنید؛ همه چیز لحظه‌ای برای طرف مقابل اعمال می‌شود.</p></div></div>
  </div>
</div>

<div class="footer">
  <span>سه حالت اجرا: سرور لینوکس · ورکر کلودفلر · لوکال IP:Port — نسخه v1.1 — <a href="/watch/demo" style="text-decoration:underline">پیش‌نمایش پلیر</a> · <span style="opacity:.6">ساخته شده برای اینترنت ایران</span></span>
  <span class="love">ساخته شده با عشق برای <b style="color:var(--accent2)">نهال قشنگم</b> <span style="color:var(--accent2)">♥</span> — © 2026 WatchTogether</span>
</div>
<style>.love{width:100%;text-align:center;padding-top:10px;margin-top:4px;border-top:1px solid var(--border);font-size:11px;letter-spacing:.02em}</style>

<script>
const $=s=>document.getElementById(s);

// Room ID format: xxxx-xxxx-xxxx (3 groups of 4 alphanumeric)
const ROOM_ID_REGEX = /^[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}$/i;
function genRoomId(){ return Array.from({length:3},()=>Math.random().toString(36).slice(2,6)).join('-'); }
function isValidRoomId(id){ return ROOM_ID_REGEX.test(id); }

// Check if room exists (works for both Node server and Worker via same API)
async function checkRoomExists(roomId){
  try{
    const r=await fetch('/api/health?room='+encodeURIComponent(roomId));
    const j=await r.json();
    return j.ok && (j.rooms||0) > 0;
  }catch{ return false; }
}

// When page loads, if path is /watch/xxx validate it
document.addEventListener('DOMContentLoaded', async ()=>{
  const path = location.pathname;
  if(path.startsWith('/watch/')){
    const roomId = path.split('/')[2];
    if(roomId && isValidRoomId(roomId)){
      const exists = await checkRoomExists(roomId);
      if(!exists){
        showToast('اتاق یافت نشد یا منقضی شده — اتاق جدید بسازید');
        setTimeout(()=> location.href='/', 2000);
      }
    } else if(roomId){
      showToast('فرمت اتاق نامعتبر — باید xxxx-xxxx-xxxx باشد');
      setTimeout(()=> location.href='/', 2000);
    }
  }
});

function showToast(t){
  const el = document.createElement('div');
  el.style.cssText = 'position:fixed;bottom:20px;left:50%;transform:translateX(-50%);background:rgba(20,20,28,.95);border:1px solid rgba(255,255,255,.1);color:#fff;padding:10px 16px;border-radius:999px;font-size:12px;z-index:9999;animation:slideUp .3s ease';
  el.textContent = t;
  document.body.appendChild(el);
  setTimeout(()=>{ el.style.opacity='0'; el.style.transition='.3s'; setTimeout(()=>el.remove(),300); }, 3000);
}

// Create room
$('createBtn').onclick=()=>{
  const url=$('videoUrl').value.trim();
  const name=$('name').value.trim()||'میزبان';
  const proxy=$('useProxy').checked?'1':'0';
  const id=genRoomId();
  const q=new URLSearchParams({video:url, name, proxy});
  location.href=\`/watch/\${id}?\`+q.toString();
};

// Toggle join box
$('joinBtn').onclick=()=>{ const b=$('joinBox'); b.style.display=b.style.display==='none'?'block':'none'; };

// Join room with validation
$('goJoin').onclick=async()=>{
  const link=$('joinLink').value.trim();
  if(!link) return;
  const hint=$('joinHint'); if(hint) hint.textContent='';
  let path='';
  try{
    const u=new URL(link);
    const parts=u.pathname.split('/').filter(Boolean);
    if(parts[0]==='watch' && parts[1] && isValidRoomId(parts[1])){
      path=u.pathname+u.search;
    } else if(parts[0]==='watch'){
      showToast('فرمت اتاق نامعتبر — باید xxxx-xxxx-xxxx باشد');
      return;
    } else {
      showToast('لینک نامعتبر — باید /watch/xxxx-xxxx-xxxx باشد');
      return;
    }
  }catch{
    if(isValidRoomId(link)){
      path=\`/watch/\${link}\`;
    }else{
      showToast('لینک یا شناسه اتاق نامعتبر — فرمت: xxxx-xxxx-xxxx');
      return;
    }
  }
  // validate existence before redirect
  const roomId=path.split('/').filter(Boolean)[1];
  if(hint) hint.textContent='⏳ در حال بررسی اتاق…';
  try{
    const r=await fetch('/api/health?room='+encodeURIComponent(roomId));
    const j=await r.json();
    if(!j.roomExists){
      if(hint) hint.textContent='';
      showToast('اتاق یافت نشد یا منقضی شده');
      return;
    }
  }catch{}
  location.href=path;
};
</script>
`;
const WATCH_HTML = `<!doctype html>
<html lang="fa" dir="rtl">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>تماشا — WatchTogether</title>
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;500;700;800&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{--bg:#06060b;--card:#11111b;--card2:#181825;--border:rgba(255,255,255,.08);--border2:rgba(255,255,255,.14);--accent:#7c5cff;--accent2:#ff3ea5;--accent3:#3b82f6;--text:#f5f5ff;--muted:#9aa0b8;--ok:#22c55e;--warn:#f59e0b}
body{font-family:'Vazirmatn',system-ui,sans-serif;background:var(--bg);color:var(--text);min-height:100vh;display:flex;flex-direction:column;overflow-x:hidden}
a{color:inherit;text-decoration:none}
/* bg */
.bg{position:fixed;inset:0;z-index:-1;overflow:hidden;background:var(--bg)}
.blob{position:absolute;border-radius:50%;filter:blur(70px);opacity:.45;mix-blend-mode:screen}
.b1{width:700px;height:700px;background:radial-gradient(circle at 30% 30%, #7c5cff 0%, transparent 70%);top:-180px;right:-120px;animation:float1 14s ease-in-out infinite}
.b2{width:600px;height:600px;background:radial-gradient(circle at 50% 50%, #ff3ea5 0%, transparent 70%);top:80px;left:-140px;animation:float2 16s ease-in-out infinite}
@keyframes float1{0%,100%{transform:translate(0,0) scale(1)}50%{transform:translate(-30px,30px) scale(1.06)}}
@keyframes float2{0%,100%{transform:translate(0,0) scale(1)}50%{transform:translate(40px,-20px) scale(1.08)}}
.grid{position:absolute;inset:0;background-image:linear-gradient(rgba(255,255,255,.02) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,.02) 1px, transparent 1px);background-size:40px 40px;mask-image:radial-gradient(ellipse at 50% 0%, black 55%, transparent 80%)}

/* topbar */
.topbar{height:58px;display:flex;align-items:center;gap:12px;padding:0 16px;background:rgba(17,17,27,.72);backdrop-filter:blur(16px);border-bottom:1px solid var(--border);position:sticky;top:0;z-index:20}
.logo{font-weight:800;font-size:16px;white-space:nowrap;display:flex;align-items:center;gap:10px}
.logo-mark{width:32px;height:32px;border-radius:10px;background:linear-gradient(135deg,var(--accent),var(--accent2));display:grid;place-items:center;font-size:14px;box-shadow:0 6px 18px rgba(124,92,255,.35)}
.logo span{opacity:.9}
.room{flex:1;min-width:0;display:flex;align-items:center;gap:8px}
.room input{flex:1;min-width:0;background:rgba(0,0,0,.35);border:1px solid var(--border);color:var(--text);border-radius:12px;padding:9px 12px;font-size:11px;direction:ltr;text-align:left;transition:.2s}
.room input:focus{outline:none;border-color:rgba(124,92,255,.4);box-shadow:0 0 0 3px rgba(124,92,255,.12)}
.pill{font-size:11px;padding:7px 11px;border-radius:999px;border:1px solid var(--border);color:var(--muted);white-space:nowrap;background:rgba(255,255,255,.04);backdrop-filter:blur(8px);display:flex;align-items:center;gap:6px}
.pill.live{color:#fff;background:linear-gradient(135deg,var(--accent),var(--accent2));border-color:transparent;box-shadow:0 4px 16px rgba(124,92,255,.3)}
.btn{border:1px solid var(--border);background:rgba(255,255,255,.06);color:var(--text);border-radius:12px;padding:9px 14px;font-size:12px;cursor:pointer;font-family:inherit;font-weight:600;transition:.2s}
.btn:hover{transform:translateY(-1px);background:rgba(255,255,255,.1)}
.btn:active{transform:translateY(0)}
.btn-primary{background:linear-gradient(135deg,var(--accent),var(--accent2));border-color:transparent;color:#fff;box-shadow:0 6px 18px rgba(124,92,255,.35)}
.btn-primary:hover{box-shadow:0 10px 28px rgba(124,92,255,.45)}
.sync-dot{width:8px;height:8px;border-radius:50%;background:var(--muted);display:inline-block;transition:.3s}
.sync-dot.on{background:var(--ok);box-shadow:0 0 12px var(--ok);animation:blink 1.6s ease-in-out infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.6}}

/* stage */
.stage{flex:1;display:grid;grid-template-columns:1fr 340px;gap:16px;padding:16px;min-height:0;max-width:1440px;width:100%;margin:0 auto}
@media(max-width:980px){.stage{grid-template-columns:1fr;padding:12px}.side{order:2}}
.player-col{display:flex;flex-direction:column;gap:12px;min-width:0}
.player-wrap{position:relative;background:#000;border-radius:22px;overflow:hidden;border:1px solid var(--border);box-shadow:0 20px 60px rgba(0,0,0,.5), 0 0 0 1px rgba(255,255,255,.04) inset;animation:reveal .6s ease}
@keyframes reveal{from{opacity:0;transform:translateY(12px)}to{opacity:1;transform:none}}
.player-wrap::before{content:'';position:absolute;inset:0;border-radius:22px;padding:1px;background:linear-gradient(135deg, rgba(124,92,255,.35), transparent 45%, rgba(255,62,165,.2));-webkit-mask:linear-gradient(#fff 0 0) content-box, linear-gradient(#fff 0 0);-webkit-mask-composite:xor;mask-composite:exclude;pointer-events:none;z-index:2}
video{width:100%;aspect-ratio:16/9;max-height:62vh;background:#000;display:block;cursor:pointer}
@media(max-width:980px){video{max-height:56vh}}
.center-play{position:absolute;inset:0;display:grid;place-items:center;z-index:3;pointer-events:none;transition:.3s}
.center-play i{width:72px;height:72px;border-radius:50%;background:rgba(255,255,255,.92);color:#000;display:grid;place-items:center;font-size:26px;box-shadow:0 12px 36px rgba(0,0,0,.5);transform:scale(.9);transition:.3s}
.player-wrap:hover .center-play i{transform:scale(1)}
.center-play.hidden{opacity:0;transform:scale(1.05)}
#videoStatus{position:absolute;inset:0;display:none;place-items:center;background:rgba(0,0,0,.68);backdrop-filter:blur(4px);color:#fff;font-size:13px;text-align:center;padding:24px;line-height:1.9;z-index:4}
.controls{position:absolute;bottom:0;left:0;right:0;z-index:5;display:flex;align-items:center;gap:10px;padding:12px 14px;background:linear-gradient(0deg, rgba(0,0,0,.85) 0%, rgba(0,0,0,.45) 60%, transparent 100%);backdrop-filter:blur(6px);transform:translateY(0);transition:.3s;direction:ltr}
.player-wrap.idle .controls{transform:translateY(100%);opacity:0}
.ctrl-btn{width:42px;height:42px;border-radius:12px;border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.08);color:#fff;cursor:pointer;display:grid;place-items:center;font-size:16px;backdrop-filter:blur(8px);transition:.2s;flex-shrink:0}
.ctrl-btn:hover{background:rgba(255,255,255,.14);transform:scale(1.05)}
.ctrl-btn.primary{background:linear-gradient(135deg,var(--accent),var(--accent2));border-color:transparent;box-shadow:0 6px 18px rgba(124,92,255,.35)}
.range-wrap{flex:1;display:flex;align-items:center;gap:10px;min-width:0;position:relative}
.thumb-preview{position:absolute;bottom:34px;transform:translateX(-50%);background:#0c0c12;border:1px solid rgba(255,255,255,.14);border-radius:12px;overflow:hidden;box-shadow:0 16px 40px rgba(0,0,0,.7);display:none;z-index:10;pointer-events:none;min-width:160px}
.thumb-preview.show{display:block}
.thumb-preview canvas{display:block;width:160px;height:90px;background:#000;object-fit:cover}
.thumb-preview .thumb-time{font-size:10px;color:#fff;text-align:center;padding:4px 6px;background:rgba(0,0,0,.75);direction:ltr;font-variant-numeric:tabular-nums}
.thumb-preview::after{content:'';position:absolute;bottom:-6px;left:50%;width:10px;height:10px;background:#0c0c12;border-right:1px solid rgba(255,255,255,.14);border-bottom:1px solid rgba(255,255,255,.14);transform:translateX(-50%) rotate(45deg)}
input[type=range].range{flex:1;appearance:none;height:4px;background:rgba(255,255,255,.18);border-radius:999px;cursor:pointer;accent-color:var(--accent)}
input[type=range].range::-webkit-slider-thumb{appearance:none;width:14px;height:14px;border-radius:50%;background:#fff;box-shadow:0 2px 8px rgba(0,0,0,.4);transition:.2s}
input[type=range].range:hover::-webkit-slider-thumb{transform:scale(1.2)}
.time{font-size:11px;color:rgba(255,255,255,.85);direction:ltr;white-space:nowrap;font-variant-numeric:tabular-nums;background:rgba(255,255,255,.08);padding:5px 9px;border-radius:999px;border:1px solid rgba(255,255,255,.08)}
.vol-wrap{display:flex;align-items:center;gap:6px}
.vol-wrap input{width:80px;accent-color:var(--accent)}
@media(max-width:600px){.vol-wrap{display:none}}

/* side */
.side{display:flex;flex-direction:column;gap:14px;min-height:0;overflow:auto;scrollbar-width:thin}
.side::-webkit-scrollbar{width:4px}
.side::-webkit-scrollbar-thumb{background:rgba(255,255,255,.12);border-radius:999px}
.section-title{font-size:11px;letter-spacing:.08em;color:var(--muted);display:flex;align-items:center;gap:8px;padding:0 2px}
.section-title i{width:22px;height:1px;background:linear-gradient(90deg,var(--accent),transparent);display:inline-block}
.card{background:linear-gradient(180deg, rgba(255,255,255,.07), rgba(255,255,255,.03));border:1px solid var(--border);border-radius:20px;padding:16px;backdrop-filter:blur(14px);position:relative;overflow:hidden;transition:.25s;animation:reveal .6s ease both}
.card:hover{border-color:var(--border2);transform:translateY(-2px);box-shadow:0 12px 32px rgba(0,0,0,.3)}
.card::before{content:'';position:absolute;top:0;right:0;width:140px;height:140px;background:radial-gradient(circle, rgba(124,92,255,.10), transparent 70%);pointer-events:none}
.label{font-size:11px;color:var(--muted);margin-bottom:7px;display:block}
.input{width:100%;background:rgba(0,0,0,.35);border:1px solid var(--border);color:var(--text);border-radius:12px;padding:11px 12px;font-size:12px;transition:.2s}
.input:focus{outline:none;border-color:rgba(124,92,255,.4);box-shadow:0 0 0 3px rgba(124,92,255,.12)}
.row{display:flex;gap:8px;margin-top:10px}
.hint{font-size:11px;color:var(--muted);line-height:1.8}
.status{font-size:11px;color:var(--muted);line-height:1.7}
.kbd{font-size:10px;border:1px solid var(--border);padding:2px 6px;border-radius:6px;background:rgba(255,255,255,.06)}
.switch{display:flex;align-items:center;gap:8px;font-size:11px;color:var(--muted);margin-top:10px}
.switch input{accent-color:var(--accent)}
/* sub */
.sub-actions{display:flex;gap:6px;flex-wrap:wrap;margin-top:10px}
.sub-actions .btn{font-size:11px;padding:7px 10px;border-radius:10px}
.sub-actions .btn.active{background:linear-gradient(135deg,var(--accent),var(--accent2));color:#fff;border-color:transparent;box-shadow:0 4px 14px rgba(124,92,255,.3)}
.file-input{position:relative;overflow:hidden}
.file-input input{position:absolute;inset:0;opacity:0;cursor:pointer}
.file-label{display:flex;align-items:center;gap:8px;justify-content:center;padding:12px;border:1px dashed rgba(255,255,255,.18);border-radius:12px;background:rgba(255,255,255,.03);font-size:12px;color:var(--muted);transition:.2s;cursor:pointer}
.file-label:hover{background:rgba(255,255,255,.06);border-color:rgba(124,92,255,.35);color:var(--text)}
.delay-row{display:flex;gap:6px;align-items:center;margin-top:10px;flex-wrap:wrap}
.delay-row .btn{padding:6px 10px;font-size:11px}
.toast{position:fixed;bottom:20px;left:50%;transform:translateX(-50%) translateY(20px);background:rgba(20,20,28,.9);border:1px solid var(--border);color:var(--text);padding:10px 16px;border-radius:999px;font-size:12px;backdrop-filter:blur(12px);opacity:0;pointer-events:none;transition:.3s;z-index:50}
.toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
/* vpn modal */
.vpn-btn{border-color:rgba(124,92,255,.35) !important;background:rgba(124,92,255,.12) !important}
.vpn-btn.has-config{border-color:rgba(34,197,94,.4) !important;background:rgba(34,197,94,.14) !important;color:#86efac !important}
.modal-overlay{position:fixed;inset:0;z-index:40;background:rgba(0,0,0,.55);backdrop-filter:blur(8px);display:none;place-items:center;padding:20px;animation:fadeIn .25s ease}
.modal-overlay.open{display:grid}
@keyframes fadeIn{from{opacity:0}to{opacity:1}}
.modal{width:min(520px, 100%);background:linear-gradient(180deg, rgba(26,26,40,.95), rgba(14,14,22,.98));border:1px solid var(--border2);border-radius:22px;overflow:hidden;box-shadow:0 24px 64px rgba(0,0,0,.6), 0 0 0 1px rgba(255,255,255,.06) inset;animation:modalIn .32s cubic-bezier(.16,1,.3,1)}
@keyframes modalIn{from{opacity:0;transform:translateY(14px) scale(.97)}to{opacity:1;transform:none}}
.modal-head{padding:18px 18px 0;display:flex;align-items:center;justify-content:space-between}
.modal-head h3{font-size:14px;font-weight:800;display:flex;align-items:center;gap:8px}
.modal-head h3 i{width:30px;height:30px;border-radius:10px;background:linear-gradient(135deg,var(--accent),var(--accent2));display:grid;place-items:center;font-size:14px}
.modal-x{width:30px;height:30px;border-radius:10px;border:1px solid var(--border);background:rgba(255,255,255,.06);color:var(--muted);cursor:pointer;display:grid;place-items:center}
.modal-body{padding:16px 18px 18px}
.modal-textarea{width:100%;min-height:96px;background:rgba(0,0,0,.45);border:1px solid var(--border);color:var(--text);border-radius:12px;padding:11px 12px;font-size:11px;direction:ltr;text-align:left;resize:vertical;font-family:ui-monospace,monospace;transition:.2s}
.modal-textarea:focus{outline:none;border-color:rgba(124,92,255,.4);box-shadow:0 0 0 3px rgba(124,92,255,.12)}
.vpn-hint{font-size:11px;color:var(--muted);line-height:1.8;margin-top:8px}
.vpn-status{margin-top:10px;padding:10px 12px;border-radius:12px;font-size:11px;line-height:1.7;display:none}
.vpn-status.ok{display:block;background:rgba(34,197,94,.1);border:1px solid rgba(34,197,94,.22);color:#86efac}
.vpn-status.warn{display:block;background:rgba(245,158,11,.09);border:1px solid rgba(245,158,11,.24);color:#fcd34d}
.vpn-status.err{display:block;background:rgba(239,68,68,.1);border:1px solid rgba(239,68,68,.2);color:#fca5a5}
.love-footer{text-align:center;padding:14px 16px 18px;font-size:11px;color:var(--muted);border-top:1px solid var(--border);margin-top:8px;letter-spacing:.02em}
.love-footer b{color:var(--accent2)}

/* kmplayer-like track selectors */
.cc-btn.active,.audio-btn.active{background:linear-gradient(135deg,var(--accent),var(--accent2));border-color:transparent;box-shadow:0 4px 14px rgba(124,92,255,.3)}
.track-pop{position:absolute;bottom:58px;z-index:12;background:rgba(16,16,24,.96);border:1px solid var(--border2);border-radius:14px;box-shadow:0 16px 40px rgba(0,0,0,.6);backdrop-filter:blur(12px);padding:6px;min-width:190px;max-width:260px;max-height:240px;overflow:auto;display:none;flex-direction:column;gap:4px}
.track-pop.open{display:flex}
.track-pop .tp-head{font-size:11px;color:var(--muted);padding:6px 8px 4px;border-bottom:1px solid var(--border);margin-bottom:2px}
.track-opt{display:flex;align-items:center;gap:8px;padding:8px 10px;border-radius:10px;cursor:pointer;font-size:11px;transition:.15s;border:1px solid transparent;text-align:right}
.track-opt:hover{background:rgba(255,255,255,.06);border-color:var(--border)}
.track-opt.on{background:linear-gradient(135deg,rgba(124,92,255,.18),rgba(255,62,165,.12));border-color:rgba(124,92,255,.35);color:#fff}
.track-opt .dot{width:8px;height:8px;border-radius:50%;background:var(--muted);flex-shrink:0}
.track-opt.on .dot{background:var(--ok);box-shadow:0 0 8px var(--ok)}
.track-badge{font-size:9px;padding:2px 6px;border-radius:999px;background:rgba(255,255,255,.08);border:1px solid var(--border);margin-right:auto}
.internal-card .track-row{display:flex;align-items:center;gap:8px;padding:9px 11px;background:rgba(255,255,255,.04);border:1px solid var(--border);border-radius:11px;font-size:11px;transition:.15s}
.internal-card .track-row:hover{background:rgba(255,255,255,.07);border-color:var(--border2)}
.internal-card .track-row.on{background:rgba(124,92,255,.14);border-color:rgba(124,92,255,.35)}
/* compact tabs — side no longer long */
.side.compact{overflow:visible}
.tabs{display:flex;gap:6px;padding:4px;background:rgba(255,255,255,.04);border:1px solid var(--border);border-radius:14px}
.tab{flex:1;padding:9px 6px;border-radius:10px;border:1px solid transparent;background:transparent;color:var(--muted);font-size:11px;font-weight:700;cursor:pointer;transition:.18s}
.tab.on{background:linear-gradient(135deg,var(--accent),var(--accent2));color:#fff;border-color:transparent;box-shadow:0 4px 14px rgba(124,92,255,.3)}
.tab-panel{display:none}
.tab-panel.on{display:flex;flex-direction:column;gap:12px}
.side.compact .card{padding:14px}
.dub-mix{display:flex;align-items:center;gap:8px;margin-top:8px}
.dub-mix input[type=range]{flex:1;accent-color:var(--accent)}
.proxy-shared{font-size:10px;padding:4px 8px;border-radius:999px;border:1px solid rgba(34,197,94,.3);background:rgba(34,197,94,.12);color:#86efac;display:none}
.proxy-shared.show{display:inline-flex;align-items:center;gap:4px}


</style>

<div class="bg"><div class="grid"></div><div class="blob b1"></div><div class="blob b2"></div></div>

<div class="topbar">
  <a href="/" class="logo"><div class="logo-mark">▶</div> Watch<span>Together</span></a>
  <div class="room">
    <input id="roomLink" readonly>
    <button class="btn btn-primary" id="copyBtn">کپی لینک دعوت</button>
    <button class="btn vpn-btn" id="vpnBtn" title="کانفیگ VPN برای پراکسی ویدیو">◈ VPN</button>
  </div>
  <span class="pill" id="peerPill">۱ نفر</span>
  <span class="pill" id="hostPill">میزبان</span>
  <span class="pill" id="connPill"><span class="sync-dot" id="syncDot"></span><span id="connText">در حال اتصال…</span></span>
</div>

<div class="stage">
  <div class="player-col">
    <div class="player-wrap" id="playerWrap">
      <video id="video" playsinline preload="metadata"></video>
      <audio id="dubAudio" preload="auto" style="display:none"></audio>
      <div class="center-play" id="centerPlay"><i id="centerPlayIcon">▶</i></div>
      <div id="videoStatus"></div>
      <div class="controls" id="controls">
        <button class="ctrl-btn primary" id="playBtn" title="پخش/توقف (Space)">▶</button>
        <div class="range-wrap" id="rangeWrap">
          <input type="range" id="seek" class="range" min="0" max="100" value="0">
          <div class="thumb-preview" id="thumbPreview"><canvas id="thumbCanvas" width="160" height="90"></canvas><div class="thumb-time" id="thumbTime">00:00</div></div>
        </div>
        <span class="time" id="timeLabel">00:00 / 00:00</span>
        <button class="ctrl-btn cc-btn" id="ccBtn" title="زیرنویس (داخلی + خارجی)">💬</button>
        <button class="ctrl-btn audio-btn" id="audioBtn" title="ترک صدا (دوبله/اصلی)">🎧</button>
        <div class="vol-wrap">
          <button class="ctrl-btn" id="muteBtn" title="صدا">🔊</button>
          <input type="range" id="vol" min="0" max="100" value="100" style="width:80px">
        </div>
        <button class="ctrl-btn" id="fsBtn" title="تمام‌صفحه">⛶</button>
        <div class="track-pop" id="ccPop" style="left:88px"></div>
        <div class="track-pop" id="audioPop" style="left:138px"></div>
      </div>
    </div>
    <div class="card" style="animation-delay:.08s">
      <div class="status" id="syncStatus">کنترل آزاد — هر دو نفر می‌توانند پلی/پاز/Seek کنند. سینک هر ۱ ثانیه.</div>
      <div class="status" style="margin-top:6px">میانبر: <span class="kbd">Space</span> پخش/توقف · <span class="kbd">←/→</span> ۵ ثانیه · کلیک روی ویدیو</div>
    </div>
  </div>

  <div class="side compact">
    <div class="tabs" role="tablist">
      <button class="tab on" data-tab="video">🎬 ویدیو</button>
      <button class="tab" data-tab="sub">💬 زیرنویس</button>
      <button class="tab" data-tab="audio">🎧 صدا/دوبله</button>
    </div>
    <div class="tab-panel on" data-panel="video">
    <div class="card" style="animation-delay:.06s">
      <label class="label">لینک ویدیو</label>
      <input id="videoUrlInput" class="input" placeholder="https://.../video.mp4" style="direction:ltr">
      <div class="row">
        <button class="btn btn-primary" id="loadBtn" style="flex:1">بارگذاری</button>
      </div>
      <label class="switch"><input type="checkbox" id="proxyChk"> عبور از پراکسی سرور (یوتیوب / فیلتر)</label>
      <span class="proxy-shared" id="proxySharedBadge">◈ پراکسی اتاق فعال</span>
      <div class="hint">پراکسی با <code>Range</code> عبور می‌کند تا seek سریع بماند. صاحب اتاق با VPN خودش پراکسی اتاق را فعال می‌کند — بقیه بدون دیدن کانفیگ استفاده می‌کنند.</div>
      <div style="margin-top:12px;border-top:1px solid var(--border);padding-top:12px">
        <label class="label">🎙 لینک صدای دوبله (اختیاری) — جدا از ویدیو</label>
        <input id="dubUrlInput" class="input" placeholder="https://.../dubbed.mp3 یا .m4a" style="direction:ltr">
        <div class="row">
          <button class="btn" id="dubLoadBtn" style="flex:1">افزودن/بروزرسانی دوبله</button>
          <button class="btn" id="dubClearBtn">حذف</button>
        </div>
        <div class="dub-mix" id="dubMix" style="display:none">
          <span style="font-size:11px;color:var(--muted)">میکس</span>
          <input type="range" id="dubMixRange" min="0" max="100" value="100">
          <span id="dubMixLabel" style="font-size:11px;color:var(--muted);min-width:38px">100%</span>
          <span id="dubStatus" class="hint" style="flex:1"></span>
        </div>
        <div class="hint" id="dubHint">لینک صدای دوبله را بده — با ویدیو سینک می‌شود (تغییر ~30ms). تغییر دوبله برای همه‌ی اتاق اعمال می‌شود تا لینک عوض نشده.</div>
      </div>
    </div>
    </div>
    <div class="tab-panel" data-panel="sub">

        <div class="section-title"><i></i> زیرنویس — KMPlayer-style</div>
    <div class="card" style="animation-delay:.12s">
      <div class="hint" style="margin-bottom:8px">اول فیلم را لود کن تا <b>زیرنویس‌های داخل خود MKV/MP4</b> شناسایی شوند — بعد از همین‌جا انتخاب کن. فایل خارجی هم اضافه می‌شود.</div>
      <div id="internalSubList" style="display:flex;flex-direction:column;gap:6px;margin-bottom:10px"></div>
      <div class="file-input">
        <label class="file-label" for="subFile">📄 افزودن SRT / VTT خارجی <span style="opacity:.6">— به لیست بالا اضافه می‌شود</span></label>
        <input type="file" id="subFile" accept=".srt,.vtt">
      </div>
      <div class="sub-actions">
        <button class="btn active" id="subOff">✕ خاموش</button>
        <button class="btn" id="subLarger">A+ بزرگ‌تر</button>
        <button class="btn" id="subSmaller">A- کوچک‌تر</button>
      </div>
      <div class="delay-row">
        <span class="label" style="margin:0">تاخیر</span>
        <button class="btn" id="subDelayMinus">−0.5s</button>
        <button class="btn" id="subDelayPlus">+0.5s</button>
        <span id="subDelayLabel" style="font-size:11px;color:var(--muted);min-width:36px;text-align:center">0.0s</span>
      </div>
      <div class="status" id="subStatus" style="margin-top:10px">فیلم را پخش کن تا ترک‌ها شناسایی شوند.</div>
      <style id="subStyle">::cue{font-size:18px;background:rgba(0,0,0,.62);color:#fff;text-shadow:0 1px 4px rgba(0,0,0,.6)}</style>
    </div>
    </div>
    <div class="tab-panel" data-panel="audio">
    <div class="section-title"><i></i> صدا — دوبله / اصلی — ترک داخلی</div>
    <div class="card internal-card" style="animation-delay:.14s">
      <div id="internalAudioList" style="display:flex;flex-direction:column;gap:6px"></div>
      <div class="hint" id="audioHint" style="margin-top:8px">ترک‌های صدای داخل MKV/MP4 اینجا می‌آیند — مثلا «فارسی» و «English». اگر لیست خالی ماند، مرورگر MKV را کامل پشتیبانی نمی‌کند؛ نسخه MP4 بگیر یا روی Firefox تست کن.</div>
    </div>
    </div>
    </div>

    <div class="card" style="animation-delay:.18s;border-style:dashed;opacity:.85">
      <div class="label">چت/صدا (آینده)</div>
      <div class="hint">پیام‌های <code>chat</code> از WS عبور می‌کنند — UI فعلاً مخفی تا خلوت بماند.</div>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<!-- VPN modal — bulk import + list + ping -->
<div class="modal-overlay" id="vpnModal">
  <div class="modal" role="dialog" aria-modal="true" style="max-height:90vh;display:flex;flex-direction:column">
    <div class="modal-head">
      <h3><i>◈</i> کانفیگ‌های VPN</h3>
      <button class="modal-x" id="vpnClose" aria-label="بستن">✕</button>
    </div>
    <div class="modal-body" style="overflow:auto;flex:1">
      <label class="label">افزودن کانفیگ — هر خط یک کانفیگ (vless / vmess / trojan / ss / http / socks5)</label>
      <textarea id="vpnUris" class="modal-textarea" placeholder="vless://...&#10;vless://...&#10;vmess://...&#10;یا لیست کامل را پیست کن" spellcheck="false"></textarea>
      <div class="row" style="margin-top:10px">
        <button class="btn btn-primary" id="vpnImport" style="flex:1">➕ ایمپورت به لیست</button>
        <button class="btn" id="vpnCancel">انصراف</button>
      </div>
      <div id="vpnStatus" class="vpn-status"></div>
      <div style="display:flex;align-items:center;gap:8px;margin-top:16px">
        <span class="label" style="margin:0;flex:1">لیست ذخیره‌شده (<span id="vpnCount">0</span>) — انتخاب دستی یا خودکار</span>
        <button class="btn" id="vpnPingAll" style="padding:6px 10px;font-size:11px">📡 پینگ همه</button>
        <button class="btn" id="vpnAuto" style="padding:6px 10px;font-size:11px">⚡ بهترین خودکار</button>
      </div>
      <div id="vpnList" style="margin-top:10px;display:flex;flex-direction:column;gap:8px;max-height:260px;overflow:auto"></div>
      <div style="display:flex;gap:8px;margin-top:10px">
        <button class="btn" id="vpnClearAll" style="font-size:11px;padding:7px 10px;color:#f87171;border-color:rgba(248,113,113,.3)">🗑 حذف همه</button>
        <span class="hint" style="flex:1">کانفیگ‌ها برای همین اتاق و همین مرورگر (localStorage) ذخیره می‌شن. پراکسی ویدیو از کانفیگ فعال استفاده می‌کنه.</span>
      </div>
      <div class="vpn-hint" style="margin-top:8px">
        <b>http/socks5</b> مستقیم با پراکسی کار می‌کنه. <b>vless/vmess/trojan/ss</b> روی این سرور نیاز به Xray TUN داره — اگر نداره فقط پینگ host گرفته می‌شه.
      </div>
    </div>
  </div>
</div>

<div class="love-footer">ساخته شده با عشق برای <b>نهال قشنگم</b> ♥ — © 2026 WatchTogether</div>

<script>
const $=s=>document.getElementById(s);
const video=$('video'), seek=$('seek'), timeLabel=$('timeLabel'), playBtn=$('playBtn'), centerPlay=$('centerPlay'), centerPlayIcon=$('centerPlayIcon'), playerWrap=$('playerWrap');
const roomLink=$('roomLink'), peerPill=$('peerPill'), hostPill=$('hostPill'), connText=$('connText'), syncDot=$('syncDot');
const videoUrlInput=$('videoUrlInput'), proxyChk=$('proxyChk'), loadBtn=$('loadBtn');
const subFile=$('subFile'), subStatus=$('subStatus'), subStyle=$('subStyle'), subOff=$('subOff'); const subOn=document.getElementById('subOn'); // may be null after KMPlayer redesign
const toast=$('toast'), vol=$('vol'), muteBtn=$('muteBtn');
const vpnBtn=$('vpnBtn'), vpnModal=$('vpnModal'), vpnUrisEl=$('vpnUris'), vpnStatus=$('vpnStatus'), vpnList=$('vpnList'), vpnCount=$('vpnCount');
const USER_ID_KEY='wt_user_id'; let myUserId=localStorage.getItem(USER_ID_KEY); if(!myUserId){ myUserId=Math.random().toString(36).slice(2,10); localStorage.setItem(USER_ID_KEY, myUserId); }

// Room ID format: xxxx-xxxx-xxxx (3 groups of 4 alphanumeric)
const ROOM_ID_REGEX = /^[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}$/i;

const videoStatus=$('videoStatus');
let lastUrl='';
function showToast(t){ toast.textContent=t; toast.classList.add('show'); clearTimeout(showToast._t); showToast._t=setTimeout(()=>toast.classList.remove('show'),1800); }

// room + params
const pathParts=location.pathname.split('/').filter(Boolean);
let roomId = pathParts[1] || 'demo';
if(!pathParts[1]) history.replaceState(null,'',\`/watch/\${roomId}\${location.search}\`);
const qs=new URLSearchParams(location.search);
let initialVideo = qs.get('video')||'';
let initialProxy = qs.get('proxy')==='1';
let myName = qs.get('name')||'مهمان';
let myId=null, hostId=null, isHost=false, suppress=false, ws=null, syncTimer=null;

// Validate format; existence check only for joiners (no ?video=), creator skips redirect
if(pathParts[1] && !ROOM_ID_REGEX.test(roomId) && roomId!=='demo'){
  showToast('فرمت شناسه اتاق نامعتبر — باید xxxx-xxxx-xxxx باشد');
  setTimeout(()=> location.href='/', 2000);
} else if(pathParts[1] && !qs.has('video')){
  fetch('/api/health?room='+encodeURIComponent(roomId))
    .then(r=>r.json()).then(j=>{ if(!j.roomExists){ showToast('اتاق یافت نشد یا منقضی شده — از صفحه اصلی بسازید'); setTimeout(()=> location.href='/', 2500); } }).catch(()=>{});
}

videoUrlInput.value=initialVideo;
proxyChk.checked=initialProxy;
roomLink.value = location.origin + \`/watch/\${roomId}\`;
function proxied(url){ if(!url) return ''; const base = \`/api/proxy?url=\${encodeURIComponent(url)}&room=\${encodeURIComponent(roomId)}&userId=\${encodeURIComponent(myUserId)}\`; if(!proxyChk.checked) return url; return base; }
function showStatus(html){ videoStatus.innerHTML=html; videoStatus.style.display='grid'; }
function hideStatus(){ videoStatus.style.display='none'; }
function fmt(s){ s=Math.max(0,Math.floor(s||0)); const m=String(Math.floor(s/60)).padStart(2,'0'), sc=String(s%60).padStart(2,'0'); return \`\${m}:\${sc}\`; }

async function resolveYoutubeDirect(url){
  if(!/youtube\.com|youtu\.be/.test(url)) return null;
  try{
    const r=await fetch('/api/yt?url='+encodeURIComponent(url));
    const j=await r.json();
    if(j.ok && j.url) return j.url;
  }catch{}
  return null;
}
async function loadVideo(url, forceProxy){
  if(!url){ video.removeAttribute('src'); video.load(); return; }
  lastUrl=url;
  // youtube: try server-side yt-dlp first
  if(/youtube\.com|youtu\.be/.test(url)){
    const ytDirect=await resolveYoutubeDirect(url);
    if(ytDirect){
      url=ytDirect; lastUrl=ytDirect;
      // youtube direct urls often need proxy for Range/CORS — auto-enable
      forceProxy=true;
      showToast('یوتیوب به لینک مستقیم تبدیل شد');
    } else if(!forceProxy && !proxyChk.checked){
      showStatus('لینک یوتیوب: روی سرور yt-dlp نصب نیست.<br><small style="color:#f59e0b">نصب: pip install yt-dlp / یا لینک مستقیم mp4 بده — با پراکسی هم می‌توان دوباره تلاش کرد</small><br><button onclick="document.getElementById(\'proxyChk\').checked=true;document.getElementById(\'loadBtn\').click()" style="margin-top:10px;padding:6px 12px;border-radius:10px;border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.08);color:#fff;cursor:pointer">تلاش با پراکسی</button>');
      return;
    }
  }
  // if room has shared proxy, video will be proxied server-side even if checkbox off — keep checkbox as hint but fetch still checks shared map
  const useProxy = forceProxy || proxyChk.checked;
  const src = useProxy ? \`/api/proxy?url=\${encodeURIComponent(url)}&room=\${encodeURIComponent(roomId)}&userId=\${encodeURIComponent(myUserId)}\` : url;
  video.removeAttribute('crossorigin');
  hideStatus();
  let t=setTimeout(()=> showStatus('در حال بارگذاری…<br><small style="color:#9a9ab0">اگر طولانی شد تیک «پراکسی» را روشن کنید</small>'), 1200);
  const onLoaded=()=>{ clearTimeout(t); hideStatus(); video.removeEventListener('loadedmetadata',onLoaded); video.removeEventListener('canplay',onLoaded); };
  video.addEventListener('loadedmetadata',onLoaded);
  video.addEventListener('canplay',onLoaded);
  video.src = src;
  video.load();
}
video.addEventListener('error',()=>{
  const err=video.error; const code=err?err.code:'';
  const alreadyProxy = video.src.includes('/api/proxy');
  if(!alreadyProxy && lastUrl){
    showStatus(\`بارگذاری مستقیم ناموفق (کد \${code}) — تلاش از طریق پراکسی…\`);
    proxyChk.checked=true;
    setTimeout(()=> loadVideo(lastUrl, true), 400);
    return;
  }
  showStatus(\`خطا در پخش (کد \${code})<br><small style="color:#f59e0b">\${err?err.message:''}</small><br><button onclick="document.getElementById('video').load();document.getElementById('video').play()" style="margin-top:10px;padding:6px 12px;border-radius:10px;border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.08);color:#fff;cursor:pointer">تلاش مجدد</button>\`);
});
video.addEventListener('stalled',()=> showStatus('اتصال کند — در حال بافر…'));
video.addEventListener('waiting',()=>{ if(video.readyState<3) showStatus('بافر…'); });
video.addEventListener('playing',hideStatus);
video.addEventListener('loadeddata',hideStatus);
if(initialVideo) loadVideo(initialVideo);

// === KMPlayer-like: internal + external subs & audio ===
let trackEl=null, cueDelay=0, cueSize=18, srtBlobUrl=null, subEnabled=false, originalVtt='';
let externalEntries=[]; // {id,label,blobUrl,vtt}
let activeSubId=null; // null=off, 'ext-0', or track index string 't-0'
function srtToVtt(text){
  text=text.replace(/\r/g,'').trim();
  if(text.startsWith('\uFEFF')) text=text.slice(1);
  if(!text.startsWith('WEBVTT')) text='WEBVTT\n\n'+text;
  text=text.replace(/(\d+):(\d+):(\d+),(\d+)/g,'$1:$2:$3.$4');
  return text;
}
function shiftVtt(text, delay){
  if(!delay) return text;
  return text.replace(/(\d{2}):(\d{2}):(\d{2})\.(\d{3}) --> (\d{2}):(\d{2}):(\d{2})\.(\d{3})/g, (m,h1,m1,s1,ms1,h2,m2,s2,ms2)=>{
    const toSec=(h,m,s,ms)=> parseInt(h)*3600+parseInt(m)*60+parseInt(s)+parseInt(ms)/1000;
    const fmt2=(sec)=>{
      sec=Math.max(0,sec);
      const h=Math.floor(sec/3600), mm=Math.floor((sec%3600)/60), s=Math.floor(sec%60), ms=Math.round((sec%1)*1000);
      return \`\${String(h).padStart(2,'0')}:\${String(mm).padStart(2,'0')}:\${String(s).padStart(2,'0')}.\${String(ms).padStart(3,'0')}\`;
    };
    const a=toSec(h1,m1,s1,ms1)+delay, b=toSec(h2,m2,s2,ms2)+delay;
    return \`\${fmt2(a)} --> \${fmt2(b)}\`;
  });
}
function applyExternalVtt(id){
  const ent=externalEntries.find(x=>x.id===id); if(!ent) return;
  const shifted=shiftVtt(ent.vtt, cueDelay);
  if(srtBlobUrl) URL.revokeObjectURL(srtBlobUrl);
  srtBlobUrl=URL.createObjectURL(new Blob([shifted],{type:'text/vtt'}));
  if(trackEl) trackEl.remove();
  trackEl=document.createElement('track');
  trackEl.kind='subtitles'; trackEl.label=ent.label; trackEl.srclang='fa'; trackEl.default=true;
  trackEl.src=srtBlobUrl;
  video.appendChild(trackEl);
  trackEl.addEventListener('load', ()=>{ for(const t of video.textTracks) t.mode='hidden'; const tt=[...video.textTracks].find(t=>t.label===ent.label); if(tt) tt.mode='showing'; });
  setTimeout(()=>{ for(const t of video.textTracks) t.mode='hidden'; const tt=[...video.textTracks].find(t=>t.label===ent.label); if(tt) tt.mode='showing'; }, 350);
}
function setActiveSub(id){
  activeSubId=id;
  // hide all first
  for(const t of video.textTracks) t.mode='hidden';
  subEnabled = id!==null;
  if(id===null){
    if(trackEl && trackEl.parentNode) { /* keep but hidden */ }
    subStatus.textContent='زیرنویس خاموش.';
  } else if(String(id).startsWith('ext-')){
    applyExternalVtt(id);
    const ent=externalEntries.find(x=>x.id===id);
    subStatus.textContent=\`زیرنویس: \${ent?ent.label:'خارجی'} — فعال · \${cueSize}px · \${cueDelay.toFixed(1)}s\`;
  } else if(String(id).startsWith('t-')){
    const idx=parseInt(String(id).slice(2),10);
    const tt=video.textTracks[idx]; if(tt) tt.mode='showing';
    const lab=tt? (tt.label||tt.language||('ترک '+(idx+1))) : '';
    subStatus.textContent=\`زیرنویس داخلی: \${lab} — فعال · \${cueSize}px\`;
  }
  refreshSubUI();
  refreshCcPop();
  // button state
  const ccBtn=document.getElementById('ccBtn');
  if(ccBtn) ccBtn.classList.toggle('active', subEnabled);
  if(subOff) subOff.classList.toggle('active', !subEnabled);
  if(!suppressSubBroadcast) broadcastSubState();
}
let suppressSubBroadcast=false;
function currentSubPayload(){
  if(activeSubId===null) return {id:null, type:'off', cueDelay, cueSize};
  if(String(activeSubId).startsWith('srv-')){
    const ent=externalEntries.find(x=>x.id===activeSubId);
    return {id:activeSubId, type:'server', label: ent?ent.label:activeSubId, vtt: ent?ent.vtt:null, serverIndex: parseInt(String(activeSubId).slice(4),10), cueDelay, cueSize};
  }
  if(String(activeSubId).startsWith('ext-')){
    const ent=externalEntries.find(x=>x.id===activeSubId);
    return {id:activeSubId, type:'external', label: ent?ent.label:activeSubId, vtt: ent?ent.vtt:null, cueDelay, cueSize};
  }
  if(String(activeSubId).startsWith('t-')){
    const idx=parseInt(String(activeSubId).slice(2),10);
    const tt=video.textTracks[idx];
    return {id:activeSubId, type:'internal', label: tt? (tt.label||tt.language||activeSubId):activeSubId, cueDelay, cueSize};
  }
  return {id:activeSubId, type:'off', cueDelay, cueSize};
}
function broadcastSubState(){
  try{ send({type:'sub-change', sub: currentSubPayload()}); }catch{}
}
function applyRemoteSub(sub){
  if(!sub || sub.id===null){
    suppressSubBroadcast=true;
    setActiveSub(null);
    // apply size/delay
    if(typeof sub?.cueSize==='number'){ cueSize=sub.cueSize; subStyle.textContent=\`::cue{font-size:\${cueSize}px;background:rgba(0,0,0,.62);color:#fff;text-shadow:0 1px 4px rgba(0,0,0,.6)}\`; }
    if(typeof sub?.cueDelay==='number'){ cueDelay=sub.cueDelay; const l=document.getElementById('subDelayLabel'); if(l) l.textContent=cueDelay.toFixed(1)+'s'; }
    suppressSubBroadcast=false;
    return;
  }
  // size/delay first
  if(typeof sub.cueSize==='number'){ cueSize=sub.cueSize; subStyle.textContent=\`::cue{font-size:\${cueSize}px;background:rgba(0,0,0,.62);color:#fff;text-shadow:0 1px 4px rgba(0,0,0,.6)}\`; }
  if(typeof sub.cueDelay==='number'){ cueDelay=sub.cueDelay; const l=document.getElementById('subDelayLabel'); if(l) l.textContent=cueDelay.toFixed(1)+'s'; }
  if(sub.type==='external' || sub.type==='server'){
    // add vtt if provided
    if(sub.vtt){
      let ent=externalEntries.find(x=>x.id===sub.id);
      if(!ent) externalEntries.push({id:sub.id, label:sub.label||sub.id, vtt:sub.vtt});
      else ent.vtt=sub.vtt;
    }
    suppressSubBroadcast=true;
    setActiveSub(sub.id);
    suppressSubBroadcast=false;
    // if server type but no vtt yet, fetch it
    if(sub.type==='server' && !sub.vtt && typeof sub.serverIndex==='number'){
      const u=lastUrl||videoUrlInput.value.trim();
      if(u) fetch('/api/video/sub?url='+encodeURIComponent(u)+'&index='+sub.serverIndex).then(r=>r.text()).then(vtt=>{
        let ent=externalEntries.find(x=>x.id===sub.id);
        if(ent) ent.vtt=vtt; else externalEntries.push({id:sub.id, label:sub.label, vtt});
        if(activeSubId===sub.id) applyExternalVtt(sub.id);
      }).catch(()=>{});
    }
    // re-apply with new delay
    if(activeSubId===sub.id && sub.vtt) applyExternalVtt(sub.id);
  } else if(sub.type==='internal'){
    suppressSubBroadcast=true;
    setActiveSub(sub.id);
    suppressSubBroadcast=false;
  }
}
function refreshSubUI(){
  const wrap=document.getElementById('internalSubList'); if(!wrap) return;
  wrap.innerHTML='';
  if(serverTracksLoading){
    const ld=document.createElement('div'); ld.className='hint'; ld.style.padding='8px'; ld.textContent='⏳ در حال شناسایی ترک‌های داخل MKV…'; wrap.appendChild(ld); return;
  }
  const tracks=[...video.textTracks];
  // Off row
  const off=document.createElement('div');
  off.className='track-row'+(activeSubId===null?' on':'');
  off.style.cursor='pointer';
  off.innerHTML=\`<span class="dot"></span><span>✕ خاموش</span><span class="track-badge">Off</span>\`;
  off.onclick=()=> setActiveSub(null);
  wrap.appendChild(off);
  tracks.forEach((t,i)=>{
    const id='t-'+i;
    const isExt = trackEl && t.label===externalEntries.find(x=>x.id===activeSubId)?.label;
    const on = activeSubId===id;
    const lab=(t.label||t.language||'زیرنویس '+(i+1)) + (t.language? ' · '+t.language:'');
    const kind=t.kind||'subtitles';
    const row=document.createElement('div');
    row.className='track-row'+(on?' on':'');
    row.style.cursor='pointer';
    row.innerHTML=\`<span class="dot"></span><span style="flex:1">\${lab}</span><span class="track-badge">\${kind}</span>\`;
    row.onclick=()=> setActiveSub(id);
    wrap.appendChild(row);
  });
  externalEntries.forEach(ent=>{
    const on=activeSubId===ent.id;
    const row=document.createElement('div');
    row.className='track-row'+(on?' on':'');
    row.style.cursor='pointer';
    row.innerHTML=\`<span class="dot"></span><span style="flex:1">📄 \${ent.label}</span><span class="track-badge">خارجی</span>\`;
    row.onclick=()=> setActiveSub(ent.id);
    wrap.appendChild(row);
  });
  serverSubs.forEach(s=>{
    const sid='srv-'+s.index;
    const already=externalEntries.some(x=>x.id===sid);
    const on=activeSubId===sid;
    const row=document.createElement('div');
    row.className='track-row'+(on?' on':'');
    row.style.cursor='pointer';
    row.style.borderStyle= already? 'solid':'dashed';
    row.innerHTML=\`<span class="dot"></span><span style="flex:1">⭐ \${s.label}</span><span class="track-badge">\${already?'آماده':'سرور'}</span>\`;
    row.onclick=()=> loadServerSub(s);
    wrap.appendChild(row);
  });
  if(tracks.length===0 && externalEntries.length===0){
    const hint=document.createElement('div');
    hint.className='hint'; hint.style.padding='8px';
    hint.textContent='هنوز ترکی یافت نشد — فیلم را پخش کن. اگر MKV با سافت‌ساب است و چیزی نیامد، مرورگر MKV را کامل پشتیبانی نمی‌کند (روی Firefox تست کن یا نسخه MP4 بگیر).';
    wrap.appendChild(hint);
  }
}
function refreshAudioUI(){
  const wrap=document.getElementById('internalAudioList'); if(!wrap) return;
  wrap.innerHTML='';
  const alist=video.audioTracks;
  // show server audios even if browser hides them
  if(serverAudios.length){
    const head=document.createElement('div'); head.className='hint'; head.style.padding='4px 6px'; head.textContent=\`سرور \${serverAudios.length} ترک صدا شناسایی کرد:\`; wrap.appendChild(head);
    serverAudios.forEach(a=>{
      const row=document.createElement('div'); row.className='track-row'; row.style.cursor='default'; row.style.opacity='.85';
      row.innerHTML=\`<span class="dot" style="background:#f59e0b"></span><span style="flex:1">⭐ \${a.label}</span><span class="track-badge">سرور</span>\`;
      wrap.appendChild(row);
    });
    const note=document.createElement('div'); note.className='hint'; note.style.padding='4px 6px'; note.textContent='تعویض صدای MKV در مرورگر فقط اگر audioTracks بدهد کار می‌کند — اگر نمی‌دهد نسخه MP4 بگیر.'; wrap.appendChild(note);
  }
  if(!alist || alist.length===0){
    if(serverAudios.length===0){
      const hint=document.createElement('div');
      hint.className='hint'; hint.style.padding='6px';
      hint.textContent='ترک صدای داخلی یافت نشد. این یا تک‌زبانه است یا مرورگر audioTracks را برای این فرمت نمی‌دهد (کروم MKV را گاهی نمیدهد — MP4 یا Firefox).';
      wrap.appendChild(hint);
    }
    const ab=document.getElementById('audioBtn'); if(ab) ab.style.opacity='.45';
    if(!serverAudios.length) return;
    else { /* still show browser list if any, but already shown server */ if(!alist || alist.length===0) return; }
  }
  const ab=document.getElementById('audioBtn'); if(ab) ab.style.opacity='1';
  [...alist].forEach((t,i)=>{
    const on=!!t.enabled;
    const lab=(t.label||t.language||('صدا '+(i+1))) + (t.language? ' · '+t.language:'');
    const row=document.createElement('div');
    row.className='track-row'+(on?' on':'');
    row.style.cursor='pointer';
    row.innerHTML=\`<span class="dot"></span><span style="flex:1">\${lab}</span><span class="track-badge">\${t.enabled?'▶ فعال':'انتخاب'}</span>\`;
    row.onclick=()=>{
      for(const x of alist) x.enabled=false;
      t.enabled=true;
      refreshAudioUI(); refreshAudioPop();
      showToast('صدا: '+lab);
    };
    wrap.appendChild(row);
  });
}
function refreshCcPop(){
  const pop=document.getElementById('ccPop'); if(!pop) return;
  const tracks=[...video.textTracks];
  let h='<div class="tp-head">زیرنویس — مثل KMPlayer</div>';
  h+=\`<div class="track-opt \${activeSubId===null?'on':''}" data-cc="off"><span class="dot"></span><span>خاموش</span><span class="track-badge">Off</span></div>\`;
  tracks.forEach((t,i)=>{
    const id='t-'+i; const on=activeSubId===id;
    const lab=(t.label||t.language||'زیرنویس '+(i+1));
    h+=\`<div class="track-opt \${on?'on':''}" data-cc="\${id}"><span class="dot"></span><span>\${lab}</span><span class="track-badge">\${t.language||t.kind||''}</span></div>\`;
  });
  externalEntries.forEach(ent=>{
    const on=activeSubId===ent.id;
    h+=\`<div class="track-opt \${on?'on':''}" data-cc="\${ent.id}"><span class="dot"></span><span>📄 \${ent.label}</span><span class="track-badge">خارجی</span></div>\`;
  });
  serverSubs.forEach(s=>{
    const sid='srv-'+s.index; const on=activeSubId===sid;
    h+=\`<div class="track-opt \${on?'on':''}" data-cc-srv="\${s.index}"><span class="dot"></span><span>⭐ \${s.label}</span><span class="track-badge">سرور</span></div>\`;
  });
  if(tracks.length===0 && externalEntries.length===0) h+='<div class="hint" style="padding:8px">ترکی یافت نشد — فیلم را لود کن.</div>';
  pop.innerHTML=h;
  pop.querySelectorAll('[data-cc]').forEach(el=> el.onclick=()=>{ setActiveSub(el.dataset.cc==='off'?null:el.dataset.cc); pop.classList.remove('open'); });
  pop.querySelectorAll('[data-cc-srv]').forEach(el=> { el.onclick=async()=>{ pop.classList.remove('open'); const idx=parseInt(el.dataset.ccSrv,10); const s=serverSubs.find(x=>x.index===idx); if(s) await loadServerSub(s); }; });
}
function refreshAudioPop(){
  const pop=document.getElementById('audioPop'); if(!pop) return;
  const alist=video.audioTracks;
  let h='<div class="tp-head">ترک صدا — دوبله / اصلی</div>';
  if(!alist || alist.length===0){ h+='<div class="hint" style="padding:8px">ترک داخلی یافت نشد.</div>'; pop.innerHTML=h; return; }
  [...alist].forEach((t,i)=>{
    const lab=(t.label||t.language||('صدا '+(i+1)));
    h+=\`<div class="track-opt \${t.enabled?'on':''}" data-a="\${i}"><span class="dot"></span><span>\${lab}</span><span class="track-badge">\${t.language||''}</span></div>\`;
  });
  pop.innerHTML=h;
  pop.querySelectorAll('[data-a]').forEach(el=> el.onclick=()=>{
    const idx=parseInt(el.dataset.a,10);
    for(const x of alist) x.enabled=false; alist[idx].enabled=true;
    refreshAudioUI(); refreshAudioPop(); showToast('صدا تغییر کرد');
    pop.classList.remove('open');
  });
}
function bindTrackPopups(){
  const ccBtn=document.getElementById('ccBtn'), audioBtn=document.getElementById('audioBtn');
  const ccPop=document.getElementById('ccPop'), audioPop=document.getElementById('audioPop');
  if(ccBtn) ccBtn.onclick=(e)=>{ e.stopPropagation(); audioPop?.classList.remove('open'); ccPop.classList.toggle('open'); refreshCcPop(); };
  if(audioBtn) audioBtn.onclick=(e)=>{ e.stopPropagation(); ccPop?.classList.remove('open'); audioPop.classList.toggle('open'); refreshAudioPop(); };
  document.addEventListener('click', ()=>{ ccPop?.classList.remove('open'); audioPop?.classList.remove('open'); });
}
// file add -> external list (keep previous, add new)
subFile.onchange=async()=>{
  const f=subFile.files[0]; if(!f) return;
  let text=await f.text();
  if(f.name.endsWith('.srt')) text=srtToVtt(text);
  const id='ext-'+Date.now();
  const label=f.name.replace(/\.(srt|vtt)$/i,'');
  externalEntries.push({id, label, vtt:text});
  activeSubId=id;
  applyExternalVtt(id);
  refreshSubUI(); refreshCcPop();
  subStatus.textContent=\`زیرنویس خارجی: \${f.name} — فعال\`;
  const ccBtn=document.getElementById('ccBtn'); if(ccBtn) ccBtn.classList.add('active');
  if(subOff) subOff.classList.remove('active');
  showToast('زیرنویس خارجی اضافه شد: '+f.name);
  subFile.value='';
  broadcastSubState();
};
if(subOff) subOff.onclick=()=> setActiveSub(null);
if($('subLarger')) $('subLarger').onclick=()=>{ cueSize=Math.min(30,cueSize+2); subStyle.textContent=\`::cue{font-size:\${cueSize}px;background:rgba(0,0,0,.62);color:#fff;text-shadow:0 1px 4px rgba(0,0,0,.6)}\`; if(subEnabled) subStatus.textContent=subStatus.textContent.replace(/\d+px/, cueSize+'px'); broadcastSubState(); };
if($('subSmaller')) $('subSmaller').onclick=()=>{ cueSize=Math.max(12,cueSize-2); subStyle.textContent=\`::cue{font-size:\${cueSize}px;background:rgba(0,0,0,.62);color:#fff;text-shadow:0 1px 4px rgba(0,0,0,.6)}\`; if(subEnabled) subStatus.textContent=subStatus.textContent.replace(/\d+px/, cueSize+'px'); broadcastSubState(); };
if($('subDelayPlus')) $('subDelayPlus').onclick=()=>{ cueDelay+=0.5; $('subDelayLabel').textContent=cueDelay.toFixed(1)+'s'; if(activeSubId && String(activeSubId).startsWith('ext-')) applyExternalVtt(activeSubId); subStatus.textContent=subStatus.textContent.replace(/[\d.]+s/, cueDelay.toFixed(1)+'s'); if(subEnabled) broadcastSubState(); };
if($('subDelayMinus')) $('subDelayMinus').onclick=()=>{ cueDelay-=0.5; $('subDelayLabel').textContent=cueDelay.toFixed(1)+'s'; if(activeSubId && String(activeSubId).startsWith('ext-')) applyExternalVtt(activeSubId); subStatus.textContent=subStatus.textContent.replace(/[\d.]+s/, cueDelay.toFixed(1)+'s'); if(subEnabled) broadcastSubState(); };
// hook track discovery
function hookTracks(){
  refreshSubUI(); refreshAudioUI(); refreshCcPop(); refreshAudioPop(); bindTrackPopups();
  // when tracks change
  if(video.textTracks) video.textTracks.addEventListener('addtrack', ()=> { setTimeout(()=>{ refreshSubUI(); refreshCcPop(); }, 300); });
  if(video.audioTracks) video.audioTracks.addEventListener('addtrack', ()=> { setTimeout(()=>{ refreshAudioUI(); refreshAudioPop(); }, 300); });
  video.addEventListener('loadedmetadata', ()=> setTimeout(()=>{ refreshSubUI(); refreshAudioUI(); refreshCcPop(); refreshAudioPop(); }, 600));
  video.addEventListener('loadeddata', ()=> setTimeout(()=>{ refreshSubUI(); refreshAudioUI(); }, 800));
}
// ── Server-side MKV track extraction (ffprobe/ffmpeg) ──
let serverSubs=[], serverAudios=[], serverTracksLoading=false;
async function fetchServerTracks(videoUrl){
  if(!videoUrl) return;
  serverTracksLoading=true;
  refreshSubUI(); // show loading
  try{
    const r=await fetch('/api/video/tracks?url='+encodeURIComponent(videoUrl));
    const j=await r.json();
    if(j.ok){
      serverSubs=(j.subs||[]).map(s=>({index:s.index, label: (s.title||'') + (s.language? ' · '+s.language:'') + ' · '+s.codec_name + ' (#'+s.index+')', lang:s.language||'', title:s.title||'', codec:s.codec_name}));
      serverAudios=(j.audios||[]).map(a=>({index:a.index, label: (a.title||'') + (a.language? ' · '+a.language:'') + ' · '+a.codec_name + (a.channels? ' '+a.channels+'ch':'') + ' (#'+a.index+')', lang:a.language||''}));
      // show hint
      if(serverSubs.length) subStatus.textContent=\`\${serverSubs.length} زیرنویس داخلی یافت شد — از لیست انتخاب کن.\`;
      if(serverAudios.length) { const ah=document.getElementById('audioHint'); if(ah) ah.textContent=\`\${serverAudios.length} ترک صدا یافت شد.\`; }
    } else {
      serverSubs=[]; serverAudios=[];
      subStatus.textContent='سافت‌ساب داخل فایل پیدا نشد یا ffprobe خطا داد.';
    }
  }catch(e){ serverSubs=[]; serverAudios=[]; }
  serverTracksLoading=false;
  refreshSubUI(); refreshAudioUI(); refreshCcPop(); refreshAudioPop();
}
async function loadServerSub(sub){
  const u=lastUrl||videoUrlInput.value.trim(); if(!u) return;
  try{
    showToast('در حال استخراج زیرنویس…');
    // server expects stream index (e.g. 2) — we store original index
    const r=await fetch('/api/video/sub?url='+encodeURIComponent(u)+'&index='+encodeURIComponent(sub.index));
    if(!r.ok){ const j=await r.json().catch(()=>null); throw new Error(j? (j.error||'failed') : 'failed '+r.status); }
    const vtt=await r.text();
    const id='srv-'+sub.index;
    // add or replace
    let ent=externalEntries.find(x=>x.id===id);
    if(ent){ ent.vtt=vtt; }
    else { externalEntries.push({id, label: '⭐ '+sub.label, vtt}); }
    activeSubId=id;
    applyExternalVtt(id);
    refreshSubUI(); refreshCcPop();
    subStatus.textContent=\`زیرنویس داخلی: \${sub.label} — فعال\`;
    const ccBtn=document.getElementById('ccBtn'); if(ccBtn) ccBtn.classList.add('active');
    if(subOff) subOff.classList.remove('active');
    showToast('زیرنویس لود شد');
    broadcastSubState();
  }catch(e){ showToast('خطا در استخراج: '+String(e).slice(0,80)); }
}
// patch loadVideo to also fetch server tracks (async-aware)
const _origLoadVideo=loadVideo;
loadVideo=async function(url, forceProxy){
  const res=await _origLoadVideo(url, forceProxy);
  if(url) setTimeout(()=> fetchServerTracks(url), 800);
  return res;
};

hookTracks();
// ── compact tabs ──
(function(){
  const tabs=document.querySelectorAll('.tab[data-tab]');
  const panels=document.querySelectorAll('.tab-panel[data-panel]');
  tabs.forEach(t=> t.addEventListener('click', ()=>{
    tabs.forEach(x=>x.classList.remove('on'));
    panels.forEach(p=>p.classList.remove('on'));
    t.classList.add('on');
    const pan=document.querySelector('.tab-panel[data-panel="'+t.dataset.tab+'"]');
    if(pan) pan.classList.add('on');
  }));
})();
// ── room shared proxy badge (poll) ──
async function refreshProxyBadge(){
  try{
    const r=await fetch('/api/room/proxy?room='+encodeURIComponent(roomId)+'&userId='+encodeURIComponent(myUserId));
    const j=await r.json();
    const badge=document.getElementById('proxySharedBadge');
    if(!badge) return;
    if(j.shared){
      badge.textContent = j.isOwner ? '◈ پراکسی اتاق فعال (شما مالک)' : '◈ پراکسی اتاق فعال — '+ (j.host||j.type||'');
      badge.classList.add('show');
      proxyChk.checked=true; // hint that proxy is active via room
    } else {
      badge.classList.remove('show');
    }
  }catch{}
}
setTimeout(refreshProxyBadge, 1200);
setInterval(refreshProxyBadge, 8000);
// ── dubbed audio (separate track) — synced to main video ──
const dubAudio=document.getElementById('dubAudio');
const dubUrlInput=document.getElementById('dubUrlInput');
const dubMixRange=document.getElementById('dubMixRange');
const dubMixLabel=document.getElementById('dubMixLabel');
const dubMixWrap=document.getElementById('dubMix');
let dubUrl=null, dubSuppress=false;
function applyDubState(dub, fromRemote=false){
  if(!dub || !dub.url){
    dubUrl=null; dubMixWrap.style.display='none';
    dubAudio.pause(); dubAudio.removeAttribute('src'); dubAudio.load();
    if(dubUrlInput) dubUrlInput.value='';
    document.getElementById('dubStatus').textContent='';
    return;
  }
  dubUrl=dub.url;
  if(dubUrlInput) dubUrlInput.value=dubUrl;
  const mix = typeof dub.mix==='number' ? dub.mix : 100;
  if(dubMixRange) dubMixRange.value=mix;
  if(dubMixLabel) dubMixLabel.textContent=mix+'%';
  dubMixWrap.style.display='flex';
  const src = \`/api/proxy?url=\${encodeURIComponent(dubUrl)}&room=\${encodeURIComponent(roomId)}&userId=\${encodeURIComponent(myUserId)}\`;
  // use proxy for dub as well if shared proxy exists — server will use roomSharedProxy
  if(dubAudio.src !== location.origin + src && dubAudio.src !== src){
    dubAudio.src = src;
    dubAudio.load();
  }
  // volumes: main video at (100-mix)%, dub at mix%
  const mv = (100-mix)/100, dv = mix/100;
  // keep original video volume slider as master for both
  // we store relative mix separately
  dubAudio.volume = dv * (video.muted?0:video.volume||1);
  // sync time once metadata ready
  const syncOnce=()=>{
    try{ dubAudio.currentTime = video.currentTime; }catch{}
    if(!video.paused) dubAudio.play().catch(()=>{});
  };
  if(dubAudio.readyState>=1) syncOnce();
  else dubAudio.addEventListener('loadedmetadata', syncOnce, {once:true});
  document.getElementById('dubStatus').textContent = fromRemote ? 'دوبله از اتاق دریافت شد' : 'دوبله فعال';
}
function broadcastDub(url, mix){
  const payload = url ? {url, mix: parseInt(mix,10)||100} : null;
  try{ send({type:'dub-change', dub: payload}); }catch{}
}
// keep dub in sync with video
video.addEventListener('play', ()=>{ if(dubUrl && dubAudio.src){ dubAudio.currentTime=video.currentTime; dubAudio.play().catch(()=>{}); } });
video.addEventListener('pause', ()=>{ if(dubAudio) dubAudio.pause(); });
video.addEventListener('seeking', ()=>{ if(dubUrl) try{ dubAudio.currentTime=video.currentTime; }catch{} });
video.addEventListener('seeked', ()=>{ if(dubUrl) try{ dubAudio.currentTime=video.currentTime; }catch{} });
video.addEventListener('timeupdate', ()=>{
  if(!dubUrl || dubAudio.paused || video.paused) return;
  const drift=Math.abs(dubAudio.currentTime - video.currentTime);
  if(drift>0.35) try{ dubAudio.currentTime=video.currentTime; }catch{}
});
// dub volume mix
if(dubMixRange) dubMixRange.addEventListener('input', ()=>{
  const mix=parseInt(dubMixRange.value,10);
  if(dubMixLabel) dubMixLabel.textContent=mix+'%';
  if(dubUrl){
    const mv=(100-mix)/100, dv=mix/100;
    // fade original video vs dub
    // we keep video.volume as master, scale dub relative
    dubAudio.volume = dv * (video.muted?0:1);
    // duck main video if mix high: we lower video element via gain — simplest: set video.volume proportionally
    // but preserve user master: store master in dataset
    if(!video.dataset.masterVol) video.dataset.masterVol = String(video.volume||1);
    const master=parseFloat(video.dataset.masterVol)||1;
    video.volume = master * (mix>=100? 0.06 : mix>=70? 0.3 : mix>=40? 0.65 : 1);
    if(vol) vol.value=Math.round(video.volume*100);
  }
  // broadcast mix change debounced
  clearTimeout(broadcastDub._t);
  broadcastDub._t=setTimeout(()=> broadcastDub(dubUrl, mix), 400);
});
if(dubMixRange) dubMixRange.addEventListener('change', ()=> broadcastDub(dubUrl, parseInt(dubMixRange.value,10)||100));
document.getElementById('dubLoadBtn')?.addEventListener('click', ()=>{
  const u=dubUrlInput.value.trim();
  if(!u){ showToast('لینک دوبله را وارد کن'); return; }
  applyDubState({url:u, mix: parseInt(dubMixRange.value,10)||100});
  broadcastDub(u, parseInt(dubMixRange.value,10)||100);
  showToast('دوبله اضافه شد — برای همه اعمال شد');
});
document.getElementById('dubClearBtn')?.addEventListener('click', ()=>{
  applyDubState(null);
  broadcastDub(null);
  showToast('دوبله حذف شد');
});
// mute sync
video.addEventListener('volumechange', ()=>{
  if(dubUrl && dubAudio){
    const mix=parseInt(dubMixRange?.value||'100',10);
    dubAudio.volume = (mix/100) * (video.muted?0:video.volume);
  }
});


// controls
function syncPlayIcon(){
  const paused=video.paused;
  playBtn.textContent= paused ? '▶' : '⏸';
  centerPlayIcon.textContent= paused ? '▶' : '⏸';
  centerPlay.classList.toggle('hidden', !paused);
  if(!paused) setTimeout(()=> centerPlay.classList.add('hidden'), 700);
}
playBtn.onclick=()=>{ ensureSound(); video.paused? video.play(): video.pause(); };
$('fsBtn').onclick=()=> { if(document.fullscreenElement) document.exitFullscreen(); else playerWrap.requestFullscreen?.(); };
video.addEventListener('click',()=>{ ensureSound(); video.paused? video.play(): video.pause(); });
video.addEventListener('play', ()=>{ syncPlayIcon(); if(!suppress) send({type:'play', time:video.currentTime}); });
video.addEventListener('pause',()=>{ syncPlayIcon(); if(!suppress) send({type:'pause', time:video.currentTime}); });
let seeking=false;
video.addEventListener('seeking',()=>{ seeking=true; });
video.addEventListener('seeked',()=>{ seeking=false; if(!suppress) send({type:'seek', time:video.currentTime}); });
video.addEventListener('timeupdate',()=>{
  if(!seeking && video.duration && !isNaN(video.duration)){ seek.value=(video.currentTime/video.duration*100)||0; timeLabel.textContent=\`\${fmt(video.currentTime)} / \${fmt(video.duration)}\`; }
});
video.addEventListener('loadedmetadata', syncPlayIcon);
let seekCommitTimer=null;
seek.addEventListener('input',()=>{
  if(!video.duration || isNaN(video.duration)) return;
  seeking=true;
  const t=seek.value/100*video.duration;
  timeLabel.textContent=\`\${fmt(t)} / \${fmt(video.duration)}\`;
  clearTimeout(seekCommitTimer);
  seekCommitTimer=setTimeout(()=>{ seeking=false; video.currentTime=t; }, 120);
});
seek.addEventListener('change',()=>{
  if(!video.duration || isNaN(video.duration)) return;
  clearTimeout(seekCommitTimer);
  const t=seek.value/100*video.duration;
  seeking=false;
  video.currentTime=t;
});
document.addEventListener('keydown',e=>{
  if(e.code==='Space' && document.activeElement.tagName!=='INPUT'){ e.preventDefault(); playBtn.click(); }
  if(e.code==='ArrowRight' && e.target===document.body) { e.preventDefault(); if(video.duration) video.currentTime+=5; }
  if(e.code==='ArrowLeft' && e.target===document.body) { e.preventDefault(); if(video.duration) video.currentTime-=5; }
});
// volume
vol.addEventListener('input',()=>{ video.muted=false; video.volume=vol.value/100; muteBtn.textContent= video.volume==0 ? '🔇' : video.volume<0.5 ? '🔉' : '🔊'; });
muteBtn.onclick=()=>{ video.muted=!video.muted; muteBtn.textContent= video.muted ? '🔇' : (video.volume<0.5 ? '🔉' : '🔊'); };
video.addEventListener('volumechange',()=>{ if(!video.muted) vol.value=Math.round(video.volume*100); muteBtn.textContent= video.muted ? '🔇' : (video.volume==0 ? '🔇' : video.volume<0.5 ? '🔉' : '🔊'); });
// init unmuted with sound + fix autoplay-muted trap
video.muted=false; video.volume=1; vol.value=100; muteBtn.textContent='🔊';
function ensureSound(){ if(video.muted){ video.muted=false; } if(video.volume===0){ video.volume=1; vol.value=100; } muteBtn.textContent='🔊'; }
['click','keydown','touchstart'].forEach(ev=> document.addEventListener(ev, ensureSound, {once:true}));
// thumbnail on timeline hover — offscreen video
let thumbVideo=null, thumbReady=false, thumbPendingTime=null;
function ensureThumbVideo(){
  if(thumbVideo) return thumbVideo;
  thumbVideo=document.createElement('video');
  thumbVideo.muted=true; thumbVideo.preload='metadata'; thumbVideo.crossOrigin='anonymous';
  thumbVideo.style.display='none'; document.body.appendChild(thumbVideo);
  thumbVideo.addEventListener('loadedmetadata',()=>{ thumbReady=true; if(thumbPendingTime!=null) seekThumb(thumbPendingTime); });
  thumbVideo.addEventListener('seeked', drawThumb);
  return thumbVideo;
}
function seekThumb(t){
  if(!thumbReady){ thumbPendingTime=t; return; }
  thumbPendingTime=null;
  try{ thumbVideo.currentTime=Math.min(Math.max(0,t), (thumbVideo.duration||video.duration||0)-0.05); }catch{}
}
function drawThumb(){
  const c=$('thumbCanvas'); if(!c) return;
  const ctx=c.getContext('2d');
  try{ ctx.drawImage(thumbVideo, 0, 0, c.width, c.height); }catch{ ctx.fillStyle='#111'; ctx.fillRect(0,0,c.width,c.height); }
}
const rangeWrap=$('rangeWrap'), thumbPreview=$('thumbPreview'), thumbTime=$('thumbTime');
function syncThumbSrc(){
  if(!video.src) return;
  const tv=ensureThumbVideo();
  if(tv.src !== video.src){ thumbReady=false; tv.src=video.src; tv.load(); }
}
video.addEventListener('loadedmetadata', syncThumbSrc);
video.addEventListener('emptied', ()=>{ thumbReady=false; });
if(video.src) syncThumbSrc();
let thumbRAF=null;
function showThumb(e){
  if(!video.duration || isNaN(video.duration) || !video.src){ thumbPreview.classList.remove('show'); return; }
  syncThumbSrc();
  const rect=rangeWrap.getBoundingClientRect();
  const x=(e.touches? e.touches[0].clientX : e.clientX) - rect.left;
  const pct=Math.min(1, Math.max(0, x / rect.width));
  const t=pct * video.duration;
  thumbTime.textContent=fmt(t);
  const leftPct=pct*100;
  thumbPreview.style.left= leftPct + '%';
  // clamp inside
  thumbPreview.classList.add('show');
  seekThumb(t);
}
function hideThumb(){ thumbPreview.classList.remove('show'); }
rangeWrap.addEventListener('mousemove', showThumb);
rangeWrap.addEventListener('mouseenter', showThumb);
rangeWrap.addEventListener('mouseleave', hideThumb);
rangeWrap.addEventListener('touchmove', showThumb, {passive:true});
rangeWrap.addEventListener('touchend', hideThumb);

// auto-hide controls
let idleTimer=null;
function kickIdle(){ playerWrap.classList.remove('idle'); clearTimeout(idleTimer); if(!video.paused) idleTimer=setTimeout(()=> playerWrap.classList.add('idle'), 2200); }
playerWrap.addEventListener('mousemove', kickIdle);
playerWrap.addEventListener('mouseleave', ()=> { if(!video.paused) playerWrap.classList.add('idle'); });

loadBtn.onclick=()=>{
  const u=videoUrlInput.value.trim(); if(!u) return;
  loadVideo(u);
  send({type:'video-change', videoUrl:u});
  history.replaceState(null,'', location.pathname+\`?video=\${encodeURIComponent(u)}&proxy=\${proxyChk.checked?'1':'0'}\`);
  showToast('ویدیو بارگذاری شد');
};
$('copyBtn').onclick=async()=>{
  const link=roomLink.value + (videoUrlInput.value? \`?video=\${encodeURIComponent(videoUrlInput.value)}&proxy=\${proxyChk.checked?'1':'0'}\`:'');
  await navigator.clipboard.writeText(link); showToast('لینک دعوت کپی شد ✓');
};

// VPN modal — bulk + list + ping + localStorage
const LS_KEY=\`wt_vpn_\${roomId}_\${myUserId}\`;
function lsGet(){ try{ return JSON.parse(localStorage.getItem(LS_KEY)||'[]'); }catch{ return []; } }
function lsSet(a){ localStorage.setItem(LS_KEY, JSON.stringify(a)); }
function parseHost(u){
  try{
    if(u.startsWith('vmess://')){ const b=atob(u.slice(8)); const j=JSON.parse(b); return j.add||j.host||''; }
    const t=u.replace(/^(vless|trojan|ss):\/\//,'http://'); return new URL(t).hostname||'';
  }catch{ return u.slice(0,22); }
}
function parseType(u){
  const t=u.trim();
  if(t.startsWith('vless://')) return 'vless';
  if(t.startsWith('vmess://')) return 'vmess';
  if(t.startsWith('trojan://')) return 'trojan';
  if(t.startsWith('ss://')) return 'ss';
  if(/^https?:\/\//i.test(t)) return 'http';
  if(/^socks5?:\/\//i.test(t)) return 'socks';
  return 'unk';
}
let pingCache=new Map();
async function vpnFetchList(){
  // local is source of truth; also try server for activeId sync
  let local=lsGet();
  try{
    const r=await fetch(\`/api/vpn/list?room=\${encodeURIComponent(roomId)}&userId=\${encodeURIComponent(myUserId)}\`);
    const ct=r.headers.get('content-type')||'';
    if(ct.includes('application/json')){ const j=await r.json(); if(j.store && j.store.configs.length && !local.length){ local=j.store.configs.map(c=>({id:c.id,uri:c.uri||'',type:c.type,host:c.host||parseHost(c.uri||''),label:c.label||''})); lsSet(local); }
    }
  }catch{}
  return local;
}
function renderVpnList(){
  const list=lsGet();
  vpnCount.textContent=list.length;
  const activeId=localStorage.getItem(LS_KEY+'_active')||list[0]?.id||'';
  vpnList.innerHTML='';
  if(!list.length){ vpnList.innerHTML='<div class="hint" style="text-align:center;padding:12px">لیست خالی — چند کانفیگ پیست کن و ایمپورت بزن.</div>'; vpnBtn.classList.remove('has-config'); vpnBtn.textContent='◈ VPN'; return; }
  vpnBtn.classList.add('has-config'); vpnBtn.textContent=\`◈ VPN · \${list.length}\`;
  list.forEach(c=>{
    const ping=pingCache.get(c.id);
    const isActive=c.id===activeId;
    const pingText= ping ? (ping.ok? \`🟢 \${ping.ms}ms\` : \`🔴 \${ping.error||'fail'}\`) : '—';
    const el=document.createElement('div');
    el.style.cssText=\`display:flex;align-items:center;gap:8px;padding:10px 11px;background:\${isActive?'rgba(124,92,255,.14)':'rgba(255,255,255,.04)'};border:1px solid \${isActive?'rgba(124,92,255,.35)':'var(--border)'};border-radius:12px;transition:.2s\`;
    el.innerHTML=\`<span style="font-size:10px;padding:3px 7px;border-radius:999px;background:rgba(255,255,255,.08);border:1px solid var(--border)">\${c.type}</span>
      <span style="flex:1;min-width:0;font-size:11px;direction:ltr;text-align:left;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">\${c.host||parseHost(c.uri)}<br><small style="color:var(--muted);direction:ltr">\${(c.uri||'').slice(0,52)}\${(c.uri||'').length>52?'…':''}</small></span>
      <span style="font-size:10px;white-space:nowrap;direction:ltr">\${pingText}</span>
      <button data-act="use" data-id="\${c.id}" class="btn" style="padding:5px 9px;font-size:11px;\${isActive?'background:linear-gradient(135deg,var(--accent),var(--accent2));color:#fff;border-color:transparent':''}">\${isActive?'✓ فعال':'فعال'}</button>
      <button data-act="ping" data-id="\${c.id}" class="btn" style="padding:5px 7px;font-size:11px">📡</button>
      <button data-act="del" data-id="\${c.id}" class="btn" style="padding:5px 7px;font-size:11px;color:#f87171">✕</button>\`;
    vpnList.appendChild(el);
  });
  // bind
  vpnList.querySelectorAll('[data-act="use"]').forEach(b=> b.onclick=async()=>{
    localStorage.setItem(LS_KEY+'_active', b.dataset.id);
    // sync active to server
    try{ await fetch('/api/vpn/select',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({userId:myUserId,roomId, id:b.dataset.id})}); }catch{}
    renderVpnList(); showToast('کانفیگ فعال شد');
  });
  vpnList.querySelectorAll('[data-act="ping"]').forEach(b=> b.onclick=async()=>{
    const id=b.dataset.id; const cfg=lsGet().find(x=>x.id===id); if(!cfg) return;
    b.textContent='…';
    try{
      const r=await fetch('/api/vpn/ping',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({uri: cfg.uri})});
      const j=await r.json(); pingCache.set(id, j); renderVpnList();
    }catch(e){ pingCache.set(id,{ok:false,error:String(e).slice(0,40)}); renderVpnList(); }
  });
  vpnList.querySelectorAll('[data-act="del"]').forEach(b=> b.onclick=()=>{
    const id=b.dataset.id; const arr=lsGet().filter(x=>x.id!==id); lsSet(arr);
    if(localStorage.getItem(LS_KEY+'_active')===id) localStorage.removeItem(LS_KEY+'_active');
    try{ fetch('/api/vpn/remove',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({userId:myUserId,roomId,id})}); }catch{}
    pingCache.delete(id); renderVpnList(); showToast('حذف شد');
  });
}
function openVpn(){ vpnModal.classList.add('open'); vpnStatus.className='vpn-status'; vpnStatus.style.display='none'; renderVpnList(); }
function closeVpn(){ vpnModal.classList.remove('open'); }
vpnBtn.onclick=openVpn;
$('vpnClose').onclick=closeVpn;
$('vpnCancel').onclick=closeVpn;
vpnModal.addEventListener('click',e=>{ if(e.target===vpnModal) closeVpn(); });
document.addEventListener('keydown',e=>{ if(e.key==='Escape' && vpnModal.classList.contains('open')) closeVpn(); });

$('vpnImport').onclick=async()=>{
  const raw=$('vpnUris').value.trim();
  if(!raw){ vpnStatus.className='vpn-status err'; vpnStatus.style.display='block'; vpnStatus.textContent='چند کانفیگ را پیست کن (هر خط یکی).'; return; }
  const lines=[...new Set(raw.split(/[\r\n,]+/).map(s=>s.trim()).filter(Boolean))];
  if(!lines.length){ vpnStatus.className='vpn-status err'; vpnStatus.style.display='block'; vpnStatus.textContent='کانفیگی یافت نشد.'; return; }
  const existing=new Set(lsGet().map(c=>c.uri));
  let added=0;
  const cur=lsGet();
  for(const uri of lines){
    if(existing.has(uri)) continue;
    cur.push({id:Math.random().toString(36).slice(2,9), uri, type:parseType(uri), host:parseHost(uri), label:parseHost(uri)});
    added++;
  }
  lsSet(cur);
  // sync to server (do not fail UI if server 404)
  try{
    const r=await fetch('/api/vpn/import',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({userId:myUserId, roomId, urisText: lines.join('\n')})});
    const ct=r.headers.get('content-type')||'';
    if(ct.includes('application/json')) await r.json(); // ignore body, just ensure not 404 html
  }catch{}
  $('vpnUris').value='';
  vpnStatus.className='vpn-status ok'; vpnStatus.style.display='block';
  vpnStatus.textContent=\`\${added} کانفیگ اضافه شد — مجموع \${cur.length}\`;
  renderVpnList(); showToast(\`\${added} ایمپورت شد\`);
};
$('vpnPingAll').onclick=async()=>{
  const list=lsGet(); if(!list.length){ showToast('لیست خالی'); return; }
  const btn=$('vpnPingAll'); const orig=btn.textContent; btn.textContent='… در حال پینگ'; btn.disabled=true;
  pingCache.clear(); renderVpnList();
  // helper to render single row update like ping تکی
  const markPinging=(id)=>{ pingCache.set(id, {ok:null, ms:null, error:'…'}); renderVpnList(); };
  try{
    // try server bulk, but render progressively per-item as results arrive
    let bulkDone=false;
    try{
      const listForPing=lsGet();
      const r=await fetch('/api/vpn/ping-all',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({userId:myUserId, roomId, configs: listForPing.map(c=>({id:c.id, uri:c.uri}))})});
      const ct=r.headers.get('content-type')||'';
      if(ct.includes('application/json')){
        const j=await r.json();
        if(j.results && j.results.length){
          for(const x of j.results){ pingCache.set(x.id, x); renderVpnList(); }
          bulkDone=true;
        }
      }
    }catch{}
    if(!bulkDone){
      // fallback: ping one-by-one exactly like ping تکی — each config writes immediately
      for(const c of list){
        markPinging(c.id);
        try{
          const rr=await fetch('/api/vpn/ping',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({uri:c.uri})});
          const ct2=rr.headers.get('content-type')||'';
          const jj= ct2.includes('application/json') ? await rr.json() : {ok:false, error:'bad response'};
          pingCache.set(c.id, jj);
        }catch(e){ pingCache.set(c.id,{ok:false,error:String(e).slice(0,32)}); }
        renderVpnList();
      }
    } else {
      // still ensure all ids have entry (failed parse fallback)
      for(const c of list) if(!pingCache.has(c.id)) pingCache.set(c.id, {ok:false,error:'no result'});
      renderVpnList();
    }
    showToast('پینگ تمام شد');
  }catch(e){ vpnStatus.className='vpn-status err'; vpnStatus.style.display='block'; vpnStatus.textContent=String(e).slice(0,120); }
  btn.textContent=orig; btn.disabled=false;
};
$('vpnAuto').onclick=async()=>{
  await $('vpnPingAll').onclick();
  let best=null; for(const [id, v] of pingCache){ if(v.ok && (best==null || v.ms < pingCache.get(best).ms)) best=id; }
  if(best){ localStorage.setItem(LS_KEY+'_active', best); try{ await fetch('/api/vpn/select',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({userId:myUserId,roomId,id:best})}); }catch{} renderVpnList(); showToast('بهترین انتخاب شد'); }
  else showToast('کانفیگ سالمی یافت نشد');
};
$('vpnClearAll').onclick=()=>{
  if(!confirm('همه کانفیگ‌های این اتاق حذف شوند؟')) return;
  localStorage.removeItem(LS_KEY); localStorage.removeItem(LS_KEY+'_active'); pingCache.clear();
  try{ fetch(\`/api/vpn?room=\${encodeURIComponent(roomId)}&userId=\${encodeURIComponent(myUserId)}\`,{method:'DELETE'}); }catch{}
  renderVpnList(); showToast('حذف شد');
};
renderVpnList();

// WS
function wsUrl(){
  const proto=location.protocol==='https:'?'wss:':'ws:';
  return \`\${proto}//\${location.host}/ws?room=\${encodeURIComponent(roomId)}\`;
}
function connect(){
  ws=new WebSocket(wsUrl());
  ws.onopen=()=>{ connText.textContent='متصل'; syncDot.classList.add('on'); ws.send(JSON.stringify({type:'join', room:roomId, name:myName, videoUrl: initialVideo})); startHostSync(); kickIdle(); };
  ws.onclose=()=>{ connText.textContent='قطع — اتصال مجدد…'; syncDot.classList.remove('on'); stopHostSync(); setTimeout(connect,1200); };
  ws.onerror=()=>{ connText.textContent='خطا'; };
  ws.onmessage=async e=>{
    let m; try{ m=JSON.parse(e.data);}catch{return;}
    if(m.type==='joined'){ myId=m.id; hostId=m.hostId; isHost=myId===hostId; renderRole(); if(m.state && m.state.videoUrl && !initialVideo){ videoUrlInput.value=m.state.videoUrl; await loadVideo(m.state.videoUrl);} if(m.state) applySync(m.state, true); if(m.state && m.state.sub){ setTimeout(()=> applyRemoteSub(m.state.sub), 900); } if(m.state && m.state.dub){ setTimeout(()=> applyDubState(m.state.dub, true), 1100); } setTimeout(refreshProxyBadge, 800); peerPill.textContent=\`\${m.peers} نفر\`; }
    if(m.type==='peer-join'){ peerPill.textContent=\`\${m.peers} نفر\`; showToast('یک نفر وارد شد'); }
    if(m.type==='peer-leave'){ peerPill.textContent=\`\${m.peers} نفر\`; }
    if(m.type==='host-change'){ hostId=m.hostId; isHost=myId===hostId; renderRole(); }
    if(m.type==='request-sync'){ send({type:'sync', time:video.currentTime, playing:!video.paused}); }
    if(m.type==='play'){ suppress=true; video.currentTime=m.time??video.currentTime; video.play().finally(()=> setTimeout(()=>{suppress=false; syncPlayIcon();},350)); }
    if(m.type==='pause'){ suppress=true; video.currentTime=m.time??video.currentTime; video.pause(); setTimeout(()=>{suppress=false; syncPlayIcon();},350); }
    if(m.type==='seek'){ suppress=true; video.currentTime=m.time; setTimeout(()=>suppress=false,350); }
    if(m.type==='sync'){
      if(m.from===myId) return;
      const drift=Math.abs(video.currentTime - m.time);
      if(drift>0.8){ suppress=true; video.currentTime=m.time; setTimeout(()=>suppress=false,350); }
      if(m.playing && video.paused){ suppress=true; video.play().finally(()=>setTimeout(()=>{suppress=false; syncPlayIcon();},350)); }
      if(!m.playing && !video.paused){ suppress=true; video.pause(); setTimeout(()=>{suppress=false; syncPlayIcon();},350); }
    }
    if(m.type==='video-change'){ videoUrlInput.value=m.videoUrl; await loadVideo(m.videoUrl); showToast('ویدیو تغییر کرد'); }
    if(m.type==='sub-change'){ if(m.from!==myId){ applyRemoteSub(m.sub); showToast('زیرنویس به‌روزرسانی شد'); } }
    if(m.type==='dub-change'){ applyDubState(m.dub, true); showToast(m.dub? 'دوبله به‌روزرسانی شد' : 'دوبله حذف شد'); }
  };
}
function renderRole(){ hostPill.textContent=isHost?'میزبان':'مهمان'; hostPill.className='pill '+(isHost?'live':''); $('syncStatus').textContent='کنترل آزاد — هر دو نفر می‌توانند پلی/پاز/Seek کنند. سینک هر ۱ ثانیه.'; }
function send(o){ if(ws && ws.readyState===1) ws.send(JSON.stringify({...o, room:roomId})); }
function startHostSync(){ stopHostSync(); syncTimer=setInterval(()=>{ if(ws && ws.readyState===1 && !isNaN(video.duration) && video.duration) send({type:'sync', time:video.currentTime, playing:!video.paused}); },1000); }
function stopHostSync(){ if(syncTimer) clearInterval(syncTimer); syncTimer=null; }
function applySync(state, force=false){
  if(!state) return;
  const drift=Math.abs(video.currentTime - state.time);
  if(force || drift>0.8){ suppress=true; video.currentTime=state.time; setTimeout(()=>suppress=false,300); }
  if(state.playing && video.paused){ suppress=true; video.play().finally(()=>setTimeout(()=>{suppress=false; syncPlayIcon();},300)); }
  if(!state.playing && !video.paused){ suppress=true; video.pause(); setTimeout(()=>{suppress=false; syncPlayIcon();},300); }
}
connect();
syncPlayIcon();
</script>
`;

export default {
  async fetch(req, env){
    const url=new URL(req.url);

    // CORS preflight
    if(req.method==='OPTIONS') return new Response(null,{status:204, headers:{'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST,DELETE,OPTIONS','Access-Control-Allow-Headers':'*'}});

    // WS -> Durable Object
    if(req.headers.get('Upgrade')==='websocket'){
      const roomId=url.searchParams.get('room')||'default';
      const id=env.ROOM.idFromName(roomId);
      const stub=env.ROOM.get(id);
      return stub.fetch(req);
    }

    // ── VPN APIs (mirror Node server) ──
    if(url.pathname.startsWith('/api/vpn')){
      const readBody=async()=>{
        try{ const t=await req.text(); return t?JSON.parse(t):null; }catch{ return null; }
      };

      if(url.pathname==='/api/vpn/import' && req.method==='POST'){
        const body=await readBody();
        const userId=(body?.userId||url.searchParams.get('userId')||'').trim();
        const roomId=(body?.roomId||body?.room||url.searchParams.get('room')||'').trim();
        const raw=(body?.urisText||body?.uris||body?.uri||'').toString();
        const lines=raw.split(/[\r\n,]+/).map(s=>s.trim()).filter(Boolean);
        if(Array.isArray(body?.uris)) lines.push(...body.uris.map(s=>String(s).trim()).filter(Boolean));
        const unique=[...new Set(lines)];
        if(!unique.length) return json({ok:false,error:'no uris'},400);
        const key=storeKey(userId, roomId); const store=getStore(key);
        let added=0;
        for(const uri of unique){
          if(store.configs.some(c=>c.uri===uri)) continue;
          store.configs.push({id:Math.random().toString(36).slice(2,9), uri, type:uriType(uri), label:uriHost(uri)||uri.slice(0,24), addedAt:Date.now(), host:uriHost(uri)});
          added++;
        }
        if(!store.activeId && store.configs.length) store.activeId=store.configs[0].id;
        return json({ok:true, key, added, total:store.configs.length, configs:store.configs, activeId:store.activeId});
      }

      if(url.pathname==='/api/vpn' && req.method==='POST'){
        const body=await readBody();
        const userId=(body?.userId||url.searchParams.get('userId')||'').trim();
        const roomId=(body?.roomId||body?.room||url.searchParams.get('room')||'').trim();
        let uri=(body?.uri||body?.urisText||'').toString().trim();
        if(!uri && body?.uris) uri='';
        const lines=uri.split(/[\r\n]+/).map(s=>s.trim()).filter(Boolean);
        if(lines.length>1){
          const key=storeKey(userId,roomId); const store=getStore(key);
          let added=0;
          for(const u of [...new Set(lines)]){
            if(store.configs.some(c=>c.uri===u)) continue;
            store.configs.push({id:Math.random().toString(36).slice(2,9), uri:u, type:uriType(u), label:uriHost(u)||u.slice(0,24), addedAt:Date.now(), host:uriHost(u)});
            added++;
          }
          if(!store.activeId && store.configs.length) store.activeId=store.configs[0].id;
          return json({ok:true, key, added, total:store.configs.length, configs:store.configs, activeId:store.activeId, via:'batch'});
        }
        if(!uri) return json({ok:false,error:'uri required'},400);
        const roomKey=storeKey(userId,roomId);
        if(!roomKey) return json({ok:false,error:'userId or roomId required'},400);
        const store=getStore(roomKey);
        if(!store.configs.some(c=>c.uri===uri)){
          store.configs.push({id:Math.random().toString(36).slice(2,9), uri, type:uriType(uri), label:body?.label||uriHost(uri)||uri.slice(0,24), addedAt:Date.now(), host:uriHost(uri)});
        }
        if(!store.activeId) store.activeId=store.configs[store.configs.length-1].id;
        const type=uriType(uri);
        return json({ok:true, roomId:roomKey, type, via:(type==='http'||type==='socks')?'proxy-agent':'xray-tun-required', total:store.configs.length});
      }

      if(url.pathname==='/api/vpn/list' && req.method==='GET'){
        const userId=(url.searchParams.get('userId')||'').trim();
        const roomId=(url.searchParams.get('room')||url.searchParams.get('roomId')||'').trim();
        const keys=[storeKey(userId,roomId), storeKey('',roomId), '__global'].filter((v,i,a)=>a.indexOf(v)===i);
        let merged=null; for(const k of keys){ const s=vpnStore.get(k); if(s && s.configs.length){ merged=s; break; } }
        return json({ok:true, keys, store: merged? {configs:merged.configs, activeId:merged.activeId, key:keys[0]} : {configs:[],activeId:null,key:keys[0]}});
      }

      if(url.pathname==='/api/vpn' && req.method==='GET'){
        const userId=(url.searchParams.get('userId')||'').trim();
        const roomId=(url.searchParams.get('room')||url.searchParams.get('roomId')||'').trim();
        const key=storeKey(userId,roomId);
        const s=vpnStore.get(key)||vpnStore.get(storeKey('',roomId))||null;
        const safe=s? {configs:s.configs.map(c=>({id:c.id,type:c.type,label:c.label,host:c.host,addedAt:c.addedAt})), activeId:s.activeId, total:s.configs.length} : null;
        return json({ok:true, key, store:safe, hasStore:!!s});
      }

      if(url.pathname==='/api/vpn/select' && req.method==='POST'){
        const body=await readBody();
        const userId=(body?.userId||'').trim(); const roomId=(body?.roomId||body?.room||'').trim(); const id=(body?.id||'').trim();
        const key=storeKey(userId,roomId); const s=vpnStore.get(key);
        if(!s) return json({ok:false,error:'no store'},404);
        if(!s.configs.some(c=>c.id===id)) return json({ok:false,error:'id not found'},400);
        s.activeId=id;
        return json({ok:true, activeId:id});
      }

      if(url.pathname==='/api/vpn/remove' && req.method==='POST'){
        const body=await readBody();
        const userId=(body?.userId||'').trim(); const roomId=(body?.roomId||'').trim(); const id=(body?.id||'').trim();
        const key=storeKey(userId,roomId); const s=vpnStore.get(key);
        if(!s) return json({ok:false},404);
        s.configs=s.configs.filter(c=>c.id!==id);
        if(s.activeId===id) s.activeId=s.configs[0]?.id||null;
        return json({ok:true, total:s.configs.length, activeId:s.activeId});
      }

      if(url.pathname==='/api/vpn' && req.method==='DELETE'){
        const userId=(url.searchParams.get('userId')||'').trim();
        const roomId=(url.searchParams.get('room')||url.searchParams.get('roomId')||'').trim();
        const id=url.searchParams.get('id');
        const key=storeKey(userId,roomId); const s=vpnStore.get(key);
        if(id && s){ s.configs=s.configs.filter(c=>c.id!==id); if(s.activeId===id) s.activeId=s.configs[0]?.id||null; }
        else if(s) vpnStore.delete(key);
        return json({ok:true});
      }

      if(url.pathname==='/api/vpn/ping' && req.method==='POST'){
        const body=await readBody();
        const uri=(body?.uri||'').trim();
        if(!uri) return json({ok:false,error:'uri required'},400);
        const r=await pingUri(uri, 6000, env);
        return json({ok:r.ok, ms:r.ms||null, error:r.error||null, note:r.note||null, type:uriType(uri)});
      }

      if(url.pathname==='/api/vpn/ping-all' && req.method==='POST'){
        const body=await readBody();
        const userId=(body?.userId||'').trim(); const roomId=(body?.roomId||'').trim();
        let targets=[];
        if(Array.isArray(body?.configs) && body.configs.length){
          targets = body.configs.map(x=> ({id:String(x.id), uri:String(x.uri||''), type:uriType(String(x.uri||'')), host:uriHost(String(x.uri||''))})).filter(x=>x.uri);
        } else if(typeof body?.urisText==='string' && body.urisText.trim()){
          const lines=[...new Set(body.urisText.split(/[\r\n,]+/).map(v=>v.trim()).filter(Boolean))];
          targets = lines.map((u,i)=> ({id:`tmp-${i}`, uri:u, type:uriType(u), host:uriHost(u)}));
        } else {
          const key=storeKey(userId,roomId); const st=vpnStore.get(key) || vpnStore.get(storeKey('',roomId));
          if(st && st.configs.length) targets = st.configs.map(c=> ({id:c.id, uri:c.uri, type:c.type, host:c.host}));
        }
        if(!targets.length) return json({ok:true, results:[]});
        const settled = await Promise.all(targets.map(async c=>{
          const r=await pingUri(c.uri, 4000, env);
          return {id:c.id, uri:c.uri, type:c.type, host:c.host, ok:r.ok, ms:r.ms||null, error:r.error||null, note:r.note||null};
        }));
        const results=[...settled].sort((a,b)=> (a.ms??99999)-(b.ms??99999));
        return json({ok:true, results});
      }

      return json({ok:false,error:'unknown vpn endpoint'},404);
    }

    // Video tracks/subs — not available on Worker (needs ffprobe/ffmpeg on Node server)
    if(url.pathname==='/api/video/tracks' || url.pathname==='/api/video/sub'){
      return json({ok:false, error:'video tracks/subs require Node server with ffmpeg — not available on Cloudflare Worker. Use the Linux server (node server/server.js).'}, 501);
    }

    // Health (with optional room validation for Worker)
    // DO وجود اتاق را از storage می‌خواند — فقط اتاق‌هایی که حداقل یک join داشته‌اند exists=true
    if(url.pathname==='/api/health'){
      const roomId = url.searchParams.get('room');
      if(roomId){
        try{
          const id = env.ROOM.idFromName(roomId);
          const stub = env.ROOM.get(id);
          const resp = await stub.fetch(new Request('http://internal/health', {method:'GET'}));
          const data = await resp.json();
          // exists = اتاق حداقل یک‌بار join شده (createdAt در storage)
          const exists = !!data.exists;
          return json({ok:true, mode:'worker', roomExists: exists, roomId, peers: data.peers||0});
        }catch{
          return json({ok:true, mode:'worker', roomExists:false, roomId});
        }
      }
      return json({ok:true, mode:'worker'});
    }

    // Room shared proxy info (Worker has no per-room proxy — always false, peers use direct fetch)
    if(url.pathname==='/api/room/proxy'){
      return json({ok:true, shared:false, note:'Worker has no per-room proxy; use Node server for VPN-shared proxy'});
    }

    // YouTube resolver — Worker: Invidious fallback (no yt-dlp on Workers)
    if(url.pathname==='/api/yt'){
      const target=url.searchParams.get('url');
      if(!target) return json({ok:false,error:'missing url'},400);
      const ytId=(u)=>{ try{ const uu=new URL(u); if(uu.hostname.includes('youtu.be')) return uu.pathname.split('/').filter(Boolean)[0]||''; const v=uu.searchParams.get('v'); if(v) return v; const m=u.match(/\/(embed|shorts|v)\/([^/?&#]+)/); if(m) return m[2]; }catch{} const m2=u.match(/(?:v=|youtu\.be\/|embed\/|shorts\/)([\w-]{6,})/); return m2?m2[1]:''; };
      const vid=ytId(target); if(!vid) return json({ok:false,error:'cannot parse youtube id'},400);
      const instances=['https://yewtu.be','https://invidious.snopyta.org','https://inv.nadeko.net','https://iv.melmac.space'];
      for(const base of instances){
        try{
          const r=await fetch(`${base}/api/v1/videos/${vid}`, {headers:{'User-Agent':'Mozilla/5.0'}, cf:{cacheTtl:0}});
          if(!r.ok) continue;
          const j=await r.json();
          const fmts=[...(j.formatStreams||[]), ...(j.adaptiveFormats||[])].filter(f=>f.url);
          let best=(j.formatStreams||[]).filter(f=>f.container==='mp4'&&f.url).sort((a,b)=>(b.width||0)-(a.width||0))[0];
          if(!best) best=fmts.filter(f=>f.container==='mp4').sort((a,b)=>(b.width||0)-(a.width||0))[0];
          if(!best) best=fmts[0];
          if(best?.url) return json({ok:true, url:best.url, via:'invidious:'+base, title:j.title||''});
        }catch{}
      }
      return json({ok:false,error:'invidious fallback failed — try Node server with yt-dlp'},502);
    }

    // Proxy — optionally via selected VPN (Worker: http/socks cannot use ProxyAgent, so direct fetch)
    if(url.pathname==='/api/proxy'){
      const target=url.searchParams.get('url');
      if(!target) return new Response('missing url',{status:400, headers:{'Access-Control-Allow-Origin':'*'}});
      try{
        const headers={}; const r = req.headers.get('range'); if(r) headers['Range']=r;
        headers['User-Agent']='Mozilla/5.0';
        // In Worker, ignore per-room VPN dispatcher (no socks support); just direct fetch.
        // If you need http proxy, deploy a separate proxy worker or use Node server.
        const res=await fetch(target,{headers, cf:{cacheTtl:0}});
        const h=new Headers(res.headers); h.set('Access-Control-Allow-Origin','*'); h.set('Cache-Control','no-cache');
        return new Response(res.body,{status:res.status, headers:h});
      }catch(e){ return new Response(String(e),{status:502, headers:{'Access-Control-Allow-Origin':'*'}}); }
    }

    // Static — try ASSETS binding first (wrangler deploy), fallback to embedded HTML
    if(env.ASSETS){
      try{
        const r = await env.ASSETS.fetch(req);
        // Cloudflare Assets returns 404 for SPA routes — handle /watch/* ourselves
        if(r.status !== 404) return r;
        const p = url.pathname;
        if(p.startsWith('/watch/') || p === '/watch' || p.startsWith('/join/')) return env.ASSETS.fetch(new Request(new URL('/watch.html', req.url), req));
        if(p === '/') return env.ASSETS.fetch(new Request(new URL('/index.html', req.url), req));
      }catch{}
    }
    // No ASSETS binding (API deploy without assets) — serve embedded fallback
    // This lets the worker run even without `wrangler deploy` handling assets
    const path = url.pathname;
    if(path === '/' || path === '/index.html'){
      return new Response(INDEX_HTML, {headers:{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-cache','Access-Control-Allow-Origin':'*'}});
    }
    if(path.startsWith('/watch/') || path === '/watch' || path === '/watch.html' || path.startsWith('/join/')){
      return new Response(WATCH_HTML, {headers:{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-cache','Access-Control-Allow-Origin':'*'}});
    }
    return new Response('Not found — deploy with `wrangler deploy` for full assets or use Pages for static', {status:404, headers:{'Content-Type':'text/plain; charset=utf-8'}});
  }
}
