import http from 'http';
import fs from 'fs';
import path from 'path';
import os from 'os';
import { fileURLToPath } from 'url';
import { execFile } from 'child_process';
import { promisify } from 'util';
import { WebSocketServer } from 'ws';
const execFileAsync = promisify(execFile);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const CLIENT_DIR = path.join(ROOT, 'client');
const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || '0.0.0.0';

const MIME = { '.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css','.json':'application/json','.svg':'image/svg+xml','.ico':'image/x-icon','.vtt':'text/vtt','.srt':'text/plain; charset=utf-8' };

const rooms = new Map();
function getRoom(id){
  if(!rooms.has(id)) rooms.set(id,{ hostId:null, clients:new Map(), state:{ playing:false, time:0, updatedAt:Date.now(), videoUrl:'', sub:null, dub:null }});
  return rooms.get(id);
}
function broadcast(roomId, data, except=null){
  const room=rooms.get(roomId); if(!room) return;
  const msg=JSON.stringify(data);
  for(const c of room.clients.values()) if(c!==except && c.readyState===1) c.send(msg);
}
function parseBody(req){
  return new Promise(res=>{
    let b=''; req.setEncoding('utf8'); req.on('data',c=>b+=c); req.on('end',()=>{ try{ res(b?JSON.parse(b):null); }catch{ res(null); } });
  });
}

// ── VPN store: key = userId || roomId ──
const vpnStore = new Map(); // key -> { configs: [{id,uri,type,label,addedAt,host}], activeId }
const roomSharedProxy = new Map(); // roomId -> { ownerUserId, uri, type, host, updatedAt } — shared for video proxy, URI not exposed to peers
function uriType(u){
  const t=u.trim();
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
    if(u.startsWith('vmess://')){ const b=Buffer.from(u.slice(8),'base64').toString('utf8'); const j=JSON.parse(b); return j.add||j.host||''; }
    const tmp=u.replace(/^(vless|trojan|ss):\/\//,'http://');
    const parsed=new URL(tmp);
    return parsed.hostname||'';
  }catch{ return ''; }
}
function storeKey(userId, roomId){ return (userId||'').trim() || (roomId||'').trim() || '__global'; }
function getStore(key){ if(!vpnStore.has(key)) vpnStore.set(key,{configs:[],activeId:null}); return vpnStore.get(key); }

async function fetchViaProxy(target, headers, roomId, userId){
  // shared proxy for the room (owner's active VPN) — peers use it without seeing URI
  let cfg=null;
  if(roomId && roomSharedProxy.has(roomId)){
    const sh=roomSharedProxy.get(roomId);
    if(sh && sh.uri) cfg={ uri: sh.uri, type: sh.type };
  }
  if(!cfg){
    const keys=[storeKey(userId,roomId), storeKey('',roomId), '__global'];
    for(const k of keys){
      const st=vpnStore.get(k);
      if(st && st.activeId){ const f=st.configs.find(c=>c.id===st.activeId); if(f){ cfg=f; break; } }
      if(st && st.configs.length===1 && !st.activeId) { cfg=st.configs[0]; break; }
    }
  }
  let dispatcher;
  if(cfg && cfg.uri){
    const u=cfg.uri.trim();
    if(/^https?:\/\//i.test(u) || /^socks5?:\/\//i.test(u)){
      try{ const {ProxyAgent}=await import('undici'); dispatcher=new ProxyAgent(u); }catch{}
    }
  }
  const opts={headers};
  if(dispatcher) opts.dispatcher=dispatcher;
  return fetch(target, opts);
}

async function pingUri(uri, timeoutMs=5000){
  const u=uri.trim();
  const type=uriType(u);
  if(type==='http' || type==='socks'){
    try{
      const {ProxyAgent}=await import('undici');
      const agent=new ProxyAgent(u);
      const t0=Date.now();
      const ctrl=new AbortController(); const t=setTimeout(()=>ctrl.abort(), timeoutMs);
      const r=await fetch('https://1.1.1.1/cdn-cgi/trace', {dispatcher:agent, signal:ctrl.signal, headers:{'User-Agent':'Mozilla/5.0'}});
      clearTimeout(t);
      const ms=Date.now()-t0;
      if(r.ok) return {ok:true, ms};
      return {ok:false, error:`http ${r.status}`, ms};
    }catch(e){ return {ok:false, error: String(e.message||e).slice(0,120)}; }
  }
  const host=uriHost(u);
  if(!host) return {ok:false, error:'cannot parse host'};
  try{
    const t0=Date.now();
    const ctrl=new AbortController(); const t=setTimeout(()=>ctrl.abort(), timeoutMs);
    await fetch(`https://${host}/`, {method:'HEAD', signal:ctrl.signal, headers:{'User-Agent':'Mozilla/5.0'}}).catch(()=>{});
    clearTimeout(t);
    return {ok:true, ms: Date.now()-t0, note:'xray-required — host reachable, needs TUN'};
  }catch(e){ return {ok:false, error: String(e.message||e).slice(0,120)}; }
}

// ── YouTube direct URL extraction: yt-dlp → Invidious fallback ──
function ytId(url){
  try{
    const u=new URL(url);
    if(u.hostname.includes('youtu.be')) return u.pathname.split('/').filter(Boolean)[0]||'';
    const v=u.searchParams.get('v'); if(v) return v;
    const m=url.match(/\/(embed|shorts|v)\/([^/?&#]+)/); if(m) return m[2];
  }catch{}
  const m2=url.match(/(?:v=|youtu\.be\/|embed\/|shorts\/)([\w-]{6,})/); return m2?m2[1]:'';
}
async function tryInvidious(videoId, timeoutMs=8000){
  const instances=['https://yewtu.be','https://invidious.snopyta.org','https://inv.nadeko.net','https://iv.melmac.space'];
  for(const base of instances){
    try{
      const ctrl=new AbortController(); const t=setTimeout(()=>ctrl.abort(), timeoutMs);
      const r=await fetch(`${base}/api/v1/videos/${videoId}`, {signal:ctrl.signal, headers:{'User-Agent':'Mozilla/5.0'}});
      clearTimeout(t);
      if(!r.ok) continue;
      const j=await r.json();
      const fmts=[...(j.formatStreams||[]), ...(j.adaptiveFormats||[])].filter(f=>f.url);
      // prefer mp4 with both audio+video
      let best=(j.formatStreams||[]).filter(f=>f.container==='mp4'&&f.url).sort((a,b)=>(b.width||0)-(a.width||0))[0];
      if(!best) best=fmts.filter(f=>f.container==='mp4').sort((a,b)=>(b.width||0)-(a.width||0))[0];
      if(!best) best=fmts[0];
      if(best?.url) return {ok:true, url:best.url, via:'invidious:'+base, title:j.title||''};
    }catch{}
  }
  return null;
}
async function tryYtDlp(url, timeoutMs=15000){
  const id=ytId(url);
  // 1) try local yt-dlp if installed
  try{
    const {stdout}=await execFileAsync('yt-dlp', ['-g','--no-playlist','-f','best[ext=mp4]/best', url], {timeout: timeoutMs, maxBuffer: 2*1024*1024});
    const line=(stdout||'').trim().split(/\r?\n/).filter(Boolean)[0];
    if(line && /^https?:\/\//.test(line)) return {ok:true, url: line, via:'yt-dlp'};
  }catch(e){
    // ENOENT → no yt-dlp installed, fall through to Invidious
    if(!String(e.message||'').includes('ENOENT') && !String(e.code||'').includes('ENOENT')){
      // yt-dlp exists but failed (maybe blocked) — still try Invidious
    }
  }
  if(id){
    const inv=await tryInvidious(id, 7000);
    if(inv) return inv;
  }
  return {ok:false, error:'no url (install yt-dlp: pip install yt-dlp, or try Invidious fallback failed)'};
}

// ── MKV soft-sub/audio extraction via ffprobe/ffmpeg ──
const tracksCache = new Map(); // url -> {at, data}
const vttCache = new Map(); // url#idx -> {at, vtt}
function ffprobeBin(){ return process.env.FFPROBE || 'ffprobe'; }
function ffmpegBin(){ return process.env.FFMPEG || 'ffmpeg'; }
async function probeTracks(url, timeoutMs=15000){
  const key=url;
  const cached=tracksCache.get(key);
  if(cached && Date.now()-cached.at < 10*60*1000) return cached.data;
  const args=['-v','quiet','-print_format','json','-show_streams','-show_format', url];
  try{
    const {stdout}=await execFileAsync(ffprobeBin(), args, {timeout: timeoutMs, maxBuffer: 4*1024*1024});
    const j=JSON.parse(stdout);
    const streams=j.streams||[];
    const subs=streams.filter(x=>x.codec_type==='subtitle').map(x=>({index:x.index, codec_name:x.codec_name, language:(x.tags&&x.tags.language)||'', title:(x.tags&&x.tags.title)||'', disposition:x.disposition||{}}));
    const audios=streams.filter(x=>x.codec_type==='audio').map(x=>({index:x.index, codec_name:x.codec_name, language:(x.tags&&x.tags.language)||'', title:(x.tags&&x.tags.title)||'', channels:x.channels||0, disposition:x.disposition||{}}));
    const videos=streams.filter(x=>x.codec_type==='video').map(x=>({index:x.index, codec_name:x.codec_name, width:x.width, height:x.height}));
    const data={ok:true, videos, audios, subs, format:j.format||null};
    tracksCache.set(key,{at:Date.now(), data});
    return data;
  }catch(e){
    const msg=String(e.message||e).slice(0,400);
    return {ok:false, error: msg};
  }
}
async function extractSubToVtt(url, subIndex, timeoutMs=300000){
  const vkey=url+'#'+subIndex;
  const vc=vttCache.get(vkey);
  if(vc && Date.now()-vc.at < 30*60*1000){ console.log('[sub] cache hit', vkey.slice(0,60)); return {ok:true, vtt: vc.vtt}; }
  console.log('[sub] extract START', url.slice(0,80), 'idx', subIndex);
  const tryArgs = [
    ['-v', 'error', '-i', url, '-map', `0:${subIndex}`, '-c:s', 'webvtt', '-f','webvtt','pipe:1'],
    ['-v', 'error', '-i', url, '-map', `0:s:0`, '-c:s', 'webvtt', '-f','webvtt','pipe:1'],
    ['-v', 'error', '-i', url, '-skip_initial_bytes', '1024', '-map', `0:${subIndex}`, '-c:s', 'webvtt', '-f','webvtt','pipe:1'],
  ];
  for(let i=0;i<tryArgs.length;i++){
    const args=tryArgs[i];
    console.log('[sub] try', i+1, args.join(' ').slice(0,120));
    try{
      const { spawn } = await import('child_process');
      const result = await new Promise((resolve, reject)=>{
        const ffmpeg = spawn(ffmpegBin(), args);
        const chunks=[]; let errBuf='';
        const timer=setTimeout(()=>{ try{ffmpeg.kill('SIGKILL');}catch{}; reject(new Error('timeout '+timeoutMs+'ms')); }, timeoutMs);
        ffmpeg.stdout.on('data',c=>chunks.push(c));
        ffmpeg.stderr.on('data',c=>errBuf+=c.toString());
        ffmpeg.on('error',e=>{ clearTimeout(timer); reject(e); });
        ffmpeg.on('close',code=>{
          clearTimeout(timer);
          if(code===0){
            const buf=Buffer.concat(chunks);
            if(!buf.length) return reject(new Error('empty output stderr:'+errBuf.slice(0,600)));
            resolve({buf, errBuf});
          } else reject(Object.assign(new Error('ffmpeg exited '+code+' stderr:'+errBuf.slice(0,600)),{stderr:errBuf}));
        });
      });
      let vtt = result.buf.toString('utf8').trimStart();
      if(!vtt) continue;
      if(!vtt.startsWith('WEBVTT')) vtt='WEBVTT\n\n'+vtt;
      vttCache.set(vkey,{at:Date.now(), vtt});
      console.log('[sub] cached', vkey.slice(0,60), 'len', vtt.length);
      return {ok:true, vtt};
    }catch(e){
      const msg=String(e.message||e);
      const stderr=String(e.stderr||'');
      console.log('[sub] try', i+1, 'FAIL', 'msg:', msg.slice(0,400), 'stderr:', stderr.slice(0,600));
      if(/404|Not Found|Server returned 404/i.test(stderr+msg)) return {ok:false, error: msg.slice(0,600), stderr: stderr.slice(0,800)};
      if(i===tryArgs.length-1) return {ok:false, error: msg.slice(0,800), stderr: stderr.slice(0,800)};
    }
  }
  return {ok:false, error:'no subtitle extracted'};
}

const server = http.createServer(async (req,res)=>{
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,DELETE,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','*');
  if(req.method==='OPTIONS'){ res.writeHead(204); return res.end(); }
  const url = new URL(req.url, `http://${req.headers.host}`);

  // Health (with optional room validation)
  if(url.pathname==='/api/health'){
    const roomId = url.searchParams.get('room');
    if(roomId){
      const room = rooms.get(roomId);
      res.writeHead(200,{'Content-Type':'application/json'});
      return res.end(JSON.stringify({ok:true, rooms:rooms.size, roomExists:!!room, roomId}));
    }
    res.writeHead(200,{'Content-Type':'application/json'});
    return res.end(JSON.stringify({ok:true, rooms:rooms.size}));
  }

  // ── VPN APIs ──
  if(url.pathname.startsWith('/api/vpn')){
    // POST /api/vpn/import — batch import
    if(url.pathname==='/api/vpn/import' && req.method==='POST'){
      const body=await parseBody(req);
      const userId=(body?.userId||url.searchParams.get('userId')||'').trim();
      const roomId=(body?.roomId||body?.room||url.searchParams.get('room')||'').trim();
      const raw=(body?.urisText||body?.uris||body?.uri||'').toString();
      const lines=raw.split(/[\r\n,]+/).map(s=>s.trim()).filter(Boolean);
      if(Array.isArray(body?.uris)) lines.push(...body.uris.map(s=>String(s).trim()).filter(Boolean));
      const unique=[...new Set(lines)];
      if(!unique.length){ res.writeHead(400,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:false,error:'no uris'})); }
      const key=storeKey(userId, roomId);
      const store=getStore(key);
      let added=0;
      for(const uri of unique){
        if(store.configs.some(c=>c.uri===uri)) continue;
        store.configs.push({id:Math.random().toString(36).slice(2,9), uri, type:uriType(uri), label: uriHost(uri)||uri.slice(0,24), addedAt: Date.now(), host: uriHost(uri)});
        added++;
      }
      if(!store.activeId && store.configs.length) store.activeId=store.configs[0].id;
      res.writeHead(200,{'Content-Type':'application/json'});
      return res.end(JSON.stringify({ok:true, key, added, total: store.configs.length, configs: store.configs, activeId: store.activeId}));
    }
    // POST /api/vpn — single uri (back-compat, also accepts batch via newline)
    if(url.pathname==='/api/vpn' && req.method==='POST'){
      const body=await parseBody(req);
      const userId=(body?.userId||url.searchParams.get('userId')||'').trim();
      const roomId=(body?.roomId||body?.room||url.searchParams.get('room')||'').trim();
      let uri=(body?.uri||body?.urisText||'').toString().trim();
      if(!uri && body?.uris){ uri=''; }
      const lines=uri.split(/[\r\n]+/).map(s=>s.trim()).filter(Boolean);
      if(lines.length>1){
        const key=storeKey(userId,roomId);
        const store=getStore(key);
        let added=0;
        for(const u of [...new Set(lines)]){
          if(store.configs.some(c=>c.uri===u)) continue;
          store.configs.push({id:Math.random().toString(36).slice(2,9), uri:u, type:uriType(u), label: uriHost(u)||u.slice(0,24), addedAt:Date.now(), host:uriHost(u)});
          added++;
        }
        if(!store.activeId && store.configs.length) store.activeId=store.configs[0].id;
        res.writeHead(200,{'Content-Type':'application/json'});
        return res.end(JSON.stringify({ok:true, key, added, total:store.configs.length, configs:store.configs, activeId:store.activeId, via:'batch'}));
      }
      if(!uri){ res.writeHead(400,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:false,error:'uri required'})); }
      const roomKey=storeKey(userId,roomId);
      if(!roomKey){ res.writeHead(400,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:false,error:'userId or roomId required'})); }
      const store=getStore(roomKey);
      const existing=store.configs.find(c=>c.uri===uri);
      if(!existing){
        store.configs.push({id:Math.random().toString(36).slice(2,9), uri, type:uriType(uri), label: body?.label||uriHost(uri)||uri.slice(0,24), addedAt:Date.now(), host:uriHost(uri)});
      }
      if(!store.activeId) store.activeId=store.configs[store.configs.length-1].id;
      const type=uriType(uri);
      res.writeHead(200,{'Content-Type':'application/json'});
      return res.end(JSON.stringify({ok:true, roomId:roomKey, type, via:(type==='http'||type==='socks')?'proxy-agent':'xray-tun-required', total:store.configs.length}));
    }
    if(url.pathname==='/api/vpn/list' && req.method==='GET'){
      const userId=(url.searchParams.get('userId')||'').trim();
      const roomId=(url.searchParams.get('room')||url.searchParams.get('roomId')||'').trim();
      const keys=[storeKey(userId,roomId), storeKey('',roomId), '__global'].filter((v,i,a)=>a.indexOf(v)===i);
      let merged=null; for(const k of keys){ const s=vpnStore.get(k); if(s && s.configs.length){ merged=s; break; } }
      res.writeHead(200,{'Content-Type':'application/json'});
      return res.end(JSON.stringify({ok:true, keys, store: merged? {configs:merged.configs, activeId:merged.activeId, key:keys[0]} : {configs:[],activeId:null,key:keys[0]}}));
    }
    if(url.pathname==='/api/vpn' && req.method==='GET'){
      const userId=(url.searchParams.get('userId')||'').trim();
      const roomId=(url.searchParams.get('room')||url.searchParams.get('roomId')||'').trim();
      const key=storeKey(userId,roomId);
      const s=vpnStore.get(key)||vpnStore.get(storeKey('',roomId))||null;
      const safe=s? {configs:s.configs.map(c=>({id:c.id,type:c.type,label:c.label,host:c.host,addedAt:c.addedAt})), activeId:s.activeId, total:s.configs.length} : null;
      res.writeHead(200,{'Content-Type':'application/json'});
      return res.end(JSON.stringify({ok:true, key, store:safe, hasStore:!!s}));
    }
    if(url.pathname==='/api/vpn/select' && req.method==='POST'){
      const body=await parseBody(req);
      const userId=(body?.userId||'').trim(); const roomId=(body?.roomId||body?.room||'').trim(); const id=(body?.id||'').trim();
      const key=storeKey(userId,roomId); const s=vpnStore.get(key);
      if(!s){ res.writeHead(404,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:false,error:'no store'})); }
      if(!s.configs.some(c=>c.id===id)){ res.writeHead(400,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:false,error:'id not found'})); }
      s.activeId=id;
      // share active proxy for the whole room so peers' /api/proxy works via owner's VPN without seeing URI
      try{
        const active=s.configs.find(c=>c.id===id);
        if(active && roomId){
          roomSharedProxy.set(roomId, { ownerUserId: userId, uri: active.uri, type: active.type, host: active.host, updatedAt: Date.now() });
        }
      }catch{}
      res.writeHead(200,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:true, activeId:id, shared: !!roomId}));
    }
    if(url.pathname==='/api/vpn/remove' && req.method==='POST'){
      const body=await parseBody(req);
      const userId=(body?.userId||'').trim(); const roomId=(body?.roomId||'').trim(); const id=(body?.id||'').trim();
      const key=storeKey(userId,roomId); const s=vpnStore.get(key);
      if(!s){ res.writeHead(404,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:false})); }
      s.configs=s.configs.filter(c=>c.id!==id);
      if(s.activeId===id) s.activeId=s.configs[0]?.id||null;
      res.writeHead(200,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:true, total:s.configs.length, activeId:s.activeId}));
    }
    if(url.pathname==='/api/vpn' && req.method==='DELETE'){
      const userId=(url.searchParams.get('userId')||'').trim();
      const roomId=(url.searchParams.get('room')||url.searchParams.get('roomId')||'').trim();
      const id=url.searchParams.get('id');
      const key=storeKey(userId,roomId);
      const s=vpnStore.get(key);
      if(id && s){ s.configs=s.configs.filter(c=>c.id!==id); if(s.activeId===id) s.activeId=s.configs[0]?.id||null; }
      else if(s) vpnStore.delete(key);
      else vpnStore.delete(key);
      res.writeHead(200,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:true}));
    }
    if(url.pathname==='/api/vpn/ping' && req.method==='POST'){
      const body=await parseBody(req);
      const uri=(body?.uri||'').trim();
      if(!uri){ res.writeHead(400,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:false,error:'uri required'})); }
      const r=await pingUri(uri, 6000);
      res.writeHead(200,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:r.ok, ms:r.ms||null, error:r.error||null, note:r.note||null, type:uriType(uri)}));
    }
    if(url.pathname==='/api/vpn/ping-all' && req.method==='POST'){
      const body=await parseBody(req);
      const userId=(body?.userId||'').trim(); const roomId=(body?.roomId||'').trim();
      let targets=[];
      if(Array.isArray(body?.configs) && body.configs.length){
        targets = body.configs.map(x=> ({id:String(x.id), uri:String(x.uri||''), type: uriType(String(x.uri||'')), host: uriHost(String(x.uri||''))})).filter(x=>x.uri);
      } else if(typeof body?.urisText==='string' && body.urisText.trim()){
        const lines=[...new Set(body.urisText.split(/[\r\n,]+/).map(v=>v.trim()).filter(Boolean))];
        targets = lines.map((u,i)=> ({id:`tmp-${i}`, uri:u, type:uriType(u), host:uriHost(u)}));
      } else {
        const key=storeKey(userId,roomId); const st=vpnStore.get(key) || vpnStore.get(storeKey('',roomId));
        if(st && st.configs.length) targets = st.configs.map(c=> ({id:c.id, uri:c.uri, type:c.type, host:c.host}));
      }
      if(!targets.length){ res.writeHead(200,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:true, results:[]})); }
      const settled = await Promise.all(targets.map(async c=>{
        const r=await pingUri(c.uri, 4000);
        return {id:c.id, uri:c.uri, type:c.type, host:c.host, ok:r.ok, ms:r.ms||null, error:r.error||null, note:r.note||null};
      }));
      const results=[...settled].sort((a,b)=> (a.ms??99999)-(b.ms??99999));
      res.writeHead(200,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:true, results}));
    }
    // fallback for /api/vpn* unknown
    res.writeHead(404,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:false,error:'unknown vpn endpoint'}));
  }

  // Room shared proxy info (no URI leak to non-owner) — OUTSIDE vpn block
  if(url.pathname==='/api/room/proxy' && req.method==='GET'){
    const roomId=(url.searchParams.get('room')||url.searchParams.get('roomId')||'').trim();
    const userId=(url.searchParams.get('userId')||'').trim();
    const sh=roomSharedProxy.get(roomId);
    if(!sh){ res.writeHead(200,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:true, shared:false})); }
    const isOwner = !!userId && sh.ownerUserId===userId;
    res.writeHead(200,{'Content-Type':'application/json'});
    return res.end(JSON.stringify({ok:true, shared:true, isOwner, host: sh.host||'', type: sh.type||'', owner: sh.ownerUserId.slice(0,4)+'***'}));
  }
  if(url.pathname==='/api/room/proxy' && req.method==='DELETE'){
    const body=await parseBody(req);
    const roomId=(body?.roomId||body?.room||'').trim(); const userId=(body?.userId||'').trim();
    const sh=roomSharedProxy.get(roomId);
    if(sh && sh.ownerUserId===userId) roomSharedProxy.delete(roomId);
    res.writeHead(200,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:true}));
  }

  // YouTube: try to resolve to direct mp4 via yt-dlp (if installed on server)
  if(url.pathname==='/api/yt'){
    const target=url.searchParams.get('url'); if(!target){ res.writeHead(400,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:false,error:'missing url'})); }
    if(!/youtube\.com|youtu\.be/.test(target)){ res.writeHead(400,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:false,error:'not a youtube url'})); }
    const r=await tryYtDlp(target, 20000);
    res.writeHead(r.ok?200:502,{'Content-Type':'application/json'});
    return res.end(JSON.stringify(r));
  }

  // Video tracks (ffprobe) — internal MKV subs/audio
  if(url.pathname==='/api/video/tracks'){
    const target=url.searchParams.get('url');
    if(!target){ res.writeHead(400,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:false,error:'missing url'})); }
    console.log('[tracks] probe', target.slice(0,80)); const data=await probeTracks(target, 20000);
    res.writeHead(data.ok?200:502,{'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    return res.end(JSON.stringify(data));
  }
  if(url.pathname==='/api/video/sub'){
    const target=url.searchParams.get('url');
    const idx=url.searchParams.get('index');
    if(!target || idx===null){ res.writeHead(400,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:false,error:'missing url or index'})); }
    const subIndex=parseInt(idx,10);
    if(Number.isNaN(subIndex)){ res.writeHead(400,{'Content-Type':'application/json'}); return res.end(JSON.stringify({ok:false,error:'invalid index'})); }
    const r=await extractSubToVtt(target, subIndex, 110000);
    if(!r.ok){ res.writeHead(502,{'Content-Type':'application/json'}); return res.end(JSON.stringify(r)); }
    res.writeHead(200,{'Content-Type':'text/vtt; charset=utf-8','Access-Control-Allow-Origin':'*','Cache-Control':'no-cache'});
    return res.end(r.vtt);
  }

  // Proxy — optionally via selected VPN
  if(url.pathname==='/api/proxy'){
    const target=url.searchParams.get('url');
    const roomForProxy=url.searchParams.get('room')||url.searchParams.get('roomId')||'';
    const userForProxy=url.searchParams.get('userId')||'';
    if(!target){ res.writeHead(400); return res.end('missing url'); }
    try{
      const h={}; if(req.headers.range) h['Range']=req.headers.range;
      h['User-Agent']='Mozilla/5.0';
      const r=await fetchViaProxy(target,h,roomForProxy,userForProxy);
      res.writeHead(r.status,{
        'Content-Type': r.headers.get('content-type')||'video/mp4',
        'Content-Length': r.headers.get('content-length')||undefined,
        'Accept-Ranges': r.headers.get('accept-ranges')||'bytes',
        'Content-Range': r.headers.get('content-range')||undefined,
        'Access-Control-Allow-Origin':'*',
        'Cache-Control':'no-cache'
      });
      if(r.body) for await(const chunk of r.body) res.write(chunk);
      res.end();
    }catch(e){ res.writeHead(502); res.end(String(e)); }
    return;
  }

  // Static
  let fp = path.join(CLIENT_DIR, url.pathname==='/' ? 'index.html' : url.pathname);
  if(url.pathname.startsWith('/watch/') || url.pathname.startsWith('/join/')) fp=path.join(CLIENT_DIR,'watch.html');
  if(url.pathname==='/watch.html') fp=path.join(CLIENT_DIR,'watch.html');
  if(!fp.startsWith(CLIENT_DIR)){ res.writeHead(403); return res.end('forbidden'); }
  if(fs.existsSync(fp) && fs.statSync(fp).isDirectory()) fp=path.join(fp,'index.html');
  if(!fs.existsSync(fp)){ res.writeHead(404,{'Content-Type':'text/html'}); return res.end('<h1>404</h1><a href="/">Home</a>'); }
  const ext=path.extname(fp);
  res.writeHead(200,{'Content-Type': MIME[ext]||'application/octet-stream','Cache-Control':'no-cache'});
  fs.createReadStream(fp).pipe(res);
});

const wss = new WebSocketServer({ server, path:'/ws' });
setInterval(()=>{ wss.clients.forEach(ws=>{ if(!ws.isAlive) return ws.terminate(); ws.isAlive=false; ws.ping(); }); },30000);

wss.on('connection',(ws,req)=>{
  ws.isAlive=true; ws.on('pong',()=>ws.isAlive=true);
  const url=new URL(req.url,'http://localhost');
  ws._room=url.searchParams.get('room')||'';
  ws._id=Math.random().toString(36).slice(2,8);
  ws._name='';
  ws.on('message',raw=>{
    let m; try{ m=JSON.parse(raw.toString()); }catch{ return; }
    const roomId=m.room||ws._room; if(!roomId) return;
    ws._room=roomId;
    const room=getRoom(roomId);
    if(m.type==='join'){
      ws._name=m.name||'مهمان';
      room.clients.set(ws._id, ws);
      if(!room.hostId) room.hostId=ws._id;
      if(m.videoUrl) room.state.videoUrl=m.videoUrl;
      ws.send(JSON.stringify({type:'joined', id:ws._id, hostId:room.hostId, state:room.state, peers:room.clients.size}));
      broadcast(roomId,{type:'peer-join', id:ws._id, name:ws._name, peers:room.clients.size}, ws);
      if(room.hostId && room.hostId!==ws._id){
        const host=room.clients.get(room.hostId);
        if(host && host.readyState===1) host.send(JSON.stringify({type:'request-sync'}));
      }
      return;
    }
    if(['play','pause','seek','sync'].includes(m.type)){
      if(m.type==='play'){ room.state.playing=true; room.state.time=m.time??room.state.time; room.state.updatedAt=Date.now(); }
      if(m.type==='pause'){ room.state.playing=false; room.state.time=m.time??room.state.time; room.state.updatedAt=Date.now(); }
      if(m.type==='seek'){ room.state.time=m.time; room.state.updatedAt=Date.now(); }
      if(m.type==='sync'){ room.state.time=m.time; room.state.playing=m.playing; room.state.updatedAt=Date.now(); }
      broadcast(roomId,{...m, from:ws._id}, ws);
      return;
    }
    if(m.type==='ping'){ ws.send(JSON.stringify({type:'pong', t:m.t})); }
    if(m.type==='video-change'){
      room.state.videoUrl=m.videoUrl; room.state.time=0; room.state.playing=false; room.state.sub=null; room.state.dub=null;
      broadcast(roomId,{type:'video-change', videoUrl:m.videoUrl, from:ws._id}, ws);
    }
    if(m.type==='sub-change'){
      room.state.sub=m.sub||null;
      room.state.updatedAt=Date.now();
      broadcast(roomId,{type:'sub-change', sub: m.sub, from:ws._id}, ws);
      return;
    }
    if(m.type==='dub-change'){
      room.state.dub=m.dub||null;
      room.state.updatedAt=Date.now();
      broadcast(roomId,{type:'dub-change', dub: m.dub, from:ws._id}, ws);
      return;
    }
  });
  ws.on('close',()=>{
    const rid=ws._room; if(!rid) return;
    const room=rooms.get(rid); if(!room) return;
    room.clients.delete(ws._id);
    if(room.hostId===ws._id){
      const next=room.clients.keys().next().value;
      room.hostId=next||null;
      if(next) broadcast(rid,{type:'host-change', hostId:next});
    }
    broadcast(rid,{type:'peer-leave', id:ws._id, peers:room.clients.size});
    if(room.clients.size===0) rooms.delete(rid);
  });
});

server.listen(PORT, HOST, ()=>{
  console.log(`WatchTogether http://${HOST}:${PORT}`);
  for(const i of Object.values(os.networkInterfaces()).flat()){
    if(i && i.family==='IPv4' && !i.internal) console.log(`  LAN: http://${i.address}:${PORT}`);
  }
});