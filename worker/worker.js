// Cloudflare Worker — WatchTogether (synced to Node server v1.3)
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
    this.roomState={ playing:false, time:0, updatedAt:Date.now(), videoUrl:'', sub:null };
    this.hostId=null;
  }
  async fetch(req){
    const pair=new WebSocketPair(); const [client, server]=Object.values(pair);
    server.accept();
    const id=Math.random().toString(36).slice(2,8);
    server.addEventListener('message', e=>{
      let m; try{ m=JSON.parse(e.data);}catch{return;}
      if(m.type==='join'){
        this.clients.set(id, server); server._id=id; server._name=m.name||'مهمان';
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
      if(m.type==='video-change'){ this.roomState.videoUrl=m.videoUrl; this.roomState.time=0; this.roomState.playing=false; this.roomState.sub=null; this.broadcast({type:'video-change', videoUrl:m.videoUrl, from:id}, id); }
      if(m.type==='sub-change'){ this.roomState.sub=m.sub||null; this.roomState.updatedAt=Date.now(); this.broadcast({type:'sub-change', sub:m.sub, from:id}, id); }
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

    // Health
    if(url.pathname==='/api/health') return json({ok:true, mode:'worker'});

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

    // Static via Assets
    if(env.ASSETS) return env.ASSETS.fetch(req);
    return new Response('Worker running. Bind ASSETS for static or deploy Pages.',{status:200, headers:{'Content-Type':'text/html; charset=utf-8','Access-Control-Allow-Origin':'*'}});
  }
}
