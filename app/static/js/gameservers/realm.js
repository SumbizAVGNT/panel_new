(() => {
  const realm = document.querySelector("[data-realm]").dataset.realm;
  const LS_AUTO_KEY = `gs_auto_${realm}`;
  const LS_INT_KEY  = `gs_auto_int_${realm}`;

  let tpsSeries = [], lastStats = null;
  let timer = null, maintTimer = null, maintUntil = 0;
  let es = null; // EventSource

  /* ------------ helpers ------------ */
  const toastWrap = () => document.getElementById('toasts');
  function toast(msg, ok=true){
    const el = document.createElement('div');
    el.className = 'toastx ' + (ok?'ok':'err');
    el.innerHTML = `<div class="small">${msg}</div>`;
    toastWrap().appendChild(el);
    setTimeout(()=>{ el.style.opacity='0'; setTimeout(()=>el.remove(), 320); }, 2200);
  }

  function setStatusBadge(state){
    const el = document.getElementById("status-badge");
    el.classList.remove("bg-success","bg-warning","bg-danger","bg-secondary");
    if(state==="online") el.classList.add("bg-success");
    else if(state==="maintenance") el.classList.add("bg-warning");
    else if(state==="offline") el.classList.add("bg-danger");
    else el.classList.add("bg-secondary");
    el.innerHTML = `<i class="bi bi-activity me-1"></i>${state || "unknown"}`;

    const ms = document.getElementById("maint-state");
    ms.classList.remove("o","m");
    ms.classList.add(state==="maintenance" ? "m" : "o");
    ms.querySelector("span:last-child").textContent = `state: ${state}`;
  }

  function drawSparkline(canvas, values){
    const ctx = canvas.getContext("2d");
    const w = canvas.width = canvas.clientWidth, h = canvas.height = canvas.clientHeight;
    ctx.clearRect(0,0,w,h);
    if(!values.length) return;
    const min = Math.min(...values), max = Math.max(...values), pad = 6;
    const xi = i => pad + (w-2*pad) * (i/((values.length-1)||1));
    const yi = v => h - pad - (h-2*pad) * ((v-min)/((max-min)||1));
    ctx.lineWidth = 2;
    ctx.strokeStyle = "rgba(148,163,184,.9)";
    ctx.beginPath();
    values.forEach((v,i)=>{ const x=xi(i), y=yi(v); i?ctx.lineTo(x,y):ctx.moveTo(x,y); });
    ctx.stroke();
  }

  function drawBars(canvas, items){
    const ctx = canvas.getContext("2d");
    const w = canvas.width = canvas.clientWidth, h = canvas.height = canvas.clientHeight;
    ctx.clearRect(0,0,w,h);
    if(!items.length) return;
    const pad = 18, gap = 6, n = items.length;
    const maxVal = Math.max(...items.map(i=>i.value));
    const bw = (w - pad*2 - gap*(n-1)) / n;
    ctx.fillStyle = "rgba(148,163,184,.85)";
    ctx.font = "12px system-ui, -apple-system, Segoe UI, Roboto";
    ctx.textAlign = "center"; ctx.textBaseline = "top";
    items.forEach((it,idx)=>{
      const x = pad + idx*(bw+gap);
      const hNorm = Math.max(2, (h-36) * (it.value/maxVal || 0));
      const y = h-18 - hNorm;
      ctx.fillRect(x, y, bw, hNorm);
      const label = (it.name||"").slice(0,10);
      ctx.fillText(label, x + bw/2, h-16);
    });
  }

  function bytesFmt(v){
    if(v==null||v<0) return "—";
    const u = ["B","KB","MB","GB","TB"]; let i=0; let x=v;
    while(x>=1024 && i<u.length-1){ x/=1024; i++; }
    return `${x.toFixed(1)} ${u[i]}`;
  }
  function percentFmt(x){ if(x==null||x<0) return "—"; return `${Math.round(x*100)}%`; }
  function humanMs(ms){
    if(!ms||ms<0) return "—";
    let s = Math.floor(ms/1000);
    const d = Math.floor(s/86400); s%=86400;
    const h = Math.floor(s/3600); s%=3600;
    const m = Math.floor(s/60); s%=60;
    const parts=[]; if(d) parts.push(d+"д"); if(h) parts.push(h+"ч"); if(m) parts.push(m+"м"); if(s) parts.push(s+"с");
    return parts.join(" ")||"0с";
  }
  function avgTps(tps){
    if(!tps) return null;
    const arr = [tps["1m"], tps["5m"], tps["15m"]].filter(v=>typeof v==="number");
    if(!arr.length) return null;
    return arr.reduce((a,b)=>a+b,0) / arr.length;
  }

  /* ---------- normalize ---------- */
  function normalize(raw){
    const p = raw && typeof raw === 'object'
      ? (raw.data && typeof raw.data === 'object' ? raw.data
        : raw.payload && typeof raw.payload === 'object' ? raw.payload
        : raw)
      : {};
    const tps = p.tps && typeof p.tps === 'object'
      ? { "1m":p.tps["1m"] ?? p.tps_1m, "5m":p.tps["5m"] ?? p.tps_5m, "15m":p.tps["15m"] ?? p.tps_15m, "mspt": p.tps.mspt ?? p.mspt }
      : { "1m":p.tps_1m, "5m":p.tps_5m, "15m":p.tps_15m, "mspt": p.mspt };
    const players = p.players && typeof p.players === 'object'
      ? { max:p.players.max ?? p.players_max, online:p.players.online ?? p.players_online }
      : { max:p.players_max, online:p.players_online };
    const players_list =
      (Array.isArray(p.players_list) && p.players_list) ||
      (p.players && Array.isArray(p.players.list) && p.players.list) ||
      (Array.isArray(p.playersList) && p.playersList) ||
      [];
    let worlds = [];
    if (Array.isArray(p.worlds)) worlds = p.worlds;
    else if (p.worlds && typeof p.worlds === 'object'){
      worlds = Object.keys(p.worlds).map(name => {
        const w = p.worlds[name] || {};
        return {
          name, type: w.type || "", tps: w.tps || [],
          players: w.players ?? w.players_count ?? "",
          loadedChunks: w.chunks_loaded ?? w.loadedChunks ?? "",
          entities: w.entities ?? "",
          view_distance: w.view_distance ?? "",
          simulation_distance: w.simulation_distance ?? "",
          spawn: w.spawn || null
        };
      });
    }
    const heap = p.heap || {}; const nonheap = p.nonheap || {};
    const os = p.os || {}; const jvm = p.jvm || {}; const fs = p.fs || {};
    const entities_top = p.entities_top_types || {};
    return { realm: p.realm || raw.realm || realm, tps, mspt: tps.mspt ?? p.mspt ?? null, players, players_list, worlds, heap, nonheap, os, jvm, fs, entities_top };
  }

  /* ------------ render ------------ */
  function unskeleton(){
    ["t-mspt","t-tps","t-players","t-online"].forEach(id=>{
      const el = document.getElementById(id);
      el?.classList.remove('skel');
      el?.style.removeProperty('width');
      el?.style.removeProperty('height');
    });
  }

  function render(payload){
    const p = normalize(payload);
    lastStats = p;

    const tps1m = p?.tps?.["1m"];
    const online = (p?.players?.online ?? null);
    const status = (typeof online === "number" && online>0) || (typeof tps1m === "number" && tps1m>0) ? "online" : "offline";
    setStatusBadge(status);

    const mspt = p.mspt ?? null;
    const tpsAvg = avgTps(p.tps);
    document.getElementById("t-mspt").textContent = (typeof mspt==="number") ? mspt.toFixed(2) : "—";
    document.getElementById("t-tps").textContent  = (typeof tpsAvg==="number") ? tpsAvg.toFixed(2) : "—";
    document.getElementById("t-online").textContent = (typeof online==="number") ? online : "—";
    document.getElementById("t-players").textContent = (p.players_list||[]).map(x=>x.name).join(", ") || "—";
    unskeleton();

    if(typeof tps1m==="number"){ tpsSeries.push(tps1m); if(tpsSeries.length>60) tpsSeries.shift(); }
    drawSparkline(document.getElementById("tpsChart"), tpsSeries);

    const entPairs = Object.entries(p.entities_top||{}).map(([name,value])=>({name,value})).sort((a,b)=>b.value-a.value).slice(0,12);
    drawBars(document.getElementById("entChart"), entPairs);

    // CPU
    const sys = p.os?.cpu_load?.system ?? null;
    const proc = p.os?.cpu_load?.process ?? null;
    document.getElementById("cpu-system").style.width = (sys!=null? (sys*100).toFixed(0):0)+"%";
    document.getElementById("cpu-system-lbl").textContent = percentFmt(sys);
    document.getElementById("cpu-proc").style.width = (proc!=null? (proc*100).toFixed(0):0)+"%";
    document.getElementById("cpu-proc-lbl").textContent = percentFmt(proc);

    // Heap/Non-heap
    const hu = p.heap?.used ?? null, hm = p.heap?.max ?? null;
    const nhu = p.nonheap?.used ?? null;
    const perc = (hu!=null && hm>0) ? Math.min(100, Math.round(hu/hm*100)) : 0;
    document.getElementById("heap-used-lbl").textContent = `used ${bytesFmt(hu)}`;
    document.getElementById("heap-max-lbl").textContent = `max ${bytesFmt(hm)}`;
    document.getElementById("heap-bar").style.width = perc + "%";
    document.getElementById("nonheap-lbl").textContent = bytesFmt(nhu);

    // FS
    const rf = p.fs?.root_free ?? p.fs?.free;
    const rt = p.fs?.root_total ?? p.fs?.total;
    document.getElementById("fs-free").textContent  = bytesFmt(rf);
    document.getElementById("fs-total").textContent = bytesFmt(rt);

    // Uptime
    document.getElementById("uptime").textContent = humanMs(p.jvm?.uptime_ms);

    // Players table
    const filter = (document.getElementById("players-filter").value || "").toLowerCase().trim();
    const playersArr = p.players_list || [];
    const filtered = !filter ? playersArr : playersArr.filter(x =>
      (x.name||"").toLowerCase().includes(filter) || (x.uuid||"").toLowerCase().includes(filter)
    );
    const pt = document.getElementById("tbl-players");
    pt.innerHTML = filtered.length ? filtered.map(pl=>`
      <tr>
        <td>${pl.name}</td>
        <td class="small">${pl.uuid||""}</td>
        <td>${pl.world||""}</td>
        <td class="text-end">${(pl.ping??"")}</td>
        <td>${pl.lp_primary||""}</td>
      </tr>
    `).join("") : `<tr><td colspan="5" class="text-secondary">нет данных</td></tr>`;

    // Worlds table
    const wt = document.getElementById("tbl-worlds");
    const worlds = p.worlds||[];
    wt.innerHTML = worlds.length ? worlds.map(w=>`
      <tr>
        <td>${w.name}</td>
        <td>${w.type||""}</td>
        <td>${(w.tps||[]).join(", ")}</td>
        <td>${w.players??""}</td>
        <td>${w.loadedChunks??w.chunks_loaded??""}</td>
        <td>${w.entities??""}</td>
        <td>${w.view_distance??""}</td>
        <td>${w.simulation_distance??""}</td>
        <td>${w.spawn?`${w.spawn.x},${w.spawn.y},${w.spawn.z}`:""}</td>
      </tr>
    `).join("") : `<tr><td colspan="9" class="text-secondary">нет данных</td></tr>`;
  }

  /* ------------ API calls ------------ */
  async function fetchStats(){
    try{
      const r = await fetch(`/admin/gameservers/api/stats?realm=${encodeURIComponent(realm)}`, {cache:"no-store"});
      const j = await r.json();
      if(!j.ok){ setStatusBadge("offline"); return; }
      render(j.data||{});
    }catch{ setStatusBadge("offline"); }
  }

  async function postJSON(url, obj){
    const r = await fetch(url, {
      method:"POST",
      headers: { "Content-Type":"application/json" },
      body: JSON.stringify(obj)
    });
    return r.json();
  }

  async function setMaintenance(enabled){
    const msg = document.getElementById("maint-msg").value.trim();
    const j = await postJSON("/admin/gameservers/api/maintenance", {realm, enabled, kickMessage: msg, message: msg});
    if (j.ok && enabled) setStatusBadge("maintenance");
    if (j.ok && !enabled) setStatusBadge("online");
    toast(j.ok ? (enabled?"Техработы включены":"Техработы отключены") : (j.error||"Ошибка"), j.ok);
    return j.ok;
  }

  async function wlChange(op){
    const player = document.getElementById("wl-player").value.trim();
    if(!player) return;
    const j = await postJSON("/admin/gameservers/api/maintenance/whitelist", {realm, op, player});
    toast(j.ok ? `Whitelist: ${op} ${player}` : (j.error||"Ошибка"), j.ok);
    if (j.ok && op === "add") document.getElementById("wl-player").value = "";
  }

  /* ------------ metrics modal ------------ */
  async function fetchSeries({minutes=180, step=60, fields=[]}){
    const params = new URLSearchParams({
      realm, minutes:String(minutes), step:String(step),
      ...(fields.length? {fields: fields.join(",")} : {})
    });
    const r = await fetch(`/admin/gameservers/api/stats/series?`+params.toString(), {cache:"no-store"});
    const j = await r.json();
    if(!j.ok) throw new Error(j.error||"db error");
    return j.data||[];
  }

  let _metricsLast = { seriesMap: null, opts: null, kind: null, rows: [] };

  function drawLines(canvas, seriesMap, opts={}){
    const ctx = canvas.getContext("2d");
    const w = canvas.width = canvas.clientWidth, h = canvas.height = canvas.clientHeight;
    ctx.clearRect(0,0,w,h);

    const labels = Object.keys(seriesMap);
    const all = [];
    labels.forEach(k=>{ all.push(...seriesMap[k].map(p=>p.y).filter(v=>v!=null && !isNaN(v))); });
    if(!all.length){ ctx.fillStyle="#94a3b8"; ctx.fillText("Нет данных", 12, 16); return; }

    const min = Math.min(...all), max = Math.max(...all);
    const padL = 42, padR = 10, padT = 14, padB = 24;

    const xs = seriesMap[labels[0]].map(p=>p.x);
    const x0 = xs[0], x1 = xs[xs.length-1] || (x0+1);

    function X(ts){ return padL + (w-padL-padR) * ((ts - x0)/((x1-x0)||1)); }
    function Y(val){
      const v = (opts.y01? Math.max(0, Math.min(1, val)) : val);
      const mi = opts.y01 ? 0 : min;
      const ma = opts.y01 ? 1 : Math.max(max, min+1e-9);
      return h - padB - (h-padT-padB) * ((v - mi)/((ma - mi)||1));
    }

    ctx.font="12px system-ui,-apple-system,Segoe UI,Roboto";
    ctx.fillStyle="#94a3b8";
    for(let i=0;i<=4;i++){
      const yy = padT + (h-padT-padB)*i/4;
      ctx.save();
      ctx.strokeStyle="rgba(148,163,184,.22)";
      ctx.lineWidth=1;
      ctx.beginPath(); ctx.moveTo(padL,yy); ctx.lineTo(w-padR,yy); ctx.stroke();
      ctx.restore();
    }

    const palette = ["#60a5fa","#34d399","#f59e0b","#f472b6","#a78bfa","#22d3ee","#f87171"];
    labels.forEach((label, idx)=>{
      const pts = seriesMap[label];
      ctx.beginPath();
      pts.forEach((p,i)=>{ const x=X(p.x), y=Y(p.y); i?ctx.lineTo(x,y):ctx.moveTo(x,y); });
      ctx.lineWidth = 2;
      ctx.strokeStyle = palette[idx % palette.length];
      ctx.stroke();
    });
  }

  function renderMetricsChart(){
    const canvas = document.getElementById("metricsChart");
    if (!canvas || !_metricsLast.seriesMap) {
      if (canvas) { canvas.width = canvas.clientWidth; canvas.height = 280; }
      return;
    }
    canvas.width  = canvas.clientWidth;
    canvas.height = 280;
    drawLines(canvas, _metricsLast.seriesMap, _metricsLast.opts || {});
  }
  window.addEventListener('resize', ()=> requestAnimationFrame(renderMetricsChart));

  async function openMetric(kind){
    const modalEl = document.getElementById('metricsModal');
    const modal   = bootstrap.Modal.getOrCreateInstance(modalEl);
    const title   = document.getElementById("metricsTitle");
    const legend  = document.getElementById("metricsLegend");
    const canvas  = document.getElementById("metricsChart");
    let fields = [], titleText = "", y01=false;

    switch(kind){
      case "mspt":    fields = ["mspt"]; titleText = "MSPT (tick duration, ms)"; break;
      case "tps":     fields = ["tps_1m","tps_5m","tps_15m"]; titleText = "TPS (1m / 5m / 15m)"; break;
      case "tps1m":   fields = ["tps_1m"]; titleText = "TPS (1m)"; break;
      case "online":
      case "players": fields = ["players_online"]; titleText = "Онлайн игроков"; break;
      case "cpu":     fields = ["cpu_system_load","cpu_process_load"]; titleText = "CPU load (system / process)"; y01=true; break;
      case "heap":    fields = ["heap_used","heap_max","nonheap_used"]; titleText = "Память Heap / Non-heap (байты)"; break;
      case "disk":    fields = ["fs_root_free","fs_root_total"]; titleText = "Диск (root free / total, байты)"; break;
      case "entities":
        title.textContent = "Top entities (последний снимок) · " + realm;
        legend.textContent = "";
        modal.show();
        modalEl.addEventListener('shown.bs.modal', function once(){
          modalEl.removeEventListener('shown.bs.modal', once);
          canvas.width = canvas.clientWidth; canvas.height = 280;
          const data = lastStats?.entities_top || {};
          const items = Object.entries(data).map(([name,val])=>({name,value:val}))
                         .sort((a,b)=>b.value-a.value).slice(0,16);
          const ctx = canvas.getContext("2d"); ctx.clearRect(0,0,canvas.width,canvas.height);
          drawBars(canvas, items);
        });
        return;
      default:
        fields = []; titleText = "Metrics";
    }

    title.textContent = `${titleText} · ${realm}`;
    legend.textContent = "Загрузка…";
    modal.show();

    const minutes = parseInt(document.getElementById("m-range").value,10)||180;
    const step    = parseInt(document.getElementById("m-step").value,10)||60;

    try{
      const rows = await fetchSeries({minutes, step, fields});
      if(!rows.length){
        _metricsLast = { seriesMap:null, opts:null, kind, rows:[] };
        legend.textContent = "Нет данных за выбранный период";
      }else{
        const seriesMap = {}; fields.forEach(f=> seriesMap[f]=[]);
        rows.forEach(r=>{
          const ts = r.ts || r.time || r.timestamp || r.t || Math.floor(Date.now()/1000);
          fields.forEach(f=>{
            let v = r[f];
            if(v==null) return;
            if (y01 && typeof v === "number") v = Math.max(0, Math.min(1, v));
            seriesMap[f].push({x: ts, y: v});
          });
        });
        Object.keys(seriesMap).forEach(k=>{ if(!seriesMap[k].length) delete seriesMap[k]; });
        _metricsLast = { seriesMap, opts:{y01}, kind, rows };

        const doDraw = ()=>{ renderMetricsChart(); };
        if (modalEl.classList.contains('show')) { doDraw(); }
        else {
          modalEl.addEventListener('shown.bs.modal', function once(){
            modalEl.removeEventListener('shown.bs.modal', once);
            doDraw();
          });
        }

        const names = {
          mspt: "MSPT",
          tps_1m: "TPS 1m", tps_5m:"TPS 5m", tps_15m:"TPS 15m",
          players_online:"Players online",
          cpu_system_load:"CPU system", cpu_process_load:"CPU process",
          heap_used:"Heap used", heap_max:"Heap max", nonheap_used:"Non-heap used",
          fs_root_free:"Root free", fs_root_total:"Root total"
        };
        const span = txt => `<span class="badge bg-secondary me-1">${txt}</span>`;
        legend.innerHTML = Object.keys(seriesMap).map(k=>span(names[k]||k)).join("") +
          `<div class="mt-2 text-secondary small">Точек: ${rows.length}, шаг: ${step}s, период: ${minutes}m</div>`;

        document.getElementById("exportCsv").onclick = ()=>{
          const keys = Object.keys(rows[0]||{});
          const esc=v=> `"${String(v??"").replace(/"/g,'""')}"`;
          const csv = [keys.join(","), ...rows.map(r=>keys.map(k=>esc(r[k])).join(","))].join("\n");
          const blob = new Blob([csv], {type:"text/csv;charset=utf-8"});
          const a = document.createElement("a");
          a.href = URL.createObjectURL(blob);
          a.download = `${realm}_${kind}_${minutes}m_${step}s.csv`;
          a.click();
          URL.revokeObjectURL(a.href);
        };
      }
    }catch(e){
      _metricsLast = { seriesMap:null, opts:null, kind, rows:[] };
      legend.innerHTML = `<span class="text-danger">${e.message||e}</span>`;
    }
  }

  /* ------------ SSE console ------------ */
  function appendLine(line){
    const pre = document.getElementById('console-log');
    pre.textContent += (pre.textContent?'\n':'') + line;
    if(document.getElementById('auto-scroll').checked){
      pre.scrollTop = pre.scrollHeight;
    }
  }
  function fmtLine(p){
    const t  = new Date().toLocaleTimeString();
    const lv = p.level ? `[${p.level}] ` : '';
    const sc = p.source ? `${p.source}: ` : '';
    const msg = p.line || p.message || JSON.stringify(p);
    return `[${t}] ${lv}${sc}${msg}`;
  }
  function startLive(){
    if(es) return;
    es = new EventSource(`/admin/gameservers/api/console/stream?realm=${encodeURIComponent(realm)}`);
    es.onmessage = (ev)=>{ try{ appendLine(fmtLine(JSON.parse(ev.data||'{}')));}catch{} };
    es.addEventListener('err', (ev)=>{ appendLine(`[stream] ${ev.data}`); });
    es.onerror = ()=>{}; // auto-reconnect by browser
  }
  function stopLive(){ try{es?.close();}catch{} es=null; }

  /* ------------ binds ------------ */
  function bind(){
    document.getElementById("refresh").addEventListener("click", fetchStats);
    document.getElementById("players-tile")?.addEventListener("click", ()=>{
      const list = document.getElementById("players-list");
      list.innerHTML = "";
      const players = (lastStats?.players_list)||[];
      if(players.length===0){
        list.innerHTML = `<div class="text-secondary">Игроков нет.</div>`;
      } else {
        for(const p of players){
          const row = document.createElement("div");
          row.className = "player-row";
          const uuid = (p.uuid||"").replace(/-/g,"");
          const headSrc = `/admin/gameservers/api/player-head?name=${encodeURIComponent(p.name)}&uuid=${encodeURIComponent(uuid)}`;
          row.innerHTML = `
            <img class="player-head" width="32" height="32" src="${headSrc}" alt="">
            <div class="flex-grow-1">
              <div class="text-light">${p.name}</div>
              <div class="small text-secondary">${uuid}${p.world? " · "+p.world : ""}</div>
            </div>`;
          list.appendChild(row);
        }
      }
      bootstrap.Modal.getOrCreateInstance(document.getElementById('playersModal')).show();
    });
    document.getElementById("players-filter").addEventListener("input", ()=>{ if(lastStats) render(lastStats); });

    document.getElementById("live-toggle").addEventListener("change", e=>{ e.target.checked ? startLive() : stopLive(); });
    document.getElementById("clear-log").addEventListener("click", ()=>{ document.getElementById('console-log').textContent=""; });

    document.getElementById("wl-add").addEventListener("click", ()=>wlChange("add"));
    document.getElementById("wl-remove").addEventListener("click", ()=>wlChange("remove"));

    document.querySelectorAll("[data-maint-preset]").forEach(btn=>{
      btn.addEventListener("click", async ()=>{
        const minutes = parseInt(btn.dataset.maintPreset,10);
        const ok = await setMaintenance(true);
        if(!ok) return;
        maintUntil = Date.now() + minutes*60*1000;
        clearInterval(maintTimer);
        maintTimer = setInterval(()=>{
          const left = maintUntil - Date.now();
          document.getElementById("maint-countdown").textContent = humanMs(Math.max(0,left));
          if(left<=0){
            clearInterval(maintTimer);
            maintUntil = 0; document.getElementById("maint-countdown").textContent = "—";
            setMaintenance(false);
            document.getElementById("maint-toggle").checked = false;
          }
        }, 1000);
      });
    });

    document.getElementById("maint-toggle").addEventListener("change", async (e)=>{
      const on = e.target.checked;
      const ok = await setMaintenance(on);
      if(!ok) e.target.checked = !on;
      if(!on){ clearInterval(maintTimer); maintUntil = 0; document.getElementById("maint-countdown").textContent = "—"; }
    });

    // modal metric openers
    document.querySelectorAll('.tile[data-metric]').forEach(el=>{
      el.addEventListener('click', ()=>{
        const kind = el.getAttribute('data-metric');
        if(kind === 'players'){ /* handled above */ } else { openMetric(kind); }
      });
    });

    // modal controls
    document.getElementById("m-range").addEventListener("change", ()=>{ if (_metricsLast.kind) openMetric(_metricsLast.kind); });
    document.getElementById("m-step").addEventListener("change",  ()=>{ if (_metricsLast.kind) openMetric(_metricsLast.kind); });

    // console send
    document.getElementById("send").addEventListener("click", async ()=>{
      const cmd = document.getElementById("cmd").value.trim();
      if(!cmd) return;
      const btn = document.getElementById("send"), st = document.getElementById("console-status");
      btn.disabled = true; st.textContent = "Отправка...";
      try{
        const j = await postJSON("/admin/gameservers/api/console", {realm, cmd});
        st.textContent = j.ok ? "OK" : (j.error||"error");
        toast(j.ok ? "Команда отправлена" : (j.error||"Ошибка"), j.ok);
        if(j.ok){ document.getElementById("cmd").value=""; }
      }catch(e){ st.textContent = e.message||"network error"; toast(st.textContent, false); }
      finally{ btn.disabled = false; }
    });
    document.getElementById("cmd").addEventListener("keydown", e=>{ if(e.key==="Enter") document.getElementById("send").click(); });
  }

  /* ------------ auto refresh ------------ */
  function applyTimer(){
    clearInterval(timer);
    if(document.getElementById("autorefresh").checked){
      const sec = Math.max(2, parseInt(document.getElementById("refresh-interval").value,10)||3);
      timer = setInterval(fetchStats, sec*1000);
    }
  }

  function initAuto(){
    const autoEl = document.getElementById("autorefresh");
    const intEl  = document.getElementById("refresh-interval");
    const intLbl = document.getElementById("refresh-interval-label");
    autoEl.checked = (localStorage.getItem(LS_AUTO_KEY) ?? "1") !== "0";
    intEl.value    = localStorage.getItem(LS_INT_KEY) || "3";
    intLbl.textContent = intEl.value + "s";
    autoEl.addEventListener("change", e=>{
      localStorage.setItem(LS_AUTO_KEY, e.target.checked ? "1" : "0");
      applyTimer();
    });
    intEl.addEventListener("input", e=>{
      intLbl.textContent = e.target.value + "s";
      localStorage.setItem(LS_INT_KEY, e.target.value);
      applyTimer();
    });
    applyTimer();
  }

  /* ------------ boot ------------ */
  document.addEventListener('DOMContentLoaded', () => {
    bind();
    initAuto();
    fetchStats();
    startLive();
  });
})();
