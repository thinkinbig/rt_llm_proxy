const $ = (id) => document.getElementById(id);
const toggleBtn = $('toggle');
const dot = $('dot');
const statusText = $('status-text');
const logEl = $('log');
const audioEl = $('audio');

let pc = null;
let dc = null;
let localStream = null;
let audioCtx = null;
let rafId = null;
let connected = false;

function log(msg) {
  const t = new Date().toLocaleTimeString();
  logEl.textContent += `${t}  ${msg}\n`;
  logEl.scrollTop = logEl.scrollHeight;
}

function setStatus(state, text) {
  dot.className = 'dot' + (state ? ' ' + state : '');
  statusText.textContent = text;
}

// --- device list ---
async function listMics() {
  try {
    const devices = await navigator.mediaDevices.enumerateDevices();
    const sel = $('mic');
    const cur = sel.value;
    sel.innerHTML = '<option value="">默认设备</option>';
    devices.filter(d => d.kind === 'audioinput').forEach((d, i) => {
      const o = document.createElement('option');
      o.value = d.deviceId;
      o.textContent = d.label || `麦克风 ${i + 1}`;
      sel.appendChild(o);
    });
    sel.value = cur;
  } catch (e) { /* ignore */ }
}
listMics();
navigator.mediaDevices?.addEventListener?.('devicechange', listMics);

// --- level meters via Web Audio ---
function attachMeter(stream, barId, pctId) {
  if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
  audioCtx.resume?.().catch(() => {});
  const src = audioCtx.createMediaStreamSource(stream);
  const analyser = audioCtx.createAnalyser();
  analyser.fftSize = 512;
  src.connect(analyser);
  const data = new Uint8Array(analyser.frequencyBinCount);
  return () => {
    analyser.getByteTimeDomainData(data);
    let sum = 0;
    for (let i = 0; i < data.length; i++) { const v = (data[i] - 128) / 128; sum += v * v; }
    const rms = Math.sqrt(sum / data.length);
    const pct = Math.min(100, Math.round(rms * 280));
    $(barId).style.width = pct + '%';
    $(pctId).textContent = pct ? pct + '%' : '—';
  };
}

function startMeterLoop(fns) {
  const tick = () => { fns.forEach(f => f && f()); rafId = requestAnimationFrame(tick); };
  tick();
}

// --- connect / disconnect ---
async function start() {
  const model = $('model').value;

  toggleBtn.disabled = true;
  setStatus('connecting', '连接中…');
  log('请求麦克风权限…');

  try {
    const deviceId = $('mic').value;
    localStream = await navigator.mediaDevices.getUserMedia({
      audio: deviceId ? { deviceId: { exact: deviceId } } : true,
    });
  } catch (e) {
    setStatus('error', '麦克风被拒绝');
    log('getUserMedia 失败: ' + e.message);
    toggleBtn.disabled = false;
    return;
  }
  listMics(); // labels become available after permission

  pc = new RTCPeerConnection({}); // no ICE servers — host candidates only

  pc.addEventListener('connectionstatechange', () => {
    log('connection: ' + pc.connectionState);
    if (pc.connectionState === 'connected') { setStatus('live', '已连接 · 说话试试'); }
    else if (pc.connectionState === 'failed') { setStatus('error', '连接失败'); stop(); }
    else if (pc.connectionState === 'disconnected') { setStatus('error', '连接断开'); }
  });

  pc.addEventListener('track', (evt) => {
    log('收到远端音轨');
    audioEl.srcObject = evt.streams[0];
    audioEl.play?.().catch(() => {});
    meterFns[1] = attachMeter(evt.streams[0], 'ai-bar', 'ai-pct');
  });

  dc = pc.createDataChannel('data', { ordered: true });
  dc.addEventListener('message', (e) => log('« ' + e.data));

  localStream.getTracks().forEach(t => pc.addTrack(t, localStream));
  meterFns[0] = attachMeter(localStream, 'you-bar', 'you-pct');
  startMeterLoop(meterFns);

  try {
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    log('发送 SDP offer → /?model=' + model);
    const resp = await fetch(`/?model=${model}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/sdp' },
      body: offer.sdp,
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`);
    const answer = await resp.text();
    await pc.setRemoteDescription({ type: 'answer', sdp: answer });
    log('收到 SDP answer，协商完成');
  } catch (e) {
    setStatus('error', '信令失败');
    log('协商失败: ' + e.message);
    stop();
    return;
  }

  connected = true;
  toggleBtn.disabled = false;
  toggleBtn.classList.add('live');
}

function stop() {
  connected = false;
  toggleBtn.classList.remove('live');
  if (rafId) { cancelAnimationFrame(rafId); rafId = null; }
  meterFns = [null, null];
  ['you-bar', 'ai-bar'].forEach(id => $(id).style.width = '0%');
  $('you-pct').textContent = $('ai-pct').textContent = '—';
  if (dc) { try { dc.close(); } catch {} dc = null; }
  if (pc) { try { pc.close(); } catch {} pc = null; }
  if (localStream) { localStream.getTracks().forEach(t => t.stop()); localStream = null; }
  audioEl.srcObject = null;
  setStatus('', '未连接');
  log('已断开');
}

let meterFns = [null, null];
toggleBtn.addEventListener('click', () => {
  if (connected) stop(); else start().catch(e => { log('错误: ' + e.message); stop(); });
});
