const $ = (id) => document.getElementById(id);
const toggleBtn = $('toggle');
const dot = $('dot');
const statusText = $('status-text');
const statusHint = $('status-hint');
const logEl = $('log');
const audioEl = $('audio');
const transcriptEl = $('transcript');
const transcriptEmpty = $('transcript-empty');
const transcriptBadge = $('transcript-badge');
const SESSION_STORAGE_KEY = 'rt-llm-proxy.session';

let pc = null;
let dc = null;
let localStream = null;
let audioCtx = null;
let rafId = null;
let cbCountdownId = null;
let connected = false;
let sessionState = loadSessionState();

function log(msg) {
  const t = new Date().toLocaleTimeString();
  logEl.textContent += `${t}  ${msg}\n`;
  logEl.scrollTop = logEl.scrollHeight;
}

const HINT = {
  idle: '点麦克风开始，再点结束',
  connecting: '正在协商 WebRTC…',
  live: '对着麦克风说话',
  error: '可重试或查看日志',
};

function setStatus(state, text, hint) {
  dot.className = 'dot' + (state ? ' ' + state : '');
  statusText.textContent = text;
  if (statusHint) statusHint.textContent = hint ?? HINT[state] ?? HINT.idle;
  toggleBtn.setAttribute('aria-pressed', state === 'live' ? 'true' : 'false');
  toggleBtn.setAttribute('aria-label',
    state === 'live' ? '结束通话' : state === 'connecting' ? '连接中' : '开始通话');
}

function updateTranscriptBadge(model) {
  const names = { gemini: 'Gemini', doubao: '豆包', loopback: 'Loopback', cascade: 'Cascade' };
  const name = names[model];
  const on = !!name;
  transcriptBadge.textContent = on ? name : '暂不支持';
  transcriptBadge.classList.toggle('off', !on);
  transcriptEmpty.textContent = on
    ? `连接 ${name} 后开始显示语音转文字`
    : '当前模型暂不支持字幕';
}

function clearTranscript() {
  transcriptEl.querySelectorAll('.bubble').forEach(el => el.remove());
  transcriptEmpty.hidden = false;
}

function loadSessionState() {
  try {
    const raw = localStorage.getItem(SESSION_STORAGE_KEY);
    if (!raw) return { id: '', lastSeq: 0, model: '' };
    const parsed = JSON.parse(raw);
    return {
      id: typeof parsed.id === 'string' ? parsed.id : '',
      lastSeq: Number.isFinite(parsed.lastSeq) ? Math.max(0, parsed.lastSeq) : 0,
      model: typeof parsed.model === 'string' ? parsed.model : '',
    };
  } catch {
    return { id: '', lastSeq: 0, model: '' };
  }
}

function saveSessionState() {
  try {
    localStorage.setItem(SESSION_STORAGE_KEY, JSON.stringify(sessionState));
  } catch {
    // Ignore storage failures (private mode / quota).
  }
}

function dcText(raw) {
  if (typeof raw === 'string') return raw;
  if (raw instanceof ArrayBuffer) return new TextDecoder().decode(raw);
  if (ArrayBuffer.isView(raw)) return new TextDecoder().decode(raw);
  return String(raw);
}

function showTranscript(role, text) {
  transcriptEmpty.hidden = true;
  const last = transcriptEl.querySelector('.bubble:last-child');
  if (last && last.dataset.role === role) {
    last.querySelector('.body').textContent = text;
    return;
  }
  const bubble = document.createElement('div');
  bubble.className = 'bubble ' + role;
  bubble.dataset.role = role;
  const label = document.createElement('span');
  label.className = 'bubble-label';
  label.textContent = role === 'user' ? '你' : 'AI';
  const body = document.createElement('span');
  body.className = 'body';
  body.textContent = text;
  bubble.append(label, body);
  transcriptEl.appendChild(bubble);
  transcriptEl.scrollTop = transcriptEl.scrollHeight;
}

// Browser-side tool implementations (demo stubs). A real app (e.g. the DJ) would
// query a knowledge base / live API here. Each returns a plain object that is
// sent back to the model as the function result.
const toolHandlers = {
  get_weather(args) {
    const city = (args && args.city) || '某地';
    return { city, temperature: '25°C', condition: '晴' };
  },
};

function handleToolCall(msg) {
  let args = msg.args || {};
  if (typeof args === 'string') {
    try { args = JSON.parse(args); } catch { args = {}; }
  }
  const handler = toolHandlers[msg.name];
  const response = handler ? handler(args) : { error: 'unknown tool: ' + msg.name };
  log(`tool ${msg.name}(${JSON.stringify(args)}) → ${JSON.stringify(response)}`);
  if (dc && dc.readyState === 'open') {
    dc.send(JSON.stringify({ type: 'tool_result', id: msg.id, name: msg.name, response }));
  }
}

function handleDataChannelMessage(raw) {
  const line = dcText(raw).trim();
  if (!line) return;
  try {
    const msg = JSON.parse(line);
    if (msg && msg.type === 'tool_call') {
      handleToolCall(msg);
      return;
    }
    if (msg && typeof msg.role === 'string' && typeof msg.text === 'string') {
      if (Number.isFinite(msg.seq)) {
        sessionState.lastSeq = Math.max(sessionState.lastSeq, Number(msg.seq));
        saveSessionState();
      }
      showTranscript(msg.role, msg.text);
      return;
    }
  } catch {
    // Backward-compatible fallback: server always sends JSON, but this regex
    // guards against any non-JSON message (e.g. debug tools or future clients).
  }
  const m = line.match(/^(user|model):\s*(.*)$/s);
  if (m) {
    sessionState.lastSeq += 1;
    saveSessionState();
    showTranscript(m[1], m[2]);
    return;
  }
  log('« ' + line);
}

function cbReasonLabel(reason) {
  const map = {
    auth: '鉴权失败',
    transient: '上游抖动',
    other: '上游异常',
  };
  return map[reason] || reason || '上游异常';
}

function clearCbCountdown() {
  if (cbCountdownId != null) {
    clearInterval(cbCountdownId);
    cbCountdownId = null;
  }
}

function cbHintText(secondsLeft, reason) {
  const label = cbReasonLabel(reason);
  if (secondsLeft > 0) return `${secondsLeft}s 后可重试（${label}）`;
  return `可重试（${label}）`;
}

function startCbCountdown(retryAfterSec, reason) {
  clearCbCountdown();
  let left = Math.max(0, parseInt(retryAfterSec, 10) || 0);
  if (statusHint) statusHint.textContent = cbHintText(left, reason);
  if (left <= 0) return;
  cbCountdownId = setInterval(() => {
    left -= 1;
    if (statusHint) statusHint.textContent = cbHintText(left, reason);
    if (left <= 0) clearCbCountdown();
  }, 1000);
}

// --- device list ---
async function listMics() {
  try {
    const devices = await navigator.mediaDevices.enumerateDevices();
    const sel = $('mic');
    const cur = sel.value;
    sel.innerHTML = '<option value="">默认</option>';
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
  clearCbCountdown();
  const model = $('model').value;
  const resume = sessionState.id && sessionState.model === model;
  const priorSessionID = sessionState.id;
  updateTranscriptBadge(model);
  if (!resume) clearTranscript();

  toggleBtn.disabled = true;
  setStatus('connecting', '连接中…', '正在请求麦克风…');
  log('麦克风…');

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
  $('model').disabled = $('mic').disabled = true;
  setStatus('connecting', '连接中…');

  pc = new RTCPeerConnection({}); // no ICE servers — host candidates only

  pc.addEventListener('connectionstatechange', () => {
    log('rtc ' + pc.connectionState);
    if (pc.connectionState === 'connected') { setStatus('live', '通话中'); }
    else if (pc.connectionState === 'failed') { setStatus('error', '连接失败'); stop(); }
    else if (pc.connectionState === 'disconnected') { setStatus('error', '连接断开'); }
  });

  pc.addEventListener('track', (evt) => {
    log('远端音频');
    audioEl.srcObject = evt.streams[0];
    audioEl.play?.().catch(() => {});
    meterFns[1] = attachMeter(evt.streams[0], 'ai-bar', 'ai-pct');
  });

  dc = pc.createDataChannel('data', { ordered: true });
  dc.addEventListener('message', (e) => handleDataChannelMessage(e.data));

  localStream.getTracks().forEach(t => pc.addTrack(t, localStream));
  meterFns[0] = attachMeter(localStream, 'you-bar', 'you-pct');
  startMeterLoop(meterFns);

  try {
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    log('SDP → ?model=' + model);
    const headers = { 'Content-Type': 'application/sdp' };
    if (resume) {
      headers['X-Session-ID'] = sessionState.id;
      headers['X-Last-Seq'] = String(sessionState.lastSeq);
      headers['X-Replay-Version'] = '1';
    }
    // DEV-ONLY: in production the orchestrator sets X-Listener-Brief server-side.
    // For local testing, pass it via ?brief=... and we base64(UTF-8)-encode it.
    const brief = new URLSearchParams(location.search).get('brief');
    if (brief) {
      headers['X-Listener-Brief'] = btoa(unescape(encodeURIComponent(brief)));
      log('brief → ' + brief);
    }
    const resp = await fetch(`/?model=${model}`, {
      method: 'POST',
      headers,
      body: offer.sdp,
    });
    if (!resp.ok) {
      const cbState = resp.headers.get('X-Model-CB-State');
      const cbReason = resp.headers.get('X-Model-CB-Reason');
      const retryAfter = resp.headers.get('Retry-After');
      const detail = await resp.text();
      if (resp.status === 503 && cbState) {
        stop({ keepStatus: true });
        toggleBtn.disabled = false;
        setStatus('error', '上游熔断中');
        if (retryAfter) {
          startCbCountdown(retryAfter, cbReason);
        } else if (statusHint) {
          statusHint.textContent = `稍后重试（${cbReasonLabel(cbReason)}）`;
        }
        log(`熔断 ${cbState} reason=${cbReason || 'unknown'} retry_after=${retryAfter || '-'}`);
        return;
      }
      throw new Error(`HTTP ${resp.status}: ${detail}`);
    }
    const replayStatus = resp.headers.get('X-Replay-Status');
    if (replayStatus) log('replay ' + replayStatus);
    const sessionID = resp.headers.get('X-Session-ID') || sessionState.id;
    if (!resume || !sessionID || sessionID !== priorSessionID) {
      clearTranscript();
      sessionState.lastSeq = 0;
    }
    sessionState.id = sessionID;
    sessionState.model = model;
    saveSessionState();
    const answer = await resp.text();
    await pc.setRemoteDescription({ type: 'answer', sdp: answer });
    log('SDP 完成');
  } catch (e) {
    stop();
    setStatus('error', '信令失败', '可重试或查看日志');
    log('协商失败: ' + e.message);
    return;
  }

  connected = true;
  toggleBtn.disabled = false;
  toggleBtn.classList.add('live');
  setStatus('live', '通话中');
}

function stop(opts = {}) {
  clearCbCountdown();
  connected = false;
  $('model').disabled = $('mic').disabled = false;
  updateTranscriptBadge($('model').value);
  toggleBtn.classList.remove('live');
  if (rafId) { cancelAnimationFrame(rafId); rafId = null; }
  meterFns = [null, null];
  ['you-bar', 'ai-bar'].forEach(id => $(id).style.width = '0%');
  $('you-pct').textContent = $('ai-pct').textContent = '—';
  if (dc) { try { dc.close(); } catch {} dc = null; }
  if (pc) { try { pc.close(); } catch {} pc = null; }
  if (localStream) { localStream.getTracks().forEach(t => t.stop()); localStream = null; }
  audioEl.srcObject = null;
  if (!opts.keepStatus) setStatus('', '未连接');
  log('结束');
}

let meterFns = [null, null];
updateTranscriptBadge($('model').value);
$('model').addEventListener('change', () => updateTranscriptBadge($('model').value));
toggleBtn.addEventListener('click', () => {
  if (connected) stop(); else start().catch(e => { log('错误: ' + e.message); stop(); });
});
