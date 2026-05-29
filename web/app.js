const DEFAULT_IP = "10.174.16.11";
const API_BASE_STORAGE_KEY = "robot_api_base";

function normalizeApiBase(base) {
  if (!base) {
    return "/api";
  }
  if (/^https?:\/\//i.test(base)) {
    return base.replace(/\/+$/, "");
  }
  return `/${String(base).replace(/^\/+|\/+$/g, "")}`;
}

function detectApiBases() {
  const candidates = [];
  const urlApiBase = new URLSearchParams(window.location.search).get("apiBase");
  if (urlApiBase) {
    candidates.push(urlApiBase);
  }

  if (typeof window.ROBOT_API_BASE === "string" && window.ROBOT_API_BASE.trim()) {
    candidates.push(window.ROBOT_API_BASE.trim());
  }

  try {
    const saved = window.localStorage.getItem(API_BASE_STORAGE_KEY);
    if (saved) {
      candidates.push(saved);
    }
  } catch (_error) {
    // localStorage may be unavailable.
  }

  const pathname = window.location.pathname || "/";
  const webSegment = pathname.match(/^(.*)\/web(?:\/.*)?$/);
  if (webSegment && webSegment[1] !== undefined) {
    const prefix = webSegment[1];
    candidates.push(`${prefix}/api`);
    candidates.push(`${prefix}/web/api`);
  }

  candidates.push("/api");

  const unique = [];
  for (const candidate of candidates) {
    const normalized = normalizeApiBase(candidate);
    if (!unique.includes(normalized)) {
      unique.push(normalized);
    }
  }
  return unique;
}

const API_BASE_CANDIDATES = detectApiBases();

const elements = {
  ipInput: document.getElementById("ipInput"),
  aesInput: document.getElementById("aesInput"),
  connectBtn: document.getElementById("connectBtn"),
  disconnectBtn: document.getElementById("disconnectBtn"),
  statusBadge: document.getElementById("statusBadge"),
  logBox: document.getElementById("logBox"),
  stopBtn: document.getElementById("btnStop"),
  layDownBtn: document.getElementById("btnLayDown"),
  standUpBtn: document.getElementById("btnStandUp"),
  moveButtons: {
    forward: document.getElementById("btnForward"),
    backward: document.getElementById("btnBackward"),
    left: document.getElementById("btnLeft"),
    right: document.getElementById("btnRight"),
  },
  videoFeed: document.getElementById("videoFeed"),
  videoOverlay: document.getElementById("videoOverlay"),
  videoMeta: document.getElementById("videoMeta"),
  startVoiceBtn: document.getElementById("startVoiceBtn"),
  stopVoiceBtn: document.getElementById("stopVoiceBtn"),
  voiceBadge: document.getElementById("voiceBadge"),
  heardText: document.getElementById("heardText"),
  targetInput: document.getElementById("targetInput"),
  startGoalBtn: document.getElementById("startGoalBtn"),
  stopGoalBtn: document.getElementById("stopGoalBtn"),
  agentBadge: document.getElementById("agentBadge"),
};

const state = {
  apiBase: API_BASE_CANDIDATES[0] || "/api",
  connected: false,
  activeHold: "",
  holdIntervalId: null,
  commandInFlight: false,
  pendingStop: false,
  videoIntervalId: null,
  videoRequestInFlight: false,
  lastVideoTimestamp: 0,
  voiceRecognition: null,
  voiceSupported: false,
  voiceListening: false,
  agentRunning: false,
  logLines: [],
};

function apiUrl(path) {
  if (/^https?:\/\//i.test(path)) {
    return path;
  }
  const suffix = path.startsWith("/") ? path : `/${path}`;
  return `${state.apiBase}${suffix}`;
}

function appendLog(message, isError = false) {
  const timestamp = new Date().toLocaleTimeString();
  const line = `[${timestamp}] ${isError ? "ERROR" : "INFO"}  ${message}`;
  state.logLines.push(line);
  if (state.logLines.length > 140) {
    state.logLines.shift();
  }
  elements.logBox.textContent = state.logLines.join("\n");
  elements.logBox.scrollTop = elements.logBox.scrollHeight;
}

async function requestJSON(path, options = {}) {
  const init = { ...options };
  if (options.body !== undefined) {
    init.headers = { "Content-Type": "application/json", ...(options.headers || {}) };
  }

  const url = apiUrl(path);
  const response = await fetch(url, init);
  const payload = await response
    .json()
    .catch(() => ({ ok: false, error: `HTTP ${response.status}` }));

  if (!response.ok || !payload.ok) {
    throw new Error(`${payload.error || `HTTP ${response.status}`} (${url})`);
  }

  return payload;
}

async function pickApiBase() {
  for (const candidate of API_BASE_CANDIDATES) {
    try {
      const response = await fetch(`${candidate}/status`, {
        method: "GET",
        headers: { Accept: "application/json" },
      });
      if (response.status !== 404) {
        state.apiBase = candidate;
        try {
          window.localStorage.setItem(API_BASE_STORAGE_KEY, candidate);
        } catch (_error) {
          // localStorage may be unavailable.
        }
        appendLog(`API base: ${candidate}`);
        return;
      }
    } catch (_error) {
      // Try next candidate.
    }
  }

  state.apiBase = API_BASE_CANDIDATES[0] || "/api";
  appendLog(`API base fallback: ${state.apiBase}`, true);
}

function setStatusBadge(element, online, text) {
  element.classList.remove("online", "offline");
  element.classList.add(online ? "online" : "offline");
  element.textContent = text;
}

function updateAgentUI(agent) {
  if (!agent || !agent.running) {
    state.agentRunning = false;
    setStatusBadge(elements.agentBadge, false, "Agent idle");
    return;
  }

  state.agentRunning = true;
  const action = agent.last_action ? ` | ${agent.last_action}` : "";
  setStatusBadge(elements.agentBadge, true, `Agent running: ${agent.target || "target"}${action}`);
}

function updateVoiceUI() {
  if (!state.voiceSupported) {
    setStatusBadge(elements.voiceBadge, false, "Voice unsupported");
  } else if (state.voiceListening) {
    setStatusBadge(elements.voiceBadge, true, "Voice listening");
  } else {
    setStatusBadge(elements.voiceBadge, false, "Voice idle");
  }

  elements.startVoiceBtn.disabled = !state.voiceSupported || !state.connected || state.voiceListening;
  elements.stopVoiceBtn.disabled = !state.voiceSupported || !state.voiceListening;
}

function setConnectedUI(connected, ip = "") {
  state.connected = connected;

  if (connected) {
    setStatusBadge(elements.statusBadge, true, `Connected to ${ip || "robot"}`);
    startVideoLoop();
  } else {
    setStatusBadge(elements.statusBadge, false, "Disconnected");
    stopVideoLoop();
    showNoVideo("No video (disconnected)");
  }

  for (const button of Object.values(elements.moveButtons)) {
    button.disabled = !connected;
  }
  elements.stopBtn.disabled = !connected;
  elements.layDownBtn.disabled = !connected;
  elements.standUpBtn.disabled = !connected;
  elements.connectBtn.disabled = connected;
  elements.disconnectBtn.disabled = !connected;
  elements.startGoalBtn.disabled = !connected;
  elements.stopGoalBtn.disabled = !connected;
  updateVoiceUI();
}

async function refreshStatus(silent = false) {
  try {
    const payload = await requestJSON("/status");
    const status = payload.status || { connected: false, ip: "" };
    const changed = status.connected !== state.connected;

    setConnectedUI(Boolean(status.connected), status.ip || "");
    updateAgentUI(payload.agent || null);

    if (!silent && changed) {
      appendLog(status.connected ? `Connected to ${status.ip}` : "Disconnected");
    }
  } catch (error) {
    if (!silent) {
      appendLog(`Status failed: ${error.message}`, true);
    }
  }
}

function showNoVideo(message) {
  elements.videoOverlay.hidden = false;
  elements.videoOverlay.textContent = message;
  elements.videoMeta.textContent = message;
}

async function fetchVideoFrame() {
  if (!state.connected || state.videoRequestInFlight) {
    return;
  }

  state.videoRequestInFlight = true;
  try {
    const payload = await requestJSON("/video/frame");
    const video = payload.video || {};

    if (video.image_b64) {
      elements.videoFeed.src = `data:image/jpeg;base64,${video.image_b64}`;
      elements.videoOverlay.hidden = true;

      const frameTime = video.timestamp_ms ? new Date(video.timestamp_ms) : new Date();
      if (video.timestamp_ms !== state.lastVideoTimestamp) {
        state.lastVideoTimestamp = video.timestamp_ms || 0;
        elements.videoMeta.textContent = `Frame: ${frameTime.toLocaleTimeString()}`;
      }
    } else {
      showNoVideo(video.error || "Waiting for first frame...");
    }
  } catch (_error) {
    showNoVideo("Video request failed");
  } finally {
    state.videoRequestInFlight = false;
  }
}

function startVideoLoop() {
  if (state.videoIntervalId !== null) {
    return;
  }
  state.videoIntervalId = window.setInterval(() => {
    void fetchVideoFrame();
  }, 260);
  void fetchVideoFrame();
}

function stopVideoLoop() {
  if (state.videoIntervalId !== null) {
    window.clearInterval(state.videoIntervalId);
    state.videoIntervalId = null;
  }
}

async function connectRobot() {
  const ip = (elements.ipInput.value || "").trim() || DEFAULT_IP;
  const aesKey = (elements.aesInput.value || "").trim();
  const body = { ip };
  if (aesKey) {
    body.aesKey = aesKey;
  }

  try {
    const payload = await requestJSON("/connect", {
      method: "POST",
      body: JSON.stringify(body),
    });
    const status = payload.status || { connected: false, ip: "" };
    setConnectedUI(Boolean(status.connected), status.ip || ip);
    updateAgentUI(payload.agent || null);
    appendLog(`Connected to ${status.ip || ip}`);
  } catch (error) {
    setConnectedUI(false);
    appendLog(`Connect failed: ${error.message}`, true);
  }
}

async function disconnectRobot() {
  try {
    const payload = await requestJSON("/disconnect", { method: "POST" });
    const status = payload.status || { connected: false, ip: "" };
    setConnectedUI(Boolean(status.connected), status.ip || "");
    updateAgentUI(payload.agent || null);
    appendLog("Disconnected");
  } catch (error) {
    appendLog(`Disconnect failed: ${error.message}`, true);
  }
}

async function sendCommand(command, silent = false) {
  if (!state.connected) {
    return;
  }

  if (state.commandInFlight) {
    if (command === "stop") {
      state.pendingStop = true;
    }
    return;
  }

  state.commandInFlight = true;
  try {
    const payload = await requestJSON("/command", {
      method: "POST",
      body: JSON.stringify({ command }),
    });
    updateAgentUI(payload.agent || null);
    if (!silent) {
      appendLog(`Command sent: ${command}`);
    }
  } catch (error) {
    appendLog(`Command "${command}" failed: ${error.message}`, true);
    if (command !== "stop") {
      await refreshStatus(true);
    }
  } finally {
    state.commandInFlight = false;
    if (state.pendingStop) {
      state.pendingStop = false;
      void sendCommand("stop", true);
    }
  }
}

function releaseHold() {
  if (!state.activeHold) {
    return;
  }

  const activeButton = elements.moveButtons[state.activeHold];
  if (activeButton) {
    activeButton.classList.remove("active");
  }

  state.activeHold = "";
  if (state.holdIntervalId !== null) {
    window.clearInterval(state.holdIntervalId);
    state.holdIntervalId = null;
  }

  void sendCommand("stop", true);
}

function bindHoldButton(button, command) {
  const start = (event) => {
    event.preventDefault();
    if (!state.connected || state.activeHold) {
      return;
    }

    state.activeHold = command;
    button.classList.add("active");
    void sendCommand(command, false);

    state.holdIntervalId = window.setInterval(() => {
      void sendCommand(command, true);
    }, 320);
  };

  button.addEventListener("pointerdown", start);
  button.addEventListener("pointerup", releaseHold);
  button.addEventListener("pointerleave", releaseHold);
  button.addEventListener("pointercancel", releaseHold);
}

async function startGoalAgent() {
  if (!state.connected) {
    return;
  }
  const target = (elements.targetInput.value || "").trim();
  if (!target) {
    appendLog("Target object is required", true);
    return;
  }
  try {
    const payload = await requestJSON("/agent/start", {
      method: "POST",
      body: JSON.stringify({ target }),
    });
    updateAgentUI(payload.agent || null);
    appendLog(`Agent started for target "${target}"`);
  } catch (error) {
    appendLog(`Agent start failed: ${error.message}`, true);
  }
}

async function stopGoalAgent() {
  try {
    const payload = await requestJSON("/agent/stop", { method: "POST" });
    updateAgentUI(payload.agent || null);
    appendLog("Agent stopped");
  } catch (error) {
    appendLog(`Agent stop failed: ${error.message}`, true);
  }
}

function initSpeechRecognition() {
  const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
  if (!SpeechRecognition) {
    state.voiceSupported = false;
    updateVoiceUI();
    appendLog("SpeechRecognition is not supported in this browser", true);
    return;
  }

  const recognition = new SpeechRecognition();
  recognition.lang = "en-US";
  recognition.continuous = true;
  recognition.interimResults = true;
  recognition.maxAlternatives = 1;

  recognition.onresult = (event) => {
    let finalText = "";
    let interimText = "";

    for (let i = event.resultIndex; i < event.results.length; i += 1) {
      const result = event.results[i];
      const transcript = String(result[0].transcript || "").trim();
      if (!transcript) {
        continue;
      }
      if (result.isFinal) {
        finalText += `${transcript} `;
      } else {
        interimText += `${transcript} `;
      }
    }

    if (interimText) {
      elements.heardText.textContent = `Listening: ${interimText.trim()}`;
    }
    if (finalText) {
      const cleaned = finalText.trim();
      elements.heardText.textContent = `Heard: ${cleaned}`;
      void submitVoiceTranscript(cleaned);
    }
  };

  recognition.onerror = (event) => {
    appendLog(`Voice error: ${event.error || "unknown"}`, true);
  };

  recognition.onend = () => {
    if (state.voiceListening) {
      try {
        recognition.start();
      } catch (_error) {
        // Browser may reject immediate restart; next user interaction can retry.
      }
    }
  };

  state.voiceRecognition = recognition;
  state.voiceSupported = true;
  updateVoiceUI();
}

async function submitVoiceTranscript(text) {
  try {
    const payload = await requestJSON("/voice/command", {
      method: "POST",
      body: JSON.stringify({ text }),
    });

    const action = payload.action || { type: "noop" };
    const source = payload.source || "fallback";
    updateAgentUI(payload.agent || null);

    if (action.type === "move") {
      appendLog(`Voice(${source}): ${text} -> move ${action.command}`);
    } else if (action.type === "agent_start") {
      appendLog(`Voice(${source}): ${text} -> go to ${action.target}`);
      if (action.target) {
        elements.targetInput.value = action.target;
      }
    } else if (action.type === "agent_stop") {
      appendLog(`Voice(${source}): ${text} -> stop agent`);
    } else {
      appendLog(`Voice(${source}): ${text} -> no action`);
    }
  } catch (error) {
    appendLog(`Voice command failed: ${error.message}`, true);
  }
}

function startVoiceRecognition() {
  if (!state.voiceSupported || !state.voiceRecognition || !state.connected) {
    return;
  }
  if (state.voiceListening) {
    return;
  }
  state.voiceListening = true;
  updateVoiceUI();
  try {
    state.voiceRecognition.start();
    appendLog("Voice listening started");
  } catch (error) {
    state.voiceListening = false;
    updateVoiceUI();
    appendLog(`Could not start voice listening: ${error.message}`, true);
  }
}

function stopVoiceRecognition() {
  state.voiceListening = false;
  updateVoiceUI();
  if (state.voiceRecognition) {
    try {
      state.voiceRecognition.stop();
    } catch (_error) {
      // Ignore stop errors.
    }
  }
  appendLog("Voice listening stopped");
}

function wireEvents() {
  elements.connectBtn.addEventListener("click", () => {
    void connectRobot();
  });

  elements.disconnectBtn.addEventListener("click", () => {
    releaseHold();
    void disconnectRobot();
  });

  elements.stopBtn.addEventListener("click", () => {
    releaseHold();
    void sendCommand("stop", false);
  });

  elements.layDownBtn.addEventListener("click", () => {
    releaseHold();
    void sendCommand("laydown", false);
  });

  elements.standUpBtn.addEventListener("click", () => {
    releaseHold();
    void sendCommand("standup", false);
  });

  bindHoldButton(elements.moveButtons.forward, "forward");
  bindHoldButton(elements.moveButtons.backward, "backward");
  bindHoldButton(elements.moveButtons.left, "left");
  bindHoldButton(elements.moveButtons.right, "right");

  elements.startGoalBtn.addEventListener("click", () => {
    void startGoalAgent();
  });

  elements.stopGoalBtn.addEventListener("click", () => {
    void stopGoalAgent();
  });

  elements.startVoiceBtn.addEventListener("click", () => {
    startVoiceRecognition();
  });

  elements.stopVoiceBtn.addEventListener("click", () => {
    stopVoiceRecognition();
  });

  window.addEventListener("blur", releaseHold);
}

async function init() {
  elements.ipInput.value = elements.ipInput.value || DEFAULT_IP;
  wireEvents();
  initSpeechRecognition();
  setConnectedUI(false);
  showNoVideo("No video (disconnected)");

  await pickApiBase();
  await refreshStatus(true);
  appendLog("UI ready");

  window.setInterval(() => {
    void refreshStatus(true);
  }, 2600);
}

void init();
