# Unitree Go2 Air Local Web Controller

Controls a Unitree Go2 robot from a browser.

## Architecture

```
Browser ──HTTP──> Go Server ──JSON-lines──> Python Worker ──WebRTC──> Unitree Go2
                        │
                        └──WebSocket──> OpenAI Realtime API (voice commands, agent)
```

- **Browser** (`web/`): Native HTML/CSS/JS UI with video feed, movement buttons, voice input (SpeechRecognition), and object-goal agent controls.
- **Go Server** (`main.go`): HTTP + WebSocket server that serves the UI, forwards robot commands to the Python worker, and connects to OpenAI Realtime API for natural-language voice commands and autonomous object navigation with audio responses.
- **Python Worker** (`worker/unitree_worker.py`): Keeps a persistent WebRTC connection to the robot, exposes a JSON-line protocol for commands and camera frames.

## Quick Start

```bash
export OPENAI_API_KEY=your_key_here
pip install -r requirements.txt
go run .
```

Open `http://localhost:8080`, enter robot IP, and connect.

## Environment

| Variable | Default | Purpose |
|---|---|---|
| `PORT` | `8080` | HTTP port |
| `OPENAI_API_KEY` | — | Required for agent mode |
| `OPENAI_REALTIME_MODEL` | `gpt-realtime-2` | OpenAI model |
| `BASE_PATH` | — | URL path prefix (e.g. `/robot`) |
| `UNITREE_PYTHON_BIN` | `python` | Python interpreter path |
