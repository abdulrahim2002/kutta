# Unitree Go2 Air Local Web Controller

Simple local controller for Unitree Go2 Air:
- Frontend: native HTML, CSS, JavaScript
- Backend: Go HTTP server
- Robot driver bridge: `unitree_webrtc_connect` (Python)

## Features

- Connect/disconnect by IP (default `10.174.16.11`)
- Optional AES key field for newer firmware
- Live robot video in web UI
- Voice commands in web UI (`Go forward`, `Stop`, `Go to the bottle`, ...)
- Autonomous object-goal mode (Gemini-driven navigation loop with obstacle reaction)
- Basic commands:
  - Move forward
  - Move backward
  - Turn left
  - Turn right
  - Lay down
  - Stop
- Hold-to-move behavior in UI (release sends stop)

## Architecture

1. Browser calls Go HTTP API (`/api/...` or prefixed path like `/robot/api/...`).
2. Go server forwards commands to a local Python worker over JSON lines.
3. Python worker keeps a persistent WebRTC connection to the robot using `unitree_webrtc_connect` and exposes latest camera frame to the Go server.
4. Go server can call Gemini (default model `gemini-3.1-flash-live`) for:
   - voice-command interpretation
   - frame-by-frame navigation decisions for object goals

This keeps the Go server simple while reusing the proven upstream WebRTC implementation.

## Requirements

- Go 1.22+
- Python 3.10+
- Python package:

```bash
pip install unitree_webrtc_connect
```

Recommended for robust video encoding:

```bash
pip install opencv-python pillow
```

## Run

From project root:

```bash
go run .
```

Open:

```text
http://localhost:8080
```

If hosted behind a path prefix (example: `https://host/robot/web/`), the frontend auto-detects API base and also tries fallbacks.

Optional environment variables:

- `PORT` (default `8080`)
- `UNITREE_PYTHON_BIN` (default `python`)
- `BASE_PATH` (optional path prefix, for example `/robot`)
- `GEMINI_API_KEY` (required for voice command parsing and object-goal navigation)
- `GEMINI_MODEL` (default `gemini-3.1-flash-live`)

Example:

```bash
set PORT=8090
set BASE_PATH=/robot
set UNITREE_PYTHON_BIN=C:\Python311\python.exe
set GEMINI_API_KEY=your_key_here
set GEMINI_MODEL=gemini-3.1-flash-live
go run .
```

WSL note:

```bash
export UNITREE_PYTHON_BIN=python3
```

With `BASE_PATH=/robot`, both of these are available:
- UI: `http://localhost:8090/robot/web/`
- API: `http://localhost:8090/robot/api/...`

The server also keeps root routes (`/` + `/api/...`) enabled.

## Voice + Agent Notes

- Voice capture uses browser SpeechRecognition (best in Chromium browsers).
- Gemini is mandatory for voice intent parsing and object-goal navigation in this build (no fallback mode).
- Low-latency movement words are converted to robot commands.
- For object goals like `Go to the bottle`, the agent loop:
  - checks each new frame for target visibility
  - scans in sustained left/right sweeps when target is missing
  - performs short forward probes between sweeps
  - moves forward when target is visible and path looks clear
  - temporarily turns away when obstacle risk is detected, then resumes approach

## Safety Notes

- Use this only in a clear area with enough space around the robot.
- Keep a quick way to stop the robot nearby.
- Disconnect any other active Unitree app session before connecting.
