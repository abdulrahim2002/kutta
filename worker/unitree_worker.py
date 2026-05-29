#!/usr/bin/env python3
import asyncio
import base64
import io
import json
import os
import sys
import time
from typing import Any, Dict, Optional

# Keep a dedicated protocol output stream. Everything else is redirected to
# stderr so third-party library prints do not break the JSON line protocol.
PROTO_OUT = os.fdopen(os.dup(sys.stdout.fileno()), "w", buffering=1, encoding="utf-8")
sys.stdout = sys.stderr

DEFAULT_LINEAR_SPEED = 0.25
DEFAULT_YAW_SPEED = 0.80
MAX_VIDEO_WIDTH = 640
JPEG_QUALITY = 68


def send_response(payload: Dict[str, Any]) -> None:
    PROTO_OUT.write(json.dumps(payload, separators=(",", ":")) + "\n")
    PROTO_OUT.flush()


class RobotController:
    def __init__(self) -> None:
        self.conn = None
        self.ip = ""

        self._rtc_topic = None
        self._sport_cmd = None
        self._conn_class = None
        self._connection_method = None

        self.latest_frame_b64 = ""
        self.latest_frame_timestamp_ms = 0
        self.video_enabled = False
        self.video_error = ""
        self._video_callback_registered = False

    def _load_library(self) -> None:
        if self._conn_class is not None:
            return

        try:
            from unitree_webrtc_connect import (
                RTC_TOPIC,
                SPORT_CMD,
                UnitreeWebRTCConnection,
                WebRTCConnectionMethod,
            )
        except Exception as exc:
            raise RuntimeError(
                "Python package 'unitree_webrtc_connect' is missing or broken. "
                "Install it with: pip install unitree_webrtc_connect"
            ) from exc

        self._rtc_topic = RTC_TOPIC
        self._sport_cmd = SPORT_CMD
        self._conn_class = UnitreeWebRTCConnection
        self._connection_method = WebRTCConnectionMethod

    async def status(self) -> Dict[str, Any]:
        connected = bool(self.conn and getattr(self.conn, "isConnected", False))
        return {"connected": connected, "ip": self.ip if connected else ""}

    async def connect(self, ip: str, aes_key: str = "") -> Dict[str, Any]:
        self._load_library()

        robot_ip = ip.strip()
        if not robot_ip:
            raise RuntimeError("ip is required")

        if self.conn is not None:
            await self.disconnect()

        key = aes_key.strip() or None
        self.conn = self._conn_class(
            self._connection_method.LocalSTA,
            ip=robot_ip,
            aes_128_key=key,
        )

        self.latest_frame_b64 = ""
        self.latest_frame_timestamp_ms = 0
        self.video_enabled = False
        self.video_error = ""
        self._video_callback_registered = False

        await self.conn.connect()
        self.ip = robot_ip

        await self._ensure_normal_mode()
        await self._setup_video_stream()
        return await self.status()

    async def disconnect(self) -> Dict[str, Any]:
        if self.conn is not None:
            try:
                await self._stop_motion()
            except Exception:
                pass

            try:
                self.conn.video.switchVideoChannel(False)
            except Exception:
                pass

            await self.conn.disconnect()

        self.conn = None
        self.ip = ""
        self.video_enabled = False
        self.video_error = ""
        self._video_callback_registered = False
        return {"connected": False, "ip": ""}

    async def command(self, command_name: str) -> Dict[str, Any]:
        command = command_name.strip().lower()
        if not command:
            raise RuntimeError("command is required")

        self._require_connected()

        if command == "forward":
            await self._move(DEFAULT_LINEAR_SPEED, 0.0)
        elif command == "backward":
            await self._move(-DEFAULT_LINEAR_SPEED, 0.0)
        elif command == "left":
            await self._move(0.0, DEFAULT_YAW_SPEED)
        elif command == "right":
            await self._move(0.0, -DEFAULT_YAW_SPEED)
        elif command == "laydown":
            await self._request(
                self._rtc_topic["SPORT_MOD"],
                self._sport_cmd["StandDown"],
            )
        elif command == "standup":
            await self._request(
                self._rtc_topic["SPORT_MOD"],
                self._sport_cmd["StandUp"],
            )
        elif command == "stop":
            await self._stop_motion()
        else:
            raise RuntimeError(f"unsupported command: {command}")

        return {"connected": True, "ip": self.ip, "command": command}

    async def video_frame(self) -> Dict[str, Any]:
        connected = bool(self.conn and getattr(self.conn, "isConnected", False))
        if connected and not self.video_enabled:
            await self._setup_video_stream()
        return {
            "connected": connected,
            "video_enabled": self.video_enabled,
            "image_b64": self.latest_frame_b64,
            "timestamp_ms": self.latest_frame_timestamp_ms,
            "error": self.video_error,
        }

    def _require_connected(self) -> None:
        if not self.conn or not getattr(self.conn, "isConnected", False):
            raise RuntimeError("robot is not connected")

    async def _request(
        self,
        topic: str,
        api_id: int,
        parameter: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        self._require_connected()

        payload: Dict[str, Any] = {"api_id": api_id}
        if parameter is not None:
            payload["parameter"] = parameter

        response = await self.conn.datachannel.pub_sub.publish_request_new(topic, payload)
        status = ((response.get("data") or {}).get("header") or {}).get("status") or {}
        code = int(status.get("code", -1))
        if code != 0:
            raise RuntimeError(f"robot returned non-zero code {code} for api_id {api_id}")

        return response

    async def _move(self, x: float, z: float) -> None:
        await self._request(
            self._rtc_topic["SPORT_MOD"],
            self._sport_cmd["Move"],
            {"x": x, "y": 0.0, "z": z},
        )

    async def _stop_motion(self) -> None:
        if not self.conn or not getattr(self.conn, "isConnected", False):
            return

        try:
            await self._request(self._rtc_topic["SPORT_MOD"], self._sport_cmd["StopMove"])
        except Exception:
            await self._request(
                self._rtc_topic["SPORT_MOD"],
                self._sport_cmd["Move"],
                {"x": 0.0, "y": 0.0, "z": 0.0},
            )

    async def _ensure_normal_mode(self) -> None:
        if not self.conn:
            return

        try:
            response = await self._request(self._rtc_topic["MOTION_SWITCHER"], 1001)
        except Exception:
            return

        raw_data = (response.get("data") or {}).get("data")
        mode = ""
        if isinstance(raw_data, str) and raw_data.strip():
            try:
                parsed = json.loads(raw_data)
                mode = str(parsed.get("name", "")).strip().lower()
            except json.JSONDecodeError:
                return

        if mode and mode != "normal":
            await self._request(self._rtc_topic["MOTION_SWITCHER"], 1002, {"name": "normal"})
            await asyncio.sleep(2.0)

    async def _setup_video_stream(self) -> None:
        if not self.conn:
            return

        async def recv_camera_stream(track: Any) -> None:
            while self.conn and getattr(self.conn, "isConnected", False):
                frame = await track.recv()
                try:
                    self.latest_frame_b64 = self._encode_jpeg_base64(frame)
                    self.latest_frame_timestamp_ms = int(time.time() * 1000)
                    self.video_error = ""
                except Exception as exc:
                    self.video_error = f"video encode failed: {exc}"
                    await asyncio.sleep(0.05)

        try:
            # Keep enabling video channel on every retry. This is cheap and helps
            # recover from occasional startup races.
            self.conn.video.switchVideoChannel(True)
            if not self._video_callback_registered:
                self.conn.video.add_track_callback(recv_camera_stream)
                self._video_callback_registered = True
            self.video_enabled = True
        except Exception as exc:
            self.video_enabled = False
            self.video_error = f"video setup failed: {exc}"
            return

    def _encode_jpeg_base64(self, frame: Any) -> str:
        cv_error = None

        try:
            import cv2  # type: ignore

            img = frame.to_ndarray(format="bgr24")
            height, width = img.shape[:2]
            if width > MAX_VIDEO_WIDTH:
                ratio = MAX_VIDEO_WIDTH / float(width)
                target_h = max(1, int(height * ratio))
                img = cv2.resize(img, (MAX_VIDEO_WIDTH, target_h))

            ok, encoded = cv2.imencode(
                ".jpg",
                img,
                [int(cv2.IMWRITE_JPEG_QUALITY), JPEG_QUALITY],
            )
            if ok:
                return base64.b64encode(encoded.tobytes()).decode("ascii")
            cv_error = RuntimeError("cv2.imencode returned false")
        except Exception as exc:
            cv_error = exc

        try:
            image = frame.to_image()
            if image.width > MAX_VIDEO_WIDTH:
                ratio = MAX_VIDEO_WIDTH / float(image.width)
                target_h = max(1, int(image.height * ratio))
                image = image.resize((MAX_VIDEO_WIDTH, target_h))

            output = io.BytesIO()
            image.save(output, format="JPEG", quality=JPEG_QUALITY)
            return base64.b64encode(output.getvalue()).decode("ascii")
        except Exception as pil_exc:
            if cv_error is not None:
                raise RuntimeError(f"{cv_error}; {pil_exc}")
            raise


async def handle_request(controller: RobotController, request: Dict[str, Any]) -> Dict[str, Any]:
    request_id = request.get("id")
    action = str(request.get("action", "")).strip().lower()
    params = request.get("params") or {}

    try:
        if action == "status":
            data = await controller.status()
        elif action == "connect":
            ip = str(params.get("ip", "")).strip()
            aes_key = str(params.get("aes_key", "")).strip()
            data = await controller.connect(ip=ip, aes_key=aes_key)
        elif action == "disconnect":
            data = await controller.disconnect()
        elif action == "command":
            command = str(params.get("command", "")).strip()
            data = await controller.command(command)
        elif action == "video_frame":
            data = await controller.video_frame()
        elif action == "shutdown":
            data = await controller.disconnect()
            return {"id": request_id, "ok": True, "data": data, "shutdown": True}
        else:
            raise RuntimeError(f"unsupported action: {action}")

        return {"id": request_id, "ok": True, "data": data}
    except Exception as exc:
        return {"id": request_id, "ok": False, "error": str(exc)}


async def main() -> int:
    controller = RobotController()

    while True:
        line = await asyncio.to_thread(sys.stdin.readline)
        if line == "":
            await controller.disconnect()
            return 0

        line = line.strip()
        if not line:
            continue

        try:
            request = json.loads(line)
        except json.JSONDecodeError as exc:
            send_response({"id": None, "ok": False, "error": f"invalid json request: {exc}"})
            continue

        response = await handle_request(controller, request)
        send_response(response)

        if response.get("shutdown"):
            return 0


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except KeyboardInterrupt:
        raise SystemExit(0)
