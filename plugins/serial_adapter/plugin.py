from __future__ import annotations

from collections import deque
import json
import time
from typing import Any, Deque, Dict, Optional

try:
    import serial  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    serial = None


DEFAULT_BUFFER_SIZE = 512 * 1024
DEFAULT_FRAME_DELIMITER = b"\n"
DEFAULT_MAX_FRAMES = 10


def _normalize_delimiter(frame_delimiter: bytes | str) -> bytes:
    if isinstance(frame_delimiter, str):
        delimiter = frame_delimiter.encode("utf-8")
    elif isinstance(frame_delimiter, bytes):
        delimiter = frame_delimiter
    else:
        raise TypeError("frame_delimiter must be bytes or str")
    if not delimiter:
        raise ValueError("frame_delimiter must not be empty")
    return delimiter


class RingBuffer:
    """Byte-oriented frame buffer with delimiter-based extraction."""

    def __init__(
        self,
        *,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        frame_delimiter: bytes | str = DEFAULT_FRAME_DELIMITER,
        max_frames: int = DEFAULT_MAX_FRAMES,
    ) -> None:
        if int(buffer_size) <= 0:
            raise ValueError("buffer_size must be positive")
        if int(max_frames) <= 0:
            raise ValueError("max_frames must be positive")
        self._buffer_size = int(buffer_size)
        self._frame_delimiter = _normalize_delimiter(frame_delimiter)
        self._frames: Deque[bytes] = deque(maxlen=int(max_frames))
        self._buffer = bytearray()

    @property
    def frame_delimiter(self) -> bytes:
        return self._frame_delimiter

    def append(self, data: bytes) -> None:
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("append() requires bytes-like input")
        if not data:
            return
        self._buffer.extend(data)
        if len(self._buffer) > self._buffer_size:
            overflow = len(self._buffer) - self._buffer_size
            del self._buffer[:overflow]

        delimiter = self._frame_delimiter
        while True:
            idx = self._buffer.find(delimiter)
            if idx < 0:
                break
            frame = bytes(self._buffer[:idx])
            del self._buffer[: idx + len(delimiter)]
            self._frames.append(frame)

    def read_frame(self) -> Optional[bytes]:
        if not self._frames:
            return None
        return self._frames.popleft()

    def peek_frame(self) -> Optional[bytes]:
        if not self._frames:
            return None
        return self._frames[0]

    def clear(self) -> None:
        self._buffer.clear()
        self._frames.clear()


class SerialAdapter:
    """Buffered serial telemetry observer."""

    def __init__(
        self,
        port: str,
        baudrate: int,
        *,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        frame_delimiter: bytes | str = DEFAULT_FRAME_DELIMITER,
        max_frames: int = DEFAULT_MAX_FRAMES,
    ) -> None:
        self._port = port
        self._baudrate = int(baudrate)
        self._serial: Optional[Any] = None
        self._ring_buffer = RingBuffer(
            buffer_size=buffer_size,
            frame_delimiter=frame_delimiter,
            max_frames=max_frames,
        )

    def connect(self) -> bool:
        """Open the serial port."""
        if self._serial is not None:
            return True
        if serial is None:
            raise RuntimeError("pyserial is not available")
        try:
            self._serial = serial.Serial(self._port, self._baudrate, timeout=1)
            self._ring_buffer.clear()
        except Exception as exc:
            raise RuntimeError(f"Failed to open serial port: {self._port}") from exc
        return True

    def disconnect(self) -> None:
        """Close the serial port if open."""
        if self._serial is None:
            return
        try:
            self._serial.close()
        finally:
            self._serial = None
            self._ring_buffer.clear()

    def read(self) -> Optional[Dict[str, Any]]:
        """Read one frame and return telemetry, or None when no full frame exists."""
        if self._serial is None:
            raise RuntimeError("Serial not connected")
        chunk = self._serial.readline()
        if chunk and not isinstance(chunk, (bytes, bytearray)):
            raise TypeError("serial.readline() must return bytes")
        if chunk:
            self._ring_buffer.append(bytes(chunk))

        frame = self._ring_buffer.read_frame()
        if frame is None:
            return None

        raw = frame.decode("utf-8", errors="replace")
        parsed: Optional[Dict[str, Any]] = None
        try:
            payload = json.loads(raw)
            if isinstance(payload, dict):
                parsed = payload
        except json.JSONDecodeError:
            parsed = None

        frame: Dict[str, Any] = {
            "timestamp": time.time(),
            "raw": raw,
            "parsed": parsed,
        }
        if parsed is not None:
            frame.update(parsed)
        return frame

    def write(self, data: Dict[str, Any]) -> bool:
        """Write a dict as a JSON line."""
        if self._serial is None:
            raise RuntimeError("Serial not connected")
        if not isinstance(data, dict):
            raise TypeError("data must be a dict")
        payload = json.dumps(
            data,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        self._serial.write(payload.encode("utf-8") + self._ring_buffer.frame_delimiter)
        return True
