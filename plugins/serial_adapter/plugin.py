from __future__ import annotations

from collections import deque
import json
import threading
import time
from typing import Any, Callable, Deque, Dict, List, Optional

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
        self._lock = threading.Lock()

    @property
    def frame_delimiter(self) -> bytes:
        return self._frame_delimiter

    def append(self, data: bytes) -> None:
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("append() requires bytes-like input")
        if not data:
            return
        with self._lock:
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
        with self._lock:
            if not self._frames:
                return None
            return self._frames.popleft()

    def peek_frame(self) -> Optional[bytes]:
        with self._lock:
            if not self._frames:
                return None
            return self._frames[0]

    def clear(self) -> None:
        with self._lock:
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
        self._callbacks: List[Callable[[Dict[str, Any]], None]] = []
        self._callback_lock = threading.Lock()
        self._io_lock = threading.Lock()

    def connect(self) -> bool:
        """Open the serial port."""
        with self._io_lock:
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
        with self._io_lock:
            if self._serial is None:
                return
            try:
                self._serial.close()
            finally:
                self._serial = None
                self._ring_buffer.clear()

    def register_callback(self, fn: Callable[[Dict[str, Any]], None]) -> None:
        """Register a callback invoked for each full frame."""
        if not callable(fn):
            raise TypeError("callback must be callable")
        with self._callback_lock:
            self._callbacks.append(fn)

    def _read_chunk_nonblocking(self) -> bytes:
        if self._serial is None:
            raise RuntimeError("Serial not connected")

        waiting = getattr(self._serial, "in_waiting", None)
        if waiting is not None:
            try:
                waiting_count = int(waiting)
            except Exception:
                waiting_count = 0
            if waiting_count <= 0:
                return b""

            read_fn = getattr(self._serial, "read", None)
            if callable(read_fn):
                chunk = read_fn(waiting_count)
            else:
                chunk = self._serial.readline()
        else:
            chunk = self._serial.readline()

        if not chunk:
            return b""
        if not isinstance(chunk, (bytes, bytearray)):
            raise TypeError("serial read must return bytes")
        return bytes(chunk)

    def _build_frame(self, frame_bytes: bytes) -> Dict[str, Any]:
        raw = frame_bytes.decode("utf-8", errors="replace")
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
            "meta": {
                "size": len(frame_bytes),
                "source": "serial",
            },
        }
        if parsed is not None:
            frame.update(parsed)
        return frame

    def _notify_callbacks(self, frame: Dict[str, Any]) -> None:
        with self._callback_lock:
            callbacks = list(self._callbacks)
        for callback in callbacks:
            try:
                callback(frame)
            except Exception:
                # Callbacks should not break the polling loop.
                continue

    def poll(self) -> Optional[Dict[str, Any]]:
        """Non-blocking poll for one available frame."""
        with self._io_lock:
            chunk = self._read_chunk_nonblocking()
            if chunk:
                self._ring_buffer.append(chunk)
            frame_bytes = self._ring_buffer.read_frame()

        if frame_bytes is None:
            return None

        frame = self._build_frame(frame_bytes)
        self._notify_callbacks(frame)
        return frame

    def poll_all(self) -> List[Dict[str, Any]]:
        """Return all currently available frames."""
        with self._io_lock:
            chunk = self._read_chunk_nonblocking()
            if chunk:
                self._ring_buffer.append(chunk)

            collected: List[bytes] = []
            while True:
                frame_bytes = self._ring_buffer.read_frame()
                if frame_bytes is None:
                    break
                collected.append(frame_bytes)

        frames = [self._build_frame(item) for item in collected]
        for frame in frames:
            self._notify_callbacks(frame)
        return frames

    def read(self) -> Optional[Dict[str, Any]]:
        """Backward-compatible alias for poll()."""
        return self.poll()

    def write(self, data: Dict[str, Any]) -> bool:
        """Write a dict as a JSON line."""
        with self._io_lock:
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
