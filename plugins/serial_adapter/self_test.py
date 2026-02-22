from __future__ import annotations

from typing import Any, Dict, List

try:
    from plugins.serial_adapter.plugin import RingBuffer, SerialAdapter
except ImportError:
    import sys
    import os

    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
    from plugins.serial_adapter.plugin import RingBuffer, SerialAdapter


class _FakeSerial:
    def __init__(self, chunks: List[bytes]) -> None:
        self._chunks = [bytearray(chunk) for chunk in chunks]
        self.writes: List[bytes] = []
        self.closed = False

    @property
    def in_waiting(self) -> int:
        if not self._chunks:
            return 0
        return len(self._chunks[0])

    def read(self, size: int) -> bytes:
        if size <= 0 or not self._chunks:
            return b""
        head = self._chunks[0]
        chunk = bytes(head[:size])
        del head[:size]
        if not head:
            self._chunks.pop(0)
        return chunk

    def readline(self) -> bytes:
        if self._chunks:
            return bytes(self._chunks.pop(0))
        return b""

    def write(self, data: bytes) -> int:
        self.writes.append(data)
        return len(data)

    def close(self) -> None:
        self.closed = True


def run_self_test() -> None:
    ring = RingBuffer(buffer_size=32, frame_delimiter="|", max_frames=2)
    ring.append(b"a|b|c|")
    if ring.peek_frame() != b"b":
        raise RuntimeError("peek_frame() did not honor max_frames")
    if ring.read_frame() != b"b":
        raise RuntimeError("read_frame() did not return expected first frame")
    if ring.read_frame() != b"c":
        raise RuntimeError("read_frame() did not return expected second frame")
    if ring.read_frame() is not None:
        raise RuntimeError("read_frame() should return None when empty")
    ring.clear()
    if ring.peek_frame() is not None:
        raise RuntimeError("clear() did not reset buffer state")

    callbacks: List[Dict[str, Any]] = []
    adapter = SerialAdapter("mock", 9600, buffer_size=1024, frame_delimiter="|", max_frames=10)
    adapter.register_callback(lambda frame: callbacks.append(frame))
    fake = _FakeSerial([b"{\"va", b"lue\":1}|{\"value\":2}|", b"not-json|"])
    adapter._serial = fake  # type: ignore[attr-defined]

    if adapter.poll() is not None:
        raise RuntimeError("poll() should return None for partial frames")

    frames = adapter.poll_all()
    if len(frames) != 2:
        raise RuntimeError("poll_all() should return all available frames")
    first = frames[0]
    second = frames[1]

    if first is None:
        raise RuntimeError("poll_all() missing first frame")
    if not isinstance(first.get("timestamp"), float):
        raise RuntimeError("poll_all() frame missing timestamp")
    if first.get("raw") != "{\"value\":1}":
        raise RuntimeError("poll_all() raw payload mismatch for first frame")
    parsed_first = first.get("parsed")
    if not isinstance(parsed_first, dict) or parsed_first.get("value") != 1:
        raise RuntimeError("poll_all() parsed payload mismatch for first frame")
    if first.get("value") != 1:
        raise RuntimeError("poll_all() should keep parsed keys at top level for compatibility")
    if second.get("raw") != "{\"value\":2}":
        raise RuntimeError("poll_all() raw payload mismatch for second frame")
    parsed_second = second.get("parsed")
    if not isinstance(parsed_second, dict) or parsed_second.get("value") != 2:
        raise RuntimeError("poll_all() parsed payload mismatch for second frame")
    if second.get("value") != 2:
        raise RuntimeError("poll_all() should keep parsed keys at top level for compatibility")

    first_meta = first.get("meta")
    if not isinstance(first_meta, dict):
        raise RuntimeError("frame meta must be present")
    if first_meta.get("source") != "serial":
        raise RuntimeError("frame meta source mismatch")
    if first_meta.get("size") != len(b"{\"value\":1}"):
        raise RuntimeError("frame meta size mismatch")

    if len(callbacks) != 2:
        raise RuntimeError("callbacks were not fired for poll_all() frames")
    if callbacks[0].get("raw") != "{\"value\":1}" or callbacks[1].get("raw") != "{\"value\":2}":
        raise RuntimeError("callback payload mismatch for poll_all() frames")

    invalid = adapter.poll()
    if invalid is None:
        raise RuntimeError("poll() should return raw frame for non-JSON data")
    if invalid.get("raw") != "not-json":
        raise RuntimeError("poll() raw payload mismatch for non-JSON data")
    if invalid.get("parsed") is not None:
        raise RuntimeError("poll() should set parsed=None for invalid JSON")
    invalid_meta = invalid.get("meta")
    if not isinstance(invalid_meta, dict) or invalid_meta.get("source") != "serial":
        raise RuntimeError("poll() invalid frame missing meta")

    if len(callbacks) != 3:
        raise RuntimeError("callback was not fired for poll() frame")

    if adapter.poll() is not None:
        raise RuntimeError("poll() should return None when no frame is available")

    adapter_compat = SerialAdapter("mock", 9600, frame_delimiter="|")
    fake_compat = _FakeSerial([b"{\"value\":3}|"])
    adapter_compat._serial = fake_compat  # type: ignore[attr-defined]
    compat_frame = adapter_compat.read()
    if compat_frame is None or compat_frame.get("value") != 3:
        raise RuntimeError("read() compatibility path failed")
    adapter_compat.disconnect()

    adapter.write({"value": 1})
    if not fake.writes or fake.writes[0] != b"{\"value\":1}|":
        raise RuntimeError("write() did not send expected JSON")

    adapter.disconnect()
    if not fake.closed:
        raise RuntimeError("disconnect() did not close serial")

    print("SERIAL ADAPTER TEST PASSED")


def main() -> None:
    run_self_test()


if __name__ == "__main__":
    main()
