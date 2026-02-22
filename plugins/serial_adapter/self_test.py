from __future__ import annotations

from typing import List

try:
    from plugins.serial_adapter.plugin import RingBuffer, SerialAdapter
except ImportError:
    import sys
    import os

    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
    from plugins.serial_adapter.plugin import RingBuffer, SerialAdapter


class _FakeSerial:
    def __init__(self, lines: List[bytes]) -> None:
        self._lines = list(lines)
        self.writes: List[bytes] = []
        self.closed = False

    def readline(self) -> bytes:
        if self._lines:
            return self._lines.pop(0)
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

    adapter = SerialAdapter("mock", 9600, buffer_size=1024, frame_delimiter="|", max_frames=10)
    fake = _FakeSerial([b"{\"va", b"lue\":1}|{\"value\":2}|", b"", b"", b"not-json|"])
    adapter._serial = fake  # type: ignore[attr-defined]

    if adapter.read() is not None:
        raise RuntimeError("read() should return None for partial frames")

    first = adapter.read()
    if first is None:
        raise RuntimeError("read() should return first completed frame")
    if not isinstance(first.get("timestamp"), float):
        raise RuntimeError("read() did not include timestamp")
    if first.get("raw") != "{\"value\":1}":
        raise RuntimeError("read() raw payload mismatch for first frame")
    parsed_first = first.get("parsed")
    if not isinstance(parsed_first, dict) or parsed_first.get("value") != 1:
        raise RuntimeError("read() parsed payload mismatch for first frame")
    if first.get("value") != 1:
        raise RuntimeError("read() should keep parsed keys at top level for compatibility")

    second = adapter.read()
    if second is None:
        raise RuntimeError("read() should return queued second frame")
    if second.get("raw") != "{\"value\":2}":
        raise RuntimeError("read() raw payload mismatch for second frame")
    parsed_second = second.get("parsed")
    if not isinstance(parsed_second, dict) or parsed_second.get("value") != 2:
        raise RuntimeError("read() parsed payload mismatch for second frame")
    if second.get("value") != 2:
        raise RuntimeError("read() should keep parsed keys at top level for compatibility")

    if adapter.read() is not None:
        raise RuntimeError("read() should return None on timeout with no full frame")

    invalid = adapter.read()
    if invalid is None:
        raise RuntimeError("read() should return raw frame for non-JSON data")
    if invalid.get("raw") != "not-json":
        raise RuntimeError("read() raw payload mismatch for non-JSON data")
    if invalid.get("parsed") is not None:
        raise RuntimeError("read() should set parsed=None for invalid JSON")

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
