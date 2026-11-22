"""Pruebas locales del binario: python tests/e2e.py (después de cargo build)."""

import hashlib
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BIN = ROOT / "target" / "debug" / "meiloader.exe"
DATA = bytes(range(256)) * (3 * 4096 + 1)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        pass

    def do_GET(self):
        mode = self.path.strip("/")
        requested = self.headers.get("Range")
        if mode == "ranges" and requested and requested != "bytes=0-0":
            with self.server.requests_lock:
                self.server.ranges_requested.append(requested)
        partial = requested and (mode not in {"ignore", "late-ignore"} or requested == "bytes=0-0")
        if partial:
            start, end = map(int, requested.removeprefix("bytes=").split("-"))
            end = min(end, len(DATA) - 1)
            body = DATA[start : end + 1]
            content_range = f"bytes {start}-{end}/{len(DATA)}"
            if mode == "bad" and requested != "bytes=0-0":
                content_range = f"bytes {start + 1}-{end}/{len(DATA)}"
            self.send_response(206)
            self.send_header("Content-Range", content_range)
        else:
            body = DATA
            self.send_response(200)
        self.send_header("ETag", '"same-file"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class DownloadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.server.requests_lock = threading.Lock()
        cls.server.ranges_requested = []
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def run_download(self, mode, connections=8):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "result.bin"
            url = f"http://127.0.0.1:{self.server.server_port}/{mode}"
            result = subprocess.run(
                [str(BIN), url, "--output", str(output), "--connections", str(connections)],
                capture_output=True, text=True, timeout=30,
            )
            if mode == "bad":
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(output.exists())
            else:
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(
                    hashlib.sha256(output.read_bytes()).digest(), hashlib.sha256(DATA).digest()
                )

    def test_parallel(self):
        self.run_download("ranges")
        with self.server.requests_lock:
            self.assertGreater(len(self.server.ranges_requested), 3)

    def test_single(self):
        self.run_download("ranges", connections=1)

    def test_server_ignores_ranges(self):
        self.run_download("ignore")

    def test_server_stops_honoring_ranges(self):
        self.run_download("late-ignore")

    def test_rejects_wrong_content_range(self):
        self.run_download("bad")


if __name__ == "__main__":
    if sys.argv[1:] == ["--serve"]:
        server = ThreadingHTTPServer(("127.0.0.1", 8765), Handler)
        print("Servidor de prueba: http://127.0.0.1:8765/ranges", flush=True)
        print(f"SHA-256 esperado: {hashlib.sha256(DATA).hexdigest()}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()
    else:
        unittest.main()
