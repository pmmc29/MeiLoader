"""Pruebas locales del binario: python tests/e2e.py (después de cargo build)."""

import hashlib
import json
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BIN = ROOT / "target" / "debug" / "meiloader.exe"
DATA = bytes(range(256)) * (3 * 4096 + 1)
LARGE_DATA = DATA * 11


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def handle(self):
        try:
            super().handle()
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            pass

    def log_message(self, *_args):
        pass

    def do_GET(self):
        mode = self.path.strip("/")
        data = LARGE_DATA if mode == "limited" else DATA
        requested = self.headers.get("Range")
        if mode == "probe-limit" and requested == "bytes=0-0":
            with self.server.requests_lock:
                fail_now = self.server.fail_once
                self.server.fail_once = False
            if fail_now:
                self.send_response(429)
                self.send_header("Retry-After", "0")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
        if mode == "ranges" and requested and requested != "bytes=0-0":
            with self.server.requests_lock:
                self.server.ranges_requested.append(requested)
        if mode in {"flaky", "retry", "rate-limit"} and requested and requested.startswith("bytes=0-") and requested != "bytes=0-0":
            with self.server.requests_lock:
                fail_now = self.server.fail_once
                self.server.fail_once = False
            if fail_now:
                time.sleep(0.2)
                self.send_response({"flaky": 400, "retry": 503, "rate-limit": 429}[mode])
                self.send_header("Content-Length", "0")
                if mode == "rate-limit":
                    self.send_header("Retry-After", "1")
                self.end_headers()
                return
        partial = requested and (mode not in {"ignore", "late-ignore"} or requested == "bytes=0-0")
        if partial:
            start, end = map(int, requested.removeprefix("bytes=").split("-"))
            end = min(end, len(data) - 1)
            body = data[start : end + 1]
            content_range = f"bytes {start}-{end}/{len(data)}"
            if mode == "bad" and requested != "bytes=0-0":
                content_range = f"bytes {start + 1}-{end}/{len(data)}"
            self.send_response(206)
            self.send_header("Content-Range", content_range)
        else:
            body = data
            self.send_response(200)
        self.send_header("ETag", self.server.etag)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if mode == "limited" and len(body) > 1:
            for offset in range(0, len(body), 64 * 1024):
                self.wfile.write(body[offset : offset + 64 * 1024])
                time.sleep(0.02)
        else:
            self.wfile.write(body)


class DownloadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.server.requests_lock = threading.Lock()
        cls.server.ranges_requested = []
        cls.server.fail_once = False
        cls.server.etag = '"same-file"'
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        self.server.fail_once = False
        self.server.etag = '"same-file"'

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
            return result

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

    def test_resume_after_interruption(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "result.bin"
            url = f"http://127.0.0.1:{self.server.server_port}/flaky"
            command = [str(BIN), url, "--output", str(output), "--connections", "3"]
            self.server.fail_once = True
            first = subprocess.run(command, capture_output=True, text=True, timeout=30)
            self.assertNotEqual(first.returncode, 0)
            part = Path(str(output) + ".meiloader.part")
            metadata = Path(str(output) + ".meiloader.json")
            self.assertTrue(part.exists())
            self.assertTrue(metadata.exists())
            state = json.loads(metadata.read_text())
            self.assertNotIn(url, metadata.read_text())
            self.assertGreater(sum(state["completed"]), 0)

            resumed = subprocess.run(command[:-1] + ["1"], capture_output=True, text=True, timeout=30)
            self.assertEqual(resumed.returncode, 0, resumed.stderr)
            self.assertIn("Reanudando", resumed.stderr)
            self.assertEqual(hashlib.sha256(output.read_bytes()).digest(), hashlib.sha256(DATA).digest())
            self.assertFalse(part.exists())
            self.assertFalse(metadata.exists())

    def test_refuses_changed_remote_file(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "result.bin"
            url = f"http://127.0.0.1:{self.server.server_port}/flaky"
            command = [str(BIN), url, "--output", str(output), "--connections", "3"]
            self.server.fail_once = True
            first = subprocess.run(command, capture_output=True, text=True, timeout=30)
            self.assertNotEqual(first.returncode, 0)
            self.server.etag = '"changed-file"'
            second = subprocess.run(command, capture_output=True, text=True, timeout=30)
            self.assertNotEqual(second.returncode, 0)
            self.assertIn("archivo remoto cambi", second.stderr)
            self.assertFalse(output.exists())
            self.assertTrue(Path(str(output) + ".meiloader.part").exists())

    def test_retries_transient_server_error(self):
        self.server.fail_once = True
        result = self.run_download("retry")
        self.assertIn("Reintentando tramo", result.stderr)

    def test_reduces_connections_on_rate_limit(self):
        self.server.fail_once = True
        start = time.perf_counter()
        result = self.run_download("rate-limit")
        self.assertIn("Servidor limitado", result.stderr)
        self.assertGreaterEqual(time.perf_counter() - start, 1.0)

    def test_retries_initial_probe(self):
        self.server.fail_once = True
        self.run_download("probe-limit")

    def test_auto_tuning_improves_limited_connection(self):
        with tempfile.TemporaryDirectory() as directory:
            url = f"http://127.0.0.1:{self.server.server_port}/limited"
            fixed_output = Path(directory) / "fixed.bin"
            auto_output = Path(directory) / "auto.bin"
            fixed_command = [str(BIN), url, "--output", str(fixed_output), "--connections", "1"]
            auto_command = [str(BIN), url, "--output", str(auto_output), "--connections", "8"]

            start = time.perf_counter()
            fixed = subprocess.run(fixed_command, capture_output=True, text=True, timeout=40)
            fixed_time = time.perf_counter() - start
            self.assertEqual(fixed.returncode, 0, fixed.stderr)

            start = time.perf_counter()
            automatic = subprocess.run(auto_command, capture_output=True, text=True, timeout=40)
            auto_time = time.perf_counter() - start
            self.assertEqual(automatic.returncode, 0, automatic.stderr)
            self.assertIn("Ajustando conexiones: 1 -> 2", automatic.stderr)
            self.assertIn("MiB/s", automatic.stderr)
            self.assertEqual(hashlib.sha256(auto_output.read_bytes()).digest(), hashlib.sha256(LARGE_DATA).digest())
            self.assertLess(auto_time, fixed_time * 0.85)
            print(f"\nRendimiento controlado: una conexion {fixed_time:.2f} s; automatico {auto_time:.2f} s")


if __name__ == "__main__":
    if sys.argv[1:] == ["--serve"]:
        server = ThreadingHTTPServer(("127.0.0.1", 8765), Handler)
        server.requests_lock = threading.Lock()
        server.ranges_requested = []
        server.fail_once = False
        server.etag = '"same-file"'
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
