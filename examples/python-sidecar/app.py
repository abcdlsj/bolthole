"""A tiny notes API in plain Python (standard library only).

It knows nothing about relays, encryption or Go. The bolthole sidecar
exposes it; see README.md.
"""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

NOTES = []


class Handler(BaseHTTPRequestHandler):
    def _send(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/notes":
            return self._send(200, NOTES)
        self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/notes":
            return self._send(404, {"error": "not found"})
        length = int(self.headers.get("content-length") or 0)
        note = json.loads(self.rfile.read(length) or b"{}")
        note["id"] = len(NOTES) + 1
        NOTES.append(note)
        self._send(201, note)


if __name__ == "__main__":
    print("notes API on http://127.0.0.1:5000")
    ThreadingHTTPServer(("127.0.0.1", 5000), Handler).serve_forever()
