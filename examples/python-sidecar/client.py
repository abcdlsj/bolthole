"""Talks to the remote notes API through `bolthole connect`, which serves it
on localhost. Any HTTP library in any language works the same way."""
import json
import sys
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8080"

req = urllib.request.Request(
    f"{BASE}/notes",
    data=json.dumps({"text": "written from another machine"}).encode(),
    headers={"content-type": "application/json"},
    method="POST",
)
print("created:", json.load(urllib.request.urlopen(req)))
print("all notes:", json.load(urllib.request.urlopen(f"{BASE}/notes")))
