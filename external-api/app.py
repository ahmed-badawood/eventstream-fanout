from flask import Flask, request, jsonify
import time
app = Flask(__name__)
seen = set()

@app.post("/events")
def events():
    idem = request.headers.get("Idempotency-Key")
    if not idem: return jsonify({"error":"missing Idempotency-Key"}), 400
    if idem in seen: return jsonify({"status":"duplicate-ignored"}), 200
    seen.add(idem)
    return jsonify({"status":"ok","received_at":int(time.time()*1000)}), 200

@app.get("/health")
def health():
    return "ok", 200
