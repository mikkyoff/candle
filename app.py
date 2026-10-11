"""
HTTP surface. Two routes:

  GET /              → the chart page
  GET /api/candles   → JSON snapshot of the live series

The worker thread is kicked off lazily on the first request so
cold-start errors surface as a JSON 503 rather than a silent hang.
"""

import os
from flask import Flask, jsonify, render_template
from flask_cors import CORS

import server

app = Flask(__name__)
CORS(app)


@app.before_request
def _ensure_worker():
    server.start()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/candles")
def candles():
    snap = server.snapshot()

    # Still waiting on first seed or errored out — tell the client
    # so it can show an honest status instead of "no data".
    if snap["error"] and not snap["seeded"]:
        return jsonify(snap), 503
    if not snap["seeded"]:
        return jsonify(snap), 503

    return jsonify(snap)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
