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

    if snap["error"] and not snap["candles"] and not snap["forming"]:
        return jsonify(snap), 503
    if not snap["candles"] and not snap["forming"]:
        return jsonify(snap), 503

    return jsonify(snap)


@app.route("/api/debug/state")
def debug_state():
    return jsonify(server.snapshot())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
