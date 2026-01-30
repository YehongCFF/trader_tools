from __future__ import annotations

from flask import Flask, jsonify, request, send_from_directory

from ma.demo01 import calc_ma_start_dates, parse_yyyymmddhh

app = Flask(__name__, static_folder="public", static_url_path="/public")


@app.get("/")
def index() -> object:
    return send_from_directory(app.static_folder, "index.html")


@app.get("/api/ma")
def ma() -> object:
    raw = request.args.get("value", "").strip()
    if not raw:
        return jsonify({"error": "请输入日期(YYYYMMDDhh)。"}), 400

    try:
        end_dt = parse_yyyymmddhh(raw)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    result = calc_ma_start_dates(end_dt)
    return jsonify(
        {
            "input": raw,
            "ma30_start": result.ma30_start,
            "ma60_start": result.ma60_start,
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
