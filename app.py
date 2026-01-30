from __future__ import annotations

from flask import Flask, jsonify, request, send_from_directory

from ma.demo01 import calc_ma_start_dates, parse_yyyymmddhh
from ma.ma_trend import calc_ma_trend

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


@app.get("/api/ma-trend")
def ma_trend() -> object:
    raw = request.args.get("value", "").strip()
    csv_path = request.args.get("csv", "").strip()
    if not raw:
        return jsonify({"error": "请输入日期(YYYYMMDDhh)。"}), 400
    if not csv_path:
        return jsonify({"error": "请提供 CSV 文件路径。"}), 400

    try:
        end_dt = parse_yyyymmddhh(raw)
        result = calc_ma_trend(end_dt, csv_path)
    except (ValueError, FileNotFoundError) as exc:
        return jsonify({"error": str(exc)}), 400

    return jsonify(
        {
            "input": result.input,
            "latest_ts": result.latest_ts,
            "prev_ts": result.prev_ts,
            "ma30_latest": result.ma30_latest,
            "ma30_prev": result.ma30_prev,
            "ma30_trend": result.ma30_trend,
            "ma60_latest": result.ma60_latest,
            "ma60_prev": result.ma60_prev,
            "ma60_trend": result.ma60_trend,
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
