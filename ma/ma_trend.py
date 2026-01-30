from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .demo01 import INPUT_FMT


@dataclass(frozen=True)
class MaTrendResult:
    input: str
    latest_ts: str
    prev_ts: str
    ma30_latest: float
    ma30_prev: float
    ma30_trend: str
    ma60_latest: float
    ma60_prev: float
    ma60_trend: str


def _parse_float(value: str, field_name: str, row_index: int) -> float:
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"第 {row_index} 行 {field_name} 无法解析为数字。") from exc


def load_ma_csv(csv_path: str) -> list[tuple[datetime, float, float]]:
    path = Path(csv_path).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"CSV 文件不存在: {path}")

    with path.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        required_fields = {"timestamp", "ma30", "ma60"}
        if not reader.fieldnames:
            raise ValueError("CSV 文件缺少表头。")
        missing_fields = required_fields - set(reader.fieldnames)
        if missing_fields:
            missing = ", ".join(sorted(missing_fields))
            raise ValueError(f"CSV 表头缺少字段: {missing}")

        rows: list[tuple[datetime, float, float]] = []
        for row_index, row in enumerate(reader, start=2):
            raw_ts = (row.get("timestamp") or "").strip()
            if not raw_ts:
                raise ValueError(f"第 {row_index} 行 timestamp 不能为空。")
            try:
                ts = datetime.strptime(raw_ts, INPUT_FMT)
            except ValueError as exc:
                raise ValueError(
                    f"第 {row_index} 行 timestamp 格式错误，应为 YYYYMMDDhh。"
                ) from exc

            ma30 = _parse_float((row.get("ma30") or "").strip(), "ma30", row_index)
            ma60 = _parse_float((row.get("ma60") or "").strip(), "ma60", row_index)
            rows.append((ts, ma30, ma60))

    if not rows:
        raise ValueError("CSV 文件没有数据行。")

    rows.sort(key=lambda item: item[0])
    return rows


def _trend_label(latest: float, previous: float) -> str:
    if latest > previous:
        return "up"
    if latest < previous:
        return "down"
    return "flat"


def calc_ma_trend(end_dt: datetime, csv_path: str) -> MaTrendResult:
    rows = load_ma_csv(csv_path)
    filtered = [item for item in rows if item[0] <= end_dt]
    if len(filtered) < 2:
        raise ValueError("CSV 数据不足，至少需要 2 条记录用于趋势判断。")

    latest = filtered[-1]
    previous = filtered[-2]

    ma30_latest = latest[1]
    ma30_prev = previous[1]
    ma60_latest = latest[2]
    ma60_prev = previous[2]

    return MaTrendResult(
        input=end_dt.strftime(INPUT_FMT),
        latest_ts=latest[0].strftime(INPUT_FMT),
        prev_ts=previous[0].strftime(INPUT_FMT),
        ma30_latest=ma30_latest,
        ma30_prev=ma30_prev,
        ma30_trend=_trend_label(ma30_latest, ma30_prev),
        ma60_latest=ma60_latest,
        ma60_prev=ma60_prev,
        ma60_trend=_trend_label(ma60_latest, ma60_prev),
    )
