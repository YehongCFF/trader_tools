from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime, timedelta


INPUT_FMT = "%Y%m%d%H"


@dataclass(frozen=True)
class MaStartDates:
    ma30_start: str
    ma60_start: str


def parse_yyyymmddhh(value: str) -> datetime:
    try:
        return datetime.strptime(value, INPUT_FMT)
    except ValueError as exc:
        raise ValueError("输入格式错误，必须为YYYYMMDDhh，例如 2025012816") from exc


def calc_ma_start_dates(end_dt: datetime) -> MaStartDates:
    # 1h K线周期：MA30/MA60 需要 30/60 根K线，起始时间=结束时间- (N-1)小时
    ma30_start = end_dt - timedelta(hours=29)
    ma60_start = end_dt - timedelta(hours=59)
    return MaStartDates(
        ma30_start=ma30_start.strftime(INPUT_FMT),
        ma60_start=ma60_start.strftime(INPUT_FMT),
    )


def main(argv: list[str]) -> int:
    if len(argv) >= 2:
        raw = argv[1].strip()
    else:
        raw = input("请输入日期(YYYYMMDDhh): ").strip()

    end_dt = parse_yyyymmddhh(raw)
    result = calc_ma_start_dates(end_dt)

    print(f"输入时间:   {raw}")
    print(f"MA30起始: {result.ma30_start}")
    print(f"MA60起始: {result.ma60_start}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
