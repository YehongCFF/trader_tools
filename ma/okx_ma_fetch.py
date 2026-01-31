from __future__ import annotations

import argparse
import csv
import json
import ssl
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import URLError


DEFAULT_BASE_URL = "https://www.okx.com"
ENDPOINT = "/api/v5/market/history-candles"
DEFAULT_INST_ID = "SOL-USDT-SWAP"
DEFAULT_BAR = "1H"
DEFAULT_DAYS = 30
MAX_LIMIT = 100
TIMEOUT_SECONDS = 10
DEFAULT_RETRIES = 3
RETRY_BACKOFF_SECONDS = 1.5
OUTPUT_FMT = "%Y%m%d%H"


def _build_ssl_context(insecure: bool) -> ssl.SSLContext:
    if insecure:
        return ssl._create_unverified_context()
    context = ssl.create_default_context()
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    return context


def _build_opener(proxy: str | None, context: ssl.SSLContext):
    if not proxy:
        return None
    from urllib.request import ProxyHandler, build_opener, HTTPSHandler

    return build_opener(ProxyHandler({"http": proxy, "https": proxy}), HTTPSHandler(context=context))


def _okx_get(
    params: dict[str, str],
    timeout: int,
    retries: int,
    base_url: str,
    proxy: str | None,
    insecure: bool,
) -> list[list[str]]:
    url = f"{base_url}{ENDPOINT}?{urlencode(params)}"
    request = Request(url, headers={"User-Agent": "trader_tools"})
    context = _build_ssl_context(insecure)
    opener = _build_opener(proxy, context)

    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            if opener:
                response = opener.open(request, timeout=timeout)
            else:
                response = urlopen(request, timeout=timeout, context=context)
            with response:
                payload = json.loads(response.read().decode("utf-8"))
            break
        except (URLError, ssl.SSLError, TimeoutError) as exc:
            last_error = exc
            if attempt == retries:
                raise RuntimeError(
                    "网络请求失败，多次重试仍无法连接 OKX。"
                    "请检查本地网络/代理/公司防火墙，或稍后再试。"
                ) from exc
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    else:
        raise RuntimeError("网络请求失败，未能获取 OKX 数据。") from last_error

    if payload.get("code") != "0":
        message = payload.get("msg") or "未知错误"
        raise RuntimeError(f"OKX API 请求失败: {message}")

    data = payload.get("data")
    if not isinstance(data, list):
        raise RuntimeError("OKX API 返回数据格式不正确。")

    return data


def fetch_candles(
    inst_id: str,
    bar: str,
    start_ms: int,
    verbose: bool = False,
    max_batches: int = 200,
    timeout: int = TIMEOUT_SECONDS,
    retries: int = DEFAULT_RETRIES,
    base_url: str = DEFAULT_BASE_URL,
    proxy: str | None = None,
    insecure: bool = False,
) -> list[list[str]]:
    candles: list[list[str]] = []
    before: int | None = None
    after: int | None = None
    paging_mode = "before"
    last_oldest: int | None = None
    batches = 0

    while True:
        batches += 1
        if batches > max_batches:
            raise RuntimeError("分页次数过多，可能遇到接口异常。请稍后重试。")
        params = {
            "instId": inst_id,
            "bar": bar,
            "limit": str(MAX_LIMIT),
        }
        if paging_mode == "before" and before is not None:
            params["before"] = str(before)
        if paging_mode == "after" and after is not None:
            params["after"] = str(after)

        batch = _okx_get(
            params,
            timeout=timeout,
            retries=retries,
            base_url=base_url,
            proxy=proxy,
            insecure=insecure,
        )
        if not batch:
            break

        candles.extend(batch)
        oldest_ts = int(batch[-1][0])
        if verbose:
            oldest_fmt = _format_ts(oldest_ts)
            newest_fmt = _format_ts(int(batch[0][0]))
            print(f"批次 {batches}({paging_mode}): {newest_fmt} -> {oldest_fmt}")
        if last_oldest is not None and oldest_ts >= last_oldest:
            if paging_mode == "before":
                if verbose:
                    print("检测到重复分页，切换为 after 模式继续分页。")
                paging_mode = "after"
                after = oldest_ts
                last_oldest = None
                continue
            raise RuntimeError("分页未推进，OKX 返回重复数据。请稍后重试。")
        last_oldest = oldest_ts
        if oldest_ts <= start_ms:
            break
        if paging_mode == "before":
            before = oldest_ts - 1
        else:
            after = oldest_ts

    return candles


def _rolling_mean(values: Iterable[tuple[int, float]], window: int) -> dict[int, float]:
    queue: deque[float] = deque()
    running_sum = 0.0
    results: dict[int, float] = {}

    for ts, value in values:
        queue.append(value)
        running_sum += value
        if len(queue) > window:
            running_sum -= queue.popleft()
        if len(queue) == window:
            results[ts] = running_sum / window

    return results


def _format_ts(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime(OUTPUT_FMT)


def build_rows(candles: list[list[str]], start_ms: int) -> list[tuple[str, float, float]]:
    parsed = [(int(item[0]), float(item[4])) for item in candles]
    parsed.sort(key=lambda item: item[0])

    ma30 = _rolling_mean(parsed, 30)
    ma60 = _rolling_mean(parsed, 60)

    rows: list[tuple[str, float, float]] = []
    for ts, _ in parsed:
        if ts < start_ms:
            continue
        if ts not in ma30 or ts not in ma60:
            continue
        rows.append((_format_ts(ts), ma30[ts], ma60[ts]))

    return rows


def write_csv(rows: list[tuple[str, float, float]], output_path: str) -> None:
    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["timestamp", "ma30", "ma60"])
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="从 OKX 获取 SOL-USDT 永续合约 1H MA30/MA60 数据并保存为 CSV。",
    )
    parser.add_argument(
        "--inst-id",
        default=DEFAULT_INST_ID,
        help="合约 ID，默认 SOL-USDT-SWAP",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help="获取最近多少天的数据，默认 30 天",
    )
    parser.add_argument(
        "--output",
        default="sol_usdt_ma.csv",
        help="输出 CSV 文件路径，默认 sol_usdt_ma.csv",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="打印分页抓取进度",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=TIMEOUT_SECONDS,
        help="单次请求超时时间（秒），默认 10",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=DEFAULT_RETRIES,
        help="网络请求重试次数，默认 3",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="OKX API 基础地址，默认 https://www.okx.com",
    )
    parser.add_argument(
        "--proxy",
        default=None,
        help="HTTP/HTTPS 代理地址，例如 http://127.0.0.1:7890",
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="跳过 SSL 证书验证（不推荐，仅用于排查 SSL 问题）",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    now = datetime.now(tz=timezone.utc)
    target_start = now - timedelta(days=args.days)
    start_for_ma = target_start - timedelta(hours=59)

    candles = fetch_candles(
        inst_id=args.inst_id,
        bar=DEFAULT_BAR,
        start_ms=int(start_for_ma.timestamp() * 1000),
        verbose=args.verbose,
        timeout=args.timeout,
        retries=args.retries,
        base_url=args.base_url,
        proxy=args.proxy,
        insecure=args.insecure,
    )

    if not candles:
        raise RuntimeError("未能获取到任何 K 线数据。")

    rows = build_rows(candles, start_ms=int(target_start.timestamp() * 1000))
    if not rows:
        raise RuntimeError("数据不足，无法计算 MA30/MA60。")

    write_csv(rows, args.output)
    print(f"已写入 {len(rows)} 行数据到 {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
