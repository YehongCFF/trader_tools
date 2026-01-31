# trader_tools
一些 Crypto、股票市场交易小工具。

## MA 斜率预测 CSV 格式

当前使用 CSV 提供已计算好的 MA30 / MA60 数据，字段如下：

```
timestamp,ma30,ma60
2025012815,41200.1,40500.7
2025012816,41280.3,40540.2
```

- `timestamp`: 1h K 线结束时间，格式为 `YYYYMMDDhh`
- `ma30`: MA30 数值
- `ma60`: MA60 数值

## OKX MA 数据抓取脚本

`ma/okx_ma_fetch.py` 可从 OKX API 拉取 SOL-USDT 永续合约的 1H K 线，
计算 MA30/MA60，并输出 CSV（`timestamp,ma30,ma60`），默认最近 30 天数据。

示例：

```bash
python ma/okx_ma_fetch.py --output sol_usdt_ma.csv
```

如遇到拉取较慢，可加上 `--verbose` 查看分页进度。
如果遇到网络/SSL 报错，可尝试调大 `--timeout` 或增加 `--retries`，
必要时配置 `--proxy` 或临时使用 `--insecure` 排查证书问题。
