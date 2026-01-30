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
