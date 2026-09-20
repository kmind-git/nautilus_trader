# 本地模拟交易所接入（rust-trader）

状态：适配进行中（FIX initiator 核心已实现，ExecutionClient 接线为下一步）。
修订日期：2026-09-20。返回 [改造方案](../../orderhub-oms-transformation.md)。

## 目标系统

`D:\projects\zcodeworkspace\rust-trader`（gotrader）：价格-时间优先撮合、手写
FIX 4.2 会话层（acceptor :5001）、只读 REST（:8080）、回放行情（playback）。

## 接入参数

- FIX acceptor：`127.0.0.1:5001`（`SocketAcceptPort` 可配）
- 会话：CompID 由 acceptor 的 `[SESSION]` 声明准入（未声明即拒）；
  OrderHub initiator 需要独立的 SenderCompID（建议 `ORDERHUB`）并在
  acceptor 配置中声明，或使用 `DynamicSessions=Y`
- REST（只读，无鉴权）：`/api/instruments/`、`/api/book/{SYMBOL}`、
  `/api/stats/{SYMBOL}`、`/api/sessions`

## 能力矩阵（测试范围依据）

| 能力 | 支持 | 测试策略 |
| --- | --- | --- |
| 限价单（OrdType=2） | ✅ | 全流程测试 |
| 市价单（OrdType=1） | ✅ | 全流程测试 |
| 有效期 DAY（TIF=0） | ✅（仅此一种） | 全流程测试 |
| 撤单（35=F） | ✅ | 撤单竞争测试 |
| 改单（35=G，撤单替换） | ✅ | 暂不接入（OrderHub 无 ModifyOrder） |
| 拒单回报（ExecType=8） | ✅ | 非法参数拒单测试 |
| 部分成交（ExecType=1） | ✅ | 限价单挂簿后逐步成交测试 |
| ExecType New/2/4/5/C | ✅ | 随全流程断言 |
| TIF GTC/IOC/FOK | ❌ | 不测试；OrderHub 在该通道拒绝非 DAY |
| post_only / reduce_only | ❌ 无对应 FIX 字段 | 不测试；能力表披露，不静默丢弃 |
| 冰山/条件单/组合单 | ❌ | 不测试 |
| 行情 FIX 订阅（W/推送） | ❌（playback 单向） | OrderHub 不经该通道取行情 |
| 行情 REST 轮询 | ✅（只读快照） | 盯市数据源候选，暂不接 |
| 合约 | IBM/AAPL/AMZN/GOOG/FB/NFLX/ORCL/700/9988/1810 | 以 AAPL 为主 |
| 价格/数量精度 | 任意十进制 | 适配层不截断 |

HandlInst 必须为 1（自动化私有执行）；HeartBtInt 必须为正。

## 不支持项的处置原则

按方案能力表原则：客户端请求了该通道不支持的指令组合时明确拒绝并
回带稳定原因码，不静默丢弃字段、不伪造支持。
