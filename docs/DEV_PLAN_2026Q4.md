---
AIGC:
    Label: "1"
    ContentProducer: 001191440300708461136T1XGW3
    ProduceID: 60bbebc6db5a512fc5d10abafd5f3b57_8b7beedbcda911f1b307525400393706
    ReservedCode1: mtUVUBll1eNcmoqKO+yOqPRXrduyv0oJb187hSyE2ResZ0mZd5mrAbJMOXboPypg+N9Z92B/inV1txahd/ejstASggVdIraYULlfOTa0swIm0QChNJHQ1brY9c125XfqLw3ZxJ/Ure5/dJAzMrH0LM4F21QA2oTVSQPEz2226+gbI3Ojp1FKM0caJOM=
    ContentPropagator: 001191440300708461136T1XGW3
    PropagateID: 60bbebc6db5a512fc5d10abafd5f3b57_8b7beedbcda911f1b307525400393706
    ReservedCode2: mtUVUBll1eNcmoqKO+yOqPRXrduyv0oJb187hSyE2ResZ0mZd5mrAbJMOXboPypg+N9Z92B/inV1txahd/ejstASggVdIraYULlfOTa0swIm0QChNJHQ1brY9c125XfqLw3ZxJ/Ure5/dJAzMrH0LM4F21QA2oTVSQPEz2226+gbI3Ojp1FKM0caJOM=
---

# OpenXDB 开发计划（v1.1.0 - v1.3.0）

> 文档编号：DEV_PLAN_2026Q4
> 计划周期：2026-10-22 ~ 2026-12-22（约 9 周）
> 适用版本：v1.1.0 / v1.2.0 / v1.3.0

## 1. 文档说明与版本节奏概述

本文档基于《OpenXDB 功能完成度与待完善需求分析》梳理出的需求清单（P0/P1/P2 三级优先级），
将 v1.1.0 至 v1.3.0 三个版本的开发工作落成正式排期，作为后续研发与验收的依据。

**版本节奏原则**：

- 每个版本聚焦一个主题，避免多目标并行稀释投入；
- 高优先级（P0）需求优先排期，低优先级（P2）需求穿插在版本收尾阶段；
- 新功能开发与文档同步并行，防止代码先行、文档滞后；
- 依赖关系明确的项（如显式事务跨节点依赖 2PC 加固）严格串行衔接。

| 版本 | 主题 | 周期 | 聚焦 |
|---|---|---|---|
| v1.1.0 | 收尾与文档同步 | 2 周（10.22 - 11.4） | MySQL 协议收尾、文档纠偏、低风险补全 |
| v1.2.0 | P0 核心短板攻坚 | 4 周（11.5 - 12.1） | 显式事务跨节点、SQL 语义补全、外键 ON UPDATE |
| v1.3.0 | 性能与体验优化 | 3 周（12.2 - 12.22） | 会话锁并行走、备份并行化、隔离级别扩展 |

## 2. 排期总览表

| 版本 | 阶段 | 周期 | 聚焦 |
|---|---|---|---|
| v1.1.0 | 收尾与文档同步 | 2 周（10.22 - 11.4） | MySQL 协议收尾、文档纠偏、低风险补全 |
| v1.2.0 | P0 核心短板攻坚 | 4 周（11.5 - 12.1） | 显式事务跨节点、SQL 语义补全、外键 ON UPDATE |
| v1.3.0 | 性能与体验优化 | 3 周（12.2 - 12.22） | 会话锁并行走、备份并行化、隔离级别扩展 |

## 3. 详细排期

### 3.1 v1.1.0（收尾与文档同步，2026-10-22 ~ 2026-11-04）

| # | 需求项 | 优先级 | 预估工期 | 依赖与说明 |
|---|---|---|---|---|
| 1 | MySQL 协议文档同步 | P1 | 0.5 天 | README / KNOWN_LIMITATIONS / 产品手册补录 `--mysql-port`，先补码后更文档 |
| 2 | MySQL 协议缺口补全 | P1 | 3 天 | COM_CHANGE_USER + COM_STMT_SEND_LONG_DATA + 参数类型扩展 |
| 3 | mysqldump 导入列类型扩展 | P1 | 2 天 | 补充常用 MySQL 列类型/字面量映射 |
| 4 | SQL 执行层边缘分支清理 | P1 | 2 天 | 消除 unsupported constant select column 等错误路径 |
| 5 | fixregions 工具补测试 | - | 1 天 | 活跃迭代项，无测试文件 |
| 6 | 历史文档旧限制清理 | P2 | 1 天 | 清理 T14 等散落的过期描述 |

### 3.2 v1.2.0（P0 核心短板攻坚，2026-11-05 ~ 2026-12-01）

| # | 需求项 | 优先级 | 预估工期 | 依赖与说明 |
|---|---|---|---|---|
| 1 | 显式事务跨 region 写入 | P0 | 10 天 | 必须等 2PC 加固稳定后再动，分布式最大缺口 |
| 2 | HAVING + GROUP BY 语义补全 | P0 | 4 天 | 含聚合参数放宽、GROUP BY 后 ORDER BY/LIMIT 修正，按 T39 P3 清单逐项消项 |
| 3 | 窗口函数 ROWS/RANGE 框架 | P0 | 4 天 | 支持显式 PRECEDING/FOLLOWING |
| 4 | 外键 ON UPDATE CASCADE/SET NULL | P0 | 3 天 | 在现有 RESTRICT 基础上加级联动作 |
| 5 | 存储过程子集增强 | P0 | 5 天 | 评估放开递归限制、补常用 PL/SQL 语句 |

**建议顺序**：P0-1 最先启动（周期长且依赖刚加固的 2PC），P0-4/P0-5 可并行，P0-3 穿插完成。

### 3.3 v1.3.0（性能与体验优化，2026-12-02 ~ 2026-12-22）

| # | 需求项 | 优先级 | 预估工期 | 依赖与说明 |
|---|---|---|---|---|
| 1 | 全局会话锁消除 + 会话级优化 | P0 | 10 天 | 最大性能瓶颈，结合 B2 组提交文档；建议先压测量化串行损耗再动手 |
| 2 | 备份/导入并行化 | P2 | 4 天 | 大数据量场景线性耗时缓解 |
| 3 | Serializable 隔离级别接入 | P2 | 4 天 | storage.ErrConflict 已预留 |
| 4 | 行协议语义修订 | P2 | 2 天 | NULL/空串二义、单元格换行、DECIMAL 精度 |

## 4. 关键提醒

### 排期原则

1. **P0-1 显式事务跨节点建议排最优先**：文档明示的最大能力缺口，与分布式加固主线衔接最紧。
2. **P0-2 性能优化建议前置量化**：先跑一轮基准确认会话锁实际损耗占比，避免投入产出错位。
3. 三个版本约 9 周，衔接 2027 年初 v1.1 里程碑宣传窗口。

### 风险点

- v1.2.0 P0-1（显式事务跨 region 写入）依赖 2PC 加固稳定性，若上游修复延期需顺延该需求排期；
- v1.3.0 性能优化需以基线数据为准，量化不足时先完成压测再排实现；
- 文档同步贯穿全周期，避免 v1.1.0 后再次出现"代码先行、文档滞后"。
*（内容由AI生成，仅供参考）*
