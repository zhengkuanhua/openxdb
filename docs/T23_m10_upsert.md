# T23 M10 功能簇（3/3）：UPSERT（ON DUPLICATE KEY UPDATE + REPLACE INTO）

> 版本：v5.0-P12-M10, v0.14.0-alpha
> 日期：2026-10-11
> 范围：`pkg/sql`（lexer / ast / parser / executor）+ 单元测试（`m10_upsert_test.go`）+ README 勾选
> 闭环：`go build ./...` 与 `go test ./... -count=1` 全绿，不 push
> 提交：`M10: upsert (ON DUPLICATE KEY UPDATE + REPLACE INTO)`（本地，未 push）

## 背景
SQL 层此前 INSERT 遇主键冲突直接报 `duplicate primary key`，无法按 MySQL 语义做"冲突即更新"或"删旧插新"。M10 UPSERT 补齐两条 MySQL 兼容路径：

- **INSERT ... ON DUPLICATE KEY UPDATE col=expr**：插入行与既有行主键/唯一键冲突时，改为更新该行指定列（affected=2，MySQL 语义）；无冲突时正常插入（affected=1）。
- **REPLACE INTO**：冲突时删除所有冲突行（含唯一索引冲突、外键 CASCADE 联动删除）后插入新行，删除与插入在同一提交内原子完成（affected=删除数+1）。
- 无唯一约束（仅隐式主键=首列）时主键不冲突即等同普通 INSERT；主键仍是参与冲突判定的唯一约束，与 MySQL 一致。

## 设计

### 语法
```sql
INSERT INTO t (id, name, age) VALUES (1, 'alice', 30)
    ON DUPLICATE KEY UPDATE name='alice2', age=31;
REPLACE INTO t (id, name, age) VALUES (1, 'bob', 26), (4, 'dave', 40);
INSERT INTO t (id, name) VALUES (2, 'c') ON DUPLICATE KEY UPDATE name='x';
CREATE UNIQUE INDEX ux_email ON u (email);   -- 唯一索引参与冲突判定
```

### 词法 / 语法层
- `lexer.go`：关键字表已含 `REPLACE` / `DUPLICATE` / `UNIQUE`（M10 规划时预埋），`ON` / `UPDATE` 复用既有关键字；
- `ast.go`：
  - `InsertStmt` 新增 `OnDup *OnDuplicateUpdate` 与 `Replace bool`；
  - 新增 `OnDuplicateUpdate{Sets []SetItem}`，复用 UPDATE 的 `SetItem{Col, Val}`；
  - `CreateIndexStmt` 与 `IndexMeta` 新增 `Unique bool`（唯一索引登记，供冲突判定使用）；
- `parser.go`：
  - `parseStmt` 分发 `REPLACE` → `parseInsertInto(true)`；
  - `parseCreate` 识别 `UNIQUE INDEX` → 走 `parseCreateIndex` 并标记 `Unique`；
  - `parseInsertInto` 支持 REPLACE 分发；INSERT 语句尾部遇 `ON DUPLICATE KEY UPDATE` 调 `parseSetItems` 填充 `OnDup`；
  - 新增公共 `parseSetItems`（`col=expr, col=expr`），`parseUpdate` 复用同一实现。

### 执行层（executor）
- 主入口 `insertRowUpsertOps(tx, tabs, meta, cols, row, onDup, replace, pending)`：
  1. `buildRow` 补默认值 → `checkFKOnRow` 外键校验 → `pkOf` 取主键 → `locateRow` + `getKVCluster` 判定磁盘行存在；
  2. `upsertConflicts` 收集全部冲突行主键：磁盘主键冲突 + 磁盘各 UNIQUE 索引冲突（`scanMergedCluster` 区间扫描）+ 语句内 pending 行冲突（主键/唯一双通道），按发现顺序去重；
  3. 无冲突 → 普通插入（affected=1）；有冲突 → 按 `ON DUP` / `REPLACE` / 普通 INSERT（报 `duplicate primary key` / `duplicate unique key`）三分支。
- `onDupUpdateOps`：取目标行（主键冲突优先于唯一索引冲突），逐 SET 列求值并类型强转，写回目标行键（affected=2）；仅更新指定列，未涉及的列保留原值；SET 包含主键列时回滚报 `cannot modify primary key column`。
- `replaceRowOps`：对每个冲突行——外键 CASCADE 联动删除（`collectFKDeleteOps`）+ 删索引项（`delIndexKeyOps`）+ 删行键；随后 `encodeRow` 新行 Put 与索引键 Put（`putIndexKeyOps` 以 `ignore` 跳过被删行的旧索引键）；删除与插入同一批 `kvOp` 原子提交。**关键细节**：主键冲突时跳过行键 Delete——存储层 WriteBatch 先 Put 后 Delete，同 key 的 Delete 会覆盖 Put 导致新行丢失，因此主键冲突仅用 Put 覆盖（索引项与外键联动仍正常处理）。
- `upsertTargetRow`：定位冲突行，优先语句内 pending 行（本语句前序已计划写入的最终状态），否则磁盘行；唯一索引冲突返回冲突行（pk≠新 pk），主键冲突返回同 pk 行。
- pending 维护：`dropPending` / `replacePending` 保证语句内多行的主键/唯一冲突按最终状态判定（如 `REPLACE INTO t VALUES (1,...),(1,...)` 第二条以第一条为冲突目标）。
- 接线：`execInsert` 改调 `insertRowUpsertOps` 并累加 affected；`execUpdate` 记录语句内最终行并传入 `putIndexKeyOps` 做唯一性预检（UPDATE 改值撞唯一索引 → 回滚）；`execCreateIndex` / `ADD_INDEX` 回填既有数据唯一性检查并登记 `idx.Unique`。

## 测试（pkg/sql/m10_upsert_test.go）

| 测试 | 覆盖 |
|---|---|
| `TestUpsertOnDuplicateKeyPKConflict` | 主键冲突 ON DUP 更新（affected=2）+ 无冲突插入（affected=1） |
| `TestUpsertOnDuplicateKeyUniqueConflict` | UNIQUE 索引冲突 ON DUP 更新冲突行 |
| `TestUpsertNoUniqueConstraintLikeInsert` | 无唯一索引（仅主键）表：主键不同 → 普通插入不触发 ON DUP |
| `TestUpsertReplaceIntoPK` | REPLACE 主键冲突：删旧插新 affected=2、行数不变 |
| `TestUpsertReplaceIntoUnique` | REPLACE 唯一索引冲突：删冲突行插新行（不同主键） |
| `TestUpsertReplaceUniqueMultiConflicts` | 一行与多条既有行唯一冲突（复合唯一）→ 全部删除后插入 |
| `TestUpsertMultiRowsMixed` | 多行混合：冲突更新 + 无冲突插入 affected 累加 |
| `TestUpsertOnDuplicateStatementPendingConflict` | 语句内重复主键：第二行更新 pending 第一行（affected=1+2） |
| `TestUpsertReplaceMultiRowMixed` | REPLACE 多行：冲突 + 无冲突 + 冲突 affected=5 |
| `TestUpsertOnDuplicatePKColumnErr` | ON DUP SET 修改主键列 → 报错且回滚 |
| `TestUpsertInsertUniqueDuplicateErr` | 普通 INSERT 撞唯一索引 → 报错 |
| `TestUpsertUniqueIndexCreateDuplicateErr` | 对含重复值的列建 UNIQUE INDEX → 报错回滚 |
| `TestUpsertUpdateUniqueConflictErr` | UPDATE 改值撞唯一索引 → 报错回滚 |
| `TestUpsertOnDuplicateFKRestrict` | ON DUP 更新后违反外键 RESTRICT → 报错回滚 |
| `TestUpsertReplaceFKRestrict` | REPLACE 违反外键 RESTRICT → 报错回滚 |
| `TestUpsertReplaceFKCascade` | REPLACE 父行 → CASCADE 删除子行 + 父行替换（affected=2） |
| `TestUpsertReplaceNoUniqueConstraintLikeInsert` | 无唯一索引表 REPLACE 不同主键 → 普通插入 affected=2 |

## 取舍说明
- **唯一约束来源**：本引擎建表默认首列隐式主键（所有表均有主键），因此严格"无任何唯一键"的表不存在；ON DUP / REPLACE 的"无冲突即普通插入"语义在引擎中等价于"主键/唯一索引均不冲突"；
- **affected 计数**：ON DUP 更新 1 行计 2、插入计 1（MySQL 语义）；REPLACE 计 删除数+插入数 1；
- **更新范围**：ON DUP 仅更新 SET 指定列，未指定列保留冲突行原值（与 MySQL 一致）；
- **原子性**：REPLACE 的删旧插新在同一事务提交内完成，任一步失败整体回滚；ON DUP 更新失败同样整体回滚（如外键 RESTRICT、唯一索引撞车）；
- **语句内一致性**：同语句多行重复冲突按 pending 最终状态判定，避免"第一行冲突第二行却不冲突"的歧义；
- **存储层约束**：WriteBatch 先 Put 后 Delete 的同 key 语义导致 REPLACE 主键冲突必须跳过行键 Delete（见 replaceRowOps 注释），这是本实现与存储层的适配点，后续若存储层改为"操作序保持"可简化。

## README 勾选

- Status 行更新为 `M10 SQL compatibility cluster: CTE + window functions + UPSERT (v5.0-P12-M10), v0.14.0-alpha.`
- T 表格追加 `+ T23 M10 SQL compatibility cluster: CTE + window functions + UPSERT`
- 文档列表追加 `T23_m10_upsert.md`；本子功能提交暂不修改 README，整簇闭环时统一更新 Status 与 Roadmap（README 勾选约定）。
