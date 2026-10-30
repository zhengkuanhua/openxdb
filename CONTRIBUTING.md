# Contributing to OpenXDB

感谢你愿意为 OpenXDB 贡献代码。OpenXDB 是一个从零自研的学习型数据库内核项目，欢迎任何形式的参与：报告问题、提交 PR、改进文档、性能复测等。

## 开发环境

- Go 1.22+（`go.mod` 声明 `go 1.22`）
- 存储层基于 RocksDB（cgo 绑定），构建时需要可用的 C/C++ 工具链（Windows 建议 MSYS2/MinGW 或 WSL；Linux/macOS 使用系统 gcc/clang）
- 无需额外数据库运行时，OpenXDB 自带自研二进制协议与 SQL 子集

## 构建

```bash
go build ./...
```

## 测试

```bash
go test ./...
```

- 提交前请确保 `go build ./...` 与 `go test ./...` 全部通过。
- 涉及性能/正确性变更时，建议补充对应的单元测试或受控复测记录（参考 `docs/experiments/` 下的实验文档，如 E5 复测）。

## 代码风格

- 使用 `gofmt` 格式化，保持标准 Go 风格。
- 提交前运行 `go vet ./...` 消除明显问题。
- 命名遵循 Go 惯例（驼峰、导出符号带注释）。
- 新增文件需显式 `git add <path>`，**禁止** `git add -A` / `git add -u` 批量暂存，避免误提交未完成/不入库的文件（如内部排期文档）。

## 提交规范

- 提交信息使用 Conventional Commits 风格：`type(scope): description`。
  常用 type：`feat` / `fix` / `docs` / `chore` / `refactor` / `perf` / `test` / `revert`。
  示例：`feat(sql): add EXPLAIN node-level timing`、`docs: add v1.17.0 release notes`。
- 一个提交只做一件事，保持原子性。
- 提交前先 `git status` 核对暂存内容，确保只包含本次变更文件。

## PR 流程

1. Fork 仓库并创建特性分支。
2. 按 [PR 模板](../.github/PULL_REQUEST_TEMPLATE.md) 填写变更摘要、动机、测试结果与影响面。
3. 关联相关 Issue（如有）。
4. 等待 review，按反馈修改。

## 行为准则

- 讨论保持友善、就事论事。
- 涉及数据安全/一致性/事务语义的改动需格外谨慎，并在 PR 中说明设计取舍。
