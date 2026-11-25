// Package version 定义 OpenXDB 发布版本号的单一事实源。
//
// 该包不依赖仓库内任何其他包，可被 cmd/openxdb、pkg/mysqlproto、pkg/sql 等
// 任意层引用而不产生依赖环。发版时仅需修改本文件的 Version 常量：
// CLI 版本输出、MySQL 握手版本串（Server Greeting）与 select version()
// 均从该常量派生，避免多处独立常量漂移。
package version

// Version OpenXDB 当前发布版本号（发版唯一改动点）。
const Version = "v2.0.4"

// MySQLServerPrefix MySQL 兼容版本串前缀（mysql CLI 依赖该前缀识别服务端版本）。
const MySQLServerPrefix = "8.0.36-OpenXDB "

// ServerVersion 服务器版本串：8.0.36-OpenXDB v2.0.4。
// 握手包与 select version() 共用该串，保证两处口径恒定一致。
const ServerVersion = MySQLServerPrefix + Version
