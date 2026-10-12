package db

// M16-3 灰度升级辅助：数据文件格式版本（data format version）。
//
// 数据文件格式版本（DataFormatVersion）是升级判定依据：数据目录的格式
// 版本由 marker 文件 data/data_format_version 记录（旧版目录无 marker，
// 视为版本 1，向后兼容）。升级路径 = 旧格式数据被更新版本二进制打开；
// 若数据目录格式版本高于当前二进制（未来版本数据），当前二进制无法安全
// 打开，preflight 判定 FAIL（需先升级二进制）。
//
// 语义：
//   - marker 缺失（M16-2 及更早版本初始化的目录）→ 视为版本 1；
//   - 目录版本 <= 当前二进制版本 → 兼容（含旧数据升级路径）；
//   - 目录版本 > 当前二进制版本 → ErrDataFormatNewer，拒绝打开。

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	// DataFormatVersion 当前二进制支持的数据文件格式版本。RocksDB 数据 +
	// WAL + openxdb.conf + persisted.json（M16-1）格式在此版本内保持兼容；
	// 未来破坏性格式变更时递增本常量并更新兼容矩阵。
	DataFormatVersion = 1

	// DataFormatVersionFile 数据目录下记录格式版本的 marker 文件。
	DataFormatVersionFile = "data_format_version"
)

// ErrDataFormatNewer 数据目录格式版本高于当前二进制：数据由更新版本生成，
// 当前版本无法安全打开，升级前需先升级二进制。
var ErrDataFormatNewer = errors.New("db: data directory uses a newer data format version than this binary")

// ReadDataFormatVersion 读取数据目录的数据文件格式版本。marker 缺失时
// 返回 1（旧版目录默认版本，向前兼容）。marker 内容非法返回错误。
func ReadDataFormatVersion(dir string) (int, error) {
	b, err := os.ReadFile(filepath.Join(dir, DataFormatVersionFile))
	if err != nil {
		if os.IsNotExist(err) {
			return 1, nil
		}
		return 0, fmt.Errorf("db: read data format version: %w", err)
	}
	raw := strings.TrimSpace(string(b))
	v, err := strconv.Atoi(raw)
	if err != nil || v < 1 {
		return 0, fmt.Errorf("db: invalid data format version %q in %s", raw, DataFormatVersionFile)
	}
	return v, nil
}

// CheckDataFormatCompat 校验数据目录格式版本与当前二进制兼容：
// dirVersion > binaryVersion 返回 ErrDataFormatNewer；否则返回 nil。
// 目录不存在 / marker 缺失时按旧目录（版本 1）处理，由调用方自行保证
// 目录存在性检查先于本校验（preflight 先做 data_dir 检查）。
func CheckDataFormatCompat(dir string, binaryVersion int) error {
	dirVersion, err := ReadDataFormatVersion(dir)
	if err != nil {
		return err
	}
	if dirVersion > binaryVersion {
		return fmt.Errorf("%w: dir format v%d > binary v%d (marker: %s)",
			ErrDataFormatNewer, dirVersion, binaryVersion, DataFormatVersionFile)
	}
	return nil
}
