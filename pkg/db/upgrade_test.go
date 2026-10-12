package db

// M16-3 数据格式版本测试：marker 读取、默认版本、兼容判定
// （升级判定依据：目录格式版本 <= 二进制版本可升级）。

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestReadDataFormatVersionDefaultMarker 旧目录（M16-2 及更早版本初始化，
// 无 data_format_version marker）视为数据格式版本 1，向后兼容。
func TestReadDataFormatVersionDefaultMarker(t *testing.T) {
	dir := t.TempDir()
	if err := Init(dir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, DataFormatVersionFile)); !os.IsNotExist(err) {
		t.Fatalf("expected no marker after Init, got %v", err)
	}
	v, err := ReadDataFormatVersion(dir)
	if err != nil {
		t.Fatalf("ReadDataFormatVersion: %v", err)
	}
	if v != 1 {
		t.Fatalf("default marker: want 1, got %d", v)
	}
}

// TestReadDataFormatVersionMarker marker 存在时读取数值；非法内容报错。
func TestReadDataFormatVersionMarker(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, DataFormatVersionFile)
	if err := os.WriteFile(marker, []byte("7\n"), 0o644); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	v, err := ReadDataFormatVersion(dir)
	if err != nil || v != 7 {
		t.Fatalf("want 7, got %d err %v", v, err)
	}
	if err := os.WriteFile(marker, []byte("abc"), 0o644); err != nil {
		t.Fatalf("write bad marker: %v", err)
	}
	if _, err := ReadDataFormatVersion(dir); err == nil {
		t.Fatal("invalid marker must return error")
	}
}

// TestCheckDataFormatCompat 目录格式版本 <= 二进制版本 → 兼容（升级路径）；
// 目录格式版本 > 二进制版本 → ErrDataFormatNewer（需先升级二进制）。
func TestCheckDataFormatCompat(t *testing.T) {
	dir := t.TempDir()
	if err := Init(dir); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := CheckDataFormatCompat(dir, DataFormatVersion); err != nil {
		t.Fatalf("v1 dir vs binary v1: want nil, got %v", err)
	}
	marker := filepath.Join(dir, DataFormatVersionFile)
	if err := os.WriteFile(marker, []byte("999\n"), 0o644); err != nil {
		t.Fatalf("write future marker: %v", err)
	}
	err := CheckDataFormatCompat(dir, DataFormatVersion)
	if err == nil {
		t.Fatal("future data format must be rejected")
	}
	if !errors.Is(err, ErrDataFormatNewer) {
		t.Fatalf("want ErrDataFormatNewer, got %v", err)
	}
	if !strings.Contains(err.Error(), "v999") {
		t.Fatalf("error should mention versions: %v", err)
	}
}
