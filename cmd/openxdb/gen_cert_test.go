package main

// T36 商用档：gen-cert 自签名证书生成命令测试。

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenCertWritesUsablePEM(t *testing.T) {
	dir := t.TempDir()
	if err := cmdGenCert([]string{"--out-dir", dir, "--cn", "openxdb-local", "--days", "30"}); err != nil {
		t.Fatalf("cmdGenCert: %v", err)
	}
	certPath := filepath.Join(dir, "tls-cert.pem")
	keyPath := filepath.Join(dir, "tls-key.pem")
	for _, p := range []string{certPath, keyPath} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("expected output %s: %v", p, err)
		}
	}
	// 证书与私钥必须能被标准库加载为有效密钥对
	if _, err := tls.LoadX509KeyPair(certPath, keyPath); err != nil {
		t.Fatalf("LoadX509KeyPair on generated PEM: %v", err)
	}
}

func TestGenCertRefusesOverwrite(t *testing.T) {
	dir := t.TempDir()
	if err := cmdGenCert([]string{"--out-dir", dir}); err != nil {
		t.Fatalf("first gen: %v", err)
	}
	err := cmdGenCert([]string{"--out-dir", dir})
	if err == nil {
		t.Fatalf("second gen without --force must refuse to overwrite")
	}
	if !strings.Contains(err.Error(), "refusing to overwrite") {
		t.Fatalf("unexpected error text: %v", err)
	}
}

func TestGenCertForceOverwrites(t *testing.T) {
	dir := t.TempDir()
	if err := cmdGenCert([]string{"--out-dir", dir}); err != nil {
		t.Fatalf("first gen: %v", err)
	}
	if err := cmdGenCert([]string{"--out-dir", dir, "--force"}); err != nil {
		t.Fatalf("force regen: %v", err)
	}
}

func TestGenCertInvalidDays(t *testing.T) {
	dir := t.TempDir()
	for _, bad := range []string{"0", "-1", "abc", "99999"} {
		err := cmdGenCert([]string{"--out-dir", dir, "--days", bad})
		if err == nil {
			t.Fatalf("--days %s must fail", bad)
		}
	}
}
