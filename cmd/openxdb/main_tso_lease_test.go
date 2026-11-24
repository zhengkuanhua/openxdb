package main

// v2.1.0 H1（NEW-5）：TSO 租约开关的命令行/配置解析测试。
//
// 重点是"默认关闭"与非法取值拒绝——租约模式在真机验收前不得因默认值
// 或配置拼写错误被意外打开。

import (
	"strings"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/configfile"
)

func TestParseTSOLeaseOptionsDefaultsOff(t *testing.T) {
	on, ttl, err := parseTSOLeaseOptions([]string{"start", "--data-dir", "x"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if on {
		t.Fatal("tso lease must default to off")
	}
	if ttl != 10*time.Second {
		t.Fatalf("default ttl = %s, want 10s", ttl)
	}
}

func TestParseTSOLeaseOptionsBareFlagEnables(t *testing.T) {
	on, _, err := parseTSOLeaseOptions([]string{"start", "--tso-lease"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !on {
		t.Fatal("bare --tso-lease should enable lease mode")
	}
}

func TestParseTSOLeaseOptionsTTLOverride(t *testing.T) {
	on, ttl, err := parseTSOLeaseOptions([]string{"start", "--tso-lease=on", "--tso-lease-ttl", "3s"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !on || ttl != 3*time.Second {
		t.Fatalf("on=%v ttl=%s, want true/3s", on, ttl)
	}
}

func TestParseTSOLeaseOptionsRejectsBadInput(t *testing.T) {
	cases := [][]string{
		{"start", "--tso-lease=maybe"},
		{"start", "--tso-lease-ttl", "abc"},
		{"start", "--tso-lease-ttl", "0s"},
		{"start", "--tso-lease-ttl", "-5s"},
	}
	for _, args := range cases {
		if _, _, err := parseTSOLeaseOptions(args, nil); err == nil {
			t.Fatalf("args %v should be rejected", args)
		} else if !strings.Contains(err.Error(), "tso-lease") {
			t.Fatalf("args %v error should mention tso-lease, got %v", args, err)
		}
	}
}

func TestParseTSOLeaseOptionsFromConfig(t *testing.T) {
	f, err := configfile.Parse("[server]\ntso_lease = on\ntso_lease_ttl = 7s\n")
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	on, ttl, err := parseTSOLeaseOptions([]string{"start"}, f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !on || ttl != 7*time.Second {
		t.Fatalf("on=%v ttl=%s, want true/7s", on, ttl)
	}

	// 命令行优先于配置文件。
	on, ttl, err = parseTSOLeaseOptions([]string{"start", "--tso-lease=off", "--tso-lease-ttl", "2s"}, f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if on || ttl != 2*time.Second {
		t.Fatalf("cli should override config: on=%v ttl=%s", on, ttl)
	}
}

func TestParseTSOLeaseOptionsRejectsBadConfig(t *testing.T) {
	cases := []string{
		"[server]\ntso_lease = maybe\n",
		"[server]\ntso_lease_ttl = soon\n",
		"[server]\ntso_lease_ttl = 0s\n",
	}
	for _, text := range cases {
		f, err := configfile.Parse(text)
		if err != nil {
			t.Fatalf("parse config %q: %v", text, err)
		}
		if _, _, err := parseTSOLeaseOptions([]string{"start"}, f); err == nil {
			t.Fatalf("config %q should be rejected", text)
		}
	}
}

func TestServerKeysAcceptTSOLease(t *testing.T) {
	has := map[string]bool{}
	for _, k := range serverKeys {
		has[k] = true
	}
	if !has["tso_lease"] || !has["tso_lease_ttl"] {
		t.Fatal("serverKeys must whitelist tso_lease / tso_lease_ttl")
	}
}
