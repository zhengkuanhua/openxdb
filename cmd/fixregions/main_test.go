package main

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// fakeStore 内存版 kvStore：可注入 Get/Write 错误，并记录最近一次写入批次。
type fakeStore struct {
	data     map[string][]byte
	getErr   error
	writeErr error
	lastWB   *storage.WriteBatch
}

func newFakeStore() *fakeStore {
	return &fakeStore{data: map[string][]byte{}}
}

func (f *fakeStore) Get(key []byte) ([]byte, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	v, ok := f.data[string(key)]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return v, nil
}

func (f *fakeStore) Write(wb *storage.WriteBatch) error {
	if f.writeErr != nil {
		return f.writeErr
	}
	f.lastWB = wb
	for _, kv := range wb.Puts {
		f.data[string(kv.Key)] = kv.Value
	}
	return nil
}

// sampleRegions 构造两条显式 region：rid=1 与 rid=4。
func sampleRegions() []sharding.RegionInfo {
	return []sharding.RegionInfo{
		{RegionID: 1, TableID: 10, Node: "node-1", Local: true, Explicit: true, StartKey: []byte{0x01}, EndKey: []byte{0x02}},
		{RegionID: 4, TableID: 10, Node: "node-1", Local: true, Explicit: true, StartKey: []byte{0x02}},
	}
}

func mustMeta(t *testing.T, seq uint64, regions []sharding.RegionInfo) []byte {
	t.Helper()
	raw, err := sharding.MarshalMeta(seq, regions)
	if err != nil {
		t.Fatalf("MarshalMeta: %v", err)
	}
	return raw
}

func runProcess(t *testing.T, o *options, st kvStore) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := process(o, st, &out, &errb)
	return code, out.String(), errb.String()
}

func TestParseArgs(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want options
	}{
		{"fix basic", []string{"-dir", "d1", "-region", "4", "-node", "node-2"}, options{dir: "d1", rid: 4, node: "node-2"}},
		{"fix with local", []string{"-dir", "d1", "-region", "4", "-node", "node-2", "-local"}, options{dir: "d1", rid: 4, node: "node-2", localOn: true}},
		{"dump mode", []string{"-dir", "d1", "-dump", "out.txt"}, options{dir: "d1", dump: "out.txt"}},
		{"empty args", []string{}, options{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			got, err := parseArgs(fs, c.args)
			if err != nil {
				t.Fatalf("parseArgs(%v): %v", c.args, err)
			}
			if *got != c.want {
				t.Fatalf("parseArgs(%v) = %+v, want %+v", c.args, *got, c.want)
			}
		})
	}
}

func TestParseArgsUnknownFlag(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	if _, err := parseArgs(fs, []string{"-bogus"}); err == nil {
		t.Fatal("expected error for unknown flag")
	}
}

func TestRunMissingDir(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	var out, errb bytes.Buffer
	code := run(fs, []string{"-region", "4", "-node", "node-2"}, &out, &errb)
	if code != 2 {
		t.Fatalf("code = %d, want 2", code)
	}
	if !strings.Contains(errb.String(), "missing -dir") {
		t.Fatalf("stderr = %q, want contains missing -dir", errb.String())
	}
}

func TestProcessMissingNode(t *testing.T) {
	st := newFakeStore()
	st.data["m:regions"] = mustMeta(t, 1, sampleRegions())
	code, _, errs := runProcess(t, &options{dir: "d1", rid: 4}, st)
	if code != 2 {
		t.Fatalf("code = %d, want 2", code)
	}
	if !strings.Contains(errs, "missing -node") {
		t.Fatalf("stderr = %q, want contains missing -node", errs)
	}
	if st.lastWB != nil {
		t.Fatal("expected no write on missing -node")
	}
}

func TestProcessRegionsNotFound(t *testing.T) {
	st := newFakeStore()
	code, _, errs := runProcess(t, &options{dir: "d1", rid: 4, node: "node-2"}, st)
	if code != 0 {
		t.Fatalf("code = %d, want 0", code)
	}
	if !strings.Contains(errs, "m:regions not found") {
		t.Fatalf("stderr = %q, want contains m:regions not found", errs)
	}
}

func TestProcessGetError(t *testing.T) {
	st := newFakeStore()
	st.getErr = errors.New("engine broke")
	code, _, errs := runProcess(t, &options{dir: "d1", rid: 4, node: "node-2"}, st)
	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	if !strings.Contains(errs, "get m:regions: engine broke") {
		t.Fatalf("stderr = %q, want contains get m:regions", errs)
	}
}

func TestProcessUnmarshalError(t *testing.T) {
	st := newFakeStore()
	st.data["m:regions"] = []byte("{not-json")
	code, _, errs := runProcess(t, &options{dir: "d1", rid: 4, node: "node-2"}, st)
	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	if !strings.Contains(errs, "unmarshal:") {
		t.Fatalf("stderr = %q, want contains unmarshal", errs)
	}
}

func TestProcessFixRegion(t *testing.T) {
	st := newFakeStore()
	st.data["m:regions"] = mustMeta(t, 7, sampleRegions())
	o := &options{dir: "d1", rid: 4, node: "node-9", localOn: true}
	code, out, _ := runProcess(t, o, st)
	if code != 0 {
		t.Fatalf("code = %d, want 0", code)
	}
	if !strings.Contains(out, "fixed region 4 -> node node-9 local=true in d1") {
		t.Fatalf("stdout = %q, want contains fixed region", out)
	}
	// 写回后重新解析：目标 region 归属已更新，其他 region 保持不变。
	seq, regions, err := sharding.UnmarshalMeta(st.data["m:regions"])
	if err != nil {
		t.Fatalf("re-unmarshal: %v", err)
	}
	if seq != 7 {
		t.Fatalf("seq = %d, want 7", seq)
	}
	if len(regions) != 2 {
		t.Fatalf("regions = %d, want 2", len(regions))
	}
	if regions[1].RegionID != 4 || regions[1].Node != "node-9" || !regions[1].Local {
		t.Fatalf("region 4 after fix = %+v, want node-9/local=true", regions[1])
	}
	if regions[0].Node != "node-1" || !regions[0].Local {
		t.Fatalf("region 1 must be untouched, got %+v", regions[0])
	}
}

func TestProcessFixRegionLocalDefaultFalse(t *testing.T) {
	// -local 缺省时归属改写会将目标 region Local 置为 false。
	st := newFakeStore()
	st.data["m:regions"] = mustMeta(t, 3, sampleRegions())
	o := &options{dir: "d1", rid: 4, node: "node-9"}
	code, out, _ := runProcess(t, o, st)
	if code != 0 {
		t.Fatalf("code = %d, want 0", code)
	}
	if !strings.Contains(out, "local=false") {
		t.Fatalf("stdout = %q, want contains local=false", out)
	}
	_, regions, err := sharding.UnmarshalMeta(st.data["m:regions"])
	if err != nil {
		t.Fatalf("re-unmarshal: %v", err)
	}
	if regions[1].Local {
		t.Fatalf("region 4 Local = true, want false when -local absent")
	}
}

func TestProcessFixRegionNotFound(t *testing.T) {
	st := newFakeStore()
	st.data["m:regions"] = mustMeta(t, 1, sampleRegions())
	o := &options{dir: "d1", rid: 99, node: "node-2"}
	code, out, _ := runProcess(t, o, st)
	if code != 0 {
		t.Fatalf("code = %d, want 0", code)
	}
	if !strings.Contains(out, "region 99 not found in d1") {
		t.Fatalf("stdout = %q, want contains not found", out)
	}
	if st.lastWB != nil {
		t.Fatal("expected no write when region not found")
	}
}

func TestProcessWriteError(t *testing.T) {
	st := newFakeStore()
	st.data["m:regions"] = mustMeta(t, 1, sampleRegions())
	st.writeErr = errors.New("disk full")
	o := &options{dir: "d1", rid: 4, node: "node-2"}
	code, _, errs := runProcess(t, o, st)
	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	if !strings.Contains(errs, "write: disk full") {
		t.Fatalf("stderr = %q, want contains write: disk full", errs)
	}
}

func TestProcessDump(t *testing.T) {
	st := newFakeStore()
	st.data["m:regions"] = mustMeta(t, 7, sampleRegions())
	dumpPath := filepath.Join(t.TempDir(), "routes.txt")
	o := &options{dir: "d1", dump: dumpPath}
	code, out, _ := runProcess(t, o, st)
	if code != 0 {
		t.Fatalf("code = %d, want 0", code)
	}
	if !strings.Contains(out, "dumped 2 regions to "+dumpPath) {
		t.Fatalf("stdout = %q, want contains dumped", out)
	}
	raw, err := os.ReadFile(dumpPath)
	if err != nil {
		t.Fatalf("read dump: %v", err)
	}
	content := string(raw)
	for _, want := range []string{"seq=7 regions=2", "rid=4 table=10 node=\"node-1\" local=true explicit=true"} {
		if !strings.Contains(content, want) {
			t.Fatalf("dump content missing %q:\n%s", want, content)
		}
	}
}

func TestProcessDumpWriteError(t *testing.T) {
	st := newFakeStore()
	st.data["m:regions"] = mustMeta(t, 1, sampleRegions())
	o := &options{dir: "d1", dump: filepath.Join(t.TempDir(), "no-such-dir", "routes.txt")}
	code, _, errs := runProcess(t, o, st)
	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	if !strings.Contains(errs, "write dump:") {
		t.Fatalf("stderr = %q, want contains write dump", errs)
	}
}
