// 临时离线工具：读取/修复 OpenXDB 节点 RocksDB 中 m:regions 路由快照。
// 用法:
//   fixregions -dir <dataDir> -dump <out.txt>            # 导出路由视图
//   fixregions -dir <dataDir> -region 4 -node node-xxx [-local]   # 修正 region 归属
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
	"github.com/zhengkuanhua/openxdb/pkg/storage/rocksdb"
)

// kvStore 抽象 m:regions 的读取与写入，便于单元测试注入内存实现。
type kvStore interface {
	Get(key []byte) ([]byte, error)
	Write(wb *storage.WriteBatch) error
}

// options 保存解析后的命令行参数。
type options struct {
	dir     string
	rid     int
	node    string
	localOn bool
	dump    string
}

func main() {
	fs := flag.NewFlagSet("fixregions", flag.ContinueOnError)
	os.Exit(run(fs, os.Args[1:], os.Stdout, os.Stderr))
}

// parseArgs 解析命令行参数为 options。
func parseArgs(fs *flag.FlagSet, args []string) (*options, error) {
	dir := fs.String("dir", "", "RocksDB data dir (xxx/data)")
	rid := fs.Int("region", 0, "region id to fix")
	node := fs.String("node", "", "target node id")
	localOn := fs.Bool("local", false, "mark region Local=true (data physically here)")
	dump := fs.String("dump", "", "dump m:regions to this file and exit")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return &options{dir: *dir, rid: *rid, node: *node, localOn: *localOn, dump: *dump}, nil
}

// run 解析参数、打开 RocksDB 并执行主流程，返回进程退出码。
func run(fs *flag.FlagSet, args []string, stdout, stderr io.Writer) int {
	o, err := parseArgs(fs, args)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if o.dir == "" {
		fmt.Fprintln(stderr, "missing -dir")
		return 2
	}
	st, err := rocksdb.Open(o.dir, false)
	if err != nil {
		fmt.Fprintf(stderr, "open %s: %v\n", o.dir, err)
		return 1
	}
	defer st.Close()
	return process(o, st, stdout, stderr)
}

// process 基于已打开存储执行 dump 或 fix 核心流程，返回退出码。
func process(o *options, st kvStore, stdout, stderr io.Writer) int {
	raw, err := st.Get([]byte("m:regions"))
	if err == storage.ErrNotFound {
		fmt.Fprintln(stderr, "m:regions not found")
		return 0
	}
	if err != nil {
		fmt.Fprintf(stderr, "get m:regions: %v\n", err)
		return 1
	}
	seq, regions, err := sharding.UnmarshalMeta(raw)
	if err != nil {
		fmt.Fprintf(stderr, "unmarshal: %v\n", err)
		return 1
	}

	if o.dump != "" {
		return dumpRegions(o, seq, regions, stdout, stderr)
	}
	if o.node == "" {
		fmt.Fprintln(stderr, "missing -node")
		return 2
	}
	return fixRegion(o, seq, regions, st, stdout, stderr)
}

// dumpRegions 将路由视图导出到文件并返回退出码。
func dumpRegions(o *options, seq uint64, regions []sharding.RegionInfo, stdout, stderr io.Writer) int {
	var out string
	out += fmt.Sprintf("seq=%d regions=%d\n", seq, len(regions))
	for _, r := range regions {
		out += fmt.Sprintf("rid=%d table=%d node=%q local=%v explicit=%v start=%x end=%x\n",
			r.RegionID, r.TableID, r.Node, r.Local, r.Explicit, r.StartKey, r.EndKey)
	}
	if err := os.WriteFile(o.dump, []byte(out), 0o644); err != nil {
		fmt.Fprintf(stderr, "write dump: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "dumped %d regions to %s\n", len(regions), o.dump)
	return 0
}

// fixRegion 改写 region 归属并写回，返回退出码。
func fixRegion(o *options, seq uint64, regions []sharding.RegionInfo, st kvStore, stdout, stderr io.Writer) int {
	changed := 0
	for i := range regions {
		if int(regions[i].RegionID) == o.rid {
			regions[i].Node = o.node
			regions[i].Local = o.localOn
			changed++
		}
	}
	if changed == 0 {
		fmt.Fprintf(stdout, "region %d not found in %s\n", o.rid, o.dir)
		return 0
	}
	nraw, err := sharding.MarshalMeta(seq, regions)
	if err != nil {
		fmt.Fprintf(stderr, "marshal: %v\n", err)
		return 1
	}
	if err := st.Write(&storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte("m:regions"), Value: nraw}}}); err != nil {
		fmt.Fprintf(stderr, "write: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "fixed region %d -> node %s local=%v in %s\n", o.rid, o.node, o.localOn, o.dir)
	return 0
}
