package db_test

// v1.20.0 C4：持续复制 region 化验收用例（V-C1 ~ V-C6）。
//
// 判据来源：C1_M8_gap_report.md §4.4（V-C1 ~ V-C6）+ DEV_PLAN_v1.20.0.md C4 行。
// 实现入口：
//   - pkg/db/ha_region_sync.go        owner 侧 region 增量日志 + 副本侧跟随/位点
//   - pkg/replication/binlog_region.go ReadRegion 的 region 维度过滤
//   - pkg/replication/region_batch.go  RegionOfBatch 从提交批次推导 region
//   - pkg/sql/ha_region_sync.go        ha_replica_region_sync 开关（默认 off）
//
// 本文件只新增用例，不改动任何既有测试与断言；用例全部可在进程内真实跑通
// （多节点组网走既有 openNode / ADD NODE 链路，无网络外部依赖）。

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/replication"
	"github.com/zhengkuanhua/openxdb/pkg/sharding"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// ---------------------------------------------------------------------------
// C4 测试辅助
// ---------------------------------------------------------------------------

// c4RegionKV 取某 region 键空间内的全部物理键值（不含 m: 元数据与位点键）。
func c4RegionKV(t *testing.T, st storage.Storage, rid storage.ID) map[string]string {
	t.Helper()
	rows, err := st.Scan(storage.KeyRange{
		Start: sharding.EncodeRegionKey(rid, nil),
		End:   sharding.EncodeRegionKey(rid+1, nil),
	}, -1)
	if err != nil {
		t.Fatalf("scan region %d: %v", rid, err)
	}
	m := make(map[string]string, len(rows))
	for _, kv := range rows {
		m[string(kv.Key)] = string(kv.Value)
	}
	return m
}

// c4EqualMap 比较两份 region 键值快照。
func c4EqualMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if ov, ok := b[k]; !ok || ov != v {
			return false
		}
	}
	return true
}

// c4Diff 输出两份快照的差异摘要（失败诊断用）。
func c4Diff(a, b map[string]string) string {
	var missed, extra, changed int
	for k, v := range a {
		ov, ok := b[k]
		if !ok {
			missed++
			continue
		}
		if ov != v {
			changed++
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			extra++
		}
	}
	return fmt.Sprintf("want=%d got=%d missing=%d extra=%d valueMismatch=%d", len(a), len(b), missed, extra, changed)
}

// c4WaitKV 轮询直到目标存储的 region 键值与 want 完全一致，返回耗时。
func c4WaitKV(t *testing.T, st storage.Storage, rid storage.ID, want map[string]string, timeout time.Duration) time.Duration {
	t.Helper()
	begin := time.Now()
	deadline := begin.Add(timeout)
	var last map[string]string
	for {
		last = c4RegionKV(t, st, rid)
		if c4EqualMap(last, want) {
			return time.Since(begin)
		}
		if time.Now().After(deadline) {
			t.Fatalf("region %d 未在 %v 内追上: %s", rid, timeout, c4Diff(want, last))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// c4SyncLSN 读取副本位点（未推进按 0 处理）。
func c4SyncLSN(t *testing.T, d *db.DB, rid storage.ID) storage.LSN {
	t.Helper()
	lsn, err := d.RegionSyncLSN(rid)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("read sync lsn of region %d: %v", rid, err)
	}
	return lsn
}

// c4WaitLSN 轮询直到副本位点追至 want。
func c4WaitLSN(t *testing.T, d *db.DB, rid storage.ID, want storage.LSN, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last storage.LSN
	for {
		last = c4SyncLSN(t, d, rid)
		if last >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("region %d 副本位点 %d 未在 %v 内追至 %d", rid, last, timeout, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// c4RegionLastLSN owner 侧该 region 增量日志的末位点（无条目返回 0）。
func c4RegionLastLSN(t *testing.T, d *db.DB, rid storage.ID) storage.LSN {
	t.Helper()
	entries, err := d.ReadRegion(0, rid, 4096)
	if err != nil {
		t.Fatalf("read region log of %d: %v", rid, err)
	}
	var last storage.LSN
	for _, e := range entries {
		if e != nil && e.LSN > last {
			last = e.LSN
		}
	}
	return last
}

// c4OpenStandalone 打开一个不启动集群链路的单机实例（M2 复制/单机零回归用）。
func c4OpenStandalone(t *testing.T) (*db.DB, string) {
	t.Helper()
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatalf("init %s: %v", dir, err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	return d, dir
}

// c4CommitKV 走事务层提交一个原始 KV 写入（触发提交钩子 → binlog）。
func c4CommitKV(t *testing.T, d *db.DB, key, val string) {
	t.Helper()
	tx, err := d.Txn.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Put([]byte(key), []byte(val)); err != nil {
		_ = tx.Rollback()
		t.Fatalf("put %q: %v", key, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit %q: %v", key, err)
	}
}

// c4WaitStorageValue 轮询直到存储中出现 key=want。
func c4WaitStorageValue(t *testing.T, st storage.Storage, key, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	var lastErr error
	for {
		v, err := st.Get([]byte(key))
		lastErr = err
		last = string(v)
		if err == nil && last == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("key %q = %q (err=%v), want %q within %v", key, last, lastErr, want, timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// c4WaitOnlineSlaves 轮询直到主端看到 want 个在线副本。
func c4WaitOnlineSlaves(t *testing.T, d *db.DB, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last int
	for {
		last = d.ReplicationSyncStatus().OnlineSlaves
		if last >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("online slaves = %d, want >= %d within %v", last, want, timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// c4WriteLegacyBinlog 按改造前（v1，无 region 字段）的落盘格式写出 binlog 文件。
//
// v1 文件格式（pkg/replication/binlog.go 头部注释）：
//
//	文件头 magic(10B "OPENXDBBIN") + version(2B)=1
//	每条记录 crc(4B) | bodyLen(4B) | lsn(8B) | commitLSN(8B) | schemaVer(8B) |
//	          state(1B)=2 | nputs(4B) | ndeletes(4B) | body
//	crc 覆盖 len 字段到记录结尾
func c4WriteLegacyBinlog(t *testing.T, path string, entries []*replication.BinlogEntry) {
	t.Helper()
	const (
		ver        = uint16(1)
		stateComm  = uint8(2)
		recordHead = 4 + 4 + 8 + 8 + 8 + 1 + 4 + 4 // = 41B（v1 记录头）
	)
	var out bytes.Buffer
	out.WriteString("OPENXDBBIN")
	if err := binary.Write(&out, binary.BigEndian, ver); err != nil {
		t.Fatalf("write legacy header: %v", err)
	}
	for _, e := range entries {
		if e == nil || e.Batch == nil {
			t.Fatalf("legacy entry must carry a batch")
		}
		var body bytes.Buffer
		for _, p := range e.Batch.Puts {
			_ = binary.Write(&body, binary.BigEndian, uint32(len(p.Key)))
			_ = binary.Write(&body, binary.BigEndian, uint32(len(p.Value)))
			body.Write(p.Key)
			body.Write(p.Value)
		}
		for _, k := range e.Batch.Deletes {
			_ = binary.Write(&body, binary.BigEndian, uint32(len(k)))
			body.Write(k)
		}
		head := make([]byte, recordHead)
		binary.BigEndian.PutUint32(head[4:8], uint32(body.Len()))
		binary.BigEndian.PutUint64(head[8:16], uint64(e.LSN))
		binary.BigEndian.PutUint64(head[16:24], uint64(e.CommitLSN))
		binary.BigEndian.PutUint64(head[24:32], e.SchemaVer)
		head[32] = stateComm
		binary.BigEndian.PutUint32(head[33:37], uint32(len(e.Batch.Puts)))
		binary.BigEndian.PutUint32(head[37:41], uint32(len(e.Batch.Deletes)))
		payload := append(append([]byte{}, head[4:]...), body.Bytes()...)
		binary.BigEndian.PutUint32(head[0:4], crc32.ChecksumIEEE(payload))
		out.Write(head)
		out.Write(body.Bytes())
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, out.Bytes(), 0o644); err != nil {
		t.Fatalf("write legacy binlog %s: %v", path, err)
	}
}

// c4PrepareShadow 静态 bootstrap：开启副本冗余把 owner 存量推给影子副本，
// 随后立即关闭维护循环，使后续数据变化只能由 region 增量链路产生。
func c4PrepareShadow(t *testing.T, owner *db.DB, wantEnsured uint64) {
	t.Helper()
	owner.SetReplicaMaintainInterval(200 * time.Millisecond)
	owner.SetReplicaEnabled(true)
	waitB1Ensured(t, owner, wantEnsured)
	owner.SetReplicaEnabled(false)
	if owner.ReplicaMaintaining() {
		t.Fatal("副本维护循环应在 bootstrap 后关闭")
	}
}

// ---------------------------------------------------------------------------
// V-C1 单机零回归
// ---------------------------------------------------------------------------

// TestV1200C4SingleNodeZeroRegression V-C1：mgr==nil 且开关未开启时不产生
// binlog 文件、不启动跟随协程、不写位点键，业务读写完全不受影响。
func TestV1200C4SingleNodeZeroRegression(t *testing.T) {
	d, dir := c4OpenStandalone(t)
	t.Cleanup(func() { _ = d.Close() })

	if d.ClusterSrv != nil {
		t.Fatal("单机实例不应启动集群链路（未 StartCluster 时 ClusterSrv 必须为 nil）")
	}
	if d.Replicator != nil {
		t.Fatalf("未开启复制时 Replicator 必须为 nil")
	}
	if d.RegionSyncEnabled() {
		t.Fatal("ha_replica_region_sync 默认必须 off（零回归）")
	}
	if d.RegionSyncActive() {
		t.Fatal("开关未开启时不得存在 region 跟随协程")
	}

	// 业务读写走完整提交链路。
	mustSQL(t, d, "CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))")
	for i := 1; i <= 3; i++ {
		mustSQL(t, d, fmt.Sprintf("INSERT INTO t (id, name) VALUES (%d, 'n%d')", i, i))
	}
	mustSQL(t, d, "UPDATE t SET name = 'n2-updated' WHERE id = 2")
	wantIDs(t, d, "SELECT id FROM t ORDER BY id", 1, 2, 3)

	// 无 binlog 文件（region 轨与 M2 轨都不得落盘）。
	for _, name := range []string{db.RegionBinlogFile, "binlog.log"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("开关未开启时不应创建 %s（err=%v）", name, err)
		}
	}
	// 无位点键、无 region 相关元数据。
	rows, err := d.Storage.Scan(storage.KeyRange{Start: []byte("m:repl:"), End: []byte("m:repl;")}, -1)
	if err != nil {
		t.Fatalf("scan m:repl prefix: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("开关未开启时不应写 m:repl:* 元数据，实际 %d 条", len(rows))
	}
	// 无日志源：单机无集群 ID，日志源注册表不应存在任何可用来源。
	if _, err := d.ReadRegion(0, 0, 16); err == nil {
		t.Fatal("开关未开启时不应存在可读的 region 增量日志")
	}
}

// ---------------------------------------------------------------------------
// V-C2 影子副本 region 增量跟随
// ---------------------------------------------------------------------------

// TestV1200C4ShadowRegionIncrementFollow V-C2：owner 侧连续写中间 region 后，
// 影子副本侧 Storage.Scan 可见对应键、m:repl:region:<rid>:lsn 单调推进，
// 且跟随时延低于判据阈值 500ms。
func TestV1200C4ShadowRegionIncrementFollow(t *testing.T) {
	dA, addrA := openNode(t, "node-a")
	t.Cleanup(func() { _ = dA.Close() })
	dB, addrB := openNode(t, "node-b")
	t.Cleanup(func() { _ = dB.Close() })

	aID, bID := dA.Cluster.SelfID, dB.Cluster.SelfID
	mustSQL(t, dA, "ADD NODE '"+addrB+"'")
	mustSQL(t, dB, "ADD NODE '"+addrA+"'")
	speedupHeartbeat(t, dA)
	speedupHeartbeat(t, dB)
	waitEverUp(t, dA, bID)
	waitEverUp(t, dB, aID)

	regions := seedThreeRegions(t, dA)
	rid := storage.ID(regions[1].RegionID)

	// 静态 bootstrap：把 A 的存量推给 B，随后关闭维护循环，
	// 保证后续数据变化只能来自 region 增量链路（改造前的静态同步不再介入）。
	c4PrepareShadow(t, dA, uint64(len(regions)))
	waitRoute(t, dA, rid, aID, true)
	waitRoute(t, dB, rid, aID, false)

	base := c4RegionKV(t, dB.Storage, rid)
	if len(base) != 4 {
		t.Fatalf("影子副本 region %d 基线键数 = %d, want 4 (id 4..7)", rid, len(base))
	}
	if pos := c4SyncLSN(t, dB, rid); pos != 0 {
		t.Fatalf("未开启 region 增量复制前副本位点应为 0, got %d", pos)
	}

	// 开启持续复制：owner 侧落 region 增量日志，副本侧起跟随循环。
	if err := dA.SetRegionSyncEnabled(true); err != nil {
		t.Fatalf("enable region sync on owner: %v", err)
	}
	if err := dB.SetRegionSyncEnabled(true); err != nil {
		t.Fatalf("enable region sync on replica: %v", err)
	}
	if !dA.RegionSyncActive() || !dB.RegionSyncActive() {
		t.Fatal("开关开启后 owner / 副本都应有 region 跟随协程在跑")
	}

	// owner 连续写 5 条（全部落在中间 region）。
	for i := 0; i < 5; i++ {
		mustSQL(t, dA, fmt.Sprintf("UPDATE users SET age = %d WHERE id = %d", 100+i, 4+i%4))
	}
	want := c4RegionKV(t, dA.Storage, rid)
	if c4EqualMap(base, want) {
		t.Fatal("owner 写入后 region 键值未变化，用例无区分度")
	}

	elapsed := c4WaitKV(t, dB.Storage, rid, want, 10*time.Second)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("副本跟随耗时 %v 超过判据 500ms", elapsed)
	}

	pos := c4SyncLSN(t, dB, rid)
	if pos == 0 {
		t.Fatal("副本位点 m:repl:region:<rid>:lsn 未推进")
	}
	ownerLast := c4RegionLastLSN(t, dA, rid)
	if pos > ownerLast {
		t.Fatalf("副本位点 %d 越过 owner 日志末位 %d", pos, ownerLast)
	}

	// 再写一轮：位点必须继续单调前进（而非只推进一次）。
	mustSQL(t, dA, "UPDATE users SET name = 'follow-round-2' WHERE id = 4")
	c4WaitKV(t, dB.Storage, rid, c4RegionKV(t, dA.Storage, rid), 10*time.Second)
	c4WaitLSN(t, dB, rid, c4RegionLastLSN(t, dA, rid), 10*time.Second)
	if next := c4SyncLSN(t, dB, rid); next <= pos {
		t.Fatalf("位点未单调推进: %d -> %d", pos, next)
	}
	if err := dB.RegionSyncError(rid); err != nil {
		t.Fatalf("副本侧最近一轮同步报错: %v", err)
	}
}

// ---------------------------------------------------------------------------
// V-C3 断连续传
// ---------------------------------------------------------------------------

// TestV1200C4RegionSyncReconnectCatchUp V-C3：中途断开副本对 owner 增量日志
// 的访问，owner 续写后副本不前进；恢复访问后副本自动追上，位点精确归零差距、
// 数据不重复写（幂等），且不存在断档。
func TestV1200C4RegionSyncReconnectCatchUp(t *testing.T) {
	dA, addrA := openNode(t, "node-a")
	t.Cleanup(func() { _ = dA.Close() })
	dB, addrB := openNode(t, "node-b")
	t.Cleanup(func() { _ = dB.Close() })

	aID, bID := dA.Cluster.SelfID, dB.Cluster.SelfID
	mustSQL(t, dA, "ADD NODE '"+addrB+"'")
	mustSQL(t, dB, "ADD NODE '"+addrA+"'")
	speedupHeartbeat(t, dA)
	speedupHeartbeat(t, dB)
	waitEverUp(t, dA, bID)
	waitEverUp(t, dB, aID)

	regions := seedThreeRegions(t, dA)
	rid := storage.ID(regions[1].RegionID)
	c4PrepareShadow(t, dA, uint64(len(regions)))
	waitRoute(t, dB, rid, aID, false)

	if err := dA.SetRegionSyncEnabled(true); err != nil {
		t.Fatalf("enable region sync on owner: %v", err)
	}
	if err := dB.SetRegionSyncEnabled(true); err != nil {
		t.Fatalf("enable region sync on replica: %v", err)
	}
	// 先追平到基线。
	mustSQL(t, dA, "UPDATE users SET age = 60 WHERE id = 5")
	c4WaitKV(t, dB.Storage, rid, c4RegionKV(t, dA.Storage, rid), 10*time.Second)

	src, ok := db.LookupRegionLogSource(aID)
	if !ok {
		t.Fatal("owner A 未注册 region 增量日志源")
	}
	beforePos := c4SyncLSN(t, dB, rid)
	beforeKV := c4RegionKV(t, dB.Storage, rid)

	// ---- 断开：副本侧失去 owner 日志源 ----
	db.UnregisterRegionLogSource(aID)
	for i := 0; i < 6; i++ {
		mustSQL(t, dA, fmt.Sprintf("UPDATE users SET age = %d WHERE id = %d", 300+i, 4+i%4))
	}
	time.Sleep(600 * time.Millisecond) // 静置 3 个拉取周期
	if got := c4SyncLSN(t, dB, rid); got != beforePos {
		t.Fatalf("断连期间副本位点不应推进: %d -> %d", beforePos, got)
	}
	if got := c4RegionKV(t, dB.Storage, rid); !c4EqualMap(beforeKV, got) {
		t.Fatalf("断连期间副本数据不应变化: %s", c4Diff(beforeKV, got))
	}

	// ---- 恢复：重新注册日志源，副本自动追平 ----
	db.SetRegionLogSource(aID, src)
	c4WaitKV(t, dB.Storage, rid, c4RegionKV(t, dA.Storage, rid), 15*time.Second)
	ownerLast := c4RegionLastLSN(t, dA, rid)
	c4WaitLSN(t, dB, rid, ownerLast, 15*time.Second)
	if got := c4SyncLSN(t, dB, rid); got != ownerLast {
		t.Fatalf("追平后副本位点 = %d, want owner 末位 %d", got, ownerLast)
	}

	// ---- 幂等：静置若干周期，数据与位点都不再变化 ----
	settledKV := c4RegionKV(t, dB.Storage, rid)
	settledPos := c4SyncLSN(t, dB, rid)
	time.Sleep(600 * time.Millisecond)
	if got := c4SyncLSN(t, dB, rid); got != settledPos {
		t.Fatalf("静置后位点漂移: %d -> %d（重复投递未被位点拦截）", settledPos, got)
	}
	if got := c4RegionKV(t, dB.Storage, rid); !c4EqualMap(settledKV, got) {
		t.Fatalf("静置后数据漂移（重复写）: %s", c4Diff(settledKV, got))
	}

	// ---- 无断档：从副本位点之后续读 owner 日志必须成功且为空 ----
	entries, err := dA.ReadRegion(settledPos+1, rid, 128)
	if err != nil {
		t.Fatalf("从副本位点续读 owner 日志失败: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("owner 末位之后仍有 %d 条增量，副本存在断档", len(entries))
	}
}

// ---------------------------------------------------------------------------
// V-C4 兼容旧 binlog（RegionID=0）
// ---------------------------------------------------------------------------

// TestV1200C4LegacyBinlogCompat V-C4：改造前落盘的 v1 binlog（无 region 字段，
// 读回 RegionID=0）被新副本按整批应用，数据与 owner 一致；旧文件可继续追加新
// 条目并被同一链路跟随；owner 的元数据键不会被覆盖到副本自身元数据上。
func TestV1200C4LegacyBinlogCompat(t *testing.T) {
	dA, addrA, dirA := openNodeWithDir(t, "node-a")
	t.Cleanup(func() { _ = dA.Close() })
	dB, addrB := openNode(t, "node-b")
	t.Cleanup(func() { _ = dB.Close() })

	aID, bID := dA.Cluster.SelfID, dB.Cluster.SelfID
	mustSQL(t, dA, "ADD NODE '"+addrB+"'")
	mustSQL(t, dB, "ADD NODE '"+addrA+"'")
	speedupHeartbeat(t, dA)
	speedupHeartbeat(t, dB)
	waitEverUp(t, dA, bID)
	waitEverUp(t, dB, aID)

	regions := seedThreeRegions(t, dA)
	rid := storage.ID(regions[1].RegionID)
	c4PrepareShadow(t, dA, uint64(len(regions)))
	waitRoute(t, dB, rid, aID, false)

	// 构造两条"旧格式、RegionID=0"的历史条目：物理键落在中间 region 内，
	// 值使用与表结构无关的历史载荷（旧链路不保证行 JSON 形态）。
	k1 := sharding.EncodeRegionKey(rid, []byte("legacy:1"))
	k2 := sharding.EncodeRegionKey(rid, []byte("legacy:2"))
	if got, inner := sharding.DecodeRegionKey(k1); got != rid || len(inner) == 0 {
		t.Fatalf("构造的物理键 region 不匹配: %d inner=%q", got, string(inner))
	}

	legacy := []*replication.BinlogEntry{
		{
			LSN:       1,
			CommitLSN: 1,
			Batch: &storage.WriteBatch{Puts: []storage.KVPair{
				{Key: k1, Value: []byte("legacy-1")},
			}},
		},
		{
			LSN:       2,
			CommitLSN: 2,
			Batch: &storage.WriteBatch{Puts: []storage.KVPair{
				{Key: k2, Value: []byte("legacy-2")},
				{Key: []byte("m:legacy:probe"), Value: []byte("1")},
			}},
		},
	}
	// owner 侧同步施加这两条历史写入，使 A/B 具备可比的一致性基线。
	if err := dA.Storage.Write(&storage.WriteBatch{Puts: []storage.KVPair{
		{Key: k1, Value: []byte("legacy-1")},
		{Key: k2, Value: []byte("legacy-2")},
		{Key: []byte("m:legacy:probe"), Value: []byte("1")},
	}}); err != nil {
		t.Fatalf("apply legacy writes on owner: %v", err)
	}

	legacyPath := filepath.Join(dirA, db.RegionBinlogFile)
	c4WriteLegacyBinlog(t, legacyPath, legacy)
	// 用生产解码器校验构造出的文件确实是合法的 v1 文件。
	if last, err := replication.PeekLastLSN(legacyPath); err != nil || last != 2 {
		t.Fatalf("legacy v1 binlog 非法: last=%d err=%v", last, err)
	}

	if err := dA.SetRegionSyncEnabled(true); err != nil {
		t.Fatalf("enable region sync on owner: %v", err)
	}
	if err := dB.SetRegionSyncEnabled(true); err != nil {
		t.Fatalf("enable region sync on replica: %v", err)
	}
	if got, err := dA.RegionSyncLSN(rid); err != nil || got != 0 {
		t.Fatalf("owner 自身位点应保持 0（owner 不跟随时自身日志）, got=%d err=%v", got, err)
	}

	// 旧条目按整批应用：键已被 region 前缀标识，副本侧等价于按 region 裁剪。
	want := c4RegionKV(t, dA.Storage, rid)
	c4WaitKV(t, dB.Storage, rid, want, 15*time.Second)

	got := c4RegionKV(t, dB.Storage, rid)
	if got[string(k1)] != "legacy-1" || got[string(k2)] != "legacy-2" {
		t.Fatalf("旧条目未按整批应用: legacy:1=%q legacy:2=%q", got[string(k1)], got[string(k2)])
	}
	if pos := c4SyncLSN(t, dB, rid); pos != 2 {
		t.Fatalf("应用旧条目后副本位点 = %d, want 2", pos)
	}
	// owner 的元数据键不得覆盖到副本自身元数据。
	if v, err := dB.Storage.Get([]byte("m:legacy:probe")); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("副本不应应用 owner 的元数据键: value=%q err=%v", string(v), err)
	}
	if v, err := dA.Storage.Get([]byte("m:legacy:probe")); err != nil || string(v) != "1" {
		t.Fatalf("owner 侧元数据键前置条件不成立: value=%q err=%v", string(v), err)
	}

	// 旧文件继续追加：新提交写入同一 v1 文件，仍按整批应用并被副本跟随。
	mustSQL(t, dA, "UPDATE users SET name = 'legacy-tail' WHERE id = 4")
	c4WaitKV(t, dB.Storage, rid, c4RegionKV(t, dA.Storage, rid), 15*time.Second)
	tailLast := c4RegionLastLSN(t, dA, rid)
	if tailLast <= 2 {
		t.Fatalf("旧文件未继续追加条目: owner 末位 %d", tailLast)
	}
	if pos := c4SyncLSN(t, dB, rid); pos != tailLast {
		t.Fatalf("追加条目后副本位点 = %d, want %d", pos, tailLast)
	}
}

// ---------------------------------------------------------------------------
// V-C5 2PC 写进复制
// ---------------------------------------------------------------------------

// TestV1200C4TwoPCWriteEntersRegionRepl V-C5：跨 region 写（owner 在对方节点）
// 经 2PC 在参与者侧提交后进入 region 增量日志，第三方副本同时跟随，
// 三方数据一致且复制 lag 归零。
func TestV1200C4TwoPCWriteEntersRegionRepl(t *testing.T) {
	dA, addrA := openNode(t, "node-a")
	t.Cleanup(func() { _ = dA.Close() })
	dB, addrB := openNode(t, "node-b")
	t.Cleanup(func() { _ = dB.Close() })
	dC, addrC := openNode(t, "node-c")
	t.Cleanup(func() { _ = dC.Close() })

	selfA, selfB, selfC := dA.Cluster.SelfID, dB.Cluster.SelfID, dC.Cluster.SelfID
	for _, d := range []*db.DB{dA, dB, dC} {
		mustSQL(t, d, "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))")
	}
	for i := 1; i <= 5; i++ {
		mustSQL(t, dA, fmt.Sprintf("INSERT INTO users (id, name, age) VALUES (%d, 'u%d', %d)", i, i, 20+i))
	}
	if err := dA.SQL.SplitTable("users", [][]byte{splitBoundary(4), splitBoundary(8)}); err != nil {
		t.Fatalf("SplitTable: %v", err)
	}
	regions, err := dA.SQL.ListRegions("users")
	if err != nil || len(regions) != 3 {
		t.Fatalf("ListRegions: want 3, got %v err=%v", regions, err)
	}
	rid := storage.ID(regions[1].RegionID)

	for _, e := range []struct {
		d             *db.DB
		other, other2 string
	}{
		{dA, addrB, addrC},
		{dB, addrA, addrC},
		{dC, addrA, addrB},
	} {
		mustSQL(t, e.d, "ADD NODE '"+e.other+"'")
		mustSQL(t, e.d, "ADD NODE '"+e.other2+"'")
		speedupHeartbeat(t, e.d)
	}
	waitEverUp(t, dA, selfB)
	waitEverUp(t, dA, selfC)
	waitEverUp(t, dC, selfA)

	// 中间 region 归属 B：后续对 r2 的写由 A 发起、owner 在 B（跨 region 2PC）。
	mustSQL(t, dA, fmt.Sprintf("ASSIGN REGION %d TO NODE '%s'", regions[1].RegionID, selfB))
	waitRoute(t, dA, rid, selfB, false)
	waitRoute(t, dB, rid, selfB, true)

	// 让 C 也持有该 region 的存量副本：走改造前的静态 bootstrap（B1 推送），
	// 推完即关闭维护循环，后续数据变化只能来自 C4 的 region 增量链路。
	dB.SetReplicaMaintainInterval(200 * time.Millisecond)
	dB.SetReplicaEnabled(true)
	baseB := c4RegionKV(t, dB.Storage, rid)
	if len(baseB) != 2 {
		t.Fatalf("owner B 的 region %d 基线键数 = %d, want 2 (id 4,5)", rid, len(baseB))
	}
	c4WaitKV(t, dC.Storage, rid, baseB, 20*time.Second)
	dB.SetReplicaEnabled(false)
	if dB.ReplicaMaintaining() {
		t.Fatal("副本维护循环应在 bootstrap 后关闭")
	}

	baseC := c4RegionKV(t, dC.Storage, rid)
	if !c4EqualMap(baseB, baseC) {
		t.Fatalf("副本 C 存量与 owner B 不一致: %s", c4Diff(baseB, baseC))
	}

	// 开关：owner B 落 region 增量日志，副本 C 跟随（A 只做协调者）。
	if err := dB.SetRegionSyncEnabled(true); err != nil {
		t.Fatalf("enable region sync on owner B: %v", err)
	}
	if err := dC.SetRegionSyncEnabled(true); err != nil {
		t.Fatalf("enable region sync on replica C: %v", err)
	}

	// 跨 region 2PC 写：协调者 A、owner/参与者 B，id=6 落在 r2。
	res := mustSQL(t, dA, "INSERT INTO users (id, name, age) VALUES (6, 'u6', 26)")
	if res.AffectedRows != 1 {
		t.Fatalf("跨 region INSERT: affected=%d", res.AffectedRows)
	}

	// 参与者侧提交必须进入 region 增量日志，且条目带 region 维度。
	entries, err := dB.ReadRegion(0, rid, 128)
	if err != nil {
		t.Fatalf("read owner B region log: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("2PC 参与者提交未进入 region 增量日志")
	}
	for _, e := range entries {
		if e.RegionID != 0 && e.RegionID != rid {
			t.Fatalf("owner B 日志条目 region=%d 不属于 r%d", e.RegionID, rid)
		}
	}

	// 第三方副本 C 跟随，且 lag 归零。
	want := c4RegionKV(t, dB.Storage, rid)
	c4WaitKV(t, dC.Storage, rid, want, 15*time.Second)
	ownerLast := c4RegionLastLSN(t, dB, rid)
	c4WaitLSN(t, dC, rid, ownerLast, 15*time.Second)
	pos := c4SyncLSN(t, dC, rid)
	if pos != ownerLast {
		t.Fatalf("副本 C 位点 %d 未归零 lag（owner 末位 %d）", pos, ownerLast)
	}
	if err := dC.RegionSyncError(rid); err != nil {
		t.Fatalf("副本 C 最近一轮同步报错: %v", err)
	}

	// 三方数据一致（含 2PC 写入的新行）。
	wantIDs(t, dA, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6)
	wantIDs(t, dB, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6)
	wantIDs(t, dC, "SELECT id FROM users ORDER BY id", 1, 2, 3, 4, 5, 6)
	if v, err := dC.Storage.Get(mustRowKey(t, want, "u6")); err != nil {
		t.Fatalf("副本 C 未持有 2PC 写入的行: %v", err)
	} else if !bytes.Contains(v, []byte("u6")) {
		t.Fatalf("副本 C 行内容异常: %q", string(v))
	}
}

// mustRowKey 从 region 键值快照中找出含指定值的物理键（断言定位用）。
func mustRowKey(t *testing.T, kv map[string]string, needle string) []byte {
	t.Helper()
	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if bytes.Contains([]byte(kv[k]), []byte(needle)) {
			return []byte(k)
		}
	}
	for _, k := range keys {
		if bytes.Contains([]byte(k), []byte(needle)) {
			return []byte(k)
		}
	}
	t.Fatalf("region 快照中找不到包含 %q 的行", needle)
	return nil
}

// ---------------------------------------------------------------------------
// V-C6 半同步降级语义不回退
// ---------------------------------------------------------------------------

// TestV1200C4SemiSyncDegradeUnchanged V-C6：C4 开关与 M2 半同步共存时，
// 副本全断开后提交降级为异步（不阻塞、不报错），SyncStatus.Degraded 置位；
// 有副本在线时不降级。
func TestV1200C4SemiSyncDegradeUnchanged(t *testing.T) {
	dirMain, dirSlave := c4MkdirTemp(t, "c4-vc6-main"), c4MkdirTemp(t, "c4-vc6-slave")
	// 先注册目录删除（LIFO 最后执行），避免 t.TempDir 清理失败导致用例假失败。
	t.Cleanup(func() {
		c4RemoveAllRetry(t, dirMain)
		c4RemoveAllRetry(t, dirSlave)
	})
	main := c4OpenInDir(t, dirMain)
	slaveClosed := false
	t.Cleanup(func() {
		if !slaveClosed {
			_ = main.Close()
		}
		// 主端流式复制协程在副本异常断开后仍需收敛，给其时间释放 binlog 句柄，
		// 否则 Windows 上 t.TempDir 清理会因文件占用失败。
		time.Sleep(300 * time.Millisecond)
	})

	if err := main.StartReplication("127.0.0.1:0"); err != nil {
		t.Fatalf("StartReplication: %v", err)
	}
	addr, _ := main.ReplicationStatus()
	if addr == "" {
		t.Fatal("主端未返回监听地址")
	}
	slave := c4OpenInDir(t, dirSlave)
	if err := slave.StartFollower(addr); err != nil {
		t.Fatalf("StartFollower: %v", err)
	}
	t.Cleanup(func() {
		if !slaveClosed {
			_ = slave.Close()
		}
	})

	if err := main.SetReplicationMode(replication.ModeSemiSync, 1, 300*time.Millisecond); err != nil {
		t.Fatalf("SetReplicationMode: %v", err)
	}
	// C4 开关共存：owner 侧复用 M2 binlog，不额外新建 region.binlog。
	if err := main.SetRegionSyncEnabled(true); err != nil {
		t.Fatalf("enable region sync on owner: %v", err)
	}
	if !main.RegionSyncEnabled() {
		t.Fatal("region 增量复制开关未生效")
	}
	if _, err := os.Stat(filepath.Join(dirMain, db.RegionBinlogFile)); !os.IsNotExist(err) {
		t.Fatalf("已有 M2 主端时不应新建 %s（err=%v）", db.RegionBinlogFile, err)
	}
	c4WaitOnlineSlaves(t, main, 1, 10*time.Second)

	// 有副本在线：半同步确认，不降级。
	c4CommitKV(t, main, "c4/semisync/1", "v1")
	st := main.ReplicationSyncStatus()
	if st.Mode != replication.ModeSemiSync || st.SemiAckN != 1 {
		t.Fatalf("复制模式未生效: mode=%v ack=%d", st.Mode, st.SemiAckN)
	}
	if st.Degraded {
		t.Fatalf("有副本在线时不应降级: degradedCount=%d", st.DegradedCount)
	}
	if st.OnlineSlaves < 1 {
		t.Fatalf("在线副本数 = %d, want >= 1", st.OnlineSlaves)
	}
	c4WaitStorageValue(t, slave.Storage, "c4/semisync/1", "v1", 10*time.Second)

	// region 日志复用 M2 binlog：整批条目 RegionID=0（改造前语义），可被读为"整批"。
	entries, err := main.ReadRegion(0, 0, 64)
	if err != nil {
		t.Fatalf("读复用 binlog 的 region 视图: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("提交未进入 binlog")
	}
	for _, e := range entries {
		if e.RegionID != 0 {
			t.Fatalf("离线元数据写入应为整批条目(RegionID=0), got %d", e.RegionID)
		}
	}

	// 副本全断开：降级为异步，提交仍成功且 Degraded=true。
	if err := slave.Close(); err != nil {
		t.Fatalf("close slave: %v", err)
	}
	slaveClosed = true
	c4CommitKV(t, main, "c4/semisync/2", "v2")
	st = main.ReplicationSyncStatus()
	if !st.Degraded || st.DegradedCount < 1 {
		t.Fatalf("副本全断开后应降级: degraded=%v count=%d", st.Degraded, st.DegradedCount)
	}
	if st.OnlineSlaves != 0 {
		t.Fatalf("在线副本数 = %d, want 0", st.OnlineSlaves)
	}
	// 降级语义：不阻塞、不报错，后续提交继续成功且本地数据可见。
	c4CommitKV(t, main, "c4/semisync/3", "v3")
	for _, tc := range []struct{ key, want string }{
		{"c4/semisync/2", "v2"},
		{"c4/semisync/3", "v3"},
	} {
		if v, err := main.Storage.Get([]byte(tc.key)); err != nil || string(v) != tc.want {
			t.Fatalf("降级后写入 %q = %q (err=%v), want %q", tc.key, string(v), err, tc.want)
		}
	}
}

// c4MkdirTemp 建立用例私有目录（不用 t.TempDir，避免 Windows 上复制协程尚未释放 binlog 句柄时帧框清理失败）。
func c4MkdirTemp(t *testing.T, tag string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", tag+"-")
	if err != nil {
		t.Fatalf("mkdir temp %s: %v", tag, err)
	}
	return dir
}

// c4OpenInDir 在指定目录初始化并打开单机 DB。
func c4OpenInDir(t *testing.T, dir string) *db.DB {
	t.Helper()
	if err := db.Init(dir); err != nil {
		t.Fatalf("init %s: %v", dir, err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	return d
}

// c4RemoveAllRetry 轮询重试删除目录：Windows 文件句柄释放有延迟。
func c4RemoveAllRetry(t *testing.T, dir string) {
	t.Helper()
	for i := 0; i < 15; i++ {
		if err := os.RemoveAll(dir); err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("临时目录 %s 未完全清理（Windows 文件句柄延迟释放，不影响用例结论）", dir)
}
