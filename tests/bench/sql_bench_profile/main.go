// sql_bench_profile: 临时阶段计时探针（定位 SQL 单点写入各环节耗时，非基准产物）。
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime/pprof"
	"time"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

const dir = "sql_bench_profile_data"

var metaKey = []byte("m:tables")

type stat struct {
	label string
	total time.Duration
	count int
}

func (s *stat) add(d time.Duration) { s.total += d; s.count++ }
func (s *stat) row() string {
	rate := float64(s.count) * 1000.0 / float64(s.total.Milliseconds())
	avg := s.total.Microseconds() / int64(s.count)
	return fmt.Sprintf("%-36s | %6d ops | %-12s | %10.1f ops/s | %8d us/op", s.label, s.count, s.total.Round(time.Millisecond), rate, avg)
}

func main() {
	_ = os.RemoveAll(dir)
	if err := db.Init(dir); err != nil && !errorsIs(err) {
		fmt.Fprintf(os.Stderr, "init: %v\n", err)
		os.Exit(2)
	}
	d, err := db.Open(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(2)
	}
	defer d.Close()
	e := d.SQL
	if _, err := e.Execute("CREATE TABLE bench (id INT, name TEXT, score INT, PRIMARY KEY (id))"); err != nil {
		fmt.Fprintf(os.Stderr, "create: %v\n", err)
		os.Exit(2)
	}

	N := 3000
	var st []*stat

	// P1 SQL INSERT 全链路（含 pprof CPU 采样）
	s1 := &stat{label: "P1 SQL INSERT 全链路(解析+事务+WAL+存储)"}
	pf, _ := os.Create("cpu.prof")
	_ = pprof.StartCPUProfile(pf)
	t0 := time.Now()
	for i := 0; i < N; i++ {
		ts := time.Now()
		if _, err := e.Execute(fmt.Sprintf("INSERT INTO bench VALUES (%d, 'name-%08d', %d)", i, i, i%1000)); err != nil {
			fmt.Fprintf(os.Stderr, "p1 %d: %v\n", i, err)
			os.Exit(2)
		}
		s1.add(time.Since(ts))
	}
	pprof.StopCPUProfile()
	_ = pf.Close()
	_ = t0
	st = append(st, s1)

	// P2 事务框架+WAL+rocksdb（Begin+Put 1 键+Commit，无 SQL/索引/元数据）
	s2 := &stat{label: "P2 txn Begin+Put+Commit(1键,含WAL fsync)"}
	for i := 0; i < N; i++ {
		ts := time.Now()
		tx, err := d.Txn.Begin()
		if err != nil {
			fmt.Fprintf(os.Stderr, "p2 begin: %v\n", err)
			os.Exit(2)
		}
		if err := tx.Put([]byte(fmt.Sprintf("k%08d", i)), []byte("v")); err != nil {
			fmt.Fprintf(os.Stderr, "p2 put: %v\n", err)
			os.Exit(2)
		}
		if err := tx.Commit(); err != nil {
			fmt.Fprintf(os.Stderr, "p2 commit: %v\n", err)
			os.Exit(2)
		}
		s2.add(time.Since(ts))
	}
	st = append(st, s2)

	// P3 storage.Write 单条（无 openxdb WAL）
	s3 := &stat{label: "P3 storage.Write 单条(rocksdb,cgo)"}
	for i := 0; i < N; i++ {
		ts := time.Now()
		if err := d.Storage.Write(&storage.WriteBatch{Puts: []storage.KVPair{{Key: []byte(fmt.Sprintf("e%08d", i)), Value: []byte("v")}}}); err != nil {
			fmt.Fprintf(os.Stderr, "p3: %v\n", err)
			os.Exit(2)
		}
		s3.add(time.Since(ts))
	}
	st = append(st, s3)

	// P4 storage.Write 批量 1000/批
	s4 := &stat{label: "P4 storage.Write 批量(1000/批)"}
	batch := &storage.WriteBatch{}
	for i := 0; i < N; i++ {
		batch.Puts = append(batch.Puts, storage.KVPair{Key: []byte(fmt.Sprintf("f%08d", i)), Value: []byte("v")})
		if len(batch.Puts) >= 1000 {
			ts := time.Now()
			if err := d.Storage.Write(batch); err != nil {
				fmt.Fprintf(os.Stderr, "p4: %v\n", err)
				os.Exit(2)
			}
			batch = &storage.WriteBatch{}
			s4.add(time.Since(ts))
		}
	}
	st = append(st, s4)

	// P5 txn.Begin+Rollback（纯事务创建/销毁，无写入）
	s5 := &stat{label: "P5 txn Begin+Rollback(空事务)"}
	for i := 0; i < N; i++ {
		ts := time.Now()
		tx, err := d.Txn.Begin()
		if err != nil {
			fmt.Fprintf(os.Stderr, "p5 begin: %v\n", err)
			os.Exit(2)
		}
		if err := tx.Rollback(); err != nil {
			fmt.Fprintf(os.Stderr, "p5 rollback: %v\n", err)
			os.Exit(2)
		}
		s5.add(time.Since(ts))
	}
	st = append(st, s5)

	// P6 loadTables：Get metaKey + Unmarshal
	s6 := &stat{label: "P6 loadTables(Get m:tables + Unmarshal)"}
	for i := 0; i < N; i++ {
		ts := time.Now()
		raw, err := d.Storage.Get(metaKey)
		if err != nil {
			fmt.Fprintf(os.Stderr, "p6: %v\n", err)
			os.Exit(2)
		}
		var tabs []*sql.TableMeta
		_ = json.Unmarshal(raw, &tabs)
		s6.add(time.Since(ts))
	}
	st = append(st, s6)

	fmt.Println("==== sql_bench_profile ====")
	for _, s := range st {
		fmt.Println(s.row())
	}
}

func errorsIs(err error) bool {
	return false
}
