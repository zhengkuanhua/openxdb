package sql_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/zhengkuanhua/openxdb/pkg/db"
	"github.com/zhengkuanhua/openxdb/pkg/sql"
)

// TestDupReproConcurrent 单机引擎复现：8 会话并发显式事务
// UPDATE district + INSERT order_line，统计 duplicate 报错。
func TestDupReproConcurrent(t *testing.T) {
	dir := t.TempDir()
	if err := db.Init(dir); err != nil {
		t.Fatal(err)
	}
	d, err := db.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	e := d.SQL

	mustExec(t, e, "CREATE TABLE district (d_id INT PRIMARY KEY, d_next_o_id INT)")
	mustExec(t, e, "INSERT INTO district VALUES (1, 100)")
	mustExec(t, e, "CREATE TABLE order_line (ol_id INT PRIMARY KEY, d_id INT)")

	var wg sync.WaitGroup
	var mu sync.Mutex
	dup := 0
	ok := 0
	next := 800000
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(tid int) {
			defer wg.Done()
			sess := e.NewSession()
			for j := 0; j < 50; j++ {
				mu.Lock()
				pk := next
				next++
				mu.Unlock()
				_, err1 := e.ExecuteAsSession("BEGIN", sql.SuperUser, sess)
				_, err2 := e.ExecuteAsSession("UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_id = 1", sql.SuperUser, sess)
				_, err3 := e.ExecuteAsSession(fmt.Sprintf("INSERT INTO order_line (ol_id, d_id) VALUES (%d, 1)", pk), sql.SuperUser, sess)
				_, err4 := e.ExecuteAsSession("COMMIT", sql.SuperUser, sess)
				if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
					mu.Lock()
					dup++
					if err3 != nil {
						t.Logf("tid=%d j=%d pk=%d INSERT err=%v", tid, j, pk, err3)
					}
					mu.Unlock()
				} else {
					mu.Lock()
					ok++
					mu.Unlock()
				}
			}
		}(i)
	}
	wg.Wait()
	rows := mustRows(t, e, "SELECT COUNT(*) FROM order_line")
	t.Logf("ok=%d dup=%d order_line_count=%v", ok, dup, rows)
	if dup > 0 {
		t.Fatalf("duplicate errors reproduced: ok=%d dup=%d", ok, dup)
	}
}
