package sql

import (
	"encoding/json"
	"strings"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// ViewMeta 视图元数据：名称 + 定义（SELECT AST）。
// 定义以 SelectStmt JSON 持久化，查询展开时反序列化为独立副本执行，
// 避免多个查询共享同一 AST 造成状态污染。
type ViewMeta struct {
	Name   string      `json:"name"`
	Select *SelectStmt `json:"select"`
}

// viewsKey 视图目录键。
var viewsKey = []byte("m:views")

// loadViews 读取视图目录。
func loadViews(get func(key []byte) ([]byte, error)) ([]*ViewMeta, error) {
	raw, err := get(viewsKey)
	if err != nil {
		if err == storage.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	if raw == nil || len(raw) == 0 {
		return nil, nil
	}
	var views []*ViewMeta
	if err := json.Unmarshal(raw, &views); err != nil {
		return nil, &SQLError{Msg: "corrupted view catalog"}
	}
	return views, nil
}

// saveViews 序列化视图目录。
func saveViews(views []*ViewMeta) ([]byte, error) {
	return json.Marshal(views)
}

// findView 按名查找视图。
func findView(views []*ViewMeta, name string) *ViewMeta {
	for _, v := range views {
		if v.Name == name {
			return v
		}
	}
	return nil
}

// cloneSelect 深拷贝 SELECT AST（JSON 往返），供视图定义展开使用。
func cloneSelect(s *SelectStmt) (*SelectStmt, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	var out SelectStmt
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// viewRefNames 收集 SELECT 定义中引用的全部表/视图名（FROM + JOIN + 嵌套子查询）。
func viewRefNames(s *SelectStmt) []string {
	var names []string
	seen := map[string]bool{}
	add := func(n string) {
		if n != "" && !seen[n] {
			seen[n] = true
			names = append(names, n)
		}
	}
	if s == nil {
		return names
	}
	if s.From != "" {
		add(s.From)
	}
	if s.Subquery != nil {
		for _, n := range viewRefNames(s.Subquery) {
			add(n)
		}
	}
	for i := range s.Joins {
		jc := &s.Joins[i]
		if jc.Table != "" {
			add(jc.Table)
		}
		if jc.Subquery != nil {
			for _, n := range viewRefNames(jc.Subquery) {
				add(n)
			}
		}
	}
	return names
}

// checkViewCycle 创建视图时的循环引用静态防护：
// 新视图定义直接引用自身，或经由已存在的视图链间接引用自身（A→B→A），均报错。
func checkViewCycle(views []*ViewMeta, name string, stmt *SelectStmt) error {
	for _, n := range viewRefNames(stmt) {
		if n == name {
			return &SQLError{Msg: "cyclic view reference: " + name + " references itself"}
		}
	}
	visited := map[string]bool{}
	var dfs func(vn string) error
	dfs = func(vn string) error {
		if visited[vn] {
			return nil
		}
		visited[vn] = true
		v := findView(views, vn)
		if v == nil || v.Select == nil {
			return nil
		}
		for _, n := range viewRefNames(v.Select) {
			if n == name {
				return &SQLError{Msg: "cyclic view reference: " + name + " depends on " + vn}
			}
			if err := dfs(n); err != nil {
				return err
			}
		}
		return nil
	}
	for _, n := range viewRefNames(stmt) {
		if err := dfs(n); err != nil {
			return err
		}
	}
	return nil
}

// viewSummary 渲染视图定义摘要（SHOW VIEWS 展示用）。
func viewSummary(s *SelectStmt) string {
	if s == nil {
		return ""
	}
	cols := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		if c.Raw != "" {
			cols[i] = c.Raw
		} else {
			cols[i] = c.Name
		}
	}
	out := "SELECT " + strings.Join(cols, ", ")
	if s.From != "" {
		out += " FROM " + s.From
	}
	return out
}
