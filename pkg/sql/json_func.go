// Copyright (c) 2024-2026 OpenXDB Contributors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package sql

import (
	"encoding/json"
	"io"
	"strconv"
	"strings"
)

// v1.19.0 D6：SQL JSON 函数族（MySQL 子集）。
//
// 覆盖 JSON_VALID / JSON_EXTRACT / JSON_UNQUOTE / JSON_TYPE / JSON_LENGTH /
// JSON_OBJECT / JSON_ARRAY / JSON_SET 八个标量函数，参数支持列引用、字面量与
// 嵌套调用（解析层见 parser.parseJSONFuncExpr，求值入口见 jsonFuncValue）。
//
// 语义口径：
//   - 结果是既有 Value（TEXT / INT / NULL），不引入新 Kind；对象与数组以 MySQL
//     风格的紧凑文本（", " 与 ": " 分隔）回传，供协议层按 TEXT 下发并可再次
//     作为 JSON 函数入参解析。
//   - 非法 JSON 文本、非法路径表达式、参数个数不符一律返回明确错误，不静默
//     返回 NULL；例外是 JSON_VALID：按语义返回 0/1，不报错。
//   - 入参为 NULL 时按 SQL 语义传播 NULL（JSON_VALID 例外，返回 NULL）。
//   - 路径语法支持 `$`、`.member`、`[index]`（可任意组合，如 `$.a[0].b`），
//     另支持 `$."quoted member"` 形式承载含特殊字符的键名。

// jsonFuncNameSet 已注册的 JSON 函数名（大写）。
var jsonFuncNameSet = map[string]bool{
	"JSON_VALID":   true,
	"JSON_EXTRACT": true,
	"JSON_UNQUOTE": true,
	"JSON_TYPE":    true,
	"JSON_LENGTH":  true,
	"JSON_OBJECT":  true,
	"JSON_ARRAY":   true,
	"JSON_SET":     true,
}

// isJSONFuncName 判断标识符是否为已注册的 JSON 函数名（大小写不敏感）。
func isJSONFuncName(name string) bool {
	return jsonFuncNameSet[strings.ToUpper(name)]
}

// jsonNode JSON 文档内存模型（保持对象键序与数字原始字面量）。
// kind 取值："object" | "array" | "string" | "number" | "bool" | "null"。
type jsonNode struct {
	kind string
	keys []string    // object 键序
	vals []*jsonNode // object 值 / array 元素
	str  string      // string 值（已反转义）
	num  string      // number 原始字面量
	b    bool        // bool 值
}

// jsonPathStep 路径单步：isIdx 为 true 时取数组下标 idx，否则取对象成员 key。
type jsonPathStep struct {
	isIdx bool
	idx   int
	key   string
}

// jsonFuncValue 求值 JSON 函数（args 为已求值的实参列表）。
// 参数个数或类型不合法时返回 SQLError，非法 JSON / 非法路径同样报错。
// argIsJSON 标记实参本身是否为 JSON 函数结果（嵌套调用），用于保留 JSON 结构
// 语义：JSON_ARRAY(JSON_ARRAY(1)) 得到 [[1]] 而不是 ["[1]"]。
func jsonFuncValue(fn string, args []Value, argIsJSON []bool) (Value, error) {
	name := strings.ToUpper(fn)
	isJSON := func(i int) bool { return i < len(argIsJSON) && argIsJSON[i] }
	switch name {
	case "JSON_VALID":
		return jsonValidFunc(args)
	case "JSON_EXTRACT":
		return jsonExtractFunc(args)
	case "JSON_UNQUOTE":
		return jsonUnquoteFunc(args)
	case "JSON_TYPE":
		return jsonTypeFunc(args)
	case "JSON_LENGTH":
		return jsonLengthFunc(args)
	case "JSON_OBJECT":
		return jsonObjectFunc(args, isJSON)
	case "JSON_ARRAY":
		return jsonArrayFunc(args, isJSON)
	case "JSON_SET":
		return jsonSetFunc(args, isJSON)
	}
	return Value{}, errf("unsupported JSON function %q", fn)
}

// jsonNullValue 返回 SQL NULL 值。
func jsonNullValue() Value { return Value{Kind: "NULL"} }

// jsonTextOf 取 Value 的文本视图（JSON 文本入参统一按文本解析）。
func jsonTextOf(v Value) string {
	switch v.Kind {
	case "INT", "DECIMAL", "DATE", "TEXT", "BLOB", "VAR", "TRIGREF":
		return v.String()
	case "NULL":
		return ""
	}
	return v.String()
}

// jsonArgCount 校验实参个数（min..max，max<0 表示不限上界）。
func jsonArgCount(fn string, args []Value, min, max int) error {
	if len(args) >= min && (max < 0 || len(args) <= max) {
		return nil
	}
	return errf("wrong number of arguments to %s (got %d)", strings.ToLower(fn), len(args))
}

// jsonParseDocument 解析 JSON 文本文档；入参为 NULL 时返回 (nil, nil) 表示 NULL 传播。
func jsonParseDocument(fn string, v Value, argIdx int) (*jsonNode, error) {
	if v.Kind == "NULL" {
		return nil, nil
	}
	n, err := parseJSONText(jsonTextOf(v))
	if err != nil {
		return nil, errf("invalid JSON text in argument %d to %s: %s", argIdx, strings.ToLower(fn), err.Error())
	}
	return n, nil
}

// jsonParsePathArg 解析路径实参；NULL 返回 (nil, nil)。
func jsonParsePathArg(fn string, v Value, argIdx int) ([]jsonPathStep, error) {
	if v.Kind == "NULL" {
		return nil, nil
	}
	if v.Kind != "TEXT" {
		return nil, errf("invalid JSON path argument %d to %s: path expression must be a string", argIdx, strings.ToLower(fn))
	}
	steps, err := parseJSONPath(v.S)
	if err != nil {
		return nil, errf("invalid JSON path expression %q in %s: %s", v.S, strings.ToLower(fn), err.Error())
	}
	return steps, nil
}

// jsonValidFunc JSON_VALID(doc)：合法 JSON 返回 1，非法返回 0；NULL 入参返回 NULL。
func jsonValidFunc(args []Value) (Value, error) {
	if err := jsonArgCount("JSON_VALID", args, 1, 1); err != nil {
		return Value{}, err
	}
	if args[0].Kind == "NULL" {
		return jsonNullValue(), nil
	}
	if _, err := parseJSONText(jsonTextOf(args[0])); err != nil {
		return IntVal(0), nil
	}
	return IntVal(1), nil
}

// jsonExtractFunc JSON_EXTRACT(doc, path[, path ...])：
// 单路径返回该路径值（不存在为 NULL），多路径返回按路径顺序组成的 JSON 数组。
func jsonExtractFunc(args []Value) (Value, error) {
	if err := jsonArgCount("JSON_EXTRACT", args, 2, -1); err != nil {
		return Value{}, err
	}
	doc, err := jsonParseDocument("JSON_EXTRACT", args[0], 1)
	if err != nil {
		return Value{}, err
	}
	if doc == nil {
		return jsonNullValue(), nil
	}
	paths := make([][]jsonPathStep, 0, len(args)-1)
	for i, a := range args[1:] {
		steps, err := jsonParsePathArg("JSON_EXTRACT", a, i+2)
		if err != nil {
			return Value{}, err
		}
		if steps == nil {
			return jsonNullValue(), nil
		}
		paths = append(paths, steps)
	}
	if len(paths) == 1 {
		hit := jsonGetPath(doc, paths[0])
		if hit == nil {
			return jsonNullValue(), nil
		}
		return StrVal(hit.encode()), nil
	}
	out := &jsonNode{kind: "array"}
	for _, steps := range paths {
		hit := jsonGetPath(doc, steps)
		if hit == nil {
			hit = &jsonNode{kind: "null"}
		}
		out.vals = append(out.vals, hit)
	}
	return StrVal(out.encode()), nil
}

// jsonUnquoteFunc JSON_UNQUOTE(x)：JSON 字符串去引号并反转义；非 JSON 字符串原样返回。
func jsonUnquoteFunc(args []Value) (Value, error) {
	if err := jsonArgCount("JSON_UNQUOTE", args, 1, 1); err != nil {
		return Value{}, err
	}
	if args[0].Kind == "NULL" {
		return jsonNullValue(), nil
	}
	text := jsonTextOf(args[0])
	if n, err := parseJSONText(text); err == nil && n.kind == "string" {
		return StrVal(n.str), nil
	}
	return StrVal(text), nil
}

// jsonTypeFunc JSON_TYPE(x)：返回 OBJECT/ARRAY/STRING/INTEGER/DOUBLE/BOOLEAN/NULL。
func jsonTypeFunc(args []Value) (Value, error) {
	if err := jsonArgCount("JSON_TYPE", args, 1, 1); err != nil {
		return Value{}, err
	}
	doc, err := jsonParseDocument("JSON_TYPE", args[0], 1)
	if err != nil {
		return Value{}, err
	}
	if doc == nil {
		return jsonNullValue(), nil
	}
	return StrVal(doc.typeName()), nil
}

// jsonLengthFunc JSON_LENGTH(doc[, path])：对象/数组元素个数，标量固定为 1；
// 路径不存在返回 NULL。
func jsonLengthFunc(args []Value) (Value, error) {
	if err := jsonArgCount("JSON_LENGTH", args, 1, 2); err != nil {
		return Value{}, err
	}
	doc, err := jsonParseDocument("JSON_LENGTH", args[0], 1)
	if err != nil {
		return Value{}, err
	}
	if doc == nil {
		return jsonNullValue(), nil
	}
	target := doc
	if len(args) == 2 {
		steps, err := jsonParsePathArg("JSON_LENGTH", args[1], 2)
		if err != nil {
			return Value{}, err
		}
		if steps == nil {
			return jsonNullValue(), nil
		}
		target = jsonGetPath(doc, steps)
		if target == nil {
			return jsonNullValue(), nil
		}
	}
	switch target.kind {
	case "object":
		return IntVal(int64(len(target.keys))), nil
	case "array":
		return IntVal(int64(len(target.vals))), nil
	}
	return IntVal(1), nil
}

// jsonObjectFunc JSON_OBJECT(key, val[, key, val ...])：按实参顺序构造 JSON 对象。
func jsonObjectFunc(args []Value, isJSON func(int) bool) (Value, error) {
	if len(args)%2 != 0 {
		return Value{}, errf("incorrect parameter count in the call to json_object (got %d)", len(args))
	}
	out := &jsonNode{kind: "object"}
	for i := 0; i < len(args); i += 2 {
		if args[i].Kind == "NULL" {
			return Value{}, errf("json_object key must not be NULL (argument %d)", i+1)
		}
		out.keys = append(out.keys, jsonTextOf(args[i]))
		out.vals = append(out.vals, jsonNodeOfValue(args[i+1], isJSON(i+1)))
	}
	return StrVal(out.encode()), nil
}

// jsonArrayFunc JSON_ARRAY(v[, ...])：按实参顺序构造 JSON 数组。
func jsonArrayFunc(args []Value, isJSON func(int) bool) (Value, error) {
	out := &jsonNode{kind: "array", vals: make([]*jsonNode, 0, len(args))}
	for i, a := range args {
		out.vals = append(out.vals, jsonNodeOfValue(a, isJSON(i)))
	}
	return StrVal(out.encode()), nil
}

// jsonSetFunc JSON_SET(doc, path, val[, path, val ...])：
// 路径存在则替换，不存在则创建（数组下标越界仅允许等于长度处追加）。
func jsonSetFunc(args []Value, isJSON func(int) bool) (Value, error) {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return Value{}, errf("incorrect parameter count in the call to json_set (got %d)", len(args))
	}
	doc, err := jsonParseDocument("JSON_SET", args[0], 1)
	if err != nil {
		return Value{}, err
	}
	if doc == nil {
		return jsonNullValue(), nil
	}
	root := doc
	for i := 1; i < len(args); i += 2 {
		steps, err := jsonParsePathArg("JSON_SET", args[i], i+1)
		if err != nil {
			return Value{}, err
		}
		if steps == nil {
			return jsonNullValue(), nil
		}
		pathtext := args[i].S
		val := jsonNodeOfValue(args[i+1], isJSON(i+1))
		root, err = jsonSetPath(root, steps, val, pathtext)
		if err != nil {
			return Value{}, err
		}
	}
	return StrVal(root.encode()), nil
}

// jsonNodeOfValue 把 SQL 值转换为 JSON 节点（用于 JSON_SET 赋值与 JSON_OBJECT/ARRAY 元素）：
// INT/DECIMAL 按数字、TEXT 按字符串、NULL 按 null、其余按字符串（BLOB 为十六进制文本）；
// 实参为嵌套 JSON 函数结果时按 JSON 文档解析，保留对象/数组结构。
func jsonNodeOfValue(v Value, asJSON bool) *jsonNode {
	if asJSON && v.Kind != "NULL" {
		if n, err := parseJSONText(jsonTextOf(v)); err == nil {
			return n
		}
	}
	switch v.Kind {
	case "INT":
		return &jsonNode{kind: "number", num: strconv.FormatInt(v.I, 10)}
	case "DECIMAL":
		return &jsonNode{kind: "number", num: v.S}
	case "NULL":
		return &jsonNode{kind: "null"}
	}
	return &jsonNode{kind: "string", str: jsonTextOf(v)}
}

// typeName JSON_TYPE 输出名。
func (n *jsonNode) typeName() string {
	switch n.kind {
	case "object":
		return "OBJECT"
	case "array":
		return "ARRAY"
	case "string":
		return "STRING"
	case "number":
		if strings.ContainsAny(n.num, ".eE") {
			return "DOUBLE"
		}
		return "INTEGER"
	case "bool":
		return "BOOLEAN"
	}
	return "NULL"
}

// encode 序列化为 MySQL 风格紧凑 JSON 文本（分隔符 ", " 与 ": "）。
func (n *jsonNode) encode() string {
	var b strings.Builder
	n.encodeTo(&b)
	return b.String()
}

func (n *jsonNode) encodeTo(b *strings.Builder) {
	switch n.kind {
	case "object":
		b.WriteByte('{')
		for i, k := range n.keys {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(encodeJSONString(k))
			b.WriteString(": ")
			n.vals[i].encodeTo(b)
		}
		b.WriteByte('}')
	case "array":
		b.WriteByte('[')
		for i, v := range n.vals {
			if i > 0 {
				b.WriteString(", ")
			}
			v.encodeTo(b)
		}
		b.WriteByte(']')
	case "string":
		b.WriteString(encodeJSONString(n.str))
	case "number":
		b.WriteString(n.num)
	case "bool":
		if n.b {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	default:
		b.WriteString("null")
	}
}

// encodeJSONString 输出 JSON 字符串字面量（含引号），转义控制字符与引号/反斜杠。
func encodeJSONString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString("\\\"")
		case '\\':
			b.WriteString("\\\\")
		case '\n':
			b.WriteString("\\n")
		case '\r':
			b.WriteString("\\r")
		case '\t':
			b.WriteString("\\t")
		case '\b':
			b.WriteString("\\b")
		case '\f':
			b.WriteString("\\f")
		default:
			if r < 0x20 || r == 0x2028 || r == 0x2029 {
				b.WriteString("\\u")
				const hexchars = "0123456789abcdef"
				b.WriteByte(hexchars[(r>>12)&0xF])
				b.WriteByte(hexchars[(r>>8)&0xF])
				b.WriteByte(hexchars[(r>>4)&0xF])
				b.WriteByte(hexchars[r&0xF])
				continue
			}
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// parseJSONText 解析 JSON 文本为标准节点（保留对象键序与数字原文）。
func parseJSONText(text string) (*jsonNode, error) {
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	n, err := decodeJSONValue(dec)
	if err != nil {
		return nil, err
	}
	// 值之后只允许空白，多余内容按非法 JSON 处理。
	if _, err := dec.Token(); err != io.EOF {
		return nil, errf("trailing data after JSON value")
	}
	return n, nil
}

// decodeJSONValue 从 token 流解析一个 JSON 值（递归）。
func decodeJSONValue(dec *json.Decoder) (*jsonNode, error) {
	tok, err := dec.Token()
	if err != nil {
		if err == io.EOF {
			return nil, errf("unexpected end of JSON text")
		}
		return nil, err
	}
	return decodeJSONFromToken(dec, tok)
}

// decodeJSONFromToken 依据当前 token 构造节点并消费其子结构。
func decodeJSONFromToken(dec *json.Decoder, tok json.Token) (*jsonNode, error) {
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			n := &jsonNode{kind: "object"}
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return nil, err
				}
				key, ok := kt.(string)
				if !ok {
					return nil, errf("unexpected object key token")
				}
				v, err := decodeJSONValue(dec)
				if err != nil {
					return nil, err
				}
				n.keys = append(n.keys, key)
				n.vals = append(n.vals, v)
			}
			if _, err := dec.Token(); err != nil {
				return nil, err
			}
			return n, nil
		case '[':
			n := &jsonNode{kind: "array"}
			for dec.More() {
				v, err := decodeJSONValue(dec)
				if err != nil {
					return nil, err
				}
				n.vals = append(n.vals, v)
			}
			if _, err := dec.Token(); err != nil {
				return nil, err
			}
			return n, nil
		}
		return nil, errf("unexpected JSON delimiter %q", string(t))
	case string:
		return &jsonNode{kind: "string", str: t}, nil
	case json.Number:
		return &jsonNode{kind: "number", num: t.String()}, nil
	case bool:
		return &jsonNode{kind: "bool", b: t}, nil
	case nil:
		return &jsonNode{kind: "null"}, nil
	}
	return nil, errf("unexpected JSON token")
}

// parseJSONPath 解析 JSON 路径表达式：`$` 起头，后续可跟任意个 `.member` / `[index]`。
func parseJSONPath(p string) ([]jsonPathStep, error) {
	s := strings.TrimSpace(p)
	if s == "" || s[0] != '$' {
		return nil, errf("path must start with '$'")
	}
	steps := make([]jsonPathStep, 0, 4)
	i := 1
	for i < len(s) {
		switch s[i] {
		case '.':
			i++
			if i < len(s) && s[i] == '"' {
				i++
				var b strings.Builder
				closed := false
				for i < len(s) {
					c := s[i]
					if c == '\\' && i+1 < len(s) {
						b.WriteByte(s[i+1])
						i += 2
						continue
					}
					if c == '"' {
						i++
						closed = true
						break
					}
					b.WriteByte(c)
					i++
				}
				if !closed {
					return nil, errf("unterminated quoted member name")
				}
				steps = append(steps, jsonPathStep{key: b.String()})
				continue
			}
			start := i
			for i < len(s) && s[i] != '.' && s[i] != '[' {
				i++
			}
			if start == i {
				return nil, errf("empty member name after '.'")
			}
			steps = append(steps, jsonPathStep{key: s[start:i]})
		case '[':
			i++
			start := i
			for i < len(s) && s[i] != ']' {
				i++
			}
			if i >= len(s) {
				return nil, errf("missing ']' in array index")
			}
			num := strings.TrimSpace(s[start:i])
			n, err := strconv.Atoi(num)
			if err != nil || n < 0 {
				return nil, errf("invalid array index %q", num)
			}
			steps = append(steps, jsonPathStep{isIdx: true, idx: n})
			i++
		default:
			return nil, errf("unexpected character %q after path step", string(s[i]))
		}
	}
	return steps, nil
}

// jsonGetPath 按路径取值：不存在或中途类型不符返回 nil。
func jsonGetPath(n *jsonNode, steps []jsonPathStep) *jsonNode {
	cur := n
	for _, st := range steps {
		if cur == nil {
			return nil
		}
		if st.isIdx {
			if cur.kind != "array" || st.idx >= len(cur.vals) {
				return nil
			}
			cur = cur.vals[st.idx]
			continue
		}
		if cur.kind != "object" {
			return nil
		}
		found := false
		for i, k := range cur.keys {
			if k == st.key {
				cur = cur.vals[i]
				found = true
				break
			}
		}
		if !found {
			return nil
		}
	}
	return cur
}

// jsonClone 深拷贝节点（JSON_SET 写入走 copy-on-write，避免共享子树被改动）。
func jsonClone(n *jsonNode) *jsonNode {
	if n == nil {
		return &jsonNode{kind: "null"}
	}
	c := &jsonNode{kind: n.kind, str: n.str, num: n.num, b: n.b}
	if n.kind == "object" || n.kind == "array" {
		c.keys = append([]string{}, n.keys...)
		c.vals = make([]*jsonNode, len(n.vals))
		for i, v := range n.vals {
			c.vals[i] = jsonClone(v)
		}
	}
	return c
}

// jsonSetPath 按路径写入值并返回新根节点（路径不存在时创建，标量处写入报错）。
func jsonSetPath(n *jsonNode, steps []jsonPathStep, val *jsonNode, pathtext string) (*jsonNode, error) {
	if len(steps) == 0 {
		return val, nil
	}
	if n == nil {
		n = &jsonNode{kind: "null"}
	}
	st := steps[0]
	rest := steps[1:]
	if st.isIdx {
		if n.kind == "null" {
			n = &jsonNode{kind: "array"}
		}
		if n.kind != "array" {
			return nil, errf("cannot set JSON path %q: document element is not an array", pathtext)
		}
		out := jsonClone(n)
		if st.idx > len(out.vals) {
			return nil, errf("cannot set JSON path %q: array index %d out of range (length %d)", pathtext, st.idx, len(out.vals))
		}
		if st.idx == len(out.vals) {
			child, err := jsonSetPath(nil, rest, val, pathtext)
			if err != nil {
				return nil, err
			}
			out.vals = append(out.vals, child)
			return out, nil
		}
		child, err := jsonSetPath(out.vals[st.idx], rest, val, pathtext)
		if err != nil {
			return nil, err
		}
		out.vals[st.idx] = child
		return out, nil
	}
	if n.kind == "null" {
		n = &jsonNode{kind: "object"}
	}
	if n.kind != "object" {
		return nil, errf("cannot set JSON path %q: document element is not an object", pathtext)
	}
	out := jsonClone(n)
	for i, k := range out.keys {
		if k == st.key {
			child, err := jsonSetPath(out.vals[i], rest, val, pathtext)
			if err != nil {
				return nil, err
			}
			out.vals[i] = child
			return out, nil
		}
	}
	child, err := jsonSetPath(nil, rest, val, pathtext)
	if err != nil {
		return nil, err
	}
	out.keys = append(out.keys, st.key)
	out.vals = append(out.vals, child)
	return out, nil
}
