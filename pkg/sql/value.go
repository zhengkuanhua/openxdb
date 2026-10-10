package sql

import (
	"strconv"
	"strings"
)

// P1 类型系统：DATE / DECIMAL / BLOB 的值辅助函数。
//
// 设计约定（详见 docs/T9_p1_csv_types.md）：
//   - DATE：文本形态 "YYYY-MM-DD"，字典序即时间序（排序与索引键直接使用原字节）。
//   - DECIMAL：定点数，固定小数位 scale=4（decUnit=10000）。
//     内部以 int64 缩放表示（Value.I），展示/存储用规范化字符串（Value.S，如 "123.4500"）。
//     超出 int64 缩放范围（约 ±9.2e14）报错；小数位超过 4 位报错（不截断）。
//   - BLOB：二进制大对象。SQL/CSV 输入输出均使用大写十六进制文本（如 "0AFF"），
//     索引键/主键使用解码后的原始字节。

const (
	decScale = 4
	decUnit  = int64(10000)
)

// parseDecimal 解析十进制数字串为 scale=4 定点数。
// 支持 "[-+]?digits[.digits]"；返回缩放整数与规范化字符串。
func parseDecimal(s string) (int64, string, error) {
	if s == "" {
		return 0, "", &SQLError{Msg: "invalid DECIMAL: empty string"}
	}
	i := 0
	neg := false
	if s[i] == '+' || s[i] == '-' {
		neg = s[i] == '-'
		i++
	}
	intPart := int64(0)
	intDigits := 0
	for i < len(s) && isDigit(s[i]) {
		intPart = intPart*10 + int64(s[i]-'0')
		intDigits++
		i++
	}
	if intDigits == 0 {
		return 0, "", &SQLError{Msg: "invalid DECIMAL: " + s}
	}
	frac := int64(0)
	fracDigits := 0
	if i < len(s) && s[i] == '.' {
		i++
		for i < len(s) && isDigit(s[i]) {
			if fracDigits >= decScale {
				return 0, "", &SQLError{Msg: "DECIMAL too many fractional digits: " + s}
			}
			frac = frac*10 + int64(s[i]-'0')
			fracDigits++
			i++
		}
	}
	if i != len(s) {
		return 0, "", &SQLError{Msg: "invalid DECIMAL: " + s}
	}
	for fracDigits < decScale {
		frac *= 10
		fracDigits++
	}
	scaled := intPart*decUnit + frac
	if neg {
		scaled = -scaled
	}
	return scaled, decCanonical(scaled), nil
}

// decVal 从缩放整数构造 DECIMAL Value。
func decVal(scaled int64) Value {
	return Value{Kind: "DECIMAL", I: scaled, S: decCanonical(scaled)}
}

// decCanonical 将缩放整数格式化为规范化字符串（整数部分 + 固定 4 位小数）。
func decCanonical(scaled int64) string {
	neg := scaled < 0
	if neg {
		scaled = -scaled
	}
	intPart := scaled / decUnit
	frac := scaled % decUnit
	ip := strconv.FormatInt(intPart, 10)
	fp := strconv.FormatInt(frac, 10)
	for len(fp) < decScale {
		fp = "0" + fp
	}
	s := ip + "." + fp
	if neg {
		s = "-" + s
	}
	return s
}

var monthDays = [...]int{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

func isLeapYear(y int) bool {
	return y%4 == 0 && (y%100 != 0 || y%400 == 0)
}

// validateDate 校验 DATE 文本 "YYYY-MM-DD"（含真实日历合法性）。
func validateDate(s string) error {
	if len(s) != 10 || s[4] != '-' || s[7] != '-' {
		return &SQLError{Msg: "invalid DATE, want YYYY-MM-DD: " + s}
	}
	for i := 0; i < 10; i++ {
		if i == 4 || i == 7 {
			continue
		}
		if s[i] < '0' || s[i] > '9' {
			return &SQLError{Msg: "invalid DATE, want YYYY-MM-DD: " + s}
		}
	}
	y := int(s[0]-'0')*1000 + int(s[1]-'0')*100 + int(s[2]-'0')*10 + int(s[3]-'0')
	m := int(s[5]-'0')*10 + int(s[6]-'0')
	d := int(s[8]-'0')*10 + int(s[9]-'0')
	if m < 1 || m > 12 {
		return &SQLError{Msg: "invalid DATE month: " + s}
	}
	maxD := monthDays[m]
	if m == 2 && isLeapYear(y) {
		maxD = 29
	}
	if d < 1 || d > maxD {
		return &SQLError{Msg: "invalid DATE day: " + s}
	}
	return nil
}

// decodeHex 十六进制文本 → 原始字节（校验偶数长度与合法 hex 字符）。
func decodeHex(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, &SQLError{Msg: "invalid BLOB hex (odd length): " + s}
	}
	b := make([]byte, len(s)/2)
	for i := 0; i < len(b); i++ {
		hi, ok1 := hexVal(s[i*2])
		lo, ok2 := hexVal(s[i*2+1])
		if !ok1 || !ok2 {
			return nil, &SQLError{Msg: "invalid BLOB hex: " + s}
		}
		b[i] = hi<<4 | lo
	}
	return b, nil
}

func hexVal(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// encodeHex 原始字节 → 大写十六进制文本。
func encodeHex(b []byte) string {
	const digits = "0123456789ABCDEF"
	out := make([]byte, len(b)*2)
	for i, c := range b {
		out[i*2] = digits[c>>4]
		out[i*2+1] = digits[c&0xf]
	}
	return string(out)
}

// isNumericKind 判断值是否数值类（INT / DECIMAL）。
func isNumericKind(k string) bool { return k == "INT" || k == "DECIMAL" }

// compareDecInt 比较 DECIMAL 缩放值 decScaled（×1e4）与整数 i（数值序，避免溢出放大）。
func compareDecInt(decScaled, i int64) int {
	quo := decScaled / decUnit
	rem := decScaled % decUnit
	if quo > i {
		return 1
	}
	if quo < i {
		return -1
	}
	if rem > 0 {
		return 1
	}
	if rem < 0 {
		return -1
	}
	return 0
}

// coerceValue 按列类型校验/转换单个值。
// 规则：INT 列仅 INT；TEXT 列接受 TEXT/DATE（归一为 TEXT）；
// DATE 列接受 DATE 或合法 "YYYY-MM-DD" 文本；DECIMAL 列接受 DECIMAL/INT/合法数字文本；
// BLOB 列接受 BLOB 或合法 hex 文本。
func coerceValue(typ, name string, v Value) (Value, error) {
	switch typ {
	case "INT":
		if v.Kind != "INT" {
			return Value{}, &SQLError{Msg: "type mismatch for column: " + name}
		}
		return v, nil
	case "TEXT":
		if v.Kind == "INT" || v.Kind == "DECIMAL" || v.Kind == "BLOB" {
			return Value{}, &SQLError{Msg: "type mismatch for column: " + name}
		}
		return StrVal(v.S), nil
	case "DATE":
		switch v.Kind {
		case "DATE":
			return v, nil
		case "TEXT":
			if err := validateDate(v.S); err != nil {
				return Value{}, &SQLError{Msg: "invalid DATE for column " + name + ": " + v.S}
			}
			return Value{Kind: "DATE", S: v.S}, nil
		}
		return Value{}, &SQLError{Msg: "type mismatch for column: " + name}
	case "DECIMAL":
		switch v.Kind {
		case "INT":
			scaled := v.I * decUnit
			return Value{Kind: "DECIMAL", I: scaled, S: decCanonical(scaled)}, nil
		case "DECIMAL":
			return v, nil
		case "TEXT":
			scaled, canon, err := parseDecimal(strings.TrimSpace(v.S))
			if err != nil {
				return Value{}, &SQLError{Msg: "invalid DECIMAL for column " + name + ": " + v.S}
			}
			return Value{Kind: "DECIMAL", I: scaled, S: canon}, nil
		}
		return Value{}, &SQLError{Msg: "type mismatch for column: " + name}
	case "BLOB":
		switch v.Kind {
		case "BLOB":
			return v, nil
		case "TEXT":
			raw, err := decodeHex(v.S)
			if err != nil {
				return Value{}, &SQLError{Msg: "invalid BLOB hex for column " + name + ": " + v.S}
			}
			return Value{Kind: "BLOB", S: encodeHex(raw)}, nil
		}
		return Value{}, &SQLError{Msg: "type mismatch for column: " + name}
	}
	return Value{}, &SQLError{Msg: "unsupported column type: " + typ}
}

// zeroValue 返回列类型的默认值（M9 在线 DDL：新增列回填已有行）。
func zeroValue(typ string) Value {
	switch typ {
	case "INT":
		return IntVal(0)
	case "DATE":
		return Value{Kind: "DATE", S: "1970-01-01"}
	case "DECIMAL":
		return decVal(0)
	case "BLOB":
		return Value{Kind: "BLOB", S: ""}
	default:
		return StrVal("")
	}
}

// String 规范化：BLOB 以大写 hex 输出。
func (v Value) String() string {
	if v.Kind == "INT" {
		return itoa(v.I)
	}
	if v.Kind == "DECIMAL" {
		return v.S
	}
	if v.Kind == "BLOB" {
		return strings.ToUpper(v.S)
	}
	return v.S
}
