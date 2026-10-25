package sql

import (
	"strconv"
	"strings"
)

// P1 类型系统：DATE / DECIMAL / BLOB 的值辅助函数。
//
// 设计约定（详见 docs/T9_p1_csv_types.md）：
//   - DATE：文本形态 "YYYY-MM-DD"，字典序即时间序（排序与索引键直接使用原字节）。
//   - DECIMAL：定点数，小数位可配置（默认 scale=4，上限 maxDecScale=12，v1.12.0 高精度）。
//     内部以 int64 缩放表示（Value.I），展示/存储用规范化字符串（Value.S，如 "123.4500"）。
//     超出 int64 缩放范围（约 ±9.2e14）报错；小数位超过列声明 scale 按四舍五入截断
//     （如 1.12345 -> 1.1235，TPC-C 依赖；行协议 CSV 导入路径由 csv.go 精度校验拒绝超精度输入）。
//   - BLOB：二进制大对象。SQL/CSV 输入输出均使用大写十六进制文本（如 "0AFF"），
//     索引键/主键使用解码后的原始字节。

const (
	decScale    = 4
	decUnit     = int64(10000)
	maxDecScale = 12 // v1.12.0 DECIMAL 高精度：可配置小数位上限（金融场景）
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
	roundChecked := false
	roundUp := false
	if i < len(s) && s[i] == '.' {
		i++
		for i < len(s) && isDigit(s[i]) {
			if fracDigits < decScale {
				frac = frac*10 + int64(s[i]-'0')
				fracDigits++
			} else if !roundChecked {
				// MySQL 兼容：超精度小数四舍五入（如 212.10000000000002 -> 212.1000），
				// 只取被截断部分的首位判断是否进位。
				roundChecked = true
				if s[i] >= '5' {
					roundUp = true
				}
			}
			i++
		}
		if roundUp {
			frac++
			if frac >= decUnit {
				frac = 0
				intPart++
			}
		}
	}
	// 科学计数法：1.2E3 / 6.0E-4 / 1E+06（MySQL 兼容，Connector/J setDouble 内联输出）。
	exp := 0
	expNeg := false
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			expNeg = s[i] == '-'
			i++
		}
		if i >= len(s) || !isDigit(s[i]) {
			return 0, "", &SQLError{Msg: "invalid DECIMAL: " + s}
		}
		for i < len(s) && isDigit(s[i]) {
			exp = exp*10 + int(s[i]-'0')
			i++
		}
		if expNeg {
			exp = -exp
		}
	}
	if i != len(s) {
		return 0, "", &SQLError{Msg: "invalid DECIMAL: " + s}
	}
	// 定点换算：真实值 = digits × 10^(exp - fracDigits)；scaled = 真实值 × 10^decScale。
	// digits = intPart 串 + frac 串拼接成的整数（如 "6.0" -> 60）。
	digits := intPart
	for k := 0; k < fracDigits; k++ {
		digits *= 10
	}
	digits += frac
	shift := decScale - fracDigits + exp
	var scaled int64
	if shift >= 0 {
		scaled = digits
		for k := 0; k < shift; k++ {
			scaled *= 10
		}
	} else {
		scaled = digits
		for k := 0; k < -shift; k++ {
			scaled /= 10
		}
	}
	if neg {
		scaled = -scaled
	}
	return scaled, decCanonical(scaled), nil
}

// parseDecimalScaled 解析十进制数字串为指定 scale 定点数（v1.12.0 DECIMAL 高精度）。
// 支持 "[-+]?digits[.digits][eE[+-]digits]"；超精度小数按 MySQL 兼容四舍五入；
// 返回按 scale 缩放的整数与规范化字符串。scale 越界回退默认 4。
func parseDecimalScaled(s string, scale int) (int64, string, error) {
	if scale <= 0 || scale > maxDecScale {
		scale = decScale
	}
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
	roundChecked := false
	roundUp := false
	if i < len(s) && s[i] == '.' {
		i++
		for i < len(s) && isDigit(s[i]) {
			if fracDigits < scale {
				frac = frac*10 + int64(s[i]-'0')
				fracDigits++
			} else if !roundChecked {
				// MySQL 兼容：超精度小数四舍五入，只取被截断部分的首位判断是否进位。
				roundChecked = true
				if s[i] >= '5' {
					roundUp = true
				}
			}
			i++
		}
		if roundUp {
			frac++
			if frac >= pow10Int64(scale) {
				frac = 0
				intPart++
			}
		}
	}
	// 科学计数法：1.2E3 / 6.0E-4 / 1E+06（MySQL 兼容，Connector/J setDouble 内联输出）。
	exp := 0
	expNeg := false
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			expNeg = s[i] == '-'
			i++
		}
		if i >= len(s) || !isDigit(s[i]) {
			return 0, "", &SQLError{Msg: "invalid DECIMAL: " + s}
		}
		for i < len(s) && isDigit(s[i]) {
			exp = exp*10 + int(s[i]-'0')
			i++
		}
		if expNeg {
			exp = -exp
		}
	}
	if i != len(s) {
		return 0, "", &SQLError{Msg: "invalid DECIMAL: " + s}
	}
	// 定点换算：真实值 = digits × 10^(exp - fracDigits)；scaled = 真实值 × 10^scale。
	digits := intPart
	for k := 0; k < fracDigits; k++ {
		digits *= 10
	}
	digits += frac
	shift := scale - fracDigits + exp
	var scaled int64
	if shift >= 0 {
		scaled = digits
		for k := 0; k < shift; k++ {
			scaled *= 10
		}
	} else {
		scaled = digits
		for k := 0; k < -shift; k++ {
			scaled /= 10
		}
	}
	if neg {
		scaled = -scaled
	}
	return scaled, decCanonicalScaled(scaled, scale), nil
}

// decimalLiteralScale 返回纯十进制数字字面量的小数位数（不含科学计数，如 "123.456" → 3）。
// v1.12.0 高精度：SQL 字面量按实际位数保留，列写入时再按声明 scale 重缩放。
func decimalLiteralScale(text string) int {
	i := 0
	if i < len(text) && (text[i] == '+' || text[i] == '-') {
		i++
	}
	for i < len(text) && isDigit(text[i]) {
		i++
	}
	if i >= len(text) || text[i] != '.' {
		return 0
	}
	i++
	ds := 0
	for i < len(text) && isDigit(text[i]) {
		ds++
		i++
	}
	return ds
}

// decVal 从缩放整数构造 DECIMAL Value（默认 4 位小数）。
func decVal(scaled int64) Value {
	return Value{Kind: "DECIMAL", I: scaled, S: decCanonical(scaled)}
}

// decValScaled 从缩放整数按指定小数位构造 DECIMAL Value（v1.12.0 DECIMAL 高精度）。
func decValScaled(scaled int64, ds int) Value {
	if ds <= 0 || ds > maxDecScale {
		ds = decScale
	}
	return Value{Kind: "DECIMAL", I: scaled, S: decCanonicalScaled(scaled, ds), DS: ds}
}

// decDS 返回 DECIMAL 值的小数位（未设置或越界时回退默认 4）。
func decDS(v Value) int {
	if v.Kind != "DECIMAL" {
		return decScale
	}
	if v.DS <= 0 || v.DS > maxDecScale {
		return decScale
	}
	return v.DS
}

// pow10Int64 返回 10^n（n∈[0,18]）。
func pow10Int64(n int) int64 {
	p := int64(1)
	for i := 0; i < n; i++ {
		p *= 10
	}
	return p
}

// roundScaled 将缩放值从 from 位小数缩放到 to 位（四舍五入，MySQL 兼容）。
func roundScaled(scaled int64, from, to int) int64 {
	if to >= from {
		return scaled * pow10Int64(to-from)
	}
	d := from - to
	unit := pow10Int64(d)
	q := scaled / unit
	r := scaled % unit
	half := unit / 2
	if r >= half {
		q++
	} else if r <= -half {
		q--
	}
	return q
}

// compareDecScaledInt 比较按 scale 缩放的 DECIMAL 缩放值 decScaled（×10^scale）与整数 i（数值序，避免溢出放大）。
func compareDecScaledInt(decScaled int64, scale int, i int64) int {
	unit := pow10Int64(scale)
	quo := decScaled / unit
	rem := decScaled % unit
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

// decCanonicalScaled 将缩放整数按指定小数位格式化为规范化字符串（v1.12.0 高精度）。
// scale 越界（<=0 或 >maxDecScale）回退默认 4 位，保证存量路径不变。
func decCanonicalScaled(scaled int64, scale int) string {
	if scale <= 0 || scale > maxDecScale {
		scale = decScale
	}
	unit := pow10Int64(scale)
	neg := scaled < 0
	if neg {
		scaled = -scaled
	}
	intPart := scaled / unit
	frac := scaled % unit
	ip := strconv.FormatInt(intPart, 10)
	fp := strconv.FormatInt(frac, 10)
	for len(fp) < scale {
		fp = "0" + fp
	}
	s := ip + "." + fp
	if neg {
		s = "-" + s
	}
	return s
}

// decCanonical 将缩放整数格式化为规范化字符串（整数部分 + 固定 4 位小数）。
func decCanonical(scaled int64) string {
	return decCanonicalScaled(scaled, decScale)
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
// coerceValue 按列类型校验/转换单个值（默认 4 位 DECIMAL，存量路径不变）。
func coerceValue(typ, name string, v Value) (Value, error) {
	return coerceValueScaled(typ, name, 0, v)
}

// coerceValueScaled 按列类型校验/转换单个值（scale 感知版，v1.12.0 DECIMAL 高精度）。
// scale<=0 表示列未声明精度，按默认 4 位处理，与 coerceValue 行为完全一致。
// 规则：INT 列仅 INT；TEXT 列接受 TEXT/DATE（归一为 TEXT）；
// DATE 列接受 DATE 或合法 "YYYY-MM-DD" 文本；DECIMAL 列接受 DECIMAL/INT/合法数字文本；
// BLOB 列接受 BLOB 或合法 hex 文本。
func coerceValueScaled(typ, name string, scale int, v Value) (Value, error) {
	if scale <= 0 || scale > maxDecScale {
		scale = decScale
	}
	if v.Kind == "NULL" {
		return v, nil
	}
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
			s := v.S
			// TIMESTAMP 文本（"YYYY-MM-DD HH:MM:SS"）取日期部分，兼容 TPC-C schema 映射
			if len(s) > 10 && s[10] == ' ' {
				s = s[:10]
			}
			if err := validateDate(s); err != nil {
				return Value{}, &SQLError{Msg: "invalid DATE for column " + name + ": " + v.S}
			}
			return Value{Kind: "DATE", S: s}, nil
		}
		return Value{}, &SQLError{Msg: "type mismatch for column: " + name}
	case "DECIMAL":
		switch v.Kind {
		case "INT":
			scaled := v.I * pow10Int64(scale)
			return decValScaled(scaled, scale), nil
		case "DECIMAL":
			// 列小数位固定：值与列 scale 不一致时四舍五入重缩放（MySQL 兼容列语义）。
			if decDS(v) != scale {
				return decValScaled(roundScaled(v.I, decDS(v), scale), scale), nil
			}
			return v, nil
		case "TEXT":
			scaled, canon, err := parseDecimalScaled(strings.TrimSpace(v.S), scale)
			if err != nil {
				return Value{}, &SQLError{Msg: "invalid DECIMAL for column " + name + ": " + v.S}
			}
			return Value{Kind: "DECIMAL", I: scaled, S: canon, DS: scale}, nil
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

// convertColValue T39 MODIFY COLUMN：按目标类型宽松转换旧值。
// 语义对齐 MySQL 宽松转换：INT↔TEXT/DATE/DECIMAL 可互转（文本解析失败回填零值），
// BLOB 与 TEXT 按 hex 文本透传；同类型幂等透传，不做严格报错（区别于 insert 的 coerceValue）。
func convertColValue(v Value, newType string, scale int) Value {
	switch newType {
	case "INT":
		switch v.Kind {
		case "INT":
			return v
		case "DECIMAL":
			return IntVal(v.I / pow10Int64(decDS(v))) // 小数截断（v1.12.0 按声明 scale）
		case "TEXT", "DATE":
			n, err := strconv.ParseInt(strings.TrimSpace(v.S), 10, 64)
			if err != nil {
				return IntVal(0)
			}
			return IntVal(n)
		default:
			return IntVal(0)
		}
	case "DECIMAL":
		if scale <= 0 || scale > maxDecScale {
			scale = decScale
		}
		switch v.Kind {
		case "DECIMAL":
			// v1.12.0 高精度：按目标声明 scale 重缩放（同精度幂等原样）。
			ds := decDS(v)
			if ds == scale {
				return v
			}
			return decValScaled(roundScaled(v.I, ds, scale), scale)
		case "INT":
			return decValScaled(v.I*pow10Int64(scale), scale)
		case "TEXT", "DATE":
			scaled, canon, err := parseDecimalScaled(strings.TrimSpace(v.S), scale)
			if err != nil {
				return decValScaled(0, scale)
			}
			return Value{Kind: "DECIMAL", I: scaled, S: canon, DS: scale}
		default:
			return decValScaled(0, scale)
		}
	case "DATE":
		switch v.Kind {
		case "DATE":
			return v
		case "TEXT":
			s := v.S
			// TIMESTAMP 文本（"YYYY-MM-DD HH:MM:SS"）取日期部分，兼容 TPC-C schema 映射
			if len(s) > 10 && s[10] == ' ' {
				s = s[:10]
			}
			if err := validateDate(s); err != nil {
				return zeroValue("DATE")
			}
			return Value{Kind: "DATE", S: s}
		default:
			return zeroValue("DATE")
		}
	case "BLOB":
		switch v.Kind {
		case "BLOB":
			return v
		case "TEXT":
			raw, err := decodeHex(v.S)
			if err != nil {
				return zeroValue("BLOB")
			}
			return Value{Kind: "BLOB", S: encodeHex(raw)}
		default:
			return zeroValue("BLOB")
		}
	default: // TEXT
		switch v.Kind {
		case "TEXT", "DATE":
			return StrVal(v.S)
		case "INT":
			return StrVal(v.String())
		case "DECIMAL":
			return StrVal(v.S)
		case "BLOB":
			return StrVal(v.String()) // hex 文本透传
		default:
			return zeroValue("TEXT")
		}
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
	if v.Kind == "NULL" {
		return "NULL"
	}
	return v.S
}
