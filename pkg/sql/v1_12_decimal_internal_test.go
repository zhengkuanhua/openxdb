package sql

import (
	"testing"
)

// v1.12.0 内部函数：JSON 定点输出（不再 ParseFloat 近似）。
func TestV112DecJSONNumber(t *testing.T) {
	cases := []struct {
		canon string
		want  string
	}{
		{"123.456789", "123.456789"},
		{"123.456790", "123.456790"},
		{"-0.000001", "-0.000001"},
		{"1.500000", "1.500000"},
		{"1.2345", "1.2345"},
		{"0.5000", "0.5000"},
	}
	for _, c := range cases {
		if got := decJSONNumber(c.canon); got != c.want {
			t.Fatalf("decJSONNumber(%q): want %s got %s", c.canon, c.want, got)
		}
	}
}

// v1.12.0 内部函数：存储过程 DECIMAL 跨 scale 比较与截断。
func TestV112DecimalProcInternal(t *testing.T) {
	if cmp, err := procCompareValue(decValScaled(1234567, 6), IntVal(1), ">"); err != nil || cmp.I != 1 {
		t.Fatalf("procCompareValue dec6 vs int: got %+v err %v", cmp, err)
	}
	if cmp, err := procCompareValue(decValScaled(1234567, 6), decValScaled(12345, 4), ">"); err != nil || cmp.I != 1 {
		t.Fatalf("procCompareValue dec6 vs dec4: got %+v err %v", cmp, err)
	}
	if cmp, err := procCompareValue(decValScaled(-1, 6), IntVal(0), ">"); err != nil || cmp.I != 0 {
		t.Fatalf("procCompareValue -0.000001 vs 0: got %+v err %v", cmp, err)
	}
	if n, ok := procToInt(decValScaled(1234567, 6)); !ok || n != 1 {
		t.Fatalf("procToInt(1.234567): got %d %v", n, ok)
	}
	if n, ok := procToInt(decValScaled(-1234567, 6)); !ok || n != -1 {
		t.Fatalf("procToInt(-1.234567): got %d %v", n, ok)
	}
}

// v1.12.0 内部函数：字面量小数位识别。
func TestV112DecimalLiteralScale(t *testing.T) {
	cases := []struct {
		text string
		want int
	}{
		{"123.456789", 6},
		{"1.50", 2},
		{"10.5", 1},
		{"-0.000001", 6},
		{"1.2345", 4},
	}
	for _, c := range cases {
		if got := decimalLiteralScale(c.text); got != c.want {
			t.Fatalf("decimalLiteralScale(%q): want %d got %d", c.text, c.want, got)
		}
	}
}
