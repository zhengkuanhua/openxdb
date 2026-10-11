package sql

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/zhengkuanhua/openxdb/pkg/storage"
)

// UserMeta 用户元数据（M11 用户管理）。
// 密码绝不落明文：SaltHash 保存 "saltHex$sha256Hex"，
// 其中 salt 为 16 字节随机值（hex），hash = SHA-256(saltHex + password)（hex）。
type UserMeta struct {
	Name     string `json:"name"`
	SaltHash string `json:"salt_hash"` // "saltHex$sha256Hex"
}

// usersMetaKey 用户目录键（全局元数据，与 m:tables 同级）。
var usersMetaKey = []byte("m:users")

// loadUsers 读取用户目录（无目录时返回空）。
func loadUsers(get func(key []byte) ([]byte, error)) ([]*UserMeta, error) {
	raw, err := get(usersMetaKey)
	if err != nil {
		if err == storage.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	if raw == nil || len(raw) == 0 {
		return nil, nil
	}
	var users []*UserMeta
	if err := json.Unmarshal(raw, &users); err != nil {
		return nil, &SQLError{Msg: "corrupted user catalog"}
	}
	return users, nil
}

// saveUsers 序列化用户目录。
func saveUsers(users []*UserMeta) ([]byte, error) {
	return json.Marshal(users)
}

// findUser 按名查找用户（大小写敏感）。
func findUser(users []*UserMeta, name string) *UserMeta {
	for _, u := range users {
		if u.Name == name {
			return u
		}
	}
	return nil
}

// hashPassword 生成密码哈希 "saltHex$sha256Hex"：salt 16 字节随机（crypto/rand），
// hash = SHA-256(saltHex + password)。salt 每次随机，同一密码产生不同哈希。
func hashPassword(password string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	saltHex := hex.EncodeToString(salt)
	sum := sha256.Sum256(append([]byte(saltHex), []byte(password)...))
	return saltHex + "$" + hex.EncodeToString(sum[:]), nil
}

// verifyPassword 校验密码与 "saltHex$sha256Hex" 是否匹配（常数时间比较，防时序侧信道）。
func verifyPassword(password, saltHash string) bool {
	i := strings.IndexByte(saltHash, '$')
	if i <= 0 || i == len(saltHash)-1 {
		return false
	}
	saltHex := saltHash[:i]
	want := saltHash[i+1:]
	sum := sha256.Sum256(append([]byte(saltHex), []byte(password)...))
	got := hex.EncodeToString(sum[:])
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

// authenticateUser 校验用户名与密码（供 server 层连接鉴权握手使用）。
// 用户不存在或密码不匹配返回 (false, nil)；存储层错误原样返回。
func (e *Executor) authenticateUser(name, password string) (bool, error) {
	tx, err := e.tm.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	users, err := loadUsers(tx.Get)
	if err != nil {
		return false, err
	}
	u := findUser(users, name)
	if u == nil {
		return false, nil
	}
	return verifyPassword(password, u.SaltHash), nil
}
