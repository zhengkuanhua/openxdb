-- T38 真实用户验证 · mysqldump 兼容样例（T32 IMPORT MYSQLDUMP FROM）
-- 模拟从 MySQL 导出的 legacy 表（CREATE TABLE + 多行 INSERT）
CREATE TABLE IF NOT EXISTS legacy_customers (
  id INT PRIMARY KEY,
  name TEXT,
  city TEXT
);
INSERT INTO legacy_customers (id, name, city) VALUES
(1, '旧客户甲', '上海'),
(2, '旧客户乙', '北京'),
(3, '旧客户丙', '广州');
