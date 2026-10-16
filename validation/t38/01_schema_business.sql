-- =====================================================================
-- T38 真实用户验证 · 01 业务库 schema
-- mini 电商：用户中心 / 商品 / 库存 / 订单 / 订单明细
-- 配套：视图、物化视图、审计触发器、存储过程、二级/唯一索引
-- 执行方式：openxdb repl --data-dir <dir> < 01_schema_business.sql
--            或经 Python 驱动逐条执行（run_validation.ps1 使用）
-- =====================================================================

-- ---------- 用户中心 ----------
CREATE TABLE users (
  id INT PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL,
  phone TEXT,
  created_at DATE
);
CREATE UNIQUE INDEX ux_users_email ON users (email);

-- ---------- 商品 ----------
CREATE TABLE products (
  id INT PRIMARY KEY,
  sku TEXT NOT NULL,
  name TEXT NOT NULL,
  price DECIMAL NOT NULL,
  category TEXT,
  created_at DATE
);
CREATE INDEX ix_products_category ON products (category);

-- ---------- 库存 ----------
CREATE TABLE inventory (
  product_id INT PRIMARY KEY,
  qty INT NOT NULL,
  updated_at DATE,
  FOREIGN KEY (product_id) REFERENCES products (id)
);

-- ---------- 订单 ----------
CREATE TABLE orders (
  id INT PRIMARY KEY,
  user_id INT NOT NULL,
  status TEXT NOT NULL,
  total DECIMAL NOT NULL,
  created_at DATE,
  FOREIGN KEY (user_id) REFERENCES users (id)
);
CREATE INDEX ix_orders_user ON orders (user_id);
CREATE INDEX ix_orders_status ON orders (status);

-- ---------- 订单明细 ----------
CREATE TABLE order_items (
  item_id INT PRIMARY KEY,
  order_id INT NOT NULL,
  product_id INT NOT NULL,
  qty INT NOT NULL,
  price DECIMAL NOT NULL,
  FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE CASCADE,
  FOREIGN KEY (product_id) REFERENCES products (id)
);
CREATE INDEX ix_items_order ON order_items (order_id);
CREATE INDEX ix_items_product ON order_items (product_id);

-- ---------- 审计日志（触发器写入） ----------
CREATE TABLE audit_log (
  id INT PRIMARY KEY,
  msg TEXT NOT NULL,
  created_at DATE
);

-- ---------- 视图：用户订单 ----------
CREATE VIEW v_user_orders AS
SELECT o.id, u.name, o.status, o.total, o.created_at
FROM orders o JOIN users u ON o.user_id = u.id;

-- ---------- 物化视图：订单汇总 ----------
CREATE MATERIALIZED VIEW mv_order_summary AS
SELECT status, COUNT(*) AS cnt, SUM(total) AS amount
FROM orders GROUP BY status;

-- ---------- 触发器：下单写审计 ----------
CREATE TRIGGER tr_orders_audit AFTER INSERT ON orders FOR EACH ROW
UPDATE audit_log SET msg = 'order inserted';

-- ---------- 存储过程：占位下单 ----------
CREATE PROCEDURE p_place_order BEGIN
INSERT INTO orders (id, user_id, status, total, created_at) VALUES (9001, 1, 'NEW', 0, '2026-10-15');
END;

-- ---------- 初始业务数据 ----------
INSERT INTO users (id, name, email, phone, created_at) VALUES
(1, 'alice', 'alice@example.com', '13800000001', '2026-01-01'),
(2, 'bob',   'bob@example.com',   '13800000002', '2026-01-02'),
(3, 'carol', 'carol@example.com', '13800000003', '2026-01-03'),
(4, 'dave',  'dave@example.com',  '13800000004', '2026-01-04'),
(5, 'erin',  'erin@example.com',  '13800000005', '2026-01-05');

INSERT INTO products (id, sku, name, price, category, created_at) VALUES
(1, 'SKU-001', '机械键盘', 299, '外设', '2026-02-01'),
(2, 'SKU-002', '无线鼠标', 99, '外设', '2026-02-01'),
(3, 'SKU-003', '27寸显示器', 1299, '显示', '2026-02-02'),
(4, 'SKU-004', 'USB-C 扩展坞', 159, '配件', '2026-02-02'),
(5, 'SKU-005', '人体工学椅', 899, '家具', '2026-02-03'),
(6, 'SKU-006', '降噪耳机', 499, '音频', '2026-02-03'),
(7, 'SKU-007', '摄像头', 249, '外设', '2026-02-04'),
(8, 'SKU-008', '桌面支架', 69, '配件', '2026-02-04');

INSERT INTO inventory (product_id, qty, updated_at) VALUES
(1, 100, '2026-10-01'), (2, 200, '2026-10-01'), (3, 50, '2026-10-01'),
(4, 300, '2026-10-01'), (5, 40, '2026-10-01'), (6, 120, '2026-10-01'),
(7, 80, '2026-10-01'), (8, 500, '2026-10-01');

INSERT INTO audit_log (id, msg, created_at) VALUES (0, 'schema ready', '2026-10-15');

-- 下单闭环：订单 + 明细
INSERT INTO orders (id, user_id, status, total, created_at) VALUES
(1001, 1, 'PAID', 398, '2026-10-10'),
(1002, 2, 'PAID', 159, '2026-10-11'),
(1003, 1, 'SHIPPED', 1299, '2026-10-12'),
(1004, 3, 'PAID', 69, '2026-10-13');

INSERT INTO order_items (item_id, order_id, product_id, qty, price) VALUES
(1, 1001, 1, 1, 299), (2, 1001, 2, 1, 99),
(3, 1002, 4, 1, 159),
(4, 1003, 3, 1, 1299),
(5, 1004, 8, 1, 69);

-- 刷新物化视图（当前快照）
REFRESH MATERIALIZED VIEW mv_order_summary;
