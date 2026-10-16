-- =====================================================================
-- T38 真实用户验证 · 03 查询分析
-- JOIN / CTE / 窗口函数 / 子查询 / GROUP BY / upsert / EXPLAIN /
-- PREPARE+EXECUTE / @var / information_schema
-- 负用例以 --@expect-error 标记（引擎不支持，验证错误拦截不崩溃）
-- =====================================================================

-- JOIN：订单 + 用户 + 商品
SELECT o.id, u.name, p.name AS product_name, oi.qty, oi.price
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
JOIN users u ON o.user_id = u.id
JOIN products p ON oi.product_id = p.id
ORDER BY o.id;

-- 视图查询
SELECT * FROM v_user_orders ORDER BY id;

-- 物化视图汇总
SELECT * FROM mv_order_summary ORDER BY status;

-- GROUP BY 聚合
SELECT category, COUNT(*) AS cnt, SUM(price) AS amount
FROM products GROUP BY category ORDER BY category;

-- LEFT JOIN 聚合（无 CTE 等价写法，验证多表关联 + 分组）
SELECT u.id, COUNT(o.id) AS order_cnt
FROM users u LEFT JOIN orders o ON o.user_id = u.id
GROUP BY u.id ORDER BY u.id;

-- HAVING 过滤（负用例：不支持 HAVING，见兼容性附录）
--@expect-error
SELECT category, COUNT(*) AS cnt FROM products
GROUP BY category HAVING COUNT(*) > 1 ORDER BY category;

-- LIKE 模糊查询
SELECT name, email FROM users WHERE email LIKE 'alice%' ORDER BY id;

-- IN 列表查询
SELECT id, name FROM products WHERE category IN ('外设', '音频') ORDER BY id;

-- UPDATE 业务变更（订单状态流转；负用例：被引用表 ON UPDATE 受外键保护拒绝，
-- 引擎保守策略，验证错误拦截）
--@expect-error
UPDATE orders SET status = 'COMPLETED' WHERE id = 1001;
SELECT status FROM orders WHERE id = 1001;

-- CTE：每个用户订单数（负用例：不支持 WITH，见兼容性附录）
--@expect-error
WITH user_orders AS (
  SELECT user_id, COUNT(*) AS cnt FROM orders GROUP BY user_id
)
SELECT u.name, COALESCE(uo.cnt, 0) AS order_cnt
FROM users u LEFT JOIN user_orders uo ON u.id = uo.user_id
ORDER BY u.id;

-- 窗口函数：商品按价格排名（支持 OVER 窗口，正用例）
SELECT name, price,
       ROW_NUMBER() OVER (ORDER BY price DESC) AS rn,
       RANK()       OVER (ORDER BY price DESC) AS rk,
       DENSE_RANK() OVER (ORDER BY price DESC) AS drk
FROM products ORDER BY price DESC;

-- 子查询：比平均价贵的商品
SELECT name, price FROM products
WHERE price > (SELECT AVG(price) FROM products) ORDER BY price;

-- 子查询：未下过单的用户（负用例：NOT IN 前缀不支持，见兼容性附录）
--@expect-error
SELECT name FROM users
WHERE id NOT IN (SELECT DISTINCT user_id FROM orders) ORDER BY id;

-- upsert：REPLACE INTO（负用例：unknown command REPLACE）
--@expect-error
REPLACE INTO products (id, sku, name, price, category, created_at)
VALUES (1, 'SKU-001-NEW', '机械键盘 Pro', 329, '外设', '2026-10-15');

-- upsert：ON DUPLICATE KEY UPDATE（负用例：被引用表写保护拒绝）
--@expect-error
INSERT INTO products (id, sku, name, price, category, created_at)
VALUES (1, 'SKU-001-NEW', '机械键盘 Pro Max', 359, '外设', '2026-10-15')
ON DUPLICATE KEY UPDATE name = '机械键盘 Pro Max', price = 359;

-- EXPLAIN 走索引
EXPLAIN SELECT * FROM users WHERE email = 'alice@example.com';

-- 范围查询 + 排序
SELECT id, name, price FROM products
WHERE price BETWEEN 100 AND 1000 ORDER BY price LIMIT 5;

-- PREPARE / EXECUTE（负用例：服务端预备语句不支持）
--@expect-error
PREPARE ps_user FROM 'SELECT id, name FROM users WHERE id = ?';
SET @uid = 2;
--@expect-error
EXECUTE ps_user USING @uid;
--@expect-error
DEALLOCATE PREPARE ps_user;

-- information_schema
SELECT table_name FROM information_schema.tables ORDER BY table_name;
SELECT table_name, column_name FROM information_schema.columns
WHERE table_name = 'orders' ORDER BY column_name;

-- 慢查询观察（触发一条慢阈值内的普通查询即可见）
SET slow_query_threshold = 0;
SELECT COUNT(*) FROM order_items;
