-- =====================================================================
-- T38 真实用户验证 · 02 事务 ACID
-- 提交持久 / 回滚生效 / 唯一约束 / 外键约束 / 并发可见性
-- =====================================================================

-- 提交持久
BEGIN;
INSERT INTO users (id, name, email, phone, created_at) VALUES (6, 'frank', 'frank@example.com', '13800000006', '2026-10-15');
COMMIT;
SELECT COUNT(*) AS user_cnt FROM users WHERE id = 6;

-- 回滚生效
BEGIN;
INSERT INTO users (id, name, email, phone, created_at) VALUES (7, 'grace', 'grace@example.com', '13800000007', '2026-10-15');
ROLLBACK;
SELECT COUNT(*) AS grace_cnt FROM users WHERE id = 7;

-- 唯一约束拦截（负用例：预期报错）
--@expect-error
INSERT INTO users (id, name, email, phone, created_at) VALUES (8, 'dup', 'alice@example.com', '13800000008', '2026-10-15');

-- 外键约束拦截（负用例：不存在的用户下单）
--@expect-error
INSERT INTO orders (id, user_id, status, total, created_at) VALUES (2001, 9999, 'NEW', 1, '2026-10-15');

-- 外键级联删除（ON DELETE CASCADE：删除订单连带明细）
BEGIN;
DELETE FROM orders WHERE id = 1004;
COMMIT;
SELECT COUNT(*) AS items_left FROM order_items WHERE order_id = 1004;

-- 主键冲突拦截（负用例：重复订单号）
--@expect-error
INSERT INTO orders (id, user_id, status, total, created_at) VALUES (1001, 1, 'PAID', 398, '2026-10-10');
