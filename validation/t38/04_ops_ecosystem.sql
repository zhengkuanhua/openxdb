-- =====================================================================
-- T38 真实用户验证 · 04 运维与生态
-- 状态/统计/指标、分片元数据、导出、备份恢复、CDC 观察、
-- 告警规则、审计日志、RBAC、配置
-- 注：导入(mysqldump)、CDC 订阅、TLS、doctor/stats CLI 由编排脚本
--     run_validation.ps1 负责，此处为服务内 SQL 视角。
-- 负用例以 --@expect-error 标记（引擎不支持，验证错误拦截不崩溃）
-- =====================================================================

-- 状态与统计（SHOW STATUS 不支持，标负用例）
--@expect-error
SHOW STATUS;
SHOW STATS;
SHOW CONFIG;
SHOW SLOWQUERIES;

-- 表/索引/视图/触发器/过程
SHOW TABLES;
SHOW INDEX FROM orders;
SHOW VIEWS;
SHOW TRIGGERS;
SHOW PROCEDURES;

-- 分片元数据（单机零触发路径；LIST/LOCATE 不支持，标负用例）
SHOW NODES;
SHOW REGION ROUTES;
--@expect-error
LIST REGIONS;
--@expect-error
LOCATE REGION 1;

-- 手动分裂：把 products 按数据中点一分为二（负用例：不支持 SPLIT）
--@expect-error
SPLIT REGION 1;
SELECT COUNT(*) FROM products;

-- 备份（逻辑快照；路径与编排脚本 $bakPath 一致，供 PITR/恢复验证）
BACKUP TO 'C:\Users\27756\AppData\Roaming\Tencent\Marvis\User\oAN1i2RORm9ewWWUXrcT2XBfRk0c\workspace\conv_5d15f4c8d8ec46778266acaa06c35c33\temp\openxdb-t38\t38-backup.bak';

-- 导出
EXPORT TABLE products TO 'C:\Users\27756\AppData\Roaming\Tencent\Marvis\User\oAN1i2RORm9ewWWUXrcT2XBfRk0c\workspace\conv_5d15f4c8d8ec46778266acaa06c35c33\temp\openxdb-t38\products_export.json' FORMAT JSON;
EXPORT TABLE products TO 'C:\Users\27756\AppData\Roaming\Tencent\Marvis\User\oAN1i2RORm9ewWWUXrcT2XBfRk0c\workspace\conv_5d15f4c8d8ec46778266acaa06c35c33\temp\openxdb-t38\products_export.parquet' FORMAT PARQUET;
EXPORT TABLE products TO 'C:\Users\27756\AppData\Roaming\Tencent\Marvis\User\oAN1i2RORm9ewWWUXrcT2XBfRk0c\workspace\conv_5d15f4c8d8ec46778266acaa06c35c33\temp\openxdb-t38\products_export.csv';

-- CDC 观察：binlog 事件（需 --replica-port 启动）
SHOW BINLOG EVENTS LIMIT 5;

-- 告警规则（T37）
SET alert_rules_enabled = on;
SET alert_eval_interval = 1;
SET alert_rules = '[{"id":"r1","metric":"sql_qps","op":">","threshold":100000,"duration_sec":0,"severity":"warn","enabled":true}]';
SHOW ALERT RULES;
SHOW ALERT STATUS;

-- 审计日志（T30；SHOW AUDITLOG 不支持，标负用例；触发器审计经 audit_log 表验证）
--@expect-error
SHOW AUDITLOG LIMIT 5;

-- RBAC：角色与授权（T24/T30；GRANT 角色不支持，仅支持直接特权，标负用例）
CREATE USER 'analyst' IDENTIFIED BY 'pw_analyst';
CREATE ROLE 'analyst_role';
GRANT SELECT ON products TO 'analyst';
--@expect-error
GRANT analyst_role TO 'analyst';
SHOW GRANTS FOR 'analyst';
