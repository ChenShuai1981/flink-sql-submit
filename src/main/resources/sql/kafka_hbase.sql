-- 开启 mini-batch
SET table.exec.mini-batch.enabled=true;
-- mini-batch的时间间隔，即作业需要额外忍受的延迟
SET table.exec.mini-batch.allow-latency=1s;
-- 一个 mini-batch 中允许最多缓存的数据
SET table.exec.mini-batch.size=1000;
-- 开启 local-global 优化
SET table.optimizer.agg-phase-strategy=TWO_PHASE;
-- 开启 distinct agg 切分
SET table.optimizer.distinct-agg.split.enabled=true;

-- source
CREATE TABLE user_log (
    user_id VARCHAR,
    item_id VARCHAR,
    category_id VARCHAR,
    behavior VARCHAR,
    ts TIMESTAMP
) WITH (
    'connector.type' = 'kafka',
    'connector.version' = 'universal',
    'connector.topic' = 'user_behavior',
    'connector.startup-mode' = 'earliest-offset',
    'connector.properties.0.key' = 'zookeeper.connect',
    'connector.properties.0.value' = 'localhost:2181',
    'connector.properties.1.key' = 'bootstrap.servers',
    'connector.properties.1.value' = 'localhost:9092',
    'connector.properties.2.key' = 'group.id',
    'connector.properties.2.value' = 'testGroup',
    'update-mode' = 'append',
    'format.type' = 'json',
    'format.derive-schema' = 'true'
);

-- sink
CREATE TABLE pvuv_sink (
    rowkey VARCHAR,
    cf ROW<pv INT, uv INT>
) WITH (
    'connector.type' = 'hbase',
    'connector.version' = '1.4.3',
    'connector.table-name' = 'pvuv',
    'connector.property-version' = '1',
    'connector.zookeeper.quorum' = 'localhost:2181',
    'connector.zookeeper.znode.parent' = '/hbase'
);

CREATE VIEW stats_view AS
SELECT
  DATE_FORMAT(ts, 'yyyy-MM-dd HH:00') AS dt,
  COUNT(*) AS pv,
  COUNT(DISTINCT user_id) AS uv
FROM user_log
GROUP BY DATE_FORMAT(ts, 'yyyy-MM-dd HH:00');

INSERT INTO pvuv_sink
SELECT
  dt AS rowkey,
  ROW(pv, uv) AS cf
FROM stats_view;