--运行报错 AppendStreamTableSink requires that Table has only insert changes.
--原因是KafkaTableSink继承自AppendStreamTableSink，不支持Retraction，而Aggregate是需要Retraction的
--Kafka本身是支持retraction的，在compact topic mode下发送<key,null>墓碑消息就能够将key删除
--如果不用aggregate，运行没有问题

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
    'connector.properties.2.value' = 'testGroup1',
    'update-mode' = 'append',
    'format.type' = 'json',
    'format.derive-schema' = 'true'
);

-- sink
CREATE TABLE pvuv_sink (
    user_id VARCHAR,
    item_id VARCHAR,
    category_id VARCHAR,
    behavior VARCHAR,
    ts TIMESTAMP
) WITH (
    'connector.type' = 'kafka',
    'connector.version' = 'universal',
    'connector.topic' = 'pvuv_sink',
    'connector.startup-mode' = 'earliest-offset',
    'connector.properties.0.key' = 'zookeeper.connect',
    'connector.properties.0.value' = 'localhost:2181',
    'connector.properties.1.key' = 'bootstrap.servers',
    'connector.properties.1.value' = 'localhost:9092',
    'update-mode' = 'append',
    'format.type' = 'json',
    'format.derive-schema' = 'true'
);


INSERT INTO pvuv_sink
SELECT
  user_id, item_id, category_id, behavior, ts
FROM user_log WHERE behavior = 'pv';


---- aggregation sink
--CREATE TABLE pvuv_sink (
--    dt VARCHAR,
--    pv BIGINT,
--    uv BIGINT
--) WITH (
--    'connector.type' = 'kafka',
--    'connector.version' = 'universal',
--    'connector.topic' = 'pvuv_sink',
--    'connector.startup-mode' = 'earliest-offset',
--    'connector.properties.0.key' = 'zookeeper.connect',
--    'connector.properties.0.value' = 'localhost:2181',
--    'connector.properties.1.key' = 'bootstrap.servers',
--    'connector.properties.1.value' = 'localhost:9092',
--    'update-mode' = 'upsert',
--    'format.type' = 'json',
--    'format.derive-schema' = 'true'
--);
--
--
--INSERT INTO pvuv_sink
--SELECT
--  DATE_FORMAT(ts, 'yyyy-MM-dd HH:00') dt,
--  COUNT(*) AS pv,
--  COUNT(DISTINCT user_id) AS uv
--FROM user_log
--GROUP BY DATE_FORMAT(ts, 'yyyy-MM-dd HH:00');