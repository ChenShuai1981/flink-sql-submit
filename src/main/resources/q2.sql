---- 开启 mini-batch
--SET table.exec.mini-batch.enabled=true;
---- mini-batch的时间间隔，即作业需要额外忍受的延迟
--SET table.exec.mini-batch.allow-latency=1s;
---- 一个 mini-batch 中允许最多缓存的数据
--SET table.exec.mini-batch.size=1000;
---- 开启 local-global 优化
--SET table.optimizer.agg-phase-strategy=TWO_PHASE;
---- 开启 distinct agg 切分
--SET table.optimizer.distinct-agg.split.enabled=true;

-- source
CREATE TABLE user_log (
    user_id INTEGER,
    item_id INTEGER,
    category_id INTEGER,
    behavior VARCHAR,
    ts TIMESTAMP
) WITH (
    'connector.type' = 'kafka',
    'connector.version' = 'universal',
    'connector.topic' = 'user_log',
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

-- lookup
CREATE TABLE user_info (
    user_id INTEGER,
    username VARCHAR,
    sex INTEGER,
    age INTEGER,
    registertime TIMESTAMP
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' = 'jdbc:mysql://127.0.0.1:3306/test?useSSL=false',
    'connector.driver' = 'com.mysql.jdbc.Driver',
    'connector.username' = 'root',
    'connector.password' = 'root',
    'connector.table' = 'user'
);

-- sink
CREATE TABLE lookup_sink (
    user_id INTEGER,
    item_id INTEGER,
    category_id INTEGER,
    behavior VARCHAR,
    ts TIMESTAMP,
    username VARCHAR,
    sex INTEGER,
    age INTEGER,
    registertime TIMESTAMP
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' = 'jdbc:mysql://localhost:3306/test?useSSL=false',
    'connector.table' = 'lookup_sink',
    'connector.username' = 'root',
    'connector.password' = 'root',
    'connector.write.flush.max-rows' = '1'
);


INSERT INTO lookup_sink
SELECT
  user_log.user_id,
  item_id,
  category_id,
  behavior,
  ts,
  username,
  sex,
  age,
  registertime
FROM user_log
JOIN user_info
ON user_log.user_id = user_info.user_id;