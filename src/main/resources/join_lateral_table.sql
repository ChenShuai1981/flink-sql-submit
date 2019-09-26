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
CREATE TABLE userTab (
    data VARCHAR
) WITH (
    'connector.type' = 'kafka',
    'connector.version' = 'universal',
    'connector.topic' = 'simple_user3',
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
CREATE TABLE user_sink (
    data VARCHAR,
    name VARCHAR,
    age INT
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' = 'jdbc:mysql://localhost:3306/test',
    'connector.table' = 'user_sink',
    'connector.username' = 'root',
    'connector.password' = 'root',
    'connector.write.flush.max-rows' = '1'
);


INSERT INTO user_sink SELECT u.data, r.name, r.age FROM userTab AS u, LATERAL TABLE(splitTVF(u.data)) AS r;