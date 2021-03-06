-- 使用 TableEnvironment

-- source
CREATE TABLE player_source (
  player_id INT,
  team_id INT,
  player_name VARCHAR,
  height DOUBLE
) WITH (
    'connector.type' = 'kafka',
    'connector.version' = 'universal',
    'connector.topic' = 'player',
    'connector.startup-mode' = 'earliest-offset',
    'connector.properties.0.key' = 'zookeeper.connect',
    'connector.properties.0.value' = 'localhost:2181',
    'connector.properties.1.key' = 'bootstrap.servers',
    'connector.properties.1.value' = 'localhost:9092',
    'connector.properties.2.key' = 'group.id',
    'connector.properties.2.value' = 'kafka_mysql_es2',
    'update-mode' = 'append',
    'format.type' = 'json',
    'format.derive-schema' = 'true'
);

-- dimension
CREATE TABLE team_dim (
  team_id INT,
  team_name VARCHAR
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' = 'jdbc:mysql://localhost:3306/flink-test?useUnicode=true&characterEncoding=UTF-8',
    'connector.table' = 'team',
    'connector.username' = 'root',
    'connector.password' = 'root',
    'connector.write.flush.max-rows' = '1'
);

-- sink
CREATE TABLE enriched_player_sink (
  player_name VARCHAR,
  team_name VARCHAR
) WITH (
    'connector.type' = 'elasticsearch',
    'connector.version' = '6',
    'connector.hosts.0.hostname' = 'localhost',  -- required: one or more Elasticsearch hosts to connect to
    'connector.hosts.0.port' = '9200',
    'connector.hosts.0.protocol' = 'http',
    'connector.index' = 'enriched_player',       -- required: Elasticsearch index
    'connector.document-type' = '_doc',
    'update-mode' = 'append',
    'format.type' = 'json',
    'format.derive-schema' = 'true'
);

INSERT INTO enriched_player_sink
SELECT
  player_name, team_name
FROM player_source AS p
JOIN team_dim AS t
ON p.team_id = t.team_id;