-- source
CREATE TABLE player_source (
  player_id INT,
  team_id INT,
  player_name VARCHAR,
  height DOUBLE
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' = 'jdbc:mysql://localhost:3306/flink-test?useUnicode=true&characterEncoding=UTF-8',
    'connector.table' = 'player',
    'connector.username' = 'root',
    'connector.password' = 'root',
    'connector.write.flush.max-rows' = '1'
);

-- mysql表中INT(11)类型会被解析成INT

-- sink
CREATE TABLE player_sink (
  player_id BIGINT,
  team_id BIGINT,
  player_name VARCHAR,
  height DOUBLE
) WITH (
    'connector.type' = 'elasticsearch',
    'connector.version' = '6',
    'connector.hosts.0.hostname' = 'localhost',  -- required: one or more Elasticsearch hosts to connect to
    'connector.hosts.0.port' = '9200',
    'connector.hosts.0.protocol' = 'http',
    'connector.index' = 'player',       -- required: Elasticsearch index
    'connector.document-type' = '_doc',
    'update-mode' = 'append',
    'format.type' = 'json',
    'format.derive-schema' = 'true'
);

INSERT INTO player_sink
SELECT
  CAST(player_id AS BIGINT) AS player_id, CAST(team_id AS BIGINT) AS team_id, player_name, height
FROM player_source;