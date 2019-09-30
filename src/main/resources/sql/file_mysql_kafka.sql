-- source
CREATE TABLE player_source (
  player_id INT,
  team_id INT,
  player_name VARCHAR,
  height DOUBLE
) WITH (
  'connector.type' = 'filesystem',
  'connector.path' = 'file:///Users/chenshuai1/github/flink-sql-submit/src/main/resources/player.csv',
  'format.type' = 'csv',
  'format.fields.0.name' = 'player_id',
  'format.fields.0.type' = 'INT',
  'format.fields.1.name' = 'team_id',
  'format.fields.1.type' = 'INT',
  'format.fields.2.name' = 'player_name',
  'format.fields.2.type' = 'VARCHAR',
  'format.fields.3.name' = 'height',
  'format.fields.3.type' = 'DOUBLE',
  'format.field-delimiter' = ',',
  'format.ignore-first-line' = 'true'
);

-- 因为MySQL表中的int(11)会被flink sql解析成INT，所以这里统一使用INT表示id
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
  team_name VARCHAR,
  height DOUBLE
) WITH (
    'connector.type' = 'kafka',
    'connector.version' = 'universal',
    'connector.topic' = 'enriched_player',
    'connector.startup-mode' = 'earliest-offset',
    'connector.properties.0.key' = 'zookeeper.connect',
    'connector.properties.0.value' = 'localhost:2181',
    'connector.properties.1.key' = 'bootstrap.servers',
    'connector.properties.1.value' = 'localhost:9092',
    'connector.properties.2.key' = 'group.id',
    'connector.properties.2.value' = 'testGroup',
    'update-mode' = 'append',
    'format.type' = 'json',
    'format.json-schema' =                    -- or by using a JSON schema which parses to DECIMAL and TIMESTAMP
    '{
      "type": "object",
      "properties": {
        "player_name": {
          "type": "string"
        },
        "team_name": {
          "type": "string"
        },
        "height": {
          "type": "number"
        }
      }
    }'
);

INSERT INTO enriched_player_sink
SELECT
  player_name, team_name, height
FROM player_source AS p
JOIN team_dim AS t
ON p.team_id = t.team_id;