-- 使用 TableEnvironment

-- source
CREATE TABLE player_source (
  player_id DECIMAL,
  team_id DECIMAL,
  player_name VARCHAR,
  height DECIMAL
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
    'connector.properties.2.value' = 'testGroup',
    'update-mode' = 'append',
    'format.type' = 'json',
    'format.json-schema' =                    -- or by using a JSON schema which parses to DECIMAL and TIMESTAMP
    '{
      "type": "object",
      "properties": {
        "player_id": {
          "type": "number"
        },
        "team_id": {
          "type": "number"
        },
        "player_name": {
          "type": "string"
        },
        "height": {
          "type": "number"
        }
      }
    }'
);

-- json中的number会被解析成Decimal(38,18)，如果不用DECIMAL类型的话就需要进行强制转换

-- sink
CREATE TABLE player_sink (
  player_id BIGINT,
  team_id BIGINT,
  player_name VARCHAR,
  height DOUBLE
) WITH (
  'connector.type' = 'filesystem',
  'connector.path' = 'file:///Users/chenshuai1/github/flink-sql-submit/src/main/resources/player3.csv',
  'format.type' = 'csv',
  'format.fields.0.name' = 'player_id',
  'format.fields.0.type' = 'BIGINT',
  'format.fields.1.name' = 'team_id',
  'format.fields.1.type' = 'BIGINT',
  'format.fields.2.name' = 'player_name',
  'format.fields.2.type' = 'VARCHAR',
  'format.fields.3.name' = 'height',
  'format.fields.3.type' = 'DOUBLE',
  'format.field-delimiter' = ','
);

INSERT INTO player_sink
SELECT
  CAST(player_id as BIGINT) as player_id, CAST(team_id as BIGINT) as team_id, player_name, CAST(height as DOUBLE) as height
FROM player_source;