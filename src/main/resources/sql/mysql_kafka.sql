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

INSERT INTO player_sink
SELECT
  player_id, team_id, player_name, height
FROM player_source;