-- source
CREATE TABLE player_source (
  player_id BIGINT,
  team_id BIGINT,
  player_name VARCHAR,
  height DOUBLE
) WITH (
  'connector.type' = 'filesystem',
  'connector.path' = 'file:///Users/chenshuai1/github/flink-sql-submit/src/main/resources/player.csv',
  'format.type' = 'csv',
  'format.fields.0.name' = 'player_id',
  'format.fields.0.type' = 'BIGINT',
  'format.fields.1.name' = 'team_id',
  'format.fields.1.type' = 'BIGINT',
  'format.fields.2.name' = 'player_name',
  'format.fields.2.type' = 'VARCHAR',
  'format.fields.3.name' = 'height',
  'format.fields.3.type' = 'DOUBLE',
  'format.field-delimiter' = ',',
  'format.ignore-first-line' = 'true'
);

-- sink
CREATE TABLE player_sink (
  player_id BIGINT,
  team_id BIGINT,
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

INSERT INTO player_sink
SELECT
  player_id, team_id, player_name, height
FROM player_source;