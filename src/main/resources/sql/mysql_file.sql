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
  'connector.type' = 'filesystem',
  'connector.path' = 'file:///Users/chenshuai1/github/flink-sql-submit/src/main/resources/player4.csv',
  'format.type' = 'csv',
  'format.fields.0.name' = 'player_id',
  'format.fields.0.type' = 'INT',
  'format.fields.1.name' = 'team_id',
  'format.fields.1.type' = 'INT',
  'format.fields.2.name' = 'player_name',
  'format.fields.2.type' = 'VARCHAR',
  'format.fields.3.name' = 'height',
  'format.fields.3.type' = 'DOUBLE',
  'format.field-delimiter' = ','
);

INSERT INTO player_sink
SELECT
  player_id, team_id, player_name, height
FROM player_source;