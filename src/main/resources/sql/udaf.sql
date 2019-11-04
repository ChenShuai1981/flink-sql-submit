-- https://www.alibabacloud.com/help/doc-detail/69559.htm?spm=a2c63.p38356.b99.76.49f2476cuwi3jN
-- 必须使用BatchTableEnvironment才能注册TableFunction
CREATE FUNCTION wAvg AS 'com.github.wuchong.sqlsubmit.WeightedAvg';

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
  team_id BIGINT,
  avg_height DOUBLE
) WITH (
  'connector.type' = 'filesystem',
  'connector.path' = 'file:///Users/chenshuai1/github/flink-sql-submit/src/main/resources/player6.csv',
  'format.type' = 'csv',
  'format.fields.1.name' = 'team_id',
  'format.fields.1.type' = 'BIGINT',
  'format.fields.4.name' = 'avg_height',
  'format.fields.4.type' = 'DOUBLE',
  'format.field-delimiter' = ','
);

INSERT INTO player_sink
SELECT
  team_id, wAvg(height) AS avg_height
FROM player_source
GROUP BY team_id;