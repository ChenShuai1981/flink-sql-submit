-- source
CREATE TABLE player_source (
  player_id VARCHAR,
  team_id VARCHAR,
  player_name VARCHAR,
  height VARCHAR
) WITH (
  'connector.type' = 'filesystem',
  'connector.path' = 'file:///Users/chenshuai1/github/flink-sql-submit/src/main/resources/player.csv',
  'format.type' = 'csv',
  'format.fields.0.name' = 'player_id',
  'format.fields.0.type' = 'VARCHAR',
  'format.fields.1.name' = 'team_id',
  'format.fields.1.type' = 'VARCHAR',
  'format.fields.2.name' = 'player_name',
  'format.fields.2.type' = 'VARCHAR',
  'format.fields.3.name' = 'height',
  'format.fields.3.type' = 'VARCHAR',
  'format.field-delimiter' = ',',
  'format.ignore-first-line' = 'true'
);

-- sink
CREATE TABLE player_sink (
  rowkey VARCHAR,
  cf ROW<player_id VARCHAR, team_id VARCHAR, height VARCHAR>
) WITH (
    'connector.type' = 'hbase',
    'connector.version' = '1.4.3',
    'connector.table-name' = 'player',
    'connector.property-version' = '1',
    'connector.zookeeper.quorum' = 'localhost:2181',
    'connector.zookeeper.znode.parent' = '/hbase'
);

INSERT INTO player_sink
SELECT
  player_name AS rowkey, ROW(player_id, team_id, height) AS cf
FROM player_source;