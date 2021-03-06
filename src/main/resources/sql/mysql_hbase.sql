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
  rowkey INT,
  cf ROW<team_id INT, player_name VARCHAR, height DOUBLE>
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
  player_id AS rowkey,
  ROW(team_id, player_name, height) AS cf
FROM player_source;