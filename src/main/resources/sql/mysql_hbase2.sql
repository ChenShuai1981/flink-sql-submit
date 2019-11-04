-- source
CREATE TABLE team_source (
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

-- mysql表中INT(11)类型会被解析成INT

-- sink
CREATE TABLE team_sink (
  rowkey INT,
  cf ROW<team_name VARCHAR>
) WITH (
    'connector.type' = 'hbase',
    'connector.version' = '1.4.3',
    'connector.table-name' = 'team',
    'connector.property-version' = '1',
    'connector.zookeeper.quorum' = 'localhost:2181',
    'connector.zookeeper.znode.parent' = '/hbase'
);

INSERT INTO team_sink
SELECT
  team_id AS rowkey,
  ROW(team_name) AS cf
FROM team_source;