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

--CREATE TABLE `player_stats` (
--  `team_id` int(11) NOT NULL,
--  `avg_height` double DEFAULT NULL,
--  PRIMARY KEY (`team_id`)
--);
-- 一定要定义PRIMARY KEY

-- sink
CREATE TABLE player_sink (
  team_id INT,
  avg_height DOUBLE
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' = 'jdbc:mysql://localhost:3306/flink-test?useUnicode=true&characterEncoding=UTF-8',
    'connector.table' = 'player_stats',
    'connector.username' = 'root',
    'connector.password' = 'root',
    'connector.write.flush.max-rows' = '1'
);

INSERT INTO player_sink
SELECT
  team_id, avg(height) as avg_height
FROM player_source
GROUP BY team_id;