package com.github.wuchong.sqlsubmit;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.*;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;

public class BatchSqlDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        CsvTableSource source = CsvTableSource.builder()
                .ignoreFirstLine()
                .path("file:///Users/chenshuai1/github/flink-sql-submit/src/main/resources/player.csv")
                .fieldDelimiter(",")
                .field("player_id", Types.INT)
                .field("team_id", Types.INT)
                .field("player_name", Types.STRING)
                .field("height", Types.DOUBLE).build();
        Table table = tableEnv.fromTableSource(source);
        JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://127.0.0.1:3306/flink-test?useUnicode=true&characterEncoding=UTF-8")
                .setUsername("xxx")
                .setPassword("xxx")
                .setQuery("INSERT INTO player (player_id, team_id, player_name, height) VALUES (?, ?, ?, ?)")
                .setParameterTypes(INT_TYPE_INFO, INT_TYPE_INFO, STRING_TYPE_INFO, DOUBLE_TYPE_INFO).build();
        tableEnv.registerTableSink("jdbcOutputTable",
                new String[]{"player_id", "team_id", "player_name", "height"},
                new TypeInformation[]{Types.INT, Types.INT, Types.STRING, Types.DOUBLE},
                sink);
        table.insertInto("jdbcOutputTable");
        env.execute("BatchSqlDemo");
    }
}
