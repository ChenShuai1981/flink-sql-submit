package com.github.wuchong.sqlsubmit;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.*;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

import java.util.Collections;

public class JDBCAppendTableSinkDemo {
    private static final String[] FIELD_NAMES = new String[]{"id"};
    private static final TypeInformation[] FIELD_TYPES = new TypeInformation[]{
            BasicTypeInfo.STRING_TYPE_INFO
    };
    private static final RowTypeInfo ROW_TYPE = new RowTypeInfo(FIELD_TYPES, FIELD_NAMES);

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://127.0.0.1:3306/flink-test")
                .setUsername("root")
                .setPassword("root")
                .setQuery("INSERT INTO foo (id) VALUES (?)")
                .setParameterTypes(STRING_TYPE_INFO)
                .build();

        DataSet<Row> ds = env.fromCollection(Collections.singleton(Row.of("bar")), ROW_TYPE);

        sink.emitDataSet(ds);

        env.execute("JDBCAppendTableSinkDemo");
    }
}
