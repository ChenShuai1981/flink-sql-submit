package com.github.wuchong.sqlsubmit;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableFunction;

// The generic type "Tuple2<String, Integer>" determines the schema of the returned table as (String, Integer).
public class StringWithLength extends TableFunction<Tuple2<String, Integer>> {
    public void eval(String str) {
        collect(new Tuple2<String, Integer>(str, str.length()));
    }
}
