/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.wuchong.sqlsubmit;

import com.github.wuchong.sqlsubmit.cli.CliOptions;
import com.github.wuchong.sqlsubmit.cli.CliOptionsParser;
import com.github.wuchong.sqlsubmit.cli.SqlCommandParser;
import com.github.wuchong.sqlsubmit.cli.SqlCommandParser.SqlCommandCall;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class SqlSubmit {

    public static void main(String[] args) throws Exception {
        final CliOptions options = CliOptionsParser.parseClient(args);
        SqlSubmit submit = new SqlSubmit(options);
        submit.run();
    }

    // --------------------------------------------------------------------------------------------

    private String sqlFilePath;
    private String execOptionFilePath;
    private TableEnvironment tEnv;
//    private BatchTableEnvironment tEnv;
//    private StreamTableEnvironment tEnv;
    private ObjectMapper objectMapper = new ObjectMapper();

    private SqlSubmit(CliOptions options) {
        this.sqlFilePath = options.getSqlFilePath();
        this.execOptionFilePath = options.getExecOptionFilePath();
    }

    private void run() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        this.tEnv = TableEnvironment.create(settings);
        if (this.execOptionFilePath != null) {
            String jsonString = new String(Files.readAllBytes(Paths.get(this.execOptionFilePath)));
            JsonNode jsonNode = objectMapper.readTree(jsonString);
            Iterator<String> fieldNames = jsonNode.fieldNames();
            Configuration configuration = this.tEnv.getConfig().getConfiguration();
            while(fieldNames.hasNext()){
                String fieldName = fieldNames.next();
                JsonNode fieldValue = jsonNode.get(fieldName);
                if (fieldValue instanceof IntNode) {
                    configuration.setInteger(fieldName, fieldValue.asInt());
                } else if (fieldValue instanceof BooleanNode) {
                    configuration.setBoolean(fieldName, fieldValue.asBoolean());
                } else if (fieldValue instanceof LongNode) {
                    configuration.setLong(fieldName, fieldValue.asLong());
                } else if (fieldValue instanceof DoubleNode) {
                    configuration.setDouble(fieldName, fieldValue.asDouble());
                } else if (fieldValue instanceof TextNode) {
                    configuration.setString(fieldName, fieldValue.asText());
                }
            }
        }

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        this.tEnv = StreamTableEnvironment.create(env, settings);

//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        this.tEnv = BatchTableEnvironment.create(env);

        List<String> sql = Files.readAllLines(Paths.get(sqlFilePath));
        List<SqlCommandCall> calls = SqlCommandParser.parse(sql);
        for (SqlCommandCall call : calls) {
            callCommand(call);
        }
        tEnv.execute("SQL Job");
    }

    // --------------------------------------------------------------------------------------------

    private void callCommand(SqlCommandCall cmdCall) {
        switch (cmdCall.command) {
            case SET:
                callSet(cmdCall);
                break;
            case CREATE_TABLE:
                callCreateTable(cmdCall);
                break;
            case INSERT_INTO:
                callInsertInto(cmdCall);
                break;
            case CREATE_FUNCTION:
                callCreateFunction(cmdCall);
                break;
            case CREATE_VIEW:
                callCreateView(cmdCall);
                break;
            default:
                throw new RuntimeException("Unsupported command: " + cmdCall.command);
        }
    }

    private void callSet(SqlCommandCall cmdCall) {
        String key = cmdCall.operands[0];
        String value = cmdCall.operands[1];
        tEnv.getConfig().getConfiguration().setString(key, value);
    }

    private void callCreateFunction(SqlCommandCall cmdCall) {
        String functionName = cmdCall.operands[0];
        String functionClassName = cmdCall.operands[1];
        try {
            Class clazz = Class.forName(functionClassName);
            Object instance = clazz.newInstance();

            if (instance instanceof ScalarFunction) {
                tEnv.registerFunction(functionName, (ScalarFunction)instance);
            }
//            else if (instance instanceof AggregateFunction) {
//                tEnv.registerFunction(functionName, (AggregateFunction)instance);
//            } else if (instance instanceof TableAggregateFunction) {
//                tEnv.registerFunction(functionName, (TableAggregateFunction)instance);
//            } else if (instance instanceof TableFunction) {
//                tEnv.registerFunction(functionName, (TableFunction)instance);
//            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }
    }

    private void callCreateTable(SqlCommandCall cmdCall) {
        String ddl = cmdCall.operands[0];
        try {
            tEnv.sqlUpdate(ddl);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
    }

    private void callCreateView(SqlCommandCall cmdCall) {
        String ddl = cmdCall.operands[0];
        try {
            tEnv.sqlUpdate(ddl);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
    }

    private void callInsertInto(SqlCommandCall cmdCall) {
        String dml = cmdCall.operands[0];
        try {
            tEnv.sqlUpdate(dml);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + dml + "\n", e);
        }
    }
}
