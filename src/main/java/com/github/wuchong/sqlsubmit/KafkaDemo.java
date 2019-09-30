package com.github.wuchong.sqlsubmit;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.TypeInformationKeyValueSerializationSchema;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class KafkaDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        TableDescriptor testDesc = tableEnv.connect(new Kafka()
                .version("universal")
                .topic("user_behavior")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092")
                .property("group.id", "testGroup3")
                .startFromEarliest()
        ).withFormat(
                new Json().failOnMissingField(true)
                        .jsonSchema("{" +
                                "  type: 'object'," +
                                "  properties: {" +
                                "    user_id: {" +
                                "      type: 'string'" +
                                "    }," +
                                "    item_id: {" +
                                "      type: 'string'" +
                                "    }," +
                                "    category_id: {" +
                                "      type: 'string'" +
                                "    }," +
                                "    behavior: {" +
                                "      type: 'string'" +
                                "    }," +
                                "    ts: {" +
                                "      type: 'string'," +
                                "      format: 'date-time'" +
                                "    }" +
                                "  }" +
                                "}")
        ).withSchema(new Schema()
                .field("user_id", Types.STRING)     // required: specify the fields of the table (in this order)
                .field("item_id", Types.STRING)
                .field("category_id", Types.STRING)
                .field("behavior", Types.STRING)
                .field("ts", Types.SQL_TIMESTAMP)
        ).inAppendMode();

        final Map<String, String> propertiesMap = testDesc.toProperties();
        final TableSource<?> actualSource = TableFactoryService.find(StreamTableSourceFactory.class, propertiesMap)
                .createStreamTableSource(propertiesMap);

        final KafkaTableSourceBase actualKafkaSource = (KafkaTableSourceBase) actualSource;
        tableEnv.registerTableSource("user_log", actualKafkaSource);

        TableSchema schema = TableSchema.builder()
                .field("dt", org.apache.flink.table.api.Types.STRING())
                .field("pv", org.apache.flink.table.api.Types.LONG())
                .field("uv", org.apache.flink.table.api.Types.LONG())
                .build();

        Properties KAFKA_PROPERTIES = new Properties();
        KAFKA_PROPERTIES.setProperty("zookeeper.connect", "localhost:2181");
        KAFKA_PROPERTIES.setProperty("group.id", "dummy");
        KAFKA_PROPERTIES.setProperty("bootstrap.servers", "localhost:9092");

        String TOPIC = "pvuv_sink";
        KafkaTableSink kafkaTableSink = new KafkaTableSink(schema, TOPIC, KAFKA_PROPERTIES,
                Optional.of(new FlinkFixedPartitioner<>()),
                new JsonRowSerializationSchema.Builder(schema.toRowType()).build());
        tableEnv.registerTableSink("pvuv_sink", kafkaTableSink);

        Table tbl = tableEnv.sqlQuery("SELECT\n" +
                "  DATE_FORMAT(ts, 'yyyy-MM-dd HH:00') dt,\n" +
                "  COUNT(*) AS pv,\n" +
                "  COUNT(DISTINCT user_id) AS uv\n" +
                "FROM user_log\n" +
                "GROUP BY DATE_FORMAT(ts, 'yyyy-MM-dd HH:00')");

//        tbl.insertInto("pvuv_sink");
        // log.cleanup.policy=compact
        DataStream<Tuple2<Boolean, Row>> logDs = tableEnv.toRetractStream(tbl, Row.class);

        // 转成 <Boolean, String>
        DataStream<Tuple2<Boolean, Tuple3<String, String, String>>> ds = logDs.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<Boolean, Tuple3<String, String, String>>>() {
            @Override
            public Tuple2<Boolean, Tuple3<String, String, String>> map(Tuple2<Boolean, Row> value) throws Exception {
                String key = (String) value.f1.getField(0);
                return new Tuple2<Boolean, Tuple3<String, String, String>>(value.f0, new Tuple3("pvuv_sink", key, value.f1.toString()));
            }
        });

        ds.print();

        FlinkKafkaProducer sink = new FlinkKafkaProducer("pvuv_sink",
                new MySchema(), KAFKA_PROPERTIES, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        ds.addSink(sink);

        env.execute("KafkaDemo");
    }

    static class MySchema implements KafkaSerializationSchema<Tuple2<Boolean, Tuple3<String/*topic*/, String/*key*/, String/*value*/>>> {


        @Override
        public ProducerRecord<byte[], byte[]> serialize(Tuple2<Boolean, Tuple3<String, String, String>> element, @Nullable Long timestamp) {
            Boolean isAdd = element.f0;
            Tuple3<String, String, String> item = element.f1;
            String topic = item.f0;
            String key = item.f1;
            String value = item.f2;
            ProducerRecord<byte[], byte[]> record = null;
            if (isAdd) {
                record = new ProducerRecord<>(topic, key.getBytes(), value.getBytes());
            } else {
                record = new ProducerRecord<>(topic, key.getBytes(), null);
            }
            return record;
        }
    }
}
