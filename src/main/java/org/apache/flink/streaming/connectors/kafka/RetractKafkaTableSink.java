package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.internals.RetractKeyedSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

public class RetractKafkaTableSink implements RetractStreamTableSink<Row> {

    /** The schema of the table. */
    private final TableSchema schema;

    /** The Kafka topic to write to. */
    protected final String topic;

    /** Properties for the Kafka producer. */
    protected final Properties properties;

    /** Serialization schema for encoding records to Kafka. */
    protected final SerializationSchema<String> keySchema;

    /** Serialization schema for encoding records to Kafka. */
    protected final SerializationSchema<Row> valueSchema;

    /** Partitioner to select Kafka partition for each item. */
    protected final Optional<FlinkKafkaPartitioner<Row>> partitioner;

    public RetractKafkaTableSink(
            TableSchema schema,
            String topic,
            Properties properties,
            Optional<FlinkKafkaPartitioner<Row>> partitioner,
            SerializationSchema<String> keySchema,
            SerializationSchema<Row> valueSchema) {
        this.schema = Preconditions.checkNotNull(schema, "Schema must not be null.");
        this.topic = Preconditions.checkNotNull(topic, "Topic must not be null.");
        this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
        this.partitioner = Preconditions.checkNotNull(partitioner, "Partitioner must not be null.");
        this.keySchema = Preconditions.checkNotNull(keySchema, "Key schema must not be null.");
        this.valueSchema = Preconditions.checkNotNull(valueSchema, "Value schema must not be null.");
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(schema.getFieldTypes(), schema.getFieldNames());
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        consumeDataStream(dataStream);
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        final SinkFunction<Tuple2<Boolean, Row>> kafkaProducer = createKafkaProducer(
                topic,
                properties,
                keySchema,
                valueSchema,
                partitioner);

        return dataStream
                .addSink(kafkaProducer)
                .setParallelism(dataStream.getParallelism())
                .name(TableConnectorUtils.generateRuntimeName(this.getClass(), getFieldNames()));
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        if (!Arrays.equals(getFieldNames(), fieldNames) || !Arrays.equals(getFieldTypes(), fieldTypes)) {
            throw new ValidationException("Reconfiguration with different fields is not allowed. " +
                    "Expected: " + Arrays.toString(getFieldNames()) + " / " + Arrays.toString(getFieldTypes()) + ". " +
                    "But was: " + Arrays.toString(fieldNames) + " / " + Arrays.toString(fieldTypes));
        }
        return this;
    }

    private SinkFunction<Tuple2<Boolean, Row>> createKafkaProducer(
            String topic,
            Properties properties,
            SerializationSchema<String> keySchema,
            SerializationSchema<Row> valueSchema,
            Optional<FlinkKafkaPartitioner<Row>> partitioner) {
        return new FlinkKafkaProducer(
                topic,
                new RetractKeyedSerializationSchema(topic, keySchema, valueSchema),
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RetractKafkaTableSink that = (RetractKafkaTableSink) o;
        return Objects.equals(schema, that.schema) &&
                Objects.equals(topic, that.topic) &&
                Objects.equals(properties, that.properties) &&
                Objects.equals(keySchema, that.keySchema) &&
                Objects.equals(valueSchema, that.valueSchema) &&
                Objects.equals(partitioner, that.partitioner);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schema,
                topic,
                properties,
                keySchema,
                valueSchema,
                partitioner);
    }
}
