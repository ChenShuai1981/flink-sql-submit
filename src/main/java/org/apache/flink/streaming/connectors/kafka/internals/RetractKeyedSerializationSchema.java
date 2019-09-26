package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class RetractKeyedSerializationSchema implements KafkaSerializationSchema<Tuple2<Boolean, Tuple2<String, Row>>> {

    private String topic;
    private SerializationSchema<String> keySchema;
    private SerializationSchema<Row> valueSchema;

    public RetractKeyedSerializationSchema(String topic, SerializationSchema<String> keySchema, SerializationSchema<Row> valueSchema) {
        this.topic = topic;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<Boolean, Tuple2<String, Row>> element, @Nullable Long timestamp) {
        Boolean isAdd = element.f0;
        Tuple2<String, Row> item = element.f1;
        String key = item.f0;
        Row value = item.f1;
        ProducerRecord<byte[], byte[]> record = null;
        if (isAdd) {
            record = new ProducerRecord<>(topic, keySchema.serialize(key), valueSchema.serialize(value));
        } else {
            record = new ProducerRecord<>(topic, keySchema.serialize(key), null);
        }
        return record;
    }
}
