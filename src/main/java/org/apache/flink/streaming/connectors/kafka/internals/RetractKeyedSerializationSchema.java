package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class RetractKeyedSerializationSchema<K> implements KafkaSerializationSchema<Tuple2<Boolean, Row>> {

    private String topic;
    private SerializationSchema<Row> serializationSchema;

    public RetractKeyedSerializationSchema(String topic, SerializationSchema<Row> serializationSchema) {
        this.topic = topic;
        this.serializationSchema = serializationSchema;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<Boolean, Row> element, @Nullable Long timestamp) {
        Boolean isAdd = element.f0;
        Row row = element.f1;
        ProducerRecord<byte[], byte[]> record = null;
        if (isAdd) {
            record = new ProducerRecord<>(topic, row.toString().getBytes(), serializationSchema.serialize(row));
        } else {
            record = new ProducerRecord<>(topic, row.toString().getBytes(), null);
        }
        return record;
    }
}
