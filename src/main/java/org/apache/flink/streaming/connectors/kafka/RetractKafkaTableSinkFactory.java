package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.KafkaValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import java.util.*;

import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.KafkaValidator.*;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PROPERTIES_KEY;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PROPERTIES_VALUE;
import static org.apache.flink.table.descriptors.Rowtime.*;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_DELAY;
import static org.apache.flink.table.descriptors.Schema.*;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_PROCTIME;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_RETRACT;

public class RetractKafkaTableSinkFactory implements StreamTableSourceFactory<Row>, StreamTableSinkFactory<Tuple2<Boolean, Row>> {

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(UPDATE_MODE, UPDATE_MODE_VALUE_RETRACT); // append mode
        context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_KAFKA); // kafka
        context.put(CONNECTOR_VERSION, KafkaValidator.CONNECTOR_VERSION_VALUE_UNIVERSAL); // version
        context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();

        // kafka
        properties.add(CONNECTOR_TOPIC);
        properties.add(CONNECTOR_PROPERTIES);
        properties.add(CONNECTOR_PROPERTIES + ".#." + CONNECTOR_PROPERTIES_KEY);
        properties.add(CONNECTOR_PROPERTIES + ".#." + CONNECTOR_PROPERTIES_VALUE);
        properties.add(CONNECTOR_STARTUP_MODE);
        properties.add(CONNECTOR_SPECIFIC_OFFSETS + ".#." + CONNECTOR_SPECIFIC_OFFSETS_PARTITION);
        properties.add(CONNECTOR_SPECIFIC_OFFSETS + ".#." + CONNECTOR_SPECIFIC_OFFSETS_OFFSET);
        properties.add(CONNECTOR_SINK_PARTITIONER);
        properties.add(CONNECTOR_SINK_PARTITIONER_CLASS);

        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);
        properties.add(SCHEMA + ".#." + SCHEMA_FROM);

        // time attributes
        properties.add(SCHEMA + ".#." + SCHEMA_PROCTIME);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY);

        // format wildcard
        properties.add(FORMAT + ".*");

        return properties;
    }

    @Override
    public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

        final TableSchema schema = descriptorProperties.getTableSchema(SCHEMA);
        final String topic = descriptorProperties.getString(CONNECTOR_TOPIC);
        final Optional<String> proctime = SchemaValidator.deriveProctimeAttribute(descriptorProperties);
        final List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors =
                SchemaValidator.deriveRowtimeAttributes(descriptorProperties);

        // see also FLINK-9870
        if (proctime.isPresent() || !rowtimeAttributeDescriptors.isEmpty() ||
                checkForCustomFieldMapping(descriptorProperties, schema)) {
            throw new TableException("Time attributes and custom field mappings are not supported yet.");
        }

        return new RetractKafkaTableSink(
                schema,
                topic,
                getKafkaProperties(descriptorProperties),
                getFlinkKafkaPartitioner(descriptorProperties),
                getSerializationSchema(properties));
    }

    private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        // allow Kafka timestamps to be used, watermarks can not be received from source
        new SchemaValidator(true, true, false).validate(descriptorProperties);
        new KafkaValidator().validate(descriptorProperties);

        return descriptorProperties;
    }

    private boolean checkForCustomFieldMapping(DescriptorProperties descriptorProperties, TableSchema schema) {
        final Map<String, String> fieldMapping = SchemaValidator.deriveFieldMapping(
                descriptorProperties,
                Optional.of(schema.toRowType())); // until FLINK-9870 is fixed we assume that the table schema is the output type
        return fieldMapping.size() != schema.getFieldNames().length ||
                !fieldMapping.entrySet().stream().allMatch(mapping -> mapping.getKey().equals(mapping.getValue()));
    }

    private Properties getKafkaProperties(DescriptorProperties descriptorProperties) {
        final Properties kafkaProperties = new Properties();
        final List<Map<String, String>> propsList = descriptorProperties.getFixedIndexedProperties(
                CONNECTOR_PROPERTIES,
                Arrays.asList(CONNECTOR_PROPERTIES_KEY, CONNECTOR_PROPERTIES_VALUE));
        propsList.forEach(kv -> kafkaProperties.put(
                descriptorProperties.getString(kv.get(CONNECTOR_PROPERTIES_KEY)),
                descriptorProperties.getString(kv.get(CONNECTOR_PROPERTIES_VALUE))
        ));
        return kafkaProperties;
    }

    private Optional<FlinkKafkaPartitioner<Row>> getFlinkKafkaPartitioner(DescriptorProperties descriptorProperties) {
        return descriptorProperties
                .getOptionalString(CONNECTOR_SINK_PARTITIONER)
                .flatMap((String partitionerString) -> {
                    switch (partitionerString) {
                        case CONNECTOR_SINK_PARTITIONER_VALUE_FIXED:
                            return Optional.of(new FlinkFixedPartitioner<>());
                        case CONNECTOR_SINK_PARTITIONER_VALUE_ROUND_ROBIN:
                            return Optional.empty();
                        case CONNECTOR_SINK_PARTITIONER_VALUE_CUSTOM:
                            final Class<? extends FlinkKafkaPartitioner> partitionerClass =
                                    descriptorProperties.getClass(CONNECTOR_SINK_PARTITIONER_CLASS, FlinkKafkaPartitioner.class);
                            return Optional.of((FlinkKafkaPartitioner<Row>) InstantiationUtil.instantiate(partitionerClass));
                        default:
                            throw new TableException("Unsupported sink partitioner. Validator should have checked that.");
                    }
                });
    }

    private SerializationSchema<Row> getSerializationSchema(Map<String, String> properties) {
        @SuppressWarnings("unchecked")
        final SerializationSchemaFactory<Row> formatFactory = TableFactoryService.find(
                SerializationSchemaFactory.class,
                properties,
                this.getClass().getClassLoader());
        return formatFactory.createSerializationSchema(properties);
    }

    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        return null;
    }
}
