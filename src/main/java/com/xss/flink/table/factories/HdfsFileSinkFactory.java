package com.xss.flink.table.factories;

import com.xss.flink.table.factories.sink.HdfsFileStreamTableSink;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.TABLE_SCHEMA_EXPR;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.Schema.*;

public class HdfsFileSinkFactory implements StreamTableSourceFactory<Row>, StreamTableSinkFactory<Tuple2<Boolean, Row>>  {

    private static final String CONNECTOR_WRITE_BASE_PATH = "connector.write.base-path";
    private static final String CONNECTOR_WRITE_BUCKET_ASSIGNER_FORMAT = "connector.write.bucket-assigner-format";
    private static final String CONNECTOR_WRITE_FIELD_DELIMITER = "connector.write.field-delimiter";
    private static final String CONNECTOR_WRITE_ROLLOVER_INTERVAL = "connector.write.rollover-interval";
    private static final String CONNECTOR_WRITE_PART_MAX_SIZE_MB = "connector.write.part-max-size-mb";
    private static final String CONNECTOR_EVENT_FIELD = "connector.write.event-field";


    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, "hdfs");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();

        //hdfs
        properties.add(CONNECTOR_WRITE_BASE_PATH);
        properties.add(CONNECTOR_WRITE_BUCKET_ASSIGNER_FORMAT);
        properties.add(CONNECTOR_WRITE_FIELD_DELIMITER);
        properties.add(CONNECTOR_WRITE_ROLLOVER_INTERVAL);
        properties.add(CONNECTOR_WRITE_PART_MAX_SIZE_MB);
        properties.add(CONNECTOR_EVENT_FIELD);

        //primary key
        properties.add("primarykey.#.name");


        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);

        // computed column
        properties.add(SCHEMA + ".#." + TABLE_SCHEMA_EXPR);

        // format wildcard
        properties.add(FORMAT + ".*");
        return properties;
    }

    @Override
    public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        TableSchema schema = descriptorProperties.getTableSchema(SCHEMA);

        if (!descriptorProperties.containsKey(CONNECTOR_WRITE_BASE_PATH)) {
            throw new RuntimeException(CONNECTOR_WRITE_BASE_PATH + " is required");
        }
        HdfsFileStreamTableSink tableSink = new HdfsFileStreamTableSink(schema, descriptorProperties.getString(CONNECTOR_WRITE_BASE_PATH));

        if (descriptorProperties.containsKey(CONNECTOR_WRITE_BUCKET_ASSIGNER_FORMAT)) {
            tableSink.setBucketAssignerFormat(descriptorProperties.getString(CONNECTOR_WRITE_BUCKET_ASSIGNER_FORMAT));
        }

        if (descriptorProperties.containsKey(CONNECTOR_WRITE_FIELD_DELIMITER)) {
            String delimiter = StringEscapeUtils.unescapeJava(descriptorProperties.getString(CONNECTOR_WRITE_FIELD_DELIMITER));
            tableSink.setFieldDelimiter(delimiter);
        }

        if (descriptorProperties.containsKey(CONNECTOR_EVENT_FIELD)) {
           tableSink.setEventFieldName(descriptorProperties.getString(CONNECTOR_EVENT_FIELD));
        }

        return tableSink;
    }

    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> map) {
        throw new RuntimeException("hdfs source not support yet");
    }
}
