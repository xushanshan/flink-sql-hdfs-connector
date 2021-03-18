package com.xss.flink.table.factories.sink;

import com.xss.flink.table.factories.bucket.EventTimeBucketAssigner;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;

public class HdfsFileStreamTableSink implements UpsertStreamTableSink<Row> {

    private final TableSchema schema;
    private final String hdfsFileBasePath;

    private String bucketAssignerFormat = "yyyy-MM-dd";
    private CharSequence fieldDelimiter = "\u0001";
    private String eventFieldName;


    public HdfsFileStreamTableSink(TableSchema schema, String hdfsFileBasePath) {
        this.schema = schema;
        this.hdfsFileBasePath = hdfsFileBasePath;
    }

    @Override
    public void emitDataStream(final DataStream<Tuple2<Boolean, Row>> dataStream) {
        this.consumeDataStream(dataStream);
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        StreamingFileSink<Tuple2<Boolean, Row>> streamingFileSink;
        if (this.eventFieldName != null) {
            int eventFiledIndex = Arrays.asList(this.schema.getFieldNames()).indexOf(this.eventFieldName);
            if (eventFiledIndex < 0) {
                throw new RuntimeException(String.format("Event field %s not found in schema!", this.eventFieldName));
            }
            streamingFileSink = StreamingFileSink
                    .forBulkFormat(new Path(hdfsFileBasePath), new TupleRowWriterFactory(this.fieldDelimiter))
                    .withBucketAssigner(new EventTimeBucketAssigner(eventFiledIndex, bucketAssignerFormat))
                    .withBucketCheckInterval(60 * 1000)
                    .withRollingPolicy(OnCheckpointRollingPolicy.build())
                    .build();
        } else {
            streamingFileSink = StreamingFileSink
                    .forBulkFormat(new Path(hdfsFileBasePath), new TupleRowWriterFactory(this.fieldDelimiter))
                    .withBucketAssigner(new DateTimeBucketAssigner<>(bucketAssignerFormat))
                    .withBucketCheckInterval(60 * 1000)
                    .withRollingPolicy(OnCheckpointRollingPolicy.build())
                    .build();
        }
        return dataStream.addSink(streamingFileSink);
    }

    @Override
    public void setKeyFields(final String[] keys) {}

    @Override
    public void setIsAppendOnly(final Boolean isAppendOnly) {}

    @Override
    public TypeInformation<Row> getRecordType() {
        return schema.toRowType();
    }

    @Override
    public TypeInformation<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo<>(Types.BOOLEAN, getRecordType());
    }

    @Override
    public String[] getFieldNames() {
        return schema.getFieldNames();
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return schema.getFieldTypes();
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        throw new RuntimeException("hdfs sink table schema configure not support yet");
    }

    public void setBucketAssignerFormat(String bucketAssignerFormat) {
        this.bucketAssignerFormat = bucketAssignerFormat;
    }

    public void setFieldDelimiter(CharSequence fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    public void setEventFieldName(String eventFieldName) {
        this.eventFieldName = eventFieldName;
    }

    public static class TupleRowWriterFactory implements BulkWriter.Factory<Tuple2<Boolean, Row>> {
        private final CharSequence fieldDelimiter;

        public TupleRowWriterFactory(CharSequence fieldDelimiter) {
            this.fieldDelimiter = fieldDelimiter;
        }

        @Override
        public BulkWriter<Tuple2<Boolean, Row>> create(FSDataOutputStream fsDataOutputStream) throws IOException {
            return new TupleRowBulkWriter(fsDataOutputStream, this.fieldDelimiter);
        }
    }

    public static class TupleRowBulkWriter implements BulkWriter<Tuple2<Boolean, Row>> {
        private final FSDataOutputStream fsDataOutputStream;
        private final CharSequence fieldDelimiter;

        public TupleRowBulkWriter(FSDataOutputStream fsDataOutputStream, CharSequence fieldDelimiter) {
            this.fsDataOutputStream = fsDataOutputStream;
            this.fieldDelimiter = fieldDelimiter;
        }


        @Override
        public void addElement(Tuple2<Boolean, Row> element) throws IOException {
            boolean update = element.f0;
            if (update) {
                //组装数据
                Row row = element.f1;
                String[] data = new String[row.getArity()];
                for (int i = 0; i < row.getArity(); i++) {
                    Object value = row.getField(i);
                    if (value == null) {
                        data[i] = "\\N";
                    } else if (value instanceof Timestamp) {
                        Timestamp timestamp = (Timestamp) value;
                        data[i] = Long.toString(timestamp.getTime());
                    } else {
                        data[i] = value.toString();
                    }
                }
                String content = String.join(fieldDelimiter, data);
                this.fsDataOutputStream.write(content.getBytes());
                this.fsDataOutputStream.write("\n".getBytes());
            }
        }

        @Override
        public void flush() throws IOException {
            this.fsDataOutputStream.flush();
        }

        @Override
        public void finish() throws IOException {
            this.fsDataOutputStream.close();
        }
    }
}