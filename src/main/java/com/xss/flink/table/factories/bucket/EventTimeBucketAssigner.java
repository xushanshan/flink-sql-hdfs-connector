package com.xss.flink.table.factories.bucket;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class EventTimeBucketAssigner implements BucketAssigner<Tuple2<Boolean, Row>, String> {

    private static final long serialVersionUID = 1L;

    private static final String DEFAULT_FORMAT_STRING = "yyyy-MM-dd--HH";

    private final String formatString;

    private final ZoneId zoneId;

    private final int eventTimeFieldIndex;

    private transient DateTimeFormatter dateTimeFormatter;

    /**
     * Creates a new {@code DateTimeBucketAssigner} with format string {@code "yyyy-MM-dd--HH"}.
     */
    public EventTimeBucketAssigner(int eventTimeFieldIndex) {
        this(eventTimeFieldIndex, DEFAULT_FORMAT_STRING);
    }

    /**
     * Creates a new {@code DateTimeBucketAssigner} with the given date/time format string.
     *
     * @param formatString The format string that will be given to {@code SimpleDateFormat} to determine
     *                     the bucket id.
     */
    public EventTimeBucketAssigner(int eventTimeFieldIndex, String formatString) {
        this(eventTimeFieldIndex, formatString, ZoneId.systemDefault());
    }

    /**
     * Creates a new {@code DateTimeBucketAssigner} with format string {@code "yyyy-MM-dd--HH"} using the given timezone.
     *
     * @param zoneId The timezone used to format {@code DateTimeFormatter} for bucket id.
     */
    public EventTimeBucketAssigner(int eventTimeFieldIndex, ZoneId zoneId) {
        this(eventTimeFieldIndex, DEFAULT_FORMAT_STRING, zoneId);
    }

    /**
     * Creates a new {@code DateTimeBucketAssigner} with the given date/time format string using the given timezone.
     *
     * @param formatString The format string that will be given to {@code DateTimeFormatter} to determine
     *                     the bucket path.
     * @param zoneId The timezone used to format {@code DateTimeFormatter} for bucket id.
     */
    public EventTimeBucketAssigner(int eventTimeFieldIndex, String formatString, ZoneId zoneId) {
        this.eventTimeFieldIndex = eventTimeFieldIndex;
        this.formatString = Preconditions.checkNotNull(formatString);
        this.zoneId = Preconditions.checkNotNull(zoneId);
    }

    /*
    * 从记录中获取事件事件确定分区字段
    * */
    @Override
    public String getBucketId(Tuple2<Boolean, Row> element, Context context) {
        if (dateTimeFormatter == null) {
            dateTimeFormatter = DateTimeFormatter.ofPattern(formatString).withZone(zoneId);
        }
        LocalDateTime eventTime;
        Object eventTimeValue = element.f1.getField(eventTimeFieldIndex);
        if (eventTimeValue instanceof Date) {
            eventTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(((Date)eventTimeValue).getTime()), zoneId);
        } else if (eventTimeValue instanceof LocalDateTime) {
            eventTime = (LocalDateTime) eventTimeValue;
        } else if (eventTimeValue instanceof String) {
            eventTime = Timestamp.valueOf((String)eventTimeValue).toLocalDateTime();
        } else {
            eventTime = LocalDateTime.now(zoneId);
        }
        return dateTimeFormatter.format(eventTime);
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}