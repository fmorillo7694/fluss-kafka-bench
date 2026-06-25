package io.medium.flussbench.datastream;

import io.medium.flussbench.common.BenchConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import org.apache.fluss.flink.sink.FlussSink;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;

/**
 * DataStream API cross-system job: consume JSON click events from a Kafka topic
 * and write them into the Fluss {@code clicks_log} table.
 *
 * <p>The "read Kafka, write Fluss" ingestion path — e.g. landing an existing
 * Kafka feed into Fluss storage so downstream jobs get columnar reads,
 * projection pushdown, and (for PK tables) lookup/delta joins.
 *
 * <p>Note: the Fluss sink ships {@code RowDataSerializationSchema} (for Flink
 * {@code RowData}) and a custom {@code FlussSerializationSchema}, but no built-in
 * JSON sink schema — so we parse the Kafka JSON into {@code RowData} ourselves and
 * use {@code RowDataSerializationSchema}.
 */
public class KafkaReadToFlussJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(BenchConfig.KAFKA_BOOTSTRAP)
                .setTopics(BenchConfig.CLICKS_TOPIC)
                .setGroupId("bench-kafka-to-fluss")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> json =
                env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source-clicks");

        // JSON -> RowData matching the clicks_log schema (click_id, user_id, url, ts).
        org.apache.flink.streaming.api.datastream.DataStream<RowData> rows = json
                .map(KafkaReadToFlussJob::toRowData)
                .returns(InternalTypeInfo.of(ClickSchema.ROW_TYPE))
                .name("json-to-rowdata");

        // Log table is append-only -> isAppendOnly = true, ignoreDelete = false.
        FlussSink<RowData> sink = FlussSink.<RowData>builder()
                .setBootstrapServers(BenchConfig.FLUSS_BOOTSTRAP)
                .setDatabase(BenchConfig.DB)
                .setTable(BenchConfig.CLICKS_LOG)
                .setSerializationSchema(new RowDataSerializationSchema(true, false))
                .build();

        rows.sinkTo(sink).name("fluss-sink-from-kafka");

        env.execute("KafkaReadToFlussJob (Kafka -> Fluss)");
    }

    private static RowData toRowData(String value) throws Exception {
        JsonNode n = MAPPER.readTree(value);
        GenericRowData row = new GenericRowData(4);
        row.setField(0, n.get("click_id").asLong());
        row.setField(1, n.get("user_id").asLong());
        row.setField(2, StringData.fromString(n.get("url").asText()));
        // clicks_log.ts is TIMESTAMP(3); accept epoch millis or fall back to now.
        long millis = n.hasNonNull("ts") && n.get("ts").canConvertToLong()
                ? n.get("ts").asLong()
                : System.currentTimeMillis();
        row.setField(3, TimestampData.fromEpochMillis(millis));
        return row;
    }
}
