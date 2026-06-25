package io.medium.flussbench.datastream;

import io.medium.flussbench.common.BenchConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.flink.source.FlussSource;
import org.apache.fluss.flink.source.deserializer.JsonStringDeserializationSchema;

/**
 * DataStream API cross-system job: read the Fluss {@code clicks_log} table and
 * republish each record as JSON to a Kafka topic.
 *
 * <p>Shows the "read Fluss, write Kafka" interop path with the standard Flink
 * Kafka connector (no special bridge). The reverse direction (Kafka -> Fluss)
 * is the same pattern with the source/sink swapped.
 */
public class FlussReadToKafkaJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Read Fluss as JSON strings; start from earliest so we drain the table.
        FlussSource<String> source = FlussSource.<String>builder()
                .setBootstrapServers(BenchConfig.FLUSS_BOOTSTRAP)
                .setDatabase(BenchConfig.DB)
                .setTable(BenchConfig.CLICKS_LOG)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializationSchema(new JsonStringDeserializationSchema())
                .build();

        DataStreamSource<String> clicks =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "fluss-source-clicks");

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(BenchConfig.KAFKA_BOOTSTRAP)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(BenchConfig.CLICKS_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        clicks.sinkTo(kafkaSink).name("kafka-sink-clicks");

        env.execute("FlussReadToKafkaJob (Fluss -> Kafka)");
    }
}
