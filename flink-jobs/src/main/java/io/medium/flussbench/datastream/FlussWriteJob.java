package io.medium.flussbench.datastream;

import io.medium.flussbench.common.BenchConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import org.apache.fluss.flink.sink.FlussSink;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;

/**
 * DataStream API: generate synthetic click events and write them to the Fluss
 * append-only log table {@code clicks_log} via {@link FlussSink}.
 *
 * <p>Run (after `mvn package` and the jar is mounted into /opt/flink/usrlib):
 * <pre>
 *   docker compose exec jobmanager ./bin/flink run \
 *     -c io.medium.flussbench.datastream.FlussWriteJob /opt/flink/usrlib/fluss-kafka-bench-1.0.0.jar \
 *     --rps 50000 --records 5000000
 * </pre>
 */
public class FlussWriteJob {

    public static void main(String[] args) throws Exception {
        final long records = argLong(args, "--records", 5_000_000L);
        final long rps = argLong(args, "--rps", 50_000L);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Rate-limited datagen so throughput is the controlled variable.
        GeneratorFunction<Long, RowData> gen = seq -> {
            GenericRowData row = new GenericRowData(4);
            row.setField(0, seq);                                   // click_id BIGINT
            row.setField(1, seq % 1000);                            // user_id BIGINT
            row.setField(2, StringData.fromString("/p/" + (seq % 50))); // url STRING
            row.setField(3, TimestampData.fromEpochMillis(System.currentTimeMillis()));
            return row;
        };

        DataGeneratorSource<RowData> source = new DataGeneratorSource<>(
                gen,
                records,
                RateLimiterStrategy.perSecond(rps),
                org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(ClickSchema.ROW_TYPE));

        DataStreamSource<RowData> clicks =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "datagen-clicks");

        // Log table is append-only -> isAppendOnly = true.
        FlussSink<RowData> sink = FlussSink.<RowData>builder()
                .setBootstrapServers(BenchConfig.FLUSS_BOOTSTRAP)
                .setDatabase(BenchConfig.DB)
                .setTable(BenchConfig.CLICKS_LOG)
                .setSerializationSchema(new RowDataSerializationSchema(true, false))
                .build();

        clicks.sinkTo(sink).name("fluss-sink-clicks");

        env.execute("FlussWriteJob (DataStream -> Fluss log table)");
    }

    private static long argLong(String[] args, String key, long def) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(key)) {
                return Long.parseLong(args[i + 1]);
            }
        }
        return def;
    }
}
