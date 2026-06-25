package io.medium.flussbench.datastream;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.Arrays;

/** RowType describing the {@code clicks_log} table, shared by the DataStream jobs. */
public final class ClickSchema {

    private ClickSchema() {}

    public static final RowType ROW_TYPE = RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {
                    new BigIntType(false),                 // click_id
                    new BigIntType(false),                 // user_id
                    new VarCharType(VarCharType.MAX_LENGTH),// url
                    new TimestampType(3)                   // ts
            },
            new String[] {"click_id", "user_id", "url", "ts"});

    public static final java.util.List<String> FIELD_NAMES =
            Arrays.asList("click_id", "user_id", "url", "ts");
}
