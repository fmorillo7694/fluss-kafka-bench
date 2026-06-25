package io.medium.flussbench.common;

/**
 * Shared connection + tuning constants for the benchmark jobs.
 * Defaults target the docker-compose stack; override via --bootstrap / --kafka.
 */
public final class BenchConfig {

    private BenchConfig() {}

    public static final String FLUSS_BOOTSTRAP = "coordinator-server:9123";
    public static final String KAFKA_BOOTSTRAP = "kafka:9092";

    public static final String DB = "bench";

    // Fluss tables
    public static final String CLICKS_LOG = "clicks_log";
    public static final String USERS_PK = "users_pk";
    public static final String ORDERS_PK = "orders_pk";

    // Kafka topics
    public static final String CLICKS_TOPIC = "clicks";
    public static final String USERS_TOPIC = "users";
    public static final String ORDERS_TOPIC = "orders";
}
