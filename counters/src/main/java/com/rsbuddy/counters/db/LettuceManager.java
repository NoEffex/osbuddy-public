package com.rsbuddy.counters.db;

import com.typesafe.config.Config;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LettuceManager {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private final RedisClient client;

    private StatefulRedisConnection<String, String> connection;

    public LettuceManager(Config config) {
        RedisClient client = RedisClient.create(config.getString("url"));
        client.setDefaultTimeout(Duration.ofSeconds(config.getInt("timeoutSeconds")));
        this.client = client;
        this.executor.scheduleAtFixedRate(this::tickConnection, 0, 5, TimeUnit.SECONDS);
    }

    public Optional<StatefulRedisConnection<String, String>> connection() {
        return Optional.ofNullable(connection);
    }

    private void tickConnection() {
        if (connection != null) {
            return;
        }
        try {
            connection = client.connect();
        } catch (Exception e) {
            log.error("", e);
        }
    }
}
