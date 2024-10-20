package com.spring.kafka.kafka;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaBasicConfig {

    protected final MeterRegistry registry = new SimpleMeterRegistry();

    protected Map<String, Object> getBasicConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("schema.registry.url", "http://localhost:8081");
        config.put("auto.register.schemas", true);
        config.put("producer.properties.schema.registry.url", "http://localhost:8081");
        config.put("consumer.properties.schema.registry.url", "http://localhost:8081");
        return config;
    }

}
