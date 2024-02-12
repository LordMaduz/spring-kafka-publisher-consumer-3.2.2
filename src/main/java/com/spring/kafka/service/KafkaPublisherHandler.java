package com.spring.kafka.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.RoutingKafkaTemplate;
import org.springframework.stereotype.Component;


@Log4j2
@Component
@RequiredArgsConstructor
public class KafkaPublisherHandler {

    private final RoutingKafkaTemplate routingKafkaTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publish(final KafkaTemplate<String, Object> template, final String topic, final Object payload) {

        template.send(topic, payload).whenComplete((result, exception) -> {
            if (exception == null) {
                log.info("Event Published Successfully with Offset: {}", result.getRecordMetadata().offset());
                return;
            }
            log.info("Unable to Publish Message: {}", exception, exception);
        });
    }

    public void publish(final String topic, final Object payload) {

        kafkaTemplate.send(topic, payload).whenComplete((result, exception) -> {
            if (exception == null) {
                log.info("Event Published Successfully with Offset: {}", result.getRecordMetadata().offset());
                return;
            }
            log.info("Unable to Publish Message: {}", exception, exception);
        });
    }

    public void publishWithRouting(final String topic, final Object payload) {

        routingKafkaTemplate.send(topic, payload).whenComplete((result, exception) -> {
            if (exception == null) {
                log.info("Event Published Successfully with Offset: {}", result.getRecordMetadata().offset());
                return;
            }
            log.info("Unable to Publish Message: {}", exception, exception);
        });
    }
}
