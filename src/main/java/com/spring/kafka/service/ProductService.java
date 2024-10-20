package com.spring.kafka.service;

import static java.util.UUID.randomUUID;

import com.spring.kafka.mapper.InventoryProtoMapper;
import com.spring.kafka.model.Inventory;
import com.spring.kafka.model.Order;
import com.spring.kafka.model.avro.TransactionEvent;
import com.spring.kafka.model.avro.TransactionEventBody;
import com.spring.kafka.model.avro.TransactionEventHeader;
import com.spring.kafka.model.avro.TransactionType;

import lombok.RequiredArgsConstructor;

import org.apache.commons.lang3.SerializationUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ProductService {

    private final KafkaTemplate<String, Object> byteKafkaTemplate;
    private final KafkaTemplate<String, Object> jsonKafkaTemplate;
    private final KafkaTemplate<String, Object> protoKafkaTemplate;
    private final KafkaTemplate<String, Object> avroKafkaTemplate;
    private final KafkaPublisherHandler kafkaPublisherHandler;
    private final InventoryProtoMapper mapper;

    public void publishMessages() {
        publishOrderMessage();
        publishInventoryMessage();
        publishBinaryMessage();
        publishAvroMessage();
    }

    private void publishOrderMessage() {
        final Order event = order();
        kafkaPublisherHandler.publish(jsonKafkaTemplate, "json-event-topic", event);
        kafkaPublisherHandler.publish("json-event-topic", event);
        kafkaPublisherHandler.publishWithRouting("json-event-topic", event);
    }

    private void publishInventoryMessage() {
        final Inventory inventory = Inventory.builder()
            .inventoryId("123")
            .name("Inventory-A")
            .build();
        kafkaPublisherHandler.publish(protoKafkaTemplate, "protobuf-event-topic", mapper.toProto(inventory));
        kafkaPublisherHandler.publish("protobuf-event-topic", mapper.toProto(inventory));
        kafkaPublisherHandler.publishWithRouting("protobuf-event-topic", mapper.toProto(inventory));
    }

    private void publishBinaryMessage() {
        final Order event = order();
        kafkaPublisherHandler.publish(byteKafkaTemplate, "binary-event-topic", SerializationUtils.serialize(event));
        kafkaPublisherHandler.publish("binary-event-topic", SerializationUtils.serialize(event));
        kafkaPublisherHandler.publishWithRouting("binary-event-topic", SerializationUtils.serialize(event));
    }

    private void publishAvroMessage() {
        final TransactionEvent event = TransactionEvent.newBuilder()
            .setHeader(TransactionEventHeader.newBuilder()
                .setId(randomUUID().toString())
                .setSourceSystem("spring-kafka-publisher-consumer-app")
                .setCreatedAt(Instant.now().toEpochMilli())
                .build()
            )
            .setBody(TransactionEventBody.newBuilder()
                .setTransactionId(randomUUID().toString())
                .setUserId(randomUUID().toString())
                .setTransactionType(TransactionType.INSTANT_PAYMENT)
                .setDate(Instant.now().toEpochMilli())
                .setAmount(1500)
                .setCurrency("EUR")
                .setDescription("Description")
                .build())
            .build();
        kafkaPublisherHandler.publish(avroKafkaTemplate, "avro-event-topic", event);
        kafkaPublisherHandler.publish("avro-event-topic", SerializationUtils.serialize(event));
        kafkaPublisherHandler.publishWithRouting("avro-event-topic", SerializationUtils.serialize(event));
    }

    private Order order() {
        final Order event = new Order();

        event.setPayload("payload");
        event.setCreatedDate(LocalDateTime.now());

        return event;
    }
}


