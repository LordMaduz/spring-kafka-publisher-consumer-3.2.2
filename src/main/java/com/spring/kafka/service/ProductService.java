package com.spring.kafka.service;

import com.spring.kafka.mapper.InventoryProtoMapper;
import com.spring.kafka.model.Inventory;
import com.spring.kafka.model.Order;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.SerializationUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class ProductService {

    private final KafkaTemplate<String, Object> byteKafkaTemplate;
    private final KafkaTemplate<String, Object> jsonKafkaTemplate;
    private final KafkaTemplate<String, Object> protoKafkaTemplate;
    private final KafkaPublisherHandler kafkaPublisherHandler;
    private final InventoryProtoMapper mapper;

    public void publishMessages() {
        publishOrderMessage();
        publishInventoryMessage();
        //publishBinaryMessage();
    }

    private void publishOrderMessage() {
        final Order event = order();
        kafkaPublisherHandler.publish(jsonKafkaTemplate, "order-event", event);
        kafkaPublisherHandler.publish("order-event", event);
        kafkaPublisherHandler.publishWithRouting("order-event", event);
    }

    private void publishInventoryMessage() {
        final Inventory inventory = Inventory.builder()
                .inventoryId("123")
                .name("Inventory-A")
                .build();
        kafkaPublisherHandler.publish(protoKafkaTemplate, "inventory-event", mapper.toProto(inventory));
        kafkaPublisherHandler.publish("inventory-event", mapper.toProto(inventory));
        kafkaPublisherHandler.publishWithRouting("inventory-event", mapper.toProto(inventory));
    }

    private void publishBinaryMessage() {
        final Order event = order();
        kafkaPublisherHandler.publish(byteKafkaTemplate, "byteChannel", SerializationUtils.serialize(event));
        kafkaPublisherHandler.publish("other-event", SerializationUtils.serialize(event));
        kafkaPublisherHandler.publishWithRouting("other-event", SerializationUtils.serialize(event));
    }

    private Order order() {
        final Order event = new Order();

        event.setPayload("payload");
        event.setCreatedDate(LocalDateTime.now());

        return event;
    }
}


