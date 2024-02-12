package com.spring.kafka.listener;

import com.spring.kafka.InventoryProto;
import com.spring.kafka.mapper.InventoryProtoMapper;
import com.spring.kafka.model.Inventory;
import com.spring.kafka.model.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventListener {

    private final InventoryProtoMapper inventoryProtoMapper;

    @Transactional
    @KafkaListener(id = "ORDER_PROCESSED_GROUP", topics = "order-event",
            containerFactory = "jsonKafkaListenerContainerFactory")
    public void onJsonEventReceived(ConsumerRecord<String, Order> record,
                                     Acknowledgment acknowledgment) {
        Order order = record.value();
        log.info("Order Processed Event Received: {}", order);
        acknowledgment.acknowledge();
    }

    @Transactional
    @KafkaListener(id = "BYTE_ARRAY_GROUP", topics = "byteChannel",
            containerFactory = "byteArrayKafkaListenerContainerFactory")
    public void onByteArrayEventReceived(ConsumerRecord<String, byte[]> record,
                                     Acknowledgment acknowledgment) {
        byte[] kafkaPayload = record.value();
        log.info("Kafka Payload Received: {}", (Order) SerializationUtils.deserialize(kafkaPayload));
        acknowledgment.acknowledge();
    }

    @Transactional
    @KafkaListener(id = "INVENTORY_UPDATED_GROUP", topics = "inventory-event",
            containerFactory = "protoKafkaListenerContainerFactory")
    public void onProtoEventReceived(ConsumerRecord<String, InventoryProto.Inventory> record,
                                         Acknowledgment acknowledgment) {
        Inventory inventory = inventoryProtoMapper.fromProto(record.value());
        log.info("Inventory Updated Event Received: {}", inventory);
        acknowledgment.acknowledge();
    }
}
