package com.spring.kafka.kafka;

import com.spring.kafka.InventoryProto;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.*;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;
import java.util.regex.Pattern;

@Slf4j
@EnableKafka
@Configuration
public class KafkaConsumerConfig extends KafkaBasicConfig {

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> basicConfig = getBasicConfig();

        final Map map = Map.of(
                Pattern.compile("proto-event-topic"), new KafkaProtobufDeserializer<>(),
                Pattern.compile("avro-event-topic"), new KafkaAvroDeserializer(),
                Pattern.compile("json-event-topic"), jsonDeserializer(),
                Pattern.compile("byte-event-topic"), new ByteArrayDeserializer()
            );

        basicConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        basicConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        basicConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        basicConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        basicConfig.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class.getName());
        basicConfig.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, InventoryProto.Inventory.class.getName());
        basicConfig.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        basicConfig.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, DelegatingByTopicDeserializer.class.getName());
        basicConfig.put(DelegatingByTopicDeserializer.VALUE_SERIALIZATION_TOPIC_CONFIG, map);

        return new DefaultKafkaConsumerFactory<>(basicConfig);
    }

    @Bean
    public ConsumerFactory<String, Object> jsonConsumerFactory() {
        Map<String, Object> basicConfig = getBasicConfig();
        basicConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        basicConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        basicConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        basicConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        basicConfig.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class.getName());
        basicConfig.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        basicConfig.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        basicConfig.put(JsonDeserializer.REMOVE_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaConsumerFactory<>(basicConfig);
    }

    @Bean
    public ConsumerFactory<String, Object> byteArrayConsumerFactory() {
        Map<String, Object> basicConfig = getBasicConfig();
        basicConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        basicConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        basicConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        basicConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        basicConfig.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class.getName());
        basicConfig.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, ByteArrayDeserializer.class.getName());

        return new DefaultKafkaConsumerFactory<>(basicConfig);
    }

    @Bean
    public ConsumerFactory<String, Object> protoConsumerFactory() {
        Map<String, Object> basicConfig = getBasicConfig();
        basicConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        basicConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        basicConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        basicConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        basicConfig.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class.getName());
        basicConfig.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaProtobufDeserializer.class.getName());
        basicConfig.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, InventoryProto.Inventory.class.getName());

        return new DefaultKafkaConsumerFactory<>(basicConfig);
    }

    @Bean
    public ConsumerFactory<String, Object> avroConsumerFactory() {
        Map<String, Object> basicConfig = getBasicConfig();
        basicConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        basicConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        basicConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        basicConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        basicConfig.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class.getName());
        basicConfig.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class.getName());
        basicConfig.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        return new DefaultKafkaConsumerFactory<>(basicConfig);
    }

    @Bean()
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConcurrency(6);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(errorHandler());
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }


    @Bean()
    public ConcurrentKafkaListenerContainerFactory<String, Object> jsonKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConcurrency(6);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(errorHandler());
        factory.setConsumerFactory(jsonConsumerFactory());
        return factory;
    }

    @Bean()
    public ConcurrentKafkaListenerContainerFactory<String, Object> byteArrayKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConcurrency(6);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(errorHandler());
        factory.setConsumerFactory(byteArrayConsumerFactory());
        return factory;
    }

    @Bean()
    public ConcurrentKafkaListenerContainerFactory<String, Object> protoKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConcurrency(6);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(errorHandler());
        factory.setConsumerFactory(protoConsumerFactory());
        return factory;
    }

    @Bean()
    public ConcurrentKafkaListenerContainerFactory<String, Object> avroKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConcurrency(6);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(errorHandler());
        factory.setConsumerFactory(avroConsumerFactory());
        return factory;
    }


    private DefaultErrorHandler errorHandler() {
        return new DefaultErrorHandler((record, exception) -> {
            log.error("!!!!!!!!!!! Following Record keeps failing despite multiple attempts, therefore would be skipped !! : " + record);
            log.error(exception.getMessage());
        }, new FixedBackOff(0L, 2L));

    }

    private <T> JsonDeserializer<T> jsonDeserializer(){
        final JsonDeserializer<T> jsonDeserializer = new JsonDeserializer<>();
        jsonDeserializer.addTrustedPackages("*");
        jsonDeserializer.setRemoveTypeHeaders(false);

        return jsonDeserializer;
    }
}
